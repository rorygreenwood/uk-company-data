"""take fragments from s3 bucket and load them into rchis."""
import os
import sys

import boto3

from fragment_work import parse_fragment
from utils import pipeline_message_wrap, timer, handle_exception, logger, connect_preprod

__mycode = True
sys.excepthook = handle_exception

# connected to preprod
host = os.environ.get('PREPRODHOST')
user = os.environ.get('ADMINUSER')
passwd = os.environ.get('ADMINPASS')
database = os.environ.get('DATABASE')
cursor, db = connect_preprod()

bucket_name = os.environ.get('S3_COMPANIES_HOUSE_FRAGMENTS_BUCKET')
s3_client = boto3.client('s3',
                         aws_access_key_id=os.environ.get('HGDATA-S3-AWS-ACCESS-KEY-ID'),
                         aws_secret_access_key=os.environ.get('HGDATA-S3-AWS-SECRET-KEY'),
                         region_name='eu-west-1'
                         )
list_of_s3_objects = s3_client.list_objects_v2(Bucket=bucket_name, Prefix="", Delimiter="/")


def get_row_count_of_s3_csv(bucket_name, path):
    sql_stmt = """SELECT count(*) FROM s3object """
    req = boto3.client('s3').select_object_content(
        Bucket=bucket_name,
        Key=path['Key'],
        ExpressionType="SQL",
        Expression=sql_stmt,
        InputSerialization={"CSV": {"FileHeaderInfo": "Use", "AllowQuotedRecordDelimiter": True}},
        OutputSerialization={"CSV": {}},
    )

    row_count = next(int(x["Records"]["Payload"]) for x in req["Payload"])
    return row_count


def find_bucket_count():
    """
    1. list all fragments in bucket
    2. append size of bucket to rowcount
    3. once all fragments have had their rowcount appended, write value into db
    """
    rowcount = 0
    for s3_object in list_of_s3_objects['Contents']:
        rows = get_row_count_of_s3_csv(bucket_name=bucket_name, path=s3_object)
        rowcount += rows
        print(rowcount)
    cursor.execute("""
    update companies_house_rowcounts set file_rowcount = %s where 
    month(file_month) = month(curdate()) and 
    year(file_month) = year(curdate())
    """, (rowcount,))
    db.commit()


@pipeline_message_wrap
@timer
def process_section2():
    """
    1. iterate over fragments found in s3 companies house bucket
    2. use parse_fragment function on each fragment
    3. remove fragment file from local files
    4. remove fragment file from s3 bucket
    :return:
    """
    for s3_object in list_of_s3_objects['Contents']:
        # check if the file has already been processed in the past, if it has it will be in this table
        # if it is not in the table, then we can assume it has not been parsed and can continue to process it
        logger.info(s3_object["Key"])
        if s3_object['Key'].endswith('.csv'):
            fragment_file_name = s3_object['Key']
            s3_client.download_file(bucket_name, fragment_file_name,
                                    f'file_downloader/files/fragments\\{fragment_file_name}')
            logger.info('parsing ')
            fragments_abspath = os.path.abspath('file_downloader/files/fragments')
            parse_fragment(f'{fragments_abspath}/{fragment_file_name}',
                           host=host, user=user, passwd=passwd,
                           db=database, cursor=cursor, cursordb=db)
            os.remove(f'file_downloader/files/fragments/{fragment_file_name}')
            s3_client.delete_object(Bucket=bucket_name, Key=fragment_file_name)
        else:
            pass

    # once complete, update filetracker
    cursor.execute("""update companies_house_filetracker
     set section2 = true where filename = %s and section2 is null""", ('',))


if __name__ == '__main__':
    find_bucket_count()
    process_section2()

