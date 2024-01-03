"""take fragments from s3 bucket and load them into rchis."""
import os
import sys

import boto3
from botocore.exceptions import ClientError

from fragment_work import parse_fragment
from main_funcs import connect_preprod
from utils import pipeline_message_wrap, timer, handle_exception, logger

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


def get_row_count_of_s3_csv(path) -> int:
    sql_stmt = """SELECT count(*) FROM s3object """
    req = boto3.client('s3').select_object_content(
        Bucket=bucket_name,
        Key=path,
        ExpressionType="SQL",
        Expression=sql_stmt,
        InputSerialization={"CSV": {"FileHeaderInfo": "Use", "AllowQuotedRecordDelimiter": True}},
        OutputSerialization={"CSV": {}},
    )

    row_count = next(int(x["Records"]["Payload"]) for x in req["Payload"])
    return row_count


rowcount_total = 0


def find_bucket_count(rowcount_total):
    for s3_object in list_of_s3_objects['Contents']:
        print(s3_object['Key'])
        try:
            rowcount_total += get_row_count_of_s3_csv(path=s3_object['Key'])
            print(rowcount_total)
        except ClientError:
            pass


@pipeline_message_wrap
@timer
def process_section2():
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
    process_section2()
    find_bucket_count(rowcount_total)
    print(rowcount_total, ' is total')
