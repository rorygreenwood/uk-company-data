"""take fragments from s3 bucket and load them into rchis."""

import boto3

from fragment_work import parse_fragment
from utils import pipeline_message_wrap, timer
import os
import logging
from main_funcs import connect_preprod

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s [line:%(lineno)d] %(levelname)s: %(message)s')
logger = logging.getLogger()
logger.info(os.environ)
# connected to preprod
host = os.environ.get('PREPRODHOST')
user = os.environ.get('ADMINUSER')
passwd = os.environ.get('ADMINPASS')
database = os.environ.get('DATABASE')
logger.info(f'using {user}, {passwd} on host: {host} to connect to database {database}')
cursor, db = connect_preprod()

bucket_name = 'iqblade-data-services-companieshouse-fragments'
s3_client = boto3.client('s3',
                         aws_access_key_id=os.environ.get('HGDATA-S3-AWS-ACCESS-KEY-ID'),
                         aws_secret_access_key=os.environ.get('HGDATA-S3-AWS-SECRET-KEY'),
                         region_name='eu-west-1'
                         )
list_of_s3_objects = s3_client.list_objects_v2(Bucket=bucket_name, Prefix="", Delimiter="/")


@pipeline_message_wrap
@timer
def process_section2():
    for s3_object in list_of_s3_objects['Contents']:
        # check if the file has already been processed in the past, if it has it will be in this table
        # if it is not in the table, then we can assume it has not been parsed and can continue to process it
        logger.info(s3_object["Key"])
        if s3_object['Key'].endswith('.csv'):
            fragment_file_name = s3_object['Key']
            s3_client.download_file(bucket_name, fragment_file_name, f'file_downloader/files/fragments\\{fragment_file_name}')
            parse_fragment(f'file_downloader/files/fragments/{fragment_file_name}', host=host, user=user, passwd=passwd,
                           db=database, cursor=cursor, cursordb=db)
            os.remove(f'file_downloader/files/fragments/{fragment_file_name}')
            s3_client.delete_object(Bucket=bucket_name, Key=fragment_file_name)
        else:
            pass


if __name__ == '__main__':
    process_section2()
