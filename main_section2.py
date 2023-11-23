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
host = 'preprod.cqzf0yke9t3u.eu-west-1.rds.amazonaws.com'
user = os.environ.get('USER')
passwd = os.environ.get('PASS')
database = os.environ.get('DATABASE')
cursor, db = connect_preprod()

bucket_name = 'iqblade-data-services-companieshouse-fragments'
s3_client = boto3.client('s3',
                         aws_access_key_id=os.environ.get('HGDATA-S3-AWS-ACCESS-KEY-ID'),
                         aws_secret_access_key=os.environ.get('HGDATA-S3-AWS-SECRET-KEY'),
                         region_name='eu-west-1'
                         )
zipped_keys = s3_client.list_objects_v2(Bucket=bucket_name, Prefix="", Delimiter="/")
fragment_path = os.path.abspath('file_downloader/files/fragments')


@pipeline_message_wrap
@timer
def process_section2():
    for subkey in zipped_keys['Contents']:
        # check if the file has already been processed in the past, if it has it will be in this table
        # if it is not in the table, then we can assume it has not been parsed and can continue to process it
        logger.info(subkey["Key"])
        if subkey['Key'].endswith('.csv'):
            fragment_file_name = subkey['Key']
            # zd = str(subkey['LastModified'].date())
            s3_client.download_file(bucket_name, fragment_file_name, f'{fragment_path}\\{fragment_file_name}')
            parse_fragment(f'file_downloader/files/fragments/{fragment_file_name}', host=host, user=user, passwd=passwd,
                           db=database, cursor=cursor, cursordb=db)
            os.remove(f'file_downloader/files/fragments/{fragment_file_name}')
            s3_client.delete_object(Bucket=bucket_name, Key=fragment_file_name)
        else:
            pass


if __name__ == '__main__':
    process_section2()
