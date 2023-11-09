"""take fragments from s3 bucket and load them into rchis."""
import os
import time

import boto3

from file_parser.fragment_work import parse_fragment
from file_parser.utils import pipeline_messenger
from main_funcs import *

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s [line:%(lineno)d] %(levelname)s: %(message)s')
logger = logging.getLogger()

# connected to preprod
host = os.environ.get('HOST')
user = os.environ.get('ADMINUSER')
passwd = os.environ.get('ADMINPASS')
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
fragment_loading_start = time.time()
for subkey in zipped_keys['Contents']:
    if subkey['Key'].endswith('.csv'):
        # check if the file has already been processed in the past, if it has it will be in this table
        # if it is not in the table, then we can assume it has not been parsed and can continue to process it
        fragment_file_name = subkey['Key']
        zd = str(subkey['LastModified'].date())
        s3_client.download_file(bucket_name, fragment_file_name, f'{fragment_path}\\{fragment_file_name}')
        st = time.time()
        parse_fragment(f'file_downloader/files/fragments/{fragment_file_name}', host=host, user=user, passwd=passwd,
                       db=database, cursor=cursor, cursordb=db)
        os.remove(f'file_downloader/files/fragments/{fragment_file_name}')
        et = time.time()
        final_time = et - st
        logger.info(f'parse time for this iteration: {final_time}')
    else:
        pass
fragment_loading_end = time.time()
fragment_loading_time = fragment_loading_end - fragment_loading_start

pipeline_title = 'Companies House File loaded'
pipeline_message = f"""File Date:
Time taken on total pipeline: {fragment_loading_time}
"""
pipeline_hexcolour = '#00c400'
pipeline_messenger(title=pipeline_title, text=pipeline_message, hexcolour=pipeline_hexcolour)
