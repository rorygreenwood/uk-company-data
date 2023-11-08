"""
download ch file, fragment it, upload fragments to s3 and upload main file to DSIL
"""

import datetime
import os
import subprocess
import time

import boto3

from file_downloader.companyhouse_transfer import search_and_collect_ch_file
from file_parser.utils import date_check
from file_parser.utils import unzip_ch_file, fragment_file, pipeline_messenger
from locker import connect_preprod
from main_funcs import *
from pipeline_messenger_messages import *
global ch_file
global verif_check

start_time = time.time()
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s [line:%(lineno)d] %(levelname)s: %(message)s')
logger = logging.getLogger()

host = os.environ.get('HOST')
user = os.environ.get('ADMINUSER')
passwd = os.environ.get('ADMINPASS')
database = os.environ.get('DATABASE')

cursor, db = connect_preprod()

schema = 'iqblade'
aws_access_key_id = os.environ.get('HGDATA-S3-AWS-ACCESS-KEY-ID')
aws_secret_access_key = os.environ.get('HGDATA-S3-AWS-SECRET-KEY')
output_bucket_name = 'iqblade-data-services-companieshouse-fragments'
s3client = boto3.client('s3',
                        aws_access_key_id=os.environ.get('HGDATA-S3-AWS-ACCESS-KEY-ID'),
                        aws_secret_access_key=os.environ.get('HGDATA-S3-AWS-SECRET-KEY'),
                        region_name='eu-west-1'
                        )
# download file
firstDayOfMonth = datetime.date(datetime.date.today().year,
                                datetime.date.today().month,
                                datetime.date.today().day)
# verify that a new file needs to be downloaded
try:
    verif_check = date_check(file_date=firstDayOfMonth, cursor=cursor)
except Exception as e:
    pipeline_messenger(title=ch_pipeline_fail, text=f'Verification Fail: {e}', hexcolour=hexcolour_red)
    quit()

# this conditional only tirggers if we specify a file to search for
if verif_check:
    pipeline_messenger(title=ch_pipeline_fail, text='No New File', hexcolour=hexcolour_red)
    quit()
else:
    print('new file found')
    pipeline_messenger(title=ch_pipeline, text=f'New File Found: {firstDayOfMonth}', hexcolour=hexcolour_green)
    try:
        ch_file, ch_upload_date = search_and_collect_ch_file(firstDayOfMonth)
    except Exception as e:
        pipeline_messenger(title=ch_pipeline_fail, text=f'Collection Fail: {e}', hexcolour=hexcolour_red)
        quit()
    str_ch_file = str(ch_file)
    logger.info('unzipping file')
    unzipped_ch_file = unzip_ch_file(ch_file)
    fragment_file(file_name=f'file_downloader/files/{unzipped_ch_file}', output_dir='file_downloader/files/fragments/')
    os.remove(f'file_downloader/files/{unzipped_ch_file}')
    fragment_list = os.listdir('file_downloader/files/fragments/')
    s3_url = f's3://iqblade-data-services-companieshouse-fragments/'
    [subprocess.run(f'aws s3 mv {os.path.abspath(f"file_downloader/files/fragments/")} {s3_url}')
     for fragment in fragment_list]

    # once uplaoded, upload the CH file to DSIL path
    response = s3client.upload_file(ch_file, s3_url, ch_file)

