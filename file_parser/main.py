import datetime
import shutil
import time
import mysql.connector
import os
import logging
from file_downloader.companyhouse_transfer import collect_companieshouse_file
from utils import unzip_ch_file, fragment_ch_file, pipeline_messenger
from fragment_work import parse_fragment, load_fragment

logger = logging.getLogger()
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s [line:%(lineno)d] %(levelname)s: %(message)s')


db = mysql.connector.connect(
    host=os.environ.get('PREPRODHOST'),
    user=os.environ.get('USER'),
    passwd=os.environ.get('PASS'),
    database=os.environ.get('DATABASE')
)
schema = 'iqblade'
cursor = db.cursor()
t_fragment_file = 'BasicCompanyDataAsOneFile-2023-01-01.csv'
# download file
logging.info('downloading file')
ch_file, ch_upload_date = collect_companieshouse_file()
str_ch_file = str(ch_file)
logging.info('unzipping file')
unzipped_ch_file = unzip_ch_file(ch_file)
fragment_ch_file(f'../file_downloader/files/{unzipped_ch_file}')
fragment_list = os.listdir('../file_downloader/files/fragments/')
os.remove(f'../file_downloader/files/{unzipped_ch_file}')
for fragment in fragment_list:
    logging.info(fragment)
    parse_fragment(f'../file_downloader/files/fragments/{fragment}')
    load_fragment(cursor, db)
    logging.info('------')
    os.remove(f'../file_downloader/files/fragments/{fragment}')

# when done, update filetracker
filetracker_tup = (str_ch_file, ch_upload_date, datetime.datetime.now(), datetime.datetime.now())
cursor.execute("""insert into BasicCompanyData_filetracker (filename, ch_upload_date, lastDownloaded, lastProcessed) VALUES (%s, %s, %s, %s)""", filetracker_tup)
db.commit()

pipeline_title = 'Companies House File loaded'
pipeline_message = f'File Date: {ch_upload_date}'
pipeline_hexcolour = '#4798e2'
pipeline_messenger(title=pipeline_title, text=pipeline_message, hexcolour=pipeline_hexcolour)
