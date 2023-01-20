import datetime
import shutil
import time
import mysql.connector
import os
import logging
from file_downloader.companyhouse_transfer import collect_companieshouse_file
from file_parser.utils import unzip_ch_file, fragment_ch_file, pipeline_messenger, date_check
from file_parser.fragment_work import parse_fragment, load_fragment


logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s [line:%(lineno)d] %(levelname)s: %(message)s')
logger = logging.getLogger()
print(os.environ.values())
host = os.environ.get('PREPRODHOST')
user = os.environ.get('USER')
passwd = os.environ.get('PASS')
database = os.environ.get('DATABASE')
print(host, user, passwd, database)
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
logger.info('downloading file')
firstDayOfMonth = datetime.date(datetime.date.today().year, datetime.date.today().month, 1)
# verify that a new file needs to be downloaded
verif_check = date_check(file_date=firstDayOfMonth, cursor=cursor)
if verif_check:
    logger.info('file exists, pass')
    pipeline_title = 'No Companies House File'
    pipeline_message = f'Pipeline closed for today'
    pipeline_hexcolour = '#8f0d1a'
    pipeline_messenger(title=pipeline_title, text=pipeline_message, hexcolour=pipeline_hexcolour)
    quit()

ch_file, ch_upload_date = collect_companieshouse_file(firstDayOfMonth)
str_ch_file = str(ch_file)
logger.info('unzipping file')
unzipped_ch_file = unzip_ch_file(ch_file)
fragment_ch_file(f'../file_downloader/files/{unzipped_ch_file}')
fragment_list = os.listdir('file_downloader/files/fragments/')
os.remove(f'../file_downloader/files/{unzipped_ch_file}')
for fragment in fragment_list:
    logger.info(fragment)
    st = time.time()
    parse_fragment(f'../file_downloader/files/fragments/{fragment}')
    load_fragment(cursor, db)
    logger.info('------')
    os.remove(f'../file_downloader/files/fragments/{fragment}')
    et = time.time()
    final_time = et-st
    logger.info(f'parse time for this iteration: {final_time}')

# when done, update filetracker
filetracker_tup = (str_ch_file, ch_upload_date, datetime.datetime.now(), datetime.datetime.now())
cursor.execute("""insert into BasicCompanyData_filetracker (filename, ch_upload_date, lastDownloaded, lastProcessed) VALUES (%s, %s, %s, %s)""", filetracker_tup)
db.commit()

pipeline_title = 'Companies House File loaded'
pipeline_message = f'File Date: {ch_upload_date}'
pipeline_hexcolour = '#62a832'
pipeline_messenger(title=pipeline_title, text=pipeline_message, hexcolour=pipeline_hexcolour)
