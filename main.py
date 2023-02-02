import datetime
import logging
import os
import time
import mysql.connector
from file_downloader.companyhouse_transfer import collect_companieshouse_file
from file_parser.fragment_work import parse_fragment, upsert_sic, upsert_address
from file_parser.utils import unzip_ch_file, fragment_ch_file, pipeline_messenger, date_check
from post_insert_updates_sic import sql_sic
from post_insert_updates_address import sql_update_addresses_wmd5
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s [line:%(lineno)d] %(levelname)s: %(message)s')
logger = logging.getLogger()

# host = sys.argv[1]
# user = sys.argv[2]
# passwd = sys.argv[3]
# database = sys.argv[4]
#
host = 'preprod.cqzf0yke9t3u.eu-west-1.rds.amazonaws.com'
user = 'rory'
passwd = 'Me._7;cBsqQ$]JX}'
database = 'iqblade'
db = mysql.connector.connect(
    host=host,
    user=user,
    passwd=passwd,
    database=database
)
cursor = db.cursor()

schema = 'iqblade'

t_fragment_file = 'BasicCompanyDataAsOneFile-2023-01-01.csv'
# download file
logger.info('downloading file')
firstDayOfMonth = datetime.date(datetime.date.today().year, datetime.date.today().month, 1)
# verify that a new file needs to be downloaded
verif_check = date_check(file_date=firstDayOfMonth, cursor=cursor)
# if verif_check:
#     logger.info('file exists, pass')
#     pipeline_title = 'No Companies House File'
#     pipeline_message = f'Pipeline closed for today'
#     pipeline_hexcolour = '#8f0d1a'
#     pipeline_messenger(title=pipeline_title, text=pipeline_message, hexcolour=pipeline_hexcolour)
#     quit()
#
fragment_list = os.listdir('file_downloader/files/fragments/')
if len(fragment_list) == 0:
    ch_file, ch_upload_date = collect_companieshouse_file(firstDayOfMonth)
    str_ch_file = str(ch_file)
    logger.info('unzipping file')
    unzipped_ch_file = unzip_ch_file(ch_file)
    fragment_ch_file(f'file_downloader/files/{unzipped_ch_file}')
    os.remove(f'file_downloader/files/{unzipped_ch_file}')
    fragment_list = os.listdir('file_downloader/files/fragments/')
# fragment_list = os.listdir('file_downloader/files/fragments/')
for fragment in fragment_list:
    print(fragment)
    if fragment != 'fragments.txt':
        logger.info(fragment)
        st = time.time()
        parse_fragment(f'file_downloader/files/fragments/{fragment}', host=host, user=user, passwd=passwd, db=database,
                       cursor=cursor, cursordb=db)
        logger.info('------')
        os.remove(f'file_downloader/files/fragments/{fragment}')
        et = time.time()
        final_time = et - st
        logger.info(f'parse time for this iteration: {final_time}')
    else:
        pass

# update raw companies house
sql_sic(cursor=cursor, db=db)
sql_update_addresses_wmd5(cursor=cursor, db=db)

# when done, update filetracker
filetracker_tup = (str_ch_file, ch_upload_date, datetime.datetime.now(), datetime.datetime.now())
cursor.execute(
    """insert into BasicCompanyData_filetracker (filename, ch_upload_date, lastDownloaded, lastProcessed) VALUES (%s, %s, %s, %s)""",
    filetracker_tup)
db.commit()

pipeline_title = 'Companies House File loaded'
pipeline_message = f'File Date: {ch_upload_date}'
pipeline_hexcolour = '#62a832'
pipeline_messenger(title=pipeline_title, text=pipeline_message, hexcolour=pipeline_hexcolour)
