import datetime
import logging
import os
import sys
import time

import mysql.connector

from file_downloader.companyhouse_transfer import collect_companieshouse_file
from file_parser.fragment_work import parse_fragment
from file_parser.utils import unzip_ch_file, fragment_ch_file, date_check

from new_main_funcs import *

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s [line:%(lineno)d] %(levelname)s: %(message)s')
logger = logging.getLogger()

if len(sys.argv) > 2:
    host = sys.argv[1]
    user = sys.argv[2]
    passwd = sys.argv[3]
    database = sys.argv[4]
else:
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

# t_fragment_file = 'BasicCompanyDataAsOneFile-2023-01-01.csv'
# download file
logger.info('downloading file')
firstDayOfMonth = datetime.date(datetime.date.today().year, datetime.date.today().month, 1)
# verify that a new file needs to be downloaded
verif_check = date_check(file_date=firstDayOfMonth, cursor=cursor)
if verif_check:
    logger.info('file already exists in tracker')
    quit()

# check for pre-existing files to be loaded first
fragment_list = os.listdir('file_downloader/files/fragments/')

# if len(fragment_list) == 1 then only fragments.txt present, download file and begin process
if len(fragment_list) == 1:
    ch_file, ch_upload_date = collect_companieshouse_file(firstDayOfMonth)
    str_ch_file = str(ch_file)
    logger.info('unzipping file')
    unzipped_ch_file = unzip_ch_file(ch_file)
    fragment_ch_file(f'file_downloader/files/{unzipped_ch_file}')
    os.remove(f'file_downloader/files/{unzipped_ch_file}')
    fragment_list = os.listdir('file_downloader/files/fragments/')
else:
    # if fragments already present
    str_ch_file = 'BasicCompanyDataAsOneFile-' + str(firstDayOfMonth) + '.zip'
    ch_upload_date = firstDayOfMonth

logger.info('loading fragments...')
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

try:
    # add org_id and md5 on rchis
    add_organisation_id(cursor, db)
    logger.info('add_organisation_id completed')
    # update to organisation (add ids, update ids)
    try:
        update_org_website(cursor, db)
        logger.info('update_org_website completed')
    except Exception as e:
        pipeline_title = 'Companies House File Pipeline Failed'
        pipeline_message = f'check update_org_website: {e}'
        pipeline_hexcolour = '#62a832'
        pipeline_messenger(title=pipeline_title, text=pipeline_message, hexcolour=pipeline_hexcolour)
        quit()
    try:
        update_org_activity(cursor, db)
        logger.info('update_org_activity completed')
    except Exception as e:
        pipeline_title = 'Companies House File Pipeline Failed'
        pipeline_message = f'check update_org_activity: {e}'
        pipeline_hexcolour = '#62a832'
        pipeline_messenger(title=pipeline_title, text=pipeline_message, hexcolour=pipeline_hexcolour)
        quit()
    try:
        write_to_org(cursor, db)
        logger.info('write_to_org completed')
    except Exception as e:
        pipeline_title = 'Companies House File Pipeline Failed'
        pipeline_message = f'check write_to-org: {e}'
        pipeline_hexcolour = '#62a832'
        pipeline_messenger(title=pipeline_title, text=pipeline_message, hexcolour=pipeline_hexcolour)
        quit()
    # add and update sic
    try:
        sql_sic(cursor, db)
        logger.info('sql_sic completed')
    except Exception as e:
        pipeline_title = 'Companies House File Pipeline Failed'
        pipeline_message = f'check sql_sic: {e}'
        pipeline_hexcolour = '#62a832'
        pipeline_messenger(title=pipeline_title, text=pipeline_message, hexcolour=pipeline_hexcolour)
        quit()
    # add and update addresses
    try:
        geolocation_md5_gen(cursor, db)
        logger.info('geolocation_md5_gen completed')
    except Exception as e:
        pipeline_title = 'Companies House File Pipeline Failed'
        pipeline_message = f'check geolocation_md5_gem: {e}'
        pipeline_hexcolour = '#62a832'
        pipeline_messenger(title=pipeline_title, text=pipeline_message, hexcolour=pipeline_hexcolour)
        quit()
    # geolocation_update_current(cursor, db)
    # logger.info('geolocation_update_current completed')
    try:
        geolocation_insert_excess(cursor, db)
        logger.info('geolocation_insert_excess completed')
    except Exception as e:
        pipeline_title = 'Companies House File Pipeline Failed'
        pipeline_message = f'check geolocation_insert_excess: {e}'
        pipeline_hexcolour = '#62a832'
        pipeline_messenger(title=pipeline_title, text=pipeline_message, hexcolour=pipeline_hexcolour)
        quit()

    # delete organisations
    try:
        del_from_org(cursor, db)
        logger.info('update_org_activity completed')
    except Exception as e:
        pipeline_title = 'Companies House File Pipeline Failed'
        pipeline_message = f'check del_from_org: {e}'
        pipeline_hexcolour = '#62a832'
        pipeline_messenger(title=pipeline_title, text=pipeline_message, hexcolour=pipeline_hexcolour)
        quit()
except Exception as err:
    pipeline_title = 'Companies House File Pipeline Failed'
    pipeline_message = f'File Date: {ch_upload_date} - {err}'
    pipeline_hexcolour = '#62a832'
    pipeline_messenger(title=pipeline_title, text=pipeline_message, hexcolour=pipeline_hexcolour)
    quit()

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
