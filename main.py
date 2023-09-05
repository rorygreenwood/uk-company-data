import datetime
import time

from file_downloader.companyhouse_transfer import collect_companieshouse_file
from file_parser.fragment_work import parse_fragment
from file_parser.utils import unzip_ch_file, fragment_ch_file, date_check, pipeline_messenger
from main_funcs import *
import os
from locker import connect_preprod
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

# t_fragment_file = 'BasicCompanyDataAsOneFile-2023-01-01.csv'
# download file
logger.info('downloading file')
firstDayOfMonth = datetime.date(datetime.date.today().year, datetime.date.today().month, 1)
# verify that a new file needs to be downloaded
verif_check = date_check(file_date=firstDayOfMonth, cursor=cursor)
if verif_check:
    logger.info('file already exists in tracker')
    pipeline_title = 'Companies House File Pipeline Complete'
    pipeline_message = 'No new File'
    pipeline_hexcolour = '#c40000'
    pipeline_messenger(title=pipeline_title, text=pipeline_message, hexcolour=pipeline_hexcolour)
    quit()
else:
    pipeline_title = 'Companies House File Pipeline'
    pipeline_message = f'New Companies House File: {firstDayOfMonth}'
    pipeline_hexcolour = '##00c400'
    pipeline_messenger(title=pipeline_title, text=pipeline_message, hexcolour=pipeline_hexcolour)
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
fragment_number = len(fragment_list)
fragment_loading_start = time.time()
for fragment in fragment_list:
    print(fragment)
    if fragment != 'fragments.txt':
        logger.info(fragment)
        st = time.time()
        parse_fragment(f'file_downloader/files/fragments/{fragment}', host=host, user=user, passwd=passwd,
                              db=database, cursor=cursor, cursordb=db)
        os.remove(f'file_downloader/files/fragments/{fragment}')
        et = time.time()
        final_time = et - st
        logger.info(f'parse time for this iteration: {final_time}')
    else:
        pass
fragment_loading_end = time.time()

fragment_loading_time = fragment_loading_end - fragment_loading_start

integration_with_preprod_start = time.time()
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
        pipeline_hexcolour = '#c40000'
        pipeline_messenger(title=pipeline_title, text=pipeline_message, hexcolour=pipeline_hexcolour)
        quit()
    try:
        update_org_activity(cursor, db)
        logger.info('update_org_activity completed')
    except Exception as e:
        pipeline_title = 'Companies House File Pipeline Failed'
        pipeline_message = f'check update_org_activity: {e}'
        pipeline_hexcolour = '#c40000'
        pipeline_messenger(title=pipeline_title, text=pipeline_message, hexcolour=pipeline_hexcolour)
        quit()
    try:
        write_to_org(cursor, db)
        logger.info('write_to_org completed')
    except Exception as e:
        pipeline_title = 'Companies House File Pipeline Failed'
        pipeline_message = f'check write_to_org: {e}'
        pipeline_hexcolour = '#c40000'
        pipeline_messenger(title=pipeline_title, text=pipeline_message, hexcolour=pipeline_hexcolour)
        quit()
    # add and update sic
    try:
        sql_sic(cursor, db)
        logger.info('sql_sic completed')
    except Exception as e:
        pipeline_title = 'Companies House File Pipeline Failed'
        pipeline_message = f'check sql_sic: {e}'
        pipeline_hexcolour = '#c40000'
        pipeline_messenger(title=pipeline_title, text=pipeline_message, hexcolour=pipeline_hexcolour)
        quit()
    # add and update addresses
    try:
        # find postcodes amongst other address columns
        find_more_postcodes(cursor, db)
        # produce md5 to use as unique key
        geolocation_md5_gen(cursor, db)
        logger.info('geolocation_md5_gen completed')
    except Exception as e:
        pipeline_title = 'Companies House File Pipeline Failed'
        pipeline_message = f'check geolocation_md5_gen: {e}'
        pipeline_hexcolour = '#c40000'
        pipeline_messenger(title=pipeline_title, text=pipeline_message, hexcolour=pipeline_hexcolour)
        quit()
    try:
        geolocation_update_current(cursor, db)
        logger.info('geolocation_update_current completed')
    except Exception as e:
        pipeline_title = 'Companies House File Pipeline Failed'
        pipeline_message = f'check geolocation_update_current: {e}'
        pipeline_hexcolour = '#c40000'
        pipeline_messenger(title=pipeline_title, text=pipeline_message, hexcolour=pipeline_hexcolour)
        quit()
    try:
        geolocation_insert_excess(cursor, db)
        logger.info('geolocation_insert_excess completed')
    except Exception as e:
        pipeline_title = 'Companies House File Pipeline Failed'
        pipeline_message = f'check geolocation_insert_excess: {e}'
        pipeline_hexcolour = '#c40000'
        pipeline_messenger(title=pipeline_title, text=pipeline_message, hexcolour=pipeline_hexcolour)
        quit()

    # delete organisations
    try:
        del_from_org(cursor, db)
        logger.info('update_org_activity completed')
    except Exception as e:
        pipeline_title = 'Companies House File Pipeline Failed'
        pipeline_message = f'check del_from_org: {e}'
        pipeline_hexcolour = '#c40000'
        pipeline_messenger(title=pipeline_title, text=pipeline_message, hexcolour=pipeline_hexcolour)
        quit()
except Exception as err:
    pipeline_title = 'Companies House File Pipeline Failed'
    pipeline_message = f'File Date: {ch_upload_date} - {err}'
    pipeline_hexcolour = '#c40000'
    pipeline_messenger(title=pipeline_title, text=pipeline_message, hexcolour=pipeline_hexcolour)
    quit()
integration_with_preprod_end = time.time()
time_integration_with_preprod = integration_with_preprod_end - integration_with_preprod_start
# when done, update filetracker (DEPRECATED SINCE 03/03/23??)
filetracker_tup = (str_ch_file, ch_upload_date, datetime.datetime.now(), datetime.datetime.now())
cursor.execute(
    """insert into BasicCompanyData_filetracker (filename, ch_upload_date, lastDownloaded, lastProcessed) VALUES (%s, %s, %s, %s)""",
    filetracker_tup)
db.commit()

end_time = time.time()
pipeline_time = end_time-start_time
pipeline_title = 'Companies House File loaded'
pipeline_message = f"""File Date: {ch_upload_date}
Number of Fragments preload: {fragment_number}
Time taken on loading fragments: {fragment_loading_time}
Time taken on total pipeline: {pipeline_time}
Avg time per fragment: {fragment_loading_time/fragment_number}
"""
pipeline_hexcolour = '#00c400'
pipeline_messenger(title=pipeline_title, text=pipeline_message, hexcolour=pipeline_hexcolour)
