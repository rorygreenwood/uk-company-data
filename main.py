import datetime
import time

from main_section1 import search_and_collect_ch_file
from file_parser.fragment_work import parse_fragment
from file_parser.utils import unzip_ch_file, fragment_file, pipeline_messenger
from main_funcs import *
import os
from file_parser.utils import date_check
from pipeline_messenger_messages import *

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
    # check for pre-existing files to be loaded first
    # if len(fragment_list) == 1 then only fragments.txt present, download file and begin process
    try:
        ch_file, ch_upload_date = search_and_collect_ch_file(firstDayOfMonth)
    except Exception as e:
        pipeline_messenger(title=ch_pipeline_fail, text=f'Collection Fail: {e}', hexcolour=hexcolour_red)
        quit()
    str_ch_file = str(ch_file)
    logger.info('unzipping file')
    unzipped_ch_file = unzip_ch_file(ch_file)
    fragment_file(f'file_downloader/files/{unzipped_ch_file}')
    os.remove(f'file_downloader/files/{unzipped_ch_file}')
    fragment_list = os.listdir('file_downloader/files/fragments/')

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

func_list = [
    (add_organisation_id, 'add_organisation_id'),  # adds organisation_id to the staging table rchis
    (update_org_name, 'update_org_name'),  # update statement on organisation, pairing with rchis on org_id
    (update_org_website, 'update_org_website'),  # update statement on organisation, pairing with rchis on org_id
    (update_org_activity, 'update_org_activity'),  # update statement on organisation, pairing with rchis on org_id
    (write_to_org, 'write_to_org'),  # insert statement into organisation, pairing with rchis on org_id
    (sql_sic, 'sql_sic'),  # loop of statements that writes sic codes from staging into sic_codes table
    (insert_sic_counts(month=datetime.date.today().month), ''),
    load_calculations(first_month=datetime.date.today().month-1, second_month=datetime.date.today().month)
    (find_more_postcodes, 'find_more_postcodes'),  # updates rchis
    # todo 2/10 can these two functions below be put into a single statement?
    # todo 12/10 update already has issue with md5 duplicates that needs addressing still.
    (geolocation_update_current, 'geolocation_update_current'),  # updates geo_location from rchis
    (geolocation_insert_excess, 'geolocation_insert_excess'),  # insert ignores into rchis
]
for func, err in func_list:
    try:
        func(cursor, db)
    except Exception as e:
        pipeline_title = 'Companies House File Pipeline Failed'
        pipeline_message = f'{func} {e}'
        pipeline_hexcolour = '#c40000'
        pipeline_messenger(title=pipeline_title, text=pipeline_message, hexcolour=pipeline_hexcolour)
        quit()

# #todo remove this
# ch_upload_date = firstDayOfMonth
# str_ch_file = '2023-10-04'
# fragment_number = 1

integration_with_preprod_end = time.time()
time_integration_with_preprod = integration_with_preprod_end - integration_with_preprod_start
# when done, update filetracker (DEPRECATED SINCE 03/03/23??)
filetracker_tup = (str_ch_file, ch_upload_date, datetime.datetime.now(), datetime.datetime.now())
cursor.execute(
    """insert into BasicCompanyData_filetracker (filename, ch_upload_date, lastDownloaded, lastProcessed) VALUES (%s, %s, %s, %s)""",
    filetracker_tup)
db.commit()

end_time = time.time()
pipeline_time = end_time - start_time
pipeline_title = 'Companies House File loaded'
pipeline_message = f"""File Date: {ch_upload_date}
Number of Fragments preload: {fragment_number}
Time taken on loading fragments: {fragment_loading_time}
Time taken on total pipeline: {pipeline_time}
Avg time per fragment: {fragment_loading_time / fragment_number}
"""
pipeline_hexcolour = '#00c400'
pipeline_messenger(title=pipeline_title, text=pipeline_message, hexcolour=pipeline_hexcolour)
