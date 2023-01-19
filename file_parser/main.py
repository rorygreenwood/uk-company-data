import datetime
import shutil

import mysql.connector
import os
from file_downloader.companyhouse_transfer import collect_companieshouse_file
from utils import unzip_ch_file, fragment_ch_file
from fragment_work import parse_fragment, load_fragment

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
print('downloading file')
ch_file = collect_companieshouse_file()
str_ch_file = str(ch_file)
print('unzipping file')
unzipped_ch_file = unzip_ch_file(ch_file)
fragment_ch_file(f'../file_downloader/files/{unzipped_ch_file}')

fragment_list = os.listdir('../file_downloader/files/fragments/')
for fragment in fragment_list:
    print(fragment)
    parse_fragment(f'../file_downloader/files/fragments/{fragment}')
    load_fragment(cursor, db)
    print('------')
    os.remove(f'../file_downloader/files/fragments/{fragment}')

# when done, update filetracker
filetracker_tup = (str_ch_file, datetime.datetime.now(), datetime.datetime.now(), datetime.datetime.now())
cursor.execute("""insert into BasicCompanyData_filetracker (filename, lastModified, lastDownloaded, lastProcessed) VALUES (%s, %s, %s, %s)""")
db.commit()
