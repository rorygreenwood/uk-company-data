import datetime
import requests as r
import boto3
import sys
import logging

logger = logging.getLogger()
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s [line:%(lineno)d] %(levelname)s: %(message)s')


def collect_companieshouse_file():
    # todo generate a date and check it against filetracker, if that date already exists, end the script
    firstDayOfMonth = datetime.date(datetime.date.today().year, datetime.date.today().month, 1)
    logger.info(f'set date as {firstDayOfMonth}')
    filename = 'BasicCompanyDataAsOneFile-' + str(firstDayOfMonth) + '.zip'
    logger.info(f'set filename as {filename}')
    baseurl = 'http://download.companieshouse.gov.uk/' + filename
    'BasicCompanyDataAsOneFile-2022-12-01.zip'
    logger.info(f'sending request using url: {baseurl}')
    req = r.get(baseurl, stream=True)
    with open('../file_downloader/files/' + filename, 'wb') as fd:
        chunkcount = 0
        for chunk in req.iter_content(chunk_size=100000):
            chunkcount += 1
            # logger.info(f'writing chunk {chunkcount}')
            fd.write(chunk)
    return filename, firstDayOfMonth

