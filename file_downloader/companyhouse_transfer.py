import logging

import requests as r

logger = logging.getLogger()
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s [line:%(lineno)d] %(levelname)s: %(message)s')


# https://download.companieshouse.gov.uk/BasicCompanyDataAsOneFile-2023-04-01.zip
# https://download.companieshouse.gov.uk/BasicCompanyDataAsOneFile-2023-01-01.zip
def collect_companieshouse_file(firstDayOfMonth):
    logger.info(f'set date as {firstDayOfMonth}')
    filename = 'BasicCompanyDataAsOneFile-' + str(firstDayOfMonth) + '.zip'
    logger.info(f'set filename as {filename}')
    baseurl = 'https://download.companieshouse.gov.uk/' + filename
    logger.info(f'sending request using url: {baseurl}')
    req = r.get(baseurl, stream=True)
    logger.info('downloading file')
    with open('file_downloader/files/' + filename, 'wb') as fd:
        chunkcount = 0
        for chunk in req.iter_content(chunk_size=100000):
            chunkcount += 1
            fd.write(chunk)
    return filename, firstDayOfMonth
