import logging
import re
import time
import os
import subprocess

import bs4
import boto3
import requests
import requests as r
import datetime
from file_parser.utils import timer, unzip_ch_file_s3_send, fragment_file, pipeline_message_wrap
from main_funcs import connect_preprod

logger = logging.getLogger()
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s [line:%(lineno)d] %(levelname)s: %(message)s')


@timer
def collect_companieshouse_file(filename):
    # logger.info(f'set date as {firstdateofmonth}')
    logger.info('collecting ch_file')
    logger.info(f'set filename as {filename}')
    baseurl = 'http://download.companieshouse.gov.uk/' + filename
    logger.info(f'sending request using url: {baseurl}')
    req = r.get(baseurl, stream=True, verify=False)
    logger.info('downloading file')
    with open('file_downloader/files/' + filename, 'wb') as fd:
        chunkcount = 0
        for chunk in req.iter_content(chunk_size=100000):
            chunkcount += 1
            fd.write(chunk)
            logger.info(chunkcount)
    return filename


@timer
def send_to_s3(filename, s3_url='s3://iqblade-data-services-companieshouse-fragments/'):
    """sends a file to a given s3 bucket, default is iqblade-td"""
    s3client = boto3.client('s3',
                            aws_access_key_id=os.environ.get('HGDATA-S3-AWS-ACCESS-KEY-ID'),
                            aws_secret_access_key=os.environ.get('HGDATA-S3-AWS-SECRET-KEY'),
                            region_name='eu-west-1'
                            )
    s3client.upload_file(filename, s3_url, filename)


@pipeline_message_wrap
@timer
def file_check_regex(cursor, db):
    """use regex to find the specified companies house file, and then check that with a filetracker (tbd)
    if a file is found that does not match what we already have
    """
    chfile_re = 'BasicCompanyDataAsOneFile-[0-9]{4}-[0-9]{2}-[0-9]{2}\.zip'
    logger.info('search called')
    initial_req_url = 'http://download.companieshouse.gov.uk/en_output.html'
    r = requests.get(initial_req_url, verify=False)
    r_content = r.content
    rsoup = bs4.BeautifulSoup(r_content, 'html.parser')
    links = rsoup.find_all('a')
    for i in links:
        # print(i['href'])
        filecheck = re.findall(string=i['href'], pattern=chfile_re)
        # when checked, find out how if file is in filetracker
        # if the re from filecheck returns a result, it's length is larger than 1 and we can assume a file has been found.
        # this filename needs to be checked against the companies house filetracker to see if it has already been processed
        # if it already has been processed, the script ends.
        # if not, download the file, fragment it, send the original file to the sftp and the fragments to the s3 bucket.
        if len(filecheck) != 0:
            month_re = re.findall(string=filecheck[0], pattern='[0-9]{4}-[0-9]{2}-[0-9]{2}')
            print('monthcheck: ', month_re[0])
            print('filecheck: ', filecheck[0])
            cursor.execute("""select * from companies_house_filetracker where filename = %s and section1 is not null""",
                           (filecheck[0],))
            res = cursor.fetchall()
            print(res)
            # if length of results is zero, we assume the file hasn't been downloaded
            if len(res) == 0:
                # download file
                print('file found to download')
                file_to_dl = filecheck[0]
                # collect_companieshouse_file(filename=file_to_dl)
                # unzip file and send .zip to s3
                unzipped_file = unzip_ch_file_s3_send(f'{file_to_dl}')
                # fragment file
                fragment_file(file_name=f'file_downloader/files/{unzipped_file}',
                              output_dir='file_downloader/files/fragments/')
                # move fragments to s3 bucket
                fragment_list = os.listdir('file_downloader/files/fragments/')
                s3_url = f's3://iqblade-data-services-companieshouse-fragments/'
                [subprocess.run(f'aws s3 mv {os.path.abspath(f"file_downloader/files/fragments/{fragment}")} {s3_url}')
                 for fragment in fragment_list]

                # once processes
                cursor.execute("""insert into companies_house_filetracker (filename, section1) VALUES (%s, %s)""",
                               (file_to_dl, datetime.date.today()))
                db.commit()
            else:
                # ignore
                pass


@timer
def search_and_collect_ch_file(firstdateofmonth: datetime.date):
    """
    checks for presence of datestring on db and then page, downloads if new filestring, and then processes
    :param firstdateofmonth:
    :return: filename, firstdateofmonth
    """
    logger.info('search called')
    initial_req_url = 'http://download.companieshouse.gov.uk/en_output.html'
    r = requests.get(initial_req_url, verify=False)
    # filename = 'BasicCompanyDataAsOneFile-' + str(firstdateofmonth) + '.zip'
    r_content = r.content
    rsoup = bs4.BeautifulSoup(r_content, 'html.parser')
    links = rsoup.find_all('a')
    # check links on product page for the current date string from firstdateofmonth
    str_month = firstdateofmonth.strftime('%Y-%m')
    logger.info(str_month)
    filename = ''
    while filename == '':
        for link in links:
            logger.info(f'link in loop: {link}')
            if str_month in link.text:
                logger.info(f'new file found: {link.text}')
                logger.info(link)
                filename = link['href']
                logger.info(f'filename: {filename}')
                file_link = link['href']
                logger.info(f'filelink: {file_link}')
                filename = collect_companieshouse_file(file_link)
                break
        logger.info('filename is still none')
        time.sleep(4)
    return filename, firstdateofmonth


if __name__ == '__main__':
    print(os.environ)
    cursor, db = connect_preprod()
    file_check_regex(cursor, db)
