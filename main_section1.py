import datetime
import os
import pathlib
import re
import subprocess
import time

import boto3
import bs4
import pandas as pd
import requests
import requests as r

from utils import timer, unzip_ch_file_s3_send, fragment_file,\
    pipeline_message_wrap, logger, connect_preprod, get_rowcount_s3

s3_client = boto3.client('s3',
                         aws_access_key_id=os.environ.get('HGDATA-S3-AWS-ACCESS-KEY-ID'),
                         aws_secret_access_key=os.environ.get('HGDATA-S3-AWS-SECRET-KEY'),
                         region_name='eu-west-1'
                         )
def custom_sort_key(file_path):
    """takes a csv fragment name and finds out what number fragment it is, the fragments will then be ordered by
    this value in order to find the final fragment which is assumed to not have 49,999 rows.
    """
    filename = file_path.name
    number = int(filename.split('_')[-1].replace('.csv', ''))
    print(number)
    return number


@timer
def collect_companieshouse_file(filename: str):
    """
    downloads a file based on a given filename
    :param filename:
    :return:
    """
    logger.info(f'collect_companieshouse_file called, downloading {filename}')
    baseurl = 'http://download.companieshouse.gov.uk/' + filename
    logger.info(f'sending request using url: {baseurl}')
    req = r.get(baseurl, stream=True, verify=False)
    with open('file_downloader/files/' + filename, 'wb') as fd:
        chunkcount = 0
        for chunk in req.iter_content(chunk_size=100000):
            chunkcount += 1
            fd.write(chunk)
            logger.info(chunkcount)
    logger.info('collect_companies_house_file_complete')
    return filename


@timer
def send_to_s3(filename,
               s3_url=os.environ.get('S3_COMPANIES_HOUSE_FRAGMENTS_URL')):
    """sends a file to a given s3 bucket, otherwise default s3 bucket"""
    s3_client = boto3.client('s3',
                             aws_access_key_id=os.environ.get('HGDATA-S3-AWS-ACCESS-KEY-ID'),
                             aws_secret_access_key=os.environ.get('HGDATA-S3-AWS-SECRET-KEY'),
                             region_name='eu-west-1'
                             )
    s3_client.upload_file(filename, s3_url, filename)


# @pipeline_message_wrap
@timer
def file_check_regex(cursor, db):
    """use regex to find the specified companies house file, and then check if that filename
    exists in iqblade.companies_house_filetracker.
    if the file does not exist in the table, the file is downloaded.
    """
    chfile_regex_pattern = 'BasicCompanyDataAsOneFile-[0-9]{4}-[0-9]{2}-[0-9]{2}\.zip'
    logger.info('search called')
    initial_req_url = 'http://download.companieshouse.gov.uk/en_output.html'
    r = requests.get(initial_req_url, verify=False)
    r_content = r.content
    request_content_soup = bs4.BeautifulSoup(r_content, 'html.parser')
    links = request_content_soup.find_all('a')
    logger.info(links)
    for i in links:
        # finds all the links on the companies house page that match the regex
        # all the files that appear on the webpage will be of the same monthly file, but they have
        # that file as a whole product, and then divided into smaller sections.
        file_str_match = re.findall(string=i['href'], pattern=chfile_regex_pattern)
        if len(file_str_match) != 0:
            print('regex returned a result')
            month_re = re.findall(string=file_str_match[0], pattern='[0-9]{4}-[0-9]{2}-[0-9]{2}')
            logger.info(f'monthcheck: {month_re[0]}')
            logger.info(f'file_str_match: {file_str_match[0]}')

            # check if the regex matched filestring already exists in companies house
            cursor.execute("""select * from companies_house_filetracker where filename = %s and section1 is not null""",
                           (file_str_match[0],))
            result = cursor.fetchall()
            logger.info(result)

            # if length of cursor.fetchall() is zero, we assume the file hasn't been downloaded
            if len(result) == 0:
                # download file
                logger.info('file found to download')
                file_to_download = file_str_match[0]
                collect_companieshouse_file(filename=file_to_download)

                # unzip file and send .zip to s3
                unzipped_file = unzip_ch_file_s3_send(f'{file_to_download}')

                # fragment file
                fragment_file(file_name=f'file_downloader/files/{unzipped_file}',
                              output_dir='file_downloader/files/fragments/')

                # move fragments to s3 bucket
                list_of_fragments = os.listdir('file_downloader/files/fragments/')

                # calculate number of rows in file, and then
                path_objects = [pathlib.Path(f) for f in list_of_fragments]
                csv_files = list(filter(lambda path: path.suffix == '.csv', path_objects))
                sorted_csv_files = sorted(csv_files, key=custom_sort_key)

                # the majority of these fragments will be 49,999 rows long. There will be 1 fragment file that is not.
                # there is also a fragments.txt file that needs to be ignored.
                count_of_full_fragments = len(csv_files) - 1
                fragment_count = count_of_full_fragments * 49999

                # for the final file, we assume it has a rowcount <50,000.
                # we read the csv and get it's length
                final_file_df = pd.read_csv(f'file_downloader/files/fragments/{sorted_csv_files[-1]}')
                fragment_count = fragment_count - len(final_file_df)
                print(fragment_count, 'fragment count')

                # insert rowcount into companies_house_rowcounts
                cursor.execute("""insert into companies_house_rowcounts (filename, file_rowcount)
                 VALUES (%s, %s)""", (file_to_download.replace('.zip', ''), fragment_count))
                db.commit()

                # send fragment files
                s3_url = os.environ.get('S3_COMPANIES_HOUSE_FRAGMENTS_URL')
                logger.info(s3_url)
                logger.info('moving fragments')
                [subprocess.run(f'aws s3 mv {os.path.abspath(f"file_downloader/files/fragments/{fragment}")} {s3_url}')
                 for fragment in list_of_fragments]

                # once all the fragments are moved to s3, perform another count using the boto3 counter
                s3_rowcount = get_rowcount_s3(s3_client=s3_client)
                cursor.execute("""
                update companies_house_rowcounts
                 set bucket_rowcount = %s
                 where filename = %s and bucket_rowcount is null
                 """, (s3_rowcount, file_to_download.replace('.zip', '')))

                # once processed, insert the filetrackers date and the current data
                cursor.execute("""insert into companies_house_filetracker (filename, section1) VALUES (%s, %s)""",
                               (file_to_download, datetime.date.today()))
                db.commit()
            else:
                # ignore
                pass
    logger.info('function complete')


@timer
def search_and_collect_ch_file(firstdateofmonth: datetime.date):
    """
    checks for presence of datestring on db and then page, downloads if the filestring does not exist in the table
    :param firstdateofmonth:
    :return: filename, firstdateofmonth
    """
    logger.info('search_and_collect_ch_file called')
    initial_req_url = 'http://download.companieshouse.gov.uk/en_output.html'
    request = requests.get(initial_req_url, verify=False)
    request_content = request.content
    request_content_soup = bs4.BeautifulSoup(request_content, 'html.parser')
    links = request_content_soup.find_all('a')

    # check links on product page for the current date string from firstdateofmonth
    year_month = firstdateofmonth.strftime('%Y-%m')
    logger.info(year_month)
    filename = ''
    while filename == '':
        for link in links:
            logger.info(f'link in loop: {link}')
            if year_month in link.text:
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
    # return filename, firstdateofmonth


if __name__ == '__main__':
    logger.info(os.environ)
    cursor, db = connect_preprod()
    file_check_regex(cursor, db)
