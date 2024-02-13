import datetime
import os
import pathlib
import re
import subprocess

import boto3
import bs4
import pandas as pd
import requests
import requests as r
import zipfile

from utils import timer, fragment_file, \
    logger, connect_preprod, get_rowcount_s3

s3_client = boto3.client('s3',
                         aws_access_key_id=os.environ.get('HGDATA-S3-AWS-ACCESS-KEY-ID'),
                         aws_secret_access_key=os.environ.get('HGDATA-S3-AWS-SECRET-KEY'),
                         region_name='eu-west-1'
                         )


@timer
def unzip_ch_file_s3_send(file_name, s3_url=os.environ.get('S3_TDSYNNEX_SFTP_BUCKET_URL')):
    """
    unzips a given filename into the output directory specified - and then sends zip file to s3 bucket
    :param s3_url:
    :param file_name:
    :return: file_name.replace('.zip', '.csv')
    """
    filepath = f'file_downloader/files/{file_name}'
    output_directory = 'file_downloader/files'
    with zipfile.ZipFile(filepath, 'r') as zip_ref:
        zip_ref.extractall(output_directory)
    subprocess.run(f'aws s3 mv {file_name} {s3_url} {file_name}')  # subprocess also removes file
    return file_name.replace('.zip', '.csv')


def custom_sort_key(file_path):
    """takes a csv fragment name and finds out what number fragment it is, the fragments will then be ordered by
    this value in order to find the final fragment which is assumed to not have 49,999 rows.
    """
    filename = file_path.name
    number = int(filename.split('_')[-1].replace('.csv', ''))
    logger.info(number)
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
            if chunkcount % 100 == 0:
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
def process_section_1(cursor, db):
    """
    1. Send a request to companies house downloads
    2. collect all links and search them for specified string (chfile_regex_pattern)
    3. compare the string to those found in mysql table iqblade.companies_house_filetracker
    4a. if there is a string that matches, we assume the file has been processed through section 1 and
        close the pipeline
    4b. if there is no match, we download the file
    5a. unzip the file, and send the .zip file to an incoming-file
    5b. copy the file to the td-synnex bucket
    6. split the file into 'fragment' files of 50,000 lines each
    7a. calculate how many rows are found in the files and send that record to iqblade.companies_house_rowcounts
    7b. send the each fragment file to AWS s3
    7c. calculate how many rows in the fragment files in s3
    8. update iqblade.companies_house_filetracker with new filename, and then that the section1 has been complete
    """

    # 1. Send a Request to companies house downloads
    logger.info('search called')
    initial_req_url = 'http://download.companieshouse.gov.uk/en_output.html'
    r = requests.get(initial_req_url, verify=False)
    r_content = r.content
    request_content_soup = bs4.BeautifulSoup(r_content, 'html.parser')

    # 2. collect all links and search them for specified string (chfile_regex_pattenr)
    links = request_content_soup.find_all('a')
    logger.info(links)
    chfile_regex_pattern = 'BasicCompanyDataAsOneFile-[0-9]{4}-[0-9]{2}-[0-9]{2}\.zip'
    for i in links:
        file_str_match = re.findall(string=i['href'], pattern=chfile_regex_pattern)
        if len(file_str_match) != 0:
            logger.info('regex returned a result')
            month_re = re.findall(string=file_str_match[0], pattern='[0-9]{4}-[0-9]{2}-[0-9]{2}')
            logger.info(f'monthcheck: {month_re[0]}')
            logger.info(f'file_str_match: {file_str_match[0]}')

            # 3. compare the string to those found in mysql table iqblade.companies_house_filetracker
            cursor.execute("""select * from companies_house_filetracker where filename = %s and section1 is not null""",
                           (file_str_match[0],))
            result = cursor.fetchall()
            logger.info(result)

            # 4a. if there is a string that matches, we assume the file has been processed through section 1 and
            #    close the pipeline
            if len(result) == 0:
                # download file
                logger.info('file found to download')
                file_to_download = file_str_match[0]
                # 4b. if there is no match, we download the file
                collect_companieshouse_file(filename=file_to_download)

                # 5a. unzip file and send .zip to s3
                unzipped_file = unzip_ch_file_s3_send(f'{file_to_download}')

                # fragment file
                fragment_file(file_name=f'file_downloader/files/{unzipped_file}',
                              output_dir='file_downloader/files/fragments/')

                # move fragments to s3 bucket
                list_of_fragments = os.listdir('file_downloader/files/fragments/')

                # 7a. calculate number of rows in file, and then send the number to iqblade.companies_house_rowcounts
                path_objects = [pathlib.Path(f) for f in list_of_fragments]
                csv_files = list(filter(lambda path: path.suffix == '.csv', path_objects))
                sorted_csv_files = sorted(csv_files, key=custom_sort_key)
                count_of_full_fragments = len(csv_files) - 1
                fragment_count = count_of_full_fragments * 49999

                # for the final file, we assume it has a rowcount <50,000.
                # we read the csv and get it's length
                final_file_df = pd.read_csv(f'file_downloader/files/fragments/{sorted_csv_files[-1]}')
                fragment_count = fragment_count - len(final_file_df)
                logger.info('fragment count: {}'.format(fragment_count))

                # 7b. send the each fragment file to AWS s3
                s3_url = os.environ.get('S3_COMPANIES_HOUSE_FRAGMENTS_URL')
                logger.info(s3_url)
                logger.info('moving fragments')
                [subprocess.run(f'aws s3 mv {os.path.abspath(f"file_downloader/files/fragments/{fragment}")} {s3_url}')
                 for fragment in list_of_fragments]

                # 7c. calculate how many rows in the fragment files in s3
                s3_rowcount = get_rowcount_s3(s3_client=s3_client,
                                              bucket_name='iqblade-data-services-companieshouse-fragments')

                # insert rowcount into companies_house_rowcounts
                cursor.execute("""insert into companies_house_rowcounts (filename, file_rowcount, bucket_rowcount)
                 VALUES (%s, %s, %s)""", (file_to_download.replace('.zip', ''), fragment_count, s3_rowcount))
                db.commit()

                # 8. update iqblade.companies_house_filetracker with new filename, and then that the section1 has been complete
                cursor.execute("""insert into companies_house_filetracker (filename, section1) VALUES (%s, %s)""",
                               (file_to_download, datetime.date.today()))
                db.commit()
            else:
                pass
    logger.info('function complete')


if __name__ == '__main__':
    logger.info(os.environ)
    cursor, db = connect_preprod()
    process_section_1(cursor, db)
