"""
1a. find latest file
1b. if file already found in table, close script
2. download file
3a. unzip and fragment file
3b. send .zip to aws sftp server
4. send fragments to rchis table
5. run updates involving rchis
"""
import os
import pathlib
import re
import subprocess
import zipfile

import bs4
import pandas as pd
import polars as pl
import boto3
import requests
from filesplit.split import Split

from fragment_work import parse_fragment_pl
from section_3_funcs import *

cursor, db = connect_preprod()

s3_client = boto3.client('s3',
                         aws_access_key_id=os.environ.get('aws_access_key_id_data_services'), # ...XDH4
                         aws_secret_access_key=os.environ.get('aws_secret_key_data_services'), # ...wtR
                         region_name='eu-west-1'
                         )

def fragment_file(file_name: str, output_dir: str = 'ch_fragments/') -> None:
    """
    divides a given file into the given output_dir str variable,
    specifically fragments into lines of 49,999 with a header row
    deletes the manifest file - assumes output_dir str has a / at the end
    :param output_dir:
    :param file_name:
    :return:
    """
    split = Split(file_name, output_dir)
    split.bylinecount(linecount=50000, includeheader=True)
    os.remove(f'{output_dir}manifest')


def custom_sort_key(file_path) -> int:
    """takes a csv fragment name and finds out what number fragment it is, the fragments will then be ordered by
    this value in order to find the final fragment which is assumed to not have 49,999 rows.
    """
    zipped_file = file_path.name
    number = int(zipped_file.split('_')[-1].replace('.csv', ''))
    logger.info(number)
    return number


def unzip_ch_file_s3_send(file_name, s3_url=os.environ.get('aws-tdsynnex-sftp-bucket-url')) -> str:
    """
    unzips a given zipped_file into the output directory specified - and then sends zip file to s3 bucket
    :param s3_url:
    :param file_name:
    :return: file_name.replace('.zip', '.csv')
    """
    filepath = f'ch_files/latest_ch_file/{file_name}'
    output_directory = 'ch_files/latest_ch_file_unzipped'
    with zipfile.ZipFile(filepath, 'r') as zip_ref:
        zip_ref.extractall(output_directory)
    subprocess.run(f'aws s3 mv {file_name} {s3_url} {file_name}')  # subprocess also removes file
    return file_name.replace('.zip', '.csv')


def collect_companieshouse_file(zipped_file: str) -> str:
    """
    downloads a file based on a given zipped_file
    :param zipped_file:
    :return:
    """
    logger.info(f'collect_companieshouse_file called, downloading {zipped_file}')
    baseurl = 'http://download.companieshouse.gov.uk/' + zipped_file
    logger.info(f'sending request using url: {baseurl}')
    req = requests.get(baseurl,
                       stream=True,
                       verify=False)
    req.encoding = 'utf-8'
    logger.info(zipped_file, ' to download')

    with open('file_downloader/files' + zipped_file, 'wb') as fd:
        logger.info('commencing downloads')
        chunkcount = 0
        for chunk in req.iter_content(chunk_size=100000):
            chunkcount += 1
            fd.write(chunk)
            if chunkcount % 100 == 0:
                logger.info(chunkcount)

    logger.info('collect_companies_house_file_complete')
    return zipped_file


# 1a. find latest file
def process_section_1() -> None:
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
    8. update iqblade.companies_house_filetracker with new zipped_file, and then that the section1 has been complete
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
            zipped_file = file_str_match[0]
            logger.info('regex returned a result')
            month_re = re.findall(string=zipped_file, pattern='[0-9]{4}-[0-9]{2}-[0-9]{2}')
            logger.info(f'monthcheck: {month_re[0]}')
            logger.info(f'file_str_match: {zipped_file}')

            # 3. compare the string to those found in mysql table iqblade.companies_house_filetracker
            cursor.execute("""select * from companies_house_filetracker where filename = %s and section1 is not null""",
                           (zipped_file,))
            result = cursor.fetchall()
            logger.info(result)

            # 4a. if there is a string that matches, we assume the file has been processed through section 1 and
            #    close the pipeline
            if len(result) == 0 and file_str_match not in os.listdir('ch_files'):
                # download file
                logger.info('file found to download')

                # 4b. if there is no match, we download the file
                logger.info(f'collect_companieshouse_file called, downloading {zipped_file}')
                baseurl = 'http://download.companieshouse.gov.uk/' + zipped_file
                logger.info(f'sending request using url: {baseurl}')
                req = requests.get(baseurl,
                                   stream=True,
                                   verify=False)
                req.encoding = 'utf-8'
                logger.info(f'{zipped_file} to download')
                with open('ch_files/' + zipped_file, 'wb') as fd:
                    chunkcount = 0
                    for chunk in req.iter_content(chunk_size=100000):
                        chunkcount += 1
                        fd.write(chunk)
                        if chunkcount % 100 == 0:
                            logger.info(chunkcount)
                logger.info('collect_companies_house_file_complete')

            # 5a. unzip file and send .zip to s3
            filepath = f'ch_files/{zipped_file}'
            output_directory = 'ch_files/'
            with zipfile.ZipFile(filepath, 'r') as zip_ref:
                zip_ref.extractall(output_directory)
                output_file = zip_ref.namelist()[0]

            # todo this should be replaced with a boto3 client

            # send original zip file to hgdata-incoming-files
            incoming_files_bucket = 'iqblade-data-services-companieshouse-incoming-files'
            response = s3_client.upload_file(Filename=zipped_file,
                                             Bucket=incoming_files_bucket,
                                             Key=zipped_file)
            # fragment file
            fragment_file(file_name=f'ch_files/{output_file}',
                          output_dir='ch_fragments/')

            # move fragments to s3 bucket
            list_of_fragments = os.listdir('ch_fragments/')

            # 7a. calculate number of rows in file, and then send the number to iqblade.companies_house_rowcounts
            path_objects = [pathlib.Path(f) for f in list_of_fragments]
            csv_files = list(filter(lambda path: path.suffix == '.csv', path_objects))



def process_section_2() -> None:
    """
    1. list fragments found in s3 bucket
    2a. for each fragment, call parse_fragment() function to upsert to iqblade.raw_companies_house_input_staging
    2b. for each fragment, call parse_fragment_sic() to contribute to pandas dataframe monthly_df
    2c. remove file from local and s3 bucket
    3. concat monthly_df into a single dataframe based on sic_code and use dataframe.to_sql() to send it to
        companies_house_sic_code_counts
    4. take companies_house_sic_code_counts data and add it into aggregates
    :return:
    """

    global file_date
    # monthly_df variable needed for sic_counts project, which isn't in use at the moment.
    # monthly_df = pd.DataFrame(columns=['sic_code', 'sic_code_count'])

    # 1. list fragments found in s3 bucket
    for file_fragment in os.listdir('ch_fragments'):
        # check if the file has already been processed in the past, if it has it will be in this table
        # if it is not in the table, then we can assume it has not been parsed and can continue to process it
        if file_fragment.endswith('.csv'):
            fragments_abspath = os.path.abspath('ch_fragments')
            fragment_file_path = f'{fragments_abspath}/{file_fragment}'
            logger.info('parsing {}'.format(file_fragment))
            fragment = 'ch_fragments/{}'.format(file_fragment)

            # 2a. for each fragment, call parse_fragment() function to upsert to iqblade.raw_companies_house_input_staging
            parse_fragment_pl(fragment_file_path, cursor=cursor, cursordb=db)

            # 2b. remove file from local and s3 bucket
            # os.remove(f'ch_fragments/{file_fragment}')
        else:
            pass


def process_section_3() -> None:
    """
    1. check that functions need to be run for latest file by checking iqblade.companies_house_filetracker
        for rows that have section2 complete but not section3

    mostly sql queries are
    :return:
    """

    # work in organisation table
    process_section3_organisation(cursor, db)

    # work in sic_codes
    process_section3_siccode(cursor, db)

    # work in geolocation
    process_section3_geolocation(cursor, db)


if __name__ == '__main__':
    cursor, db = connect_preprod()
    process_section_1()
    process_section_2()
    process_section_3()
