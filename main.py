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
import requests
from filesplit.split import Split

from fragment_work import parse_fragment_pl
from section_3_funcs import *

cursor, db = connect_preprod()


def fragment_file(file_name: str, output_dir: str = 'file_downloader/files/fragments/') -> None:
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
    file_to_download = file_path.name
    number = int(file_to_download.split('_')[-1].replace('.csv', ''))
    logger.info(number)
    return number


def unzip_ch_file_s3_send(file_name, s3_url=os.environ.get('tdsynnex-sftp-bucket-url')) -> str:
    """
    unzips a given file_to_download into the output directory specified - and then sends zip file to s3 bucket
    :param s3_url:
    :param file_name:
    :return: file_name.replace('.zip', '.csv')
    """
    filepath = f'file_downloader/files/latest_ch_file/{file_name}'
    output_directory = 'file_downloader/files/latest_ch_file_unzipped'
    with zipfile.ZipFile(filepath, 'r') as zip_ref:
        zip_ref.extractall(output_directory)
    subprocess.run(f'aws s3 mv {file_name} {s3_url} {file_name}')  # subprocess also removes file
    return file_name.replace('.zip', '.csv')


def collect_companieshouse_file(file_to_download: str) -> str:
    """
    downloads a file based on a given file_to_download
    :param file_to_download:
    :return:
    """
    logger.info(f'collect_companieshouse_file called, downloading {file_to_download}')
    baseurl = 'http://download.companieshouse.gov.uk/' + file_to_download
    logger.info(f'sending request using url: {baseurl}')
    req = requests.get(baseurl,
                       stream=True,
                       verify=False)
    req.encoding = 'utf-8'
    logger.info(file_to_download, ' to download')
    with open('file_downloader/files' + file_to_download, 'wb') as fd:
        logger.info('commencing downloads')
        chunkcount = 0
        for chunk in req.iter_content(chunk_size=100000):
            chunkcount += 1
            fd.write(chunk)
            if chunkcount % 100 == 0:
                logger.info(chunkcount)
    logger.info('collect_companies_house_file_complete')
    return file_to_download


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
    8. update iqblade.companies_house_filetracker with new file_to_download, and then that the section1 has been complete
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
                logger.info(f'collect_companieshouse_file called, downloading {file_to_download}')
                baseurl = 'http://download.companieshouse.gov.uk/' + file_to_download
                logger.info(f'sending request using url: {baseurl}')
                req = requests.get(baseurl,
                                   stream=True,
                                   verify=False)
                req.encoding = 'utf-8'
                logger.info(f'{file_to_download} to download')
                with open('file_downloader/files/' + file_to_download, 'wb') as fd:
                    chunkcount = 0
                    for chunk in req.iter_content(chunk_size=100000):
                        chunkcount += 1
                        fd.write(chunk)
                        if chunkcount % 100 == 0:
                            logger.info(chunkcount)
                logger.info('collect_companies_house_file_complete')

                # 5a. unzip file and send .zip to s3
                filepath = f'file_downloader/files/{file_to_download}'
                output_directory = 'file_downloader/files/'
                with zipfile.ZipFile(filepath, 'r') as zip_ref:
                    zip_ref.extractall(output_directory)
                subprocess.run(f'aws s3 mv {file_to_download} {os.environ.get("tdsynnex-sftp-bucket-url")} {file_to_download}')

                # fragment file
                fragment_file(file_name=f'file_downloader/files/{file_to_download.replace(".zip", ".csv")}',
                              output_dir='file_downloader/files/fragments/')

                # move fragments to s3 bucket
                list_of_fragments = os.listdir('file_downloader/files/fragments/')

                # 7a. calculate number of rows in file, and then send the number to iqblade.companies_house_rowcounts
                path_objects = [pathlib.Path(f) for f in list_of_fragments]
                csv_files = list(filter(lambda path: path.suffix == '.csv', path_objects))
                # sorted_csv_files = sorted(csv_files, key=custom_sort_key)
                # logger.info(sorted_csv_files)
                # count_of_full_fragments = len(csv_files) - 1
                # fragment_count = count_of_full_fragments * 49999
                #
                # # for the final file, we assume it has a rowcount <50,000.
                # # we read the csv and get it's length
                # final_file_df = pd.read_csv(f'file_downloader/files/fragments/{sorted_csv_files[-1]}')
                # fragment_count = fragment_count - len(final_file_df)
                # logger.info('fragment count: {}'.format(fragment_count))


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
    monthly_df = pd.DataFrame(columns=['sic_code', 'sic_code_count'])
    # 1. list fragments found in s3 bucket
    for file_fragment in os.listdir('file_downloader/files/fragments'):
        # check if the file has already been processed in the past, if it has it will be in this table
        # if it is not in the table, then we can assume it has not been parsed and can continue to process it
        if file_fragment.endswith('.csv'):
            fragments_abspath = os.path.abspath('file_downloader/files/fragments')
            fragment_file_path = f'{fragments_abspath}/{file_fragment}'
            logger.info('parsing {}'.format(file_fragment))
            fragment = 'file_downloader/files/fragments/{}'.format(file_fragment)

            # 2a. for each fragment, call parse_fragment() function to upsert to iqblade.raw_companies_house_input_staging
            parse_fragment_pl(fragment_file_path, cursor=cursor, cursordb=db)

            # 2b. for each fragment, call parse_fragment_sic() to contribute to pandas dataframe monthly_df
            # regex of the fragment to get the file_date value
            file_date = re.search(string=fragment, pattern='\d{4}-\d{2}-\d{2,3}')[0]
            # df_counts = parse_fragment_sic(fragment, file_date)
            # monthly_df = pd.concat([monthly_df, df_counts], axis=0)

            # 2c. remove file from local and s3 bucket
            os.remove(f'file_downloader/files/fragments/{file_fragment}')
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
    process_section_2() # todo currently in test, make sure writing to correct tables
    process_section_3() # todo currently in test, make sure writing to correct tables
