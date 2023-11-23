import datetime
import json
import os
import subprocess
import zipfile
import logging
import re
import traceback

import requests
from filesplit.split import Split
import time

logger = logging.getLogger()
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s [line:%(lineno)d] %(levelname)s: %(message)s')


def timer(func):
    def wrapper(*args, **kwargs):
        t1 = time.time()
        var = func(*args, **kwargs)
        t2 = time.time() - t1
        logger.info(f'{func.__name__} took {t2} seconds')
        return var

    return wrapper


def pipeline_messenger(title, text, hexcolour):
    url = "https://tdworldwide.webhook.office.com/webhookb2/d5d1f4d1-2858-48a6-8156-5abf78a31f9b@7fe14ab6-8f5d-4139-84bf-cd8aed0ee6b9/IncomingWebhook/76b5bd9cd81946338da47e0349ba909d/c5995f3f-7ce7-4f13-8dba-0b4a7fc2c546"
    payload = json.dumps({
        "@type": "MessageCard",
        "themeColor": hexcolour,
        "title": title,
        "text": text,
        "markdown": True
    })
    headers = {
        'Content-Type': 'application/json'
    }
    requests.request("POST", url, headers=headers, data=payload)


def pipeline_message_wrap(func):
    def wrapper(*args, **kwargs):
        try:
            var = func(*args, **kwargs)
            pipeline_messenger(title=f'{func.__name__} has passed', text=str(var), hexcolour='#00c400')
        except Exception as e:
            pipeline_messenger(title=f'{func.__name__} has failed on {str(e)}', text=str(traceback.format_exc()), hexcolour='#c40000')

    return wrapper()


@timer
def date_check(file_date: datetime.date, cursor):
    """
    checks for presence of specified month's file in filetracker
    :param file_date:
    :param cursor:
    :return:
    """
    cursor.execute("select * from BasicCompanyData_filetracker where MONTH(ch_upload_date) = MONTH(%s)", (file_date,))
    res = cursor.fetchall()
    if len(res) > 0:
        logger.info('exists')
        return True
    else:
        logger.info('does not exist')
        return False


@timer
def date_check_sic(file_date: datetime.date, cursor):
    """
    returns a boolean depending on whether or not the specified monthly data file is already present in the
    sic code filetracker
    :param file_date:
    :param cursor:
    :return:
    """
    cursor.execute("select * from BasicCompanyData_filetracker_siccode_analysis where ch_upload_date = %s",
                   (file_date,))
    res = cursor.fetchall()
    if len(res) > 0:
        logger.info('exists')
        return True
    else:
        logger.info('does not exist')
        return False


@timer
def unzip_ch_file_s3_send(file_name, s3_url='s3://iqblade-data-services-tdsynnex-sftp/home/tdsynnex/'):
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
    subprocess.run(f'aws s3 mv {file_name} {s3_url} {file_name}')
    # os.remove(f'file_downloader/files/{file_name}')
    return file_name.replace('.zip', '.csv')


@timer
def fragment_file(file_name: str, output_dir: str):
    """
    divides a given file into the given output_dir str variable,
    specifically fragments into lines of 49,999 with a header row
    deletes the manifest file
    :param output_dir:
    :param file_name:
    :return:
    """
    split = Split(file_name, output_dir)
    split.bylinecount(linecount=50000, includeheader=True)
    os.remove(f'{output_dir}manifest')
