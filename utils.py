import datetime
import json
import logging
import os
import subprocess
import sys
import time
import traceback
import zipfile

import requests
from filesplit.split import Split

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


def pipeline_messenger(title, text, hexcolour_value):
    messenger_colours = {
        'pass': '#00c400',
        'fail': '#c40000',
        'notification': '#0000c4'
    }
    url = "https://tdworldwide.webhook.office.com/webhookb2/d5d1f4d1-2858-48a6-8156-5abf78a31f9b@7fe14ab6-8f5d-4139-84bf-cd8aed0ee6b9/IncomingWebhook/76b5bd9cd81946338da47e0349ba909d/c5995f3f-7ce7-4f13-8dba-0b4a7fc2c546"
    payload = json.dumps({
        "@type": "MessageCard",
        "themeColor": messenger_colours[hexcolour_value],
        "title": title,
        "text": text,
        "markdown": True
    })
    headers = {
        'Content-Type': 'application/json'
    }
    requests.request("POST", url, headers=headers, data=payload)


def handle_exception(exc_type, exc_info, tb):
    import traceback
    length = mycode_traceback_levels(tb)
    logger.info(''.join(traceback.format_exception(exc_type, exc_info, tb, length)))


def is_mycode(tb):
    # returns True if the top frame is part of my code.
    test_globals = tb.tb_frame.f_globals
    return '__mycode' in test_globals


def mycode_traceback_levels(tb):
    # counts how many frames are part of my code.
    length = 0
    while tb and is_mycode(tb):
        length += 1
        tb = tb.tb_next
    return length


__mycode = True
sys.excepthook = handle_exception


def pipeline_message_wrap(func):
    def pipeline_message_wrapper(*args, **kwargs):
        # define the azure variables here in a try/except, if it fails then we assume it has been run locally
        try:
            azure_pipeline_name = os.environ.get('BUILD_DEFINITIONNAME')
        except Exception:
            azure_pipeline_name = 'localhost'
        try:
            __mycode = False
            function_name = func.__name__
            script_name = os.path.basename(__file__)
            logger.info('starting func')
            start_time = time.time()
            result = func(*args, **kwargs)
            print(func)
            logger.info('sending message')
            end_time = time.time()
            execution_time = end_time - start_time
            logger.info(f"Function: '{function_name}' in script '{script_name}' of pipeline '{azure_pipeline_name}' took {execution_time} seconds")
            pipeline_messenger(title=f'{function_name} in {script_name} of project {azure_pipeline_name} has passed!',
                               text=str(f'process took {execution_time} seconds'),
                               hexcolour_value='pass')
            print('this is a test')
        except Exception:
            result = None
            pipeline_messenger(title=f'{func.__name__} in {__file__} has failed', text=str(traceback.format_exc()),
                               hexcolour_value='fail')
        return result

    return pipeline_message_wrapper


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
