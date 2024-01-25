import json
import logging
import os
import subprocess
import time
import traceback
import zipfile
import datetime
import boto3
import re

import mysql.connector
import requests
from filesplit.split import Split

logger = logging.getLogger()
logging.basicConfig(level=logging.INFO,
                    format='%(filename)s line:%(lineno)d %(message)s')

constring = "mysql//{}:{}@{}:3306/{}".format(
    os.environ.get('ADMINUSER'),
    os.environ.get('ADMINPASS'),
    os.environ.get('HOST'),
    os.environ.get('DATABASE')
)


def timer(func):
    def wrapper(*args, **kwargs):
        t1 = time.time()
        var = func(*args, **kwargs)
        t2 = time.time() - t1
        logger.info(f'function {func.__name__} took {t2} seconds')
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


# __mycode = True
# sys.excepthook = handle_exception


def pipeline_message_wrap(func):
    def pipeline_message_wrapper(*args, **kwargs):
        # define the azure variables here in a try/except, if it fails then we assume it has been run locally
        try:
            azure_pipeline_name = os.environ.get('BUILD_DEFINITIONNAME')
        except Exception:
            azure_pipeline_name = 'localhost'
        function_name = func.__name__
        script_name = os.path.basename(__file__)
        try:
            __mycode = False

            logger.info('starting func')
            start_time = time.time()
            result = func(*args, **kwargs)
            print(func)
            logger.info('sending message')
            end_time = time.time()
            execution_time = end_time - start_time
            logger.info(
                f"Function: '{function_name}' in script '{script_name}' of pipeline '{azure_pipeline_name}' took {execution_time} seconds")
            pipeline_messenger(title=f'{azure_pipeline_name}-{func.__name__}-{__file__} has passed!',
                               text=str(f'process took {execution_time} seconds'),
                               hexcolour_value='pass')
            print('this is a test')
        except Exception:
            result = None
            pipeline_messenger(
                title=f'{azure_pipeline_name}-{func.__name__}-{__file__} has failed',
                text=str(traceback.format_exc()),
                hexcolour_value='fail')
        return result

    return pipeline_message_wrapper


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


@timer
def unzip_ch_file(file_name, output_directory='file_downloader/files'):
    """
    unzips a given filename into the output directory specified, else it is file_downloader/files
    :param output_directory:
    :param file_name:
    :return: file_name.replace('.zip', '.csv')
    """
    filepath = f'file_downloader/files/{file_name}'
    with zipfile.ZipFile(filepath, 'r') as zip_ref:
        zip_ref.extractall(output_directory)
    return file_name.replace('.zip', '.csv')


@timer
def fragment_file(file_name: str, output_dir: str = 'file_downloader/files/fragments/'):
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


@timer
def run_query(sql, cursor, db):
    """takes a sql query with no %s args, runs it, commits it"""
    cursor.execute(sql)
    db.commit()


@timer
def connect_preprod():
    db = mysql.connector.connect(
        host=os.environ.get('PREPRODHOST'),
        user=os.environ.get('ADMINUSER'),
        passwd=os.environ.get('ADMINPASS'),
        database=os.environ.get('DATABASE'),
    )

    cursor = db.cursor()
    return cursor, db


@timer
def connect_preprod_readonly():
    db = mysql.connector.connect(
        host=os.environ.get('PREPRODHOST_READONLY'),
        user=os.environ.get('ADMINUSER'),
        passwd=os.environ.get('ADMINPASS'),
        database=os.environ.get('DATABASE'),
    )

    cursor = db.cursor()
    return cursor


# strip values of anything but numbers
def remove_non_numeric(text):
    pattern = r"\D+"
    return re.sub(pattern, "", text)


def find_previous_month(month, year):
    """get the previous month of a given date,
     if the month is janiary then the year needs to be changed as well"""

    if month == 1:
        previous_month = 12
        previous_year = str(int(year) - 1)
    else:
        previous_month = str(int(month) - 1)
        previous_year = year

    return previous_month, previous_year


def get_row_count_of_s3_csv(bucket_name, path) -> int:
    """
    provide a count of rows in a specified companies house file in the s3 bucket.
    :param bucket_name:
    :param path:
    :return:
    """
    sql_stmt = """SELECT count(*) FROM s3object """
    req = boto3.client('s3').select_object_content(
        Bucket=bucket_name,
        Key=path['Key'],
        ExpressionType="SQL",
        Expression=sql_stmt,
        InputSerialization={"CSV": {"FileHeaderInfo": "Use", "AllowQuotedRecordDelimiter": True}},
        OutputSerialization={"CSV": {}},
    )

    row_count = next(int(x["Records"]["Payload"]) for x in req["Payload"])
    return row_count


def get_rowcount_s3(s3_client, bucket_name: str = '') -> int:
    """
    use get_row_count_of_s3_csv on the files in the fragments bucket to get a total rowcount in the companies house file
    :return:
    """
    list_of_s3_objects = s3_client.list_objects_v2(Bucket=bucket_name, Prefix="", Delimiter="/")

    rowcount = 0
    for s3_object in list_of_s3_objects['Contents']:
        rows = get_row_count_of_s3_csv(bucket_name=bucket_name, path=s3_object)
        rowcount += rows
        print(rowcount)
    return rowcount

if __name__ == '__main__':
    cursor, db = connect_preprod()
