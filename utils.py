import json
import logging
import os
import re
import subprocess
import time
import traceback
import zipfile

import boto3
import mysql.connector
import requests
from filesplit.split import Split
from rich.logging import RichHandler

logger = logging.getLogger()
logging.basicConfig(level=logging.INFO,
                    format='%(filename)s line:%(lineno)d %(message)s')

constring = "mysql://{}:{}@{}:3306/{}".format(
    os.environ.get('preprod-admin-user'),
    os.environ.get('preprod-admin-pass'),
    os.environ.get('preprod-host'),
    os.environ.get('preprod-database')
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


def pipeline_message_section_1(time_taken: int, size_of_file: int, file_name: str) -> None:
    """
    sends message to teams to confirm section 1 parsing has been complete
    :param time_taken:
    :param size_of_file:
    :param file_name:
    :return:
    """
    title = 'Companies House Section 1 Complete'
    text = f'File downloaded: {file_name}\nSize of file: {size_of_file}\nTime taken: {time_taken}'
    hexcolour_value = 'pass'
    pipeline_messenger(title=title, text=text, hexcolour_value=hexcolour_value)

def pipeline_message_section_2(time_taken: float, size_of_file: str, file_name: str) -> None:
    """
    sends message to teams to confirm section 2 parsing has been complete
    :param time_taken:
    :param size_of_file:
    :param file_name:
    :return:
    """
    title = 'Companies House Section 2 Complete'
    text = f'File downloaded: {file_name}\n Size of file: {size_of_file}\n Time taken: {time_taken}'
    hexcolour_value = 'pass'
    pipeline_messenger(title=title, text=text, hexcolour_value=hexcolour_value)
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

def ch_file_s3_send(file_name, s3_url=os.environ.get('S3_TDSYNNEX_SFTP_BUCKET_URL')):
    subprocess.run(f'aws s3 mv {file_name} {s3_url} {file_name}')


@timer
def unzip_ch_file(file_name, output_directory='file_downloader/files') -> str:
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
def connect_preprod() -> tuple:
    """
    connect to preprod database
    :return:
    """
    db = mysql.connector.connect(
        host=os.environ.get('preprod-host'),
        user=os.environ.get('preprod-admin-user'),
        passwd=os.environ.get('preprod-admin-pass'),
        database=os.environ.get('preprod-database'),
    )

    cursor = db.cursor()
    return cursor, db


@timer
def connect_preprod_readonly() -> tuple:
    """
    connect to readonly db
    :return:
    """
    db = mysql.connector.connect(
        host=os.environ.get('preprod-readonly-host'),
        user=os.environ.get('preprod-admin-user'),
        passwd=os.environ.get('preprod-admin-pass'),
        database=os.environ.get('preprod-database'),
    )

    cursor = db.cursor()
    return cursor, db


# strip values of anything but numbers
def remove_non_numeric(text) -> str:
    """
    removes non-numeric characters from a string
    :param text:
    :return:
    """
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


companies_house_file_data_types = {
    'company_name': str,
    'company_number': str,
    'RegAddress_CareOf': str,
    'RegAddress_POBox': str,
    'reg_address_line1': str,
    'reg_address_line2': str,
    'reg_address_posttown': str,
    'reg_address_county': str,
    'RegAddress_Country': str,
    'reg_address_postcode': str,
    'CompanyCategory': str,
    'company_status': str,
    'country_of_origin': str,
    'DissolutionDate': str,
    'IncorporationDate': str,
    'Accounts_AccountRefDay': str,
    'Accounts_AccountRefMonth': str,
    'Accounts_NextDueDate': str,
    'Accounts_LastMadeUpDate': str,
    'Accounts_AccountCategory': str,
    'Returns_NextDueDate': str,
    'Returns_LastMadeUpDate': str,
    'Mortgages_NumMortCharges': str,
    'Mortgages_NumMortOutstanding': str,
    'Mortgages_NumMortPartSatisfied': str,
    'Mortgages_NumMortSatisfied': str,
    'sic_text_1': str,
    'sic_text_2': str,
    'SICCode_SicText_3': str,
    'SICCode_SicText_4': str,
    'LimitedPartnerships_NumGenPartners': str,
    'LimitedPartnerships_NumLimPartners': str,
    'URI': str,
    'PreviousName_1_CONDATE': str,
    'PreviousName_1_CompanyName': str,
    'PreviousName_2_CONDATE': str,
    'PreviousName_2_CompanyName': str,
    'PreviousName_3_CONDATE': str,
    'PreviousName_3_CompanyName': str,
    'PreviousName_4_CONDATE': str,
    'PreviousName_4_CompanyName': str,
    'PreviousName_5_CONDATE': str,
    'PreviousName_5_CompanyName': str,
    'PreviousName_6_CONDATE': str,
    'PreviousName_6_CompanyName': str,
    'PreviousName_7_CONDATE': str,
    'PreviousName_7_CompanyName': str,
    'PreviousName_8_CONDATE': str,
    'PreviousName_8_CompanyName': str,
    'PreviousName_9_CONDATE': str,
    'PreviousName_9_CompanyName': str,
    'PreviousName_10_CONDATE': str,
    'PreviousName_10_CompanyName': str,
    'ConfStmtNextDueDate': str,
    'ConfStmtLastMadeUpDate': str,
    'Date_Of_Insert': str,
    'SourceFile': str,
    'phone_number': str,
    'number_of_employees': str

}

# columns in SQL table
companies_house_file_table_columns = [
    'company_name',
    'company_number',
    'RegAddress_CareOf',
    'RegAddress_POBox',
    'reg_address_line1',
    'reg_address_line2',
    'reg_address_posttown',
    'reg_address_county',
    'RegAddress_Country',
    'reg_address_postcode',
    'CompanyCategory',
    'company_status',
    'country_of_origin',
    'DissolutionDate',
    'IncorporationDate',
    'Accounts_AccountRefDay',
    'Accounts_AccountRefMonth',
    'Accounts_NextDueDate',
    'Accounts_LastMadeUpDate',
    'Accounts_AccountCategory',
    'Returns_NextDueDate',
    'Returns_LastMadeUpDate',
    'Mortgages_NumMortCharges',
    'Mortgages_NumMortOutstanding',
    'Mortgages_NumMortPartSatisfied',
    'Mortgages_NumMortSatisfied',
    'sic_text_1',
    'sic_text_2',
    'SICCode_SicText_3',
    'SICCode_SicText_4',
    'LimitedPartnerships_NumGenPartners',
    'LimitedPartnerships_NumLimPartners',
    'URI',
    'PreviousName_1_CONDATE',
    'PreviousName_1_CompanyName',
    'PreviousName_2_CONDATE',
    'PreviousName_2_CompanyName',
    'PreviousName_3_CONDATE',
    'PreviousName_3_CompanyName',
    'PreviousName_4_CONDATE',
    'PreviousName_4_CompanyName',
    'PreviousName_5_CONDATE',
    'PreviousName_5_CompanyName',
    'PreviousName_6_CONDATE',
    'PreviousName_6_CompanyName',
    'PreviousName_7_CONDATE',
    'PreviousName_7_CompanyName',
    'PreviousName_8_CONDATE',
    'PreviousName_8_CompanyName',
    'PreviousName_9_CONDATE',
    'PreviousName_9_CompanyName',
    'PreviousName_10_CONDATE',
    'PreviousName_10_CompanyName',
    'ConfStmtNextDueDate',
    'ConfStmtLastMadeUpDate',
    'Date_Of_Insert',
    'SourceFile',
    'phone_number',
    'number_of_employees',
    'organisation_id']

# columns in csv
companies_house_file_csv_columns = [
    'CompanyName'
    , ' CompanyNumber'
    , 'RegAddress.CareOf'
    , 'RegAddress.POBox'
    , 'RegAddress.AddressLine1'
    , ' RegAddress.AddressLine2'
    , 'RegAddress.PostTown'
    , 'RegAddress.County'
    , 'RegAddress.Country'
    , 'RegAddress.PostCode'
    , 'CompanyCategory'
    , 'CompanyStatus'
    , 'CountryOfOrigin'
    , 'DissolutionDate'
    , 'IncorporationDate'
    , 'Accounts.AccountRefMonth'
    , 'Accounts.AccountRefDay'
    , 'Accounts.NextDueDate'
    , 'Accounts.LastMadeUpDate'
    , 'Accounts.AccountCategory'
    , 'Returns.NextDueDate'
    , 'Returns.LastMadeUpDate'
    , 'Mortgages.NumMortCharges'
    , 'Mortgages.NumMortOutstanding'
    , 'Mortgages.NumMortPartSatisfied'
    , 'Mortgages.NumMortSatisfied'
    , 'SICCode.SicText_1'
    , 'SICCode.SicText_2'
    , 'SICCode.SicText_3'
    , 'SICCode.SicText_4'
    , 'LimitedPartnerships.NumGenPartners'
    , 'LimitedPartnerships.NumLimPartners'
    , 'URI'
    , 'PreviousName_1.CONDATE'
    , ' PreviousName_1.CompanyName'
    , ' PreviousName_2.CONDATE'
    , ' PreviousName_2.CompanyName'
    , 'PreviousName_3.CONDATE'
    , ' PreviousName_3.CompanyName'
    , 'PreviousName_4.CONDATE'
    , ' PreviousName_4.CompanyName'
    , 'PreviousName_5.CONDATE'
    , ' PreviousName_5.CompanyName'
    , 'PreviousName_6.CONDATE'
    , ' PreviousName_6.CompanyName'
    , 'PreviousName_7.CONDATE'
    , ' PreviousName_7.CompanyName'
    , 'PreviousName_8.CONDATE'
    , ' PreviousName_8.CompanyName'
    , 'PreviousName_9.CONDATE'
    , ' PreviousName_9.CompanyName'
    , 'PreviousName_10.CONDATE'
    , ' PreviousName_10.CompanyName'
    , 'ConfStmtNextDueDate'
    , ' ConfStmtLastMadeUpDate'
]

# columns in csv_new
companies_house_file_csv_columns_new = ['company_name',
                                    'company_number',
                                    'RegAddress_CareOf',
                                    'RegAddress_POBox',
                                    'reg_address_line1',
                                    'reg_address_line2',
                                    'reg_address_posttown',
                                    'reg_address_county',
                                    'RegAddress_Country',
                                    'reg_address_postcode',
                                    'CompanyCategory',
                                    'company_status',
                                    'country_of_origin',
                                    'DissolutionDate',
                                    'IncorporationDate',
                                    'Accounts_AccountRefMonth',
                                    'Accounts_AccountRefDay',
                                    'Accounts_NextDueDate',
                                    'Accounts_LastMadeUpDate',
                                    'Accounts_AccountCategory',
                                    'Returns_NextDueDate',
                                    'Returns_LastMadeUpDate',
                                    'Mortgages_NumMortCharges',
                                    'Mortgages_NumMortOutstanding',
                                    'Mortgages_NumMortPartSatisfied',
                                    'Mortgages_NumMortSatisfied',
                                    'sic_text_1',
                                    'sic_text_2',
                                    'SICCode_SicText_3',
                                    'SICCode_SicText_4',
                                    'LimitedPartnerships_NumGenPartners',
                                    'LimitedPartnerships_NumLimPartners',
                                    'URI',
                                    'PreviousName_1_CONDATE',
                                    'PreviousName_1_CompanyName',
                                    'PreviousName_2_CONDATE',
                                    'PreviousName_2_CompanyName',
                                    'PreviousName_3_CONDATE',
                                    'PreviousName_3_CompanyName',
                                    'PreviousName_4_CONDATE',
                                    'PreviousName_4_CompanyName',
                                    'PreviousName_5_CONDATE',
                                    'PreviousName_5_CompanyName',
                                    'PreviousName_6_CONDATE',
                                    'PreviousName_6_CompanyName',
                                    'PreviousName_7_CONDATE',
                                    'PreviousName_7_CompanyName',
                                    'PreviousName_8_CONDATE',
                                    'PreviousName_8_CompanyName',
                                    'PreviousName_9_CONDATE',
                                    'PreviousName_9_CompanyName',
                                    'PreviousName_10_CONDATE',
                                    'PreviousName_10_CompanyName',
                                    'ConfStmtNextDueDate',
                                    'ConfStmtLastMadeUpDate'
                                    ]



# columns in SQL table for sic_loading
sic_code_table_columns = [
    'CompanyNumber',
    'SicText_1',
    'SicText_2',
    'SicText_3',
    'SicText_4'
]

# columns in csv for sic loading
sic_code_csv_columns = [
    ' CompanyNumber'
    , 'SICCode.SicText_1'
    , 'SICCode.SicText_2'
    , 'SICCode.SicText_3'
    , 'SICCode.SicText_4'
]


sic_code_conversion_dict = {sic_code_csv_columns[i]: sic_code_table_columns[i] for i in
                       range(len(sic_code_csv_columns))}


companies_house_conversion_dict = {companies_house_file_csv_columns[i]: companies_house_file_csv_columns_new[i]
                                   for i in
                                   range(len(companies_house_file_csv_columns))}

def create_s3_connection() -> boto3.client:
    s3client = boto3.client('s3',
                            aws_access_key_id=os.environ.get('aws-access-key-id'),
                            aws_secret_access_key=os.environ.get('aws-secret-key'),
                            region_name='eu-west-1'
                            )
    buckets = s3client.list_buckets()
    logger.info(f'buckets: {buckets}')
    return s3client


def download_file(client: boto3, filename: str, target_bucket: str, local_folder: str='') -> None:
    """
    download a filename from a target_bucket into a local folder+filename
    :param local_folder:
    :param client:
    :param filename:
    :param target_bucket:
    :return:
    """
    logger.info('downloading {} from bucket {}, the file is targeted locally as {}/{}'.format(filename,
                                                                                              target_bucket,
                                                                                              local_folder,
                                                                                              filename))
    t0 = time.time()
    destination_folder = local_folder + '/' + filename
    client.download_file(Filename=destination_folder, Bucket=target_bucket, Key=filename)
    t1 = time.time()
    logger.info(f'download took {round(t1 - t0)} seconds')


def upload_file(client: boto3.client, filename: str, target_bucket: str) -> None:
    """
    send a file to s3 bucket
    :param target_bucket:
    :param client:
    :param filename:
    :return:
    """

    # remove folders to provide just filename when uploading
    target_file_name = re.search(r".*/([^/]+)$", filename)
    if target_file_name:
        target_file_name = target_file_name.group(1)
    else:
        raise Exception('no file name found')

    logger.info('uploading {} to {} as {}'.format(filename, target_bucket, target_file_name))

    t0 = time.time()
    client.upload_file(Filename=filename, Bucket=target_bucket, Key=target_file_name)
    t1 = time.time()
    logger.info(f'upload took {round(t1 - t0)} seconds, check {target_bucket} for {target_file_name}')



