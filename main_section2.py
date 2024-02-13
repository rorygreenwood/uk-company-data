"""take fragments from s3 bucket and load them into rchis."""
import hashlib
import os
import re
import sys

import boto3
import pandas as pd

from fragment_work import parse_fragment, parse_fragment_sic
from utils import timer, handle_exception, \
    logger, connect_preprod, constring

__mycode = True
sys.excepthook = handle_exception

# connected to preprod
host = os.environ.get('PREPRODHOST')
user = os.environ.get('ADMINUSER')
passwd = os.environ.get('ADMINPASS')
database = os.environ.get('DATABASE')
cursor, db = connect_preprod()

bucket_name = os.environ.get('S3_COMPANIES_HOUSE_FRAGMENTS_BUCKET')
s3_client = boto3.client('s3',
                         aws_access_key_id=os.environ.get('HGDATA-S3-AWS-ACCESS-KEY-ID'),
                         aws_secret_access_key=os.environ.get('HGDATA-S3-AWS-SECRET-KEY'),
                         region_name='eu-west-1'
                         )
list_of_s3_objects = s3_client.list_objects_v2(Bucket=bucket_name, Prefix="", Delimiter="/")

global file_date


# @pipeline_message_wrap
@timer
def process_section2():
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
    for s3_object in list_of_s3_objects['Contents']:
        # check if the file has already been processed in the past, if it has it will be in this table
        # if it is not in the table, then we can assume it has not been parsed and can continue to process it
        logger.info(s3_object["Key"])
        if s3_object['Key'].endswith('.csv'):
            fragment_file_name = s3_object['Key']
            fragments_abspath = os.path.abspath('file_downloader/files/fragments')
            fragment_file_path = f'{fragments_abspath}/{fragment_file_name}'
            s3_client.download_file(bucket_name, fragment_file_name,
                                    f'file_downloader/files/fragments\\{fragment_file_name}')
            logger.info('parsing {}'.format(fragment_file_name))
            fragment = 'file_downloader/files/fragments/{}'.format(fragment_file_name)

            # 2a. for each fragment, call parse_fragment() function to upsert to iqblade.raw_companies_house_input_staging
            parse_fragment(fragment_file_path,
                           host=host, user=user, passwd=passwd,
                           db=database, cursor=cursor, cursordb=db)

            # 2b. for each fragment, call parse_fragment_sic() to contribute to pandas dataframe monthly_df
            # regex of the fragment to get the file_date value
            file_date = re.search(string=fragment, pattern='\d{4}-\d{2}-\d{2,3}')[0]
            df_counts = parse_fragment_sic(fragment, file_date)
            monthly_df = pd.concat([monthly_df, df_counts], axis=0)

            # 2c. remove file from local and s3 bucket
            os.remove(f'file_downloader/files/fragments/{fragment_file_name}')
            s3_client.delete_object(Bucket=bucket_name, Key=fragment_file_name)
        else:
            pass

    # 3. concat monthly_df into a single dataframe based on sic_code and use dataframe.to_sql() to send it to
    # companies_house_sic_code_counts
    grouped_df = monthly_df.groupby('sic_code')
    sic_count = grouped_df['sic_code_count'].sum()
    sic_code_index = grouped_df.groups.keys()
    df2 = pd.DataFrame({'sic_code': sic_code_index, 'sic_code_count': sic_count})
    df2['file_date'] = file_date
    df2['md5_str'] = ''
    # apply md5
    for idx, row in df2.iterrows():
        row.loc['md5_str'] = hashlib.md5((file_date + row.loc['sic_code']).encode('utf-8')).hexdigest()

    df2.dropna()
    df2.to_sql(
        name='companies_house_sic_counts',
        con=constring,
        if_exists='append',
        index=False
    )

    # pair with existing sic code categories table to assign category to sic code
    cursor.execute("""
    update companies_house_sic_counts t1 inner join 
    sic_code_categories t2 on t1.sic_code = t2.`SIC Code`
    set t1.sic_code_category = t2.Category
    """)
    db.commit()

    # 4. take companies_house_sic_code_counts data and add it into aggregates
    cursor.execute("""insert into companies_house_sic_code_aggregates
                        (file_date, Category, count, md5_str)
                        select file_date, sic_code_category, sum(sic_code_count), md5(concat(file_date, sic_code_category))
                        from companies_house_sic_counts
                        where sic_code_category is not null
                        and file_date = %s
                        group by file_date, sic_code_category""", (file_date,))
    db.commit()

    # once complete, update iqblade.comapnies_house_filetracker and set section_2 value to curdate
    cursor.execute("""update companies_house_filetracker
     set section2 = DATE(CURDATE()) where filename = %s and section2 is null""", (file_date,))


if __name__ == '__main__':
    process_section2()
