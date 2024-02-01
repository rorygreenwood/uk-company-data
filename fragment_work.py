import datetime
import os

import pandas as pd

from locker import *
from utils import timer, connect_preprod, remove_non_numeric

cursor, db = connect_preprod()


@timer
def parse_fragment(fragment: str,
                   cursor, cursordb,
                   host: str = os.environ.get('PREPRODHOST'),
                   user: str = os.environ.get('ADMINUSER'),
                   passwd: str = os.environ.get('ADMINPASS'),
                   db: str = os.environ.get('DATABASE')
                   ) -> None:
    """
    takes a filepath of a fragment and preprod credentials
    turns fragment into dataframe, prepares and inserts
    into companies house staging table

    1. read fragment as csv
    2. add additional columns
    3. send to staging table
    4. upsert live table with data from staging table
    5. truncate staging table and remove files from s3_buckets and folder

    :param fragment:
    :param host:
    :param user:
    :param passwd:
    :param db:
    :param cursor:
    :param cursordb:
    :return:
    """
    constring = f'mysql://{user}:{passwd}@{host}:3306/{db}'
    df = pd.read_csv(fragment, encoding='utf-8', low_memory=False)

    # clean data and add debugging
    df.rename(columns=companies_house_conversion_dict, inplace=True)
    df['SourceFile'] = fragment
    df['Date_of_insert'] = datetime.datetime.today()
    df['number_of_employees'] = None
    df['phone_number'] = ''
    cursor.execute("""truncate raw_companies_house_input_stage_df""")
    cursordb.commit()

    # send to staging
    df.to_sql(name='raw_companies_house_input_stage_df', con=constring, if_exists='append',
              index=False)


@timer
def parse_fragment_sic(fragment: str, file_date: str) -> pd.DataFrame:
    df = pd.read_csv(fragment, usecols=['SICCode.SicText_1', 'SICCode.SicText_2', 'SICCode.SicText_3',
                                        'SICCode.SicText_4', 'CompanyStatus'])
    # filter by company status, and then drop the column
    df = df[df['CompanyStatus'].isin(['Active', 'Active - Proposal to Strike Off'])]
    df = df.drop('CompanyStatus', axis=1)
    df.rename(columns=sic_code_conversion_dict, inplace=True)
    temp = []

    # monthly_df will be what each fragment dataframe feeds into, gradually increasing the size of the counts
    # based on what

    # add all the sic_codes to a single column
    for row in df.itertuples():
        temp.append(row.SicText_1)
        temp.append(row.SicText_2)
        temp.append(row.SicText_3)
        temp.append(row.SicText_4)

    df = pd.DataFrame({'sic_code': temp,
                       'file_date': file_date})

    # remove all null values
    df = df.dropna()

    # remove inactive companies

    df['sic_code'] = df['sic_code'].apply(remove_non_numeric)
    df_counts = df['sic_code'].value_counts()
    df_counts = df_counts.to_frame(name='sic_code_count')
    df_counts = df_counts.reset_index()
    df_counts.columns = ['sic_code', 'sic_code_count']
    return df_counts


@timer
def write_to_organisation(cursor, db):
    cursor.execute("""insert into organisation (id, company_name, company_number, company_status, country, date_formed, last_modified_by )
select rchis.organisation_id, rchis.company_name, rchis.company_number, rchis.company_status, 'United Kingdom', rchis.IncorporationDate, 'Rory' from raw_companies_house_input_stage rchis
left join organisation o on rchis.organisation_id = o.id
where o.id is null and rchis.reg_address_postcode is not null
;""")
    db.commit()
