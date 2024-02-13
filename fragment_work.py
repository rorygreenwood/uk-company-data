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

    cursor.execute("""insert into raw_companies_house_input_stage
    select * from raw_companies_house_input_stage_df t2
    on duplicate key update 
    raw_companies_house_input_stage.company_name = t2.company_name,
    raw_companies_house_input_stage.RegAddress_CareOf = t2.RegAddress_CareOf,
    raw_companies_house_input_stage.RegAddress_POBox = t2.RegAddress_POBox,
    raw_companies_house_input_stage.reg_address_line1 = t2.reg_address_line1,
    raw_companies_house_input_stage.reg_address_line2 = t2.reg_address_line2,
    raw_companies_house_input_stage.reg_address_posttown = t2.reg_address_posttown,
    raw_companies_house_input_stage.reg_address_county = t2.reg_address_county,
    raw_companies_house_input_stage.RegAddress_Country = t2.RegAddress_Country,
    raw_companies_house_input_stage.reg_address_postcode = t2.reg_address_postcode,
    raw_companies_house_input_stage.CompanyCategory = t2.CompanyCategory,
    raw_companies_house_input_stage.company_status = t2.company_status,
    raw_companies_house_input_stage.country_of_origin = t2.country_of_origin,
    raw_companies_house_input_stage.DissolutionDate = t2.DissolutionDate,
    raw_companies_house_input_stage.IncorporationDate = t2.IncorporationDate,
    raw_companies_house_input_stage.Accounts_AccountRefDay = t2.Accounts_AccountRefDay,
    raw_companies_house_input_stage.Accounts_AccountRefMonth = t2.Accounts_AccountRefMonth,
    raw_companies_house_input_stage.Accounts_NextDueDate = t2.Accounts_NextDueDate,
    raw_companies_house_input_stage.Accounts_LastMadeUpDate = t2.Accounts_LastMadeUpDate,
    raw_companies_house_input_stage.Accounts_AccountCategory = t2.Accounts_AccountCategory,
    raw_companies_house_input_stage.Returns_NextDueDate = t2.Returns_NextDueDate,
    raw_companies_house_input_stage.Returns_LastMadeUpDate = t2.Returns_LastMadeUpDate,
    raw_companies_house_input_stage.Mortgages_NumMortCharges = t2.Mortgages_NumMortCharges,
    raw_companies_house_input_stage.Mortgages_NumMortOutstanding = t2.Mortgages_NumMortOutstanding,
    raw_companies_house_input_stage.Mortgages_NumMortPartSatisfied = t2.Mortgages_NumMortPartSatisfied,
    raw_companies_house_input_stage.Mortgages_NumMortSatisfied = t2.Mortgages_NumMortSatisfied,
    raw_companies_house_input_stage.sic_text_1 = t2.sic_text_1,
    raw_companies_house_input_stage.sic_text_2 = t2.sic_text_2,
    raw_companies_house_input_stage.SICCode_SicText_3 = t2.SICCode_SicText_3,
    raw_companies_house_input_stage.SICCode_SicText_4 = t2.SICCode_SicText_4,
    raw_companies_house_input_stage.LimitedPartnerships_NumGenPartners = t2.LimitedPartnerships_NumGenPartners,
    raw_companies_house_input_stage.LimitedPartnerships_NumLimPartners = t2.LimitedPartnerships_NumLimPartners,
    raw_companies_house_input_stage.URI = t2.URI,
    raw_companies_house_input_stage.PreviousName_1_CONDATE = t2.PreviousName_1_CONDATE,
    raw_companies_house_input_stage.PreviousName_1_CompanyName = t2.PreviousName_1_CompanyName,
    raw_companies_house_input_stage.PreviousName_2_CONDATE = t2.PreviousName_2_CONDATE,
    raw_companies_house_input_stage.PreviousName_2_CompanyName = t2.PreviousName_2_CompanyName,
    raw_companies_house_input_stage.PreviousName_3_CONDATE = t2.PreviousName_3_CONDATE,
    raw_companies_house_input_stage.PreviousName_3_CompanyName = t2.PreviousName_3_CompanyName,
    raw_companies_house_input_stage.PreviousName_4_CONDATE = t2.PreviousName_4_CONDATE,
    raw_companies_house_input_stage.PreviousName_4_CompanyName = t2.PreviousName_4_CompanyName,
    raw_companies_house_input_stage.PreviousName_5_CONDATE = t2.PreviousName_5_CONDATE,
    raw_companies_house_input_stage.PreviousName_5_CompanyName = t2.PreviousName_5_CompanyName,
    raw_companies_house_input_stage.PreviousName_6_CONDATE = t2.PreviousName_6_CONDATE,
    raw_companies_house_input_stage.PreviousName_6_CompanyName = t2.PreviousName_6_CompanyName,
    raw_companies_house_input_stage.PreviousName_7_CONDATE = t2.PreviousName_7_CONDATE,
    raw_companies_house_input_stage.PreviousName_7_CompanyName = t2.PreviousName_7_CompanyName,
    raw_companies_house_input_stage.PreviousName_8_CONDATE = t2.PreviousName_8_CONDATE,
    raw_companies_house_input_stage.PreviousName_8_CompanyName = t2.PreviousName_8_CompanyName,
    raw_companies_house_input_stage.PreviousName_9_CONDATE = t2.PreviousName_9_CONDATE,
    raw_companies_house_input_stage.PreviousName_9_CompanyName = t2.PreviousName_9_CompanyName,
    raw_companies_house_input_stage.PreviousName_10_CONDATE = t2.PreviousName_10_CONDATE,
    raw_companies_house_input_stage.PreviousName_10_CompanyName = t2.PreviousName_10_CompanyName,
    raw_companies_house_input_stage.ConfStmtNextDueDate = t2.ConfStmtNextDueDate,
    raw_companies_house_input_stage.ConfStmtLastMadeUpDate = t2.ConfStmtLastMadeUpDate,
    raw_companies_house_input_stage.Date_Of_Insert = t2.Date_Of_Insert,
    raw_companies_house_input_stage.SourceFile = t2.SourceFile,
    raw_companies_house_input_stage.phone_number = t2.phone_number,
    raw_companies_house_input_stage.number_of_employees = t2.number_of_employees,
    raw_companies_house_input_stage.md5_key = md5(concat(t2.organisation_id, t2.reg_address_postcode))
    """)
    cursordb.commit()



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
