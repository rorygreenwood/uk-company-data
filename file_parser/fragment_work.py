import datetime

import mysql.connector
import os
import sqlalchemy
import pandas as pd
from locker import *


def parse_fragment(fragment, host, user, passwd, db, cursor, cursordb):
    constring = f'mysql://{user}:{passwd}@{host}:3306/{db}'
    dbEngine = sqlalchemy.create_engine(constring)

    df = pd.read_csv(fragment, encoding='utf-8', dtype=dtype_dict, low_memory=False)
    # df.rename(columns=dtype_dict_comp, inplace=True)
    df['SourceFile'] = fragment
    df['Date_of_insert'] = datetime.datetime.today()
    df['number_of_employees'] = None
    df['phone_number'] = ''
    df.set_axis(dtype_dict_columns_output, copy=False, axis=1)
    df.to_sql(name='raw_companies_house_input_stage_df', con=dbEngine, if_exists='append', index=False,
              schema='iqblade')
    cursor.execute("""insert into raw_companies_house_input_stage_test_DA797 select * from raw_companies_house_input_stage_df
    on duplicate key update
    raw_companies_house_input_stage_test_DA797.company_name = raw_companies_house_input_stage_df.company_name,
                                                        raw_companies_house_input_stage_test_DA797.company_number = raw_companies_house_input_stage_df.company_number,
                                                        raw_companies_house_input_stage_test_DA797.RegAddress_CareOf = raw_companies_house_input_stage_df.RegAddress_CareOf,
                                                        raw_companies_house_input_stage_test_DA797.RegAddress_POBox = raw_companies_house_input_stage_df.RegAddress_POBox,
                                                        raw_companies_house_input_stage_test_DA797.reg_address_line1 = raw_companies_house_input_stage_df.reg_address_line1,
                                                        raw_companies_house_input_stage_test_DA797.reg_address_line2 = raw_companies_house_input_stage_df.reg_address_line2,
                                                        raw_companies_house_input_stage_test_DA797.reg_address_posttown = raw_companies_house_input_stage_df.reg_address_posttown,
                                                        raw_companies_house_input_stage_test_DA797.reg_address_county = raw_companies_house_input_stage_df.reg_address_county,
                                                        raw_companies_house_input_stage_test_DA797.RegAddress_Country = raw_companies_house_input_stage_df.RegAddress_Country,
                                                        raw_companies_house_input_stage_test_DA797.reg_address_postcode = raw_companies_house_input_stage_df.reg_address_postcode,
                                                        raw_companies_house_input_stage_test_DA797.CompanyCategory = raw_companies_house_input_stage_df.CompanyCategory,
                                                        raw_companies_house_input_stage_test_DA797.company_status = raw_companies_house_input_stage_df.company_status,
                                                        raw_companies_house_input_stage_test_DA797.country_of_origin = raw_companies_house_input_stage_df.country_of_origin,
                                                        raw_companies_house_input_stage_test_DA797.DissolutionDate = raw_companies_house_input_stage_df.DissolutionDate,
                                                        raw_companies_house_input_stage_test_DA797.IncorporationDate = raw_companies_house_input_stage_df.IncorporationDate,
                                                        raw_companies_house_input_stage_test_DA797.Accounts_AccountRefDay = raw_companies_house_input_stage_df.Accounts_AccountRefDay,
                                                        raw_companies_house_input_stage_test_DA797.Accounts_AccountRefMonth = raw_companies_house_input_stage_df.Accounts_AccountRefMonth,
                                                        raw_companies_house_input_stage_test_DA797.Accounts_NextDueDate = raw_companies_house_input_stage_df.Accounts_NextDueDate,
                                                        raw_companies_house_input_stage_test_DA797.Accounts_LastMadeUpDate = raw_companies_house_input_stage_df.Accounts_LastMadeUpDate,
                                                        raw_companies_house_input_stage_test_DA797.Accounts_AccountCategory = raw_companies_house_input_stage_df.Accounts_AccountCategory,
                                                        raw_companies_house_input_stage_test_DA797.Returns_NextDueDate = raw_companies_house_input_stage_df.Returns_NextDueDate,
                                                        raw_companies_house_input_stage_test_DA797.Returns_LastMadeUpDate = raw_companies_house_input_stage_df.Returns_LastMadeUpDate,
                                                        raw_companies_house_input_stage_test_DA797.Mortgages_NumMortCharges = raw_companies_house_input_stage_df.Mortgages_NumMortCharges,
                                                        raw_companies_house_input_stage_test_DA797.Mortgages_NumMortOutstanding = raw_companies_house_input_stage_df.Mortgages_NumMortOutstanding,
                                                        raw_companies_house_input_stage_test_DA797.Mortgages_NumMortPartSatisfied = raw_companies_house_input_stage_df.Mortgages_NumMortPartSatisfied,
                                                        raw_companies_house_input_stage_test_DA797.Mortgages_NumMortSatisfied = raw_companies_house_input_stage_df.Mortgages_NumMortSatisfied,
                                                        raw_companies_house_input_stage_test_DA797.sic_text_1 = raw_companies_house_input_stage_df.sic_text_1,
                                                        raw_companies_house_input_stage_test_DA797.sic_text_2 = raw_companies_house_input_stage_df.sic_text_2,
                                                        raw_companies_house_input_stage_test_DA797.SICCode_SicText_3 = raw_companies_house_input_stage_df.SICCode_SicText_3,
                                                        raw_companies_house_input_stage_test_DA797.SICCode_SicText_4 = raw_companies_house_input_stage_df.SICCode_SicText_4,
                                                        raw_companies_house_input_stage_test_DA797.LimitedPartnerships_NumGenPartners = raw_companies_house_input_stage_df.LimitedPartnerships_NumGenPartners,
                                                        raw_companies_house_input_stage_test_DA797.LimitedPartnerships_NumLimPartners = raw_companies_house_input_stage_df.LimitedPartnerships_NumLimPartners,
                                                        raw_companies_house_input_stage_test_DA797.URI = raw_companies_house_input_stage_df.URI,
                                                        raw_companies_house_input_stage_test_DA797.PreviousName_1_CONDATE = raw_companies_house_input_stage_df.PreviousName_1_CONDATE,
                                                        raw_companies_house_input_stage_test_DA797.PreviousName_1_CompanyName = raw_companies_house_input_stage_df.PreviousName_1_CompanyName,
                                                        raw_companies_house_input_stage_test_DA797.PreviousName_2_CONDATE = raw_companies_house_input_stage_df.PreviousName_2_CONDATE,
                                                        raw_companies_house_input_stage_test_DA797.PreviousName_2_CompanyName = raw_companies_house_input_stage_df.PreviousName_2_CompanyName,
                                                        raw_companies_house_input_stage_test_DA797.PreviousName_3_CONDATE = raw_companies_house_input_stage_df.PreviousName_3_CONDATE,
                                                        raw_companies_house_input_stage_test_DA797.PreviousName_3_CompanyName = raw_companies_house_input_stage_df.PreviousName_3_CompanyName,
                                                        raw_companies_house_input_stage_test_DA797.PreviousName_4_CONDATE = raw_companies_house_input_stage_df.PreviousName_4_CONDATE,
                                                        raw_companies_house_input_stage_test_DA797.PreviousName_4_CompanyName = raw_companies_house_input_stage_df.PreviousName_4_CompanyName,
                                                        raw_companies_house_input_stage_test_DA797.PreviousName_5_CONDATE = raw_companies_house_input_stage_df.PreviousName_5_CONDATE,
                                                        raw_companies_house_input_stage_test_DA797.PreviousName_5_CompanyName = raw_companies_house_input_stage_df.PreviousName_5_CompanyName,
                                                        raw_companies_house_input_stage_test_DA797.PreviousName_6_CONDATE = raw_companies_house_input_stage_df.PreviousName_6_CONDATE,
                                                        raw_companies_house_input_stage_test_DA797.PreviousName_6_CompanyName = raw_companies_house_input_stage_df.PreviousName_6_CompanyName,
                                                        raw_companies_house_input_stage_test_DA797.PreviousName_7_CONDATE = raw_companies_house_input_stage_df.PreviousName_7_CONDATE,
                                                        raw_companies_house_input_stage_test_DA797.PreviousName_7_CompanyName = raw_companies_house_input_stage_df.PreviousName_7_CompanyName,
                                                        raw_companies_house_input_stage_test_DA797.PreviousName_8_CONDATE = raw_companies_house_input_stage_df.PreviousName_8_CONDATE,
                                                        raw_companies_house_input_stage_test_DA797.PreviousName_8_CompanyName = raw_companies_house_input_stage_df.PreviousName_8_CompanyName,
                                                        raw_companies_house_input_stage_test_DA797.PreviousName_9_CONDATE = raw_companies_house_input_stage_df.PreviousName_9_CONDATE,
                                                        raw_companies_house_input_stage_test_DA797.PreviousName_9_CompanyName = raw_companies_house_input_stage_df.PreviousName_9_CompanyName,
                                                        raw_companies_house_input_stage_test_DA797.PreviousName_10_CONDATE = raw_companies_house_input_stage_df.PreviousName_10_CONDATE,
                                                        raw_companies_house_input_stage_test_DA797.PreviousName_10_CompanyName = raw_companies_house_input_stage_df.PreviousName_10_CompanyName,
                                                        raw_companies_house_input_stage_test_DA797.ConfStmtNextDueDate = raw_companies_house_input_stage_df.ConfStmtNextDueDate,
                                                        raw_companies_house_input_stage_test_DA797.ConfStmtLastMadeUpDate = raw_companies_house_input_stage_df.ConfStmtLastMadeUpDate,
                                                        raw_companies_house_input_stage_test_DA797.Date_Of_Insert = raw_companies_house_input_stage_df.Date_Of_Insert,
                                                        raw_companies_house_input_stage_test_DA797.SourceFile = raw_companies_house_input_stage_df.SourceFile,
                                                        raw_companies_house_input_stage_test_DA797.phone_number = raw_companies_house_input_stage_df.phone_number,
                                                        raw_companies_house_input_stage_test_DA797.number_of_employees = raw_companies_house_input_stage_df.number_of_employees""")
    cursordb.commit()
    cursor.execute("""truncate raw_companies_house_input_stage_df""")
    cursordb.commit()

def upsert_sic(cursor, db):
    cursor.execute("""insert into raw_companies_house_data  (company_number, sic_text_1, sic_text_2, sic_text_3, sic_text_4)
     SELECT company_number, sic_text_1, sic_text_2, SICCode_SicText_3, SICCode_SicText_4 from raw_companies_house_input_stage_test_DA797
      on DUPLICATE KEY UPDATE
       sic_text_1 = raw_companies_house_data.sic_text_1,
       sic_text_2 = raw_companies_house_data.sic_text_2,
       sic_text_3 = SICCode_SicText_3,
       sic_text_4 = SICCode_SicText_4""")
    db.commit()


def upsert_address(cursor, db):
    cursor.execute("""insert into raw_companies_house_data_1
     (company_number, reg_address_pobox, reg_address_line1, reg_address_line2, reg_address_posttown, reg_address_county, reg_address_postcode, phone_number) 
    select 
    company_number, RegAddress_POBox, reg_address_line1, reg_address_line2, reg_address_posttown, reg_address_county, reg_address_postcode, phone_number from raw_companies_house_input_stage_test_DA797
    on duplicate key update 
    reg_address_pobox = RegAddress_POBox,
    reg_address_line1 = raw_companies_house_input_stage_test_DA797.reg_address_line1,
    reg_address_line2 = raw_companies_house_input_stage_test_DA797.reg_address_line2,
    reg_address_posttown = raw_companies_house_input_stage_test_DA797.reg_address_posttown,
    reg_address_county = raw_companies_house_input_stage_test_DA797.reg_address_county,
    reg_address_postcode = raw_companies_house_input_stage_test_DA797.reg_address_postcode,
    phone_number = raw_companies_house_input_stage_test_DA797.phone_number
    """)
    db.commit()
