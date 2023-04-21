import datetime

import pandas as pd
import sqlalchemy

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
    cursor.execute("""truncate raw_companies_house_input_stage_df""")
    cursordb.commit()
    # may change to this line after deprecations
    # df = df.set_axis(dtype_dict_columns_output, copy=False, axis=1)
    df.set_axis(dtype_dict_columns_output, inplace=True, axis=1)
    df.to_sql(name='raw_companies_house_input_stage_df', con=dbEngine, if_exists='append', index=False,
              schema='iqblade')
    cursor.execute("""insert into raw_companies_house_input_stage select * from raw_companies_house_input_stage_df
    on duplicate key update
    raw_companies_house_input_stage.company_name = raw_companies_house_input_stage_df.company_name,
                                                        raw_companies_house_input_stage.company_number = raw_companies_house_input_stage_df.company_number,
                                                        raw_companies_house_input_stage.RegAddress_CareOf = raw_companies_house_input_stage_df.RegAddress_CareOf,
                                                        raw_companies_house_input_stage.RegAddress_POBox = raw_companies_house_input_stage_df.RegAddress_POBox,
                                                        raw_companies_house_input_stage.reg_address_line1 = raw_companies_house_input_stage_df.reg_address_line1,
                                                        raw_companies_house_input_stage.reg_address_line2 = raw_companies_house_input_stage_df.reg_address_line2,
                                                        raw_companies_house_input_stage.reg_address_posttown = raw_companies_house_input_stage_df.reg_address_posttown,
                                                        raw_companies_house_input_stage.reg_address_county = raw_companies_house_input_stage_df.reg_address_county,
                                                        raw_companies_house_input_stage.RegAddress_Country = raw_companies_house_input_stage_df.RegAddress_Country,
                                                        raw_companies_house_input_stage.reg_address_postcode = raw_companies_house_input_stage_df.reg_address_postcode,
                                                        raw_companies_house_input_stage.CompanyCategory = raw_companies_house_input_stage_df.CompanyCategory,
                                                        raw_companies_house_input_stage.company_status = raw_companies_house_input_stage_df.company_status,
                                                        raw_companies_house_input_stage.country_of_origin = raw_companies_house_input_stage_df.country_of_origin,
                                                        raw_companies_house_input_stage.DissolutionDate = raw_companies_house_input_stage_df.DissolutionDate,
                                                        raw_companies_house_input_stage.IncorporationDate = raw_companies_house_input_stage_df.IncorporationDate,
                                                        raw_companies_house_input_stage.Accounts_AccountRefDay = raw_companies_house_input_stage_df.Accounts_AccountRefDay,
                                                        raw_companies_house_input_stage.Accounts_AccountRefMonth = raw_companies_house_input_stage_df.Accounts_AccountRefMonth,
                                                        raw_companies_house_input_stage.Accounts_NextDueDate = raw_companies_house_input_stage_df.Accounts_NextDueDate,
                                                        raw_companies_house_input_stage.Accounts_LastMadeUpDate = raw_companies_house_input_stage_df.Accounts_LastMadeUpDate,
                                                        raw_companies_house_input_stage.Accounts_AccountCategory = raw_companies_house_input_stage_df.Accounts_AccountCategory,
                                                        raw_companies_house_input_stage.Returns_NextDueDate = raw_companies_house_input_stage_df.Returns_NextDueDate,
                                                        raw_companies_house_input_stage.Returns_LastMadeUpDate = raw_companies_house_input_stage_df.Returns_LastMadeUpDate,
                                                        raw_companies_house_input_stage.Mortgages_NumMortCharges = raw_companies_house_input_stage_df.Mortgages_NumMortCharges,
                                                        raw_companies_house_input_stage.Mortgages_NumMortOutstanding = raw_companies_house_input_stage_df.Mortgages_NumMortOutstanding,
                                                        raw_companies_house_input_stage.Mortgages_NumMortPartSatisfied = raw_companies_house_input_stage_df.Mortgages_NumMortPartSatisfied,
                                                        raw_companies_house_input_stage.Mortgages_NumMortSatisfied = raw_companies_house_input_stage_df.Mortgages_NumMortSatisfied,
                                                        raw_companies_house_input_stage.sic_text_1 = raw_companies_house_input_stage_df.sic_text_1,
                                                        raw_companies_house_input_stage.sic_text_2 = raw_companies_house_input_stage_df.sic_text_2,
                                                        raw_companies_house_input_stage.SICCode_SicText_3 = raw_companies_house_input_stage_df.SICCode_SicText_3,
                                                        raw_companies_house_input_stage.SICCode_SicText_4 = raw_companies_house_input_stage_df.SICCode_SicText_4,
                                                        raw_companies_house_input_stage.LimitedPartnerships_NumGenPartners = raw_companies_house_input_stage_df.LimitedPartnerships_NumGenPartners,
                                                        raw_companies_house_input_stage.LimitedPartnerships_NumLimPartners = raw_companies_house_input_stage_df.LimitedPartnerships_NumLimPartners,
                                                        raw_companies_house_input_stage.URI = raw_companies_house_input_stage_df.URI,
                                                        raw_companies_house_input_stage.PreviousName_1_CONDATE = raw_companies_house_input_stage_df.PreviousName_1_CONDATE,
                                                        raw_companies_house_input_stage.PreviousName_1_CompanyName = raw_companies_house_input_stage_df.PreviousName_1_CompanyName,
                                                        raw_companies_house_input_stage.PreviousName_2_CONDATE = raw_companies_house_input_stage_df.PreviousName_2_CONDATE,
                                                        raw_companies_house_input_stage.PreviousName_2_CompanyName = raw_companies_house_input_stage_df.PreviousName_2_CompanyName,
                                                        raw_companies_house_input_stage.PreviousName_3_CONDATE = raw_companies_house_input_stage_df.PreviousName_3_CONDATE,
                                                        raw_companies_house_input_stage.PreviousName_3_CompanyName = raw_companies_house_input_stage_df.PreviousName_3_CompanyName,
                                                        raw_companies_house_input_stage.PreviousName_4_CONDATE = raw_companies_house_input_stage_df.PreviousName_4_CONDATE,
                                                        raw_companies_house_input_stage.PreviousName_4_CompanyName = raw_companies_house_input_stage_df.PreviousName_4_CompanyName,
                                                        raw_companies_house_input_stage.PreviousName_5_CONDATE = raw_companies_house_input_stage_df.PreviousName_5_CONDATE,
                                                        raw_companies_house_input_stage.PreviousName_5_CompanyName = raw_companies_house_input_stage_df.PreviousName_5_CompanyName,
                                                        raw_companies_house_input_stage.PreviousName_6_CONDATE = raw_companies_house_input_stage_df.PreviousName_6_CONDATE,
                                                        raw_companies_house_input_stage.PreviousName_6_CompanyName = raw_companies_house_input_stage_df.PreviousName_6_CompanyName,
                                                        raw_companies_house_input_stage.PreviousName_7_CONDATE = raw_companies_house_input_stage_df.PreviousName_7_CONDATE,
                                                        raw_companies_house_input_stage.PreviousName_7_CompanyName = raw_companies_house_input_stage_df.PreviousName_7_CompanyName,
                                                        raw_companies_house_input_stage.PreviousName_8_CONDATE = raw_companies_house_input_stage_df.PreviousName_8_CONDATE,
                                                        raw_companies_house_input_stage.PreviousName_8_CompanyName = raw_companies_house_input_stage_df.PreviousName_8_CompanyName,
                                                        raw_companies_house_input_stage.PreviousName_9_CONDATE = raw_companies_house_input_stage_df.PreviousName_9_CONDATE,
                                                        raw_companies_house_input_stage.PreviousName_9_CompanyName = raw_companies_house_input_stage_df.PreviousName_9_CompanyName,
                                                        raw_companies_house_input_stage.PreviousName_10_CONDATE = raw_companies_house_input_stage_df.PreviousName_10_CONDATE,
                                                        raw_companies_house_input_stage.PreviousName_10_CompanyName = raw_companies_house_input_stage_df.PreviousName_10_CompanyName,
                                                        raw_companies_house_input_stage.ConfStmtNextDueDate = raw_companies_house_input_stage_df.ConfStmtNextDueDate,
                                                        raw_companies_house_input_stage.ConfStmtLastMadeUpDate = raw_companies_house_input_stage_df.ConfStmtLastMadeUpDate,
                                                        raw_companies_house_input_stage.Date_Of_Insert = raw_companies_house_input_stage_df.Date_Of_Insert,
                                                        raw_companies_house_input_stage.SourceFile = raw_companies_house_input_stage_df.SourceFile,
                                                        raw_companies_house_input_stage.phone_number = raw_companies_house_input_stage_df.phone_number,
                                                        raw_companies_house_input_stage.number_of_employees = raw_companies_house_input_stage_df.number_of_employees""")
    cursordb.commit()
    cursor.execute("""truncate raw_companies_house_input_stage_df""")
    cursordb.commit()


def parse_fragment_retro(fragment, host, user, passwd, db, cursor, cursordb, company_file_table):
    constring = f'mysql://{user}:{passwd}@{host}:3306/{db}'
    dbEngine = sqlalchemy.create_engine(constring)

    df = pd.read_csv(fragment, encoding='utf-8', dtype=dtype_dict, low_memory=False,
                     # usecols=['company_number', 'company_name', 'sic_text_1', 'sic_text_2', 'SICCode_SicText_3', 'SICCode_SicText_4']
                     )
    # df.rename(columns=dtype_dict_comp, inplace=True)
    # todo need to have a table created for each stage based on data of file, this file must then be
    cursor.execute("""truncate raw_companies_house_input_stage_df""")
    cursordb.commit()
    # may change to this line after deprecations
    # df = df.set_axis(dtype_dict_columns_output, copy=False, axis=1)
    # df.set_axis(dtype_dict_columns_output, inplace=True, axis=1)
    df.to_sql(name=f'BasicCompanyData_sic_data_{company_file_table}', con=dbEngine, if_exists='append', index=False,
              schema='iqblade')
    cursordb.commit()


def write_to_organisation(cursor, db):
    cursor.execute("""insert ignore into organisation (id, company_name, company_number, company_status, country, date_formed, last_modified_by )
select rchis.organisation_id, rchis.company_name, rchis.company_number, rchis.company_status, 'United Kingdom', rchis.IncorporationDate, 'Rory' from raw_companies_house_input_stage rchis
left join organisation o on rchis.organisation_id = o.id
where o.id is null and rchis.reg_address_postcode is not null;""")
    db.commit()
#
# cursor, db = connect_preprod()
# # write_to_organisation(cursor, db)
# sic_code_processing(cursor, db)
