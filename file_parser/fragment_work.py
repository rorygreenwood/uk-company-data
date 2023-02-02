import datetime
import sqlalchemy
import pandas as pd
from locker import *
import re


def parse_fragment(fragment, host, user, passwd, db, cursor, cursordb):
    constring = f'mysql://{user}:{passwd}@{host}:3306/{db}'
    dbEngine = sqlalchemy.create_engine(constring)

    df = pd.read_csv(fragment, encoding='utf-8', dtype=dtype_dict, low_memory=False)
    # df.rename(columns=dtype_dict_comp, inplace=True)
    df['SourceFile'] = fragment
    df['Date_of_insert'] = datetime.datetime.today()
    df['number_of_employees'] = None
    df['phone_number'] = ''
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


def upsert_sic(cursor, db):
    cursor.execute("""insert into raw_companies_house_data  (company_number, sic_text_1, sic_text_2, sic_text_3, sic_text_4)
     SELECT company_number, sic_text_1, sic_text_2, SICCode_SicText_3, SICCode_SicText_4 from raw_companies_house_input_stage
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
    company_number, RegAddress_POBox, reg_address_line1, reg_address_line2, reg_address_posttown, reg_address_county, reg_address_postcode, phone_number from raw_companies_house_input_stage
    on duplicate key update 
    reg_address_pobox = RegAddress_POBox,
    reg_address_line1 = raw_companies_house_input_stage.reg_address_line1,
    reg_address_line2 = raw_companies_house_input_stage.reg_address_line2,
    reg_address_posttown = raw_companies_house_input_stage.reg_address_posttown,
    reg_address_county = raw_companies_house_input_stage.reg_address_county,
    reg_address_postcode = raw_companies_house_input_stage.reg_address_postcode,
    phone_number = raw_companies_house_input_stage.phone_number
    """)
    db.commit()


def sic_code_processing(cursor, db):
    """ load all results from monthly parse - divide sic code from text and insert into"""
    cursor.execute("""select raw_companies_house_data.* from raw_companies_house_data
    left join sic_code sc on raw_companies_house_data.company_number = sc.company_number
    where sc.code is null""")
    res = cursor.fetchall()
    count = 0
    for company_number, sic1, sic2, sic3, sic4 in res:
        org_id = f'UK{company_number}'
        count += 1
        if count == 100:
            print(count)
            count = 0
        siclist = [sic1, sic2, sic3, sic4]
        for sic in siclist:
            if sic is not None:
                sic_code = re.findall(r'\d+', sic)
                if len(sic_code) == 1:
                    sic_code = sic_code[0]
                else:
                    sic_code = 'None Supplied'
            else:
                sic_code = 'None Supplied'
            cursor.execute(
                """insert ignore into sic_code (code, organisation_id, company_number) VALUES (%s, %s, %s)""",
                (sic_code, org_id, company_number))
            db.commit()


def address_processing(cursor, db):
    """move address data from staging to address table for front-end use"""
    cursor.execute("""select
    company_number
    , company_name
    , RegAddress_POBox
    , reg_address_line1
    , reg_address_line2
    , reg_address_posttown
    , reg_address_county
    , reg_address_postcode
    , RegAddress_Country
     from raw_companies_house_input_stage""")
    res = cursor.fetchall()
    for cnumber, cname, pobox, line1, line2, posttown, county, postcode, country in res:
        org_id = f'UK{cnumber}'
        format_postcode = postcode.lower().replace(' ', '')
        cursor.execute("""select organisation_id, address_1, address_2 from geo_location where organisation_id = %s""",
                       (org_id,))
        search_res = cursor.fetchall()
        print(len(search_res))
        if search_res == 0:
            print('---no org_id, add logic to add in---')
        else:
            if len(search_res) > 10:
                print('org_id exists, either skip or update')
                for org_id, ad1, ad2 in search_res:
                    print(org_id, ad1, ad2)
        # print(format_postcode)
        # print('---')
        # office_type = 'HEAD OFFICE'
        # cursor.execute("""insert into geo_location
        # (organisation_id, address_1, address_2, town, county, post_code, post_code_formatted, area_location,
        # address_type) VALUES (%s, %s, %ws, %s, %s, %s, %s, %s, %s)
        # """,
        #                (org_id, line1, line2, posttown, county, postcode, format_postcode, county, office_type
        #                 )
        #                )


def write_to_organisation(cursor, db):
    cursor.execute("""select company_number, company_name from raw_companies_house_input_stage limit 100000""")
    res = cursor.fetchall()
    for cnumber, cname in res:
        org_id = f'UK{cnumber}'
        cursor.execute("""select * from organisation where id = %s""", (org_id,))
        org_id_check = cursor.fetchall()
        if len(org_id_check) == 0:
            print('new record here')
            print(org_id)
            cursor.execute("""
            insert ignore into organisation_insert_test (id, company_name, company_number, company_status, country, date_formed, last_modified_by)
            select %s, company_name, company_number, company_status, UPPER(country_of_origin), IncorporationDate, %s 
            from raw_companies_house_input_stage where company_number = %s""", (org_id, 'Rory', cnumber))
            db.commit()

#
# cursor, db = connect_preprod()
# # write_to_organisation(cursor, db)
# sic_code_processing(cursor, db)
