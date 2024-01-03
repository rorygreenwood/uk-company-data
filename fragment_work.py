import datetime
import time

import pandas as pd

from locker import *
from utils import timer, logger


@timer
def parse_fragment(fragment: str, host: str, user: str, passwd: str, db, cursor, cursordb):
    """
    takes a filepath of a fragment and preprod credentials
    turns fragment into dataframe, prepares and inserts into companies house staging table

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
    size_of_fragment = len(df)
    # ensure table is empty by truncating here
    logger.info(f'size of fragment: {size_of_fragment}')
    cursor.execute("""truncate raw_companies_house_input_stage_df""")
    cursordb.commit()

    # send to staging
    df_to_sql_t1 = time.time()
    df.to_sql(name='raw_companies_house_input_stage_df', con=constring, if_exists='append',
              index=False)
    df.fillna(' ')
    df_to_sql_t2 = time.time()
    logger.info(f'time for df_to_sql: {df_to_sql_t2 - df_to_sql_t1}')
    cursor.execute("""select count(*) from raw_companies_house_input_stage_df""")
    count_result = cursor.fetchall()
    count_num_in_rchisdf = count_result[0][0]
    logger.info(f'size of raw_companies_house_input_stage_df: {count_num_in_rchisdf}')
    cursor.execute("""
    insert into raw_companies_house_input_stage select * from raw_companies_house_input_stage_df rchisdf
    on duplicate key update
    raw_companies_house_input_stage.company_name = rchisdf.company_name,
                                                        raw_companies_house_input_stage.company_number = rchisdf.company_number,
                                                        raw_companies_house_input_stage.RegAddress_CareOf = rchisdf.RegAddress_CareOf,
                                                        raw_companies_house_input_stage.RegAddress_POBox = rchisdf.RegAddress_POBox,
                                                        raw_companies_house_input_stage.reg_address_line1 = rchisdf.reg_address_line1,
                                                        raw_companies_house_input_stage.reg_address_line2 = rchisdf.reg_address_line2,
                                                        raw_companies_house_input_stage.reg_address_posttown = rchisdf.reg_address_posttown,
                                                        raw_companies_house_input_stage.reg_address_county = rchisdf.reg_address_county,
                                                        raw_companies_house_input_stage.RegAddress_Country = rchisdf.RegAddress_Country,
                                                        raw_companies_house_input_stage.reg_address_postcode = rchisdf.reg_address_postcode,
                                                        raw_companies_house_input_stage.CompanyCategory = rchisdf.CompanyCategory,
                                                        raw_companies_house_input_stage.company_status = rchisdf.company_status,
                                                        raw_companies_house_input_stage.country_of_origin = rchisdf.country_of_origin,
                                                        raw_companies_house_input_stage.DissolutionDate = rchisdf.DissolutionDate,
                                                        raw_companies_house_input_stage.IncorporationDate = rchisdf.IncorporationDate,
                                                        raw_companies_house_input_stage.Accounts_AccountRefDay = rchisdf.Accounts_AccountRefDay,
                                                        raw_companies_house_input_stage.Accounts_AccountRefMonth = rchisdf.Accounts_AccountRefMonth,
                                                        raw_companies_house_input_stage.Accounts_NextDueDate = rchisdf.Accounts_NextDueDate,
                                                        raw_companies_house_input_stage.Accounts_LastMadeUpDate = rchisdf.Accounts_LastMadeUpDate,
                                                        raw_companies_house_input_stage.Accounts_AccountCategory = rchisdf.Accounts_AccountCategory,
                                                        raw_companies_house_input_stage.Returns_NextDueDate = rchisdf.Returns_NextDueDate,
                                                        raw_companies_house_input_stage.Returns_LastMadeUpDate = rchisdf.Returns_LastMadeUpDate,
                                                        raw_companies_house_input_stage.Mortgages_NumMortCharges = rchisdf.Mortgages_NumMortCharges,
                                                        raw_companies_house_input_stage.Mortgages_NumMortOutstanding = rchisdf.Mortgages_NumMortOutstanding,
                                                        raw_companies_house_input_stage.Mortgages_NumMortPartSatisfied = rchisdf.Mortgages_NumMortPartSatisfied,
                                                        raw_companies_house_input_stage.Mortgages_NumMortSatisfied = rchisdf.Mortgages_NumMortSatisfied,
                                                        raw_companies_house_input_stage.sic_text_1 = rchisdf.sic_text_1,
                                                        raw_companies_house_input_stage.sic_text_2 = rchisdf.sic_text_2,
                                                        raw_companies_house_input_stage.SICCode_SicText_3 = rchisdf.SICCode_SicText_3,
                                                        raw_companies_house_input_stage.SICCode_SicText_4 = rchisdf.SICCode_SicText_4,
                                                        raw_companies_house_input_stage.LimitedPartnerships_NumGenPartners = rchisdf.LimitedPartnerships_NumGenPartners,
                                                        raw_companies_house_input_stage.LimitedPartnerships_NumLimPartners = rchisdf.LimitedPartnerships_NumLimPartners,
                                                        raw_companies_house_input_stage.URI = rchisdf.URI,
                                                        raw_companies_house_input_stage.PreviousName_1_CONDATE = rchisdf.PreviousName_1_CONDATE,
                                                        raw_companies_house_input_stage.PreviousName_1_CompanyName = rchisdf.PreviousName_1_CompanyName,
                                                        raw_companies_house_input_stage.PreviousName_2_CONDATE = rchisdf.PreviousName_2_CONDATE,
                                                        raw_companies_house_input_stage.PreviousName_2_CompanyName = rchisdf.PreviousName_2_CompanyName,
                                                        raw_companies_house_input_stage.PreviousName_3_CONDATE = rchisdf.PreviousName_3_CONDATE,
                                                        raw_companies_house_input_stage.PreviousName_3_CompanyName = rchisdf.PreviousName_3_CompanyName,
                                                        raw_companies_house_input_stage.PreviousName_4_CONDATE = rchisdf.PreviousName_4_CONDATE,
                                                        raw_companies_house_input_stage.PreviousName_4_CompanyName = rchisdf.PreviousName_4_CompanyName,
                                                        raw_companies_house_input_stage.PreviousName_5_CONDATE = rchisdf.PreviousName_5_CONDATE,
                                                        raw_companies_house_input_stage.PreviousName_5_CompanyName = rchisdf.PreviousName_5_CompanyName,
                                                        raw_companies_house_input_stage.PreviousName_6_CONDATE = rchisdf.PreviousName_6_CONDATE,
                                                        raw_companies_house_input_stage.PreviousName_6_CompanyName = rchisdf.PreviousName_6_CompanyName,
                                                        raw_companies_house_input_stage.PreviousName_7_CONDATE = rchisdf.PreviousName_7_CONDATE,
                                                        raw_companies_house_input_stage.PreviousName_7_CompanyName = rchisdf.PreviousName_7_CompanyName,
                                                        raw_companies_house_input_stage.PreviousName_8_CONDATE = rchisdf.PreviousName_8_CONDATE,
                                                        raw_companies_house_input_stage.PreviousName_8_CompanyName = rchisdf.PreviousName_8_CompanyName,
                                                        raw_companies_house_input_stage.PreviousName_9_CONDATE = rchisdf.PreviousName_9_CONDATE,
                                                        raw_companies_house_input_stage.PreviousName_9_CompanyName = rchisdf.PreviousName_9_CompanyName,
                                                        raw_companies_house_input_stage.PreviousName_10_CONDATE = rchisdf.PreviousName_10_CONDATE,
                                                        raw_companies_house_input_stage.PreviousName_10_CompanyName = rchisdf.PreviousName_10_CompanyName,
                                                        raw_companies_house_input_stage.ConfStmtNextDueDate = rchisdf.ConfStmtNextDueDate,
                                                        raw_companies_house_input_stage.ConfStmtLastMadeUpDate = rchisdf.ConfStmtLastMadeUpDate,
                                                        raw_companies_house_input_stage.Date_Of_Insert = rchisdf.Date_Of_Insert,
                                                        raw_companies_house_input_stage.SourceFile = rchisdf.SourceFile,
                                                        raw_companies_house_input_stage.phone_number = rchisdf.phone_number,
                                                        raw_companies_house_input_stage.number_of_employees = rchisdf.number_of_employees,
                                                        raw_companies_house_input_stage.md5_key = md5(concat(rchisdf.organisation_id, rchisdf.reg_address_postcode))""")
    cursordb.commit()
    cursor.execute("""select count(*) from raw_companies_house_input_stage""")
    res = cursor.fetchall()
    logger.info(f'count of raw_companies_house_input_stage: {res[0][0]}')


@timer
def _parse_fragment_sic(fragment, user, passwd, host, constring_database, cursor, db):
    """
    REQUIRES CSV FRAGMENT
    rewrite into new stages
    1. write into table where are sic codes are in one column
    2. apply md5 of concat(sic_code, filepath)
    3. insert into final table (sic code, file path, count, md5)
    """
    cursor.execute("""truncate companies_house_sic_pool""")
    db.commit()
    constring = f'mysql://{user}:{passwd}@{host}:3306/{constring_database}'
    df = pd.read_csv(fragment, usecols=[' CompanyNumber', 'SICCode.SicText_1', 'SICCode.SicText_2', 'SICCode.SicText_3',
                                        'SICCode.SicText_4'])
    df.rename(columns=companies_house_conversion_dict, inplace=True)
    df['FilePath'] = fragment + 'HISTORICAL'
    df.to_sql(name='companies_house_sic_pool', con=constring, if_exists='append',
              index=False)

    # change pandas work to a sql query pulling from the rchis file
    cursor.execute("""insert into companies_house_sic_pool (CompanyNumber, SicText_1, SicText_2, SicText_3, SicText_4, FilePath, md5_str) SELECT 
    company_number, sic_text_1, sic_text_2, SICCode_SicText_3, SICCode_SicText_4, SourceFile, md5_key from raw_companies_house_input_stage
    """)
    db.commit()

    # remove the text from the sic codes so we are just left with numbers
    cursor.execute("""update companies_house_sic_pool
set
    SicText_1 = SUBSTRING_INDEX(SicText_1, '-', 1),
    SicText_2 = SUBSTRING_INDEX(SicText_2, '-', 1),
    SicText_3 = SUBSTRING_INDEX(SicText_3, '-', 1),
    SicText_4 = SUBSTRING_INDEX(SicText_4, '-', 1)""")
    db.commit()

    cursor.execute("""update companies_house_sic_pool
     set FilePath = regexp_substr(FilePath, '[0-9]{4}-[0-9]{2}-[0-9]{2}', 1)
    """)
    db.commit()

    # takes records from the sic pool as a fragment is upload to sic_pool
    # writes them into staging
    cursor.execute("""
    insert into companies_house_sic_code_staging_1 (sic_code, sic_code_count, file_date)
select sic_code, sum(sic_count), FilePath from (select SicText_1 as sic_code, count(*) as sic_count, Filepath
                             from companies_house_sic_pool
                             where SicText_1 is not null
                             group by SicText_1, Filepath
                             union
                             select SicText_2, count(*), Filepath
                             from companies_house_sic_pool
                             where SicText_2 is not null
                             group by SicText_2, Filepath
                             union
                             select SicText_3, count(*), Filepath
                             from companies_house_sic_pool
                             where SicText_3 is not null
                             group by SicText_3, Filepath
                             union
                             select SicText_4, count(*), Filepath
                             from companies_house_sic_pool
                             where SicText_4 is not null
                             group by SicText_4, Filepath) t1 group by sic_code""")
    db.commit()

    # truncate sic_pool table as data no longer required
    cursor.execute("""truncate table companies_house_sic_pool""")
    db.commit()

    # once in staging_1, they are given an md5
    cursor.execute("""
    update companies_house_sic_code_staging_1 
    set md5_str = md5(concat(sic_code, file_date))
    where md5_str is null""")
    db.commit()

    # inserted into second staging table, where on a duplicate update the two
    # counts for that sic code are added together
    # sic_code_staging_2 houses the counts for the CH file being processed at that time. Once complete, these will
    # all be moved to the companies_house
    cursor.execute("""insert into companies_house_sic_code_staging_2 (sic_code, file_date,  sic_code_count, md5_str)
        select sic_code, file_date, sic_code_count, md5_str from companies_house_sic_code_staging_1 t2
        on duplicate key update
        companies_house_sic_code_staging_2.sic_code_count = companies_house_sic_code_staging_2.sic_code_count + t2.sic_code_count
        """)
    db.commit()

    # truncate staging_1 as it is no longer required
    cursor.execute("""truncate table companies_house_sic_code_staging_1""")
    db.commit()


@timer
def section_3_sic_data_inserts(cursor, db):
    logger.info('inserting into companies_house_sic_pool')
    cursor.execute("""insert into companies_house_sic_pool (CompanyNumber, SicText_1, SicText_2, SicText_3, SicText_4, FilePath, md5_str) SELECT 
        company_number, sic_text_1, sic_text_2, SICCode_SicText_3, SICCode_SicText_4, SourceFile, md5_key from raw_companies_house_input_stage
        """)
    db.commit()

    # remove the text from the sic codes so we are just left with numbers
    logger.info('beginning parse_rchis_sic datacleaning')
    cursor.execute("""update companies_house_sic_pool
    set
        SicText_1 = SUBSTRING_INDEX(SicText_1, '-', 1),
        SicText_2 = SUBSTRING_INDEX(SicText_2, '-', 1),
        SicText_3 = SUBSTRING_INDEX(SicText_3, '-', 1),
        SicText_4 = SUBSTRING_INDEX(SicText_4, '-', 1)""")
    db.commit()
    cursor.execute("""update companies_house_sic_pool
         set FilePath = regexp_substr(FilePath, '[0-9]{4}-[0-9]{2}-[0-9]{2}', 1)
        """)
    db.commit()
    # takes records from the sic pool as a fragment is upload to sic_pool
    # writes them into staging
    logger.info('writing into staging')
    cursor.execute("""
        insert into companies_house_sic_code_staging_1 (sic_code, sic_code_count, file_date)
    select sic_code, sum(sic_count), FilePath from (select SicText_1 as sic_code, count(*) as sic_count, Filepath
                                 from companies_house_sic_pool
                                 where SicText_1 is not null
                                 group by SicText_1, Filepath
                                 union
                                 select SicText_2, count(*), Filepath
                                 from companies_house_sic_pool
                                 where SicText_2 is not null
                                 group by SicText_2, Filepath
                                 union
                                 select SicText_3, count(*), Filepath
                                 from companies_house_sic_pool
                                 where SicText_3 is not null
                                 group by SicText_3, Filepath
                                 union
                                 select SicText_4, count(*), Filepath
                                 from companies_house_sic_pool
                                 where SicText_4 is not null
                                 group by SicText_4, Filepath) t1 group by sic_code""")
    db.commit()
    # truncate sic_pool table as data no longer required
    logger.info('truncating sic_pool')
    cursor.execute("""truncate table companies_house_sic_pool""")
    db.commit()
    # once in staging_1, they are given an md5
    logger.info('updating_md5 values')
    cursor.execute("""
        update companies_house_sic_code_staging_1 
        set md5_str = md5(concat(sic_code, file_date))
        where md5_str is null""")
    db.commit()
    # inserted into second staging table, where on a duplicate update the two
    # counts for that sic code are added together
    # sic_code_staging_2 houses the counts for the CH file being processed at that time. Once complete, these will
    # all be moved to the companies_house
    logger.info('inserting into sic_code_staging_2')
    cursor.execute("""insert into companies_house_sic_code_staging_2 (sic_code, file_date,  sic_code_count, md5_str)
            select sic_code, file_date, sic_code_count, md5_str from companies_house_sic_code_staging_1 t2
            on duplicate key update
            companies_house_sic_code_staging_2.sic_code_count = companies_house_sic_code_staging_2.sic_code_count + t2.sic_code_count
            """)
    db.commit()
    # truncate staging_1 as it is no longer required
    logger.info('truncating staging_1')
    cursor.execute("""truncate table companies_house_sic_code_staging_1""")
    db.commit()
    # todo insert into sic_code_analytics

@timer
def write_to_organisation(cursor, db):
    cursor.execute("""insert into organisation (id, company_name, company_number, company_status, country, date_formed, last_modified_by )
select rchis.organisation_id, rchis.company_name, rchis.company_number, rchis.company_status, 'United Kingdom', rchis.IncorporationDate, 'Rory' from raw_companies_house_input_stage rchis
left join organisation o on rchis.organisation_id = o.id
where o.id is null and rchis.reg_address_postcode is not null
;""")
    db.commit()
