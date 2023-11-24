import datetime
import logging

import pandas as pd
import sqlalchemy

from locker import *
from utils import timer

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s [line:%(lineno)d] %(levelname)s: %(message)s')
logger = logging.getLogger()


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
    # transform
    df.rename(columns=companies_house_conversion_dict, inplace=True)
    df['SourceFile'] = fragment
    df['Date_of_insert'] = datetime.datetime.today()
    df['number_of_employees'] = None
    df['phone_number'] = ''
    cursor.execute("""truncate raw_companies_house_input_stage_df""")
    cursordb.commit()
    df.to_sql(name='raw_companies_house_input_stage_df', con=constring, if_exists='append',
              index=False)
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


@timer
def _parse_fragment_sic(fragment, user, passwd, host, db, cursor, ppdb):
    logger.info(f'parse_fragment_sic: {fragment}')
    cursor.execute("""truncate companies_house_sic_pool""")
    ppdb.commit()
    constring = f'mysql://{user}:{passwd}@{host}:3306/{db}'
    df = pd.read_csv(fragment, usecols=[' CompanyNumber', 'SICCode.SicText_1',
                                        'SICCode.SicText_2', 'SICCode.SicText_3', 'SICCode.SicText_4'])
    df.rename(columns=sic_code_conversion_dict, inplace=True)
    df['FilePath'] = fragment + 'HISTORICAL'
    fragment_path = fragment + 'HISTORICAL'
    df.to_sql(name='companies_house_sic_pool', con=constring, if_exists='append',
              index=False)

    cursor.execute("""update companies_house_sic_pool
set
    SicText_1 = SUBSTRING_INDEX(SicText_1, '-', 1),
    SicText_2 = SUBSTRING_INDEX(SicText_2, '-', 1),
    SicText_3 = SUBSTRING_INDEX(SicText_3, '-', 1),
    SicText_4 = SUBSTRING_INDEX(SicText_4, '-', 1)""")
    ppdb.commit()
    # change md5 from company number to sic code
    cursor.execute("""update companies_house_sic_pool
     set md5_str = md5(concat(CompanyNumber, FilePath))
    where md5_str is null and FilePath = %s
    """, (fragment_path,))
    ppdb.commit()
    # remove text and keep regex
    cursor.execute("""update companies_house_sic_pool
     set FilePath = regexp_substr(FilePath, '[0-9]{4}-[0-9]{2}-[0-9]{2}', 1)
    """)
    ppdb.commit()
    # add parsing to sic_count counts with a truncate
    cursor.execute("""
    insert into companies_house_sic_counts (sic_code, sic_code_count, file_date, md5_str)
                        select SicText_1, count(*), Filepath,
                        md5_str from companies_house_sic_pool
                        group by SicText_1, Filepath
                    union
                        select SicText_2, count(*), Filepath,
                        md5_str from companies_house_sic_pool
                        group by SicText_2, Filepath
                    union
                        select SicText_3, count(*), Filepath,
                        md5_str from companies_house_sic_pool
                        group by SicText_3, Filepath
                    union
                        select SicText_4, count(*), Filepath,
                        md5_str from companies_house_sic_pool
                        group by SicText_4, Filepath
                    on duplicate key update 
                     companies_house_sic_counts.md5_str = companies_house_sic_counts.md5_str""")
    ppdb.commit()
    cursor.execute("""truncate table companies_house_sic_pool""")
    ppdb.commit()


@timer
def parse_fragment_sic(fragment, user, passwd, host, db, cursor, ppdb):
    """
    rewrite into new stages
    1. write into table where are sic codes are in one column
    2. apply md5 of concat(sic_code, filepath)
    3. insert into final table (sic code, file path, count, md5)
    """
    cursor.execute("""truncate companies_house_sic_pool""")
    ppdb.commit()
    constring = f'mysql://{user}:{passwd}@{host}:3306/{db}'
    df = pd.read_csv(fragment, usecols=[' CompanyNumber', 'SICCode.SicText_1', 'SICCode.SicText_2', 'SICCode.SicText_3',
                                        'SICCode.SicText_4'])
    df.rename(columns=companies_house_conversion_dict, inplace=True)
    df['FilePath'] = fragment + 'HISTORICAL'
    fragment_path = fragment + 'HISTORICAL'
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
    ppdb.commit()
    cursor.execute("""update companies_house_sic_pool
     set FilePath = regexp_substr(FilePath, '[0-9]{4}-[0-9]{2}-[0-9]{2}', 1)
    """)
    ppdb.commit()
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
    ppdb.commit()
    # truncate sic_pool table as data no longer required
    cursor.execute("""truncate table companies_house_sic_pool""")
    ppdb.commit()
    # once in staging_1, they are given an md5
    cursor.execute("""
    update companies_house_sic_code_staging_1 
    set md5_str = md5(concat(sic_code, file_date))
    where md5_str is null""")
    ppdb.commit()
    # inserted into second staging table, where on a duplicate update the two
    # counts for that sic code are added together
    # sic_code_staging_2 houses the counts for the CH file being processed at that time. Once complete, these will
    # all be moved to the companies_house
    cursor.execute("""insert into companies_house_sic_code_staging_2 (sic_code, file_date,  sic_code_count, md5_str)
        select sic_code, file_date, sic_code_count, md5_str from companies_house_sic_code_staging_1 t2
        on duplicate key update
        companies_house_sic_code_staging_2.sic_code_count = companies_house_sic_code_staging_2.sic_code_count + t2.sic_code_count
        """)
    ppdb.commit()
    # truncate staging_1 as it is no longer required
    cursor.execute("""truncate table companies_house_sic_code_staging_1""")


@timer
def parse_rchis_sic(cursor, ppdb):
    cursor.execute("""insert into companies_house_sic_pool (CompanyNumber, SicText_1, SicText_2, SicText_3, SicText_4, FilePath, md5_str) SELECT 
        company_number, sic_text_1, sic_text_2, SICCode_SicText_3, SICCode_SicText_4, SourceFile, md5_key from raw_companies_house_input_stage
        """)
    ppdb.commit()

    # remove the text from the sic codes so we are just left with numbers
    cursor.execute("""update companies_house_sic_pool
    set
        SicText_1 = SUBSTRING_INDEX(SicText_1, '-', 1),
        SicText_2 = SUBSTRING_INDEX(SicText_2, '-', 1),
        SicText_3 = SUBSTRING_INDEX(SicText_3, '-', 1),
        SicText_4 = SUBSTRING_INDEX(SicText_4, '-', 1)""")
    ppdb.commit()
    cursor.execute("""update companies_house_sic_pool
         set FilePath = regexp_substr(FilePath, '[0-9]{4}-[0-9]{2}-[0-9]{2}', 1)
        """)
    ppdb.commit()
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
    ppdb.commit()
    # truncate sic_pool table as data no longer required
    cursor.execute("""truncate table companies_house_sic_pool""")
    ppdb.commit()
    # once in staging_1, they are given an md5
    cursor.execute("""
        update companies_house_sic_code_staging_1 
        set md5_str = md5(concat(sic_code, file_date))
        where md5_str is null""")
    ppdb.commit()
    # inserted into second staging table, where on a duplicate update the two
    # counts for that sic code are added together
    # sic_code_staging_2 houses the counts for the CH file being processed at that time. Once complete, these will
    # all be moved to the companies_house
    cursor.execute("""insert into companies_house_sic_code_staging_2 (sic_code, file_date,  sic_code_count, md5_str)
            select sic_code, file_date, sic_code_count, md5_str from companies_house_sic_code_staging_1 t2
            on duplicate key update
            companies_house_sic_code_staging_2.sic_code_count = companies_house_sic_code_staging_2.sic_code_count + t2.sic_code_count
            """)
    ppdb.commit()
    # truncate staging_1 as it is no longer required
    cursor.execute("""truncate table companies_house_sic_code_staging_1""")


def _parse_fragment(fragment, host, user, passwd, db, cursor, cursordb, company_file_table):
    constring = f'mysql://{user}:{passwd}@{host}:3306/{db}'
    dbEngine = sqlalchemy.create_engine(constring)

    df = pd.read_csv(fragment, encoding='utf-8', dtype=companies_house_file_data_types, low_memory=False,
                     # usecols=['company_number', 'company_name', 'sic_text_1', 'sic_text_2', 'SICCode_SicText_3', 'SICCode_SicText_4']
                     )
    # df.rename(columns=dtype_dict_comp, inplace=True)
    cursor.execute("""truncate raw_companies_house_input_stage_df""")
    cursordb.commit()
    # may change to this line after deprecations
    # df = df.set_axis(dtype_dict_columns_output, copy=False, axis=1)
    # df.set_axis(dtype_dict_columns_output, inplace=True, axis=1)
    df.to_sql(name=f'BasicCompanyData_sic_data_{company_file_table}', con=dbEngine, if_exists='append', index=False,
              schema='iqblade')
    cursordb.commit()


@timer
def write_to_organisation(cursor, db):
    cursor.execute("""insert into organisation (id, company_name, company_number, company_status, country, date_formed, last_modified_by )
select rchis.organisation_id, rchis.company_name, rchis.company_number, rchis.company_status, 'United Kingdom', rchis.IncorporationDate, 'Rory' from raw_companies_house_input_stage rchis
left join organisation o on rchis.organisation_id = o.id
where o.id is null and rchis.reg_address_postcode is not null
;""")
    db.commit()
