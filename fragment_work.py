import datetime
import time
import os

import pandas as pd

from locker import *
from utils import timer, logger


@timer
def parse_fragment(fragment: str,
                   cursor, cursordb,
                   host: str = os.environ.get('PREPRODHOST'),
                   user: str = os.environ.get('ADMINUSER'),
                   passwd: str = os.environ.get('ADMINPASS'),
                   db: str = os.environ.get('DATABASE')
                   ):
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
    df_to_sql_t1 = time.time()
    df.to_sql(name='raw_companies_house_input_stage_df', con=constring, if_exists='append',
              index=False)
    df.fillna(' ')


@timer
def parse_fragment_sic(fragment, user, passwd, host, constring_database, cursor, db):
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
    df.rename(columns=sic_code_conversion_dict, inplace=True)
    df['FilePath'] = fragment + 'HISTORICAL'
    df.to_sql(name='companies_house_sic_pool', con=constring, if_exists='append',
              index=False)
    logger.info(f'{fragment} inserted into sic_pool')

    logger.info(f'{fragment} substring_index called')
    # remove the text from the sic codes so we are just left with numbers
    cursor.execute("""update companies_house_sic_pool
    set
    SicText_1 = regexp_substr(SicText_1, '[0-9]+'),
    SicText_2 = regexp_substr(SicText_2, '[0-9]+'),
    SicText_3 = regexp_substr(SicText_3, '[0-9]+'),
    SicText_4 = regexp_substr(SicText_4, '[0-9]+')
    """)
    db.commit()

    logger.info(f'{fragment} updating file_paths')
    cursor.execute("""update companies_house_sic_pool
     set FilePath = regexp_substr(FilePath, '[0-9]{4}-[0-9]{2}-[0-9]{2}', 1)
    """)
    db.commit()

    # takes records from the sic pool as a fragment is upload to sic_pool
    # writes them into staging 1
    logger.info(f'{fragment} union insert into staging_1')
    cursor.execute("""
    insert into companies_house_sic_code_staging_1 (sic_code, sic_code_count, file_date)
    select sic_code, sum(sic_count), FilePath from 
        (select SicText_1 as sic_code, count(*) as sic_count, Filepath
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
        group by SicText_4, Filepath)
         t1 group by sic_code
     """)
    db.commit()
    cursor.execute("""truncate table companies_house_sic_pool""")
    db.commit()

    logger.info('applying md5 in staging_1')
    # once in staging_1, they are given an md5
    cursor.execute("""
    update companies_house_sic_code_staging_1 
    set md5_str = md5(concat(sic_code, file_date))
    where md5_str is null""")
    db.commit()

    # inserted into second staging table, duplicate md5s are +='d together
    # sic_code_staging_2 houses the counts for the CH file being processed at that time. Once complete, these will
    # all be moved to the companies_house_counts
    logger.info(f'inserting into staging_2')
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
    SicText_1 = regexp_substr(SicText_1, '[0-9]+'),
    SicText_2 = regexp_substr(SicText_2, '[0-9]+'),
    SicText_3 = regexp_substr(SicText_3, '[0-9]+'),
    SicText_4 = regexp_substr(SicText_4, '[0-9]+')
        """)
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
