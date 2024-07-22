from utils import pipeline_message_wrap, find_previous_month, logger, timer, connect_preprod
import datetime


@timer
def sic_code_db_insert(cursor, db) -> None:
    """iterate over other sic code columns,
    and for each column, insert the contents of the rchis table"""
    sic_code_columns = ['sic_text_1', 'sic_text_2', 'SICCode_SicText_3', 'SICCode_SicText_4']
    for column in sic_code_columns:
        logger.info(f'inserting {column}')
        sql_insert_query = f"""
        insert ignore into sic_code
         (code, organisation_id, company_number, md5, date_last_modified)
        select 
        regexp_substr({column}, '[0-9]+'), 
        concat('UK', company_number), 
        rchis.company_number,
        MD5(CONCAT(rchis.company_number, regexp_substr({column}, '[0-9]+'))), 
        CURDATE()
        from raw_companies_house_input_stage rchis
        where {column} is not null
          on duplicate key update 
          date_last_modified = curdate()"""
        cursor.execute(sql_insert_query)
        db.commit()


def insert_sic_codes_into_sic_code_counts(cursor, db):
    """
    why can't we just insert sums into sic_code_aggregates by using the sum() function in sic_code and
    ignoring duplicate md5s?
    :param cursor:
    :param db:
    :return:
    """

    cursor.execute("""
    insert ignore into companies_house_sic_code_aggregates (file_date, Category, count, md5_str)
select file_date, sic_code_category, sum(counts),
       md5(concat(file_date, sic_code_category))
from companies_house_sic_counts group by file_date, sic_code_category
    """)


@timer
def load_calculations(cursor, db,
                      current_month=datetime.datetime.now().month,
                      current_year=datetime.datetime.now().year) -> None:
    """sql query that takes two different months and calculates the difference between them"""

    # use current month and current year arg to find previous month and current/previous year
    print('calling load_calculations')
    previous_month, previous_year = find_previous_month(month=current_month, year=current_year)
    cursor.execute("""
    insert into companies_house_sic_code_analytics 
    (sic_code, first_month, second_month, first_month_count, second_month_count, diff, pct_change, md5_str, last_modified_by, last_modified_date) 
        select t1.sic_code,
       t1.file_date as first_month,
       t2.file_date as second_month,
       t1.counts as `first_month_count`,
       t2.counts as `second_month_count`,
       (t2.counts - t1.counts) as diff,
       100*(t2.counts-t1.counts)/t2.counts as pct_change,
       md5(concat(t1.sic_code, t1.file_date, t2.file_date)) as md5_str,
       'load_calculations_insert',
       now()
       from (
            select sic_code, counts, file_date from
             companies_house_sic_counts where month(file_date) = %s and year(file_date) = %s) t1
             inner join (
             select sic_code, counts, file_date from companies_house_sic_counts
             where month(file_date) = %s and year(file_date) = %s) t2
             on t1.sic_code = t2.sic_code
             order by sic_code
             on duplicate key update 
             companies_house_sic_code_analytics.diff = (t2.counts - t1.counts),
             companies_house_sic_code_analytics.first_month = t1.file_date,
             companies_house_sic_code_analytics.second_month = t2.file_date,
             companies_house_sic_code_analytics.first_month_count = t1.counts,
             companies_house_sic_code_analytics.second_month_count = t2.counts,
             companies_house_sic_code_analytics.sic_code = t1.sic_code,
             companies_house_sic_code_analytics.pct_change = 100*(t2.counts-t1.counts)/t2.counts,
             companies_house_sic_code_analytics.last_modified_date = now(),
             companies_house_sic_code_analytics.last_modified_by = 'load calculations update'
             """, (previous_month, previous_year, current_month, current_year))
    db.commit()
    print('calculations loaded for {}/{} and {}/{}'.format(previous_month, previous_year, current_month, current_year))


def load_calculations_aggregates(cursor, db,
                                 current_month=datetime.datetime.now().month,
                                 current_year=datetime.datetime.now().year) -> None:
    """
    select current month and previous month, and collect the sic code aggregate data for these months and compare them
    :param cursor:
    :param db:
    :param current_month:
    :param current_year:
    :return:
    """
    # use current month and current year arg to find previous month and current/previous year
    previous_month, previous_year = find_previous_month(month=current_month, year=current_year)

    cursor.execute("""
    insert into companies_house_sic_code_aggregate_analytics (sic_code_category, first_month, second_month, first_month_count, second_month_count, diff, pct_change, md5_str,
    last_modified_date, last_modified_by)
    select
        t1.Category,
        t1.file_date as first_month,
        t2.file_date as second_month,
        t1.count as first_month_count,
        t2.count as second_month_count,
        (t2.count - t1.count) as diff,
        100*(t2.count-t1.count)/t1.count as pct_change,
        md5(concat(t1.Category, t1.file_date, t2.file_date)) as md5_str,
        now(), 'aggregate insert'
    from (
        select Category, count, file_date
        from companies_house_sic_code_aggregates
        where month(file_date) = %s and
              year(file_date) = %s and Category is not null and Category not in ('', '-')) t1
    inner join (
        select Category, count, file_date
        from companies_house_sic_code_aggregates
        where month(file_date) = %s and
              year(file_date) = %s and Category is not null and Category not in ('', '-')) t2
    on t1.Category = t2.Category
    on duplicate key update 
    sic_code_category = t1.Category,
    first_month = t1.file_date,
    second_month = t2.file_date,
    first_month_count = t1.count,
    second_month_count = t2.count,
    diff = t2.count - t1.count,
    pct_change = 100*(t2.count-t1.count)/t1.count,
    last_modified_date = now(),
    last_modified_by = 'load aggregates update'
    """,
                   (previous_month, previous_year, current_month, current_year))
    db.commit()
    cursor.execute("""
        update companies_house_sic_code_aggregate_analytics scaa
    inner join sic_code_categories scc
    on scc.`SIC Code` = scaa.sic_code_category
    set scaa.category_description = scc.Category_Description
    where scaa.category_description is null""")
    db.commit()



@timer
def find_more_postcodes(cursor, db):
    """
        mysql query that finds companies house records without a value in their postcode column
    uses a regexp_substr function to find a postcode in a concat of the rest of the address. Sometimes
    the postcode can be found in there. If found, update the record with the new postcode
    # todo can this be done in pandas in section1?
    :param cursor:
    :param db:
    :return:
    """
    logger.info('find_more_postcodes called')
    cursor.execute("""update raw_companies_house_input_stage
    set reg_address_postcode = REGEXP_SUBSTR(concat(reg_address_line1, ' ', reg_address_line2
    , ' ', reg_address_posttown,' ', reg_address_county), '[A-Z]{1,2}[0-9]+ +[0-9]+[A-Z]+')
        where reg_address_postcode is null
          and REGEXP_SUBSTR(concat(reg_address_line1, ' ', reg_address_line2
    , ' ', reg_address_posttown,' ', reg_address_county), '[A-Z]{1,2}[0-9]+ +[0-9]+[A-Z]+') is not null;
    """)
    db.commit()


# GEOLOCATION
@timer
def geolocation_md5_gen(cursor, db):
    """
    Generate md5 hash in the companies house staging table
    We use organisation_id, postcode, country and address_type because
    address_1 and address_2 can be null values
    as of 29/09/23 837k rows don't have address_1, 2.9m don't have address_2 and 800k have neither
    (across all countries)
    for uk this is 115, 21k
    :param cursor:
    :param db:
    :return:
    """
    cursor.execute(
        """update raw_companies_house_input_stage set reg_address_postcode = '' where reg_address_postcode is null""")
    db.commit()
    cursor.execute("""update raw_companies_house_input_stage
    set md5_key = MD5(CONCAT(organisation_id, reg_address_postcode)) where md5_key is null """)
    db.commit()


@timer
def geo_location_remove_old_head_offices(cursor, db):
    """
    rank geo_location head offices by their ID, for countries with more than one head_office in geo_location

    """
    cursor.execute("""
    delete from geo_location where id in (
select id from (
    select id, organisation_id, rank() over (partition by organisation_id order by id desc) as id_rank
    from geo_location
    where address_type = 'HEAD_OFFICE' and country = 'UK'
              ) t1
where id_rank > 1
);
    """)
    db.commit()


@timer
def geolocation_upsert(cursor, db):
    """
    upsert from rchis into geo_location
    on duplicate key we set address type as head_office
    :param cursor:
    :param db:
    :return:
    """
    logger.info('geolocation_upsert called')
    # this pulls an Integrity Error: 1452 - cannot add or update a child row, how to bypass?
    # being review by Niall, sent 04/01/24
    cursor.execute("""
    insert ignore into geo_location
        (address_1,
         address_2,
         town,
         county,
         post_code,
         area_location,
         country,
         address_type,
         post_code_formatted,
         organisation_id,
         md5_key,
         date_last_modified,
         last_modified_by)
        select
        reg_address_line1,
        reg_address_line2,
        reg_address_posttown,
        reg_address_county,
        reg_address_postcode,
        reg_address_county,
        'UK',
        'HEAD_OFFICE',
        LOWER(REPLACE(reg_address_postcode, ' ', '')), -- postcode formatted
        CONCAT('UK', company_number), -- organisation_id
        md5(concat(rchis.organisation_id, reg_address_postcode)),  -- md5 key
        curdate(), -- date_last_modified
        'chp section 3 - geo_location_insert' -- last_modified_by
        from raw_companies_house_input_stage rchis

        -- if a duplicate is found, ensure gl record is HEAD_OFFICE and lmb and dlm are updated for rowcounts
        on duplicate key update
        address_type = 'HEAD_OFFICE',
        last_modified_by = 'chp section3 geo_location_upsert',
        date_last_modified = CURDATE()
        """)
    db.commit()


def check_for_section3_parsable() -> bool:
    """
    uses the filetracker to determine whether the latest file has been put through
    section3.py
    ideally we would have a way to determine which file needs to be put in, however we will
    only have the current file on hand.
    :return:
    """
    cursor.execute("""
    select * from companies_house_filetracker
     where
      section3 is null and 
      section2 is not null -- to ensure that section3 isn't performed before section 2
    """)
    res = cursor.fetchall()
    if len(res) != 0:
        return True
    else:
        return False


@timer
def add_counties(cursor, db):
    """
    joins rchis data with postcode data
    1. insert into separate table (t2) using a select query
    2. upsert t2 data on rchis, joining on organisation_id
    3. truncate t2
    :param cursor:
    :param db:
    :return:
    """
    # step 1:
    cursor.execute("""
        insert into raw_companies_house_input_counties_to_update
        select distinct
            rchis.organisation_id,
            pcms.County as new_county
        from raw_companies_house_input_stage rchis
        inner join post_code_mappings_staging pcms
        on replace(rchis.reg_address_postcode, ' ', '') = replace(pcms.Postcode, ' ', '')
        where rchis.reg_address_county is null and pcms.County is not null and reg_address_postcode <> ''""")
    db.commit()

    # step 2:
    cursor.execute("""
        update raw_companies_house_input_stage rchis
        inner join raw_companies_house_input_counties_to_update rnctu on rchis.organisation_id = rnctu.organisation_id
        set rchis.reg_address_county = new_county where reg_address_county is null
    """)
    db.commit()

    # step 3:
    cursor.execute("""truncate table raw_companies_house_input_counties_to_update""")
    db.commit()


@timer
def process_section3_geolocation(cursor, db):
    # work in geolocation
    # updates rchis with new postcodes
    find_more_postcodes(cursor, db)
    # update rchis and add counties
    add_counties(cursor, db)
    # create md5 hash for geo_location
    geolocation_md5_gen(cursor, db)
    # upsert geo_location with rchis data
    geolocation_upsert(cursor, db)
    # delete old head_offices
    geo_location_remove_old_head_offices(cursor, db)


@timer
# todo is this needed? section one processes into sic_code_counts and sic_code_aggregates
def process_section3_siccode(cursor, db) -> None:
    # work in sic_code

    # previous_month = datetime.datetime.now() - datetime.timedelta(days=30)
    current_month = datetime.datetime.now().month
    current_year = datetime.datetime.now().year
    # previous_month, previous_year = find_previous_month(month=current_month, year=current_year)

    # loop of statements that writes sic codes from staging into sic_codes table
    sic_code_db_insert(cursor, db)

    # calculate differences between sic code counts from the current month with counts from the previous month
    load_calculations(current_month=current_month, current_year=current_year, cursor=cursor, db=db)

    # calculate aggregate calculations
    load_calculations_aggregates(cursor, db)


@timer
def process_section3_organisation(cursor, db) -> None:
    """
    1. update statement on organisation, pairing with rchis on org_id, to change company names
    :param cursor:
    :param db:
    :return:
    """

    # insert any new companies in as well
    cursor.execute("""insert into organisation 
    (id, company_number, 
    company_name, company_status,
     date_formed, last_modified_date,
    last_modified_by)
    select 
    organisation_id,
    company_number, 
    company_name, 
    company_status, 
    STR_TO_DATE(IncorporationDate, '%d/%m/%Y') as date_formed, 
    curdate() as last_modified_date,
     'ch3 upsert statement - Rory (new insert)' as last_modified_by
     from raw_companies_house_input_stage on duplicate key update
     last_modified_date = curdate(), last_modified_by='ch3 upsert statement - Rory (duplicate key)'
    """)
    db.commit()


@pipeline_message_wrap
@timer
def process_section3(cursor, db) -> None:
    """
    1. check that functions need to be run for latest file by checking iqblade.companies_house_filetracker
        for rows that have section2 complete but not section3
    2.
    :param cursor:
    :param db:
    :return:
    """
    # todo check that there is a section2 file that needs to be handled

    # work in organisation table
    process_section3_organisation(cursor, db)

    # work in sic_codes
    process_section3_siccode(cursor, db)

    # work in geolocation
    process_section3_geolocation(cursor, db)


def _retro_update_sic_code_analytics(cursor, db) -> None:
    """
    a function to add analytics data for a given pair of consecutive dates
    labelled retro to imply it's use case for previous months
    :param cursor:
    :param db:
    :return:
    """
    logger.info(f'reto_caller updated')
    years = ['2020', '2021', '2022', '2023', '2024']
    for year in years:
        logger.info(f'starting year {year}')
        for i in range(1, 12):
            logger.info(f'starting month {i} for year {year}')
            load_calculations(current_month=i, current_year=year, cursor=cursor, db=db)


def _retro_update_sic_code_aggregates(cursor, db) -> None:
    """
    add aggregate analytics data for a given pair of consecutive dates
    labelled retro to imply it's use case for previous months
    :param cursor:
    :param db:
    :return:
    """
    logger.info(f'reto_caller updated')
    years = ['2020', '2021', '2022', '2023', '2024']
    for year in years:
        logger.info(f'starting year {year}')
        for i in range(1, 12):
            logger.info(f'starting month {i} for year {year}')
            load_calculations_aggregates(current_month=i, current_year=int(year), cursor=cursor, db=db)


def rowcount_sic_codes(cursor, db):
    """
    updates companies_house row counts.
    sets the rows_changed_in_siccode equal to the number of rows in sic_code
     that have been updated on the current date.

     sic_codes does not have a date_last_modified.

    :param cursor:
    :param db:
    :return:
    """
    # identify potential sic_code entries
    cursor.execute("""
    update companies_house_rowcounts set potential_sic_code_entries = (
select count(*) from (
select TRIM(SUBSTRING_INDEX(sic_text_1,'-',1)) as code,
       concat('UK', company_number),
       rchis.company_number as company_number,
       MD5(CONCAT(rchis.company_number, TRIM(SUBSTRING_INDEX(sic_text_1,'-',1)))) as md5,
       CURDATE() as date_last_modified
from raw_companies_house_input_stage rchis
union
select TRIM(SUBSTRING_INDEX(sic_text_2,'-',1)) as code,
       concat('UK', company_number),
       rchis.company_number as company_number,
       MD5(CONCAT(rchis.company_number, TRIM(SUBSTRING_INDEX(sic_text_2,'-',1)))) as md5,
       CURDATE() as date_last_modified
from raw_companies_house_input_stage rchis
union
select TRIM(SUBSTRING_INDEX(SICCode_SicText_3,'-',1)) as code,
       concat('UK', company_number),
       rchis.company_number as company_number,
       MD5(CONCAT(rchis.company_number, TRIM(SUBSTRING_INDEX(SICCode_SicText_3,'-',1)))) as md5,
       CURDATE() as date_last_modified
from raw_companies_house_input_stage rchis
union
select TRIM(SUBSTRING_INDEX(SICCode_SicText_4,'-',1)) as code,
       concat('UK', company_number),
       rchis.company_number as company_number,
       MD5(CONCAT(rchis.company_number, TRIM(SUBSTRING_INDEX(SICCode_SicText_4,'-',1)))) as md5,
       CURDATE() as date_last_modified
from raw_companies_house_input_stage rchis) t1)""")
    db.commit()
    cursor.execute("""
    update companies_house_rowcounts set rows_changed_in_siccode = (
    select count(*) from sic_code
    where MONTH(date_last_modified) = MONTH(CURDATE()) and
      YEAR(date_last_modified) = YEAR(CURDATE()))
    where month(file_month) = month(curdate()) and year(file_month) = year(curdate())
       """)
    db.commit()


def rowcount_geo_location(cursor, db):
    """
    updates companies_house row counts.
    sets the rows_changed_in_geolocation equal to the number of rows in sic_code
     that have been updated on the current date.
    :param cursor:
    :param db:
    :return:
    """
    cursor.execute("""
    update companies_house_rowcounts
    set 
    rows_changed_in_geolocation = (
    select count(*)
     from iqblade.geo_location
      where month(date_last_modified) = month(curdate()) and
      year(date_last_modified) = year(curdate())
      and country = 'UK' and address_type = 'HEAD_OFFICE'
    )
    where month(file_month) = month(curdate()) and
    year(file_month) = year(curdate())
    """)
    db.commit()


def rowcount_organisation(cursor, db):
    """
    updates companies_house row counts.
    sets the rows_changed_in_org equal to the number of rows in sic_code
     that have been updated on the current date.
    :param cursor:
    :param db:
    :return:
    """
    cursor.execute("""
    update companies_house_rowcounts
    set rows_changed_in_org = (
    select count(*)
    from iqblade.organisation
    where country = 'UK' and
    month(last_modified_date) = month(curdate()) and
      year(last_modified_date) = year(curdate())
    )
    """)
    db.commit()


def post_rowcount_update_calculations(cursor, db):
    # find difference (A) by subtracting the number of rows affected by latest file (B) from the size of the file in rows (C)
    # A = C-B
    # find percentage change (D)
    # D = (A/B)*100
    cursor.execute(
        """
update companies_house_rowcounts
set
    geolocation_diff = file_rowcount - rows_changed_in_geolocation,
    organisation_diff = file_rowcount - rows_changed_in_org,
    sic_code_diff = file_rowcount - rows_changed_in_siccode
    where MONTH(file_month) = month(curdate()) and year(file_month) - year(curdate())
        """
    )
    db.commit()



if __name__ == '__main__':
    cursor, db = connect_preprod()
    bool_check = check_for_section3_parsable()
    bool_check = True
    if bool_check:
        process_section3(cursor, db)
        cursor.execute("""update companies_house_filetracker set section3 = DATE(CURDATE()) where section3 is null""")
        db.commit()
    else:
        logger.info('no section 3 required')
        quit()
