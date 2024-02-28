from section_3_geo_location_funcs import *
from section_3_sic_code_funcs import *
from utils import pipeline_message_wrap, find_previous_month, logger


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
        insert into rchis_new_counties_to_update
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
        inner join rchis_new_counties_to_update rnctu on rchis.organisation_id = rnctu.organisation_id
        set rchis.reg_address_county = new_county where reg_address_county is null
    """)
    db.commit()

    # step 3:
    cursor.execute("""truncate table rchis_new_counties_to_update""")
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
    previous_month, previous_year = find_previous_month(month=current_month, year=current_year)
    # todo add year arg, which will change based on whether or not the current month is january

    # loop of statements that writes sic codes from staging into sic_codes table
    sic_code_db_insert(cursor, db)

    # calculate differences between sic code counts from the current month with counts from the previous month
    load_calculations(current_month=current_month, current_year=current_year, cursor=cursor, db=db)

    # calculate aggregate calculations
    load_calculations_aggregates(cursor, db)


@timer
def process_section3_organisation(cursor, db) -> None:
    """
    1. adds organisation_id to the staging table rchis
    2. update statement on organisation, pairing with rchis on org_id, to change company names
    :param cursor:
    :param db:
    :return:
    """
    # 1. adds organisation_id to the staging table rchis
    cursor.execute("""update raw_companies_house_input_stage
     set 
     organisation_id = CONCAT('UK', company_number) where organisation_id is null""")
    db.commit()

    # 2. update statement on organisation, pairing with rchis on org_id, to change company names
    cursor.execute("""update organisation o
                        inner join raw_companies_house_input_stage rchis on o.company_number = rchis.company_number
                        set 
                        o.company_name = rchis.company_name,
                        o.company_status = rchis.company_status,
                        last_modified_by = 'companies house section3',
                        last_modified_date = CURDATE()
                         where o.company_name <> rchis.company_name and o.company_number = rchis.company_number""")
    db.commit()

    # insert any new companies in as well
    cursor.execute("""insert into organisation (id, company_number, company_name, company_status, date_formed, last_modified_date,
    last_modified_by)
    select organisation_id, company_number, company_name, company_status, STR_TO_DATE(IncorporationDate, '%d/%m/%Y'), curdate(), 'ch3 upsert statement - Rory'
     from raw_companies_house_input_stage on duplicate key update
     last_modified_date = curdate(), last_modified_by='ch3 upsert statement - Rory'
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


def retro_update_sic_code_analytics(cursor, db) -> None:
    logger.info(f'reto_caller updated')
    years = ['2020', '2021', '2022', '2023', '2024']
    for year in years:
        logger.info(f'starting year {year}')
        for i in range(1, 12):
            logger.info(f'starting month {i} for year {year}')
            load_calculations(current_month=i, current_year=year, cursor=cursor, db=db)


def retro_update_sic_code_aggregates(cursor, db) -> None:
    logger.info(f'reto_caller updated')
    years = ['2020', '2021', '2022', '2023', '2024']
    for year in years:
        logger.info(f'starting year {year}')
        for i in range(1, 12):
            logger.info(f'starting month {i} for year {year}')
            load_calculations_aggregates(current_month=i, current_year=year, cursor=cursor, db=db)


if __name__ == '__main__':
    cursor, db = connect_preprod()
    bool_check = check_for_section3_parsable()
    if bool_check:
        process_section3(cursor, db)
        cursor.execute("""update companies_house_filetracker set section3 = DATE(CURDATE()) where section3 is null""")
        db.commit()
    else:
        logger.info('no section 3 required')
        quit()
