import datetime

from main_funcs import *
from section_3_geo_location_funcs import *
from section_3_sic_code_funcs import *
from utils import pipeline_message_wrap, find_previous_month


@timer
def process_section3_geolocation(cursor, db):
    # work in geolocation
    # updates rchis with new postcodes
    find_more_postcodes(cursor, db)
    # create md5 hash for geo_location
    geolocation_md5_gen(cursor, db)
    # upsert geo_location with rchis data
    geolocation_upsert(cursor, db)
    # delete old head_offices
    geo_location_remove_old_head_offices(cursor, db)


@timer
def process_section3_siccode(cursor, db):
    # work in sic_code
    # previous_month = datetime.datetime.now() - datetime.timedelta(days=30)
    current_month = datetime.datetime.now().month
    current_year = datetime.datetime.now().year
    previous_month, previous_year = find_previous_month(month=current_month, year=current_year)
    # todo add year arg, which will change based on whether or not the current month is january

    # loop of statements that writes sic codes from staging into sic_codes table
    sic_code_db_insert(cursor, db)

    # insert a count(*) of sic code data for the latest month, grouped by the sic code
    insert_sic_counts(month=datetime.date.today().month, cursor=cursor, db=db)

    # calculate differences between sic code counts from the current month with counts from the previous month
    load_calculations(first_month=previous_month, second_month=current_month, cursor=cursor, db=db)


@timer
def process_section3_organisation(cursor, db):
    # adds organisation_id to the staging table rchis
    add_organisation_id(cursor, db)

    # update statement on organisation, pairing with rchis on org_id
    update_org_name(cursor, db)

    # update statement on organisation, pairing with rchis on org_id
    update_org_activity(cursor, db)

    # insert statement into organisation, pairing with rchis on org_id
    write_to_org(cursor, db)


@pipeline_message_wrap
@timer
def process_section3(cursor, db):
    # work in organisation table
    process_section3_organisation(cursor, db)

    # work in sic_codes
    process_section3_siccode(cursor, db)

    # work in geolocation
    process_section3_geolocation(cursor, db)


# todo check counts for sic_code and geo_location
def retro_update_sic_code_analytics(cursor, db):
    logger.info(f'reto_caller updated')
    years = ['2020', '2021', '2022', '2023', '2024']
    for year in years:
        logger.info(f'starting year {year}')
        for i in range(1, 12):
            logger.info(f'starting month {i} for year {year}')
            load_calculations(current_month=i, current_year=year, cursor=cursor, db=db)


def retro_update_sic_code_aggregates(cursor, db):
    logger.info(f'reto_caller updated')
    years = ['2020', '2021', '2022', '2023', '2024']
    for year in years:
        logger.info(f'starting year {year}')
        for i in range(1, 12):
            logger.info(f'starting month {i} for year {year}')
            load_calculations_aggregates(current_month=i, current_year=year, cursor=cursor, db=db)


if __name__ == '__main__':
    cursor, db = connect_preprod()
    # process_section3(cursor, db)
    # cursor.execute("""update companies_house_filetracker set section3 = TRUE where filename = %s""", ('',))
    # db.commit()
    retro_update_sic_code_aggregates(cursor, db)
    retro_update_sic_code_analytics(cursor, db)