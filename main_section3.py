import datetime

from utils import pipeline_message_wrap, checkcount_organisation_inserts, connect_preprod, connect_preprod_readonly
from main_funcs import *
from section_3_sic_code_funcs import *
from section_3_geo_location_funcs import *

previous_month = datetime.datetime.now() - datetime.timedelta(days=30)


@pipeline_message_wrap
@timer
def process_section3(cursor, db):
    # take count of rows in rchis

    # work in organisation table
    add_organisation_id(cursor, db)  # adds organisation_id to the staging table rchis
    update_org_name(cursor, db)  # update statement on organisation, pairing with rchis on org_id
    update_org_website(cursor, db)  # update statement on organisation, pairing with rchis on org_id
    update_org_activity(cursor, db)  # update statement on organisation, pairing with rchis on org_id
    write_to_org(cursor, db)  # insert statement into organisation, pairing with rchis on org_id

    # work in sic_code
    sic_code_db_insert(cursor, db)  # loop of statements that writes sic codes from staging into sic_codes table
    insert_sic_counts(month=datetime.date.today().month, cursor=cursor, db=db)
    load_calculations(first_month=previous_month.month, second_month=datetime.date.today().month, cursor=cursor, db=db)

    # work in geolocation
    find_more_postcodes(cursor, db)  # updates rchis
    geolocation_md5_gen(cursor, db)  # generates md5 for companies
    geolocation_upsert(cursor, db)  # update geo_location
    geo_location_remove_old_head_offices(cursor, db)  # insert geolocation data

    # write a record of counts in order to


@timer
def process_section3_geolocation(cursor, db):
    # work in geolocation
    find_more_postcodes(cursor, db)  # updates rchis
    geolocation_md5_gen(cursor, db)  # generates md5 for companies
    geolocation_upsert(cursor, db)  # update geo_location
    geo_location_remove_old_head_offices(cursor, db)  # insert geolocation data

# todo check counts for sic_code and geo_location

if __name__ == '__main__':
    cursor, db = connect_preprod()
    process_section3_geolocation(cursor, db)
    cursor.execute("""update companies_house_filetracker set section3 = TRUE where filename = %s""", ('',))
    db.commit()
    # readonly_cursor = connect_preprod_readonly()
    # checkcount_organisation_inserts(cursor=readonly_cursor)
