import datetime

from fragment_work import parse_rchis_sic
from utils import pipeline_message_wrap
from main_funcs import *

previous_month = datetime.datetime.now() - datetime.timedelta(days=30)


@pipeline_message_wrap
@timer
def process_section3(cursor, db):
    add_organisation_id(cursor, db)  # adds organisation_id to the staging table rchis
    update_org_name(cursor, db)  # update statement on organisation, pairing with rchis on org_id
    update_org_website(cursor, db)  # update statement on organisation, pairing with rchis on org_id
    update_org_activity(cursor, db)  # update statement on organisation, pairing with rchis on org_id
    write_to_org(cursor, db)  # insert statement into organisation, pairing with rchis on org_id
    sic_code_db_insert(cursor, db)  # loop of statements that writes sic codes from staging into sic_codes table
    insert_sic_counts(month=datetime.date.today().month, cursor=cursor, db=db)
    load_calculations(first_month=previous_month.month, second_month=datetime.date.today().month, cursor=cursor, db=db)
    find_more_postcodes(cursor, db)  # updates rchis
    geolocation_update_current(cursor, db)  # update geo_location
    geolocation_insert_excess(cursor, db)  # insert geolocation data
    parse_rchis_sic(cursor=cursor, ppdb=db)  # insert sic data for sic_code_analytics


if __name__ == '__main__':
    cursor, db = connect_preprod()
    process_section3(cursor, db)
    cursor.execute("""update companies_house_filetracker set section3 = TRUE where filename = %s""", ('', ))
    db.commit()
