import datetime
import time

from file_parser.utils import pipeline_messenger
from main_funcs import *
from file_parser.fragment_work import parse_rchis_sic

previous_month = datetime.datetime.now() - datetime.timedelta(days=30)
start_time = time.time()

add_organisation_id(cursor, db)  # adds organisation_id to the staging table rchis
update_org_name(cursor, db)  # update statement on organisation, pairing with rchis on org_id
update_org_website(cursor, db)  # update statement on organisation, pairing with rchis on org_id
update_org_activity(cursor, db)  # update statement on organisation, pairing with rchis on org_id
write_to_org(cursor, db)  # insert statement into organisation, pairing with rchis on org_id
sql_sic(cursor, db)  # loop of statements that writes sic codes from staging into sic_codes table
insert_sic_counts(month=datetime.date.today().month)
load_calculations(first_month=previous_month.month, second_month=datetime.date.today().month)
find_more_postcodes(cursor, db)  # updates rchis
geolocation_update_current(cursor, db)  # update geo_location
geolocation_insert_excess(cursor, db)  # insert geolocation data
parse_rchis_sic(cursor=cursor, ppdb=db)  # insert sic data for sic_code_analytics

end_time = time.time()
pipeline_time = end_time - start_time
pipeline_title = 'Companies House File loaded'
pipeline_message = f"""File Date:
Time taken on total pipeline: {pipeline_time}
"""
pipeline_hexcolour = '#00c400'
pipeline_messenger(title=pipeline_title, text=pipeline_message, hexcolour=pipeline_hexcolour)
