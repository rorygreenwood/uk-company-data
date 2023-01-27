# this will look over companies already on the platform without sic_codes or addresses and look the
# company up in companies house updating/inserting as they find companies without this info
# based on results from companies house
import re
from file_parser.utils import pipeline_messenger
from locker import connect_preprod

cursor, db = connect_preprod()


def mass_update_address(cursor, db):
    cursor.execute("""select o.id, o.company_number, gl.id from organisation o
    left join geo_location gl on o.id = gl.organisation_id
    inner join raw_companies_house_input_stage rchis on o.company_number = rchis.company_number
    where gl.id is null and o.country = 'United Kingdom'""")
    res = cursor.fetchall()
    for id, cnum, _ in res:
        print(cnum, id)
        cursor.execute("""insert into geo_location
        (address_1, address_2,
         town, county,
          post_code, area_location, country, address_type,
           post_code_formatted, organisation_id)
        select
        reg_address_line1, reg_address_line2
        , reg_address_posttown, reg_address_county,
         reg_address_postcode, reg_address_county, 'UK', 'HEAD OFFICE',
          LOWER(REPLACE(reg_address_postcode, ' ', '')), CONCAT('UK', company_number)
        from raw_companies_house_input_stage where company_number = %s""", (cnum,))
        print('insert done')
        db.commit()
    rowcount = len(res)
    pipeline_title = 'Successful Parsing of Addresses '
    pipeline_message = f'total rows affected: {rowcount}'
    pipeline_hexcolour = '#83eb34'
    pipeline_messenger(title=pipeline_title, text=pipeline_message, hexcolour=pipeline_hexcolour)


# mass_update_sic_codes(cursor, db)
mass_update_address(cursor, db)
