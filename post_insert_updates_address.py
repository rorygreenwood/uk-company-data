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
         reg_address_postcode, reg_address_county, 'UK', 'HEAD_OFFICE',
          LOWER(REPLACE(reg_address_postcode, ' ', '')), CONCAT('UK', company_number)
        from raw_companies_house_input_stage where company_number = %s""", (cnum,))
        print('insert done')
        db.commit()
    rowcount = len(res)
    pipeline_title = 'Successful Parsing of Addresses '
    pipeline_message = f'total rows affected: {rowcount}'
    pipeline_hexcolour = '#83eb34'
    pipeline_messenger(title=pipeline_title, text=pipeline_message, hexcolour=pipeline_hexcolour)


def sql_update_addresses(cursor, db):
    cursor.execute("""insert ignore into geo_location_insert_test
        (address_1, address_2,
         town, county,
          post_code, area_location, country, address_type,
           post_code_formatted, organisation_id, md5_key)
        select
        reg_address_line1, reg_address_line2,
        reg_address_posttown, reg_address_county,
        reg_address_postcode, reg_address_county, 'UK', 'HEAD_OFFICE',
        LOWER(TRIM(reg_address_postcode)), CONCAT('UK', company_number), md5_key
        from raw_companies_house_input_stage
            on duplicate key update
            address_1 = reg_address_line1,
            address_2 = reg_address_line2,
            town = reg_address_posttown,
            county = reg_address_county,
            area_location = reg_address_county,
            post_code = reg_address_postcode,
            post_code_formatted = LOWER(TRIM(reg_address_postcode))""")
    db.commit()


def sql_update_addresses_wmd5(cursor, db):
    # develop md5 for addresses in rchis
    cursor.execute("""update raw_companies_house_input_stage
    set md5_key = MD5(CONCAT(organisation_id, reg_address_line1)) where md5_key is null """)
    db.commit()
    # update current table
    cursor.execute("""
    update geo_location gl inner join raw_companies_house_input_stage rchis on gl.organisation_id = rchis.organisation_id
            set gl.address_1 = rchis.reg_address_line1,
            gl.address_2 = rchis.reg_address_line2,
            gl.town = rchis.reg_address_posttown,
            gl.county = rchis.reg_address_county,
            gl.area_location = rchis.reg_address_county,
            gl.post_code = rchis.reg_address_postcode,
            gl.post_code_formatted = LOWER(TRIM(rchis.reg_address_postcode)),
            gl.md5_key = rchis.md5_key
            where gl.md5_key <> rchis.md5_key;""")
    db.commit()
    # insert remaining results
    # insert into organisation as well
    cursor.execute("""insert ignore into organisation (id, company_name,
                                 company_number,
                                 company_status, country,
                                 date_formed, last_modified_by,
                                 last_modified_date, country_code)
                        select CONCAT('UK', company_number),
                               company_name, company_number,
                               company_status,
                               'UK', STR_TO_DATE(IncorporationDate, '%d/%m/%Y'),
                               'Rory', CURDATE(), 'UK' from raw_companies_house_input_stage
                                where CONCAT('UK', company_number) not in
                                (select id from organisation where country = 'UNITED KINGDOM')""")
    db.commit()
    cursor.execute("""insert into geo_location
        (address_1, address_2,
         town, county,
          post_code, area_location, country, address_type,
           post_code_formatted, organisation_id, md5_key)
        select
        reg_address_line1, reg_address_line2
        , reg_address_posttown, reg_address_county,
         reg_address_postcode, reg_address_county, 'UK', 'HEAD_OFFICE',
          LOWER(REPLACE(reg_address_postcode, ' ', '')), CONCAT('UK', company_number), gl.md5_key
        from raw_companies_house_input_stage rchis
        left join geo_location gl on gl.organisation_id = rchis.organisation_id
        where gl.organisation_id is null""")
    db.commit()
    # cleanup

    cursor.execute("""""")
