from file_parser.utils import pipeline_messenger
from locker import connect_preprod

cursor, db = connect_preprod()


def sql_update_addresses_wmd5(cursor, db):
    # develop md5 for addresses in rchis
    cursor.execute("""update raw_companies_house_input_stage
    set md5_key = MD5(CONCAT(organisation_id, reg_address_line1)) where md5_key is null """)
    db.commit()
    # update current table
    cursor.execute("""update geo_location_insert_test gl inner join raw_companies_house_input_stage rchis on gl.organisation_id = rchis.organisation_id
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
    cursor.execute("""insert into geo_location_insert_test
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


def sql_sic(cursor, db):
    # for sic_text_1
    cursor.execute("""insert ignore into sic_code (code, organisation_id, company_number, md5)
select TRIM(SUBSTRING_INDEX(sic_text_1,'-',1)), organisation_id, company_number,
       MD5(CONCAT(company_number, TRIM(SUBSTRING_INDEX(sic_text_1,'-',1))))
from raw_companies_house_input_stage rchis
where TRIM(SUBSTRING_INDEX(sic_text_1,'-',1)) is not null and
      TRIM(SUBSTRING_INDEX(sic_text_1,'-',1)) <> '';""")
    db.commit()

    # for sic_text_2
    cursor.execute("""insert ignore into sic_code (code, organisation_id, company_number, md5)
select TRIM(SUBSTRING_INDEX(sic_text_2,'-',1)), organisation_id, company_number,
       MD5(CONCAT(company_number, TRIM(SUBSTRING_INDEX(sic_text_2,'-',1))))
from raw_companies_house_input_stage rchis
where TRIM(SUBSTRING_INDEX(sic_text_2,'-',1)) is not null and
      TRIM(SUBSTRING_INDEX(sic_text_2,'-',1)) <> '';""")
    db.commit()

    # for sic_text_3
    cursor.execute("""insert ignore into sic_code (code, organisation_id, company_number, md5)
select TRIM(SUBSTRING_INDEX(SICCode_SicText_3,'-',1)), organisation_id, company_number,
       MD5(CONCAT(company_number, TRIM(SUBSTRING_INDEX(SICCode_SicText_3,'-',1))))
from raw_companies_house_input_stage rchis
where TRIM(SUBSTRING_INDEX(SICCode_SicText_3,'-',1)) is not null and
      TRIM(SUBSTRING_INDEX(SICCode_SicText_3,'-',1)) <> '';""")
    db.commit()

    cursor.execute("""insert ignore into sic_code (code, organisation_id, company_number, md5)
select TRIM(SUBSTRING_INDEX(SICCode_SicText_4,'-',1)), organisation_id, company_number,
       MD5(CONCAT(company_number, TRIM(SUBSTRING_INDEX(SICCode_SicText_4,'-',1))))
from raw_companies_house_input_stage rchis
where TRIM(SUBSTRING_INDEX(SICCode_SicText_4,'-',1)) is not null and
      TRIM(SUBSTRING_INDEX(SICCode_SicText_4,'-',1)) <> '';""")
    db.commit()

def post_update_activity(cursor, db):
    cursor.execute("""
    update organisation o
    inner join raw_companies_house_input_stage rchis
    on o.id = rchis.organisation_id
    set o.company_status = rchis.company_status""")

    db.commit()


def delete_from_org(cursor, db):
    # put together tuplelist of
    cursor.execute("""
    select o.id, o.company_name from organisation o 
    inner join raw_companies_house_input_stage rchis on o.id = rchis.organisation_id
    where rchis.reg_address_postcode is null and rchis.Accounts_AccountCategory = 'NO ACCOUNTS FILED' """)
    res = cursor.fetchall()
    for oid, cname in res:
        print(cname)
        cursor.execute("""select * from organisation_filing_history where organisation_id = %s""", (oid,))
        ofh_res = cursor.fetchall()
        if len(ofh_res) > 0:
            print(len(ofh_res), ' for ofh ' , oid)
        cursor.execute("""select * from organisation_funding_details where funded_organisation_id = %s""", (oid,))
        ofd_res = cursor.fetchall()
        if len(ofd_res) > 0:
            print(len(ofd_res), ' for ofh ' , oid)
        cursor.execute("""select * from organisation_merger_details where org_1_id = %s""", (oid,))
        omd_res = cursor.fetchall()
        if len(omd_res) > 0:
            print(len(omd_res), ' for omd ' , oid)
        cursor.execute("""select * from organisation_officer_appointment where organisation_id = %s""", (oid,))
        ooa_res = cursor.fetchall()
        if len(ooa_res) > 0:
            print(len(ooa_res), ' for ooa ' , oid)


def write_to_org(cursor, db):
    # insert into organisation as well
    cursor.execute("""insert ignore into organisation_insert_test (id, company_name,
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
                                (select id from organisation_insert_test where country = 'UNITED KINGDOM')""")
    db.commit()


def update_org_website(cursor, db):
    print('add websites')
    cursor.execute("""update organisation_insert_test o
                        inner join raw_companies_house_input_stage rchis on o.company_number = rchis.company_number
                        set o.website = rchis.URI where o.website is null and o.website <> ''""")
    db.commit()


def run_updates(cursor, db):
    try:
        write_to_org(cursor, db)
    except Exception as e:
        pipeline_title = 'Error on post_inserts_organsation'
        pipeline_message = f'write_to_org: {e}'
        pipeline_hexcolour = '#83eb34'
        pipeline_messenger(title=pipeline_title, text=pipeline_message, hexcolour=pipeline_hexcolour)
        pass
    try:
        update_org_website(cursor, db)
    except Exception as e:
        pipeline_title = 'Error on post_inserts_organsation'
        pipeline_message = f'update_org_website: {e}'
        pipeline_hexcolour = '#83eb34'
        pipeline_messenger(title=pipeline_title, text=pipeline_message, hexcolour=pipeline_hexcolour)
        pass

    pipeline_title = 'Organisation Updates complete'
    pipeline_message = f''
    pipeline_hexcolour = '#83eb34'
    pipeline_messenger(title=pipeline_title, text=pipeline_message, hexcolour=pipeline_hexcolour)


delete_from_org(cursor, db)
