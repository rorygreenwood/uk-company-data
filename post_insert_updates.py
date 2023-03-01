from file_parser.utils import pipeline_messenger
from locker import connect_preprod
import logging

cursor, db = connect_preprod()

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s [line:%(lineno)d] %(levelname)s: %(message)s')
logger = logging.getLogger()


def sql_update_addresses_wmd5(cursor, db):
    # develop md5 for addresses in rchis
    cursor.execute("""update raw_companies_house_input_stage
    set md5_key = MD5(CONCAT(organisation_id, reg_address_postcode)) where md5_key is null """)
    db.commit()
    # update current table
    cursor.execute("""update geo_location gl inner join raw_companies_house_input_stage rchis on gl.organisation_id = rchis.organisation_id
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


def sql_sic(cursor, db):
    # for sic_text_1
    logger.info('starting sql_sic')
    cursor.execute("""insert into sic_code (code, organisation_id, company_number, md5)
select TRIM(SUBSTRING_INDEX(sic_text_1,'-',1)), o.id, rchis.company_number,
       MD5(CONCAT(rchis.company_number, TRIM(SUBSTRING_INDEX(sic_text_1,'-',1))))
from raw_companies_house_input_stage rchis
inner join (select * from organisation where country = 'united kingdom') o on rchis.organisation_id = o.id
left join sic_code sc on o.id = sc.organisation_id
where sc.id is null""")
    db.commit()

    # for sic_text_2
    cursor.execute("""insert ignore into sic_code (code, organisation_id, company_number, md5)
select TRIM(SUBSTRING_INDEX(sic_text_2,'-',1)), rchis.organisation_id, rchis.company_number,
       MD5(CONCAT(rchis.company_number, TRIM(SUBSTRING_INDEX(sic_text_2,'-',1))))
from raw_companies_house_input_stage rchis
    inner join organisation o on rchis.organisation_id = o.id
    left join sic_code sc on MD5(CONCAT(rchis.company_number, TRIM(SUBSTRING_INDEX(sic_text_2,'-',1)))) = sc.md5
where TRIM(SUBSTRING_INDEX(sic_text_2,'-',1)) is not null and
      TRIM(SUBSTRING_INDEX(sic_text_2,'-',1)) <> '' and sc.md5 is null;""")
    db.commit()

    # for sic_text_3
    cursor.execute("""insert ignore into sic_code (code, organisation_id, company_number, md5)
select TRIM(SUBSTRING_INDEX(SICCode_SicText_3,'-',1)), rchis.organisation_id, rchis.company_number,
       MD5(CONCAT(rchis.company_number, TRIM(SUBSTRING_INDEX(SICCode_SicText_3,'-',1))))
from raw_companies_house_input_stage rchis
inner join organisation o on rchis.organisation_id = o.id
    left join sic_code sc on MD5(CONCAT(rchis.company_number, TRIM(SUBSTRING_INDEX(SICCode_SicText_3,'-',1)))) = sc.md5
where TRIM(SUBSTRING_INDEX(SICCode_SicText_3,'-',1)) is not null and
      TRIM(SUBSTRING_INDEX(SICCode_SicText_3,'-',1)) <> '' and sc.md5 is null;""")
    db.commit()

    cursor.execute("""insert ignore into sic_code (code, organisation_id, company_number, md5)
select TRIM(SUBSTRING_INDEX(SICCode_SicText_4,'-',1)), rchis.organisation_id, rchis.company_number,
       MD5(CONCAT(rchis.company_number, TRIM(SUBSTRING_INDEX(SICCode_SicText_4,'-',1))))
from raw_companies_house_input_stage rchis
inner join organisation o on rchis.organisation_id = o.id
    left join sic_code sc on MD5(CONCAT(rchis.company_number, TRIM(SUBSTRING_INDEX(SICCode_SicText_4,'-',1)))) = sc.md5
where TRIM(SUBSTRING_INDEX(SICCode_SicText_4,'-',1)) is not null and
      TRIM(SUBSTRING_INDEX(SICCode_SicText_4,'-',1)) <> '' and sc.md5 is null;""")
    db.commit()


def post_update_activity(cursor, db):
    cursor.execute("""
    update organisation o
    inner join raw_companies_house_input_stage rchis
    on o.id = rchis.organisation_id
    set o.company_status = rchis.company_status,
    last_modified_by = 'Rory',
    last_modified_date = CURDATE()""")

    db.commit()


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
