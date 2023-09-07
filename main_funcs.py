import logging

from locker import connect_preprod

cursor, db = connect_preprod()

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s [line:%(lineno)d] %(levelname)s: %(message)s')
logger = logging.getLogger()


# COMPANIES HOUSE TABLE

def add_organisation_id(cursor, db):
    """
    adds organisation id to company in raw companies house file
    :param cursor:
    :param db:
    :return:
    """
    cursor.execute("""update raw_companies_house_input_stage
     set organisation_id = CONCAT('UK', company_number) where organisation_id is null""")
    db.commit()


def _add_organisation_id_retro(cursor, db):
    cursor.execute("""update raw_companies_house_input_stage
     set organisation_id = CONCAT('UK', company_number) where organisation_id is null""")
    db.commit()


# ORGANISATION WORK

def write_to_org(cursor, db):
    """
    write new companies into organisation from companies house staging table
    :param cursor:
    :param db:
    :return:
    """
    # insert into organisation as well
    cursor.execute("""insert ignore into organisation (id, company_name,
                                 company_number,
                                 company_status, country,
                                 date_formed, last_modified_by,
                                 last_modified_date, country_code)
                        select CONCAT('UK', company_number),
                               company_name, company_number,
                               company_status,
                               'UNITED KINGDOM', STR_TO_DATE(IncorporationDate, '%d/%m/%Y'),
                               'Rory - CHP - write_to_org', CURDATE(), 'UK' from raw_companies_house_input_stage
                                where CONCAT('UK', company_number) not in
                                (select id from organisation_insert_test where country = 'UNITED KINGDOM')""")
    db.commit()


def update_org_website(cursor, db):
    """
    if a company does not have a website in organisation, update with new website from companies house raw file
    :param cursor:
    :param db:
    :return:
    """
    cursor.execute("""update organisation o
                        inner join raw_companies_house_input_stage rchis on o.company_number = rchis.company_number
                        set 
                        o.website = rchis.URI,
                        last_modified_by = 'Rory - CHP - update_org_website',
                        last_modified_date = CURDATE()
                         where o.website is null and o.website <> ''""")
    db.commit()


def update_org_activity(cursor, db):
    """
    update companies with new activity status
    :param cursor:
    :param db:
    :return:
    """
    cursor.execute("""
    update organisation o
    inner join raw_companies_house_input_stage rchis
    on o.id = rchis.organisation_id
    set o.company_status = rchis.company_status,
    last_modified_by = 'Rory - CHP - update_org_activity',
    last_modified_date = CURDATE()
    where o.company_status != rchis.company_status
    """)

    db.commit()


def del_from_org(cursor, db):
    """
    iterates over several delete statements to remove records of a company from all tables linked to organisation
    and then delete the row itself.
    :param cursor:
    :param db:
    :return:
    """
    table_set_a = ['organisation_filing_history', 'organisation_officer_appointment', 'sic_code', 'geo_location',
                   'organisation_digital_maturity', 'director', 'alias_name', 'isic_organisations_mapped',
                   'domain_alias', 'tags', 'social_handles', 'frontend_quicksearch_history', 'hgdata_entries',
                   'previous_name', 'organisation_appointment_details', 'hgdata_organisation',
                   'organisation_competencies',
                   'financial_summary', 'detailed_financials', 'employee_estimation', 'organisation_group_structure',
                   'ryan_competencyID_organisationID']
    table_set_b = [('organisation_merger_details', 'org_1_id', 'org_2_id'),
                   ('organisation_funding_details', 'funded_organisation_id', 'leading_organisation_id')]
    table_set_c = ['social_youtube_account_topics',
                   'social_youtube_history']

    cursor.execute("""select o.id, o.company_name from organisation o inner join raw_companies_house_input_stage rchis on o.id = rchis.organisation_id
    where rchis.reg_address_postcode is null and rchis.Accounts_AccountCategory = 'NO ACCOUNTS FILED' """)
    companies = cursor.fetchall()
    # cname fail list consists of company name, table name
    c_name_fail_list = []
    for c_id, c_name in companies:

        for table in table_set_a:
            try:
                cursor.execute(f"""delete from {table} where organisation_id = {c_id})""")
                db.commit()
            except Exception as e:
                print(e)
                c_name_fail_list.append((c_id, c_name, table, e))
                pass

        for table, col1, col2 in table_set_b:
            try:
                cursor.execute(f"""delete from {table} where {col1} = {c_id}
            or {col2} = {c_id};""")
                db.commit()
            except Exception as e:
                print(e)
                c_name_fail_list.append((c_id, c_name, table, e))
                pass

        for table in table_set_c:
            try:
                cursor.execute(f"""delete from {table} where youtube_account_id in
            (select id from social_youtube_account where organisation_id = {c_id})""")
                db.commit()
            except Exception as e:
                print(e)
                c_name_fail_list.append((c_id, c_name, table, e))
                pass
            try:
                cursor.execute(f"""delete from social_youtube_account where organisation_id = {c_id};""")
                db.commit()
            except Exception as e:
                print(e)
                c_name_fail_list.append((c_id, c_name, table, e))
                pass
            try:
                cursor.execute(f"""delete from organisation where id = {c_id}""")
                db.commit()
            except Exception as e:
                print(e)
                c_name_fail_list.append((c_id, c_name, table, e))
                pass
    for company_id, company_name, preprod_table, error_text in c_name_fail_list:
        cursor.execute("""insert into companies_house_pipeline_failed_removals
         (company_id, company_name, table_name, error_text) VALUES (%s, %s, %s, %s) """,
                       company_id, company_name, preprod_table, error_text
                       )
        db.commit()


def find_more_postcodes(cursor, db):
    """
        mysql query that finds companies house records without a value in their postcode column
    uses a regexp_substr function to find a postcode in a concat of the rest of the address. Sometimes
    the postcode can be found in there. If found, update the record with the new postcode
    :param cursor:
    :param db:
    :return:
    """
    cursor.execute("""update raw_companies_house_input_stage
    set reg_address_postcode = REGEXP_SUBSTR(concat(reg_address_line1, ' ', reg_address_line2
    , ' ', reg_address_posttown,' ', reg_address_county), '[A-Z]{1,2}[0-9]+ +[0-9]+[A-Z]+')
        where reg_address_postcode is null
          and REGEXP_SUBSTR(concat(reg_address_line1, ' ', reg_address_line2
    , ' ', reg_address_posttown,' ', reg_address_county), '[A-Z]{1,2}[0-9]+ +[0-9]+[A-Z]+') is not null;
    """)
    db.commit()


# GEOLOCATION

def geolocation_md5_gen(cursor, db):
    """
    Generate md5 hash in the companies house staging table
    :param cursor:
    :param db:
    :return:
    """
    cursor.execute("""update raw_companies_house_input_stage
    set md5_key = MD5(CONCAT(organisation_id, reg_address_postcode)) where md5_key is null """)
    db.commit()


def geolocation_update_current(cursor, db):
    """
    MySQL query to update existing records in geo_location
    :param cursor:
    :param db:
    :return:
    """
    cursor.execute("""
    update geo_location gl inner join raw_companies_house_input_stage rchis on gl.organisation_id = rchis.organisation_id
            set gl.address_1 = rchis.reg_address_line1,
            gl.address_2 = rchis.reg_address_line2,
            gl.town = rchis.reg_address_posttown,
            gl.county = rchis.reg_address_county,
            gl.area_location = rchis.reg_address_county,
            gl.post_code = rchis.reg_address_postcode,
            gl.post_code_formatted = LOWER(TRIM(rchis.reg_address_postcode)),
            gl.md5_key = rchis.md5_key,
            gl.date_last_modified = curdate(),
            gl.last_modified_by = 'Rory - CHP - geolocation_update_current'
            where gl.md5_key <> rchis.md5_key and gl.md5_key is null;""")
    db.commit()


def geolocation_insert_excess(cursor, db):
    """
    Bulk Insert ignore statement from staging table to geo_location
    :param cursor:
    :param db:
    :return:
    """
    cursor.execute("""insert ignore into geo_location
        (address_1, address_2,
         town, county,
          post_code, area_location, country, address_type,
           post_code_formatted, organisation_id, md5_key, date_last_modified, last_modified_by)
        select
        reg_address_line1, reg_address_line2
        , reg_address_posttown, reg_address_county,
         reg_address_postcode, reg_address_county, 'UK', 'HEAD_OFFICE',
          LOWER(REPLACE(reg_address_postcode, ' ', '')), CONCAT('UK', company_number),
           md5(concat(rchis.organisation_id, reg_address_postcode)), curdate(), 'Rory - CHP - geo_location_insert'
        from raw_companies_house_input_stage rchis
        left join geo_location gl on gl.md5_key = rchis.md5_key
        where gl.md5_key is null""")
    db.commit()


# SIC
def sql_sic(cursor, db):
    """
    insert sic_code data from staging table to sic_code table. different queries from multiple sic columns
    :param cursor:
    :param db:
    :return:
    """
    # for sic_text_1
    logger.info('starting sql_sic')
    cursor.execute("""insert ignore into sic_code (code, organisation_id, company_number, md5)
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

    cursor.execute("""
    insert into isic_organisations_mapped (organisation_id, isic_id)
select o.id, il3.id from organisation o
left join isic_organisations_mapped iom on iom.organisation_id = o.id
inner join sic_code sc on sc.organisation_id = o.id
inner join isic_level_3 il3 on sc.code = il3.sic_code_1
where iom.organisation_id is null
    """)
    db.commit()
