import logging

from locker import connect_preprod

cursor, db = connect_preprod()

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s [line:%(lineno)d] %(levelname)s: %(message)s')
logger = logging.getLogger()


# COMPANIES HOUSE TABLE

def add_organisation_id(cursor, db):
    """
    adds organisation id to company in raw companies house file table,
    required for insertion to organisation
    :param cursor:
    :param db:
    :return:
    """
    logger.info('add_organisation_id called')
    cursor.execute("""update raw_companies_house_input_stage
     set 
     organisation_id = CONCAT('UK', company_number) where organisation_id is null""")
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
    logger.info('calling write_to_org')
    # insert into organisation as well
    cursor.execute("""insert ignore into organisation (id, company_name,
                                 company_number,
                                 company_status, country,
                                 date_formed, last_modified_by,
                                 last_modified_date, country_code)
                        select CONCAT('UK', company_number),
                               company_name, company_number,
                               company_status,
                               'UNITED KINGDOM', CURDATE(),
                               'CompaniesHouse_DataTransfer - write_to_org', CURDATE(), 'UK' from raw_companies_house_input_stage
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
    logger.info('update_org_website called')
    cursor.execute("""update organisation o
                        inner join raw_companies_house_input_stage rchis on o.company_number = rchis.company_number
                        set 
                        o.website = rchis.URI,
                        last_modified_by = 'CompaniesHouse_DataTransfer - update_org_website',
                        last_modified_date = CURDATE()
                         where o.website is null and o.website <> ''""")
    db.commit()


def update_org_name(cursor, db):
    """

    :param cursor:
    :param db:
    :return:
    """
    logger.info('update_org_name_called')
    cursor.execute("""update organisation o
                        inner join raw_companies_house_input_stage rchis on o.company_number = rchis.company_number
                        set 
                        o.company_name = rchis.company_name,
                        last_modified_by = 'CompaniesHouse_DataTransfer - update_org_name',
                        last_modified_date = CURDATE()
                         where o.company_name <> rchis.company_name and o.company_number = rchis.company_number""")
    db.commit()


def update_org_activity(cursor, db):
    """
    update companies with new activity status
    :param cursor:
    :param db:
    :return:
    """
    logger.info('update_org_activity called')
    cursor.execute("""
    update organisation o
    inner join raw_companies_house_input_stage rchis
    on o.id = rchis.organisation_id
    set o.company_status = rchis.company_status,
    last_modified_by = 'CompaniesHouse_DataTransfer - update_org_activity',
    last_modified_date = CURDATE()
    where o.company_status != rchis.company_status
    """)

    db.commit()


def _del_from_org(cursor, db):
    """
    iterates over several delete statements to remove records of a company from all tables linked to organisation
    and then delete the row itself.
    DO NOT RUN - WE NOT LONGER DELETE FROM ORGANISATION
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
        # cursor.execute("""insert into companies_house_pipeline_failed_removals
        #  (company_id, company_name, table_name, error_text) VALUES (%s, %s, %s, %s) """,
        #                company_id, company_name, preprod_table, error_text
        #                )
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
    logger.info('find_more_postcodes called')
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
    We use organisation_id, postcode, country and address_type because
    address_1 and address_2 can be null values
    as of 29/09/23 837k rows don't have address_1, 2.9m don't have address_2 and 800k have neither
    (across all countries)
    for uk this is 115, 21k
    :param cursor:
    :param db:
    :return:
    """
    cursor.execute("""update raw_companies_house_input_stage
    set md5_key = MD5(CONCAT(organisation_id, reg_address_postcode)) where md5_key is null """)
    db.commit()


def geolocation_update_current(cursor, db):
    """
    MySQL query to update existing records in geo_location, some companies will have changed there head office
    and this needs to be reflected.
    :param cursor:
    :param db:
    :return:
    """
    logger.info('geolocation_update_current called')
    cursor.execute("""
    update ignore geo_location gl inner join raw_companies_house_input_stage rchis on gl.organisation_id = rchis.organisation_id
            set gl.address_1 = rchis.reg_address_line1,
            gl.address_2 = rchis.reg_address_line2,
            gl.town = rchis.reg_address_posttown,
            gl.county = rchis.reg_address_county,
            gl.area_location = rchis.reg_address_county,
            gl.post_code = rchis.reg_address_postcode,
            gl.post_code_formatted = LOWER(TRIM(rchis.reg_address_postcode)),
            gl.md5_key = rchis.md5_key,
            gl.date_last_modified = curdate(),
            gl.last_modified_by = 'CompaniesHouse_DataTransfer - geolocation_update_current'
            where gl.md5_key <> rchis.md5_key and gl.organisation_id = rchis.organisation_id
            and gl.address_type = 'HEAD_OFFICE';"""
                   )
    db.commit()


def geolocation_insert_excess(cursor, db):
    """
    Bulk Insert ignore statement from staging table to geo_location
    :param cursor:
    :param db:
    :return:
    """
    logger.info('geolocation_insert_excess called')
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
           md5(concat(rchis.organisation_id, reg_address_postcode)), curdate(), 'CompaniesHouse_DataTransfer - geo_location_insert'
        from raw_companies_house_input_stage rchis
        left join geo_location gl on gl.md5_key = rchis.md5_key
        where gl.md5_key is null""")
    db.commit()


def geolocation_clean_suboffices(cursor, db):
    """
    delete sub_offices in geo_location that do not exist in current companies house file.
    if these do not
    :param cursor:
    :param db:
    :return:
    """
    cursor.execute("""select
    gl.md5_key
from
    (select
         md5_key
     from geo_location
     where
         country = 'UK' and
         address_type = 'SUB_OFFICE') gl
left join raw_companies_house_input_stage rchis on gl.md5_key = rchis.md5_key
where rchis.md5_key is null""")
    db.commit()


def _geolocation_clean_headoffices(cursor, db):
    """
    removes head office addresses that do not appear in the raw companies house file.
    If they do not appear in the companies house file for the month, we can assume the address is no longer part
    of the company?
    todo as of 15/09/23 not using this
    :param cursor:
    :param db:
    :return:
    """
    cursor.execute("""delete from geo_location where md5_key in (
select
    gl.md5_key
from
    (select
         md5_key
     from geo_location
     where
         country = 'UK' and
         address_type = 'HEAD_OFFICE') gl
left join raw_companies_house_input_stage rchis on gl.md5_key = rchis.md5_key
where rchis.md5_key is null)""")
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


def load_calculations(first_month, second_month):
    """sql query that takes two different months and calculates the difference between them"""
    cursor.execute("""insert ignore into companies_house_sic_code_analytics
select t1.sic_code,
       t1.file_date as first_month,
       t2.file_date as second_month,
       t1.sic_code_count as `first_month_count`,
       t2.sic_code_count as `second_month_count`,
       (t2.sic_code_count - t1.sic_code_count) as diff,
       100*(t2.sic_code_count-t1.sic_code_count)/t2.sic_code_count as pct_change,
       md5(concat(t1.sic_code, t1.file_date, t2.file_date)) as md5_str
       from (
select sic_code, sic_code_count, file_date from companies_house_sic_counts where month(file_date) = %s) t1
inner join (
select sic_code, sic_code_count, file_date from companies_house_sic_counts where month(file_date) = %s) t2
on t1.sic_code = t2.sic_code
order by sic_code""", (first_month, second_month))
    db.commit()


def insert_sic_counts(month):
    cursor.execute("""insert ignore into companies_house_sic_counts (sic_code, file_date, sic_code_count, md5_str) 
    SELECT code, %s, count(*), md5(concat(code, %s))  from sic_code""", (month, month))
    db.commit()
