from locker import connect_preprod
from file_parser.utils import pipeline_messenger

cursor, db = connect_preprod()


def write_to_org(cursor, db):
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


def update_org_website(cursor, db):
    print('add websites')
    cursor.execute("""update organisation o
                        inner join raw_companies_house_input_stage rchis on o.company_number = rchis.company_number
                        set 
                        o.website = rchis.URI,
                        last_modified_by = 'Rory',
                        last_modified_date = CURDATE()
                         where o.website is null and o.website <> ''""")
    db.commit()


def del_from_org(cursor, db):
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
    for table in table_set_a:
        cursor.execute(f"""delete from {table} where organisation_id in (select o.id from organisation o
    inner join raw_companies_house_input_stage rchis on o.id = rchis.organisation_id
    where rchis.reg_address_postcode is null and rchis.Accounts_AccountCategory = 'NO ACCOUNTS FILED')""", (table,))
        db.commit()

    for table, col1, col2 in table_set_b:
        cursor.execute(f"""delete from {table} where {col1} in (select o.id from organisation o
    inner join raw_companies_house_input_stage rchis on o.id = rchis.organisation_id
    where rchis.reg_address_postcode is null and rchis.Accounts_AccountCategory = 'NO ACCOUNTS FILED' )
    or {col2} in (select o.id from organisation o
    inner join raw_companies_house_input_stage rchis on o.id = rchis.organisation_id
    where rchis.reg_address_postcode is null and rchis.Accounts_AccountCategory = 'NO ACCOUNTS FILED' );""")
        db.commit()

    for table in table_set_c:
        cursor.execute(f"""delete from {table} where youtube_account_id in
    (select id from social_youtube_account where organisation_id in
    (select o.id from organisation o
    inner join raw_companies_house_input_stage rchis on o.id = rchis.organisation_id
    where rchis.reg_address_postcode is null and rchis.Accounts_AccountCategory = 'NO ACCOUNTS FILED'))""")
        db.commit()

    cursor.execute("""delete from social_youtube_account where organisation_id in (select o.id from organisation o
    inner join raw_companies_house_input_stage rchis on o.id = rchis.organisation_id
    where rchis.reg_address_postcode is null and rchis.Accounts_AccountCategory = 'NO ACCOUNTS FILED');""")
    db.commit()

    cursor.execute("""delete from organisation where id in (
    select organisation_id from raw_companies_house_input_stage rchis
                           where rchis.reg_address_postcode is null and rchis.Accounts_AccountCategory = 'NO ACCOUNTS FILED'
    );""")
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

    try:
        del_from_org(cursor, db)
    except Exception as e:
        pipeline_title = 'Error on post_inserts_organsation'
        pipeline_message = f'del_from_org: {e}'
        pipeline_hexcolour = '#83eb34'
        pipeline_messenger(title=pipeline_title, text=pipeline_message, hexcolour=pipeline_hexcolour)
        pass

    pipeline_title = 'Organisation Updates complete'
    pipeline_message = f''
    pipeline_hexcolour = '#83eb34'
    pipeline_messenger(title=pipeline_title, text=pipeline_message, hexcolour=pipeline_hexcolour)
