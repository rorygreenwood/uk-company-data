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
