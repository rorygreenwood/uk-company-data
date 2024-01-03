import os

import mysql.connector

from utils import timer, logger


# COMPANIES HOUSE TABLE
@timer
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


# ORGANISATION WORK
@timer
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


@timer
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


@timer
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


@timer
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
    last_modified_by = 'chp section 3 - update_org_activity',
    last_modified_date = CURDATE()
    where o.company_status != rchis.company_status
    """)

    db.commit()


