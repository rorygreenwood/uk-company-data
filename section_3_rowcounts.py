"""
count queries that record how many rows have been affected by the latest
updates
"""


def rowcount_sic_codes(cursor, db):
    """
    updates companies_house row counts.
    sets the rows_changed_in_siccode equal to the number of rows in sic_code
     that have been updated on the current date.

     sic_codes does not have a date_last_modified.

    :param cursor:
    :param db:
    :return:
    """
    # identify potential sic_code entries
    cursor.execute("""
    update companies_house_rowcounts set potential_sic_code_entries = (
select count(*) from (
select TRIM(SUBSTRING_INDEX(sic_text_1,'-',1)) as code,
       concat('UK', company_number),
       rchis.company_number as company_number,
       MD5(CONCAT(rchis.company_number, TRIM(SUBSTRING_INDEX(sic_text_1,'-',1)))) as md5,
       CURDATE() as date_last_modified
from raw_companies_house_input_stage rchis
union
select TRIM(SUBSTRING_INDEX(sic_text_2,'-',1)) as code,
       concat('UK', company_number),
       rchis.company_number as company_number,
       MD5(CONCAT(rchis.company_number, TRIM(SUBSTRING_INDEX(sic_text_2,'-',1)))) as md5,
       CURDATE() as date_last_modified
from raw_companies_house_input_stage rchis
union
select TRIM(SUBSTRING_INDEX(SICCode_SicText_3,'-',1)) as code,
       concat('UK', company_number),
       rchis.company_number as company_number,
       MD5(CONCAT(rchis.company_number, TRIM(SUBSTRING_INDEX(SICCode_SicText_3,'-',1)))) as md5,
       CURDATE() as date_last_modified
from raw_companies_house_input_stage rchis
union
select TRIM(SUBSTRING_INDEX(SICCode_SicText_4,'-',1)) as code,
       concat('UK', company_number),
       rchis.company_number as company_number,
       MD5(CONCAT(rchis.company_number, TRIM(SUBSTRING_INDEX(SICCode_SicText_4,'-',1)))) as md5,
       CURDATE() as date_last_modified
from raw_companies_house_input_stage rchis) t1)""")
    db.commit()
    cursor.execute("""
    update companies_house_rowcounts set rows_changed_in_siccode = (
    select count(*) from sic_code
    where MONTH(date_last_modified) = MONTH(CURDATE()) and
      YEAR(date_last_modified) = YEAR(CURDATE()))
    where month(file_month) = month(curdate()) and year(file_month) = year(curdate())
       """)
    db.commit()


def rowcount_geo_location(cursor, db):
    """
    updates companies_house row counts.
    sets the rows_changed_in_geolocation equal to the number of rows in sic_code
     that have been updated on the current date.
    :param cursor:
    :param db:
    :return:
    """
    cursor.execute("""
    update companies_house_rowcounts
    set 
    rows_changed_in_geolocation = (
    select count(*)
     from iqblade.geo_location
      where month(date_last_modified) = month(curdate()) and
      year(date_last_modified) = year(curdate())
      and country = 'UK' and address_type = 'HEAD_OFFICE'
    )
    where month(file_month) = month(curdate()) and
    year(file_month) = year(curdate())
    """)
    db.commit()


def rowcount_organisation(cursor, db):
    """
    updates companies_house row counts.
    sets the rows_changed_in_org equal to the number of rows in sic_code
     that have been updated on the current date.
    :param cursor:
    :param db:
    :return:
    """
    cursor.execute("""
    update companies_house_rowcounts
    set rows_changed_in_org = (
    select count(*)
    from iqblade.organisation
    where country = 'UK' and
    month(last_modified_date) = month(curdate()) and
      year(last_modified_date) = year(curdate())
    )
    """)
    db.commit()


def post_rowcount_update_calculations(cursor, db):
    # find difference (A) by subtracting the number of rows affected by latest file (B) from the size of the file in rows (C)
    # A = C-B
    # find percentage change (D)
    # D = (A/B)*100
    cursor.execute(
        """
update companies_house_rowcounts
set
    geolocation_diff = file_rowcount - rows_changed_in_geolocation,
    organisation_diff = file_rowcount - rows_changed_in_org,
    sic_code_diff = file_rowcount - rows_changed_in_siccode
    where MONTH(file_month) = month(curdate()) and year(file_month) - year(curdate())
        """
    )
    db.commit()


if __name__ == '__main__':
    from utils import connect_preprod

    cursor, db = connect_preprod()
    rowcount_organisation(cursor, db)
    print('org done')
    rowcount_sic_codes(cursor, db)
    print('sic_codes done')
    rowcount_geo_location(cursor, db)
    print('geo_location done')
