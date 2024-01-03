"""
count queries that record how many rows have been affected by the latest
updates
"""


def rowcount_sic_codes(cursor, db):
    cursor.execute("""update companies_house_rowcounts
     set sic_code_change_rowcount = (
     select count(*)
      from sic_code
       where month(date_last_modified) = month(CURDATE()) and
       year(date_last_modified) = year(CURDATE())
       )
       where month(file_month) = month(curdate()) and 
       year(file_month) = year(curdate()) 
       """)
    db.commit()


def rowcount_geo_location(cursor, db):
    cursor.execute("""
    update companies_house_rowcounts
    set geolocation_change_rowcount = (
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
    cursor.execute("""
    update companies_house_rowcounts
    set organisation_change_rowcount = (
    select count(*)
    from iqblade.organisation
    where country = 'UK' and
    month(last_modified_date) = month(curdate()) and
      year(last_modified_date) = year(curdate())
    )
    """)
    db.commit()

def post_rowcount_update_calculations(cursor, db):
    pass
