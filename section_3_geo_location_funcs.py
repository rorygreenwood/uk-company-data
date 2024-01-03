"""
functions that are used in the geo_location upserts
"""
from utils import timer, logger


@timer
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
@timer
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
    cursor.execute(
        """update raw_companies_house_input_stage set reg_address_postcode = '' where reg_address_postcode is null""")
    db.commit()
    cursor.execute("""update raw_companies_house_input_stage
    set md5_key = MD5(CONCAT(organisation_id, reg_address_postcode)) where md5_key is null """)
    db.commit()

@timer
def geo_location_remove_old_head_offices(cursor, db):
    """
    rank geo_location head offices by their ID, for countries with more than one head_office in geo_location

    """
    cursor.execute("""
    delete from geo_location where id in (
select id from (
    select id, organisation_id, rank() over (partition by organisation_id order by id desc) as id_rank
    from geo_location
    where address_type = 'HEAD_OFFICE' and country = 'UK'
              ) t1
where id_rank > 1
);
    """)
    db.commit()


@timer
def geolocation_upsert(cursor, db):
    """
    upsert from rchis into geo_location
    on duplicate key we set address type as head_office
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
           md5(concat(rchis.organisation_id, reg_address_postcode)), curdate(), 'chp section 3 - geo_location_insert'
        from raw_companies_house_input_stage rchis
        left join geo_location gl on gl.md5_key = rchis.md5_key
        where gl.md5_key is null
on duplicate key update 
                     address_type = 'HEAD_OFFICE',
                     last_modified_by = 'chp section3 geo_location_upsert',
                     date_last_modified = CURDATE()
        """)
    db.commit()
