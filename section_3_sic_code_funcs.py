"""
store functions used in section 3 sic code updates and sic code analytics
"""
from utils import timer, logger


# SIC
@timer
def sic_code_db_insert(cursor, db):
    """
    insert sic_code data from staging table to sic_code table. different queries from multiple sic columns
    :param cursor:
    :param db:
    :return:
    """
    # for sic_text_1
    logger.info('starting sql_sic inserts')
    logger.info('sic_text_1 insert')
    cursor.execute("""insert ignore into sic_code (code, organisation_id, company_number, md5, date_last_modified)
select TRIM(SUBSTRING_INDEX(sic_text_1,'-',1)), o.id, rchis.company_number,
       MD5(CONCAT(rchis.company_number, TRIM(SUBSTRING_INDEX(sic_text_1,'-',1)))), CURDATE()
from raw_companies_house_input_stage rchis
inner join (select * from organisation where country = 'united kingdom') o on rchis.organisation_id = o.id
left join sic_code sc on o.id = sc.organisation_id
where sc.id is null""")
    db.commit()

    # for sic_text_2
    cursor.execute("""insert ignore into sic_code (code, organisation_id, company_number, md5, date_last_modified)
select TRIM(SUBSTRING_INDEX(sic_text_2,'-',1)), rchis.organisation_id, rchis.company_number,
       MD5(CONCAT(rchis.company_number, TRIM(SUBSTRING_INDEX(sic_text_2,'-',1)))),
       CURDATE()
from raw_companies_house_input_stage rchis
    inner join organisation o on rchis.organisation_id = o.id
    left join sic_code sc on MD5(CONCAT(rchis.company_number, TRIM(SUBSTRING_INDEX(sic_text_2,'-',1)))) = sc.md5
where TRIM(SUBSTRING_INDEX(sic_text_2,'-',1)) is not null and
      TRIM(SUBSTRING_INDEX(sic_text_2,'-',1)) <> '' and sc.md5 is null;""")
    db.commit()

    # for sic_text_3
    cursor.execute("""insert ignore into sic_code (code, organisation_id, company_number, md5, date_last_modified)
select TRIM(SUBSTRING_INDEX(SICCode_SicText_3,'-',1)), rchis.organisation_id, rchis.company_number,
       MD5(CONCAT(rchis.company_number, TRIM(SUBSTRING_INDEX(SICCode_SicText_3,'-',1)))), CURDATE()
from raw_companies_house_input_stage rchis
inner join organisation o on rchis.organisation_id = o.id
    left join sic_code sc on MD5(CONCAT(rchis.company_number, TRIM(SUBSTRING_INDEX(SICCode_SicText_3,'-',1)))) = sc.md5
where TRIM(SUBSTRING_INDEX(SICCode_SicText_3,'-',1)) is not null and
      TRIM(SUBSTRING_INDEX(SICCode_SicText_3,'-',1)) <> '' and sc.md5 is null;""")
    db.commit()

    cursor.execute("""insert ignore into sic_code (code, organisation_id, company_number, md5, date_last_modified)
select TRIM(SUBSTRING_INDEX(SICCode_SicText_4,'-',1)), rchis.organisation_id, rchis.company_number,
       MD5(CONCAT(rchis.company_number, TRIM(SUBSTRING_INDEX(SICCode_SicText_4,'-',1)))), CURDATE()
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


@timer
def load_calculations(first_month, second_month, cursor, db):
    """sql query that takes two different months and calculates the difference between them"""
    cursor.execute("""insert ignore into companies_house_sic_code_analytics (sic_code, first_month, second_month, first_month_count, second_month_count, diff, pct_change, md5_str) 
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


@timer
def insert_sic_counts(month, cursor, db):
    cursor.execute("""insert ignore into companies_house_sic_counts (sic_code, file_date, sic_code_count, md5_str) 
    SELECT code, %s, count(*), md5(concat(code, %s))  from sic_code""", (month, month))
    db.commit()


