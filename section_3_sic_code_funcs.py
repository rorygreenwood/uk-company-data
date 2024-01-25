"""
store functions used in section 3 sic code updates and sic code analytics
"""
import datetime

from utils import timer, logger, connect_preprod, find_previous_month


# SIC


@timer
def sic_code_db_insert(cursor, db) -> None:
    sic_code_columns = ['sic_text_1', 'sic_text_2', 'SICCode_SicText_3', 'SICCode_SicText_4']
    for column in sic_code_columns:
        logger.info(f'inserting {column}')
        sql_insert_query = f"""
        insert into sic_code
         (code, organisation_id, company_number, md5, date_last_modified)
        select 
        regexp_substr({column}, '[0-9]+'), 
        concat('UK', company_number), 
        rchis.company_number,
        MD5(CONCAT(rchis.company_number, regexp_substr({column}, '[0-9]+'))), 
        CURDATE()
        from raw_companies_house_input_stage rchis
        where {column} is not null
          on duplicate key update 
          date_last_modified = curdate()"""
        cursor.execute(sql_insert_query)
        db.commit()


@timer
def load_calculations(cursor, db,
                      current_month=datetime.datetime.now().month,
                      current_year=datetime.datetime.now().year) -> None:
    """sql query that takes two different months and calculates the difference between them"""

    # use current month and current year arg to find previous month and current/previous year
    previous_month, previous_year = find_previous_month(month=current_month, year=current_year)
    cursor.execute("""
    insert ignore into companies_house_sic_code_analytics 
    (sic_code, first_month, second_month, first_month_count, second_month_count, diff, pct_change, md5_str) 
        select t1.sic_code,
       t1.file_date as first_month,
       t2.file_date as second_month,
       t1.sic_code_count as `first_month_count`,
       t2.sic_code_count as `second_month_count`,
       (t2.sic_code_count - t1.sic_code_count) as diff,
       100*(t2.sic_code_count-t1.sic_code_count)/t2.sic_code_count as pct_change,
       md5(concat(t1.sic_code, t1.file_date, t2.file_date)) as md5_str
       from (
            select sic_code, sic_code_count, file_date from
             companies_house_sic_counts where month(file_date) = %s and year(file_date) = %s) t1
             inner join (
             select sic_code, sic_code_count, file_date from companies_house_sic_counts
             where month(file_date) = %s and year(file_date) = %s) t2
             on t1.sic_code = t2.sic_code
             order by sic_code""", (previous_month, previous_year, current_month, current_year))
    db.commit()


def load_calculations_aggregates(cursor, db,
                                 current_month=datetime.datetime.now().month,
                                 current_year=datetime.datetime.now().year) -> None:
    # use current month and current year arg to find previous month and current/previous year
    previous_month, previous_year = find_previous_month(month=current_month, year=current_year)

    cursor.execute("""
    insert into sic_code_aggregate_analytics (sic_code_category, first_month, second_month, first_month_count, second_month_count, diff, pct_change, md5_str)
    select
        t1.Category,
        t1.file_date as first_month,
        t2.file_date as second_month,
        t1.count as first_month_count,
        t2.count as second_month_count,
        (t2.count - t1.count) as diff,
        100*(t2.count-t1.count)/t1.count as pct_change,
        md5(concat(t1.Category, t1.file_date, t2.file_date)) as md5_str
    from (
        select Category, count, file_date
        from companies_house_sic_code_aggregates
        where month(file_date) = %s and
              year(file_date) = %s and Category is not null and Category not in ('', '-')) t1
    inner join (
        select Category, count, file_date
        from companies_house_sic_code_aggregates
        where month(file_date) = %s and
              year(file_date) = %s and Category is not null and Category not in ('', '-')) t2
    on t1.Category = t2.Category""",
                   (previous_month, previous_year, current_month, current_year))
    db.commit()
    cursor.execute("""
        update sic_code_aggregate_analytics scaa
    inner join sic_code_categories scc
    on scc.`SIC Code` = scaa.sic_code_category
    set scaa.category_description = scc.Category_Description
    where scaa.category_description is null""")
    db.commit()


@timer
def insert_sic_counts(month, cursor, db) -> None:
    cursor.execute("""
    insert ignore into companies_house_sic_counts (sic_code, file_date, sic_code_count, md5_str) 
    SELECT code, %s, count(*), md5(concat(code, %s))  from sic_code
    where month(date_last_modified) = month(curdate())
    and year(date_last_modified) = year(curdate())""", (month, month))
    db.commit()


if __name__ == '__main__':
    cursor, db = connect_preprod()
