import datetime
import logging

from locker import connect_preprod
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s [line:%(lineno)d] %(levelname)s: %(message)s')
logger = logging.getLogger()
cursor, db = connect_preprod()

# todo - compile data from month n and month n-1 as two separate tables
# this might require retroactive fileloader
# we would need to get the date, and then the date minus one month
first_day_of_this_month = datetime.date.today().replace(day=1)
first_day_of_last_month = first_day_of_this_month.replace(month=first_day_of_this_month.month - 1)
first_day_of_this_month_as_str = first_day_of_this_month.strftime('%Y-%m-%d')
first_day_of_last_month_as_str = first_day_of_last_month.strftime('%Y-%m-%d')
first_day_of_this_month_table_name = f'BasicCompanyData_sic_data_{first_day_of_this_month_as_str}'
first_day_of_last_month_table_name = f'BasicCompanyData_sic_data_{first_day_of_last_month_as_str}'

# todo from here, we would need to perform the company loading
union_query_tuple_this_month = (
    first_day_of_this_month_as_str, first_day_of_this_month_table_name,
    first_day_of_this_month_as_str, first_day_of_this_month_table_name,
    first_day_of_this_month_as_str, first_day_of_this_month_table_name,
    first_day_of_this_month_as_str, first_day_of_this_month_table_name,
)
union_query_tuple_last_month = (
    first_day_of_last_month_as_str, first_day_of_last_month_table_name,
    first_day_of_last_month_as_str, first_day_of_last_month_table_name,
    first_day_of_last_month_as_str, first_day_of_last_month_table_name,
    first_day_of_last_month_as_str, first_day_of_last_month_table_name,
)
# use 'union' to put all the sic_columns into a single column for both tables, within a single table
# todo add format args to include dates declared above

union_query_str = f"""insert into BasicCompanyData_DA873_sic_company_combined
select distinct * from (
select t1.` CompanyNumber`, CompanyName,
       1 as sic_section,
       SUBSTRING_INDEX(`SICCode.SicText_1`, '-', 1) as sic_code_num,
       SUBSTRING(`SICCode.SicText_1`, INSTR(`SICCode.SicText_1`, '-') + 2) as sic_code_text,
       %s as fragment_month
from %s t1 where `SICCode.SicText_1` <> ''
union all
select t1.` CompanyNumber`, CompanyName,
       2 as sic_section,
       SUBSTRING_INDEX(`SICCode.SicText_2`, '-', 1) as sic_code_num,
       SUBSTRING(`SICCode.SicText_2`, INSTR(`SICCode.SicText_2`, '-')+2) as sic_code_text,
       %s as fragment_month
from %s t1 where `SICCode.SicText_2` <> ''
union all
select t1.` CompanyNumber`, CompanyName,
       3 as sic_section,
       SUBSTRING_INDEX(`SICCode.SicText_3`, '-', 1) as sic_code_num,
       SUBSTRING(`SICCode.SicText_3`, INSTR(`SICCode.SicText_3`, '-')+2) as sic_code_text,
       %s as fragment_month
from %s t1 where `SICCode.SicText_3` <> ''
union all
select t1.` CompanyNumber`, CompanyName,
       4 as sic_section,
       SUBSTRING_INDEX(`SICCode.SicText_4`, '-', 1) as sic_code_num,
       SUBSTRING(`SICCode.SicText_4`, INSTR(`SICCode.SicText_4`, '-')+2) as sic_code_text,
       %s as fragment_month
from %s t1 where `SICCode.SicText_4` <> '') t1
order by t1.` CompanyNumber` desc """

cursor.execute(union_query_str, union_query_tuple_this_month)
db.commit()
cursor.execute(union_query_str, union_query_tuple_last_month)
db.commit()
# perform query to map results to established sic codes
cursor.execute("""insert into basiccompanydata_DA873_sic_totals_from_fragments
select t1.SIC_code_1 as sic_code,
       t1.sic_letter_category as sic_category,
       t1.general_description_index as sic_category_name,
       t1.descriptions_index as sic_category_desc,
       t2.fragment_month as fragment_month,
       count(*) as num_of_companies from basiccompanydata_DA873_siccode_map_distinct t1
inner join BasicCompanyData_DA873_sic_company_combined t2
on t1.SIC_code_1 = TRIM(t2.sic_code_num)
group by t1.SIC_code_1, t1.sic_letter_category, t2.fragment_month""")
db.commit()
# final output will be a table
#  (month, month-1, then (count for month, count for month-1, net change, net change pct) for each sic_category
cursor.execute("""
insert into  basiccompanydata_DA873_sic_category_analysis
select 01table.sic_category as main_category,
       01table.sic_category_desc as main_category_desc,
       01table.totalnum as first_month_count,
       02table.totalnum as second_num_count,
       (02table.totalnum - 01table.totalnum) as net_change,
        (((02table.totalnum - 01table.totalnum) / 01table.totalnum) * 100) as pct_change,
        01table.fragment_month as fragment_month,
        02table.fragment_month as fragment_month_n_1

       from (
    select sic_category, sic_category_desc, fragment_month, sum(num_of_companies) as totalnum from basiccompanydata_DA873_sic_totals_from_fragments where fragment_month = '2023-01-01'
            group by sic_category_desc) as 01table
inner join (
        select sic_category, sic_category_desc, fragment_month, sum(num_of_companies) as totalnum from basiccompanydata_DA873_sic_totals_from_fragments where fragment_month = '2023-02-01'
            group by sic_category_desc
) as 02table
on 01table.sic_category_desc = 02table.sic_category_desc
group by `01table`.sic_category_desc;""")
db.commit()

