from locker import connect_preprod, host, user, passwd, database
import re
from file_parser.utils import pipeline_messenger
import sqlalchemy
import pandas as pd

cursor, db = connect_preprod()


def mass_update_sic_codes(cursor, db):
    cursor.execute("""select o.id, o.company_number,sc.company_number, rchis.sic_text_1 from organisation o left join sic_code sc on o.id = sc.organisation_id
                     inner join raw_companies_house_input_stage rchis on o.company_number = rchis.company_number
                     where sc.code is null and o.country = 'UNITED KINGDOM'""")
    res = cursor.fetchall()
    for org_id, cnumber, _, rchis_sic in res:
        print(rchis_sic)
        if rchis_sic != 'None Supplied':
            print(org_id, cnumber, rchis_sic)
            sic_code = re.findall(r'\d+', rchis_sic)[0]
            print(sic_code, ' for ', org_id)
        else:
            sic_code = rchis_sic
        cursor.execute(
            """insert into sic_code (code, organisation_id, company_number) VALUES (%s, %s, %s)""",
            (sic_code, org_id, cnumber))
        db.commit()
    rowcount = len(res)
    pipeline_title = 'Successful Parsing of SIC_CODES '
    pipeline_message = f'total rows affected: {rowcount}'
    pipeline_hexcolour = '#83eb34'
    pipeline_messenger(title=pipeline_title, text=pipeline_message, hexcolour=pipeline_hexcolour)


def pandas_sic(host, user, passwd, db, cursor, cursordb):
    constring = f'mysql://{user}:{passwd}@{host}:3306/{db}'
    dbEngine = sqlalchemy.create_engine(constring)
    can_run = 1
    while can_run == 1:
        cursor.execute("""select rchis.company_number, rchis.sic_text_1
                    from raw_companies_house_input_stage rchis
                    left join sic_code sc on rchis.company_number = sc.company_number
                    where sc.company_number is null""")
        result_check = cursor.fetchall()
        if len(result_check) == 0:
            can_run = 0
            pass
        else:
            print('running sic_update')
            df_query = """select rchis.company_number, rchis.sic_text_1
                            from raw_companies_house_input_stage rchis
                            left join sic_code sc on rchis.company_number = sc.company_number
                            where sc.company_number is null limit 50000"""
            df = pd.read_sql(sql=df_query, con=dbEngine)
            df['organisation_id'] = ''
            for idx, row in df.iterrows():
                row['organisation_id'] = f"UK{row['company_number']}"
                print(row['organisation_id'])
                if row['sic_text_1'] is not None and row['sic_text_1'] != 'None Supplied':
                    row['sic_text_1'] = re.findall(r'\d+', row['sic_text_1'])[0]

            df.to_sql(name='sic_code_staging', con=dbEngine, if_exists='append', index=False,
                      schema='iqblade')
            cursor.execute("""insert ignore into sic_code (code, organisation_id, company_number)
             select sic_text_1, organisation_id, company_number from sic_code_staging""")
            cursordb.commit()
            cursor.execute("""truncate table sic_code_staging""")
            cursordb.commit()


def sql_sic(cursor, db):
    cursor.execute("""insert into sic_code
    (code, organisation_id, company_number)
    select TRIM(SUBSTRING_INDEX(sic_text_1,'-',1))
    as sic_code_clean, company_number, CONCAT('UK', company_number)
    from raw_companies_house_input_stage
    on duplicate key update code = TRIM(SUBSTRING_INDEX(sic_text_1,'-',1))""")
    db.commit()
