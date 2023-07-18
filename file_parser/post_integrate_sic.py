import re
from locker import connect_preprod
import pandas as pd

def sic_code_processing(cursor, db):
    """ load all results from monthly parse - divide sic code from text and insert into"""
    cursor.execute("""select * from raw_companies_house_data""")
    res = cursor.fetchall()
    for company_number, sic1, sic2, sic3, sic4 in res:
        org_id = f'UK{company_number}'
        siclist = [sic1, sic2, sic3, sic4]
        for sic in siclist:
            if sic is not None:
                sic_code = re.findall(r'\d+', sic)[0]
                print(sic)
                print(sic_code)
                print('-')
            elif sic == 'None Supplied':
                sic_code = 'None Supplied'
            else:
                sic_code = 'None Supplied - else'
            cursor.execute(
                """insert ignore into sic_code (code, organisation_id, company_number) VALUES (%s, %s, %s)""",
                (sic_code, org_id, company_number))
            db.commit()

def sic_code_processing_wpandas(cursor, db):
    processable = 1
    while processable == 1:
        cursor.execute("""select raw_companies_house_data.*, sc.company_number
    from raw_companies_house_data
    left join sic_code sc on raw_companies_house_data.company_number = sc.company_number
    where sc.code is null limit 1""")
        res = cursor.fetchall()
        if len(res) == 0:
            processable = 0
        else:
            df = pd.read_sql()


cursor, db = connect_preprod()
