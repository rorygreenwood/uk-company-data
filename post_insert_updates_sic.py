from locker import connect_preprod
import re
from file_parser.utils import pipeline_messenger

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
    pipeline_title = 'Successful Parsing of Addresses '
    pipeline_message = f'total rows affected: {rowcount}'
    pipeline_hexcolour = '#83eb34'
    pipeline_messenger(title=pipeline_title, text=pipeline_message, hexcolour=pipeline_hexcolour)


mass_update_sic_codes(cursor, db)
