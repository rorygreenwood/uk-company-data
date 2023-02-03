
def post_update_activity(cursor, db):
    cursor.execute("""
    update organisation o
    inner join raw_companies_house_input_stage rchis
    on o.id = rchis.organisation_id
    set o.company_status = rchis.company_status""")

    db.commit()
