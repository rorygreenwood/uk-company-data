from locker import connect_preprod


def address_processing(cursor, db):
    """move address data from staging to address table for front-end use"""
    cursor.execute("""select
    company_number,
    company_name,
    RegAddress_POBox,
    reg_address_line1,
    reg_address_line2,
    reg_address_posttown,
    reg_address_county,
    reg_address_postcode,
    RegAddress_Country
     from raw_companies_house_input_stage""")
    res = cursor.fetchall()
    for cnumber, cname, pobox, line1, line2, posttown, county, postcode, country in res:
        org_id = f'UK{cnumber}'
        format_postcode = postcode.lower().replace(' ', '')
        cursor.execute("""select organisation_id, address_1, address_2 from geo_location where organisation_id = %s""",
                       (org_id,))
        search_res = cursor.fetchall()
        print(len(search_res))
        if search_res == 0:
            print('---no org_id, add logic to add in---')
        else:
            if len(search_res) > 10:
                print('org_id exists, either skip or update')
                for org_id, ad1, ad2 in search_res:
                    print(org_id, ad1, ad2)
        print(format_postcode)
        print('---')
        office_type = 'HEAD OFFICE'
        cursor.execute("""insert into geo_location
        (organisation_id, address_1, address_2, town, county, post_code, post_code_formatted, area_location,
        address_type) VALUES (%s, %s, %ws, %s, %s, %s, %s, %s, %s)
        """, (org_id, line1, line2, posttown, county, postcode, format_postcode, county, office_type))
        db.commit()


cursor, db = connect_preprod()
address_processing(cursor, db)
