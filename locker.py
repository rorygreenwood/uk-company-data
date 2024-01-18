companies_house_file_data_types = {
    'company_name': str,
    'company_number': str,
    'RegAddress_CareOf': str,
    'RegAddress_POBox': str,
    'reg_address_line1': str,
    'reg_address_line2': str,
    'reg_address_posttown': str,
    'reg_address_county': str,
    'RegAddress_Country': str,
    'reg_address_postcode': str,
    'CompanyCategory': str,
    'company_status': str,
    'country_of_origin': str,
    'DissolutionDate': str,
    'IncorporationDate': str,
    'Accounts_AccountRefDay': str,
    'Accounts_AccountRefMonth': str,
    'Accounts_NextDueDate': str,
    'Accounts_LastMadeUpDate': str,
    'Accounts_AccountCategory': str,
    'Returns_NextDueDate': str,
    'Returns_LastMadeUpDate': str,
    'Mortgages_NumMortCharges': str,
    'Mortgages_NumMortOutstanding': str,
    'Mortgages_NumMortPartSatisfied': str,
    'Mortgages_NumMortSatisfied': str,
    'sic_text_1': str,
    'sic_text_2': str,
    'SICCode_SicText_3': str,
    'SICCode_SicText_4': str,
    'LimitedPartnerships_NumGenPartners': str,
    'LimitedPartnerships_NumLimPartners': str,
    'URI': str,
    'PreviousName_1_CONDATE': str,
    'PreviousName_1_CompanyName': str,
    'PreviousName_2_CONDATE': str,
    'PreviousName_2_CompanyName': str,
    'PreviousName_3_CONDATE': str,
    'PreviousName_3_CompanyName': str,
    'PreviousName_4_CONDATE': str,
    'PreviousName_4_CompanyName': str,
    'PreviousName_5_CONDATE': str,
    'PreviousName_5_CompanyName': str,
    'PreviousName_6_CONDATE': str,
    'PreviousName_6_CompanyName': str,
    'PreviousName_7_CONDATE': str,
    'PreviousName_7_CompanyName': str,
    'PreviousName_8_CONDATE': str,
    'PreviousName_8_CompanyName': str,
    'PreviousName_9_CONDATE': str,
    'PreviousName_9_CompanyName': str,
    'PreviousName_10_CONDATE': str,
    'PreviousName_10_CompanyName': str,
    'ConfStmtNextDueDate': str,
    'ConfStmtLastMadeUpDate': str,
    'Date_Of_Insert': str,
    'SourceFile': str,
    'phone_number': str,
    'number_of_employees': str

}

# columns in SQL table
companies_house_file_table_columns = [
    'company_name',
    'company_number',
    'RegAddress_CareOf',
    'RegAddress_POBox',
    'reg_address_line1',
    'reg_address_line2',
    'reg_address_posttown',
    'reg_address_county',
    'RegAddress_Country',
    'reg_address_postcode',
    'CompanyCategory',
    'company_status',
    'country_of_origin',
    'DissolutionDate',
    'IncorporationDate',
    'Accounts_AccountRefDay',
    'Accounts_AccountRefMonth',
    'Accounts_NextDueDate',
    'Accounts_LastMadeUpDate',
    'Accounts_AccountCategory',
    'Returns_NextDueDate',
    'Returns_LastMadeUpDate',
    'Mortgages_NumMortCharges',
    'Mortgages_NumMortOutstanding',
    'Mortgages_NumMortPartSatisfied',
    'Mortgages_NumMortSatisfied',
    'sic_text_1',
    'sic_text_2',
    'SICCode_SicText_3',
    'SICCode_SicText_4',
    'LimitedPartnerships_NumGenPartners',
    'LimitedPartnerships_NumLimPartners',
    'URI',
    'PreviousName_1_CONDATE',
    'PreviousName_1_CompanyName',
    'PreviousName_2_CONDATE',
    'PreviousName_2_CompanyName',
    'PreviousName_3_CONDATE',
    'PreviousName_3_CompanyName',
    'PreviousName_4_CONDATE',
    'PreviousName_4_CompanyName',
    'PreviousName_5_CONDATE',
    'PreviousName_5_CompanyName',
    'PreviousName_6_CONDATE',
    'PreviousName_6_CompanyName',
    'PreviousName_7_CONDATE',
    'PreviousName_7_CompanyName',
    'PreviousName_8_CONDATE',
    'PreviousName_8_CompanyName',
    'PreviousName_9_CONDATE',
    'PreviousName_9_CompanyName',
    'PreviousName_10_CONDATE',
    'PreviousName_10_CompanyName',
    'ConfStmtNextDueDate',
    'ConfStmtLastMadeUpDate',
    'Date_Of_Insert',
    'SourceFile',
    'phone_number',
    'number_of_employees']

# columns in csv
companies_house_file_csv_columns = [
    'CompanyName'
    , ' CompanyNumber'
    , 'RegAddress.CareOf'
    , 'RegAddress.POBox'
    , 'RegAddress.AddressLine1'
    , ' RegAddress.AddressLine2'
    , 'RegAddress.PostTown'
    , 'RegAddress.County'
    , 'RegAddress.Country'
    , 'RegAddress.PostCode'
    , 'CompanyCategory'
    , 'CompanyStatus'
    , 'CountryOfOrigin'
    , 'DissolutionDate'
    , 'IncorporationDate'
    , 'Accounts.AccountRefMonth'
    , 'Accounts.AccountRefDay'
    , 'Accounts.NextDueDate'
    , 'Accounts.LastMadeUpDate'
    , 'Accounts.AccountCategory'
    , 'Returns.NextDueDate'
    , 'Returns.LastMadeUpDate'
    , 'Mortgages.NumMortCharges'
    , 'Mortgages.NumMortOutstanding'
    , 'Mortgages.NumMortPartSatisfied'
    , 'Mortgages.NumMortSatisfied'
    , 'SICCode.SicText_1'
    , 'SICCode.SicText_2'
    , 'SICCode.SicText_3'
    , 'SICCode.SicText_4'
    , 'LimitedPartnerships.NumGenPartners'
    , 'LimitedPartnerships.NumLimPartners'
    , 'URI'
    , 'PreviousName_1.CONDATE'
    , ' PreviousName_1.CompanyName'
    , ' PreviousName_2.CONDATE'
    , ' PreviousName_2.CompanyName'
    , 'PreviousName_3.CONDATE'
    , ' PreviousName_3.CompanyName'
    , 'PreviousName_4.CONDATE'
    , ' PreviousName_4.CompanyName'
    , 'PreviousName_5.CONDATE'
    , ' PreviousName_5.CompanyName'
    , 'PreviousName_6.CONDATE'
    , ' PreviousName_6.CompanyName'
    , 'PreviousName_7.CONDATE'
    , ' PreviousName_7.CompanyName'
    , 'PreviousName_8.CONDATE'
    , ' PreviousName_8.CompanyName'
    , 'PreviousName_9.CONDATE'
    , ' PreviousName_9.CompanyName'
    , 'PreviousName_10.CONDATE'
    , ' PreviousName_10.CompanyName'
    , 'ConfStmtNextDueDate'
    , ' ConfStmtLastMadeUpDate'
    , 'Date_Of_Insert'
    , 'SourceFile'
    , 'phone_number'
    , 'number_of_employees'
]

# columns in SQL table for sic_loading
sic_code_table_columns = [
    'CompanyNumber',
    'SicText_1',
    'SicText_2',
    'SicText_3',
    'SicText_4'
]

# columns in csv for sic loading
sic_code_csv_columns = [
    ' CompanyNumber'
    , 'SICCode.SicText_1'
    , 'SICCode.SicText_2'
    , 'SICCode.SicText_3'
    , 'SICCode.SicText_4'
]

companies_house_conversion_dict = {companies_house_file_csv_columns[i]: companies_house_file_table_columns[i] for i in
                   range(len(companies_house_file_csv_columns))}

sic_code_conversion_dict = {sic_code_csv_columns[i]: sic_code_table_columns[i] for i in
                       range(len(sic_code_csv_columns))}

