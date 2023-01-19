import mysql.connector
import os
import sqlalchemy
import pandas as pd
from locker import dtype_dict


def parse_fragment(fragment):
    constring = f'mysql://{os.environ.get("USER")}:{os.environ.get("PASS")}@{os.environ.get("PREPRODHOST")}:3306/{os.environ.get("DATABASE")}'
    dbEngine = sqlalchemy.create_engine(constring)

    df = pd.read_csv(fragment, encoding='utf-8', dtype=dtype_dict)
    df['org_id'] = 'UK' + df[list(df.columns.values)[1]]
    df['fragment_name'] = fragment
    df.to_sql(name='BasicCompanyData_upload_staging', con=dbEngine, if_exists='append', index=False, schema='iqblade')


def load_fragment(cursor, db):
    cursor.execute(
        """INSERT INTO BasicCompanyData_upload_final
         (CompanyName
         ,` CompanyNumber`
         ,`RegAddress.CareOf`
         ,`RegAddress.POBox`
         ,`RegAddress.AddressLine1`
         ,` RegAddress.AddressLine2`
         ,`RegAddress.PostTown`
         ,`RegAddress.County`
         ,`RegAddress.Country`
         ,`RegAddress.PostCode`
         ,CompanyCategory
         ,CompanyStatus
         ,CountryOfOrigin
         ,DissolutionDate
         ,IncorporationDate
         ,`Accounts.AccountRefDay`
         ,`Accounts.AccountRefMonth`
         ,`Accounts.NextDueDate`
         ,`Accounts.LastMadeUpDate`
         ,`Accounts.AccountCategory`
         ,`Returns.NextDueDate`
         ,`Returns.LastMadeUpDate`
         ,`Mortgages.NumMortCharges`
         ,`Mortgages.NumMortOutstanding`
         ,`Mortgages.NumMortPartSatisfied`
         ,`Mortgages.NumMortSatisfied`
         ,`SICCode.SicText_1`
         ,`SICCode.SicText_2`
         ,`SICCode.SicText_3`
         ,`SICCode.SicText_4`
         ,`LimitedPartnerships.NumGenPartners`
         ,`LimitedPartnerships.NumLimPartners`
         ,URI
         ,`PreviousName_1.CONDATE`
         ,` PreviousName_1.CompanyName`
         ,` PreviousName_2.CONDATE`
         ,` PreviousName_2.CompanyName`
         ,`PreviousName_3.CONDATE`
         ,` PreviousName_3.CompanyName`
         ,`PreviousName_4.CONDATE`
         ,` PreviousName_4.CompanyName`
         ,`PreviousName_5.CONDATE`
         ,` PreviousName_5.CompanyName`
         ,`PreviousName_6.CONDATE`
         ,` PreviousName_6.CompanyName`
         ,`PreviousName_7.CONDATE`
         ,` PreviousName_7.CompanyName`
         ,`PreviousName_8.CONDATE`
         ,` PreviousName_8.CompanyName`
         ,`PreviousName_9.CONDATE`
         ,` PreviousName_9.CompanyName`
         ,`PreviousName_10.CONDATE`
         ,` PreviousName_10.CompanyName`
         ,ConfStmtNextDueDate
         ,` ConfStmtLastMadeUpDate`
         ,org_id
         ,fragment_name)
         SELECT CompanyName
         ,` CompanyNumber`
         ,`RegAddress.CareOf`
         ,`RegAddress.POBox`
         ,`RegAddress.AddressLine1`
         ,` RegAddress.AddressLine2`
         ,`RegAddress.PostTown`
         ,`RegAddress.County`
         ,`RegAddress.Country`
         ,`RegAddress.PostCode`
         ,CompanyCategory
         ,CompanyStatus
         ,CountryOfOrigin
         ,DissolutionDate
         ,IncorporationDate
         ,`Accounts.AccountRefDay`
         ,`Accounts.AccountRefMonth`
         ,`Accounts.NextDueDate`
         ,`Accounts.LastMadeUpDate`
         ,`Accounts.AccountCategory`
         ,`Returns.NextDueDate`
         ,`Returns.LastMadeUpDate`
         ,`Mortgages.NumMortCharges`
         ,`Mortgages.NumMortOutstanding`
         ,`Mortgages.NumMortPartSatisfied`
         ,`Mortgages.NumMortSatisfied`
         ,`SICCode.SicText_1`
         ,`SICCode.SicText_2`
         ,`SICCode.SicText_3`
         ,`SICCode.SicText_4`
         ,`LimitedPartnerships.NumGenPartners`
         ,`LimitedPartnerships.NumLimPartners`
         ,URI
         ,`PreviousName_1.CONDATE`
         ,` PreviousName_1.CompanyName`
         ,` PreviousName_2.CONDATE`
         ,` PreviousName_2.CompanyName`
         ,`PreviousName_3.CONDATE`
         ,` PreviousName_3.CompanyName`
         ,`PreviousName_4.CONDATE`
         ,` PreviousName_4.CompanyName`
         ,`PreviousName_5.CONDATE`
         ,` PreviousName_5.CompanyName`
         ,`PreviousName_6.CONDATE`
         ,` PreviousName_6.CompanyName`
         ,`PreviousName_7.CONDATE`
         ,` PreviousName_7.CompanyName`
         ,`PreviousName_8.CONDATE`
         ,` PreviousName_8.CompanyName`
         ,`PreviousName_9.CONDATE`
         ,` PreviousName_9.CompanyName`
         ,`PreviousName_10.CONDATE`
         ,` PreviousName_10.CompanyName`
         ,ConfStmtNextDueDate
         ,` ConfStmtLastMadeUpDate`
         ,org_id
         ,fragment_name from BasicCompanyData_upload_staging
         
         on duplicate key update 
         CompanyName = VALUES(CompanyName)
         ,` CompanyNumber` = VALUES(` CompanyNumber`)
         ,`RegAddress.CareOf` = VALUES(`RegAddress.CareOf`)
         ,`RegAddress.POBox` = VALUES(`RegAddress.POBox`)
         ,`RegAddress.AddressLine1` = VALUES(`RegAddress.AddressLine1`)
         ,` RegAddress.AddressLine2` = VALUES(` RegAddress.AddressLine2`)
         ,`RegAddress.PostTown` = VALUES(`RegAddress.PostTown`)
         ,`RegAddress.County` = VALUES(`RegAddress.County`)
         ,`RegAddress.Country` = VALUES(`RegAddress.Country`)
         ,`RegAddress.PostCode` = VALUES(`RegAddress.PostCode`)
         ,CompanyCategory = VALUES(CompanyCategory)
         ,CompanyStatus = VALUES(CompanyStatus)
         ,CountryOfOrigin = VALUES(CountryOfOrigin)
         ,DissolutionDate = VALUES(DissolutionDate)
         ,IncorporationDate = VALUES(IncorporationDate)
         ,`Accounts.AccountRefDay` = VALUES(`Accounts.AccountRefDay`)
         ,`Accounts.AccountRefMonth` = VALUES(`Accounts.AccountRefMonth`)
         ,`Accounts.NextDueDate` = VALUES(`Accounts.NextDueDate`)
         ,`Accounts.LastMadeUpDate` = VALUES(`Accounts.LastMadeUpDate`)
         ,`Accounts.AccountCategory` = VALUES(`Accounts.AccountCategory`)
         ,`Returns.NextDueDate` = VALUES(`Returns.NextDueDate`)
         ,`Returns.LastMadeUpDate` = VALUES(`Returns.LastMadeUpDate`)
         ,`Mortgages.NumMortCharges` = VALUES(`Mortgages.NumMortCharges`)
         ,`Mortgages.NumMortOutstanding` = VALUES(`Mortgages.NumMortOutstanding`)
         ,`Mortgages.NumMortPartSatisfied` = VALUES(`Mortgages.NumMortPartSatisfied`)
         ,`Mortgages.NumMortSatisfied` = VALUES(`Mortgages.NumMortSatisfied`)
         ,`SICCode.SicText_1` = VALUES(`SICCode.SicText_1`)
         ,`SICCode.SicText_2` = VALUES(`SICCode.SicText_2`)
         ,`SICCode.SicText_3` = VALUES(`SICCode.SicText_3`)
         ,`SICCode.SicText_4` = VALUES(`SICCode.SicText_4`)
         ,`LimitedPartnerships.NumGenPartners` = VALUES(`LimitedPartnerships.NumGenPartners`)
         ,`LimitedPartnerships.NumLimPartners` = VALUES(`LimitedPartnerships.NumLimPartners`)
         ,URI = VALUES(URI)
         ,`PreviousName_1.CONDATE` = VALUES(`PreviousName_1.CONDATE`)
         ,` PreviousName_1.CompanyName` = VALUES(` PreviousName_1.CompanyName`)
         ,` PreviousName_2.CONDATE` = VALUES(` PreviousName_2.CONDATE`)
         ,` PreviousName_2.CompanyName` = VALUES(` PreviousName_2.CompanyName`)
         ,`PreviousName_3.CONDATE` = VALUES(`PreviousName_3.CONDATE`)
         ,` PreviousName_3.CompanyName` = VALUES(` PreviousName_3.CompanyName`)
         ,`PreviousName_4.CONDATE` = VALUES(`PreviousName_4.CONDATE`)
         ,` PreviousName_4.CompanyName` = VALUES(` PreviousName_4.CompanyName`)
         ,`PreviousName_5.CONDATE` = VALUES(`PreviousName_5.CONDATE`)
         ,` PreviousName_5.CompanyName` = VALUES(` PreviousName_5.CompanyName`)
         ,`PreviousName_6.CONDATE` = VALUES(`PreviousName_6.CONDATE`)
         ,` PreviousName_6.CompanyName` = VALUES(` PreviousName_6.CompanyName`)
         ,`PreviousName_7.CONDATE` = VALUES(`PreviousName_7.CONDATE`)
         ,` PreviousName_7.CompanyName` = VALUES(` PreviousName_7.CompanyName`)
         ,`PreviousName_8.CONDATE` = VALUES(`PreviousName_8.CONDATE`)
         ,` PreviousName_8.CompanyName` = VALUES(` PreviousName_8.CompanyName`)
         ,`PreviousName_9.CONDATE` = VALUES(`PreviousName_9.CONDATE`)
         ,` PreviousName_9.CompanyName` = VALUES(` PreviousName_9.CompanyName`)
         ,`PreviousName_10.CONDATE` = VALUES(`PreviousName_10.CONDATE`)
         ,` PreviousName_10.CompanyName` = VALUES(` PreviousName_10.CompanyName`)
         ,ConfStmtNextDueDate = VALUES(ConfStmtNextDueDate)
         ,` ConfStmtLastMadeUpDate` = VALUES(` ConfStmtLastMadeUpDate`)
         ,org_id = VALUES(org_id)
         ,fragment_name = VALUES(fragment_name)
         """
    )
    db.commit()
    print('merge complete')
    cursor.execute("""truncate table BasicCompanyData_upload_staging""")
    db.commit()
