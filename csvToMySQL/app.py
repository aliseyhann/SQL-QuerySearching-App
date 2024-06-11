import random
import pandas as pd
from datetime import datetime
import mysql.connector
from mysql.connector import errorcode
import inflect

p = inflect.engine()

csv_file_path = '' #csv file path
data = pd.read_csv(csv_file_path)


data['SkinTypeId'] = data.index + 1 if 'SkinTypeId' not in data.columns else data['SkinTypeId']
data['SkinType'] = "Unknown" if 'SkinType' not in data.columns else data['SkinType']
data['DiscontinuedReported'] = pd.NA if 'DiscontinuedReported' not in data.columns else data['DiscontinuedReported']
data['DemographicId'] = data.index + 1 if 'DemographicId' not in data.columns else data['DemographicId']
data['Targetdemographic'] = "Unknown" if 'Targetdemographic' not in data.columns else data['Targetdemographic']
data['Demographic_DemographicId'] = data.index + 1 if 'Demographic_DemographicId' not in data.columns else data['Demographic_DemographicId']
data['SkinType_SkinTypeId'] = data.index + 1 if 'SkinType_SkinTypeId' not in data.columns else data['SkinType_SkinTypeId']
data['Chemicals_ChemicalId'] = data.index + 1 if 'Chemicals_ChemicalId' not in data.columns else data['Chemicals_ChemicalId']
data['UsageInfoId'] = data.index + 1 if 'UsageInfoId' not in data.columns else data['UsageInfoId']
data['DayNightUsage'] = "Unknown" if 'DayNightUsage' not in data.columns else data['DayNightUsage']
data['MaxUsageDuration'] = random.randint(1, 24) if 'MaxUsageDuration' not in data.columns else data['MaxUsageDuration']


def convert_volume(volume):
    number_part = ''.join([char for char in volume if char.isdigit()])
    unit_part = ''.join([char for char in volume if char.isalpha()])
    
    if not number_part:
        return volume  
    
    number_word = p.number_to_words(number_part).capitalize()
    unit_word = "milliliters" if unit_part.lower() == "ml" else "grams"
    
    return f"{number_word} {unit_word}"

def convert_date_format(date_str):
    if pd.isna(date_str):
        return None 
    try:
        return datetime.strptime(str(date_str), '%m/%d/%Y').strftime('%Y-%m-%d')
    except ValueError:
        return None 


date_columns = ['ChemicalCreatedAt', 'ChemicalUpdatedAt', 'ChemicalDateRemoved', 'InitialDateReported', 'MostRecentDateReported', 'DiscontinuedReported']
for col in date_columns:
    if col in data.columns:
        data[col] = data[col].apply(convert_date_format)    


data['ProductId'] = range(1, len(data) + 1)
data['CompanyId'] = range(1, len(data) + 1)
data['ChemicalId'] = range(1, len(data) + 1)
data['CDPHId'] = range(1, len(data) + 1)
data['UsageInfoId'] = range(1, len(data) + 1)
data['PackagingId'] = range(1, len(data) + 1)
data['IngredientId'] = range(1, len(data) + 1)
data['SkinTypeId'] = range(1, len(data) + 1)
data['CategoryId'] = range(1, len(data) + 1)
data['DemographicId'] = range(1, len(data) + 1)


data['ProductVolume'] = data['ProductVolume'].apply(convert_volume)


duplicates = data.duplicated(subset=['ProductId'], keep=False)
if duplicates.any():
    print("Duplicate ProductId values found. Removing duplicates...")
    data = data.drop_duplicates(subset=['ProductId'])

duplicates = data.duplicated(subset=['CompanyId'], keep=False)
if duplicates.any():
    print("Duplicate CompanyId values found. Removing duplicates...")
    data = data.drop_duplicates(subset=['CompanyId']) 

duplicates = data.duplicated(subset=['ChemicalId'], keep=False)
if duplicates.any():
    print("Duplicate ChemicalId values found. Removing duplicates...")
    data = data.drop_duplicates(subset=['ChemicalId'])        


print(f"Number of unique ProductId values: {data['ProductId'].nunique()}")


config = {
    'user': 'root',
    'password': '', #your database password
    'host': 'localhost',
    'database': 'chem_cosmetics',
    'raise_on_warnings': True
}

def create_tables(cursor):
    tables = {}
    tables['CDPH'] = (
        "CREATE TABLE IF NOT EXISTS CDPH ("
        "  CDPHId INT PRIMARY KEY,"
        "  InitialDateReported DATE,"
        "  MostRecentDateReported DATE,"
        "  DiscontinuedReported DATE"
        ") ENGINE=InnoDB")

    tables['Companies'] = (
        "CREATE TABLE IF NOT EXISTS Companies ("
        "  CompanyId INT PRIMARY KEY,"
        "  CompanyName VARCHAR(255)"
        ") ENGINE=InnoDB")

    tables['UsageInfo'] = (
        "CREATE TABLE IF NOT EXISTS UsageInfo ("
        "  UsageInfoId INT PRIMARY KEY,"
        "  DayNightUsage VARCHAR(45),"
        "  MaxUsageDuration INT"
        ") ENGINE=InnoDB")

    tables['Packaging'] = (
        "CREATE TABLE IF NOT EXISTS Packaging ("
        "  PackagingId INT PRIMARY KEY,"
        "  PackagingType VARCHAR(45),"
        "  RecyclablePackaging VARCHAR(45),"
        "  Certifications VARCHAR(45)"
        ") ENGINE=InnoDB")

    tables['Chemicals'] = (
        "CREATE TABLE IF NOT EXISTS Chemicals ("
        "  ChemicalId INT PRIMARY KEY,"
        "  ChemicalName VARCHAR(255),"
        "  CasId INT,"
        "  CasNumber VARCHAR(50),"
        "  ChemicalCreatedAt DATE,"
        "  ChemicalUpdatedAt DATE,"
        "  ChemicalDateRemoved DATE,"
        "  ChemicalCount INT"
        ") ENGINE=InnoDB")

    tables['Ingredient'] = (
        "CREATE TABLE IF NOT EXISTS Ingredient ("
        "  IngredientId INT PRIMARY KEY,"
        "  IngredientCount VARCHAR(10)"
        ") ENGINE=InnoDB")

    tables['SkinType'] = (
        "CREATE TABLE IF NOT EXISTS SkinType ("
        "  SkinTypeId INT PRIMARY KEY,"
        "  SkinType VARCHAR(50)"
        ") ENGINE=InnoDB")

    tables['Categories'] = (
        "CREATE TABLE IF NOT EXISTS Categories ("
        "  CategoryId INT PRIMARY KEY,"
        "  PrimaryCategoryId INT,"
        "  PrimaryCategory VARCHAR(45),"
        "  SubCategoryId INT,"
        "  SubCategory VARCHAR(255)"
        ") ENGINE=InnoDB")

    tables['Demographic'] = (
        "CREATE TABLE IF NOT EXISTS Demographic ("
        "  DemographicId INT PRIMARY KEY,"
        "  Targetdemographic VARCHAR(45)"
        ") ENGINE=InnoDB")

    tables['Products'] = (
        "CREATE TABLE IF NOT EXISTS Products ("
        "  ProductId INT PRIMARY KEY,"
        "  ProductName VARCHAR(255),"
        "  BrandName VARCHAR(255),"
        "  ProductVolume VARCHAR(50),"
        "  ProductRating DECIMAL(3,2),"
        "  ReviewCount DECIMAL(7,2),"
        "  ProductForm VARCHAR(50),"
        "  Packaging_PackagingId INT,"
        "  CDPH_CDPHId INT,"
        "  Companies_CompanyId INT,"
        "  UsageInfo_UsageInfoId INT,"
        "  Categories_CategoryId INT,"
        "  Ingredient_IngredientId INT,"
        "  FOREIGN KEY (Packaging_PackagingId) REFERENCES Packaging(PackagingId),"
        "  FOREIGN KEY (CDPH_CDPHId) REFERENCES CDPH(CDPHId),"
        "  FOREIGN KEY (Companies_CompanyId) REFERENCES Companies(CompanyId),"
        "  FOREIGN KEY (UsageInfo_UsageInfoId) REFERENCES UsageInfo(UsageInfoId),"
        "  FOREIGN KEY (Categories_CategoryId) REFERENCES Categories(CategoryId),"
        "  FOREIGN KEY (Ingredient_IngredientId) REFERENCES Ingredient(IngredientId)"
        ") ENGINE=InnoDB")

    tables['Ingredient_has_Chemicals'] = (
        "CREATE TABLE IF NOT EXISTS Ingredient_has_Chemicals ("
        "  Ingredient_IngredientId INT,"
        "  Chemicals_ChemicalId INT,"
        "  PRIMARY KEY (Ingredient_IngredientId, Chemicals_ChemicalId),"
        "  FOREIGN KEY (Ingredient_IngredientId) REFERENCES Ingredient(IngredientId),"
        "  FOREIGN KEY (Chemicals_ChemicalId) REFERENCES Chemicals(ChemicalId)"
        ") ENGINE=InnoDB")

    tables['Demographic_has_SkinType'] = (
        "CREATE TABLE IF NOT EXISTS Demographic_has_SkinType ("
        "  Demographic_DemographicId INT,"
        "  SkinType_SkinTypeId INT,"
        "  PRIMARY KEY (Demographic_DemographicId, SkinType_SkinTypeId),"
        "  FOREIGN KEY (Demographic_DemographicId) REFERENCES Demographic(DemographicId),"
        "  FOREIGN KEY (SkinType_SkinTypeId) REFERENCES SkinType(SkinTypeId)"
        ") ENGINE=InnoDB")

    for table_name in tables:
        table_description = tables[table_name]
        try:
            print(f"Creating table {table_name}: ", end='')
            cursor.execute(table_description)
            print("OK")
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                print("already exists.")
            else:
                print(err.msg)
        else:
            print("OK")


try:
    cnx = mysql.connector.connect(**config)
    cursor = cnx.cursor()
    print("Successfully connected to the database")


    drop_tables_query = """
    DROP TABLE IF EXISTS Ingredient_has_Chemicals, Demographic_has_SkinType, Products, CDPH, Companies, UsageInfo, Packaging, Chemicals, Ingredient, SkinType, Categories, Demographic;
    """
    cursor.execute(drop_tables_query, multi=True)
    print("Existing tables dropped successfully")


    create_tables(cursor)
    
 
    for index, row in data.iterrows():
        try:
            insert_query = """
            INSERT INTO Products (
                ProductId, ProductName, BrandName, ProductVolume, ProductRating, ReviewCount, ProductForm, 
                Packaging_PackagingId, CDPH_CDPHId, Companies_CompanyId, UsageInfo_UsageInfoId, Categories_CategoryId, 
                Ingredient_IngredientId
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """
            cursor.execute(insert_query, (
                row['ProductId'], row.get('ProductName', None), row.get('BrandName', None), row.get('ProductVolume', None), 
                row.get('ProductRating', None), row.get('ReviewCount', None), row.get('ProductForm', None), row.get('Packaging_PackagingId', None), 
                row.get('CDPH_CDPHId', None), row.get('Companies_CompanyId', None), row.get('UsageInfo_UsageInfoId', None), 
                row.get('Categories_CategoryId', None), row.get('Ingredient_IngredientId', None)
            ))
        except KeyError as e:
            print(f"KeyError: {e} - This column is not in the CSV file, skipping the entry.")
        except mysql.connector.Error as err:
            print(f"MySQL Error: {err.msg} for ProductId: {row['ProductId']}")
    cnx.commit()
    print("Products data inserted successfully")


    if 'SkinTypeId' in data.columns and 'SkinType' in data.columns:
        for index, row in data.iterrows():
            try:
                insert_query = """
                INSERT INTO SkinType (
                    SkinTypeId, SkinType
                ) VALUES (%s, %s);
                """
                cursor.execute(insert_query, (
                    row['SkinTypeId'], row['SkinType']
                ))
            except KeyError as e:
                print(f"KeyError: {e} - This column is not in the CSV file, skipping the entry.")
            except mysql.connector.Error as err:
                print(f"MySQL Error: {err.msg} for SkinTypeId: {row['SkinTypeId']}, SkinType: {row['SkinType']}")
        cnx.commit()
        print("SkinType data inserted successfully")


    if 'CategoryId' in data.columns and 'PrimaryCategoryId' in data.columns and 'PrimaryCategory' in data.columns and 'SubCategoryId' in data.columns and 'SubCategory' in data.columns:
        for index, row in data.iterrows():
            try:
                insert_query = """
                INSERT INTO Categories (
                    CategoryId, PrimaryCategoryId, PrimaryCategory, SubCategoryId, SubCategory
                ) VALUES (%s, %s, %s, %s, %s);
                """
                cursor.execute(insert_query, (
                    row['CategoryId'], row['PrimaryCategoryId'], row['PrimaryCategory'], row['SubCategoryId'], row['SubCategory']
                ))
            except KeyError as e:
                print(f"KeyError: {e} - This column is not in the CSV file, skipping the entry.")
            except mysql.connector.Error as err:
                print(f"MySQL Error: {err.msg} for CategoryId: {row['CategoryId']}")
        cnx.commit()
        print("Categories data inserted successfully")

    if 'DemographicId' in data.columns and 'TargetDemographic' in data.columns:
        for index, row in data.iterrows():
            try:
                insert_query = """
                INSERT INTO Demographic (
                    DemographicId, TargetDemographic
                ) VALUES (%s, %s);
                """
                cursor.execute(insert_query, (
                    row['DemographicId'], row['TargetDemographic']
                ))
            except KeyError as e:
                print(f"KeyError: {e} - This column is not in the CSV file, skipping the entry.")
            except mysql.connector.Error as err:
                print(f"MySQL Error: {err.msg} for DemographicId: {row['DemographicId']}, TargetDemographic: {row['TargetDemographic']}")
        cnx.commit()
        print("Demographic data inserted successfully")

    if 'CompanyId' in data.columns and 'CompanyName' in data.columns:
        for index, row in data.iterrows():
            try:
                insert_query = """
                INSERT INTO Companies (
                    CompanyId, CompanyName
                ) VALUES (%s, %s);
                """
                cursor.execute(insert_query, (
                    row['CompanyId'], row['CompanyName']
                ))
            except KeyError as e:
                print(f"KeyError: {e} - This column is not in the CSV file, skipping the entry.")
            except mysql.connector.Error as err:
                print(f"MySQL Error: {err.msg} for CompanyId: {row['CompanyId']}, CompanyName: {row['CompanyName']}")
        cnx.commit()
        print("Companies data inserted successfully")

    if 'UsageInfoId' in data.columns and 'DayNightUsage' in data.columns and 'MaxUsageDuration' in data.columns:
        for index, row in data.iterrows():
            try:
                insert_query = """
                INSERT INTO UsageInfo (
                    UsageInfoId, DayNightUsage, MaxUsageDuration
                ) VALUES (%s, %s, %s);
                """
                cursor.execute(insert_query, (
                    row['UsageInfoId'], row['DayNightUsage'], row['MaxUsageDuration']
                ))
            except KeyError as e:
                print(f"KeyError: {e} - This column is not in the CSV file, skipping the entry.")
            except mysql.connector.Error as err:
                print(f"MySQL Error: {err.msg} for UsageInfoId: {row['UsageInfoId']}, DayNightUsage: {row['DayNightUsage']}, MaxUsageDuration: {row['MaxUsageDuration']}")
        cnx.commit()
        print("UsageInfo data inserted successfully")

    if 'PackagingId' in data.columns and 'PackagingType' in data.columns and 'RecyclablePackaging' in data.columns and 'Certifications' in data.columns:
        for index, row in data.iterrows():
            try:
                insert_query = """
                INSERT INTO Packaging (
                    PackagingId, PackagingType, RecyclablePackaging, Certifications
                ) VALUES (%s, %s, %s, %s);
                """
                cursor.execute(insert_query, (
                    row['PackagingId'], row['PackagingType'], row['RecyclablePackaging'], row['Certifications']
                ))
            except KeyError as e:
                print(f"KeyError: {e} - This column is not in the CSV file, skipping the entry.")
            except mysql.connector.Error as err:
                print(f"MySQL Error: {err.msg} for PackagingId: {row['PackagingId']}, PackagingType: {row['PackagingType']}, RecyclablePackaging: {row['RecyclablePackaging']}, Certifications: {row['Certifications']}")
        cnx.commit()
        print("Packaging data inserted successfully")

    if 'ChemicalId' in data.columns and 'ChemicalName' in data.columns and 'CasId' in data.columns and 'CasNumber' in data.columns and 'ChemicalCreatedAt' in data.columns and 'ChemicalUpdatedAt' in data.columns and 'ChemicalDateRemoved' in data.columns and 'ChemicalCount' in data.columns:
        for index, row in data.iterrows():
            try:
                insert_query = """
                INSERT INTO Chemicals (
                    ChemicalId, ChemicalName, CasId, CasNumber, ChemicalCreatedAt, ChemicalUpdatedAt, ChemicalDateRemoved, ChemicalCount
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
                """
                cursor.execute(insert_query, (
                    row['ChemicalId'], row['ChemicalName'], row['CasId'], row['CasNumber'], row['ChemicalCreatedAt'], row['ChemicalUpdatedAt'], row['ChemicalDateRemoved'], row['ChemicalCount']
                ))
            except KeyError as e:
                print(f"KeyError: {e} - This column is not in the CSV file, skipping the entry.")
            except mysql.connector.Error as err:
                print(f"MySQL Error: {err.msg} for ChemicalId: {row['ChemicalId']}, ChemicalName: {row['ChemicalName']}")
        cnx.commit()
        print("Chemicals data inserted successfully")

    if 'IngredientId' in data.columns and 'IngredientCount' in data.columns:
        for index, row in data.iterrows():
            try:
                insert_query = """
                INSERT INTO Ingredient (
                    IngredientId, IngredientCount
                ) VALUES (%s, %s);
                """
                cursor.execute(insert_query, (
                    row['IngredientId'], row['IngredientCount']
                ))
            except KeyError as e:
                print(f"KeyError: {e} - This column is not in the CSV file, skipping the entry.")
            except mysql.connector.Error as err:
                print(f"MySQL Error: {err.msg} for IngredientId: {row['IngredientId']}, IngredientCount: {row['IngredientCount']}")
        cnx.commit()
        print("Ingredient data inserted successfully")

    if 'CDPHId' in data.columns and 'InitialDateReported' in data.columns and 'MostRecentDateReported' in data.columns and 'DiscontinuedReported' in data.columns:
        for index, row in data.iterrows():
            try:
                insert_query = """
                INSERT INTO CDPH (
                    CDPHId, InitialDateReported, MostRecentDateReported, DiscontinuedReported
                ) VALUES (%s, %s, %s, %s);
                """
                cursor.execute(insert_query, (
                    row['CDPHId'], row['InitialDateReported'], row['MostRecentDateReported'], row['DiscontinuedReported']
                ))
            except KeyError as e:
                print(f"KeyError: {e} - This column is not in the CSV file, skipping the entry.")
            except mysql.connector.Error as err:
                print(f"MySQL Error: {err.msg} for CDPHId: {row['CDPHId']}, InitialDateReported: {row['InitialDateReported']}")
        cnx.commit()
        print("CDPH data inserted successfully")

    if 'IngredientId' in data.columns and 'ChemicalId' in data.columns:
        for index, row in data.iterrows():
            try:
                insert_query = """
                INSERT INTO Ingredient_has_Chemicals (
                    Ingredient_IngredientId, Chemicals_ChemicalId
                ) VALUES (%s, %s);
                """
                cursor.execute(insert_query, (
                    row['IngredientId'], row['ChemicalId']
                ))
            except KeyError as e:
                print(f"KeyError: {e} - This column is not in the CSV file, skipping the entry.")
            except mysql.connector.Error as err:
                print(f"MySQL Error: {err.msg} for IngredientId: {row['IngredientId']}, ChemicalId: {row['ChemicalId']}")
        cnx.commit()
        print("Ingredient_has_Chemicals data inserted successfully")

    if 'DemographicId' in data.columns and 'SkinTypeId' in data.columns:
        for index, row in data.iterrows():
            try:
                insert_query = """
                INSERT INTO Demographic_has_SkinType (
                    Demographic_DemographicId, SkinType_SkinTypeId
                ) VALUES (%s, %s);
                """
                cursor.execute(insert_query, (
                    row['DemographicId'], row['SkinTypeId']
                ))
            except KeyError as e:
                print(f"KeyError: {e} - This column is not in the CSV file, skipping the entry.")
            except mysql.connector.Error as err:
                print(f"MySQL Error: {err.msg} for DemographicId: {row['DemographicId']}, SkinTypeId: {row['SkinTypeId']}")
        cnx.commit()
        print("Demographic_has_SkinType data inserted successfully")

except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("Something is wrong with your user name or password")
    elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print("Database does not exist")
    else:
        print(err)
finally:
    cursor.close()
    cnx.close()
