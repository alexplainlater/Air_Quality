from dotenv import load_dotenv
import sqlalchemy as SA
import pandas as pd
import os
import zipfile
import io
import requests

# =========================================================================
# This script will clear out any existing AirQuality_DW.dbo.Sites table
# and replace its data with the most currently available sites data
# from the EPA website.  It also drops any existing and creates an 
# AQI breakpoints lookup table.
# =========================================================================

Database = 'AirQuality_DW'
TableName = 'Sites'

# =========================================================================
# Grab database attributes from environment file
# =========================================================================
current_dir = os.getcwd()
dotenv_path = os.path.join(
    current_dir
    , 'config' #check in the config folder of the current directory
    , 'Update_Background_Task.env'
)
load_dotenv( dotenv_path )
username = os.getenv( 'DB_USERNAME' )
password = os.getenv( 'DB_PASSWORD' )
server = os.getenv( 'DB_SERVER' )

# =========================================================================
# Set up connection to database
# =========================================================================
dialect_string = "mssql+pyodbc"
alchemy_url_object = SA.URL.create(
    dialect_string
    , username = username
    , password = password
    , host = server
    , port = None
    , database = Database
    , query={"driver": "ODBC Driver 17 for SQL Server"}
)
try:
    Engine = SA.create_engine( alchemy_url_object )
except Exception as e:
    log_message = f"Error creating SQL engine with url: {alchemy_url_object}. Exception received: {e}"
    print( log_message )

# =========================================================================
# Grab sites file and load into AirQuality_DW.dbo.Sites
# =========================================================================

log_message = 'Grabbing site file'
print(log_message)

# Step 1: Download the ZIP file
url = 'https://aqs.epa.gov/aqsweb/airdata/aqs_sites.zip'
response = requests.get( url )

log_message = 'Extracting site file'
print(log_message)

# Step 2: Extract the CSV file from the ZIP archive and load it into a DataFrame
with zipfile.ZipFile( io.BytesIO( response.content ) ) as z:
    with z.open( 'aqs_sites.csv' ) as f:
        log_message = 'Loading site file into data frame.'
        print(log_message)
        df = pd.read_csv( f, dtype = str )
        df.fillna( '', inplace = True )

# Step 3: Load into SQL
log_message = 'Loading site file into SQL.'
print(log_message)

try:
    with Engine.connect() as conn:
        total_inserted = 0
        conn.execute("TRUNCATE TABLE AirQuality_DW.dbo.Sites;")
        for index, row in df.iterrows():
            insert_stmt = SA.text( f"""
                INSERT INTO {Database}.dbo.{TableName}
                (
                    Full_Site_Number, State_Code, County_Code, Site_Number, Latitude, Longitude, Geog, Datum, Elevation
                    , Land_Use, Location_Setting, Site_Established_Date, Site_Closed_Date, GMT_Offset, Owning_Agency
                    , Local_Site_Name, Address, ZIP_Code, State_Name, County_Name, City_Name, CBSA_Name, Tribe_Name
                    , INSERT_DT
                )
                SELECT
                    Full_Site_Number = CONVERT( CHAR(11), :State_Code + '-' + :County_Code + '-' + :Site_Number )
                    , State_Code = CONVERT( CHAR(2), :State_Code )
                    , County_Code = CONVERT( CHAR(3), :County_Code )
                    , Site_Number = CONVERT( CHAR(4), :Site_Number )
                    , Latitude = CONVERT( DECIMAL(9,6), NULLIF( :Latitude, '' ) )
                    , Longitude = CONVERT( DECIMAL(9,6), NULLIF( :Longitude, '' ) )
                    , Geog = CONVERT( GEOGRAPHY, 
                        GEOGRAPHY::STGeomFromText( 
                            'POINT( ' 
                                + CONVERT( VARCHAR, NULLIF( :Longitude, '' ) ) 
                                + ' ' 
                                + CONVERT( VARCHAR, NULLIF( :Latitude, '' ) ) 
                            + ')'
                            , CASE WHEN :Datum = 'WGS84' THEN 4326 WHEN :Datum = 'NAD27' THEN 4267 WHEN :Datum = 'NAD83' THEN 4269 ELSE 4326 END 
                        ) 
                    )
                    , Datum = CONVERT( CHAR(5), NULLIF( :Datum, '' ) )
                    , Elevation = CONVERT( DECIMAL(12,6), NULLIF( :Elevation, '' ) )
                    , Land_Use = CONVERT( VARCHAR(25), NULLIF( :Land_Use, '' ) )
                    , Location_Setting = CONVERT( VARCHAR(25), NULLIF( :Location_Setting, '' ) )
                    , Site_Established_Date = CONVERT( DATE, NULLIF( :Site_Established_Date, '' ) )
                    , Site_Closed_Date = CONVERT( DATE, NULLIF( :Site_Closed_Date, '' ) )
                    , GMT_Offset = CONVERT( SMALLINT, NULLIF( :GMT_Offset, '' ) )
                    , Owning_Agency = CONVERT( VARCHAR(100), NULLIF( :Owning_Agency, '' ) )
                    , Local_Site_Name = CONVERT( VARCHAR(100), NULLIF( :Local_Site_Name, '' ) )
                    , Address = CONVERT( VARCHAR(50), NULLIF( UPPER( :Address ), '' ) )
                    , ZIP_Code = CONVERT( VARCHAR(5), NULLIF( :ZIP_Code, '' )  )
                    , State_Name = CONVERT( VARCHAR(30), NULLIF( :State_Name, '' ) )
                    , County_Name = CONVERT( VARCHAR(50), NULLIF( :County_Name, '' ) )
                    , City_Name = CONVERT( VARCHAR(50), NULLIF( :City_Name, '' ) )
                    , CBSA_Name = CONVERT( VARCHAR(100), NULLIF( :CBSA_Name, '' )  )
                    , Tribe_Name = CONVERT( VARCHAR(100), NULLIF( :Tribe_Name, '' ) )
                    , INSERT_DT = CONVERT( SMALLDATETIME, GETDATE() )

            """ )
            conn.execute( insert_stmt, {
                'State_Code': row['State Code']
                , 'County_Code': row['County Code']
                , 'Site_Number': row['Site Number']
                , 'Latitude': row['Latitude']
                , 'Longitude': row['Longitude']
                , 'Datum': row['Datum']
                , 'Elevation': row['Elevation']
                , 'Land_Use': row['Land Use']
                , 'Location_Setting': row['Location Setting']
                , 'Site_Established_Date': row['Site Established Date']
                , 'Site_Closed_Date': row['Site Closed Date']
                , 'GMT_Offset': row['GMT Offset']
                , 'Owning_Agency': row['Owning Agency']
                , 'Local_Site_Name': row['Local Site Name']
                , 'Address': row['Address']
                , 'ZIP_Code': row['Zip Code']
                , 'State_Name': row['State Name']
                , 'County_Name': row['County Name']
                , 'City_Name': row['City Name']
                , 'CBSA_Name': row['CBSA Name']
                , 'Tribe_Name': row['Tribe Name']
                , 'Extraction_Date': row['Extraction Date']
            } )                    
            total_inserted += 1
            conn.commit()

    log_message = f"Data successfully inserted into SQL Server. Total records inserted: {total_inserted}"
    print( log_message )
except Exception as e:
    log_message = f"Error inserting data into SQL Server: {e}"
    print( log_message )

# =========================================================================
# Create and populate AQI Breakpoints lookup table in SQL
# =========================================================================
log_message = 'Create and populate AQI Breakpoints lookup table in SQL.'
print(log_message)

TableName = 'lkp_AQI_Breakpoints'
try:
    with Engine.connect() as conn:
        create_stmt = SA.text( f"""
            IF OBJECT_ID( '{Database}.dbo.{TableName}', 'U' ) IS NOT NULL
                DROP TABLE {Database}.dbo.{TableName}
            CREATE TABLE {Database}.dbo.{TableName}
            (
                Pollutant VARCHAR(100)   
                , BreakpointLo DECIMAL(9, 5)
                , BreakpointHi DECIMAL(9, 5)
                , AQILo DECIMAL(9, 5)
                , AQIHi DECIMAL(9, 5)
            )
        """ )
        conn.execute( create_stmt )
        conn.commit()

        insert_stmt = SA.text( f"""
            INSERT INTO {Database}.dbo.{TableName} 
                ( Pollutant, BreakpointLo, BreakpointHi, AQILo, AQIHi )
            VALUES
                ( 'O3 - 8hr', 0.000, 0.054, 0, 50 )
                , ( 'O3 - 8hr', 0.055, 0.070, 51, 100 )
                , ( 'O3 - 8hr', 0.071, 0.085, 101, 150 )
                , ( 'O3 - 8hr', 0.086, 0.105, 151, 200 )
                , ( 'O3 - 8hr', 0.106, 0.200, 201, 300 )

                , ( 'O3 - 1hr', 0.125, 0.164, 101, 150 )
                , ( 'O3 - 1hr', 0.165, 0.204, 151, 200 )
                , ( 'O3 - 1hr', 0.205, 0.404, 201, 300 )
                , ( 'O3 - 1hr', 0.405, 0.604, 301, 500 )
                
                , ( 'PM25', 0.0, 9.0, 0, 50 )
                , ( 'PM25', 9.1, 35.4, 51, 100 )
                , ( 'PM25', 35.5, 55.4, 101, 150 )
                , ( 'PM25', 55.5, 125.4, 151, 200 )
                , ( 'PM25', 125.5, 225.4, 201, 300 )
                , ( 'PM25', 225.5, 325.4, 301, 500 )

                , ( 'PM10', 0, 54, 0, 50 )
                , ( 'PM10', 55, 154, 51, 100 )
                , ( 'PM10', 155, 254, 101, 150 )
                , ( 'PM10', 255, 354, 151, 200 )
                , ( 'PM10', 355, 424, 201, 300 )
                , ( 'PM10', 425, 604, 301, 500 )

                , ( 'CO', 0.0, 4.4, 0, 50 )
                , ( 'CO', 4.5, 9.4, 51, 100 )
                , ( 'CO', 9.5, 12.4, 101, 150 )
                , ( 'CO', 12.5, 15.4, 151, 200 )
                , ( 'CO', 15.5, 30.4, 201, 300 )
                , ( 'CO', 30.5, 50.4, 301, 500 )

                , ( 'SO2', 0, 35, 0, 50 )
                , ( 'SO2', 36, 75, 51, 100 )
                , ( 'SO2', 76, 185, 101, 150 )
                , ( 'SO2', 186, 304, 151, 200 )
                , ( 'SO2', 305, 604, 201, 300 )
                , ( 'SO2', 605, 1004, 301, 500 )

                , ( 'NO2', 0, 53, 0, 50 )
                , ( 'NO2', 54, 100, 51, 100 )
                , ( 'NO2', 101, 360, 101, 150 )
                , ( 'NO2', 361, 649, 151, 200 )
                , ( 'NO2', 650, 1249, 201, 300 )
                , ( 'NO2', 1250, 2049, 301, 500 )
        """ )
        conn.execute( insert_stmt )
        conn.commit()
    log_message = f"Data successfully inserted into SQL Server."
    print( log_message )
except Exception as e:
    log_message = f"Error inserting data into SQL Server: {e}"
    print( log_message )