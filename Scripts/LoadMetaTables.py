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
# from the EPA website.
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