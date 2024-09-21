from dotenv import load_dotenv
import os
from datetime import datetime
from AirQualityAdmin import AirQualityAdmin
from EPA_AirQualityDBHandler import EPA_AirQualityDBHandler
from EPA_AirQualityDataUpdater import EPA_AirQualityDataUpdater

def main():
    now = datetime.now()
    current_dir = os.getcwd()

    # =========================================================================
    # Set up logging attributes
    # =========================================================================
    log_path_info = os.path.join( 
        current_dir
        , 'logs' #put the log in the logs folder of the current directory
        , f"EPA_API_UpdateBackground_INFO_{ now.strftime( '%Y%m%d' ) }.log" 
    )
    log_path_error = os.path.join( 
        current_dir
        , 'logs' #put the log in the logs folder of the current directory
        , f"EPA_API_UpdateBackground_ERROR_{ now.strftime( '%Y%m%d' ) }.log" 
    )
    log_console = True

    # =========================================================================
    # Grab database attributes from environment file
    # =========================================================================
    dotenv_path = os.path.join(
        current_dir
        , 'config' #check in the config folder of the current directory
        , 'Update_Background_Task.env'
    )
    load_dotenv( dotenv_path )
    username = os.getenv( 'DB_USERNAME' )
    password = os.getenv( 'DB_PASSWORD' )
    server = os.getenv( 'DB_SERVER' )
    EPA_API_Email = os.getenv( 'EPA_API_EMAIL' )
    EPA_API_Key = os.getenv( 'EPA_API_KEY' )

    # =========================================================================
    # Set up additional attributes
    # =========================================================================
    database = 'AirQuality_Staging'
    table_name = 'EPA_API_Raw'
    aqsids = [ 
        '320030043'     # Paul Meyer
        , '320030044'   # Mountains Edge
        , '320030071'   # Walter Johnson
        , '320030073'   # Palo Verde
        , '320030075'   # Joe Neal
        , '320030299'   # Liberty High School
        , '320030540'   # Jerome Mack
        , '320030561'   # Sunrise Acres
        , '320031501'   # Rancho Teddy
        , '320031502'   # Casino Center
        , '320032003'   # Walnut Rec.
    ]

    params = [
        '86101'     # PM10
        , '88101'   # PM2.5
        , '42101'   # CO - Carbon Monoxide
        , '42401'   # SO2 - Sulfur Dioxide
        , '42602'   # NO2 - Nitrogen Dioxide
        , '62101'   # TEMP - Outdoor Temperature
        , '61104'   # RWD - Wind Direction Resultant
        , '61103'   # RWS - Wind Speed Resultant
        , '44201'   # OZONE - Ozone
        , '64101'   # BARPR - Barometric Pressure
        , '62201'   # RHUM - Relative Humidity
        , '65102'   # Rain/melt precipitation
        , '63302'   # Ultraviolet radiation
        , '63301'   # SRAD - Solar Radiation
    ]
    
    # =========================================================================
    # Instaniate our AirQualityAdmin object with logging attributes from above
    # =========================================================================
    myAirQualityAdmin = AirQualityAdmin( 
        InfoLogFile = log_path_info
        , ErrorLogFile = log_path_error
        , logToConsole = log_console 
    )

    # =========================================================================
    # Instantiate our AirNow DB Handler object with attributes from above
    # =========================================================================
    myDBHandler = EPA_AirQualityDBHandler(
        server = server
        , database = database
        , username = username
        , password = password
        , port = None
        , log = myAirQualityAdmin.Logger
    )

    # =========================================================================
    # Instaniate our AirNow Data Updater object with the attributes from above
    # =========================================================================
    updater = EPA_AirQualityDataUpdater( 
        database = database
        , staging_tablename = table_name
        , EPA_Email = EPA_API_Email
        , EPA_Key = EPA_API_Key
        , AQSIDs = aqsids
        , params = params
        , DBHandler = myDBHandler
        , log = myAirQualityAdmin.Logger
    )
    
    # =========================================================================
    # continuously run the service
    # TODO: look into a more elegant way to stop
    # =========================================================================
    #while True:
    beginDate = datetime.strptime('1/1/2014', '%m/%d/%Y')
    endDate = datetime.strptime('12/31/2024', '%m/%d/%Y')
    updater.runUpdate( beginDate = beginDate, endDate = endDate )
        
        # Wait until quarter past the next hour
    #    myAirQualityAdmin.waitUntilNextDay( hour = 12, minute = 15 )

if __name__ == "__main__":
    main()
