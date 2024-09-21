from dotenv import load_dotenv
import os
from datetime import datetime
from AirQualityAdmin import AirQualityAdmin
from AirNow_AirQualityDBHandler import AirNow_AirQualityDBHandler
from AirNow_AirQualityDataUpdater import AirNow_AirQualityDataUpdater

def main():
    now = datetime.now()
    current_dir = os.getcwd()

    # =========================================================================
    # Set up logging attributes
    # =========================================================================
    log_path_info = os.path.join( 
        current_dir
        , 'logs' #put the log in the logs folder of the current directory
        , f"AirNowUpdateBackground_INFO_{ now.strftime( '%Y%m%d' ) }.log" 
    )
    log_path_error = os.path.join( 
        current_dir
        , 'logs' #put the log in the logs folder of the current directory
        , f"AirNowUpdateBackground_ERROR_{ now.strftime( '%Y%m%d' ) }.log" 
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

    # =========================================================================
    # Set up additional attributes
    # =========================================================================
    database = 'AirQuality_Staging'
    table_name = 'AirNowData'
    aqsids = [ '320030043', '320030044', '320030071', '320030073', '320030075'
              , '320030299', '320030540', '320030561', '320031501', '320031502'
              , '320032003' ]
    
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
    myDBHandler = AirNow_AirQualityDBHandler(
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
    updater = AirNow_AirQualityDataUpdater( 
        database = database
        , staging_tablename = table_name
        , AQSIDs = aqsids
        , DBHandler = myDBHandler
        , log = myAirQualityAdmin.Logger
    )
    
    # =========================================================================
    # continuously run the service
    # TODO: look into a more elegant way to stop
    # =========================================================================
    while True:
        updater.runUpdate()
        
        # Wait until quarter past the next hour
        myAirQualityAdmin.waitUntilNextHour( 15 )

if __name__ == "__main__":
    main()
