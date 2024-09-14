from dotenv import load_dotenv
import os
from AirNow_AirQualityDataUpdater import AirNow_AirQualityDataUpdater
from datetime import datetime

def main():
    now = datetime.now()
    current_dir = os.getcwd()

    # set up logging attributes
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

    # grab database attributes from environment file
    dotenv_path = os.path.join(
        current_dir
        , 'config' #check in the config folder of the current directory
        , 'AirNow_Update_Background_Task.env'
    )
    load_dotenv( dotenv_path )
    username = os.getenv( 'DB_USERNAME' )
    password = os.getenv( 'DB_PASSWORD' )
    server = os.getenv( 'DB_SERVER' )

    # set up additional attributes
    database = 'AirQuality'
    table_name = 'AirNowData'
    aqsids = [ '320030043', '320030044', '320030071', '320030073', '320030075'
              , '320030299', '320030540', '320030561', '320031501', '320031502'
              , '320032003' ]

    # instaniate our AirNow updater object with the attributes from above
    updater = AirNow_AirQualityDataUpdater( 
        server
        , database
        , username
        , password
        , table_name
        , aqsids
        , log_path_info
        , log_path_error
        , log_console 
    )
    
    # continuously run the service
    # TODO: look into a more elegant way to stop
    while True:
        updater.runUpdate()
        
        # Wait until quarter past the next hour
        updater.waitUntilNextHour( 15 )

if __name__ == "__main__":
    main()
