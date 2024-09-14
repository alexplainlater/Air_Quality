import logging
import time
from datetime import datetime, timedelta
from AirQualityDBHandler import AirQualityDBHandler

class AirQualityDataUpdater:
    """
        Base class for AirQuality data updater
        
        Creates a logger object and a DBHandler with the provided credientials
        and stores the list of AQSIDs we're interested in updating.

        Requires a runUpdate method be implemented.

    """
    def __init__( self, server, database, username, password, AQSIDs, InfoLogFile = None, ErrorLogFile = None, logToConsole = True ):
        self.AQSIDs = AQSIDs
        self.logger = self._create_logger( InfoLogFile, ErrorLogFile, logToConsole )
        self.DBHandler = AirQualityDBHandler( 
            server
            , database
            , username
            , password
            , log = self.logger 
        )

    def runUpdate( self ):
        raise NotImplementedError("Subclasses must implement this method")

    def _create_logger( self, infoLogFile = None, errorLogFile = None, logToConsole = True ):            
        """
            Set up and configure the logger for this class.

            Parameters:
                infoLogFile (str): Full location and file name for information messages. None if not wanted.
                errorLogFile (str): Full location and file name for error messages. None if not wanted.
                logToConsole (bool): Output log info and errors to console.
            Returns:
                Logger with configurations applied.
        """
       
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.DEBUG)

        # Create a formatter
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

        # Create and add handlers
        if infoLogFile:
            info_handler = logging.FileHandler(infoLogFile)
            info_handler.setLevel(logging.INFO)
            info_handler.setFormatter(formatter)
            logger.addHandler(info_handler)

        if errorLogFile:
            error_handler = logging.FileHandler(errorLogFile)
            error_handler.setLevel(logging.ERROR)
            error_handler.setFormatter(formatter)
            logger.addHandler(error_handler)

        if logToConsole:
            console_handler = logging.StreamHandler()
            console_handler.setLevel(logging.DEBUG)  # Set to DEBUG to capture all levels
            console_handler.setFormatter(formatter)
            logger.addHandler(console_handler)

        return logger
    
    def waitUntilNextHour( self, TargetMinute = 15 ):        
        """
            Function puts the process to sleep until the next hour (not necessarily an hour elapsed) 
            and resumes on the minute of the next hour provided.

            Parameters:
                TargetMinute (int): Value between 0 and 59 to represent the minute in the hour to resume

            Returns:
                None: Holds up processing until next hour + TargetMinute has elapsed

            Examples:

            Example 1:
                Current Time: 9:30am
                TargetMinute = 15
                Next Execution: 10:15am
            
            Example 2:
                Current Time: 9:30am
                Target Minute = 45
                Next Execution: 10:45am

            Example 3:
                Current Time: 9:59am
                Target Minute = 0
                Next Execution: 10:00am
        """
        now = datetime.now()

        # Calculate the next full hour
        next_hour = ( now + timedelta( hours = 1 ) ).replace( minute = TargetMinute, second = 0, microsecond = 0 )
        
        self.logger.info( f"Finished updating at { now.strftime( '%Y-%m-%d %H:%M' ) }, going to sleep for an hour until { next_hour.strftime( '%Y-%m-%d %H:%M' ) }." )
        sleep_duration = ( next_hour - now ).total_seconds()

        time.sleep( sleep_duration )

    