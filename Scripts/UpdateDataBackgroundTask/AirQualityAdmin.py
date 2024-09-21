import logging
import time
from datetime import datetime, timedelta

class AirQualityAdmin:
    """
        
    """
    def __init__( self, InfoLogFile = None, ErrorLogFile = None, logToConsole = True ):
        self.Logger = self._create_logger( InfoLogFile, ErrorLogFile, logToConsole )

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

    def waitUntilNextDay( self, TargetHour = 10, TargetMinute = 15 ):        
        """
            Function puts the process to sleep until the next day (not necessarily a day elapsed) 
            and resumes on the hour and minute of the next day provided.

            Parameters:
                TargetHour (int): Value between 0 and 23 to represent the hour of the next day to resume
                TargetMinute (int): Value between 0 and 59 to represent the minute in the hour to resume

            Returns:
                None: Holds up processing until next day + TargetHour + TargetMinute has elapsed

            Examples:

            Example 1:
                Current DateTime: 1/1/2024 9:30am
                TargetHour = 10
                TargetMinute = 15
                Next Execution: 1/2/2024 10:15am
            
            Example 2:
                Current Time: 1/1/2024 11:30pm
                TargetHour = 0
                Target Minute = 15
                Next Execution: 1/2/2024 12:15am
        """
        now = datetime.now()

        # Calculate the next full day
        next_day = ( now + timedelta( days = 1 ) ).replace( hour = TargetHour, minute = TargetMinute, second = 0, microsecond = 0 )
        
        self.logger.info( f"Finished updating at { now.strftime( '%Y-%m-%d %H:%M' ) }, going to sleep for a day until { next_day.strftime( '%Y-%m-%d %H:%M' ) }." )
        sleep_duration = ( next_day - now ).total_seconds()

        time.sleep( sleep_duration )