import time
import requests
import logging
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone
from io import StringIO
from EPA_AirQualityDBHandler import EPA_AirQualityDBHandler
class EPA_AirQualityDataUpdater:
    """
    
    """

    def __init__( self, database: str, staging_tablename: str, EPA_Email: str, EPA_Key: str, AQSIDs: list[str], params: list[str], DBHandler: EPA_AirQualityDBHandler, log: logging = None ):
        self.Database = database
        self.EPA_Staging_Table = staging_tablename
        self.EPA_API_EMAIL = EPA_Email
        self.EPA_API_KEY = EPA_Key
        self.AQSIDs = AQSIDs
        self.Params = params
        self.DBHandler = DBHandler
        self.Log = log        
        
        # Ensure AirNow table exists in the database.  If not, create it.
        self.DBHandler.setStagingTable( self.EPA_Staging_Table, True )

    def _split_list( self, lst: list[ str ], n: int ) -> list[ list[ str ] ]:
        """Split list into chunks of size n."""
        return [ lst[ i:i + n ] for i in range( 0, len( lst ), n ) ]
    
    def runUpdate( self, beginDate: datetime, endDate: datetime = None, specificAQSIDs: list[str] = None, specificParamsToUpdate: list[str] = None ) -> None:
        AQSIDsToCheck = specificAQSIDs if specificAQSIDs else self.AQSIDs
        ParamsToUpdate = specificParamsToUpdate if specificParamsToUpdate else self.Params

        for aqsid in AQSIDsToCheck:
            state = aqsid[:2]   # state code is the first 2 characters of an AQSID
            county = aqsid[2:5] # county code is the next 3 characters of an AQSID
            site = aqsid[5:]    # site code is the final 4 characters of an AQSID

            log_message = f"Starting to process AQSID: {aqsid}"
            self.Log.info( log_message ) if self.Log else print( log_message )

            current_bdate = beginDate
            # The EPA API requires that all services must have the end date (edate field) be in the same year as the begin date (bdate field).
            while current_bdate < endDate:
                edate = endDate if current_bdate == endDate.year else datetime( current_bdate.year, 12, 31 )
                
                log_message = f"Gathering data from begin date: { current_bdate.strftime( '%Y%m%d' ) } to end date: { edate.strftime( '%Y%m%d' ) }"
                self.Log.info( log_message ) if self.Log else print( log_message )

                # The EPA API only allows a maximum of 5 params to be retrieved in one call
                for params_chunk in self._split_list( ParamsToUpdate, 5 ):
                    api_url = (
                        'https://aqs.epa.gov/data/api/sampleData/bySite?email=' + self.EPA_API_EMAIL
                        + '&key=' + self.EPA_API_KEY
                        + '&param=' + ','.join( params_chunk )
                        + '&bdate=' + current_bdate.strftime( '%Y%m%d' )
                        + '&edate=' + edate.strftime( '%Y%m%d' )
                        + '&state=' + state
                        + '&county=' + county
                        + '&site=' + site
                    )

                    # The EPA API requires us not to make more than 10 requests per minute and a pause of at least 5 seconds between requests.
                    # I'll wait at least 7 seconds between requests
                    time.sleep( 7 )
                    
                    log_message = f"Requesting API URL: {api_url}"
                    self.Log.info( log_message ) if self.Log else print( log_message )

                    response = requests.get( api_url )
                    if response.status_code == 200:
                        json_data = response.json()
                        data_section = json_data.get( "Data", [] )
                        df = pd.DataFrame( data_section )
                        if df.empty:
                            log_message = f"No data to insert from this site, parameters, and time frame.  Moving on."
                            self.Log.info( log_message ) if self.Log else print( log_message )
                        else:
                            # Replace NaN and null values with None
                            df.replace( { np.nan: None }, inplace = True )
                            
                            # Ensure some data types are properly applied
                            df['sample_measurement'] = df['sample_measurement'].astype(float)
                            df['detection_limit'] = df['detection_limit'].astype(float)
                            
                            log_message = f"Inserting data into staging table."
                            self.Log.info( log_message ) if self.Log else print( log_message )

                            self.DBHandler.insertIntoStagingTable( df = df, file_url = api_url, chunk_size = 50 )
                            
                    else:
                        log_message = f"Failed to retrieve data from {api_url}: {response.status_code}"
                        self.Log.error( log_message ) if self.Log else print( log_message )
                current_bdate = datetime( current_bdate.year + 1, 1, 1 )