import time
import requests
import logging
import pandas as pd
from datetime import datetime, timedelta, timezone
from io import StringIO
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from AirNow_AirQualityDBHandler import AirNow_AirQualityDBHandler

class AirNow_AirQualityDataUpdater:
    """
        - Uses: 
            - AirQualityDBHandler
            - Pandas
            - BeautifulSoup
            - Selenium
    """

    def __init__( self, database: str, staging_tablename: str, AQSIDs: list[str], DBHandler: AirNow_AirQualityDBHandler, log: logging = None ):
        self.Database = database
        self.airNowTable = staging_tablename
        self.AQSIDs = AQSIDs
        self.DBHandler = DBHandler
        self.Log = log
        
        # Ensure AirNow table exists in the database.  If not, create it.
        self.DBHandler.setStagingTable( self.airNowTable, True )

    def runUpdate( self ) -> None:
        """
            Main driver of class
            - get the last inserted date and hour (DB Handler)
            - check for available files 
                - starting with the last inserted date going until now
            - download and process files
                - get the file
                - read pipe-delimited csv into dataframe
                - insert pertinent records into the AirNow staging table (DB Handler)
            - update DW fact tables (DB Handler)
        """
        lastDateFound, lastHourFound = self.DBHandler.getLastInsertedDate( self.AQSIDs )
        
        current_date = datetime.now( timezone.utc ).date() #file names are based on GMT time
        
        # Iterate through each day from the last inserted date to the current date
        date_to_check = lastDateFound
        while date_to_check <= current_date:
            available_files = self.check_for_available_files( date_to_check )
            for file_date, hour in available_files:
                if file_date == lastDateFound and hour <= lastHourFound:
                    # skip
                    self.Log.debug( f"Skipping file with date: {file_date} and hour: {hour}" )
                else:
                    self.download_and_process_files( file_date, hour )
            date_to_check += timedelta( days = 1 )
        self.DBHandler.updateDWFactTables()
    
    def check_for_available_files( self, dateToCheck: datetime ) -> list[tuple[datetime, int]]:
        """
            Checks the AirNow website for a list of files given a date to check.
            Uses Selenium and a Chrome WebDriver to allow the Javascript to load
            the content.  Uses BeautifulSoup to parse the available files.

            Parameters:
                dateToCheck (date) - which day to check for files

            Returns:
                List of tuples of dates and hours available for download
        """
        # Set up Chrome options for WebDriver
        chrome_options = Options()
        chrome_options.add_argument( "--headless" )  # Browser window isn't visible
        chrome_options.add_experimental_option( 'excludeSwitches', ['enable-logging'] )  # Suppress logging

        # Set up the WebDriver with the options created above
        driver = webdriver.Chrome( options = chrome_options )

        base_url = f'https://files.airnowtech.org/?prefix=airnow/{dateToCheck.strftime("%Y")}/{dateToCheck.strftime("%Y%m%d")}/'

        try:
            driver.get( base_url )
            time.sleep(  5 )  # Wait 5 seconds for JavaScript to load the content

            # Parse the page with BeautifulSoup
            soup = BeautifulSoup( driver.page_source, 'html.parser' )
            driver.quit()
            links = soup.find_all( 'a' )
            files = []
            
            for link in links:
                href = link.get( 'href' )
                if href and f'HourlyData_{dateToCheck.strftime("%Y%m%d")}' in href:
                    hour = int( href.split( '_' )[-1].split( '.' )[0][-2:] )
                    files.append( ( dateToCheck, hour ) )
            return files
        except requests.exceptions.RequestException as e:
            self.Log.error( f"Error checking for new files: {e}" )
            return []

    def download_and_process_files( self, date: datetime, hour: int ) -> None:
        """
            Downloads a file from the AirNow website given a date and hour.
            Calls the process_file function once downloaded.
        """
        date_str = date.strftime( '%Y%m%d' )
        hour_str = str( hour ).zfill( 2 )
        file_url = f'https://files.airnowtech.org/airnow/{date.year}/{date_str}/HourlyData_{date_str}{hour_str}.dat'
        self.Log.debug( f"Fetching file: {file_url}" )
        try:
            response = requests.get( file_url )
            response.raise_for_status()
            file_content = response.text
            self.Log.info( f"Processing file: {file_url}" )
            self._process_file( file_content, file_url )            
        except requests.exceptions.RequestException as e:
            self.Log.error( f"Failed to download {file_url}: {e}" )

    def _process_file( self, file_content: str, file_url: str ) -> None:
        """
            Reads the file_content into a Pandas data frame. Filters on AQSIDs.
            Uses the DBHandler to insertIntoAirNowTable if records remain to be loaded.
        """
        # Define column headers of the file that will be downloaded as it has no column headers in the file
        column_headers = ['Valid date', 'valid time', 'AQSID', 'sitename', 'GMT offset', 'parameter name', 'reporting units', 'value', 'data source']
        try:
            df = pd.read_csv( StringIO( file_content ), delimiter = '|', names = column_headers )
            filtered_df = df[df['AQSID'].isin( self.AQSIDs )] #only grab records with AQSIDs we're interested in
            if not filtered_df.empty:
                self.Log.info( f"Filtered data and sending {len( filtered_df )} records to SQL Server" )
                self.DBHandler.insertIntoStagingTable( filtered_df, file_url )
                self.Log.info( f"Successfully processed file: {file_url}" )
            else:
                self.Log.info( "No matching records found for AQSID list" )
        except Exception as e:
            self.Log.error( f"Error processing file content: {e}" )



    
