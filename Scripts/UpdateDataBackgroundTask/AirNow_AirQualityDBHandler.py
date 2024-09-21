import sqlalchemy as SA
import pandas as pd
import logging
from AirQualityDBHandler import AirQualityDBHandler

class AirNow_AirQualityDBHandler(AirQualityDBHandler):
    """
        Child class to handle database transactions from the AirNow API

        Attributes:
            self.Database (inherited)
            self.Log (inherited)
            self.Engine (inherited)
            self.StagingTable
    """
    def __init__( self, server: str, database: str, username: str, password: str, port: int = None, log: logging = None ):
        super().__init__( server, database, username, password, port, log )
        
    def createStagingTable( self, tableName: str ) -> bool:
        if self.checkIfTableExists( tableName ):
            log_message = f"Table: {tableName} already exists."
            self.Log.info( log_message ) if self.Log else print( log_message )
            return False
        else:
            SQLCode = f"""
                CREATE TABLE {self.Database}.dbo.{tableName}
                (
                    recID INT IDENTITY(1, 1)
                    , Valid_Date DATE
                    , Valid_Time TIME
                    , AQSID CHAR(9)
                    , SiteName VARCHAR(20)
                    , GMT_Offset VARCHAR(3)
                    , Parameter_Name VARCHAR(10)
                    , Reporting_Units VARCHAR(8)
                    , Reported_Value DECIMAL(9,5)
                    , Reported_Data_Source VARCHAR(1000)
                    , URL_Source VARCHAR(1000)
                )
            """
            try:
                with self.Engine.connect() as conn:
                    conn.execute( SA.text( SQLCode ) )
                    conn.commit()
                return self.checkIfTableExists( tableName )
            except Exception as e:
                log_message = f"Error creating table: {tableName}.  {e}"
                self.Log.error( log_message ) if self.Log else print( log_message )
                return False
            
    def getLastInsertedDate( self, AQSIDs: list[str] ) -> None:
        SQLCode = f"""
                SELECT
                    CONVERT( DATE, MIN( a.MX_DateTime ) ) AS Last_Date,
                    CONVERT( TIME, MIN( a.MX_DateTime ) ) AS Last_Time
                FROM (
                    SELECT
                        AQSID,
                        MAX( CONVERT( DATETIME, Valid_Date ) + CONVERT( DATETIME, Valid_Time ) ) AS MX_DateTime
                    FROM {self.Database}.dbo.{self.StagingTable}
                    WHERE AQSID IN ( {','.join( [f"'{aqsid}'" for aqsid in AQSIDs] )} )
                    GROUP BY AQSID
                ) a
            """
        try:
            with self.Engine.connect() as conn:
                date_found, hour_found = conn.execute( SA.text( SQLCode ) ).fetchone()
            log_message = f"The last inserted date is: {date_found.strftime( '%m/%d/%Y' )} with a time of {str( hour_found ).zfill(2)}:00"
            self.Log.info( log_message ) if self.Log else print( log_message )
        except Exception as e:
            log_message = f"Error retrieving latest insert date/time from SQL table: {self.StagingTable}.  {e}"
            self.Log.error( log_message ) if self.Log else print( log_message )
            date_found = None
            hour_found = None            
        return date_found, hour_found.hour
    
    def insertIntoStagingTable( self, df: pd.DataFrame, file_url: str ) -> None:
        try:
            with self.Engine.connect() as conn:
                total_inserted = 0
                for index, row in df.iterrows():
                    merge_stmt = SA.text( f"""
                        MERGE 
                        INTO {self.Database}.dbo.{self.StagingTable} AS target
                        USING 
                        ( 
                            VALUES 
                            ( 
                                :Valid_date, :Valid_time, :AQSID, :sitename, :GMT_offset, :parameter_name
                                , :reporting_units, :Reported_Value, :Reported_Data_Source, :URL_Source
                            )
                        ) AS source
                        (
                            Valid_date, Valid_time, AQSID, sitename, GMT_offset, parameter_name
                            , reporting_units, Reported_Value, Reported_Data_Source, URL_Source 
                        )
                        ON 
                            target.AQSID = source.AQSID 
                            AND target.Valid_date = source.Valid_date 
                            AND target.Valid_time = source.Valid_time 
                            AND target.parameter_name = source.parameter_name
                        WHEN NOT MATCHED THEN
                            INSERT 
                            (
                                Valid_date, Valid_time, AQSID, sitename, GMT_offset, parameter_name
                                , reporting_units, Reported_Value, Reported_Data_Source, URL_Source
                            )
                            VALUES 
                            (
                                source.Valid_date, source.Valid_time, source.AQSID, source.sitename, source.GMT_offset, source.parameter_name
                                , source.reporting_units, source.Reported_Value, source.Reported_Data_Source, source.URL_Source
                            )
                        OUTPUT $action;
                    """ )
                    result = conn.execute( merge_stmt, {
                        'Valid_date': row['Valid date']
                        , 'Valid_time': row['valid time']
                        , 'AQSID': row['AQSID']
                        , 'sitename': row['sitename']
                        , 'GMT_offset': row['GMT offset']
                        , 'parameter_name': row['parameter name']
                        , 'reporting_units': row['reporting units']
                        , 'Reported_Value': row['value']
                        , 'Reported_Data_Source': row['data source']
                        , 'URL_Source': file_url
                    } )                    
                    total_inserted += sum( 1 for row in result if row[0] == 'INSERT' )
                    conn.commit()

            log_message = f"Data successfully inserted into SQL Server. Total records inserted: {total_inserted}"
            self.Log.info( log_message ) if self.Log else print( log_message )
        except Exception as e:
            log_message = f"Error inserting data into SQL Server: {e}"
            self.Log.error( log_message ) if self.Log else print( log_message )
    
    def updateDWFactTables( self ) -> None:
        log_message = "Updating data warehouse fact tables."
        self.Log.info( log_message ) if self.Log else print( log_message )
        try:
            with self.Engine.connect() as conn:
                SQL_Code = f"EXEC {self.Database}.dbo.spUpdateFactTables_w_AirNow"
                conn.execute( SA.text( SQL_Code ) )
                conn.commit()
            log_message = "Fact tables updated."
            self.Log.info( log_message ) if self.Log else print( log_message )
        except Exception as e:
            log_message = f"Error updating DW fact tables on SQL server. {e}"
            self.Log.error( log_message ) if self.Log else print( log_message )