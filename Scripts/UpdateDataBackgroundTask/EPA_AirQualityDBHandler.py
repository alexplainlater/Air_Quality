import sqlalchemy as SA
import pandas as pd
import logging
from AirQualityDBHandler import AirQualityDBHandler

class EPA_AirQualityDBHandler(AirQualityDBHandler):
    """
        Child class to handle database transactions from the EPA API
    """
    def __init__( self, server: str, database: str, username: str, password: str, port: int = None, log: logging = None ):
        super().__init__( server, database, username, password, port, log )
        
    def createStagingTable( self, tableName: str ) -> bool:
        if self.checkIfTableExists( tableName ):
            log_message = f"Table: {self.Database}.dbo.{tableName} already exists."
            self.Log.info( log_message ) if self.Log else print( log_message )
            return False
        else:
            SQLCode = f"""
                CREATE TABLE {self.Database}.dbo.{tableName}
                (
                    recID INT IDENTITY(1, 1)
                    , state_code CHAR(2)
                    , county_code CHAR(3)
                    , site_number CHAR(4)
                    , parameter_code CHAR(5)
                    , poc TINYINT
                    , latitude DECIMAL(9, 6)
                    , longitude DECIMAL(9, 6)
                    , datum CHAR(5)
                    , parameter VARCHAR(50)
                    , date_local DATE
                    , time_local TIME
                    , date_gmt DATE
                    , time_gmt TIME
                    , sample_measurement DECIMAL(9, 5)
                    , units_of_measure VARCHAR(50)
                    , units_of_measure_code CHAR(3)
                    , sample_duration VARCHAR(25)
                    , sample_duration_code VARCHAR(25)
                    , sample_frequency VARCHAR(25)
                    , detection_limit DECIMAL(9, 5)
                    , uncertainty VARCHAR(25)
                    , qualifier VARCHAR(100)
                    , method_type VARCHAR(25)
                    , method VARCHAR(100)
                    , method_code CHAR(3)
                    , state VARCHAR(50)
                    , county VARCHAR(50)
                    , date_of_last_change DATE
                    , cbsa_code CHAR(5)
                    , URL_Source VARCHAR(1000)
                )
            """
            try:
                with self.Engine.connect() as conn:
                    conn.execute( SA.text( SQLCode ) )
                    conn.commit()
                    log_message = f"{self.Database}.dbo.{tableName} has been successfully created."
                    self.Log.info(log_message) if self.Log else print(log_message)
                return self.checkIfTableExists( tableName )
            except Exception as e:
                log_message = f"Error creating table: {tableName}.  {e}"
                self.Log.error( log_message ) if self.Log else print( log_message )
                return False
            
    def insertIntoStagingTable(self, df: pd.DataFrame, file_url: str, chunk_size: int = 50 ) -> None:
        try:           
            # Add the file_url to the DataFrame
            df['URL_Source'] = file_url

            # Insert data into the staging table
            total_inserted = df.to_sql( 
                name = self.StagingTable
                , con = self.Engine
                , schema = 'dbo'
                , if_exists = 'append'
                , index = False
                , chunksize = chunk_size
                , method = 'multi'
            )

            log_message = f"Data successfully inserted into SQL Server. Total records inserted: {total_inserted}"
            self.Log.info( log_message ) if self.Log else print( log_message )
        except Exception as e:
            log_message = f"Error inserting data into SQL Server: {e}"
            self.Log.error( log_message ) if self.Log else print( log_message )
      
    def updateDWFactTables( self ) -> None:
        print("update")