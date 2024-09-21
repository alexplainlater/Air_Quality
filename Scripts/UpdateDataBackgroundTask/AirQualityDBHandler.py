import sqlalchemy as SA
import pandas as pd
import logging

class AirQualityDBHandler:
    """
        Base class Database handler for AirQuality data

        TODO: Add default start date option parameter for example a new table is created

        Attributes:
            self.Database
            self.Log
            self.Engine
    """
    def __init__( self, server: str, database: str, username: str, password: str, port:int = None, log: logging = None ):
        self.Database = database
        self.Log = log

        dialect_string = "mssql+pyodbc"
        alchemy_url_object = SA.URL.create(
            dialect_string
            , username = username
            , password = password
            , host = server
            , port = port
            , database = self.Database
            , query={"driver": "ODBC Driver 17 for SQL Server"}
        )
        try:
            self.Engine = SA.create_engine( alchemy_url_object, fast_executemany = True )
        except Exception as e:
            log_message = f"Error creating SQL engine with url: {alchemy_url_object}. Exception received: {e}"
            self.Log.error( log_message ) if self.Log else print( log_message )            

    def setStagingTable( self, stagingTable: str, createIfNotExists: bool = True ) -> bool:
        self.StagingTable = stagingTable
        if not self.checkIfTableExists( self.StagingTable ):
            if createIfNotExists:
                return self.createStagingTable( self.StagingTable )
            else:
                return False
        else:
            return True
    
    def checkIfTableExists( self, tableName: str ) -> bool:
        SQLCode = SA.text( """
                    SELECT object_id
                    FROM sys.tables t
                    WHERE t.name = :tableName
                """ )
        try:
            with self.Engine.connect() as conn:
                result = conn.execute( SQLCode, {"tableName":tableName} ).fetchone()
                if result is not None:
                    return True
                else:
                    return False
        except Exception as e:
            log_message = f"Error checking for table: {tableName}. {e}"
            self.Log.error( log_message ) if self.Log else print( log_message )
            return False
        
    def createStagingTable( self, tableName: str ) -> None:
        raise NotImplementedError("Subclasses must implement this method")
    
    def insertIntoStagingTable( self, df: pd.DataFrame, file_url: str = None ) -> None:
        raise NotImplementedError("Subclasses must implement this method")
      
    def updateDWFactTables( self ) -> None:
        raise NotImplementedError("Subclasses must implement this method")