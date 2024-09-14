import sqlalchemy as SA
import pandas as pd

"""
Will handle both EPA API operations and AirNow operations

Check if database exists
    if not ->
        Create Database
        Create warehouse tables
        Create AirNow table
        Create EPA table

Check if warehouse tables exist
    if not ->
        Create warehouse tables

If doing an AirNow operation, check if AirNow table exists
    if not ->
        Create AirNow table

If doing an EPA API operation, check if EPA table exists
    if not ->
        Create EPA API table


"""

class AirQualityDBHandler:
    """
        Database handler for AirQuality data

        TODO: Check if DW tables exist, create if not
        TODO: Add support for other backends
        TODO: Add support if database does not exist, create
        TODO: Add default start date option parameter for example a new table is created
    """
    def __init__( self, server, database, username, password, port = None, log=None ):
        self.serverName = server
        self.serverPort = port
        self.userName = username
        self.password = password
        self.database = database
        self.log = log
        dialect_string = "mssql+pyodbc"
        alchemy_url_object = SA.URL.create(
            dialect_string
            , username = self.userName
            , password = self.password
            , host = self.serverName
            , port = self.serverPort
            , database = self.database
            , query={"driver": "ODBC Driver 17 for SQL Server"}
        )
        try:
            self.engine = SA.create_engine(alchemy_url_object)
        except Exception as e:
            log_message = f"Error creating SQL engine with url: {alchemy_url_object}. Exception received: {e}"
            self.log.error(log_message) if self.log else print(log_message)            

    def setAirNowTable( self, airNowTable, createIfNotExists = True ):
        self.airNowTable = airNowTable
        if self.checkIfTableExists( self.airNowTable ) == False:
            if createIfNotExists == True:
                return self.createAirNowTable( self.airNowTable )
            else:
                return False
        else:
            return True
    
    def checkIfTableExists(self,tableName):
        SQLCode = SA.text("""
                    SELECT object_id
                    FROM AirQuality.sys.tables t
                    WHERE t.name = :tableName
                """)
        try:
            with self.engine.connect() as conn:
                result = conn.execute(SQLCode,{"tableName":tableName}).fetchone()
                if result is not None:
                    return True
                else:
                    return False
        except Exception as e:
            log_message = f"Error checking for table: {tableName}. {e}"
            self.log.error(log_message) if self.log else print(log_message)
            return False
        
    def createAirNowTable(self,tableName):
        if self.checkIfTableExists(tableName) == True:
            log_message = f"Table: {tableName} already exists."
            self.log.info(log_message) if self.log else print(log_message)
            return False
        else:
            SQLCode = f"""
                CREATE TABLE {self.database}.dbo.{tableName}
                (
                    Valid_Date DATE
                    , Valid_Time TIME
                    , AQSID CHAR(9)
                    , SiteName VARCHAR(20)
                    , GMT_Offset VARCHAR(3)
                    , Parameter_Name VARCHAR(10)
                    , Reporting_Units VARCHAR(8)
                    , Reported_Value DECIMAL(9,5)
                    , Reported_Data_Source VARCHAR(1000)
                    , recID INT IDENTITY(1, 1)
                    , URL_Source VARCHAR(1000)
                )
            """
            try:
                with self.engine.connect() as conn:
                    conn.execute(SA.text(SQLCode))
                    conn.commit()
                return self.checkIfTableExists(tableName)
            except Exception as e:
                log_message = f"Error creating table: {tableName}.  {e}"
                self.log.error(log_message) if self.log else print(log_message)
                return False
    
    def getLastInsertedDate(self, AQSIDs):
        SQLCode = f"""
                SELECT
                    CONVERT(DATE, MIN(a.MX_DateTime)) AS Last_Date,
                    CONVERT(TIME, MIN(a.MX_DateTime)) AS Last_Time
                FROM (
                    SELECT
                        AQSID,
                        MAX(CONVERT(DATETIME, Valid_Date) + CONVERT(DATETIME, Valid_Time)) AS MX_DateTime
                    FROM {self.database}.dbo.{self.airNowTable}
                    WHERE AQSID IN ({','.join([f"'{aqsid}'" for aqsid in AQSIDs])})
                    GROUP BY AQSID
                ) a
            """
        try:
            with self.engine.connect() as conn:
                date_found, hour_found = conn.execute(SA.text(SQLCode)).fetchone()
            log_message = f"The last inserted date is: {date_found.strftime('%m/%d/%Y')} with a time of {str(hour_found).zfill(2)}:00"
            self.log.info(log_message) if self.log else print(log_message)
        except Exception as e:
            log_message = f"Error retrieving latest insert date/time from SQL table: {self.airNowTable}.  {e}"
            self.log.error(log_message) if self.log else print(log_message)
            date_found = None
            hour_found = None            
        return date_found, hour_found.hour    

    def insertIntoAirNowTable(self, df, file_url):
        try:
            with self.engine.connect() as conn:
                total_inserted = 0
                for index, row in df.iterrows():
                    merge_stmt = SA.text(f"""
                        MERGE INTO {self.database}.dbo.{self.airNowTable} AS target
                        USING (VALUES (:Valid_date, :Valid_time, :AQSID, :sitename, :GMT_offset, :parameter_name, :reporting_units, :Reported_Value, :Reported_Data_Source, :URL_Source)) AS source
                        (Valid_date, Valid_time, AQSID, sitename, GMT_offset, parameter_name, reporting_units, Reported_Value, Reported_Data_Source, URL_Source)
                        ON target.AQSID = source.AQSID AND target.Valid_date = source.Valid_date AND target.Valid_time = source.Valid_time AND target.parameter_name = source.parameter_name
                        WHEN NOT MATCHED THEN
                            INSERT (Valid_date, Valid_time, AQSID, sitename, GMT_offset, parameter_name, reporting_units, Reported_Value, Reported_Data_Source, URL_Source)
                            VALUES (source.Valid_date, source.Valid_time, source.AQSID, source.sitename, source.GMT_offset, source.parameter_name, source.reporting_units, source.Reported_Value, source.Reported_Data_Source, source.URL_Source)
                        OUTPUT $action;
                    """)
                    result = conn.execute(merge_stmt, {
                        'Valid_date': row['Valid date'],
                        'Valid_time': row['valid time'],
                        'AQSID': row['AQSID'],
                        'sitename': row['sitename'],
                        'GMT_offset': row['GMT offset'],
                        'parameter_name': row['parameter name'],
                        'reporting_units': row['reporting units'],
                        'Reported_Value': row['value'],
                        'Reported_Data_Source': row['data source'],
                        'URL_Source': file_url
                    })                    
                    total_inserted += sum(1 for row in result if row[0] == 'INSERT')
                    conn.commit()

            log_message = f"Data successfully inserted into SQL Server. Total records inserted: {total_inserted}"
            self.log.info(log_message) if self.log else print(log_message)
        except Exception as e:
            log_message = f"Error inserting data into SQL Server: {e}"
            self.log.error(log_message) if self.log else print(log_message)

    def updateDWFactTables(self):
        log_message = "Updating data warehouse fact tables."
        self.log.info(log_message) if self.log else print(log_message)
        try:
            with self.engine.connect() as conn:
                SQL_Code = f"EXEC {self.database}.dbo.spUpdateFactTables_w_AirNow"
                conn.execute(SA.text(SQL_Code))
                conn.commit()
            log_message = "Fact tables updated."
            self.log.info(log_message) if self.log else print(log_message)
        except Exception as e:
            log_message = f"Error updating DW fact tables on SQL server. {e}"
            self.log.error(log_message) if self.log else print(log_message)