USE master

GO

--=============================================================================
-- Create a database to hold the warehoused data.
--=============================================================================
CREATE DATABASE AirQuality_DW

--=============================================================================
-- Create a database to hold the staging of new data to be inserted into the
-- warehouse.
--=============================================================================
CREATE DATABASE AirQuality_Staging

GO

USE AirQuality_DW

GO

--=============================================================================
--Create warehouse table shells
-- Considerations:
--		I could have created and used schemas to separate Fact and Dimension tables
--		but I'd prefer not to have to manage them.
--
--		I could have created a table that links Full_Site_Number, Date, and Time
--		but I'd prefer to have these fields present in each table instead of having
--		to look up what each key means.
--
--		I could have created a lookup table for Qualifiers, but I'd prefer
--		having the fields present in the table for ease of identifying.
--=============================================================================

--=============================================================================
-- Create a table to hold the metadata for each site.
--=============================================================================
IF OBJECT_ID( 'AirQuality_DW.dbo.Sites' ) IS NOT NULL
	DROP TABLE AirQuality_DW.dbo.Sites
CREATE TABLE AirQuality_DW.dbo.Sites
(
	Full_Site_Number CHAR(11) PRIMARY KEY
	, State_Code CHAR(2) NOT NULL
	, County_Code CHAR(3) NOT NULL
	, Site_Number CHAR(4) NOT NULL	
	, Latitude DECIMAL(9, 6) NULL
	, Longitude DECIMAL(9, 6) NULL
	, Geog GEOGRAPHY NULL
	, Datum CHAR(5) NULL
	, Elevation DECIMAL(12, 6) NULL
	, Land_Use VARCHAR(25) NULL
	, Location_Setting VARCHAR(25) NULL
	, Site_Established_Date DATE NULL
	, Site_Closed_Date DATE NULL
	, GMT_Offset SMALLINT NULL
	, Owning_Agency VARCHAR(100) NULL
	, Local_Site_Name VARCHAR(100) NULL
	, Address VARCHAR(50) NULL
	, ZIP_Code VARCHAR(5) NULL
	, State_Name VARCHAR(30) NULL
	, County_Name VARCHAR(50) NULL
	, City_Name VARCHAR(50) NULL
	, CBSA_Name VARCHAR(100) NULL
	, Tribe_Name VARCHAR(100) NULL
	, INSERT_DT SMALLDATETIME NOT NULL
)

--=============================================================================
-- Create a table to hold the Air Quality Index from all pollutants
--=============================================================================
IF OBJECT_ID( 'AirQuality_DW.dbo.Fact_CombinedAQI' ) IS NOT NULL
	DROP TABLE AirQuality_DW.dbo.Fact_CombinedAQI
CREATE TABLE AirQuality_DW.dbo.Fact_CombinedAQI
(
	Full_Site_Number CHAR(11)
	, Date_Local DATE
	, Time_Local TIME
	, Date_Time_Local DATETIME
	, Combined_AQI SMALLINT
	, Combined_AQI_Contributor VARCHAR(100)
	, src VARCHAR(25)
)

CREATE UNIQUE CLUSTERED INDEX UC_IDX_SiteDateTime ON AirQuality_DW.dbo.Fact_CombinedAQI( Full_Site_Number, Date_Time_Local )

--=============================================================================
-- Create a table to hold all the data for the Nitrogen Dioxide pollutant
--=============================================================================
IF OBJECT_ID( 'AirQuality_DW.dbo.Fact_NitrogenDioxide' ) IS NOT NULL
	DROP TABLE AirQuality_DW.dbo.Fact_NitrogenDioxide
CREATE TABLE AirQuality_DW.dbo.Fact_NitrogenDioxide
(
	Full_Site_Number CHAR(11)
	, Date_Local DATE
	, Time_Local TIME
	, Date_Time_Local DATETIME
	, NO2_Sample_Measurement DECIMAL(9, 5)
	, NO2_Units_of_Measure VARCHAR(50)
	, NO2_Sample_Duration VARCHAR(25)
	, NO2_QualifierID SMALLINT
	, NO2_AQI SMALLINT
	, src VARCHAR(25)
)

CREATE UNIQUE CLUSTERED INDEX UC_IDX_SiteDateTimeID ON AirQuality_DW.dbo.Fact_NitrogenDioxide ( Full_Site_Number, Date_Time_Local )

--=============================================================================
-- Create a table to hold all the data for the Ozone pollutant
--=============================================================================
IF OBJECT_ID( 'AirQuality_DW.dbo.Fact_Ozone' ) IS NOT NULL
	DROP TABLE AirQuality_DW.dbo.Fact_Ozone
CREATE TABLE AirQuality_DW.dbo.Fact_Ozone
(
	Full_Site_Number CHAR(11)
	, Date_Local DATE
	, Time_Local TIME
	, Date_Time_Local DATETIME
	, Ozone_Sample_Measurement DECIMAL(9,5)
	, Ozone_Units_of_Measure VARCHAR(50)
	, Ozone_Sample_Duration VARCHAR(25)
	, Ozone_QualifierID SMALLINT
	, Ozone_AQI SMALLINT
	, Ozone_8Hr_Rolling_Avg DECIMAL (9,5)
	, Ozone_8Hr_AQI SMALLINT
	, src VARCHAR(25)
)

CREATE UNIQUE CLUSTERED INDEX UC_IDX_SiteDateTimeID ON AirQuality_DW.dbo.Fact_Ozone ( Full_Site_Number, Date_Time_Local )

--=============================================================================
-- Create a table to hold all the data for the PM10 pollutant
--=============================================================================
IF OBJECT_ID( 'AirQuality_DW.dbo.Fact_ParticulateMatterCoarse_PM10' ) IS NOT NULL
	DROP TABLE AirQuality_DW.dbo.Fact_ParticulateMatterCoarse_PM10
CREATE TABLE AirQuality_DW.dbo.Fact_ParticulateMatterCoarse_PM10
(
	Full_Site_Number CHAR(11)
	, Date_Local DATE
	, Time_Local TIME
	, Date_Time_Local DATETIME
	, PM_10_Sample_Measurement DECIMAL(9,5)
	, PM_10_Units_of_Measure VARCHAR(50)
	, PM_10_Sample_Duration VARCHAR(25)
	, PM_10_QualifierID SMALLINT
	, PM_10_AQI SMALLINT
	, src VARCHAR(25)
)

CREATE UNIQUE CLUSTERED INDEX UC_IDX_SiteDateTimeID ON AirQuality_DW.dbo.Fact_ParticulateMatterCoarse_PM10 ( Full_Site_Number, Date_Time_Local )

--=============================================================================
-- Create a table to hold all the data for the PM 2.5 pollutant
--=============================================================================
IF OBJECT_ID( 'AirQuality_DW.dbo.Fact_ParticulateMatterFine_PM2_5' ) IS NOT NULL
	DROP TABLE AirQuality_DW.dbo.Fact_ParticulateMatterFine_PM2_5
CREATE TABLE AirQuality_DW.dbo.Fact_ParticulateMatterFine_PM2_5
(
	Full_Site_Number CHAR(11)
	, Date_Local DATE
	, Time_Local TIME
	, Date_Time_Local DATETIME
	, PM_2_5_Sample_Measurement DECIMAL(9,5)
	, PM_2_5_Units_of_Measure VARCHAR(50)
	, PM_2_5_Sample_Duration VARCHAR(25)
	, PM_2_5_QualifierID SMALLINT
	, PM_2_5_AQI SMALLINT
	, src VARCHAR(25)
)

CREATE UNIQUE CLUSTERED INDEX UC_IDX_SiteDateTimeID ON AirQuality_DW.dbo.Fact_ParticulateMatterFine_PM2_5 ( Full_Site_Number, Date_Time_Local )

--=============================================================================
-- Create a table to hold all the data for the Carbon Monoxide pollutant
--=============================================================================
IF OBJECT_ID( 'AirQuality_DW.dbo.Fact_CarbonMonoxide' ) IS NOT NULL
	DROP TABLE AirQuality_DW.dbo.Fact_CarbonMonoxide
CREATE TABLE AirQuality_DW.dbo.Fact_CarbonMonoxide
(
	Full_Site_Number CHAR(11)
	, Date_Local DATE
	, Time_Local TIME
	, Date_Time_Local DATETIME
	, CO_Sample_Measurement DECIMAL(9,5)
	, CO_Units_of_Measure VARCHAR(50)
	, CO_Sample_Duration VARCHAR(25)
	, CO_QualifierID SMALLINT
	, CO_AQI SMALLINT
	, src VARCHAR(25)
)

CREATE UNIQUE CLUSTERED INDEX UC_IDX_SiteDateTimeID ON AirQuality_DW.dbo.Fact_CarbonMonoxide ( Full_Site_Number, Date_Time_Local )

--=============================================================================
-- Create a table to hold all the data for the Sulfur Dioxide pollutant
--=============================================================================
IF OBJECT_ID( 'AirQuality_DW.dbo.Fact_SulfurDioxide' ) IS NOT NULL
	DROP TABLE AirQuality_DW.dbo.Fact_SulfurDioxide
CREATE TABLE AirQuality_DW.dbo.Fact_SulfurDioxide
(
	Full_Site_Number CHAR(11)
	, Date_Local DATE
	, Time_Local TIME
	, Date_Time_Local DATETIME
	, SO2_Sample_Measurement DECIMAL(9,5)
	, SO2_Units_of_Measure VARCHAR(50)
	, SO2_Sample_Duration VARCHAR(25)
	, SO2_QualifierID SMALLINT
	, SO2_AQI SMALLINT
	, src VARCHAR(25)
)

CREATE UNIQUE CLUSTERED INDEX UC_IDX_SiteDateTimeID ON AirQuality_DW.dbo.Fact_SulfurDioxide ( Full_Site_Number, Date_Time_Local )

--=============================================================================
-- Create a table to hold all the data for the outdoor temperature
--=============================================================================
IF OBJECT_ID( 'AirQuality_DW.dbo.Fact_OutdoorTemperature' ) IS NOT NULL
	DROP TABLE AirQuality_DW.dbo.Fact_OutdoorTemperature
CREATE TABLE AirQuality_DW.dbo.Fact_OutdoorTemperature
(
	Full_Site_Number CHAR(11)
	, Date_Local DATE
	, Time_Local TIME
	, Date_Time_Local DATETIME
	, Temperature_Sample_Measurement DECIMAL(9,5)
	, Temperature_Units_of_Measure VARCHAR(50)
	, Temperature_Sample_Duration VARCHAR(25)
	, Temperature_QualifierID SMALLINT
	, src VARCHAR(25)
)

CREATE UNIQUE CLUSTERED INDEX UC_IDX_SiteDateTimeID ON AirQuality_DW.dbo.Fact_OutdoorTemperature ( Full_Site_Number, Date_Time_Local )

--=============================================================================
-- Create a table to hold all the data for the wind direction
--=============================================================================
IF OBJECT_ID( 'AirQuality_DW.dbo.Fact_WindDirection' ) IS NOT NULL
	DROP TABLE AirQuality_DW.dbo.Fact_WindDirection
CREATE TABLE AirQuality_DW.dbo.Fact_WindDirection
(
	Full_Site_Number CHAR(11)
	, Date_Local DATE
	, Time_Local TIME
	, Date_Time_Local DATETIME
	, Wind_Direction_Sample_Measurement DECIMAL(9,5)
	, Wind_Direction_Units_of_Measure VARCHAR(50)
	, Wind_Direction_Grouped VARCHAR(2)
	, Wind_Direction_Sample_Duration VARCHAR(25)
	, Wind_Direction_QualifierID SMALLINT
	, src VARCHAR(25)
)

CREATE UNIQUE CLUSTERED INDEX UC_IDX_SiteDateTimeID ON AirQuality_DW.dbo.Fact_WindDirection ( Full_Site_Number, Date_Time_Local )

--=============================================================================
-- Create a table to hold all the data for the wind speed
--=============================================================================
IF OBJECT_ID( 'AirQuality_DW.dbo.Fact_WindSpeed' ) IS NOT NULL
	DROP TABLE AirQuality_DW.dbo.Fact_WindSpeed
CREATE TABLE AirQuality_DW.dbo.Fact_WindSpeed
(
	Full_Site_Number CHAR(11)
	, Date_Local DATE
	, Time_Local TIME
	, Date_Time_Local DATETIME
	, Wind_Speed_Sample_Measurement DECIMAL(9,5)
	, Wind_Speed_Units_of_Measure VARCHAR(50)
	, Wind_Speed_Sample_Duration VARCHAR(25)
	, Wind_Speed_MPH TINYINT
	, Wind_Speed_QualifierID SMALLINT
	, src VARCHAR(25)
)

CREATE UNIQUE CLUSTERED INDEX UC_IDX_SiteDateTimeID ON AirQuality_DW.dbo.Fact_WindSpeed ( Full_Site_Number, Date_Time_Local )

GO

USE AirQuality_Staging

GO

--=============================================================================
--Create data loading staging table shells
--=============================================================================

--=============================================================================
-- A staging table to hold the data from the AirNow data provider
--=============================================================================
IF OBJECT_ID( 'AirQuality_Staging.dbo.AirNowData' ) IS NOT NULL
	DROP TABLE AirQuality_Staging.dbo.AirNowData
CREATE TABLE AirQuality_Staging.dbo.AirNowData
(
	recID INT IDENTITY(1, 1)
	, Valid_Date DATE
	, Valid_Time TIME
	, AQSID CHAR(9)
	, SiteName VARCHAR(20)
	, GMT_Offset VARCHAR(3)
	, Parameter_Name VARCHAR(10)
	, Reporting_Units VARCHAR(8)
	, Reported_Value DECIMAL(9, 5)
	, Reported_Data_Source VARCHAR(100)	
	, URL_Source VARCHAR(1000)
)

--=============================================================================
-- A staging table to hold the data from the EPA API
--=============================================================================
IF OBJECT_ID( 'AirQuality_Staging.dbo.EPA_API_Raw' ) IS NOT NULL
	DROP TABLE AirQuality_Staging.dbo.EPA_API_Raw
CREATE TABLE AirQuality_Staging.dbo.EPA_API_Raw
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