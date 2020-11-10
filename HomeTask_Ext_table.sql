CREATE EXTERNAL FILE FORMAT [soroc_file] WITH (FORMAT_TYPE = DELIMITEDTEXT, FORMAT_OPTIONS (FIELD_TERMINATOR = N',', FIRST_ROW = 2, USE_TYPE_DEFAULT = False))

IF EXISTS (SELECT * FROM sys.external_tables where name = 'fact_tripdata_ex')
  DROP EXTERNAL TABLE fact_tripdata_ex;
IF EXISTS (SELECT * FROM sys.external_data_sources where name = 'soroc_blob')
  DROP EXTERNAL DATA SOURCE soroc_blob;
IF EXISTS (SELECT * from sys.database_scoped_credentials where name ='SorAzureBlobStorageCredential')
  DROP DATABASE SCOPED CREDENTIAL SorAzureBlobStorageCredential;

-- Create Storage Credential
CREATE DATABASE SCOPED CREDENTIAL SorAzureBlobStorageCredential
WITH IDENTITY = 'SHARED ACCESS SIGNATURE',
SECRET = 'CKYfdRVUiWoQeOXYYtjZBrfjx5wdGNEUGm8fJbV1tPwyphkf3ADBKfcxnjKs1Jk90jTy2a7gRJ9QavSLLyS+vA=='

-- Create External Data Source
CREATE EXTERNAL DATA SOURCE soroc_blob
WITH ( TYPE = HADOOP ,
    LOCATION = 'wasbs://sorochynskyi@osstorege.blob.core.windows.net',
    CREDENTIAL= SorAzureBlobStorageCredential)

-- Create External table
CREATE EXTERNAL TABLE fact_tripdata_ex 
    (  [VendorID] INT
      ,[tpep_pickup_datetime] datetime
      ,[tpep_dropoff_datetime] datetime
      ,[passenger_count] INT
      ,[Trip_distance] float
      ,[RatecodeID] varchar(255)
      ,[store_and_fwd_flag] char(1)
      ,[PULocationID] INT
      ,[DOLocationID] INT
      ,[payment_type] INT
      ,[fare_amount] float
      ,[extra] float
      ,[mta_tax] float
      ,[tip_amount] float
      ,[tolls_amount] float
      ,[improvement_surcharge] float
      ,[total_amount] float
      ,[congestion_surcharge] float
	 )
WITH (
        LOCATION = '/yellow_tripdata_2020-01.csv',
        DATA_SOURCE = soroc_blob,
        FILE_FORMAT = soroc_file
    )

-- Create HASHED table
CREATE TABLE sor.fact_tripdata
(      [VendorID] INT
      ,[tpep_pickup_datetime] datetime
      ,[tpep_dropoff_datetime] datetime
      ,[passenger_count] INT
      ,[Trip_distance] float
      ,[RatecodeID] varchar(255)
      ,[store_and_fwd_flag] char(1)
      ,[PULocationID] INT
      ,[DOLocationID] INT
      ,[payment_type] INT
      ,[fare_amount] float
      ,[extra] float
      ,[mta_tax] float
      ,[tip_amount] float
      ,[tolls_amount] float
      ,[improvement_surcharge] float
      ,[total_amount] float
      ,[congestion_surcharge] float
)
WITH
(   CLUSTERED COLUMNSTORE INDEX
,   DISTRIBUTION = HASH([tpep_pickup_datetime])  
)

-- copy data
INSERT INTO sor.fact_tripdata
    SELECT *   
    FROM fact_tripdata_ex




--create dictionary
--****Vendor
CREATE TABLE sor.Vendor ([ID] INT, [Name] varchar(255))
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = REPLICATE)
--****RateCode
CREATE TABLE sor.RateCode ([ID] INT, [Name] varchar(255))
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = REPLICATE)
--****Payment_type
CREATE TABLE sor.Payment_type ([ID] INT, [Name] varchar(255))
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = REPLICATE)

--insert values
INSERT INTO sor.Vendor VALUES (1, 'Creative Mobile Technologies, LLC');
INSERT INTO sor.Vendor VALUES (2, 'VeriFone Inc.');

INSERT INTO sor.RateCode VALUES (1, 'Standard rate');
INSERT INTO sor.RateCode VALUES (2, 'JFK');
INSERT INTO sor.RateCode VALUES (3, 'Newark');
INSERT INTO sor.RateCode VALUES (4, 'Nassau or Westchester');
INSERT INTO sor.RateCode VALUES (5, 'Negotiated fare');
INSERT INTO sor.RateCode VALUES (6, 'Group ride');

INSERT INTO sor.Payment_type VALUES (1, 'Credit card');
INSERT INTO sor.Payment_type VALUES (2, 'Cash');
INSERT INTO sor.Payment_type VALUES (3, 'No charge');
INSERT INTO sor.Payment_type VALUES (4, 'Dispute');
INSERT INTO sor.Payment_type VALUES (5, 'Unknown');
INSERT INTO sor.Payment_type VALUES (6, 'Voided trip');
