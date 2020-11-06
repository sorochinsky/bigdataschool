

drop  EXTERNAL DATA SOURCE soroc_blob
drop DATABASE SCOPED CREDENTIAL SorAzureBlobStorageCredential

go
-- Create Storage Credential
--print 'Creating credential'
CREATE DATABASE SCOPED CREDENTIAL SorAzureBlobStorageCredential
WITH IDENTITY = 'SHARED ACCESS SIGNATURE',
SECRET = 'sp=r&st=2020-11-05T07:40:04Z&se=2020-11-05T15:40:04Z&spr=https&sv=2019-12-12&sr=b&sig=2vsQXMJJl9ECWw3Dd2qI%2BaiWazhlnfrqMROaMHzCy1c%3D';

-- Create External Data Source
--full url https://myaccount.blob.core.windows.net/files/tran.csv
   --N'https://osstorege.blob.core.windows.net/sorochynskyi/yellow_tripdata_2020-01.csv', 
--print 'creating external data source'
CREATE EXTERNAL DATA SOURCE soroc_blob
WITH ( TYPE = HADOOP ,
    LOCATION = 'https://osstorege.blob.core.windows.net/sorochynskyi',
    CREDENTIAL= SorAzureBlobStorageCredential);
 
--test
SELECT top 100 * FROM OPENROWSET(
   BULK  'https://osstorege.blob.core.windows.net/sorochynskyi',
   DATA_SOURCE = 'soroc_blob',
   SINGLE_CLOB
   )

/****** Object:  ExternalFileFormat [soroc_file]    Script Date: 05.11.2020 9:18:28 ******/
CREATE EXTERNAL FILE FORMAT [soroc_file] WITH (FORMAT_TYPE = DELIMITEDTEXT, FORMAT_OPTIONS (FIELD_TERMINATOR = N',', FIRST_ROW = 2, USE_TYPE_DEFAULT = False))
GO

	-- Create a new external table
CREATE EXTERNAL TABLE sqldwschool.sor.fact_tripdata_ex
    ( [VendorID] INT
      ,[tpep_pickup_datetime] INT
      ,[tpep_dropoff_datetime] INT
      ,[passenger_count] INT
      ,[Trip_distance] real
      ,[RatecodeID] INT
      ,[store_and_fwd_flag] char(1)
      ,[PULocationID] INT
      ,[DOLocationID] INT
      ,[payment_type] INT
      ,[fare_amount] real
      ,[extra] real
      ,[mta_tax] real
      ,[tip_amount] real
      ,[tolls_amount] real
      ,[improvement_surcharge] real
      ,[total_amount] real
      ,[congestion_surcharge] real)
    WITH (
        LOCATION = 'https://osstorege.blob.core.windows.net/sorochynskyi',
        DATA_SOURCE = soroc_blob,
        FILE_FORMAT = soroc_file
      
    )