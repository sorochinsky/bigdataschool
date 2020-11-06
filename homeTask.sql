CREATE EXTERNAL FILE FORMAT [soroc_file] WITH (FORMAT_TYPE = DELIMITEDTEXT, FORMAT_OPTIONS (FIELD_TERMINATOR = N',', FIRST_ROW = 2, USE_TYPE_DEFAULT = False))

drop EXTERNAL table fact_tripdata_ex
drop  EXTERNAL DATA SOURCE soroc_blob
drop DATABASE SCOPED CREDENTIAL SorAzureBlobStorageCredential

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
CREATE TABLE sor.Vendor
(      [ID] INT
      ,[Mame] varchar(255)
)
WITH
(   CLUSTERED COLUMNSTORE INDEX
,   DISTRIBUTION = REPLICATE  
)
--****RateCode
CREATE TABLE sor.RateCode
(      [ID] INT
      ,[Mame] varchar(255)
)
WITH
(   CLUSTERED COLUMNSTORE INDEX
,   DISTRIBUTION = REPLICATE  
)
--****Payment_type
CREATE TABLE sor.Payment_type
(      [ID] INT
      ,[Mame] varchar(255)
)
WITH
(   CLUSTERED COLUMNSTORE INDEX
,   DISTRIBUTION = REPLICATE 
)

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



--*******Vendor
select distinct [VendorID]  as [ID]
into #t1 
from sor.fact_tripdata
where [VendorID] is not null

select [ID]
      ,case when [ID] = 1 then 'Creative Mobile Technologies, LLC'
		  when [ID] = 2 then 'VeriFone Inc.'
		  when [ID] is not null then 'Unknown_category' 
		  end as [Name]
into sor.Vendor2
from #t1
drop table #t1


--*******RateCode
select distinct [RateCodeID] as [ID]
into #t2 
from sor.fact_tripdata
where [RateCodeID] is not null

select [ID]
      ,case when [ID] = 1 then 'Standard rate'
		  when [ID] = 2 then 'JFK'
		  when [ID] = 3 then 'Newark'
		  when [ID] = 4 then 'Nassau or Westchester'
		  when [ID] = 5 then 'Negotiated fare'
		  when [ID] = 6 then 'Group ride'
		  when [ID] is not null then 'Unknown_category' 
		  end as [Name]
into sor.RateCode2
from #t2 
drop table #t2 

--*******Payment_type
select distinct [Payment_type] as [ID]
--into #t3 
from sor.fact_tripdata
where [Payment_type] is not null

select [ID]
      ,case when [ID] = 1 then 'Credit card'
		  when [ID] = 2 then 'Cash'
		  when [ID] = 3 then 'No charge'
		  when [ID] = 4 then 'Dispute'
		  when [ID] = 5 then 'Unknown'
		  when [ID] = 6 then 'Voided trip'
		  when [ID] is not null then 'Unknown_category' 
		  end as [Name]
into sor.Payment_type2
from #t3 
drop table #t3

select * from sor.RateCode2
