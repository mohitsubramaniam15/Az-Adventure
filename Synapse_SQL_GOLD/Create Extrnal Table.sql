------------------------
-- CREATE DATABASE SCOPED CREDENTIAL
-- This credential uses Managed Identity for accessing Azure Blob Storage
------------------------
CREATE DATABASE SCOPED CREDENTIAL cred_mo
WITH 
IDENTITY = 'Managed Identity';

------------------------
-- CREATE EXTERNAL DATA SOURCE FOR SILVER LAYER
-- This data source points to the silver layer in Azure Blob Storage
------------------------
CREATE EXTERNAL DATA SOURCE source_silver
WITH
(
    LOCATION = 'https://awsttoragedl.blob.core.windows.net/silver',
    CREDENTIAL = cred_mo
);

------------------------
-- CREATE EXTERNAL DATA SOURCE FOR GOLD LAYER
-- This data source points to the gold layer in Azure Blob Storage
------------------------
CREATE EXTERNAL DATA SOURCE source_gold
WITH
(
    LOCATION = 'https://awsttoragedl.blob.core.windows.net/gold',
    CREDENTIAL = cred_mo
);

------------------------
-- CREATE EXTERNAL FILE FORMAT
-- This defines the Parquet file format with Snappy compression
------------------------
CREATE EXTERNAL FILE FORMAT format_parquet
WITH
(
    FORMAT_TYPE = PARQUET,
    DATA_COMPRESSION ='org.apache.hadoop.io.compress.SnappyCodec'
);

------------------------
-- CREATE EXTERNAL TABLE: SALES
-- External table for sales data in the gold layer
------------------------
CREATE EXTERNAL TABLE gold.extsales
WITH
(
    LOCATION = 'extsales',
    DATA_SOURCE = source_gold,
    FILE_FORMAT = format_parquet
)
AS SELECT * FROM gold.sales;

-- Query the external sales table
SELECT * FROM gold.extsales;

------------------------
-- CREATE EXTERNAL TABLE: CALENDAR
-- External table for calendar data in the gold layer
------------------------
CREATE EXTERNAL TABLE gold.extcalender
WITH
(
    LOCATION = 'extcal',
    DATA_SOURCE = source_gold,
    FILE_FORMAT = format_parquet
)
AS SELECT * FROM gold.calendar;

-- Query the external calendar table
SELECT * FROM gold.extcalender;

------------------------
-- CREATE EXTERNAL TABLE: CUSTOMERS
-- External table for customer data in the gold layer
------------------------
CREATE EXTERNAL TABLE gold.extcustomer
WITH
(
    LOCATION = 'extcustomer',
    DATA_SOURCE = source_gold,
    FILE_FORMAT = format_parquet
)
AS SELECT * FROM gold.customers;

------------------------
-- CREATE EXTERNAL TABLE: PRODUCTS
-- External table for product data in the gold layer
------------------------
CREATE EXTERNAL TABLE gold.extproducts
WITH
(
    LOCATION = 'extProducts',
    DATA_SOURCE = source_gold,
    FILE_FORMAT = format_parquet
)
AS SELECT * FROM gold.products;

------------------------
-- CREATE EXTERNAL TABLE: RETURNS
-- External table for return data in the gold layer
------------------------
CREATE EXTERNAL TABLE gold.extreturns
WITH
(
    LOCATION = 'extReturns',
    DATA_SOURCE = source_gold,
    FILE_FORMAT = format_parquet
)
AS SELECT * FROM gold.returns;

------------------------
-- CREATE EXTERNAL TABLE: SUBCATEGORIES
-- External table for subcategory data in the gold layer
------------------------
CREATE EXTERNAL TABLE gold.extsubcat
WITH
(
    LOCATION = 'extSubCat',
    DATA_SOURCE = source_gold,
    FILE_FORMAT = format_parquet
)
AS SELECT * FROM gold.subcat;

------------------------
-- CREATE EXTERNAL TABLE: TERRITORIES
-- External table for territory data in the gold layer
------------------------
CREATE EXTERNAL TABLE gold.extterritories
WITH
(
    LOCATION = 'extTerriotories',
    DATA_SOURCE = source_gold,
    FILE_FORMAT = format_parquet
)
AS SELECT * FROM gold.territories;

------------------------
-- CREATE EXTERNAL TABLE: PRODUCT CATEGORIES
-- External table for product category data in the gold layer
------------------------
CREATE EXTERNAL TABLE gold.extproductCategories
WITH
(
    LOCATION = 'extProductCategories',
    DATA_SOURCE = source_gold,
    FILE_FORMAT = format_parquet
)
AS SELECT * FROM gold.productCategories;
