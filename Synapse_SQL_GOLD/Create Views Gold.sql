------------------------
-- CREATE VIEW CALENDAR
-- This view fetches all data from the AdventureWorks_Calendar directory
------------------------
CREATE VIEW gold.calendar
AS
SELECT 
    * 
FROM 
    OPENROWSET
        (
            BULK 'https://awsttoragedl.blob.core.windows.net/silver/AdventureWorks_Calendar/',
            FORMAT = 'PARQUET'
        ) as QUER1


------------------------
-- CREATE VIEW CUSTOMERS
-- This view fetches all data from the AdventureWorks_Customers directory
------------------------
CREATE VIEW gold.customers
AS
SELECT 
    * 
FROM 
    OPENROWSET
        (
            BULK 'https://awsttoragedl.blob.core.windows.net/silver/AdventureWorks_Customers/',
            FORMAT = 'PARQUET'
        ) as QUER1



------------------------
-- CREATE VIEW PRODUCTS
-- This view fetches all data from the AdventureWorks_Products directory
------------------------
CREATE VIEW gold.products
AS
SELECT 
    * 
FROM 
    OPENROWSET
        (
            BULK 'https://awsttoragedl.blob.core.windows.net/silver/AdventureWorks_Products/',
            FORMAT = 'PARQUET'
        ) as QUER1


------------------------
-- CREATE VIEW RETURNS
-- This view fetches all data from the AdventureWorks_Returns directory
------------------------
CREATE VIEW gold.returns
AS
SELECT 
    * 
FROM 
    OPENROWSET
        (
            BULK 'https://awsttoragedl.blob.core.windows.net/silver/AdventureWorks_Returns/',
            FORMAT = 'PARQUET'
        ) as QUER1
        

------------------------
-- CREATE VIEW SALES
-- This view fetches all data from the AdventureWorks_Sales directory
------------------------
CREATE VIEW gold.sales
AS
SELECT 
    * 
FROM 
    OPENROWSET
        (
            BULK 'https://awsttoragedl.blob.core.windows.net/silver/AdventureWorks_Sales/',
            FORMAT = 'PARQUET'
        ) as QUER1


------------------------
-- CREATE VIEW SUBCAT
-- This view fetches all data from the AdventureWorks_Subcategories directory
------------------------
CREATE VIEW gold.subcat
AS
SELECT 
    * 
FROM 
    OPENROWSET
        (
            BULK 'https://awsttoragedl.blob.core.windows.net/silver/AdventureWorks_Subcategories/',
            FORMAT = 'PARQUET'
        ) as QUER1



------------------------
-- CREATE VIEW TERRITORIES
-- This view fetches all data from the AdventureWorks_Territories directory
------------------------
CREATE VIEW gold.territories
AS
SELECT 
    * 
FROM 
    OPENROWSET
        (
            BULK 'https://awsttoragedl.blob.core.windows.net/silver/AdventureWorks_Territories/',
            FORMAT = 'PARQUET'
        ) as QUER1

------------------------
-- CREATE VIEW PRODUCT CATEGORIES
-- This view fetches all data from the AdventureWorks_ProductCategories directory
------------------------
CREATE VIEW gold.productCategories
AS
SELECT 
    * 
FROM 
    OPENROWSET
        (
            BULK 'https://awsttoragedl.blob.core.windows.net/silver/AdventureWorks_ProductCategories',
            FORMAT = 'PARQUET'
        ) as QUER1
