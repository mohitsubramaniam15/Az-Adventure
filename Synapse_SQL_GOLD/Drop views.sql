-- Drop views from the gold schema
-- The following statements remove the specified views
DROP VIEW gold.calendar;
DROP VIEW gold.customers;
DROP VIEW gold.products;
DROP VIEW gold.returns;
DROP VIEW gold.sales;
DROP VIEW gold.subcat;
DROP VIEW gold.territories;
DROP VIEW gold.productCategories;

-- Query to list remaining views in the database
-- This retrieves the schema and name of all views
SELECT TABLE_SCHEMA, TABLE_NAME 
FROM information_schema.tables 
WHERE TABLE_TYPE LIKE 'VIEW';