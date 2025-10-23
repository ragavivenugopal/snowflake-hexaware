
--  CREATE DATABASE AND SCHEMA

CREATE DATABASE IF NOT EXISTS ITTG_SALES_DB;
USE DATABASE ITTG_SALES_DB;

CREATE SCHEMA IF NOT EXISTS RAW_DATA;
USE SCHEMA RAW_DATA;


--  CREATE STAGE WITH YOUR SAS TOKEN

CREATE OR REPLACE STAGE azure_sales_stage
  URL = 'azure://ittechgeniestorage.blob.core.windows.net/sales-data/'
  CREDENTIALS = (
    AZURE_SAS_TOKEN = '?sp=racwdl&st=2025-10-22T10:47:09Z&se=2025-10-23T19:02:09Z&spr=https&sv=2024-11-04&sr=c&sig=hEF7601nEZP%2Byvbuk9F2FVtAou%2F3%2BoDvfC3fNQ5fLbs%3D'
  )
  FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = ',' SKIP_HEADER = 1);


--  TEST CONNECTION - LIST FILES

LIST @azure_sales_stage;


--  CREATE TARGET TABLE MATCHING YOUR CSV STRUCTURE

CREATE OR REPLACE TABLE raw_sales_data (
    OrderID STRING,
    OrderDate DATE,
    MonthOfSale STRING,
    CustomerID STRING,
    CustomerName STRING,
    Country STRING,
    Region STRING,
    City STRING,
    Category STRING,
    Subcategory STRING,
    Quantity INTEGER,
    Discount NUMBER(10,2),
    Sales NUMBER(10,2),
    Profit NUMBER(10,2),
    load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);


--  COPY DATA FROM AZURE TO SNOWFLAKE

COPY INTO raw_sales_data (
    OrderID, OrderDate, MonthOfSale, CustomerID, CustomerName,
    Country, Region, City, Category, Subcategory,
    Quantity, Discount, Sales, Profit
)
FROM @azure_sales_stage/Retail_Sales__500_rows__Preview.csv
FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = ',' SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE';


--  VERIFY DATA LOADED SUCCESSFULLY

-- Check how many rows loaded
SELECT COUNT(*) AS total_loaded_rows FROM raw_sales_data;

-- Preview the data
SELECT * FROM raw_sales_data LIMIT 10;

-- Check for any errors
SELECT * FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
  TABLE_NAME => 'RAW_SALES_DATA',
  START_TIME => DATEADD(HOUR, -1, CURRENT_TIMESTAMP())
));


--  CREATE CLEAN DATA VIEW FOR POWER BI

CREATE OR REPLACE VIEW clean_sales_data AS
SELECT 
    OrderID,
    OrderDate,
    MonthOfSale,
    CustomerID,
    CustomerName,
    Country,
    Region,
    City,
    Category,
    Subcategory,
    Quantity,
    Discount,
    Sales,
    Profit,
    -- Calculate additional metrics
    Sales * Discount AS DiscountAmount,
    Profit / NULLIF(Sales, 0) AS ProfitMargin,
    -- Date parts for analysis
    YEAR(OrderDate) AS OrderYear,
    MONTH(OrderDate) AS OrderMonth,
    QUARTER(OrderDate) AS OrderQuarter,
    load_timestamp
FROM raw_sales_data
WHERE OrderDate IS NOT NULL AND Sales > 0;


--  CREATE POWER BI DASHBOARD VIEW

CREATE OR REPLACE VIEW vw_powerbi_dashboard AS
SELECT 
    OrderID,
    OrderDate,
    CustomerName,
    Region,
    City,
    Category,
    Subcategory,
    Quantity,
    Discount,
    Sales,
    Profit,
    ProfitMargin,
    OrderYear,
    OrderMonth,
    OrderQuarter
FROM clean_sales_data;


--  FINAL VERIFICATION

-- Test Power BI view
SELECT * FROM vw_powerbi_dashboard LIMIT 5;

-- Summary statistics
SELECT 
    COUNT(*) AS total_records,
    COUNT(DISTINCT CustomerID) AS unique_customers,
    COUNT(DISTINCT Region) AS regions_covered,
    SUM(Sales) AS total_sales,
    SUM(Profit) AS total_profit
FROM clean_sales_data;
