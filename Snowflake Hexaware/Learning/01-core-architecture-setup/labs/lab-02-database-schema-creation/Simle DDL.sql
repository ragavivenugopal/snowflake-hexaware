
-- Basic Database Creation

-- Create a basic database
CREATE DATABASE sales_db;

CREATE TAG IF NOT EXISTS department;
CREATE TAG IF NOT EXISTS environment;
CREATE TAG IF NOT EXISTS business_unit;
CREATE TAG IF NOT EXISTS data_sensitivity;
CREATE TAG IF NOT EXISTS owner;

-- Create database with comment and tags
CREATE DATABASE analytics_db
COMMENT = 'Database for analytical processing'
TAG (department = 'analytics', environment = 'production');


-- Advanced Database Options

-- Create a clone of existing database
CREATE DATABASE sales_db_dev CLONE sales_db;

-- Create database with data retention period


CREATE DATABASE transient_db
TRANSIENT
DATA_RETENTION_TIME_IN_DAYS = 7;

-- Basic Schema Creation

-- Use the database first
USE DATABASE sales_db;

-- Create basic schema
CREATE SCHEMA raw_data;

-- Create schema with specific properties
CREATE SCHEMA processed_data
COMMENT = 'Schema for processed and cleaned data'
DATA_RETENTION_TIME_IN_DAYS = 30
TAG (data_sensitivity = 'low', owner = 'data_team');

-- Multiple Schema Types

-- Create transient schema (doesn't have Fail-safe)
CREATE TRANSIENT SCHEMA staging_data;

-- Create managed schema with specific privileges
CREATE SCHEMA secure_data
WITH MANAGED ACCESS;

-- Create schema in a specific database
CREATE SCHEMA sales_db.reporting;

-- Basic Table Structure

-- Use the schema
USE SCHEMA sales_db.raw_data;

-- Create a simple customer table
CREATE TABLE customers (
    customer_id NUMBER(38,0) AUTOINCREMENT START 1 INCREMENT 1,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(100),
    created_date TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),
    is_active BOOLEAN DEFAULT TRUE
);

-- Advanced Table with Constraints

CREATE TABLE orders (
    order_id NUMBER AUTOINCREMENT PRIMARY KEY,
    customer_id NUMBER NOT NULL,
    order_date DATE NOT NULL,
    total_amount NUMBER(10,2) CHECK (total_amount >= 0),
    status VARCHAR(20) DEFAULT 'PENDING',
    -- Foreign key constraint (enforced in Snowflake)
    CONSTRAINT fk_customer 
        FOREIGN KEY (customer_id) 
        REFERENCES customers(customer_id),
    -- Unique constraint
    CONSTRAINT uk_order_customer_date 
        UNIQUE (customer_id, order_date)
)
COMMENT = 'Main orders table for sales transactions'
TAG (business_unit = 'sales', pii_level = 'medium');

-- Table with Clustering and Partitioning

-- Table with clustering keys for performance
CREATE TABLE sales_transactions (
    transaction_id NUMBER AUTOINCREMENT,
    product_id NUMBER,
    sale_date DATE,
    quantity NUMBER,
    unit_price NUMBER(10,2),
    total_price NUMBER(10,2),
    region VARCHAR(50),
    -- Clustering key for better query performance
    CLUSTER BY (sale_date, region)
)
COMMENT = 'Sales transactions with date-based clustering';

-- Table with Data Retention Settings

-- Transient table (shorter retention)
CREATE TRANSIENT TABLE temp_sessions (
    session_id VARCHAR(100),
    user_id NUMBER,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    session_data VARIANT
)
DATA_RETENTION_TIME_IN_DAYS = 1;

-- Permanent table with custom retention
CREATE TABLE user_logs (
    log_id NUMBER AUTOINCREMENT,
    user_id NUMBER,
    action VARCHAR(100),
    log_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    details VARIANT
)
DATA_RETENTION_TIME_IN_DAYS = 90;

-- ---------------------- --
-- Complete Example Workflow

-- Step 1: Create Database
CREATE DATABASE ecommerce
COMMENT = 'E-commerce platform database'
TAG (business_domain = 'retail', criticality = 'high');

-- Step 2: Create Schemas
USE DATABASE ecommerce;

-- Raw data schema
CREATE SCHEMA raw
COMMENT = 'Raw incoming data from sources';

-- Staging schema
CREATE SCHEMA staging
COMMENT = 'Cleaned and validated data';

-- Analytics schema
CREATE SCHEMA analytics
COMMENT = 'Processed data for reporting and analysis';

-- Step 3: Create Tables in Raw Schema
USE SCHEMA ecommerce.raw;

-- Products table
CREATE TABLE products (
    product_id NUMBER AUTOINCREMENT PRIMARY KEY,
    product_name VARCHAR(255) NOT NULL,
    category VARCHAR(100),
    price NUMBER(10,2),
    cost NUMBER(10,2),
    supplier_id NUMBER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Customers table
CREATE TABLE customers (
    customer_id NUMBER AUTOINCREMENT PRIMARY KEY,
    email VARCHAR(255) UNIQUE NOT NULL,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    date_of_birth DATE,
    registration_date DATE DEFAULT CURRENT_DATE(),
    country VARCHAR(50),
    CLUSTER BY (registration_date, country)
);

-- Orders table
CREATE TABLE orders (
    order_id NUMBER AUTOINCREMENT PRIMARY KEY,
    customer_id NUMBER NOT NULL,
    order_date TIMESTAMP NOT NULL,
    status VARCHAR(20) DEFAULT 'PENDING',
    total_amount NUMBER(10,2),
    shipping_address VARIANT,
    CONSTRAINT fk_order_customer 
        FOREIGN KEY (customer_id) 
        REFERENCES customers(customer_id),
    CLUSTER BY (order_date, status)
);

-- Order items table
CREATE TABLE order_items (
    order_item_id NUMBER AUTOINCREMENT PRIMARY KEY,
    order_id NUMBER NOT NULL,
    product_id NUMBER NOT NULL,
    quantity NUMBER NOT NULL,
    unit_price NUMBER(10,2),
    line_total NUMBER(10,2) AS (quantity * unit_price),
    CONSTRAINT fk_order_item_order 
        FOREIGN KEY (order_id) 
        REFERENCES orders(order_id),
    CONSTRAINT fk_order_item_product 
        FOREIGN KEY (product_id) 
        REFERENCES products(product_id)
);


-- Table Creation with Variant Data Type

-- Table for semi-structured data (JSON)
CREATE TABLE event_logs (
    log_id NUMBER AUTOINCREMENT,
    event_type VARCHAR(50),
    event_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    user_id NUMBER,
    -- Variant column for flexible JSON data
    event_data VARIANT,
    -- Extract commonly queried fields for performance
    event_name VARCHAR(100) AS (event_data:event_name::VARCHAR),
    page_url VARCHAR(500) AS (event_data:page_url::VARCHAR),
    CLUSTER BY (event_timestamp, event_type)
);


-- Verification Queries

-- Verify database creation
SHOW DATABASES LIKE 'ecommerce';

-- Verify schemas
SHOW SCHEMAS IN DATABASE ecommerce;

-- Verify tables
SHOW TABLES IN ecommerce.raw;
SHOW TABLES IN ecommerce.staging;
SHOW TABLES IN ecommerce.analytics;

-- Describe table structure
DESC TABLE ecommerce.raw.customers;

-- Check table details
SELECT * 
FROM INFORMATION_SCHEMA.TABLES 
WHERE TABLE_CATALOG = 'ECOMMERCE';