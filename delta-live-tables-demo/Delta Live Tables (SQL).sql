-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Prerequisites
-- MAGIC - Personal Databricks Instance on Premium Tier Capacity
-- MAGIC - Enable DBFS access
-- MAGIC   - Go to settings under your account profile (right-hand side).
-- MAGIC   - Under the "Workspace Admin" section on your left side bar, click "Advanced".
-- MAGIC   - Scroll down at the bottom and look for the "DBFS File Browser" option under the section "Other" and enable it.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Business Context
-- MAGIC XYZ Retail Corporation operates an e-commerce platform that generates thousands of sales transactions daily. The Data Engineering team receives CSV and JSON files containing sales data in a storage account, updated hourly.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Problem Statement
-- MAGIC The current data processing system lacks consistency in data ingestion, leading to:
-- MAGIC - Duplicate processing of files
-- MAGIC - Missing data quality checks
-- MAGIC - No clear audit trail of processed files
-- MAGIC - Delayed reporting for business analytics

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Tech Lead's Requirements #1 (Bronze Layer)
-- MAGIC As part of our data modernization initiative, we need to implement a robust data ingestion pipeline using Delta Live Tables. Your task is to:
-- MAGIC
-- MAGIC 1. Create a bronze layer table that ingests raw sales data from our storage account
-- MAGIC 2. Implement a mechanism to prevent duplicate file processing
-- MAGIC 3. Add basic metadata tracking (file source, processing timestamp)
-- MAGIC 4. Ensure the solution is scalable for future data volume growth
-- MAGIC
-- MAGIC ## Technical Specifications
-- MAGIC - Source: Azure Storage Account
-- MAGIC - File Format: CSV, JSON
-- MAGIC - Update Frequency: Hourly
-- MAGIC - Expected Data Volume: ~10MB per hour
-- MAGIC - Required Latency: < 15 minutes
-- MAGIC
-- MAGIC ## Success Criteria
-- MAGIC - All files from storage account are processed exactly once
-- MAGIC - Each record in bronze table is traceable to its source file
-- MAGIC - Pipeline runs reliably without manual intervention
-- MAGIC - Documentation is clear and maintainable

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Sales Bronze
-- MAGIC
-- MAGIC Create a Bronze Layer Streaming Table using DLT
-- MAGIC
-- MAGIC - [How to Create Streaming Tables?](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/sql-ref-syntax-ddl-create-streaming-table)

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE sales_bronze
AS 
SELECT 
  *, 
  input_file_name() AS filesource, 
  current_timestamp() AS inserted_timestamp
FROM STREAM read_files(
  '/FileStore/demo-data/',
  format => 'csv',
  header => true,
  inferSchema => true
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Tech Lead's Requirements #2 (Silver Layer)
-- MAGIC As part of our data quality initiative, we need to implement a silver layer table that ensures data integrity. Your task is to:
-- MAGIC
-- MAGIC 1. Create a silver table that filters out invalid transactions
-- MAGIC 2. Implement data quality checks for:
-- MAGIC   - Mandatory transaction_id (cannot be null)
-- MAGIC   - Mandatory product_id (cannot be null)
-- MAGIC 3. Track data quality metrics for audit purposes
-- MAGIC 4. Document the percentage of records passing/failing quality checks
-- MAGIC
-- MAGIC ## Success Criteria
-- MAGIC - Zero null values for transaction_id and product_id in silver table
-- MAGIC - Clear visibility into rejected records
-- MAGIC - Ability to trace data lineage from bronze to silver
-- MAGIC - Documented data quality metrics for compliance

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Sales Silver

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW sales_silver
  (
  CONSTRAINT valid_transaction 
  EXPECT (
    transaction_id IS NOT NULL AND
    product_id IS NOT NULL
    )
  ON VIOLATION DROP ROW
  )
AS 
SELECT * 
FROM LIVE.sales_bronze

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Tech Lead's Requirements #3 (Gold Layer)
-- MAGIC Create a gold layer of pre-aggregated business metrics using Delta Live Tables. Your task is to:
-- MAGIC
-- MAGIC 1. Create Daily Sales Performance Metrics (daily_sales_gold):
-- MAGIC   - Track daily sales by product category
-- MAGIC   - Monitor transaction volumes
-- MAGIC   - Calculate revenue metrics
-- MAGIC   - Analyze pricing and discounts
-- MAGIC   - Measure customer engagement
-- MAGIC
-- MAGIC 2. Create Customer Purchase Pattern Analytics (customer_metrics_gold):
-- MAGIC   - Calculate customer lifetime value metrics
-- MAGIC   - Track purchase frequency
-- MAGIC   - Monitor spending patterns
-- MAGIC   - Analyze category preferences
-- MAGIC   - Measure discount utilization
-- MAGIC
-- MAGIC ## Success Criteria
-- MAGIC 1. Daily Sales Metrics:
-- MAGIC   - Complete daily aggregations
-- MAGIC   - All categories represented
-- MAGIC   - Accurate calculations
-- MAGIC   - No missing dates
-- MAGIC   - Revenue reconciliation with silver layer
-- MAGIC
-- MAGIC 2. Customer Metrics:
-- MAGIC   - All customers included
-- MAGIC   - Accurate purchase history
-- MAGIC   - Complete spending analysis
-- MAGIC   - Valid date ranges
-- MAGIC   - Proper customer attribution
-- MAGIC
-- MAGIC ## Gold Layer Table Specifications
-- MAGIC
-- MAGIC ### daily_sales_gold
-- MAGIC Metrics tracked per day and category:
-- MAGIC - total_transactions
-- MAGIC - unique_customers
-- MAGIC - total_items_sold
-- MAGIC - total_revenue
-- MAGIC - total_discounts
-- MAGIC - average_price
-- MAGIC - average_transaction_value
-- MAGIC
-- MAGIC ### customer_metrics_gold
-- MAGIC Metrics tracked per customer:
-- MAGIC - total_purchases
-- MAGIC - total_spent
-- MAGIC - average_purchase_value
-- MAGIC - unique_categories_bought
-- MAGIC - last_purchase_date
-- MAGIC - first_purchase_date
-- MAGIC - total_items_bought
-- MAGIC - total_savings

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Daily Sales Gold

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW daily_sales_gold
AS
SELECT 
    date(transaction_date) AS sale_date,
    category,
    COUNT(DISTINCT transaction_id) AS total_transactions,
    COUNT(DISTINCT customer_name) AS unique_customers,
    SUM(quantity) AS total_items_sold,
    SUM(total_amount) AS total_revenue,
    SUM(discount) AS total_discounts,
    AVG(price) AS average_price,
    SUM(total_amount)/COUNT(DISTINCT transaction_id) AS average_transaction_value
FROM LIVE.sales_silver
GROUP BY date(transaction_date), category;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Customer Metrics Gold

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW customer_metrics_gold
AS
SELECT 
    customer_name,
    COUNT(DISTINCT transaction_id) AS total_purchases,
    SUM(total_amount) AS total_spent,
    AVG(total_amount) AS average_purchase_value,
    COUNT(DISTINCT category) AS unique_categories_bought,
    MAX(transaction_date) AS last_purchase_date,
    MIN(transaction_date) AS first_purchase_date,
    SUM(quantity) AS total_items_bought,
    SUM(discount) AS total_savings
FROM LIVE.sales_silver
GROUP BY customer_name;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Tech Lead's Requirements #4 (Data Modeling)
-- MAGIC Transform our silver layer sales data into a simple star schema using Delta Live Tables. Your task is to:
-- MAGIC
-- MAGIC 1. Create the following dimension tables from sales_silver:
-- MAGIC   - DIM_LOCATION (geographical data)
-- MAGIC   - DIM_CUSTOMER (customer information)
-- MAGIC   - DIM_PRODUCT (product details)
-- MAGIC   - DIM_DATE (time dimension)
-- MAGIC   - FACT_SALES (transaction facts)
-- MAGIC
-- MAGIC 2. Ensure each dimension table:
-- MAGIC   - Has a unique surrogate key (using UUID)
-- MAGIC   - Contains no duplicate records
-- MAGIC   - Maintains referential integrity with fact table
-- MAGIC
-- MAGIC 3. Implement proper joins in fact table to:
-- MAGIC   - Connect all dimensions
-- MAGIC   - Preserve all valid transactions
-- MAGIC   - Maintain data consistency
-- MAGIC
-- MAGIC ## Success Criteria
-- MAGIC 1. Table Structure:
-- MAGIC   - Clean, deduplicated dimension tables
-- MAGIC   - Properly joined fact table
-- MAGIC   - All valid transactions preserved
-- MAGIC   - No orphaned records
-- MAGIC
-- MAGIC 2. Data Quality:
-- MAGIC   - No duplicate dimension records
-- MAGIC   - No missing joins in fact table
-- MAGIC   - Validated referential integrity
-- MAGIC   - Complete dimension attributes
-- MAGIC
-- MAGIC 3. Query Performance:
-- MAGIC   - Simplified access to dimensional data
-- MAGIC   - Improved query readability
-- MAGIC   - Faster analytical query response
-- MAGIC
-- MAGIC ## Dimensional Model Summary
-- MAGIC
-- MAGIC ### DIM_LOCATION
-- MAGIC - location_key (PK)
-- MAGIC - city
-- MAGIC - state
-- MAGIC - zip_code
-- MAGIC
-- MAGIC ### DIM_CUSTOMER
-- MAGIC - customer_key (PK)
-- MAGIC - location_key (FK)
-- MAGIC - customer_name
-- MAGIC - email
-- MAGIC
-- MAGIC ### DIM_PRODUCT
-- MAGIC - product_key (PK)
-- MAGIC - product_id
-- MAGIC - product_name
-- MAGIC - category
-- MAGIC - price
-- MAGIC
-- MAGIC ### DIM_DATE
-- MAGIC - date_key (PK)
-- MAGIC - year
-- MAGIC - month
-- MAGIC - day
-- MAGIC
-- MAGIC ### FACT_SALES
-- MAGIC - transaction_id
-- MAGIC - customer_key (FK)
-- MAGIC - product_key (FK)
-- MAGIC - location_key (FK)
-- MAGIC - date_key (FK)
-- MAGIC - quantity
-- MAGIC - price
-- MAGIC - discount
-- MAGIC - total_amount
-- MAGIC - payment_method

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Location Dimension

-- COMMAND ----------

-- CREATE OR REFRESH MATERIALIZED VIEW dim_location
-- AS 
-- SELECT 
--     uuid() AS location_key,
--     delivery_address,
--     city,
--     state,
--     zip_code
-- FROM LIVE.sales_silver
-- GROUP BY delivery_address, city, state, zip_code;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Customer Dimension

-- COMMAND ----------

-- CREATE OR REFRESH MATERIALIZED VIEW dim_customer AS
-- SELECT 
--     uuid() AS customer_key,
--     l.location_key,
--     customer_name,
--     email
-- FROM LIVE.sales_silver s
-- JOIN LIVE.dim_location l
--     ON s.delivery_address = l.delivery_address
--     AND s.city = l.city
--     AND s.state = l.state
--     AND s.zip_code = l.zip_code
-- GROUP BY customer_name, email, l.location_key;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Product Dimension

-- COMMAND ----------

-- CREATE OR REFRESH MATERIALIZED VIEW dim_product
-- AS 
-- SELECT 
--     uuid() AS product_key,
--     product_id,
--     product_name,
--     category,
--     price
-- FROM LIVE.sales_silver
-- GROUP BY product_id, product_name, category, price;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Date Dimension

-- COMMAND ----------

-- CREATE OR REFRESH MATERIALIZED VIEW dim_date
-- AS 
-- SELECT DISTINCT
--     date(transaction_date) AS date_key,
--     year(transaction_date) AS year,
--     month(transaction_date) AS month,
--     day(transaction_date) AS day
-- FROM LIVE.sales_silver
-- WHERE transaction_date IS NOT NULL;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Fact Table

-- COMMAND ----------

-- CREATE OR REFRESH MATERIALIZED VIEW fact_sales
-- AS
-- SELECT 
--     transaction_id,
--     dc.customer_key,
--     dp.product_key,
--     dl.location_key,
--     date(fs.transaction_date) AS date_key,
--     fs.quantity,
--     fs.price,
--     fs.discount,
--     fs.total_amount,
--     fs.payment_method
-- FROM LIVE.sales_silver fs
-- JOIN LIVE.dim_customer dc 
--     ON fs.customer_name = dc.customer_name 
--     AND fs.email = dc.email
-- JOIN LIVE.dim_product dp 
--     ON fs.product_id = dp.product_id
-- JOIN LIVE.dim_location dl 
--     ON fs.city = dl.city 
--     AND fs.state = dl.state 
--     AND fs.zip_code = dl.zip_code
-- WHERE fs.transaction_id IS NOT NULL;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Question and Answer
-- MAGIC
-- MAGIC - What are the Core Abstractions of DLT? Please explain the difference.
-- MAGIC - What are the different pipeline modes within DLT? Please explain the difference.
-- MAGIC - How does DLT differ from a traditional pipeline?
