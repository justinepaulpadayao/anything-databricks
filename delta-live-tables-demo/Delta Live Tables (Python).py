# Databricks notebook source
# MAGIC %md
# MAGIC ## Prerequisites
# MAGIC - Personal Databricks Instance on Premium Tier Capacity
# MAGIC - Enable DBFS access
# MAGIC   - Go to settings under your account profile (right-hand side).
# MAGIC   - Under the "Workspace Admin" section on your left side bar, click "Advanced".
# MAGIC   - Scroll down at the bottom and look for the "DBFS File Browser" option under the section "Other" and enable it.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Business Context
# MAGIC XYZ Retail Corporation operates an e-commerce platform that generates thousands of sales transactions daily. The Data Engineering team receives CSV and JSON files containing sales data in a storage account, updated hourly.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem Statement
# MAGIC The current data processing system lacks consistency in data ingestion, leading to:
# MAGIC - Duplicate processing of files
# MAGIC - Missing data quality checks
# MAGIC - No clear audit trail of processed files
# MAGIC - Delayed reporting for business analytics

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tech Lead's Requirements #1 (Bronze Layer)
# MAGIC As part of our data modernization initiative, we need to implement a robust data ingestion pipeline using Delta Live Tables. Your task is to:
# MAGIC
# MAGIC 1. Create a bronze layer table that ingests raw sales data from our storage account
# MAGIC 2. Implement a mechanism to prevent duplicate file processing
# MAGIC 3. Add basic metadata tracking (file source, processing timestamp)
# MAGIC 4. Ensure the solution is scalable for future data volume growth
# MAGIC
# MAGIC ## Technical Specifications
# MAGIC - Source: Azure Storage Account
# MAGIC - File Format: CSV, JSON
# MAGIC - Update Frequency: Hourly
# MAGIC - Expected Data Volume: ~10MB per hour
# MAGIC - Required Latency: < 15 minutes
# MAGIC
# MAGIC ## Success Criteria
# MAGIC - All files from storage account are processed exactly once
# MAGIC - Each record in bronze table is traceable to its source file
# MAGIC - Pipeline runs reliably without manual intervention
# MAGIC - Documentation is clear and maintainable

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sales Bronze
# MAGIC
# MAGIC Create a Bronze Layer Streaming Table using DLT
# MAGIC
# MAGIC - [How to Create Streaming Tables?](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/sql-ref-syntax-ddl-create-streaming-table)

# COMMAND ----------

import dlt

@dlt.table(
  name="sales_bronze_python",
  comment="This table contains the account contact list contacts data."
)
def account_contact_list_contacts():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("inferSchema", "true")
            .option("header", "true")
            .load("/FileStore/demo-data/")
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## Tech Lead's Requirements #2 (Silver Layer)
# MAGIC As part of our data quality initiative, we need to implement a silver layer table that ensures data integrity. Your task is to:
# MAGIC
# MAGIC 1. Create a silver table that filters out invalid transactions
# MAGIC 2. Implement data quality checks for:
# MAGIC   - Mandatory transaction_id (cannot be null)
# MAGIC   - Mandatory product_id (cannot be null)
# MAGIC 3. Track data quality metrics for audit purposes
# MAGIC 4. Document the percentage of records passing/failing quality checks
# MAGIC
# MAGIC ## Success Criteria
# MAGIC - Zero null values for transaction_id and product_id in silver table
# MAGIC - Clear visibility into rejected records
# MAGIC - Ability to trace data lineage from bronze to silver
# MAGIC - Documented data quality metrics for compliance

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sales Silver

# COMMAND ----------

import dlt

@dlt.table(
  name="sales_silver",
  comment="This view contains the clean sales data."
)
@dlt.expect_or_drop("valid_transaction", 
                    "transaction_id IS NOT NULL AND product_id IS NOT NULL")
def sales_silver():
    return dlt.read("sales_bronze_python")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tech Lead's Requirements #3 (Gold Layer)
# MAGIC Create a gold layer of pre-aggregated business metrics using Delta Live Tables. Your task is to:
# MAGIC
# MAGIC 1. Create Daily Sales Performance Metrics (daily_sales_gold):
# MAGIC   - Track daily sales by product category
# MAGIC   - Monitor transaction volumes
# MAGIC   - Calculate revenue metrics
# MAGIC   - Analyze pricing and discounts
# MAGIC   - Measure customer engagement
# MAGIC
# MAGIC 2. Create Customer Purchase Pattern Analytics (customer_metrics_gold):
# MAGIC   - Calculate customer lifetime value metrics
# MAGIC   - Track purchase frequency
# MAGIC   - Monitor spending patterns
# MAGIC   - Analyze category preferences
# MAGIC   - Measure discount utilization
# MAGIC
# MAGIC ## Success Criteria
# MAGIC 1. Daily Sales Metrics:
# MAGIC   - Complete daily aggregations
# MAGIC   - All categories represented
# MAGIC   - Accurate calculations
# MAGIC   - No missing dates
# MAGIC   - Revenue reconciliation with silver layer
# MAGIC
# MAGIC 2. Customer Metrics:
# MAGIC   - All customers included
# MAGIC   - Accurate purchase history
# MAGIC   - Complete spending analysis
# MAGIC   - Valid date ranges
# MAGIC   - Proper customer attribution
# MAGIC
# MAGIC ## Gold Layer Table Specifications
# MAGIC
# MAGIC ### daily_sales_gold
# MAGIC Metrics tracked per day and category:
# MAGIC - total_transactions
# MAGIC - unique_customers
# MAGIC - total_items_sold
# MAGIC - total_revenue
# MAGIC - total_discounts
# MAGIC - average_price
# MAGIC - average_transaction_value
# MAGIC
# MAGIC ### customer_metrics_gold
# MAGIC Metrics tracked per customer:
# MAGIC - total_purchases
# MAGIC - total_spent
# MAGIC - average_purchase_value
# MAGIC - unique_categories_bought
# MAGIC - last_purchase_date
# MAGIC - first_purchase_date
# MAGIC - total_items_bought
# MAGIC - total_savings

# COMMAND ----------

# MAGIC %md
# MAGIC #### Task #1: Recreate Daily Sales Gold using the Python Syntax

# COMMAND ----------

# %sql
# CREATE OR REFRESH MATERIALIZED VIEW daily_sales_gold
# AS
# SELECT 
#     date(transaction_date) AS sale_date,
#     category,
#     COUNT(DISTINCT transaction_id) AS total_transactions,
#     COUNT(DISTINCT customer_name) AS unique_customers,
#     SUM(quantity) AS total_items_sold,
#     SUM(total_amount) AS total_revenue,
#     SUM(discount) AS total_discounts,
#     AVG(price) AS average_price,
#     SUM(total_amount)/COUNT(DISTINCT transaction_id) AS average_transaction_value
# FROM LIVE.sales_silver
# GROUP BY date(transaction_date), category;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Task #2: Recreate Customer Metrics Gold using the Python Syntax

# COMMAND ----------

# %sql
# CREATE OR REFRESH MATERIALIZED VIEW customer_metrics_gold
# AS
# SELECT 
#     customer_name,
#     COUNT(DISTINCT transaction_id) AS total_purchases,
#     SUM(total_amount) AS total_spent,
#     AVG(total_amount) AS average_purchase_value,
#     COUNT(DISTINCT category) AS unique_categories_bought,
#     MAX(transaction_date) AS last_purchase_date,
#     MIN(transaction_date) AS first_purchase_date,
#     SUM(quantity) AS total_items_bought,
#     SUM(discount) AS total_savings
# FROM LIVE.sales_silver
# GROUP BY customer_name;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tech Lead's Requirements #4 (Data Modeling)
# MAGIC Transform our silver layer sales data into a simple star schema using Delta Live Tables. Your task is to:
# MAGIC
# MAGIC 1. Create the following dimension tables from sales_silver:
# MAGIC   - DIM_LOCATION (geographical data)
# MAGIC   - DIM_CUSTOMER (customer information)
# MAGIC   - DIM_PRODUCT (product details)
# MAGIC   - DIM_DATE (time dimension)
# MAGIC   - FACT_SALES (transaction facts)
# MAGIC
# MAGIC 2. Ensure each dimension table:
# MAGIC   - Has a unique surrogate key (using UUID)
# MAGIC   - Contains no duplicate records
# MAGIC   - Maintains referential integrity with fact table
# MAGIC
# MAGIC 3. Implement proper joins in fact table to:
# MAGIC   - Connect all dimensions
# MAGIC   - Preserve all valid transactions
# MAGIC   - Maintain data consistency
# MAGIC
# MAGIC ## Success Criteria
# MAGIC 1. Table Structure:
# MAGIC   - Clean, deduplicated dimension tables
# MAGIC   - Properly joined fact table
# MAGIC   - All valid transactions preserved
# MAGIC   - No orphaned records
# MAGIC
# MAGIC 2. Data Quality:
# MAGIC   - No duplicate dimension records
# MAGIC   - No missing joins in fact table
# MAGIC   - Validated referential integrity
# MAGIC   - Complete dimension attributes
# MAGIC
# MAGIC 3. Query Performance:
# MAGIC   - Simplified access to dimensional data
# MAGIC   - Improved query readability
# MAGIC   - Faster analytical query response
# MAGIC
# MAGIC ## Dimensional Model Summary
# MAGIC
# MAGIC ### DIM_LOCATION
# MAGIC - location_key (PK)
# MAGIC - city
# MAGIC - state
# MAGIC - zip_code
# MAGIC
# MAGIC ### DIM_CUSTOMER
# MAGIC - customer_key (PK)
# MAGIC - location_key (FK)
# MAGIC - customer_name
# MAGIC - email
# MAGIC
# MAGIC ### DIM_PRODUCT
# MAGIC - product_key (PK)
# MAGIC - product_id
# MAGIC - product_name
# MAGIC - category
# MAGIC - price
# MAGIC
# MAGIC ### DIM_DATE
# MAGIC - date_key (PK)
# MAGIC - year
# MAGIC - month
# MAGIC - day
# MAGIC
# MAGIC ### FACT_SALES
# MAGIC - transaction_id
# MAGIC - customer_key (FK)
# MAGIC - product_key (FK)
# MAGIC - location_key (FK)
# MAGIC - date_key (FK)
# MAGIC - quantity
# MAGIC - price
# MAGIC - discount
# MAGIC - total_amount
# MAGIC - payment_method

# COMMAND ----------

# MAGIC %md
# MAGIC #### Task #3: Recreate Location Dimension using the Python Syntax

# COMMAND ----------

# MAGIC %sql
# MAGIC -- CREATE OR REFRESH MATERIALIZED VIEW dim_location
# MAGIC -- AS 
# MAGIC -- SELECT 
# MAGIC --     uuid() AS location_key,
# MAGIC --     delivery_address,
# MAGIC --     city,
# MAGIC --     state,
# MAGIC --     zip_code
# MAGIC -- FROM LIVE.sales_silver
# MAGIC -- GROUP BY delivery_address, city, state, zip_code;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Task #4: Recreate Customer Dimension using the Python Syntax

# COMMAND ----------

# MAGIC %sql
# MAGIC -- CREATE OR REFRESH MATERIALIZED VIEW dim_customer AS
# MAGIC -- SELECT 
# MAGIC --     uuid() AS customer_key,
# MAGIC --     l.location_key,
# MAGIC --     customer_name,
# MAGIC --     email
# MAGIC -- FROM LIVE.sales_silver s
# MAGIC -- JOIN LIVE.dim_location l
# MAGIC --     ON s.delivery_address = l.delivery_address
# MAGIC --     AND s.city = l.city
# MAGIC --     AND s.state = l.state
# MAGIC --     AND s.zip_code = l.zip_code
# MAGIC -- GROUP BY customer_name, email, l.location_key;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Task #5: Recreate Product Dimension using the Python Syntax

# COMMAND ----------

# MAGIC %sql
# MAGIC -- CREATE OR REFRESH MATERIALIZED VIEW dim_product
# MAGIC -- AS 
# MAGIC -- SELECT 
# MAGIC --     uuid() AS product_key,
# MAGIC --     product_id,
# MAGIC --     product_name,
# MAGIC --     category,
# MAGIC --     price
# MAGIC -- FROM LIVE.sales_silver
# MAGIC -- GROUP BY product_id, product_name, category, price;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Task #6: Recreate Date Dimension using the Python Syntax

# COMMAND ----------

# MAGIC %sql
# MAGIC -- CREATE OR REFRESH MATERIALIZED VIEW dim_date
# MAGIC -- AS 
# MAGIC -- SELECT DISTINCT
# MAGIC --     date(transaction_date) AS date_key,
# MAGIC --     year(transaction_date) AS year,
# MAGIC --     month(transaction_date) AS month,
# MAGIC --     day(transaction_date) AS day
# MAGIC -- FROM LIVE.sales_silver
# MAGIC -- WHERE transaction_date IS NOT NULL;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Task #7: Recreate Fact Sales Table using the Python Syntax

# COMMAND ----------

# MAGIC %sql
# MAGIC -- CREATE OR REFRESH MATERIALIZED VIEW fact_sales
# MAGIC -- AS
# MAGIC -- SELECT 
# MAGIC --     transaction_id,
# MAGIC --     dc.customer_key,
# MAGIC --     dp.product_key,
# MAGIC --     dl.location_key,
# MAGIC --     date(fs.transaction_date) AS date_key,
# MAGIC --     fs.quantity,
# MAGIC --     fs.price,
# MAGIC --     fs.discount,
# MAGIC --     fs.total_amount,
# MAGIC --     fs.payment_method
# MAGIC -- FROM LIVE.sales_silver fs
# MAGIC -- JOIN LIVE.dim_customer dc 
# MAGIC --     ON fs.customer_name = dc.customer_name 
# MAGIC --     AND fs.email = dc.email
# MAGIC -- JOIN LIVE.dim_product dp 
# MAGIC --     ON fs.product_id = dp.product_id
# MAGIC -- JOIN LIVE.dim_location dl 
# MAGIC --     ON fs.city = dl.city 
# MAGIC --     AND fs.state = dl.state 
# MAGIC --     AND fs.zip_code = dl.zip_code
# MAGIC -- WHERE fs.transaction_id IS NOT NULL;
