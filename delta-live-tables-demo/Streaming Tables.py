# Databricks notebook source
# MAGIC %md
# MAGIC ## Business Context
# MAGIC XYZ Retail Corporation operates an e-commerce platform that generates thousands of sales transactions daily. The Data Engineering team receives JSON files containing sales data in a storage account, updated hourly.

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
# MAGIC ## Tech Lead's Requirements
# MAGIC As part of our data modernization initiative, we need to implement a robust data ingestion pipeline using Delta Live Tables. Your task is to:
# MAGIC
# MAGIC 1. Create a bronze layer table that ingests raw sales data from our storage account
# MAGIC 2. Implement a mechanism to prevent duplicate file processing
# MAGIC 3. Add basic metadata tracking (file source, processing timestamp)
# MAGIC 4. Ensure the solution is scalable for future data volume growth

# COMMAND ----------

# MAGIC %md
# MAGIC ## Success Criteria
# MAGIC - All files from storage account are processed exactly once
# MAGIC - Each record in bronze table is traceable to its source file
# MAGIC - Pipeline runs reliably without manual intervention
# MAGIC - Documentation is clear and maintainable

# COMMAND ----------

# MAGIC %md
# MAGIC ## Technical Specifications
# MAGIC - Source: Azure Storage Account
# MAGIC - File Format: JSON
# MAGIC - Update Frequency: Hourly
# MAGIC - Expected Data Volume: ~10MB per hour
# MAGIC - Required Latency: < 15 minutes

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delta Live Table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REFRESH STREAMING TABLE customers
# MAGIC AS SELECT * FROM read_files("/databricks-datasets/retail-org/customers/", "csv")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REFRESH STREAMING TABLE sales_orders_raw
# MAGIC AS SELECT * FROM read_files("/databricks-datasets/retail-org/sales_orders/", "json")
