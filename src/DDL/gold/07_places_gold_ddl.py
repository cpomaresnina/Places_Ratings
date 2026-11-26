# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# COMMAND ----------
# MAGIC %md
# MAGIC # Gold table definitions
# MAGIC This notebook is invoked after the Gold ETL prepares and validates all source views. Run it from ../ETL/gold/07_places_gold.py so the empty CTAS statements can infer the source schemas.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create the managed Gold schema.
# MAGIC CREATE SCHEMA IF NOT EXISTS places_ratings.gold;



# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create dim_category without replacing existing dimension data.
# MAGIC CREATE TABLE IF NOT EXISTS places_ratings.gold.dim_category
# MAGIC USING DELTA AS SELECT * FROM gold_category_source WHERE 1 = 0;



# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create dim_zone without replacing existing dimension data.
# MAGIC CREATE TABLE IF NOT EXISTS places_ratings.gold.dim_zone
# MAGIC USING DELTA AS SELECT * FROM gold_zone_source WHERE 1 = 0;



# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create dim_price_level without replacing existing dimension data.
# MAGIC CREATE TABLE IF NOT EXISTS places_ratings.gold.dim_price_level
# MAGIC USING DELTA AS SELECT * FROM gold_price_source WHERE 1 = 0;



# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create dim_date without replacing existing dimension data.
# MAGIC CREATE TABLE IF NOT EXISTS places_ratings.gold.dim_date
# MAGIC USING DELTA AS SELECT * FROM gold_date_source WHERE 1 = 0;



# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create dim_extraction without replacing existing dimension data.
# MAGIC CREATE TABLE IF NOT EXISTS places_ratings.gold.dim_extraction
# MAGIC USING DELTA AS SELECT * FROM gold_extraction_source WHERE 1 = 0;



# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create dim_business without replacing existing dimension data.
# MAGIC CREATE TABLE IF NOT EXISTS places_ratings.gold.dim_business
# MAGIC USING DELTA AS SELECT * FROM gold_business_source WHERE 1 = 0;



# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create fact_business_snapshot as a managed Delta table without replacing existing rows.
# MAGIC CREATE TABLE IF NOT EXISTS places_ratings.gold.fact_business_snapshot
# MAGIC USING DELTA AS SELECT * FROM gold_fact_source WHERE 1 = 0;



# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create bridge_business_category as a managed Delta table without replacing existing rows.
# MAGIC CREATE TABLE IF NOT EXISTS places_ratings.gold.bridge_business_category
# MAGIC USING DELTA AS SELECT * FROM gold_bridge_source WHERE 1 = 0;



