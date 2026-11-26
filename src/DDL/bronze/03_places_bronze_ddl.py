# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# COMMAND ----------
# MAGIC %md
# MAGIC # Bronze table definition
# MAGIC Create the source-response table before loading Bronze. This file is also run from the Bronze ETL notebook.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create the Bronze schema and table without replacing existing data.
# MAGIC CREATE SCHEMA IF NOT EXISTS places_ratings.bronze
# MAGIC COMMENT 'Original source responses and ingestion metadata';
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS places_ratings.bronze.api_responses (
# MAGIC     source_file_path STRING,
# MAGIC     source_modified_at TIMESTAMP,
# MAGIC     source_size_bytes BIGINT,
# MAGIC     source_content_hash STRING,
# MAGIC     extraction_id STRING,
# MAGIC     data_kind STRING,
# MAGIC     study_area STRING,
# MAGIC     requested_category STRING,
# MAGIC     run_id STRING,
# MAGIC     query_key STRING,
# MAGIC     mapping_version STRING,
# MAGIC     field_set_version STRING,
# MAGIC     extracted_at_utc TIMESTAMP,
# MAGIC     raw_json STRING,
# MAGIC     bronze_ingested_at TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Original Places API baseline responses with execution metadata';



