# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# COMMAND ----------
# MAGIC %md
# MAGIC # Silver business snapshot table
# MAGIC Run this DDL through the Silver load notebook after it prepares and validates places_silver_business_snapshot_source.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create the consolidated Silver table without replacing existing snapshots.
# MAGIC CREATE SCHEMA IF NOT EXISTS places_ratings.silver;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS places_ratings.silver.business_snapshots
# MAGIC USING DELTA
# MAGIC COMMENT 'One consolidated business snapshot per extraction run and Google place ID'
# MAGIC AS
# MAGIC SELECT
# MAGIC     *,
# MAGIC     current_timestamp() AS silver_ingested_at
# MAGIC FROM places_silver_business_snapshot_source
# MAGIC WHERE 1 = 0;



