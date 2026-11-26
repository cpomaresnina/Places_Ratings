# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# COMMAND ----------
# MAGIC %md
# MAGIC # Semantic schema
# MAGIC The semantic layer contains consumer-facing views built only from Gold tables.

# COMMAND ----------
# MAGIC %sql
# MAGIC -- Create the semantic layer after Gold.
# MAGIC CREATE SCHEMA IF NOT EXISTS places_ratings.semantic COMMENT 'Consumer-facing analytical views over Gold';

