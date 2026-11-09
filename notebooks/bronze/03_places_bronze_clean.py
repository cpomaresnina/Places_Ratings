# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Places API — Bronze ingestion
# MAGIC
# MAGIC ## Purpose and grain
# MAGIC One row per response to a planned query within an extraction run.
# MAGIC The immutable business key is `(run_id, query_key)`; it is not a business ID.
# MAGIC The original JSON includes every requested business field. Business-level
# MAGIC columns and deduplication are handled later in Silver.
# MAGIC
# MAGIC ## Current source
# MAGIC - Run: `baseline_001`
# MAGIC - Expected source files: 315
# MAGIC - Mapping: `0.2`; field set: `full_context_v1`
# MAGIC - Only the selected baseline's `observed/` directory is read.
# MAGIC
# MAGIC ## Execution
# MAGIC Run cells from top to bottom. The DDL creates the schema and table only if
# MAGIC missing. The MERGE inserts new query responses without updating or deleting
# MAGIC existing ones. Repeating this baseline should insert zero rows.
# MAGIC
# MAGIC Stop at any validation error and review its cause. Do not skip validation cells.
# MAGIC Do not run concurrently with changes to the selected landing files.
# MAGIC No Google API calls or S3 deletions are performed by this notebook.
# MAGIC
# MAGIC For a future run, update the source path, expected run ID, expected response
# MAGIC count and version expectations as needed. Do not use CREATE OR REPLACE TABLE
# MAGIC for routine ingestion: prior runs must remain in Bronze.
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS does not migrate an incompatible existing schema.
# MAGIC The existing table must have the current columns; review SHOW CREATE TABLE
# MAGIC if a column-resolution error occurs.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Define the schema and table

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

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Read the selected baseline and prepare metadata

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Read only the verified full-context baseline, excluding earlier trial files.
# MAGIC CREATE OR REPLACE TEMP VIEW places_bronze_source AS
# MAGIC SELECT
# MAGIC     path AS source_file_path,
# MAGIC     modificationTime AS source_modified_at,
# MAGIC     length AS source_size_bytes,
# MAGIC     decode(content, 'UTF-8') AS raw_json
# MAGIC FROM read_files(
# MAGIC     '/Volumes/places_ratings/landing/source_files/baselines/baseline_001/observed',
# MAGIC     format => 'binaryFile',
# MAGIC     recursiveFileLookup => true,
# MAGIC     pathGlobFilter => '*.json'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Prepare baseline responses with execution metadata and original JSON.
# MAGIC CREATE OR REPLACE TEMP VIEW places_bronze_prepared AS
# MAGIC SELECT
# MAGIC     source_file_path,
# MAGIC     source_modified_at,
# MAGIC     source_size_bytes,
# MAGIC     sha2(raw_json, 256) AS source_content_hash,
# MAGIC     get_json_object(raw_json, '$.extraction_id') AS extraction_id,
# MAGIC     get_json_object(raw_json, '$.data_kind') AS data_kind,
# MAGIC     get_json_object(raw_json, '$.study_area') AS study_area,
# MAGIC     get_json_object(raw_json, '$.requested_category') AS requested_category,
# MAGIC     get_json_object(raw_json, '$.extraction.run_id') AS run_id,
# MAGIC     get_json_object(raw_json, '$.extraction.query_key') AS query_key,
# MAGIC     get_json_object(
# MAGIC         raw_json, '$.extraction.mapping_version'
# MAGIC     ) AS mapping_version,
# MAGIC     get_json_object(
# MAGIC         raw_json, '$.extraction.field_set_version'
# MAGIC     ) AS field_set_version,
# MAGIC     TRY_CAST(
# MAGIC         get_json_object(raw_json, '$.extraction.extracted_at_utc')
# MAGIC         AS TIMESTAMP
# MAGIC     ) AS extracted_at_utc,
# MAGIC     raw_json
# MAGIC FROM places_bronze_source;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Inspect source counts and volume

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Summarize the selected response files before loading.
# MAGIC SELECT
# MAGIC     run_id,
# MAGIC     mapping_version,
# MAGIC     field_set_version,
# MAGIC     COUNT(*) AS source_files,
# MAGIC     COUNT(DISTINCT source_file_path) AS distinct_paths,
# MAGIC     COUNT(DISTINCT query_key) AS distinct_queries,
# MAGIC     COUNT(DISTINCT extraction_id) AS distinct_extractions,
# MAGIC     SUM(source_size_bytes) AS total_bytes,
# MAGIC     ROUND(SUM(source_size_bytes) / (1024.0 * 1024.0), 2) AS total_mib
# MAGIC FROM places_bronze_prepared
# MAGIC GROUP BY run_id, mapping_version, field_set_version;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Validate before writing
# MAGIC Successful assert_true checks return NULL. That is success, not missing data. A failed check raises an error.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Reject an incomplete baseline, duplicate source identities or invalid metadata.
# MAGIC WITH source_checks AS (
# MAGIC     SELECT
# MAGIC         COUNT(*) AS source_rows,
# MAGIC         COUNT(DISTINCT source_file_path) AS distinct_paths,
# MAGIC         COUNT(DISTINCT query_key) AS distinct_queries,
# MAGIC         COUNT(DISTINCT extraction_id) AS distinct_extractions,
# MAGIC         COUNT_IF(
# MAGIC             extraction_id IS NULL OR trim(extraction_id) = ''
# MAGIC             OR run_id IS NULL OR run_id <> 'baseline_001'
# MAGIC             OR query_key IS NULL OR trim(query_key) = ''
# MAGIC             OR mapping_version IS NULL OR mapping_version <> '0.2'
# MAGIC             OR field_set_version IS NULL OR field_set_version <> 'full_context_v1'
# MAGIC             OR study_area IS NULL OR trim(study_area) = ''
# MAGIC             OR requested_category IS NULL OR trim(requested_category) = ''
# MAGIC             OR extracted_at_utc IS NULL
# MAGIC             OR data_kind IS NULL OR data_kind <> 'observed'
# MAGIC             OR raw_json IS NULL OR source_content_hash IS NULL
# MAGIC         ) AS invalid_metadata_rows
# MAGIC     FROM places_bronze_prepared
# MAGIC )
# MAGIC SELECT
# MAGIC     assert_true(
# MAGIC         source_rows = 315,
# MAGIC         'Unexpected source count: baseline_001 requires 315 response files.'
# MAGIC     ) AS response_count_check,
# MAGIC     assert_true(
# MAGIC         source_rows = distinct_paths
# MAGIC         AND source_rows = distinct_queries
# MAGIC         AND source_rows = distinct_extractions,
# MAGIC         'Duplicate source path, query key or extraction ID detected.'
# MAGIC     ) AS source_uniqueness_check,
# MAGIC     assert_true(
# MAGIC         invalid_metadata_rows = 0,
# MAGIC         'Required metadata is missing or differs from the expected baseline definition.'
# MAGIC     ) AS metadata_check
# MAGIC FROM source_checks;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Reject duplicate target keys for the run being loaded.
# MAGIC SELECT assert_true(
# MAGIC     COUNT(*) = 0,
# MAGIC     'Duplicate (run_id, query_key) identities already exist in Bronze.'
# MAGIC ) AS target_uniqueness_check
# MAGIC FROM (
# MAGIC     SELECT run_id, query_key
# MAGIC     FROM places_ratings.bronze.api_responses
# MAGIC     WHERE run_id IN (SELECT DISTINCT run_id FROM places_bronze_prepared)
# MAGIC     GROUP BY run_id, query_key
# MAGIC     HAVING COUNT(*) > 1
# MAGIC ) AS duplicate_keys;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Reject changed content instead of silently ignoring an existing query identity.
# MAGIC SELECT assert_true(
# MAGIC     COUNT(*) = 0,
# MAGIC     'An existing query has changed content. Preserve the original or use a new run_id.'
# MAGIC ) AS immutable_content_check
# MAGIC FROM places_bronze_prepared AS source
# MAGIC JOIN places_ratings.bronze.api_responses AS target
# MAGIC     ON target.run_id = source.run_id
# MAGIC     AND target.query_key = source.query_key
# MAGIC WHERE NOT (target.source_content_hash <=> source.source_content_hash);

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Insert new responses

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Insert new baseline responses while preserving previously ingested runs.
# MAGIC MERGE INTO places_ratings.bronze.api_responses AS target
# MAGIC USING places_bronze_prepared AS source
# MAGIC ON target.run_id = source.run_id
# MAGIC    AND target.query_key = source.query_key
# MAGIC WHEN NOT MATCHED THEN
# MAGIC INSERT (
# MAGIC     source_file_path,
# MAGIC     source_modified_at,
# MAGIC     source_size_bytes,
# MAGIC     source_content_hash,
# MAGIC     extraction_id,
# MAGIC     data_kind,
# MAGIC     study_area,
# MAGIC     requested_category,
# MAGIC     run_id,
# MAGIC     query_key,
# MAGIC     mapping_version,
# MAGIC     field_set_version,
# MAGIC     extracted_at_utc,
# MAGIC     raw_json,
# MAGIC     bronze_ingested_at
# MAGIC )
# MAGIC VALUES (
# MAGIC     source.source_file_path,
# MAGIC     source.source_modified_at,
# MAGIC     source.source_size_bytes,
# MAGIC     source.source_content_hash,
# MAGIC     source.extraction_id,
# MAGIC     source.data_kind,
# MAGIC     source.study_area,
# MAGIC     source.requested_category,
# MAGIC     source.run_id,
# MAGIC     source.query_key,
# MAGIC     source.mapping_version,
# MAGIC     source.field_set_version,
# MAGIC     source.extracted_at_utc,
# MAGIC     source.raw_json,
# MAGIC     current_timestamp()
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Verify persisted results

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify row counts and identities while retaining any earlier baseline runs.
# MAGIC SELECT
# MAGIC     run_id,
# MAGIC     mapping_version,
# MAGIC     field_set_version,
# MAGIC     COUNT(*) AS bronze_rows,
# MAGIC     COUNT(DISTINCT query_key) AS distinct_queries,
# MAGIC     COUNT(DISTINCT extraction_id) AS distinct_extractions
# MAGIC FROM places_ratings.bronze.api_responses
# MAGIC GROUP BY run_id, mapping_version, field_set_version
# MAGIC ORDER BY run_id, mapping_version, field_set_version;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Confirm the selected source is present in Bronze with identical content.
# MAGIC SELECT assert_true(
# MAGIC     COUNT(*) = 0,
# MAGIC     'Source-to-Bronze reconciliation failed: a query is missing or its content differs.'
# MAGIC ) AS source_reconciliation_check
# MAGIC FROM places_bronze_prepared AS source
# MAGIC LEFT JOIN places_ratings.bronze.api_responses AS target
# MAGIC     ON target.run_id = source.run_id
# MAGIC     AND target.query_key = source.query_key
# MAGIC WHERE target.query_key IS NULL
# MAGIC     OR NOT (target.source_content_hash <=> source.source_content_hash);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Confirm the selected baseline has no unexpected additional query rows.
# MAGIC SELECT assert_true(
# MAGIC     COUNT(*) = 315 AND COUNT(DISTINCT query_key) = 315,
# MAGIC     'Bronze baseline_001 must contain exactly 315 distinct query responses.'
# MAGIC ) AS baseline_completeness_check
# MAGIC FROM places_ratings.bronze.api_responses
# MAGIC WHERE run_id = 'baseline_001';

# COMMAND ----------

# MAGIC %md
# MAGIC ## Expected result for this baseline
# MAGIC - `baseline_001`, `0.2`, `full_context_v1`: 315 rows and 315 distinct queries/extractions.
# MAGIC - Repeating the MERGE against the current loaded table: zero inserted rows.
# MAGIC - Validation cells: NULL values indicate checks passed; errors require investigation.
# MAGIC - API context fields remain preserved inside raw_json until Silver parses them.
# MAGIC
# MAGIC This revision was reviewed locally. SQL execution against Databricks and the
# MAGIC external Volume must be verified in the workspace. No remote data was changed
# MAGIC while preparing this file.