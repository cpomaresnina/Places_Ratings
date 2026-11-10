# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Places — Consolidated Silver loading
# MAGIC
# MAGIC ## Purpose
# MAGIC Persist the post-EDA result: one row per `(run_id, place_id)`.
# MAGIC For baseline_001 the validated source contains 1,314 distinct businesses.
# MAGIC The 3,272 appearances remain an intermediate view; this notebook does not persist
# MAGIC an observations table. Original responses and lineage remain available in Bronze.
# MAGIC
# MAGIC ## Rules selected after EDA
# MAGIC - Select the latest whole observation per business and run.
# MAGIC - Resolve timestamp ties by query_key descending, then array position descending.
# MAGIC - Preserve every distinct requested category and search nucleus as sorted arrays.
# MAGIC - Preserve optional NULL, explicit false and zero values.
# MAGIC - Do not exclude businesses by geography or temporarily closed status.
# MAGIC - Query categories and search nuclei are retrieval memberships, not verified classification
# MAGIC   or official geographic boundaries. Arrays can contain more than one value.
# MAGIC - Keep a separate row for each future run; do not overwrite previous business snapshots.
# MAGIC
# MAGIC ## Execution
# MAGIC Run from top to bottom. The first cell reuses the preparation notebook.
# MAGIC The DDL and insert-only MERGE are kept here. A rerun of the same immutable baseline
# MAGIC must insert zero rows. No Google calls, raw-file deletion, table replacement or
# MAGIC business_observations loading is included. NULL from assert_true means success.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Prepare structured observations
# MAGIC The next standalone cell executes the existing preparation notebook in this context.
# MAGIC It creates temporary views only; it does not load the earlier observations table.

# COMMAND ----------

# MAGIC %run "./04_places_silver"

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Validate source business identities before consolidation.
# MAGIC SELECT
# MAGIC     assert_true(COUNT(*) > 0, 'No source observations found.') AS source_presence_check,
# MAGIC     assert_true(COUNT_IF(
# MAGIC         run_id IS NULL OR trim(run_id) = ''
# MAGIC         OR place_id IS NULL OR trim(place_id) = ''
# MAGIC         OR observation_id IS NULL OR extracted_at_utc IS NULL
# MAGIC     ) = 0, 'Missing source business identity or observation metadata.') AS source_identity_check
# MAGIC FROM places_silver_observations;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Select the representative observation
# MAGIC This is a whole-row selection, not a maximum review count and not a field-by-field
# MAGIC combination. The EDA found no conflicting reported values in the reviewed fields
# MAGIC and no mixed missingness for review counts, website, hours, phone or price.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Select exactly one latest source observation for each business in each run.
# MAGIC CREATE OR REPLACE TEMP VIEW places_silver_latest_business AS
# MAGIC SELECT * EXCEPT (business_row_number)
# MAGIC FROM (
# MAGIC     SELECT
# MAGIC         *,
# MAGIC         ROW_NUMBER() OVER (
# MAGIC             PARTITION BY run_id, place_id
# MAGIC             ORDER BY extracted_at_utc DESC, query_key DESC, place_position DESC
# MAGIC         ) AS business_row_number
# MAGIC     FROM places_silver_observations
# MAGIC ) AS ranked
# MAGIC WHERE business_row_number = 1;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Preserve all memberships without multiplying business rows
# MAGIC One business can occur under several requested categories or search nuclei.
# MAGIC Collecting distinct values into arrays keeps these relationships inside the single
# MAGIC consolidated table. Counts and time bounds describe the source observations.
# MAGIC Search cities are search context, not verified business-address cities.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Aggregate retrieval memberships and source-coverage metadata per business.
# MAGIC CREATE OR REPLACE TEMP VIEW places_silver_business_memberships AS
# MAGIC SELECT
# MAGIC     run_id,
# MAGIC     place_id,
# MAGIC     sort_array(collect_set(requested_category)) AS requested_categories,
# MAGIC     sort_array(collect_set(study_area)) AS search_nuclei,
# MAGIC     sort_array(collect_set(parent_area)) AS search_parent_areas,
# MAGIC     sort_array(collect_set(city)) AS search_cities,
# MAGIC     COUNT(*) AS source_observation_count,
# MAGIC     MIN(extracted_at_utc) AS first_observed_at_utc,
# MAGIC     MAX(extracted_at_utc) AS last_observed_at_utc
# MAGIC FROM places_silver_observations
# MAGIC GROUP BY run_id, place_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Combine the selected business attributes with all retrieval memberships.
# MAGIC CREATE OR REPLACE TEMP VIEW places_silver_business_snapshot_base AS
# MAGIC SELECT
# MAGIC     latest.run_id AS run_id,
# MAGIC     latest.place_id AS place_id,
# MAGIC     latest.business_name AS business_name,
# MAGIC     latest.name_language AS name_language,
# MAGIC     latest.primary_type AS primary_type,
# MAGIC     latest.source_types AS source_types,
# MAGIC     latest.formatted_address AS formatted_address,
# MAGIC     latest.latitude AS latitude,
# MAGIC     latest.longitude AS longitude,
# MAGIC     latest.business_status AS business_status,
# MAGIC     latest.rating AS rating,
# MAGIC     latest.review_count AS review_count,
# MAGIC     latest.website_uri AS website_uri,
# MAGIC     latest.regular_opening_hours AS regular_opening_hours,
# MAGIC     latest.weekday_descriptions AS weekday_descriptions,
# MAGIC     latest.google_maps_uri AS google_maps_uri,
# MAGIC     latest.source_attributions AS source_attributions,
# MAGIC     latest.google_maps_links AS google_maps_links,
# MAGIC     latest.write_review_uri AS write_review_uri,
# MAGIC     latest.reviews_uri AS reviews_uri,
# MAGIC     latest.national_phone_number AS national_phone_number,
# MAGIC     latest.containing_places AS containing_places,
# MAGIC     latest.pure_service_area_business AS pure_service_area_business,
# MAGIC     latest.price_level AS price_level,
# MAGIC     membership.requested_categories AS requested_categories,
# MAGIC     membership.search_nuclei AS search_nuclei,
# MAGIC     membership.search_parent_areas AS search_parent_areas,
# MAGIC     membership.search_cities AS search_cities,
# MAGIC     membership.source_observation_count AS source_observation_count,
# MAGIC     membership.first_observed_at_utc AS first_observed_at_utc,
# MAGIC     membership.last_observed_at_utc AS last_observed_at_utc,
# MAGIC     latest.observation_id AS selected_observation_id,
# MAGIC     latest.query_key AS selected_query_key,
# MAGIC     latest.extraction_id AS selected_extraction_id,
# MAGIC     latest.source_file_path AS selected_source_file_path,
# MAGIC     latest.extracted_at_utc AS selected_extracted_at_utc,
# MAGIC     latest.mapping_version AS mapping_version,
# MAGIC     latest.field_set_version AS field_set_version,
# MAGIC     'latest_whole_observation_v1' AS consolidation_rule_version
# MAGIC FROM places_silver_latest_business AS latest
# MAGIC JOIN places_silver_business_memberships AS membership
# MAGIC     ON latest.run_id = membership.run_id
# MAGIC     AND latest.place_id = membership.place_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Fingerprint consolidated content separately from its ingestion timestamp.
# MAGIC CREATE OR REPLACE TEMP VIEW places_silver_business_snapshot_source AS
# MAGIC SELECT
# MAGIC     *,
# MAGIC     sha2(to_json(named_struct(
# MAGIC         'run_id', run_id,
# MAGIC         'place_id', place_id,
# MAGIC         'business_name', business_name,
# MAGIC         'name_language', name_language,
# MAGIC         'primary_type', primary_type,
# MAGIC         'source_types', source_types,
# MAGIC         'formatted_address', formatted_address,
# MAGIC         'latitude', latitude,
# MAGIC         'longitude', longitude,
# MAGIC         'business_status', business_status,
# MAGIC         'rating', rating,
# MAGIC         'review_count', review_count,
# MAGIC         'website_uri', website_uri,
# MAGIC         'regular_opening_hours', regular_opening_hours,
# MAGIC         'weekday_descriptions', weekday_descriptions,
# MAGIC         'google_maps_uri', google_maps_uri,
# MAGIC         'source_attributions', source_attributions,
# MAGIC         'google_maps_links', google_maps_links,
# MAGIC         'write_review_uri', write_review_uri,
# MAGIC         'reviews_uri', reviews_uri,
# MAGIC         'national_phone_number', national_phone_number,
# MAGIC         'containing_places', containing_places,
# MAGIC         'pure_service_area_business', pure_service_area_business,
# MAGIC         'price_level', price_level,
# MAGIC         'requested_categories', requested_categories,
# MAGIC         'search_nuclei', search_nuclei,
# MAGIC         'search_parent_areas', search_parent_areas,
# MAGIC         'search_cities', search_cities,
# MAGIC         'source_observation_count', source_observation_count,
# MAGIC         'first_observed_at_utc', first_observed_at_utc,
# MAGIC         'last_observed_at_utc', last_observed_at_utc,
# MAGIC         'selected_observation_id', selected_observation_id,
# MAGIC         'selected_query_key', selected_query_key,
# MAGIC         'selected_extraction_id', selected_extraction_id,
# MAGIC         'selected_source_file_path', selected_source_file_path,
# MAGIC         'selected_extracted_at_utc', selected_extracted_at_utc,
# MAGIC         'mapping_version', mapping_version,
# MAGIC         'field_set_version', field_set_version,
# MAGIC         'consolidation_rule_version', consolidation_rule_version
# MAGIC     )), 256) AS snapshot_content_hash
# MAGIC FROM places_silver_business_snapshot_base;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Validate the consolidated grain
# MAGIC The target row count must match distinct `(run_id, place_id)` source identities,
# MAGIC not the appearance count. Membership counts must reconcile to the source appearances.
# MAGIC For the current baseline: expect 1,314 rows and 3,272 source appearances accounted for.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Inspect business counts, source coverage and membership multiplicity.
# MAGIC SELECT
# MAGIC     run_id,
# MAGIC     COUNT(*) AS consolidated_business_rows,
# MAGIC     COUNT(DISTINCT place_id) AS distinct_businesses,
# MAGIC     SUM(source_observation_count) AS source_observations_accounted_for,
# MAGIC     COUNT_IF(size(requested_categories) > 1) AS businesses_with_multiple_categories,
# MAGIC     COUNT_IF(size(search_nuclei) > 1) AS businesses_with_multiple_nuclei
# MAGIC FROM places_silver_business_snapshot_source
# MAGIC GROUP BY run_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Require exactly one consolidated row per source business and complete coverage.
# MAGIC SELECT
# MAGIC     assert_true(
# MAGIC         (SELECT COUNT(*) FROM places_silver_business_snapshot_source)
# MAGIC         = (SELECT COUNT(*) FROM (
# MAGIC             SELECT DISTINCT run_id, place_id FROM places_silver_observations
# MAGIC         ) AS source_keys),
# MAGIC         'Consolidated row count does not match source business identities.'
# MAGIC     ) AS business_count_check,
# MAGIC     assert_true(
# MAGIC         (SELECT COUNT(*) FROM (
# MAGIC             SELECT run_id, place_id
# MAGIC             FROM places_silver_business_snapshot_source
# MAGIC             GROUP BY run_id, place_id
# MAGIC             HAVING COUNT(*) > 1
# MAGIC         ) AS duplicate_keys) = 0,
# MAGIC         'Duplicate consolidated business identity.'
# MAGIC     ) AS business_uniqueness_check,
# MAGIC     assert_true(
# MAGIC         (SELECT SUM(source_observation_count) FROM places_silver_business_snapshot_source)
# MAGIC         = (SELECT COUNT(*) FROM places_silver_observations),
# MAGIC         'Not all source observations are accounted for.'
# MAGIC     ) AS observation_coverage_check;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Define the consolidated Delta table
# MAGIC The empty SELECT derives nested column types from the validated source view.
# MAGIC CREATE TABLE IF NOT EXISTS does not erase data or migrate an incompatible existing schema.
# MAGIC The previous business_observations table is not read, updated or deleted here.

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

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Reject duplicate target keys before inserting consolidated businesses.
# MAGIC SELECT assert_true(
# MAGIC     COUNT(*) = 0,
# MAGIC     'Duplicate run_id/place_id identities already exist in business_snapshots.'
# MAGIC ) AS target_uniqueness_check
# MAGIC FROM (
# MAGIC     SELECT run_id, place_id
# MAGIC     FROM places_ratings.silver.business_snapshots
# MAGIC     WHERE run_id IN (SELECT DISTINCT run_id FROM places_silver_business_snapshot_source)
# MAGIC     GROUP BY run_id, place_id
# MAGIC     HAVING COUNT(*) > 1
# MAGIC ) AS duplicates;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Reject silently changed snapshots for an already loaded business/run identity.
# MAGIC SELECT assert_true(
# MAGIC     COUNT(*) = 0,
# MAGIC     'Existing snapshot content differs. Review the consolidation change before loading.'
# MAGIC ) AS immutable_snapshot_check
# MAGIC FROM places_silver_business_snapshot_source AS source
# MAGIC JOIN places_ratings.silver.business_snapshots AS target
# MAGIC     ON target.run_id = source.run_id AND target.place_id = source.place_id
# MAGIC WHERE NOT (target.snapshot_content_hash <=> source.snapshot_content_hash);

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Load and verify
# MAGIC The MERGE inserts only previously unseen business/run identities. It does not
# MAGIC update a historical run. A new monthly run produces a new row for each business.
# MAGIC The content check above detects accidental rule changes within an existing run.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Insert one consolidated row per business and run without duplicating a rerun.
# MAGIC MERGE INTO places_ratings.silver.business_snapshots AS target
# MAGIC USING (
# MAGIC     SELECT
# MAGIC         *,
# MAGIC         current_timestamp() AS silver_ingested_at
# MAGIC     FROM places_silver_business_snapshot_source
# MAGIC ) AS source
# MAGIC ON target.run_id = source.run_id
# MAGIC    AND target.place_id = source.place_id
# MAGIC WHEN NOT MATCHED THEN
# MAGIC INSERT *;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify that every selected business snapshot was persisted with matching content.
# MAGIC SELECT assert_true(
# MAGIC     COUNT(*) = 0,
# MAGIC     'Snapshot reconciliation failed: a business is missing or its content differs.'
# MAGIC ) AS snapshot_reconciliation_check
# MAGIC FROM places_silver_business_snapshot_source AS source
# MAGIC LEFT JOIN places_ratings.silver.business_snapshots AS target
# MAGIC     ON target.run_id = source.run_id AND target.place_id = source.place_id
# MAGIC WHERE target.place_id IS NULL
# MAGIC    OR NOT (target.snapshot_content_hash <=> source.snapshot_content_hash);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Show persisted business counts and representative membership arrays.
# MAGIC SELECT
# MAGIC     run_id,
# MAGIC     COUNT(*) AS business_rows,
# MAGIC     COUNT(DISTINCT place_id) AS distinct_businesses,
# MAGIC     SUM(source_observation_count) AS source_observations_accounted_for
# MAGIC FROM places_ratings.silver.business_snapshots
# MAGIC GROUP BY run_id
# MAGIC ORDER BY run_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Inspect one-row-per-business output with all associated search memberships.
# MAGIC SELECT
# MAGIC     place_id,
# MAGIC     business_name,
# MAGIC     requested_categories,
# MAGIC     search_nuclei,
# MAGIC     review_count,
# MAGIC     website_uri,
# MAGIC     price_level,
# MAGIC     source_observation_count
# MAGIC FROM places_ratings.silver.business_snapshots
# MAGIC WHERE run_id = 'baseline_001'
# MAGIC ORDER BY business_name, place_id
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from places_ratings.silver.business_snapshots

# COMMAND ----------

# MAGIC %md
# MAGIC ## Expected current result
# MAGIC - business_snapshots: 1,314 business rows for baseline_001.
# MAGIC - source_observations_accounted_for: 3,272, stored as counts rather than repeated rows.
# MAGIC - Repeating the MERGE inserts zero rows.
# MAGIC - The original 3,272-appearance responses remain recoverable from Bronze.
# MAGIC
# MAGIC This notebook was structurally reviewed locally. Actual SQL execution and table
# MAGIC counts must be verified in Databricks. No remote data or tables were changed here.
# MAGIC After validation, the unused business_observations table may be removed separately.

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from places_ratings.silver.business_snapshots