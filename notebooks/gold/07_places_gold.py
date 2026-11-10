# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Places — Gold: dimensional model and analytical views
# MAGIC
# MAGIC ## Purpose and execution
# MAGIC Run from top to bottom after loading `places_ratings.silver.business_snapshots`.
# MAGIC No `%run`, API requests or S3 reads are required. All transformations use Spark SQL.
# MAGIC This notebook creates managed Delta tables in `places_ratings.gold`.
# MAGIC It reads every run currently persisted in consolidated Silver; it does not create a new extraction.
# MAGIC
# MAGIC The fact grain is **one business per extraction**. Six dimensions describe business,
# MAGIC category, extraction, UTC observation date, price level and assigned zone.
# MAGIC A bridge retains retrieval-category memberships separately from the analytical category.
# MAGIC All persistent analytical views read Gold only.
# MAGIC
# MAGIC ## Loading rules
# MAGIC - Deterministic SHA-256 keys identify businesses and extractions; lookup codes are stable keys
# MAGIC   for categories, zones and price levels. Dates use integer YYYYMMDD keys.
# MAGIC - Business descriptions use SCD type 1 (latest description). Historical metrics and contact
# MAGIC   information remain in the fact. Historical names/addresses are not versioned in Gold.
# MAGIC - Fact and bridge loads are insert-only, except the explicit migration from address_zone_v1
# MAGIC   to detected_nucleus_zone_v1. A content guard rejects other changed existing snapshots.
# MAGIC - Each table MERGE is atomic, but the whole notebook is not a multi-table transaction.
# MAGIC   On interruption, rerun from the beginning. No DROP or replacement of data tables is included.
# MAGIC - NULL means not reported; optional values are not imputed to zero or false.
# MAGIC - `classification_v1` maps primary types only. Ambiguous types remain **Por clasificar**.
# MAGIC - Zones are assigned from detected search nuclei. Across different zones, choose the nearest
# MAGIC   detected nucleus (great-circle distance). This is search attribution, not official boundaries.
# MAGIC - Do not modify classification rules for an already loaded run without a deliberate Gold rebuild.
# MAGIC
# MAGIC Current expected baseline: 1,314 facts. Bridge size equals the number of distinct retrieval
# MAGIC memberships in Silver; it can exceed the number of facts. Successful assert_true returns NULL.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Set UTC explicitly and create the managed Gold schema.
# MAGIC SET TIME ZONE 'UTC';
# MAGIC CREATE SCHEMA IF NOT EXISTS places_ratings.gold;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Read persisted consolidated businesses, including all available historical runs.
# MAGIC CREATE OR REPLACE TEMP VIEW gold_source AS
# MAGIC SELECT * FROM places_ratings.silver.business_snapshots;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Validate the source grain and required metadata before creating Gold tables.
# MAGIC SELECT
# MAGIC   assert_true(COUNT(*) > 0, 'Silver source is empty.') AS source_presence_check,
# MAGIC   assert_true(COUNT_IF(run_id IS NULL OR trim(run_id) = '' OR place_id IS NULL
# MAGIC     OR trim(place_id) = '' OR selected_extracted_at_utc IS NULL
# MAGIC     OR snapshot_content_hash IS NULL) = 0, 'Missing source identity or timestamp.') AS identity_check,
# MAGIC   assert_true(COUNT(*) = COUNT(DISTINCT struct(run_id, place_id)),
# MAGIC     'Duplicate business/run in Silver.') AS source_grain_check,
# MAGIC   assert_true(COUNT_IF(rating < 1 OR rating > 5 OR review_count < 0) = 0,
# MAGIC     'Invalid reported rating or review count.') AS metric_range_check,
# MAGIC   assert_true(COUNT_IF(price_level IS NOT NULL AND price_level NOT IN (
# MAGIC     'PRICE_LEVEL_UNSPECIFIED', 'PRICE_LEVEL_FREE', 'PRICE_LEVEL_INEXPENSIVE',
# MAGIC     'PRICE_LEVEL_MODERATE', 'PRICE_LEVEL_EXPENSIVE', 'PRICE_LEVEL_VERY_EXPENSIVE')) = 0,
# MAGIC     'Unexpected price level; review lookup.') AS price_domain_check
# MAGIC FROM gold_source;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Prepare category, zone and price lookups
# MAGIC Category codes are project labels, not API filters in this notebook.
# MAGIC `unclassified` is different from an invalid business. Pet care remains broad as agreed.
# MAGIC Search-category memberships are preserved in the bridge, regardless of the primary classification.
# MAGIC Price is an ordinal label, not a monetary amount. Unspecified and not reported are distinct.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Define stable analytical categories including an explicit unclassified member.
# MAGIC CREATE OR REPLACE TEMP VIEW gold_category_source AS
# MAGIC SELECT * FROM VALUES
# MAGIC   ('cafes', 'Cafeterías', 1),
# MAGIC   ('restaurants', 'Restaurantes', 2),
# MAGIC   ('hair_salons_barbers', 'Peluquerías y barberías', 3),
# MAGIC   ('gyms', 'Gimnasios', 4),
# MAGIC   ('veterinary_and_grooming', 'Veterinarias y cuidado de mascotas', 5),
# MAGIC   ('nightclubs', 'Discotecas', 6),
# MAGIC   ('bars_pubs', 'Bares y pubs', 7),
# MAGIC   ('unclassified', 'Por clasificar', 99)
# MAGIC AS t(category_key, category_name, display_order);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Define the three study zones used to attribute detected businesses.
# MAGIC CREATE OR REPLACE TEMP VIEW gold_zone_source AS
# MAGIC SELECT * FROM VALUES
# MAGIC   ('sopocachi', 'Sopocachi', 'La Paz'),
# MAGIC   ('calacoto', 'Calacoto', 'La Paz'),
# MAGIC   ('ciudad_satelite', 'Ciudad Satélite', 'El Alto')
# MAGIC AS t(zone_key, zone_name, city_name);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Define readable ordinal price labels while preserving unknown and unspecified values.
# MAGIC CREATE OR REPLACE TEMP VIEW gold_price_source AS
# MAGIC SELECT * FROM VALUES
# MAGIC   ('not_reported', CAST(NULL AS STRING), 'No reportado', CAST(NULL AS INT)),
# MAGIC   ('PRICE_LEVEL_UNSPECIFIED', 'PRICE_LEVEL_UNSPECIFIED', 'Sin especificar', CAST(NULL AS INT)),
# MAGIC   ('PRICE_LEVEL_FREE', 'PRICE_LEVEL_FREE', 'Gratuito', 0),
# MAGIC   ('PRICE_LEVEL_INEXPENSIVE', 'PRICE_LEVEL_INEXPENSIVE', 'Económico', 1),
# MAGIC   ('PRICE_LEVEL_MODERATE', 'PRICE_LEVEL_MODERATE', 'Moderado', 2),
# MAGIC   ('PRICE_LEVEL_EXPENSIVE', 'PRICE_LEVEL_EXPENSIVE', 'Costoso', 3),
# MAGIC   ('PRICE_LEVEL_VERY_EXPENSIVE', 'PRICE_LEVEL_VERY_EXPENSIVE', 'Muy costoso', 4)
# MAGIC AS t(price_level_key, source_price_level, price_level_name, price_order);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Map detected nuclei to study zones and their configured center coordinates.
# MAGIC CREATE OR REPLACE TEMP VIEW gold_nucleus_source AS
# MAGIC SELECT * FROM VALUES
# MAGIC   ('sopocachi', 'sopocachi', -16.5108331, -68.1265597),
# MAGIC   ('calacoto_el_bosque', 'calacoto', -16.540707581172857, -68.08375791504788),
# MAGIC   ('calacoto_san_miguel', 'calacoto', -16.539154202002806, -68.07739742119121),
# MAGIC   ('calacoto_montenegro', 'calacoto', -16.542593699449483, -68.0780778610696),
# MAGIC   ('ciudad_satelite_tinku', 'ciudad_satelite', -16.5233706978321, -68.1507870101477)
# MAGIC AS t(nucleus_code, zone_key, center_latitude, center_longitude);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Require mapped detection nuclei and coordinates instead of assigning an unknown zone.
# MAGIC SELECT
# MAGIC   assert_true((SELECT COUNT(*) FROM gold_source WHERE latitude IS NULL OR longitude IS NULL
# MAGIC     OR search_nuclei IS NULL OR size(search_nuclei) = 0) = 0,
# MAGIC     'Missing coordinates or detection nuclei; review source before zone assignment.') AS zone_source_check,
# MAGIC   assert_true((SELECT COUNT(*) FROM (
# MAGIC     SELECT DISTINCT detected.nucleus_code FROM gold_source AS s,
# MAGIC     LATERAL explode(s.search_nuclei) AS detected(nucleus_code)
# MAGIC   ) AS detected LEFT ANTI JOIN gold_nucleus_source AS n
# MAGIC     ON detected.nucleus_code = n.nucleus_code) = 0,
# MAGIC     'Unmapped search nucleus; extend the nucleus lookup.') AS nucleus_mapping_check;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Assign each business to its nearest detected nucleus, with a stable tie-breaker.
# MAGIC CREATE OR REPLACE TEMP VIEW gold_zone_assignment AS
# MAGIC SELECT run_id, place_id, zone_key, nucleus_code AS assigned_nucleus_code
# MAGIC FROM (
# MAGIC   SELECT s.run_id, s.place_id, n.zone_key, n.nucleus_code,
# MAGIC     ROW_NUMBER() OVER (PARTITION BY s.run_id, s.place_id ORDER BY
# MAGIC       POWER(SIN(RADIANS(s.latitude - n.center_latitude) / 2), 2)
# MAGIC       + COS(RADIANS(s.latitude)) * COS(RADIANS(n.center_latitude))
# MAGIC       * POWER(SIN(RADIANS(s.longitude - n.center_longitude) / 2), 2),
# MAGIC       n.nucleus_code) AS rn
# MAGIC   FROM gold_source AS s,
# MAGIC   LATERAL explode(s.search_nuclei) AS detected(nucleus_code)
# MAGIC   JOIN gold_nucleus_source AS n ON detected.nucleus_code = n.nucleus_code
# MAGIC ) WHERE rn = 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Classify the primary business activity and attach the zone assigned by detection origin.
# MAGIC CREATE OR REPLACE TEMP VIEW gold_classified AS
# MAGIC SELECT s.*, z.zone_key, z.assigned_nucleus_code,
# MAGIC   sha2(s.place_id, 256) AS business_key,
# MAGIC   sha2(s.run_id, 256) AS extraction_key,
# MAGIC   CAST(date_format(selected_extracted_at_utc, 'yyyyMMdd') AS INT) AS date_key,
# MAGIC   COALESCE(price_level, 'not_reported') AS price_level_key,
# MAGIC   CASE
# MAGIC     WHEN primary_type IN ('coffee_shop', 'cafe', 'coffee_stand', 'cafeteria') THEN 'cafes'
# MAGIC     WHEN primary_type IN ('hair_salon', 'barber_shop', 'hair_care') THEN 'hair_salons_barbers'
# MAGIC     WHEN primary_type IN ('gym', 'fitness_center', 'sports_club', 'sports_complex') THEN 'gyms'
# MAGIC     WHEN primary_type IN ('veterinary_care', 'pet_care') THEN 'veterinary_and_grooming'
# MAGIC     WHEN primary_type = 'night_club' THEN 'nightclubs'
# MAGIC     WHEN primary_type IN ('bar', 'pub', 'cocktail_bar', 'wine_bar', 'sports_bar',
# MAGIC       'lounge_bar', 'bar_and_grill', 'beer_garden', 'brewpub', 'gastropub', 'irish_pub') THEN 'bars_pubs'
# MAGIC     WHEN primary_type = 'restaurant' OR primary_type RLIKE '_restaurant$'
# MAGIC       OR primary_type IN ('diner', 'bistro', 'food_court', 'meal_takeaway', 'meal_delivery')
# MAGIC       THEN 'restaurants'
# MAGIC     ELSE 'unclassified'
# MAGIC   END AS primary_category_key,
# MAGIC   'classification_v1' AS classification_rule_version,
# MAGIC   'detected_nucleus_zone_v1' AS zone_rule_version
# MAGIC FROM gold_source AS s
# MAGIC JOIN gold_zone_assignment AS z ON s.run_id = z.run_id AND s.place_id = z.place_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Prepare the six dimensions
# MAGIC Dimensions are loaded before facts. `dim_business` keeps the latest whole description across
# MAGIC available runs. Ties are resolved deterministically. An older source cannot overwrite a newer
# MAGIC business description. Extraction bounds describe businesses available in Silver, not API job logs.
# MAGIC The date dimension covers 2026 through 2031 and is insert-only (SCD0).

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Select the latest descriptive business attributes for the SCD type 1 dimension.
# MAGIC CREATE OR REPLACE TEMP VIEW gold_business_source AS
# MAGIC SELECT business_key, place_id, business_name, name_language, primary_type,
# MAGIC   formatted_address, latitude, longitude,
# MAGIC   selected_extracted_at_utc AS attributes_observed_at_utc,
# MAGIC   run_id AS attributes_run_id
# MAGIC FROM (
# MAGIC   SELECT *, ROW_NUMBER() OVER (PARTITION BY place_id
# MAGIC     ORDER BY selected_extracted_at_utc DESC, run_id DESC, selected_observation_id DESC) AS rn
# MAGIC   FROM gold_classified
# MAGIC ) WHERE rn = 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Describe each extraction and preserve version sets rather than selecting arbitrary versions.
# MAGIC CREATE OR REPLACE TEMP VIEW gold_extraction_source AS
# MAGIC SELECT extraction_key, run_id,
# MAGIC   MIN(first_observed_at_utc) AS started_at_utc,
# MAGIC   MAX(last_observed_at_utc) AS finished_at_utc,
# MAGIC   sort_array(collect_set(mapping_version)) AS mapping_versions,
# MAGIC   sort_array(collect_set(field_set_version)) AS field_set_versions,
# MAGIC   COUNT(*) AS source_business_count,
# MAGIC   SUM(source_observation_count) AS source_observation_count
# MAGIC FROM gold_classified GROUP BY extraction_key, run_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Build a complete calendar for 2026 and the following five years.
# MAGIC CREATE OR REPLACE TEMP VIEW gold_date_source AS
# MAGIC SELECT CAST(date_format(calendar_date, 'yyyyMMdd') AS INT) AS date_key,
# MAGIC   calendar_date, year(calendar_date) AS calendar_year,
# MAGIC   quarter(calendar_date) AS calendar_quarter, month(calendar_date) AS calendar_month,
# MAGIC   dayofmonth(calendar_date) AS day_of_month,
# MAGIC   weekday(calendar_date) + 1 AS iso_day_of_week,
# MAGIC   weekday(calendar_date) IN (5, 6) AS is_weekend
# MAGIC FROM explode(sequence(DATE '2026-01-01', DATE '2031-12-31', INTERVAL 1 DAY)) AS dates(calendar_date);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create dim_category without replacing existing dimension data.
# MAGIC CREATE TABLE IF NOT EXISTS places_ratings.gold.dim_category
# MAGIC USING DELTA AS SELECT * FROM gold_category_source WHERE 1 = 0;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Reject duplicate dimension keys before loading dim_category.
# MAGIC SELECT assert_true(COUNT(*) = COUNT(DISTINCT category_key), 'Duplicate key in dim_category.') AS uniqueness_check
# MAGIC FROM places_ratings.gold.dim_category;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Upsert dim_category using its deterministic key.
# MAGIC MERGE INTO places_ratings.gold.dim_category AS target
# MAGIC USING gold_category_source AS source ON target.category_key = source.category_key
# MAGIC WHEN MATCHED AND NOT ((target.category_name <=> source.category_name)
# MAGIC  AND (target.display_order <=> source.display_order)) THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create dim_zone without replacing existing dimension data.
# MAGIC CREATE TABLE IF NOT EXISTS places_ratings.gold.dim_zone
# MAGIC USING DELTA AS SELECT * FROM gold_zone_source WHERE 1 = 0;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Reject duplicate dimension keys before loading dim_zone.
# MAGIC SELECT assert_true(COUNT(*) = COUNT(DISTINCT zone_key), 'Duplicate key in dim_zone.') AS uniqueness_check
# MAGIC FROM places_ratings.gold.dim_zone;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Upsert dim_zone using its deterministic key.
# MAGIC MERGE INTO places_ratings.gold.dim_zone AS target
# MAGIC USING gold_zone_source AS source ON target.zone_key = source.zone_key
# MAGIC WHEN MATCHED AND NOT ((target.zone_name <=> source.zone_name)
# MAGIC  AND (target.city_name <=> source.city_name)) THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create dim_price_level without replacing existing dimension data.
# MAGIC CREATE TABLE IF NOT EXISTS places_ratings.gold.dim_price_level
# MAGIC USING DELTA AS SELECT * FROM gold_price_source WHERE 1 = 0;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Reject duplicate dimension keys before loading dim_price_level.
# MAGIC SELECT assert_true(COUNT(*) = COUNT(DISTINCT price_level_key), 'Duplicate key in dim_price_level.') AS uniqueness_check
# MAGIC FROM places_ratings.gold.dim_price_level;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Upsert dim_price_level using its deterministic key.
# MAGIC MERGE INTO places_ratings.gold.dim_price_level AS target
# MAGIC USING gold_price_source AS source ON target.price_level_key = source.price_level_key
# MAGIC WHEN MATCHED AND NOT ((target.source_price_level <=> source.source_price_level)
# MAGIC  AND (target.price_level_name <=> source.price_level_name)
# MAGIC  AND (target.price_order <=> source.price_order)) THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create dim_date without replacing existing dimension data.
# MAGIC CREATE TABLE IF NOT EXISTS places_ratings.gold.dim_date
# MAGIC USING DELTA AS SELECT * FROM gold_date_source WHERE 1 = 0;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Reject duplicate dimension keys before loading dim_date.
# MAGIC SELECT assert_true(COUNT(*) = COUNT(DISTINCT date_key), 'Duplicate key in dim_date.') AS uniqueness_check
# MAGIC FROM places_ratings.gold.dim_date;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Upsert dim_date using its deterministic key.
# MAGIC MERGE INTO places_ratings.gold.dim_date AS target
# MAGIC USING gold_date_source AS source ON target.date_key = source.date_key
# MAGIC
# MAGIC WHEN NOT MATCHED THEN INSERT *;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create dim_extraction without replacing existing dimension data.
# MAGIC CREATE TABLE IF NOT EXISTS places_ratings.gold.dim_extraction
# MAGIC USING DELTA AS SELECT * FROM gold_extraction_source WHERE 1 = 0;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Reject duplicate dimension keys before loading dim_extraction.
# MAGIC SELECT assert_true(COUNT(*) = COUNT(DISTINCT extraction_key), 'Duplicate key in dim_extraction.') AS uniqueness_check
# MAGIC FROM places_ratings.gold.dim_extraction;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Upsert dim_extraction using its deterministic key.
# MAGIC MERGE INTO places_ratings.gold.dim_extraction AS target
# MAGIC USING gold_extraction_source AS source ON target.extraction_key = source.extraction_key
# MAGIC WHEN MATCHED AND NOT ((target.run_id <=> source.run_id)
# MAGIC  AND (target.started_at_utc <=> source.started_at_utc)
# MAGIC  AND (target.finished_at_utc <=> source.finished_at_utc)
# MAGIC  AND (target.mapping_versions <=> source.mapping_versions)
# MAGIC  AND (target.field_set_versions <=> source.field_set_versions)
# MAGIC  AND (target.source_business_count <=> source.source_business_count)
# MAGIC  AND (target.source_observation_count <=> source.source_observation_count)) THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create dim_business without replacing existing dimension data.
# MAGIC CREATE TABLE IF NOT EXISTS places_ratings.gold.dim_business
# MAGIC USING DELTA AS SELECT * FROM gold_business_source WHERE 1 = 0;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Reject duplicate dimension keys before loading dim_business.
# MAGIC SELECT assert_true(COUNT(*) = COUNT(DISTINCT business_key), 'Duplicate key in dim_business.') AS uniqueness_check
# MAGIC FROM places_ratings.gold.dim_business;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Upsert dim_business using its deterministic key.
# MAGIC MERGE INTO places_ratings.gold.dim_business AS target
# MAGIC USING gold_business_source AS source ON target.business_key = source.business_key
# MAGIC WHEN MATCHED AND (source.attributes_observed_at_utc > target.attributes_observed_at_utc
# MAGIC  OR (source.attributes_observed_at_utc = target.attributes_observed_at_utc
# MAGIC  AND source.attributes_run_id >= target.attributes_run_id))
# MAGIC  AND NOT ((target.place_id <=> source.place_id)
# MAGIC  AND (target.business_name <=> source.business_name)
# MAGIC  AND (target.name_language <=> source.name_language)
# MAGIC  AND (target.primary_type <=> source.primary_type)
# MAGIC  AND (target.formatted_address <=> source.formatted_address)
# MAGIC  AND (target.latitude <=> source.latitude)
# MAGIC  AND (target.longitude <=> source.longitude)
# MAGIC  AND (target.attributes_observed_at_utc <=> source.attributes_observed_at_utc)
# MAGIC  AND (target.attributes_run_id <=> source.attributes_run_id)) THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Prepare facts and retrieval-category memberships
# MAGIC Keep one fact per business/extraction. Price, status, contact information and availability
# MAGIC describe that capture. Availability flags mean **reported by the source**, not verified absence.
# MAGIC Historical rating and review count are not additive across extractions; ratings are not additive
# MAGIC across businesses either. Medians and distributions are computed in views, not stored as facts.
# MAGIC
# MAGIC The bridge has one row per business/extraction/requested category. It describes detection,
# MAGIC not a verified list of all business activities. Never sum category-membership counts as a global total.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Prepare snapshot measures, descriptive context and source lineage without multiplying rows.
# MAGIC CREATE OR REPLACE TEMP VIEW gold_fact_base AS
# MAGIC SELECT business_key, extraction_key, date_key, primary_category_key, price_level_key, zone_key,
# MAGIC   rating, review_count, business_status, website_uri, national_phone_number,
# MAGIC   regular_opening_hours, weekday_descriptions, google_maps_uri, google_maps_links,
# MAGIC   write_review_uri, reviews_uri, source_attributions, containing_places, pure_service_area_business,
# MAGIC   COALESCE(length(trim(website_uri)) > 0, false) AS has_website,
# MAGIC   COALESCE(length(trim(national_phone_number)) > 0, false) AS has_phone,
# MAGIC   regular_opening_hours IS NOT NULL AS has_opening_hours,
# MAGIC   rating IS NOT NULL AS has_rating, review_count IS NOT NULL AS has_review_count,
# MAGIC   price_level IS NOT NULL AND price_level <> 'PRICE_LEVEL_UNSPECIFIED' AS has_price_level,
# MAGIC   COALESCE(length(trim(write_review_uri)) > 0, false) AS has_write_review_link,
# MAGIC   source_types, source_observation_count,
# MAGIC   first_observed_at_utc, last_observed_at_utc,
# MAGIC   selected_observation_id, selected_extracted_at_utc, selected_source_file_path,
# MAGIC   snapshot_content_hash AS silver_snapshot_hash,
# MAGIC   classification_rule_version, zone_rule_version
# MAGIC FROM gold_classified;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Fingerprint the complete fact content separately from its Gold loading timestamp.
# MAGIC CREATE OR REPLACE TEMP VIEW gold_fact_source AS
# MAGIC SELECT *, sha2(to_json(struct(base.*)), 256) AS fact_content_hash,
# MAGIC   current_timestamp() AS gold_ingested_at
# MAGIC FROM gold_fact_base AS base;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Expand distinct retrieval memberships into the auxiliary Gold bridge.
# MAGIC CREATE OR REPLACE TEMP VIEW gold_bridge_source AS
# MAGIC SELECT DISTINCT source.business_key, source.extraction_key, membership.category_key
# MAGIC FROM gold_classified AS source,
# MAGIC LATERAL explode(source.requested_categories) AS membership(category_key);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Reject unmapped bridge categories and invalid or duplicate prepared fact identities.
# MAGIC SELECT
# MAGIC   assert_true((SELECT COUNT(*) FROM gold_bridge_source AS b
# MAGIC     LEFT ANTI JOIN places_ratings.gold.dim_category AS c ON b.category_key = c.category_key) = 0,
# MAGIC     'Unmapped retrieval category.') AS bridge_category_check,
# MAGIC   assert_true((SELECT COUNT(*) FROM gold_fact_source) =
# MAGIC     (SELECT COUNT(DISTINCT struct(business_key, extraction_key)) FROM gold_fact_source),
# MAGIC     'Duplicate prepared fact identity.') AS fact_grain_check,
# MAGIC   assert_true((SELECT COUNT(*) FROM gold_bridge_source WHERE category_key IS NULL) = 0,
# MAGIC     'Null category in bridge.') AS bridge_identity_check;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create fact_business_snapshot as a managed Delta table without replacing existing rows.
# MAGIC CREATE TABLE IF NOT EXISTS places_ratings.gold.fact_business_snapshot
# MAGIC USING DELTA AS SELECT * FROM gold_fact_source WHERE 1 = 0;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Require unique existing keys before loading fact_business_snapshot.
# MAGIC SELECT assert_true(COUNT(*) = COUNT(DISTINCT struct(business_key, extraction_key)),
# MAGIC  'Duplicate target identity in fact_business_snapshot.') AS target_grain_check
# MAGIC FROM places_ratings.gold.fact_business_snapshot;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Apply the explicitly requested zone-rule correction to previously loaded v1 facts only.
# MAGIC MERGE INTO places_ratings.gold.fact_business_snapshot AS target
# MAGIC USING gold_fact_source AS source
# MAGIC ON target.business_key = source.business_key AND target.extraction_key = source.extraction_key
# MAGIC WHEN MATCHED
# MAGIC   AND target.zone_rule_version = 'address_zone_v1'
# MAGIC   AND source.zone_rule_version = 'detected_nucleus_zone_v1'
# MAGIC   AND target.silver_snapshot_hash = source.silver_snapshot_hash
# MAGIC   AND target.classification_rule_version = source.classification_rule_version
# MAGIC THEN UPDATE SET
# MAGIC   target.zone_key = source.zone_key,
# MAGIC   target.zone_rule_version = source.zone_rule_version,
# MAGIC   target.fact_content_hash = source.fact_content_hash;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Stop if a previously loaded snapshot has changed instead of silently rewriting history.
# MAGIC SELECT assert_true(COUNT(*) = 0,
# MAGIC   'Existing Gold fact differs. Review source/rule changes before a deliberate rebuild.') AS immutable_fact_check
# MAGIC FROM gold_fact_source AS source
# MAGIC JOIN places_ratings.gold.fact_business_snapshot AS target
# MAGIC   ON target.business_key = source.business_key AND target.extraction_key = source.extraction_key
# MAGIC WHERE NOT (target.fact_content_hash <=> source.fact_content_hash);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Insert new fact_business_snapshot identities while keeping reruns free of duplicates.
# MAGIC MERGE INTO places_ratings.gold.fact_business_snapshot AS target
# MAGIC USING gold_fact_source AS source ON target.business_key = source.business_key AND target.extraction_key = source.extraction_key
# MAGIC WHEN NOT MATCHED THEN INSERT *;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create bridge_business_category as a managed Delta table without replacing existing rows.
# MAGIC CREATE TABLE IF NOT EXISTS places_ratings.gold.bridge_business_category
# MAGIC USING DELTA AS SELECT * FROM gold_bridge_source WHERE 1 = 0;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Require unique existing keys before loading bridge_business_category.
# MAGIC SELECT assert_true(COUNT(*) = COUNT(DISTINCT struct(business_key, extraction_key, category_key)),
# MAGIC  'Duplicate target identity in bridge_business_category.') AS target_grain_check
# MAGIC FROM places_ratings.gold.bridge_business_category;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Reject unexpected old memberships for the business snapshots included in this load.
# MAGIC SELECT assert_true(COUNT(*) = 0, 'Existing bridge memberships differ from Silver.') AS bridge_history_check
# MAGIC FROM (
# MAGIC   SELECT b.* FROM places_ratings.gold.bridge_business_category AS b
# MAGIC   JOIN gold_fact_source AS f
# MAGIC     ON b.business_key = f.business_key AND b.extraction_key = f.extraction_key
# MAGIC   EXCEPT
# MAGIC   SELECT * FROM gold_bridge_source
# MAGIC ) AS unexpected;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Insert new bridge_business_category identities while keeping reruns free of duplicates.
# MAGIC MERGE INTO places_ratings.gold.bridge_business_category AS target
# MAGIC USING gold_bridge_source AS source ON target.business_key = source.business_key AND target.extraction_key = source.extraction_key AND target.category_key = source.category_key
# MAGIC WHEN NOT MATCHED THEN INSERT *;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Reconcile loaded data and dimension references
# MAGIC Checks validate the actual tables: key uniqueness, source reconciliation, bridge membership
# MAGIC equality and all dimension references. They do not establish representativeness of the sample.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Reconcile loaded fact content and the complete source bridge membership set.
# MAGIC SELECT
# MAGIC   assert_true((SELECT COUNT(*) FROM gold_fact_source AS s
# MAGIC     LEFT JOIN places_ratings.gold.fact_business_snapshot AS f
# MAGIC       ON s.business_key = f.business_key AND s.extraction_key = f.extraction_key
# MAGIC     WHERE f.business_key IS NULL OR NOT (s.fact_content_hash <=> f.fact_content_hash)) = 0,
# MAGIC     'Missing or changed loaded fact.') AS fact_reconciliation_check,
# MAGIC   assert_true((SELECT COUNT(*) FROM (
# MAGIC     SELECT * FROM gold_bridge_source EXCEPT
# MAGIC     SELECT * FROM places_ratings.gold.bridge_business_category
# MAGIC   ) AS missing) = 0, 'Bridge membership missing from Gold.') AS bridge_reconciliation_check;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Require every fact foreign key to reference an existing dimension member.
# MAGIC SELECT assert_true(SUM(missing_references) = 0, 'Orphan fact dimension reference.') AS foreign_key_check
# MAGIC FROM (
# MAGIC SELECT 'dim_business' AS dimension_name, COUNT(*) AS missing_references
# MAGIC FROM places_ratings.gold.fact_business_snapshot AS f
# MAGIC LEFT ANTI JOIN places_ratings.gold.dim_business AS d ON f.business_key = d.business_key
# MAGIC UNION ALL
# MAGIC SELECT 'dim_extraction' AS dimension_name, COUNT(*) AS missing_references
# MAGIC FROM places_ratings.gold.fact_business_snapshot AS f
# MAGIC LEFT ANTI JOIN places_ratings.gold.dim_extraction AS d ON f.extraction_key = d.extraction_key
# MAGIC UNION ALL
# MAGIC SELECT 'dim_date' AS dimension_name, COUNT(*) AS missing_references
# MAGIC FROM places_ratings.gold.fact_business_snapshot AS f
# MAGIC LEFT ANTI JOIN places_ratings.gold.dim_date AS d ON f.date_key = d.date_key
# MAGIC UNION ALL
# MAGIC SELECT 'dim_category' AS dimension_name, COUNT(*) AS missing_references
# MAGIC FROM places_ratings.gold.fact_business_snapshot AS f
# MAGIC LEFT ANTI JOIN places_ratings.gold.dim_category AS d ON f.primary_category_key = d.category_key
# MAGIC UNION ALL
# MAGIC SELECT 'dim_price_level' AS dimension_name, COUNT(*) AS missing_references
# MAGIC FROM places_ratings.gold.fact_business_snapshot AS f
# MAGIC LEFT ANTI JOIN places_ratings.gold.dim_price_level AS d ON f.price_level_key = d.price_level_key
# MAGIC UNION ALL
# MAGIC SELECT 'dim_zone' AS dimension_name, COUNT(*) AS missing_references
# MAGIC FROM places_ratings.gold.fact_business_snapshot AS f
# MAGIC LEFT ANTI JOIN places_ratings.gold.dim_zone AS d ON f.zone_key = d.zone_key
# MAGIC ) AS checks;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Require every bridge row to reference a loaded fact and a valid category.
# MAGIC SELECT
# MAGIC   assert_true((SELECT COUNT(*) FROM places_ratings.gold.bridge_business_category AS b
# MAGIC     LEFT ANTI JOIN places_ratings.gold.fact_business_snapshot AS f
# MAGIC       ON b.business_key = f.business_key AND b.extraction_key = f.extraction_key) = 0,
# MAGIC     'Bridge without a business snapshot.') AS bridge_fact_check,
# MAGIC   assert_true((SELECT COUNT(*) FROM places_ratings.gold.bridge_business_category AS b
# MAGIC     LEFT ANTI JOIN places_ratings.gold.dim_category AS c ON b.category_key = c.category_key) = 0,
# MAGIC     'Bridge without a category.') AS bridge_dimension_check;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Publish Gold-only analytical views
# MAGIC `v_business_explorer` has one row per fact and supports rankings, filters and scatter plots.
# MAGIC It uses the latest business description (SCD1), with historical capture metrics/context.
# MAGIC `v_business_category_memberships` expands retrieval categories and can repeat businesses.
# MAGIC Summary views always group by extraction; no implicit mixing of different snapshots.
# MAGIC Missing review counts are excluded from review medians, not replaced with zero.
# MAGIC Price comparisons stay within the primary category and retain missing-price groups.
# MAGIC
# MAGIC ### Hypothesis: presence, volume and observed prospect pool
# MAGIC No single statistic determines the hypothesis. Compare these complementary indicators:
# MAGIC - Presence: businesses with a positive reported review count / all sampled businesses in the category.
# MAGIC   This is observed positive coverage; missing values do not establish the absence of reviews.
# MAGIC - Data availability: reported counts / all businesses. Reported zero, positive and missing counts
# MAGIC   are kept separate. A second positive percentage uses only businesses with a reported count;
# MAGIC   read its denominator carefully, especially when no explicit zero was returned.
# MAGIC - Volume: median and mean counts among businesses with a reported count, including explicit zero.
# MAGIC - Observed pool: number of businesses with positive counts, their share of all positive businesses
# MAGIC   in the run, and counts reaching illustrative thresholds of 10 and 50 reviews.
# MAGIC   These thresholds are exploratory filters, not validated NFC purchase criteria.
# MAGIC
# MAGIC Category-positive coverage and share of the positive pool answer different questions:
# MAGIC the first compares rates within each category; the second describes the composition of this sample.
# MAGIC They do not measure population market share. Discovery limits and non-random sampling still apply.
# MAGIC Display `Por clasificar` separately. Do not sum across different runs.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Publish a convenient one-row-per-business/extraction view entirely from Gold tables.
# MAGIC CREATE OR REPLACE VIEW places_ratings.gold.v_business_explorer AS
# MAGIC SELECT f.*, b.place_id, b.business_name, b.primary_type, b.formatted_address,
# MAGIC   b.latitude, b.longitude, e.run_id, e.started_at_utc, e.finished_at_utc,
# MAGIC   d.calendar_date AS observation_date,
# MAGIC   c.category_name AS primary_category_name,
# MAGIC   z.zone_name, z.city_name AS assigned_zone_city,
# MAGIC   p.price_level_name, p.price_order
# MAGIC FROM places_ratings.gold.fact_business_snapshot AS f
# MAGIC JOIN places_ratings.gold.dim_business AS b ON f.business_key = b.business_key
# MAGIC JOIN places_ratings.gold.dim_extraction AS e ON f.extraction_key = e.extraction_key
# MAGIC JOIN places_ratings.gold.dim_date AS d ON f.date_key = d.date_key
# MAGIC JOIN places_ratings.gold.dim_category AS c ON f.primary_category_key = c.category_key
# MAGIC JOIN places_ratings.gold.dim_zone AS z ON f.zone_key = z.zone_key
# MAGIC JOIN places_ratings.gold.dim_price_level AS p ON f.price_level_key = p.price_level_key;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Publish detected-category memberships without querying Silver or reclassifying businesses.
# MAGIC CREATE OR REPLACE VIEW places_ratings.gold.v_business_category_memberships AS
# MAGIC SELECT e.*, b.category_key AS detected_category_key, c.category_name AS detected_category_name
# MAGIC FROM places_ratings.gold.v_business_explorer AS e
# MAGIC JOIN places_ratings.gold.bridge_business_category AS b
# MAGIC   ON e.business_key = b.business_key AND e.extraction_key = b.extraction_key
# MAGIC JOIN places_ratings.gold.dim_category AS c ON b.category_key = c.category_key;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Compare review presence, volume and observed pool size without treating missing counts as zero.
# MAGIC CREATE OR REPLACE VIEW places_ratings.gold.v_category_metrics AS
# MAGIC WITH category_metrics AS (
# MAGIC SELECT run_id, primary_category_key, primary_category_name,
# MAGIC   COUNT(*) AS businesses,
# MAGIC   COUNT(rating) AS businesses_with_rating,
# MAGIC   COUNT(review_count) AS businesses_with_review_count,
# MAGIC   COUNT(*) - COUNT(review_count) AS businesses_missing_review_count,
# MAGIC   COUNT_IF(review_count > 0) AS businesses_with_reviews,
# MAGIC   COUNT_IF(review_count = 0) AS businesses_with_zero_reviews,
# MAGIC   ROUND(100.0 * COUNT(review_count) / COUNT(*), 2) AS pct_reporting_review_count,
# MAGIC   ROUND(100.0 * COUNT_IF(review_count > 0) / COUNT(*), 2) AS pct_with_reviews,
# MAGIC   ROUND(100.0 * COUNT_IF(review_count = 0) / COUNT(*), 2) AS pct_with_zero_reviews,
# MAGIC   ROUND(100.0 * (COUNT(*) - COUNT(review_count)) / COUNT(*), 2) AS pct_missing_review_count,
# MAGIC   ROUND(100.0 * COUNT_IF(review_count > 0) / NULLIF(COUNT(review_count), 0), 2)
# MAGIC     AS pct_with_reviews_among_reported,
# MAGIC   COUNT_IF(review_count >= 10) AS businesses_with_at_least_10_reviews,
# MAGIC   COUNT_IF(review_count >= 50) AS businesses_with_at_least_50_reviews,
# MAGIC   percentile_cont(0.5) WITHIN GROUP (ORDER BY review_count) AS median_review_count,
# MAGIC   ROUND(AVG(review_count), 2) AS mean_review_count,
# MAGIC   ROUND(AVG(rating), 2) AS mean_business_rating,
# MAGIC   percentile_cont(0.5) WITHIN GROUP (ORDER BY rating) AS median_business_rating,
# MAGIC   COUNT_IF(has_website) AS businesses_reporting_website,
# MAGIC   ROUND(100.0 * COUNT_IF(has_website) / COUNT(*), 2) AS pct_reporting_website,
# MAGIC   ROUND(100.0 * COUNT_IF(has_phone) / COUNT(*), 2) AS pct_reporting_phone,
# MAGIC   ROUND(100.0 * COUNT_IF(has_opening_hours) / COUNT(*), 2) AS pct_reporting_hours
# MAGIC FROM places_ratings.gold.v_business_explorer
# MAGIC GROUP BY run_id, primary_category_key, primary_category_name
# MAGIC )
# MAGIC SELECT *,
# MAGIC   ROUND(100.0 * businesses_with_reviews /
# MAGIC     NULLIF(SUM(businesses_with_reviews) OVER (PARTITION BY run_id), 0), 2)
# MAGIC     AS pct_of_run_businesses_with_reviews
# MAGIC FROM category_metrics;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Compare ordinal price groups inside each primary category, retaining coverage denominators.
# MAGIC CREATE OR REPLACE VIEW places_ratings.gold.v_price_reputation AS
# MAGIC SELECT run_id, primary_category_key, primary_category_name,
# MAGIC   price_level_key, price_level_name, price_order,
# MAGIC   COUNT(*) AS businesses, COUNT(rating) AS businesses_with_rating,
# MAGIC   COUNT(review_count) AS businesses_with_review_count,
# MAGIC   ROUND(AVG(rating), 2) AS mean_business_rating,
# MAGIC   percentile_cont(0.5) WITHIN GROUP (ORDER BY rating) AS median_business_rating,
# MAGIC   percentile_cont(0.5) WITHIN GROUP (ORDER BY review_count) AS median_review_count
# MAGIC FROM places_ratings.gold.v_business_explorer
# MAGIC GROUP BY run_id, primary_category_key, primary_category_name,
# MAGIC   price_level_key, price_level_name, price_order;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Inspect the result
# MAGIC Current baseline should report 1,314 facts and 3,272 source appearances accounted for.
# MAGIC Main-category counts are mutually exclusive and can sum to the extraction total.
# MAGIC They may differ from EDA retrieval-category counts; those remain available through the bridge.
# MAGIC All businesses receive a detection-attributed study zone; unknown nuclei stop the load for review.
# MAGIC Highest-rating examples use a **configurable illustrative threshold of 30 reviews**;
# MAGIC this is not a validated quality criterion or evidence of NFC demand.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify persisted business counts and the source appearances represented by each extraction.
# MAGIC SELECT e.run_id, COUNT(*) AS business_rows,
# MAGIC   COUNT(DISTINCT f.business_key) AS distinct_businesses,
# MAGIC   SUM(f.source_observation_count) AS source_observations_accounted_for
# MAGIC FROM places_ratings.gold.fact_business_snapshot AS f
# MAGIC JOIN places_ratings.gold.dim_extraction AS e ON f.extraction_key = e.extraction_key
# MAGIC GROUP BY e.run_id ORDER BY e.run_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Inspect classification and zone coverage without excluding unresolved businesses.
# MAGIC SELECT run_id, primary_category_name, zone_name, COUNT(*) AS businesses
# MAGIC FROM places_ratings.gold.v_business_explorer
# MAGIC GROUP BY run_id, primary_category_name, zone_name
# MAGIC ORDER BY run_id, primary_category_name, businesses DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Compare positive-review coverage, volume and observed pool size for the hypothesis.
# MAGIC SELECT primary_category_name, businesses,
# MAGIC   businesses_with_reviews, businesses_with_zero_reviews,
# MAGIC   businesses_missing_review_count, pct_with_reviews,
# MAGIC   pct_missing_review_count, pct_with_reviews_among_reported,
# MAGIC   median_review_count, mean_review_count,
# MAGIC   pct_of_run_businesses_with_reviews,
# MAGIC   businesses_with_at_least_10_reviews, businesses_with_at_least_50_reviews
# MAGIC FROM places_ratings.gold.v_category_metrics
# MAGIC WHERE run_id = 'baseline_001'
# MAGIC ORDER BY pct_with_reviews DESC, businesses_with_reviews DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Inspect highly rated businesses using an illustrative minimum of 30 reported reviews.
# MAGIC SELECT business_name, primary_category_name, rating, review_count,
# MAGIC   price_level_name, zone_name, google_maps_uri
# MAGIC FROM places_ratings.gold.v_business_explorer
# MAGIC WHERE run_id = 'baseline_001' AND rating IS NOT NULL AND review_count >= 30
# MAGIC ORDER BY rating DESC, review_count DESC, place_id LIMIT 20;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Find businesses detected in both cafe and restaurant searches using Gold only.
# MAGIC SELECT place_id, business_name, primary_category_name, MAX(review_count) AS review_count
# MAGIC FROM places_ratings.gold.v_business_category_memberships
# MAGIC WHERE run_id = 'baseline_001' AND detected_category_key IN ('cafes', 'restaurants')
# MAGIC GROUP BY business_key, extraction_key, place_id, business_name, primary_category_name
# MAGIC HAVING COUNT(DISTINCT detected_category_key) = 2
# MAGIC ORDER BY review_count DESC NULLS LAST;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Compare review coverage and volume by assigned zone and primary category.
# MAGIC SELECT
# MAGIC     zone_name,
# MAGIC     primary_category_name,
# MAGIC     COUNT(*) AS businesses,
# MAGIC     COUNT_IF(review_count > 0) AS businesses_with_reviews,
# MAGIC     COUNT_IF(review_count IS NULL) AS businesses_missing_review_count,
# MAGIC     ROUND(
# MAGIC         100.0 * COUNT_IF(review_count > 0) / COUNT(*),
# MAGIC         2
# MAGIC     ) AS pct_with_reviews,
# MAGIC     percentile_cont(0.5)
# MAGIC         WITHIN GROUP (ORDER BY review_count) AS median_review_count,
# MAGIC     COUNT_IF(review_count >= 50) AS businesses_with_at_least_50_reviews
# MAGIC FROM places_ratings.gold.v_business_explorer
# MAGIC WHERE run_id = 'baseline_001'
# MAGIC GROUP BY zone_name, primary_category_name
# MAGIC ORDER BY zone_name, pct_with_reviews DESC, businesses DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Compare business ratings and review volume across price levels within each category.
# MAGIC SELECT
# MAGIC     primary_category_name,
# MAGIC     price_level_name,
# MAGIC     businesses,
# MAGIC     businesses_with_rating,
# MAGIC     businesses_with_review_count,
# MAGIC     median_business_rating,
# MAGIC     round(mean_business_rating,2) AS mean_business_rating,
# MAGIC     median_review_count
# MAGIC FROM places_ratings.gold.v_price_reputation
# MAGIC WHERE run_id = 'baseline_001'
# MAGIC ORDER BY
# MAGIC     primary_category_name,
# MAGIC     price_order ASC NULLS LAST,
# MAGIC     price_level_name;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Compare business ratings across review-count bands within each category.
# MAGIC WITH business_bands AS (
# MAGIC     SELECT
# MAGIC         *,
# MAGIC         CASE
# MAGIC             WHEN review_count IS NULL THEN 6
# MAGIC             WHEN review_count = 0 THEN 0
# MAGIC             WHEN review_count <= 10 THEN 1
# MAGIC             WHEN review_count <= 50 THEN 2
# MAGIC             WHEN review_count <= 200 THEN 3
# MAGIC             WHEN review_count <= 1000 THEN 4
# MAGIC             ELSE 5
# MAGIC         END AS band_order,
# MAGIC         CASE
# MAGIC             WHEN review_count IS NULL THEN 'No reportado'
# MAGIC             WHEN review_count = 0 THEN '0'
# MAGIC             WHEN review_count <= 10 THEN '1–10'
# MAGIC             WHEN review_count <= 50 THEN '11–50'
# MAGIC             WHEN review_count <= 200 THEN '51–200'
# MAGIC             WHEN review_count <= 1000 THEN '201–1000'
# MAGIC             ELSE 'Más de 1000'
# MAGIC         END AS review_count_band
# MAGIC     FROM places_ratings.gold.v_business_explorer
# MAGIC     WHERE run_id = 'baseline_001'
# MAGIC )
# MAGIC SELECT
# MAGIC     primary_category_name,
# MAGIC     review_count_band,
# MAGIC     COUNT(*) AS businesses,
# MAGIC     COUNT(rating) AS businesses_with_rating,
# MAGIC     ROUND(
# MAGIC         percentile_cont(0.5)
# MAGIC             WITHIN GROUP (ORDER BY rating),
# MAGIC         2
# MAGIC     ) AS median_business_rating,
# MAGIC     ROUND(AVG(rating), 2) AS mean_business_rating
# MAGIC FROM business_bands
# MAGIC GROUP BY primary_category_name, band_order, review_count_band
# MAGIC ORDER BY primary_category_name, band_order;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Completion and limitations
# MAGIC Run this notebook again to verify that fact and bridge MERGEs insert zero rows.
# MAGIC Do not commit API keys. This notebook neither reads nor stores credentials.
# MAGIC Historical growth needs a second extraction; do not derive growth from repeated query appearances.
# MAGIC The discovery cap means the sample is not a census. Price availability and zone attribution must
# MAGIC remain visible when interpreting results. Gold is ready for exploratory dashboards, not proof
# MAGIC that reviews, prices or website presence cause business performance or demand for NFC.
# MAGIC
# MAGIC The notebook was generated and structurally reviewed locally; actual Spark SQL execution and
# MAGIC reconciliation must be completed in Databricks. SQL references:
# MAGIC [MERGE](https://docs.databricks.com/aws/en/sql/language-manual/delta-merge-into) and
# MAGIC [explode](https://docs.databricks.com/aws/en/sql/language-manual/functions/explode).