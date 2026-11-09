# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Places — Silver preparation
# MAGIC
# MAGIC This notebook parses the 18 fields in baseline_001 and creates temporary structured
# MAGIC views, including places_silver_observations. It retains source lineage and required
# MAGIC technical validation. It makes no API calls and writes no persistent Silver tables.
# MAGIC
# MAGIC EDA diagnostics are in ../eda/05_places_eda. No geographic exclusion is applied:
# MAGIC coordinates and search nuclei remain context, not eligibility filters.
# MAGIC Optional nulls, explicit false and zero are preserved. Business deduplication and
# MAGIC analytical category rules remain decisions to be informed by EDA.
# MAGIC
# MAGIC Run top to bottom; NULL from assert_true means the check passed.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Select the baseline

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Select the verified baseline and retain source metadata and original place arrays.
# MAGIC CREATE OR REPLACE TEMP VIEW places_silver_source AS
# MAGIC SELECT
# MAGIC     run_id,
# MAGIC     query_key,
# MAGIC     extraction_id,
# MAGIC     source_file_path,
# MAGIC     extracted_at_utc,
# MAGIC     bronze_ingested_at,
# MAGIC     study_area,
# MAGIC     requested_category,
# MAGIC     mapping_version,
# MAGIC     field_set_version,
# MAGIC     get_json_object(raw_json, '$.extraction.search_context') AS search_context_json,
# MAGIC     get_json_object(raw_json, '$.extraction.response_data') AS response_json,
# MAGIC     get_json_object(raw_json, '$.extraction.response_data.places') AS places_json
# MAGIC FROM places_ratings.bronze.api_responses
# MAGIC WHERE run_id = 'baseline_001';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify the baseline scope and response identities before parsing businesses.
# MAGIC SELECT
# MAGIC     assert_true(COUNT(*) = 315, 'baseline_001 must contain 315 responses.') AS source_count_check,
# MAGIC     assert_true(COUNT(DISTINCT query_key) = COUNT(*), 'Duplicate or missing query keys.') AS query_key_check,
# MAGIC     assert_true(COUNT_IF(
# MAGIC         mapping_version IS NULL OR mapping_version <> '0.2'
# MAGIC         OR field_set_version IS NULL OR field_set_version <> 'full_context_v1'
# MAGIC         OR response_json IS NULL
# MAGIC         OR get_json_object(response_json, '$.error') IS NOT NULL
# MAGIC     ) = 0, 'Unexpected baseline definition or response body.') AS source_definition_check
# MAGIC FROM places_silver_source;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Parse arrays and query context

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Parse all 18 requested Place fields and the query's geographic context.
# MAGIC CREATE OR REPLACE TEMP VIEW places_silver_parsed AS
# MAGIC SELECT
# MAGIC     run_id,
# MAGIC     query_key,
# MAGIC     extraction_id,
# MAGIC     source_file_path,
# MAGIC     extracted_at_utc,
# MAGIC     bronze_ingested_at,
# MAGIC     study_area,
# MAGIC     requested_category,
# MAGIC     mapping_version,
# MAGIC     field_set_version,
# MAGIC     from_json(
# MAGIC         search_context_json,
# MAGIC         'STRUCT<
# MAGIC             point_id: STRING,
# MAGIC             study_center: STRUCT<latitude: DOUBLE, longitude: DOUBLE>,
# MAGIC             study_radius_meters: DOUBLE,
# MAGIC             parent_area: STRING,
# MAGIC             city: STRING,
# MAGIC             grid_spacing_meters: DOUBLE
# MAGIC         >',
# MAGIC         map('mode', 'FAILFAST')
# MAGIC     ) AS search_context,
# MAGIC     from_json(
# MAGIC         COALESCE(places_json, '[]'),
# MAGIC         'ARRAY<STRUCT<
# MAGIC     id: STRING,
# MAGIC     displayName: STRUCT<text: STRING, languageCode: STRING>,
# MAGIC     primaryType: STRING,
# MAGIC     types: ARRAY<STRING>,
# MAGIC     formattedAddress: STRING,
# MAGIC     location: STRUCT<latitude: DOUBLE, longitude: DOUBLE>,
# MAGIC     businessStatus: STRING,
# MAGIC     rating: DOUBLE,
# MAGIC     userRatingCount: BIGINT,
# MAGIC     websiteUri: STRING,
# MAGIC     regularOpeningHours: STRUCT<
# MAGIC         periods: ARRAY<STRUCT<
# MAGIC             open: STRUCT<
# MAGIC                 day: INT, hour: INT, minute: INT,
# MAGIC                 date: STRUCT<year: INT, month: INT, day: INT>,
# MAGIC                 truncated: BOOLEAN
# MAGIC             >,
# MAGIC             close: STRUCT<
# MAGIC                 day: INT, hour: INT, minute: INT,
# MAGIC                 date: STRUCT<year: INT, month: INT, day: INT>,
# MAGIC                 truncated: BOOLEAN
# MAGIC             >
# MAGIC         >>,
# MAGIC         weekdayDescriptions: ARRAY<STRING>,
# MAGIC         secondaryHoursType: STRING,
# MAGIC         specialDays: ARRAY<STRUCT<date: STRUCT<year: INT, month: INT, day: INT>>>,
# MAGIC         nextOpenTime: STRING,
# MAGIC         nextCloseTime: STRING,
# MAGIC         openNow: BOOLEAN
# MAGIC     >,
# MAGIC     googleMapsUri: STRING,
# MAGIC     attributions: ARRAY<STRUCT<provider: STRING, providerUri: STRING>>,
# MAGIC     googleMapsLinks: STRUCT<
# MAGIC         directionsUri: STRING,
# MAGIC         placeUri: STRING,
# MAGIC         writeAReviewUri: STRING,
# MAGIC         reviewsUri: STRING,
# MAGIC         photosUri: STRING
# MAGIC     >,
# MAGIC     nationalPhoneNumber: STRING,
# MAGIC     containingPlaces: ARRAY<STRUCT<name: STRING, id: STRING>>,
# MAGIC     pureServiceAreaBusiness: BOOLEAN,
# MAGIC     priceLevel: STRING
# MAGIC >>',
# MAGIC         map('mode', 'FAILFAST')
# MAGIC     ) AS places
# MAGIC FROM places_silver_source;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Require valid arrays and geographic context without rejecting legitimate empty responses.
# MAGIC SELECT
# MAGIC     assert_true(COUNT_IF(places IS NULL) = 0, 'A business array could not be parsed.') AS array_parse_check,
# MAGIC     assert_true(COUNT_IF(
# MAGIC         search_context.point_id IS NULL
# MAGIC         OR search_context.study_center.latitude IS NULL
# MAGIC         OR search_context.study_center.longitude IS NULL
# MAGIC         OR search_context.study_radius_meters IS NULL
# MAGIC         OR search_context.parent_area IS NULL
# MAGIC         OR search_context.city IS NULL
# MAGIC     ) = 0, 'Search context is incomplete; review Bronze metadata.') AS search_context_check
# MAGIC FROM places_silver_parsed;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Expand and reconcile observations

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Expand place arrays into observations without discarding repeated businesses.
# MAGIC CREATE OR REPLACE TEMP VIEW places_silver_observations AS
# MAGIC SELECT
# MAGIC     sha2(to_json(named_struct(
# MAGIC         'run_id', source.run_id,
# MAGIC         'query_key', source.query_key,
# MAGIC         'place_position', place_position
# MAGIC     )), 256) AS observation_id,
# MAGIC     source.run_id,
# MAGIC     source.query_key,
# MAGIC     source.extraction_id,
# MAGIC     source.source_file_path,
# MAGIC     source.extracted_at_utc,
# MAGIC     source.bronze_ingested_at,
# MAGIC     source.study_area,
# MAGIC     source.search_context.parent_area AS parent_area,
# MAGIC     source.search_context.city AS city,
# MAGIC     source.search_context.point_id AS search_point_id,
# MAGIC     source.search_context.study_center.latitude AS study_center_latitude,
# MAGIC     source.search_context.study_center.longitude AS study_center_longitude,
# MAGIC     source.search_context.study_radius_meters AS study_radius_meters,
# MAGIC     source.requested_category,
# MAGIC     source.mapping_version,
# MAGIC     source.field_set_version,
# MAGIC     place_position,
# MAGIC     place.id AS place_id,
# MAGIC     place.displayName.text AS business_name,
# MAGIC     place.displayName.languageCode AS name_language,
# MAGIC     place.primaryType AS primary_type,
# MAGIC     place.types AS source_types,
# MAGIC     place.formattedAddress AS formatted_address,
# MAGIC     place.location.latitude AS latitude,
# MAGIC     place.location.longitude AS longitude,
# MAGIC     place.businessStatus AS business_status,
# MAGIC     place.rating AS rating,
# MAGIC     place.userRatingCount AS review_count,
# MAGIC     place.websiteUri AS website_uri,
# MAGIC     place.regularOpeningHours AS regular_opening_hours,
# MAGIC     place.regularOpeningHours.weekdayDescriptions AS weekday_descriptions,
# MAGIC     place.googleMapsUri AS google_maps_uri,
# MAGIC     place.attributions AS source_attributions,
# MAGIC     place.googleMapsLinks AS google_maps_links,
# MAGIC     place.googleMapsLinks.writeAReviewUri AS write_review_uri,
# MAGIC     place.googleMapsLinks.reviewsUri AS reviews_uri,
# MAGIC     place.nationalPhoneNumber AS national_phone_number,
# MAGIC     place.containingPlaces AS containing_places,
# MAGIC     place.pureServiceAreaBusiness AS pure_service_area_business,
# MAGIC     place.priceLevel AS price_level
# MAGIC FROM places_silver_parsed AS source
# MAGIC LATERAL VIEW posexplode(source.places) expanded AS place_position, place;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Reconcile expanded observations with the parsed array element count.
# MAGIC SELECT
# MAGIC     assert_true(
# MAGIC         (SELECT COUNT(*) FROM places_silver_observations)
# MAGIC         = (SELECT COALESCE(SUM(size(places)), 0) FROM places_silver_parsed),
# MAGIC         'Expanded observation count differs from the source arrays.'
# MAGIC     ) AS expansion_count_check,
# MAGIC     assert_true(
# MAGIC         (SELECT COUNT(*) FROM places_silver_observations)
# MAGIC         = (SELECT COUNT(DISTINCT observation_id) FROM places_silver_observations),
# MAGIC         'Observation identities are not unique.'
# MAGIC     ) AS observation_identity_check;