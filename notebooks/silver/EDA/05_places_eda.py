# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Places — Exploratory Data Analysis
# MAGIC
# MAGIC ## Objective
# MAGIC Understand baseline_001 before choosing cleaning, business consolidation and Gold rules.
# MAGIC This notebook reuses places_silver_observations; it does not duplicate JSON parsing.
# MAGIC It creates only temporary analytical views and performs SELECT queries.
# MAGIC
# MAGIC ## Three grains
# MAGIC - Response: one API query result (315 responses in the validated baseline).
# MAGIC - Observation: one appearance of a business in one response (3,272 previously validated).
# MAGIC - Business: a distinct place_id in this run (1,314 previously validated).
# MAGIC
# MAGIC Repeated appearances are expected because queries, nuclei and categories overlap.
# MAGIC They must not automatically give a business more statistical weight.
# MAGIC The collection is a purposive API sample, not a census or a representative survey.
# MAGIC
# MAGIC ## How to use
# MAGIC Run sections in order. Read the Markdown before interpreting each output.
# MAGIC Report figures from executed outputs, not from the reference counts above.
# MAGIC No conclusions or business outcomes have been pre-filled.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Reuse Silver preparation
# MAGIC Temporary views from another notebook are not guaranteed to exist in this session.
# MAGIC The next standalone %run cell executes ../silver/04_places_silver inline to create
# MAGIC the same structured views here. It does not extract data or write tables.
# MAGIC Both notebooks must be imported at the paths shown, without _clean in their names.
# MAGIC If you renamed Silver, adjust this relative notebook path before executing.

# COMMAND ----------

# MAGIC %run "../04_places_silver"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Response parsing and dataset size
# MAGIC These are response-level and observation-level counts, not counts of customers.
# MAGIC An empty places array is a legitimate zero-result query. Its response remains in
# MAGIC Bronze even though it contributes no business observation after expansion.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Inspect response parsing, empty results and the number of business appearances.
# MAGIC SELECT
# MAGIC     COUNT(*) AS source_responses,
# MAGIC     COUNT_IF(places IS NULL) AS unparsed_arrays,
# MAGIC     COUNT_IF(size(places) = 0) AS empty_responses,
# MAGIC     SUM(size(places)) AS business_appearances,
# MAGIC     COUNT_IF(
# MAGIC         search_context.point_id IS NULL
# MAGIC         OR search_context.study_center.latitude IS NULL
# MAGIC         OR search_context.study_center.longitude IS NULL
# MAGIC         OR search_context.study_radius_meters IS NULL
# MAGIC         OR search_context.parent_area IS NULL
# MAGIC         OR search_context.city IS NULL
# MAGIC     ) AS responses_missing_search_context
# MAGIC FROM places_silver_parsed;

# COMMAND ----------

# MAGIC %sql
# MAGIC desc table places_silver_observations

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Summarize observation identities and missing business information.
# MAGIC SELECT
# MAGIC     COUNT(*) AS business_appearances,
# MAGIC     COUNT(DISTINCT observation_id) AS distinct_observations,
# MAGIC     COUNT(DISTINCT place_id) AS unique_businesses,
# MAGIC     COUNT_IF(place_id IS NULL OR trim(place_id) = '') AS missing_place_id,
# MAGIC     COUNT_IF(latitude IS NULL OR longitude IS NULL) AS missing_coordinates,
# MAGIC     COUNT_IF(primary_type IS NULL) AS missing_primary_type,
# MAGIC     COUNT_IF(rating IS NULL) AS missing_rating,
# MAGIC     COUNT_IF(review_count IS NULL) AS missing_review_count
# MAGIC FROM places_silver_observations;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Search coverage by nucleus and requested category
# MAGIC A response with 20 places reaches the endpoint cap and may be incomplete.
# MAGIC A response below the cap does not prove exhaustive coverage.
# MAGIC These counts explain the collection method; equal nucleus surfaces or equal
# MAGIC numbers of businesses are not study requirements. No geographic exclusions follow.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Profile response counts, empty searches and potentially truncated queries.
# MAGIC SELECT
# MAGIC     study_area,
# MAGIC     requested_category,
# MAGIC     COUNT(*) AS response_count,
# MAGIC     COUNT_IF(size(places) = 0) AS empty_responses,
# MAGIC     COUNT_IF(size(places) = 20) AS responses_at_limit,
# MAGIC     SUM(size(places)) AS business_appearances
# MAGIC FROM places_silver_parsed
# MAGIC GROUP BY study_area, requested_category
# MAGIC ORDER BY study_area, requested_category;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Missing fields and invalid values across observations
# MAGIC These results use appearances as their denominator. They are useful for auditing
# MAGIC the extraction, but they are not business-level prevalence estimates.
# MAGIC A missing website is not proof of no website. False is different from NULL.
# MAGIC A missing review count is not converted to zero. Values outside expected ranges
# MAGIC are profiled, not deleted. A future unfamiliar enum is a review case, not an
# MAGIC automatic reason to discard a business.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Measure reported context fields across appearances without treating missing values as false.
# MAGIC SELECT
# MAGIC     COUNT(*) AS business_appearances,
# MAGIC     COUNT_IF(website_uri IS NOT NULL) AS reported_website,
# MAGIC     COUNT_IF(regular_opening_hours IS NOT NULL) AS reported_opening_hours,
# MAGIC     COUNT_IF(google_maps_uri IS NOT NULL) AS reported_maps_link,
# MAGIC     COUNT_IF(source_attributions IS NOT NULL) AS reported_attributions,
# MAGIC     COUNT_IF(google_maps_links IS NOT NULL) AS reported_maps_action_links,
# MAGIC     COUNT_IF(write_review_uri IS NOT NULL) AS reported_write_review_link,
# MAGIC     COUNT_IF(national_phone_number IS NOT NULL) AS reported_phone,
# MAGIC     COUNT_IF(containing_places IS NOT NULL) AS reported_containing_places,
# MAGIC     COUNT_IF(pure_service_area_business IS NOT NULL) AS reported_service_area_status,
# MAGIC     COUNT_IF(price_level IS NOT NULL) AS reported_price_level
# MAGIC FROM places_silver_observations;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Profile invalid values for review without changing or removing observations.
# MAGIC SELECT
# MAGIC     COUNT_IF(rating IS NOT NULL AND (rating < 1 OR rating > 5)) AS invalid_rating,
# MAGIC     COUNT_IF(review_count IS NOT NULL AND review_count < 0) AS invalid_review_count,
# MAGIC     COUNT_IF(latitude IS NOT NULL AND (latitude < -90 OR latitude > 90)) AS invalid_latitude,
# MAGIC     COUNT_IF(longitude IS NOT NULL AND (longitude < -180 OR longitude > 180)) AS invalid_longitude,
# MAGIC     COUNT_IF(price_level IS NOT NULL AND price_level NOT IN (
# MAGIC         'PRICE_LEVEL_UNSPECIFIED', 'PRICE_LEVEL_FREE', 'PRICE_LEVEL_INEXPENSIVE',
# MAGIC         'PRICE_LEVEL_MODERATE', 'PRICE_LEVEL_EXPENSIVE', 'PRICE_LEVEL_VERY_EXPENSIVE'
# MAGIC     )) AS unrecognized_price_level,
# MAGIC     COUNT_IF(business_status IS NOT NULL AND business_status NOT IN (
# MAGIC         'OPERATIONAL', 'CLOSED_TEMPORARILY', 'CLOSED_PERMANENTLY', 'FUTURE_OPENING'
# MAGIC     )) AS unrecognized_business_status
# MAGIC FROM places_silver_observations;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Repeated appearances and overlapping memberships
# MAGIC Group by run_id and place_id to measure how often each business was retrieved.
# MAGIC Multiple requested categories may be legitimate. Shared coordinates or addresses
# MAGIC alone do not justify merging different place IDs.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Profile query frequency and category/nucleus membership per business.
# MAGIC CREATE OR REPLACE TEMP VIEW places_eda_business_frequency AS
# MAGIC SELECT
# MAGIC     run_id,
# MAGIC     place_id,
# MAGIC     COUNT(*) AS observation_count,
# MAGIC     COUNT(DISTINCT query_key) AS query_count,
# MAGIC     COUNT(DISTINCT requested_category) AS category_count,
# MAGIC     COUNT(DISTINCT study_area) AS nucleus_count
# MAGIC FROM places_silver_observations
# MAGIC WHERE place_id IS NOT NULL AND trim(place_id) <> ''
# MAGIC GROUP BY run_id, place_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Summarize repetition without treating multiple appearances as duplicate businesses.
# MAGIC SELECT
# MAGIC     COUNT(*) AS unique_businesses,
# MAGIC     SUM(observation_count) AS observations_with_valid_id,
# MAGIC     MIN(observation_count) AS minimum_appearances,
# MAGIC     percentile(observation_count, 0.5) AS median_appearances,
# MAGIC     MAX(observation_count) AS maximum_appearances,
# MAGIC     COUNT_IF(observation_count > 1) AS businesses_seen_multiple_times,
# MAGIC     COUNT_IF(category_count > 1) AS businesses_in_multiple_categories,
# MAGIC     COUNT_IF(nucleus_count > 1) AS businesses_in_multiple_nuclei
# MAGIC FROM places_eda_business_frequency;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Inspect the most frequently retrieved businesses with their membership counts.
# MAGIC SELECT *
# MAGIC FROM places_eda_business_frequency
# MAGIC ORDER BY observation_count DESC, place_id
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Differences between observations of the same business
# MAGIC Before choosing a consolidation rule, inspect whether non-null reported values differ.
# MAGIC COUNT(DISTINCT ...) ignores NULL, so missing-versus-reported variation is counted
# MAGIC separately. Different serialized hours are a review signal: formatting or array
# MAGIC ordering may differ without a meaningful schedule change. This is within-run
# MAGIC variation, not proof of a month-to-month business change.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Profile conflicting reported values and mixed missingness per business.
# MAGIC CREATE OR REPLACE TEMP VIEW places_eda_business_variation AS
# MAGIC SELECT
# MAGIC     run_id,
# MAGIC     place_id,
# MAGIC     COUNT(*) AS observation_count,
# MAGIC     COUNT(DISTINCT business_name) AS distinct_names,
# MAGIC     COUNT(DISTINCT primary_type) AS distinct_primary_types,
# MAGIC     COUNT(DISTINCT rating) AS distinct_ratings,
# MAGIC     COUNT(DISTINCT review_count) AS distinct_review_counts,
# MAGIC     COUNT(DISTINCT website_uri) AS distinct_websites,
# MAGIC     COUNT(DISTINCT national_phone_number) AS distinct_phones,
# MAGIC     COUNT(DISTINCT price_level) AS distinct_price_levels,
# MAGIC     COUNT(DISTINCT to_json(regular_opening_hours)) AS distinct_hours_representations,
# MAGIC     COUNT_IF(review_count IS NULL) AS observations_missing_review_count,
# MAGIC     COUNT_IF(review_count IS NOT NULL) AS observations_reporting_review_count
# MAGIC FROM places_silver_observations
# MAGIC WHERE place_id IS NOT NULL AND trim(place_id) <> ''
# MAGIC GROUP BY run_id, place_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Count businesses requiring a closer look before final deduplication rules.
# MAGIC SELECT
# MAGIC     COUNT(*) AS unique_businesses,
# MAGIC     COUNT_IF(distinct_names > 1) AS businesses_with_name_variation,
# MAGIC     COUNT_IF(distinct_primary_types > 1) AS businesses_with_primary_type_variation,
# MAGIC     COUNT_IF(distinct_ratings > 1) AS businesses_with_rating_variation,
# MAGIC     COUNT_IF(distinct_review_counts > 1) AS businesses_with_review_count_variation,
# MAGIC     COUNT_IF(distinct_websites > 1) AS businesses_with_website_variation,
# MAGIC     COUNT_IF(distinct_phones > 1) AS businesses_with_phone_variation,
# MAGIC     COUNT_IF(distinct_price_levels > 1) AS businesses_with_price_variation,
# MAGIC     COUNT_IF(distinct_hours_representations > 1) AS businesses_with_hours_variation,
# MAGIC     COUNT_IF(observations_missing_review_count > 0
# MAGIC              AND observations_reporting_review_count > 0) AS businesses_with_mixed_review_count_missingness
# MAGIC FROM places_eda_business_variation;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Inspect a bounded sample of businesses with changing reported values.
# MAGIC SELECT *
# MAGIC FROM places_eda_business_variation
# MAGIC WHERE distinct_ratings > 1 OR distinct_review_counts > 1
# MAGIC    OR distinct_websites > 1 OR distinct_hours_representations > 1
# MAGIC ORDER BY observation_count DESC, place_id
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Provisional one-row-per-business sample
# MAGIC For EDA only, take the latest whole observation per run and place ID. Ties use
# MAGIC query_key and array position for deterministic selection. This is an explicit
# MAGIC provisional rule, not a finalized Silver model. It does not choose the highest
# MAGIC review count or stitch together fields from different observations.
# MAGIC A later observation can have fewer reported optional fields; review Section 5
# MAGIC before adopting this rule for the final tables.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Select one traceable whole observation per business for descriptive EDA.
# MAGIC CREATE OR REPLACE TEMP VIEW places_eda_business_sample AS
# MAGIC SELECT * EXCEPT (eda_row_number)
# MAGIC FROM (
# MAGIC     SELECT
# MAGIC         *,
# MAGIC         ROW_NUMBER() OVER (
# MAGIC             PARTITION BY run_id, place_id
# MAGIC             ORDER BY extracted_at_utc DESC, query_key DESC, place_position DESC
# MAGIC         ) AS eda_row_number
# MAGIC     FROM places_silver_observations
# MAGIC     WHERE place_id IS NOT NULL AND trim(place_id) <> ''
# MAGIC ) AS ranked
# MAGIC WHERE eda_row_number = 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Confirm that the provisional sample gives each business one row.
# MAGIC SELECT
# MAGIC     COUNT(*) AS sample_rows,
# MAGIC     COUNT(DISTINCT place_id) AS distinct_businesses,
# MAGIC     COUNT_IF(rating IS NULL) AS businesses_missing_rating,
# MAGIC     COUNT_IF(review_count IS NULL) AS businesses_missing_review_count
# MAGIC FROM places_eda_business_sample;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Business-level field coverage
# MAGIC Use the provisional business sample, not all repeated appearances.
# MAGIC Reported means the field is non-null; it does not guarantee accuracy, a useful
# MAGIC non-empty value, an owned website, or successful customer contact.
# MAGIC Service-area false and review count zero remain reported values.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Report context availability with one vote per business in the provisional business.
# MAGIC SELECT
# MAGIC     COUNT(*) AS businesses,
# MAGIC     COUNT_IF(website_uri IS NOT NULL) AS reported_website,
# MAGIC     COUNT_IF(regular_opening_hours IS NOT NULL) AS reported_hours,
# MAGIC     COUNT_IF(national_phone_number IS NOT NULL) AS reported_phone,
# MAGIC     COUNT_IF(price_level IS NOT NULL) AS reported_price_level,
# MAGIC     COUNT_IF(write_review_uri IS NOT NULL) AS reported_write_review_link,
# MAGIC     COUNT_IF(containing_places IS NOT NULL) AS reported_containing_places,
# MAGIC     COUNT_IF(pure_service_area_business IS NOT NULL) AS reported_service_area_status,
# MAGIC     ROUND(100.0 * COUNT_IF(website_uri IS NOT NULL) / NULLIF(COUNT(*), 0), 2)
# MAGIC         AS pct_reporting_website,
# MAGIC     ROUND(100.0 * COUNT_IF(regular_opening_hours IS NOT NULL) / NULLIF(COUNT(*), 0), 2)
# MAGIC         AS pct_reporting_hours
# MAGIC FROM places_eda_business_sample;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Review-count and rating distributions
# MAGIC Exclude NULL only from the metric that requires it, and display its denominator.
# MAGIC The exact median is less sensitive to extreme counts than the mean. A large
# MAGIC review count is not automatically invalid. It does not measure store visits,
# MAGIC revenue, repeat customers or willingness to buy an NFC solution.
# MAGIC Negative counts and out-of-range ratings are excluded from these provisional
# MAGIC summaries, not deleted from the observations.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Describe valid review counts and ratings without weighting repeated appearances.
# MAGIC SELECT
# MAGIC     COUNT(*) AS businesses_in_sample,
# MAGIC     COUNT_IF(review_count >= 0) AS businesses_with_valid_review_count,
# MAGIC     MIN(CASE WHEN review_count >= 0 THEN review_count END) AS minimum_review_count,
# MAGIC     percentile(CASE WHEN review_count >= 0 THEN review_count END, 0.5) AS median_review_count,
# MAGIC     ROUND(AVG(CASE WHEN review_count >= 0 THEN review_count END), 2) AS mean_review_count,
# MAGIC     percentile(CASE WHEN review_count >= 0 THEN review_count END, 0.9) AS p90_review_count,
# MAGIC     MAX(CASE WHEN review_count >= 0 THEN review_count END) AS maximum_review_count,
# MAGIC     COUNT_IF(rating BETWEEN 1 AND 5) AS businesses_with_valid_rating,
# MAGIC     ROUND(AVG(CASE WHEN rating BETWEEN 1 AND 5 THEN rating END), 2) AS mean_business_rating
# MAGIC FROM places_eda_business_sample;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Build review-count bands suitable for a bar chart.
# MAGIC WITH bands AS (
# MAGIC     SELECT CASE
# MAGIC         WHEN review_count IS NULL THEN '07 - Not reported'
# MAGIC         WHEN review_count < 0 THEN '08 - Invalid negative'
# MAGIC         WHEN review_count = 0 THEN '01 - Zero'
# MAGIC         WHEN review_count <= 10 THEN '02 - 1 to 10'
# MAGIC         WHEN review_count <= 50 THEN '03 - 11 to 50'
# MAGIC         WHEN review_count <= 200 THEN '04 - 51 to 200'
# MAGIC         WHEN review_count <= 1000 THEN '05 - 201 to 1000'
# MAGIC         ELSE '06 - Over 1000'
# MAGIC     END AS review_count_band
# MAGIC     FROM places_eda_business_sample
# MAGIC )
# MAGIC SELECT review_count_band, COUNT(*) AS businesses
# MAGIC FROM bands
# MAGIC GROUP BY review_count_band
# MAGIC ORDER BY review_count_band;

# COMMAND ----------

# MAGIC %md
# MAGIC Optional visualization: in Databricks create a bar chart from the previous result,
# MAGIC using review_count_band on the horizontal axis and businesses on the vertical axis.
# MAGIC Keep not-reported and invalid categories visible rather than silently dropping them.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Category comparison without repeated-query weighting
# MAGIC Preserve distinct requested-category membership separately from the provisional
# MAGIC business row. A business can contribute once to each category it was retrieved
# MAGIC under, so category totals are not additive to the global business total.
# MAGIC These are query-based memberships, not manually verified classifications.
# MAGIC The exploratory comparison is descriptive, not a statistical proof of the NFC hypothesis.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Preserve distinct query-category membership for analytical comparisons.
# MAGIC CREATE OR REPLACE TEMP VIEW places_eda_category_membership AS
# MAGIC SELECT DISTINCT run_id, place_id, requested_category
# MAGIC FROM places_silver_observations
# MAGIC WHERE place_id IS NOT NULL AND trim(place_id) <> '';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Compare category medians with explicit valid-count denominators and missingness.
# MAGIC SELECT
# MAGIC     membership.requested_category,
# MAGIC     COUNT(*) AS businesses,
# MAGIC     COUNT_IF(business.review_count >= 0) AS businesses_with_valid_review_count,
# MAGIC     COUNT_IF(business.review_count IS NULL) AS businesses_missing_review_count,
# MAGIC     percentile(CASE WHEN business.review_count >= 0 THEN business.review_count END, 0.5)
# MAGIC         AS median_review_count,
# MAGIC     ROUND(AVG(CASE WHEN business.review_count >= 0 THEN business.review_count END), 2)
# MAGIC         AS mean_review_count,
# MAGIC     COUNT_IF(business.website_uri IS NOT NULL) AS businesses_reporting_website,
# MAGIC     COUNT_IF(business.regular_opening_hours IS NOT NULL) AS businesses_reporting_hours
# MAGIC FROM places_eda_category_membership AS membership
# MAGIC JOIN places_eda_business_sample AS business
# MAGIC     ON membership.run_id = business.run_id
# MAGIC     AND membership.place_id = business.place_id
# MAGIC GROUP BY membership.requested_category
# MAGIC ORDER BY median_review_count DESC NULLS LAST;

# COMMAND ----------

# MAGIC %md
# MAGIC Optional visualization: compare category medians with a bar chart, accompanied
# MAGIC by businesses_with_valid_review_count and missing-count information. Differences
# MAGIC in query truncation, membership overlap and sampling limit interpretation.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Primary types, operating status and reported price levels
# MAGIC These profiles can reveal classification edge cases and missing fields.
# MAGIC Price level is categorical; do not average enum strings or turn levels into revenue.
# MAGIC Operational status is Google's report at observation time, not an independent audit.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Profile primary business types without category-query duplication.
# MAGIC SELECT primary_type, COUNT(*) AS businesses
# MAGIC FROM places_eda_business_sample
# MAGIC GROUP BY primary_type
# MAGIC ORDER BY businesses DESC, primary_type;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Profile reported operating status and price-level categories.
# MAGIC SELECT business_status, price_level, COUNT(*) AS businesses
# MAGIC FROM places_eda_business_sample
# MAGIC GROUP BY business_status, price_level
# MAGIC ORDER BY businesses DESC, business_status, price_level;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Review cases and interpretation notes
# MAGIC The following bounded sample is for understanding records, not for automated outreach.
# MAGIC Shared names or addresses are candidate review cases, not sufficient evidence of duplicates.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Inspect a bounded sample of context columns without displaying full raw responses.
# MAGIC SELECT
# MAGIC     place_id,
# MAGIC     business_name,
# MAGIC     study_area,
# MAGIC     requested_category,
# MAGIC     website_uri,
# MAGIC     weekday_descriptions,
# MAGIC     national_phone_number,
# MAGIC     price_level,
# MAGIC     write_review_uri
# MAGIC FROM places_eda_business_sample
# MAGIC WHERE website_uri IS NOT NULL
# MAGIC    OR regular_opening_hours IS NOT NULL
# MAGIC    OR price_level IS NOT NULL
# MAGIC ORDER BY place_id, query_key
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Analyst notes — Findings and Silver decisions
# MAGIC
# MAGIC ### Dataset scope
# MAGIC
# MAGIC The analysis covers `baseline_001`, using mapping version `0.2`
# MAGIC and field set `full_context_v1`.
# MAGIC
# MAGIC - 315 API responses, including 7 valid empty responses.
# MAGIC - 3,272 business appearances.
# MAGIC - 1,314 distinct Google place IDs.
# MAGIC - Selected commercial nuclei in La Paz and El Alto.
# MAGIC
# MAGIC This is a purposive API sample, not a census or a representative
# MAGIC sample of all businesses in either city.
# MAGIC
# MAGIC ### Data quality
# MAGIC
# MAGIC The implemented checks found:
# MAGIC
# MAGIC - No missing place IDs or coordinates.
# MAGIC - One observation with a missing primary type.
# MAGIC - No invalid ratings, negative review counts or coordinates outside
# MAGIC   valid ranges.
# MAGIC - No unrecognized business-status or price-level values.
# MAGIC
# MAGIC Missing optional information is preserved as NULL.
# MAGIC Missing review counts are not converted to zero.
# MAGIC
# MAGIC ### Consistency across repeated observations
# MAGIC
# MAGIC No business had multiple distinct reported values for the following
# MAGIC fields within this extraction:
# MAGIC
# MAGIC - Business name and primary type.
# MAGIC - Rating and review count.
# MAGIC - Website and phone.
# MAGIC - Price level and serialized regular opening hours.
# MAGIC
# MAGIC Additional checks found no businesses mixing missing and reported
# MAGIC values for review count, website, opening hours, phone or price level.
# MAGIC
# MAGIC These checks support deterministic whole-observation consolidation.
# MAGIC They do not establish equality across every field or accuracy of the
# MAGIC information reported by Google.
# MAGIC
# MAGIC ### Business-level field coverage
# MAGIC
# MAGIC The provisional latest-observation sample contains 1,314 businesses.
# MAGIC
# MAGIC | Field | Businesses reporting information | Share |
# MAGIC |---|---:|---:|
# MAGIC | Website | 301 | 22.91% |
# MAGIC | Regular opening hours | 931 | 70.85% |
# MAGIC | Phone | 914 | 69.56% |
# MAGIC | Price level | 404 | 30.75% |
# MAGIC | Write-review link | 1,314 | 100.00% |
# MAGIC | Containing places | 110 | 8.37% |
# MAGIC | Pure service-area status | 0 | 0.00% |
# MAGIC
# MAGIC An absent field means it was not reported in this extraction.
# MAGIC It does not confirm that the business lacks that attribute.
# MAGIC
# MAGIC ### Review-count distribution
# MAGIC
# MAGIC - 303 businesses (23.06%) have no reported review count.
# MAGIC - 1,011 businesses have a reported count.
# MAGIC - 502 businesses have between 1 and 10 reviews: 38.20% of all
# MAGIC   sampled businesses and 49.65% of those with a reported count.
# MAGIC
# MAGIC Means substantially exceed medians in several categories, consistent
# MAGIC with a right-skewed distribution. High values are not automatically
# MAGIC treated as errors.
# MAGIC
# MAGIC ### Comparison by requested category
# MAGIC
# MAGIC | Category | Businesses | Businesses with reported review counts | Median review count |
# MAGIC |---|---:|---:|---:|
# MAGIC | Bars and pubs | 104 | 89 | 26 |
# MAGIC | Nightclubs | 49 | 33 | 15 |
# MAGIC | Gyms | 49 | 38 | 12 |
# MAGIC | Cafés | 294 | 226 | 11 |
# MAGIC | Restaurants | 627 | 507 | 11 |
# MAGIC | Veterinary care and grooming | 66 | 49 | 10 |
# MAGIC | Hair salons and barbers | 170 | 108 | 5.5 |
# MAGIC
# MAGIC Medians exclude missing review counts.
# MAGIC
# MAGIC The observed medians do not support the initial expectation that cafés
# MAGIC and restaurants have the highest typical review counts. Bars and pubs
# MAGIC have the highest observed median in this sample.
# MAGIC
# MAGIC This is a descriptive finding, not a population-level statistical test.
# MAGIC Category memberships overlap and are based on retrieval, not
# MAGIC independently verified classifications.
# MAGIC
# MAGIC ### Operating status and classification
# MAGIC
# MAGIC - 1,280 businesses are reported as operational.
# MAGIC - 34 are reported as temporarily closed.
# MAGIC
# MAGIC Temporarily closed businesses will be retained with their reported
# MAGIC status, rather than deleted.
# MAGIC
# MAGIC Some primary types require closer inspection against the complete
# MAGIC type array and requested category. An unexpected primary type alone
# MAGIC is not sufficient evidence of an invalid record.
# MAGIC
# MAGIC ### Silver consolidation decisions
# MAGIC
# MAGIC 1. Preserve all business observations and source lineage.
# MAGIC 2. Create one business snapshot per `run_id + place_id`.
# MAGIC 3. Select the latest whole observation by extraction timestamp.
# MAGIC 4. Resolve timestamp ties deterministically using `query_key`
# MAGIC    and `place_position`.
# MAGIC 5. Preserve category and commercial-nucleus memberships separately.
# MAGIC 6. Keep missing optional fields as NULL and preserve explicit false
# MAGIC    and zero values.
# MAGIC 7. Apply no geographic exclusion; coordinates and search nuclei remain
# MAGIC    available as context.
# MAGIC
# MAGIC These rules have been selected but persistent Silver tables have not
# MAGIC yet been implemented at this stage.
# MAGIC
# MAGIC ### Business interpretation and limitations
# MAGIC
# MAGIC 87 responses reached the 20-result limit, indicating potentially
# MAGIC incomplete coverage. Responses below that limit do not prove
# MAGIC exhaustive coverage.
# MAGIC
# MAGIC Review counts do not establish customer traffic, revenue, repeat
# MAGIC visits, willingness to pay or demand for an NFC solution.
# MAGIC
# MAGIC The availability of a write-review link supports a possible technical
# MAGIC implementation. It does not demonstrate that customers will use it
# MAGIC or publish reviews.
# MAGIC
# MAGIC Interviews and a small pilot are still needed to validate commercial
# MAGIC interest. Future review-count changes alone will not establish that
# MAGIC NFC caused those changes.