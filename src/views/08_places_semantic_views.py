# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# COMMAND ----------
# MAGIC %md
# MAGIC # Semantic views
# MAGIC Consumer-facing analytical views. Run after the Gold tables have loaded and the semantic schema exists.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Publish a convenient one-row-per-business/extraction view entirely from Gold tables.
# MAGIC CREATE OR REPLACE VIEW places_ratings.semantic.v_business_explorer AS
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
# MAGIC CREATE OR REPLACE VIEW places_ratings.semantic.v_business_category_memberships AS
# MAGIC SELECT e.*, b.category_key AS detected_category_key, c.category_name AS detected_category_name
# MAGIC FROM places_ratings.semantic.v_business_explorer AS e
# MAGIC JOIN places_ratings.gold.bridge_business_category AS b
# MAGIC   ON e.business_key = b.business_key AND e.extraction_key = b.extraction_key
# MAGIC JOIN places_ratings.gold.dim_category AS c ON b.category_key = c.category_key;



# COMMAND ----------

# MAGIC %sql
# MAGIC -- Compare review presence, volume and observed pool size without treating missing counts as zero.
# MAGIC CREATE OR REPLACE VIEW places_ratings.semantic.v_category_metrics AS
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
# MAGIC FROM places_ratings.semantic.v_business_explorer
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
# MAGIC CREATE OR REPLACE VIEW places_ratings.semantic.v_price_reputation AS
# MAGIC SELECT run_id, primary_category_key, primary_category_name,
# MAGIC   price_level_key, price_level_name, price_order,
# MAGIC   COUNT(*) AS businesses, COUNT(rating) AS businesses_with_rating,
# MAGIC   COUNT(review_count) AS businesses_with_review_count,
# MAGIC   ROUND(AVG(rating), 2) AS mean_business_rating,
# MAGIC   percentile_cont(0.5) WITHIN GROUP (ORDER BY rating) AS median_business_rating,
# MAGIC   percentile_cont(0.5) WITHIN GROUP (ORDER BY review_count) AS median_review_count
# MAGIC FROM places_ratings.semantic.v_business_explorer
# MAGIC GROUP BY run_id, primary_category_key, primary_category_name,
# MAGIC   price_level_key, price_level_name, price_order;



