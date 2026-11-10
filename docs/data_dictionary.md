# Data dictionary and metric contracts

## Grains and identities

| Object | One row represents | Identity |
|---|---|---|
| Landing response file | One persisted API query response | `extraction_id`; planned request tracked by `(run_id, query_key)` |
| `bronze.api_responses` | One query response in a run | `(run_id, query_key)` |
| Temporary `places_silver_observations` | One business appearance at an array position | `observation_id`, derived from run, query and position |
| `silver.business_snapshots` | One consolidated establishment in a run | `(run_id, place_id)` |
| `gold.fact_business_snapshot` | One establishment snapshot | `(extraction_key, business_key)` |
| `gold.bridge_business_category` | One detected category membership in a snapshot | `(extraction_key, business_key, category_key)` |

Appearances are not independent establishments. A repeated detection is retained for lineage but does not add another consolidated business row.

## Important fields

| Field | Meaning and handling |
|---|---|
| `place_id` | Google establishment identifier used as a source business key |
| `run_id` | Extraction identity; resuming a run is different from starting a new one |
| `business_key`, `extraction_key` | Deterministic SHA-256 keys derived from source identities |
| `rating` | Reported business star rating; preserve NULL when absent |
| `review_count` | Reported total valuation count; integer, with explicit zero distinct from NULL |
| `primary_type` | Source primary business type; initial Gold classification input |
| `source_types` | All types reported for the selected business observation |
| `requested_categories` | Categories of searches that returned the establishment; not verified activity labels |
| `primary_category_key` | One assigned analytical category for that snapshot |
| `zone_key` | Study-area attribution from nearest detected search nucleus |
| `price_level_key` | Ordinal price member; not an amount, revenue or willingness-to-pay measure |
| `has_website`, `has_phone`, `has_opening_hours` | Whether the selected response reports that information; not proof of actual absence when false |
| `date_key` | UTC date of the selected observation, YYYYMMDD |
| `source_observation_count` | Number of appearances represented by this consolidated snapshot |
| `selected_observation_id` | Lineage back to the selected whole observation |
| `silver_snapshot_hash`, `fact_content_hash` | Content fingerprints for changed-history guards; not substitutes for grain checks |

## Metrics in v_category_metrics

All metrics group by `run_id` and main analytical category. The denominator is never a sum across multiple extractions.

| Metric | Definition |
|---|---|
| `businesses` | Number of snapshot rows in the group |
| `businesses_with_reviews` | Count where `review_count > 0`; does not mean favorable reviews |
| `businesses_with_zero_reviews` | Count where `review_count = 0`, explicitly reported |
| `businesses_missing_review_count` | Count where `review_count IS NULL` |
| `pct_with_reviews` | 100 × businesses with reviews / all businesses in the category |
| `pct_missing_review_count` | 100 × missing counts / all businesses in the category |
| `pct_with_reviews_among_reported` | 100 × businesses with reviews / businesses with a non-NULL count; NULL if none report it |
| `pct_of_run_businesses_with_reviews` | Category businesses with reviews / all businesses with reviews in that run, × 100 |
| `median_review_count` | Median of reported counts, including explicit zeros; excludes NULL |
| `mean_review_count` | Arithmetic mean of reported counts, including explicit zeros; excludes NULL |
| `median_business_rating` | Median of reported establishment ratings |
| `mean_business_rating` | Arithmetic mean of reported establishment ratings, each establishment equally weighted |
| `businesses_with_at_least_10_reviews` / `...50_reviews` | Inclusive exploratory thresholds, not validated commercial scores |

The first three count categories partition the group: with reviews + explicit zero + missing = total. Their percentages sum to approximately 100% after rounding.

`pct_with_reviews_among_reported` equals 100% in this baseline because all reported counts are greater than zero. This does not establish that businesses with absent counts have zero reviews.

The mean of business ratings is not a reconstruction of the average individual review across establishments. Review counts must not be summed across dates as though they were new reviews per period. Compare snapshots explicitly when historical runs exist.

## Dashboard helper fields

`with_reviews_flag` is 1 for `review_count > 0`, otherwise 0. Its AVG is the within-selection share of establishments with reported reviews. Missing counts remain in the denominator; the flag is not an imputed source review count. Phone, website and hours flags work analogously for reported availability.

`review_count_band` contains 0, 1–10, 11–50, 51–200, 201–1,000, over 1,000 and Not reported as separate groups. This avoids an awkward slider dominated by outliers without excluding those establishments.

Requested-category bridge analysis overlaps by design. Do not sum bridge group counts to obtain the global number of establishments; use the snapshot fact instead.
