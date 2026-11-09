# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Place Details — Context enrichment
# MAGIC Enrich unique IDs from discovery Bronze with website, regular hours and a Maps link.
# MAGIC This is a separate response stream, not another geographic search.
# MAGIC Default Run all only reads Bronze and the enrichment inventory. Pilot limit: one call.
# MAGIC Responses live under `enrichment/`, outside `observed/`, so the existing discovery
# MAGIC inventory and Bronze loader are unaffected. A separate Bronze load will follow.
# MAGIC Missing fields are retained as absent, not interpreted as proof of no website or hours.
# MAGIC This one-time campaign resumes across dates; choose a new version for a future refresh.
# MAGIC One active execution at a time. Timeouts or save failures can still require repeated calls.

# COMMAND ----------

# Locate the repository and import the dedicated enrichment module.
import importlib
import json
import sys
from pathlib import Path

working_directory = Path.cwd().resolve()
REPO_ROOT = next((p for p in [working_directory, *working_directory.parents]
                  if (p / 'config/place_details.json').is_file()
                  and (p / 'src/ingestion').is_dir()), None)
if REPO_ROOT is None:
    raise RuntimeError('Enrichment configuration not found in this Git folder.')
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
from src.ingestion import place_details
importlib.reload(place_details)

# COMMAND ----------

# Load configuration and keep the pilot disabled until its plan is reviewed.
with (REPO_ROOT / 'config/places_search.json').open(encoding='utf-8') as file:
    discovery_config = json.load(file)
with (REPO_ROOT / 'config/place_details.json').open(encoding='utf-8') as file:
    details_config = json.load(file)

RUN_LIVE_ENRICHMENT = False

# COMMAND ----------

# Read unique candidate IDs from persistent Bronze without relying on Silver temp views.
candidate_rows = spark.sql('''
    SELECT DISTINCT place.id AS place_id
    FROM places_ratings.bronze.api_responses AS source
    LATERAL VIEW explode(
        from_json(
            get_json_object(source.raw_json, '$.extraction.response_data.places'),
            'ARRAY<STRUCT<id: STRING>>'
        )
    ) expanded AS place
    WHERE source.mapping_version = :mapping_version
      AND to_date(source.extracted_at_utc) = CAST(:discovery_date AS DATE)
      AND place.id IS NOT NULL
      AND trim(place.id) <> ''
    ORDER BY place_id
''', args={
    'mapping_version': details_config['discovery_mapping_version'],
    'discovery_date': details_config['discovery_date_utc']
}).collect()

place_ids = [row['place_id'] for row in candidate_rows]
if not place_ids:
    raise RuntimeError('No candidate IDs found. Check the discovery scope and date.')

# COMMAND ----------

# Display pending enrichment work and the maximum calls for this execution.
def show_details_plan():
    completed, errors = place_details.load_details_inventory(
        discovery_config['landing_path'], details_config
    )
    candidate_set = set(place_ids)
    pending = candidate_set - set(completed)
    print(f'Unique candidate businesses: {len(candidate_set)}')
    print(f'Already enriched: {len(candidate_set & set(completed))}')
    print(f'Pending businesses: {len(pending)}')
    print(f'Files requiring review: {len(errors)}')
    print(f"Fields: {', '.join(details_config['field_mask'])}")
    print(f"Maximum calls this execution: {min(len(pending), details_config['max_calls_per_run'])}")
    for error in errors:
        print(error)
    return errors

inventory_errors = show_details_plan()

# COMMAND ----------

# Execute only an explicitly enabled pilot and verify progress from persisted files.
try:
    if inventory_errors:
        raise RuntimeError('Review invalid enrichment files before execution.')
    if RUN_LIVE_ENRICHMENT:
        place_details.run_details_enrichment(
            place_ids, discovery_config['landing_path'], details_config,
            api_key_provider=lambda: dbutils.secrets.get(
                scope=discovery_config['secret_scope'], key=discovery_config['secret_key']
            ),
            enabled=True
        )
        show_details_plan()
    else:
        print('Plan only: no Google calls and no files written.')
finally:
    RUN_LIVE_ENRICHMENT = False

# COMMAND ----------

# MAGIC %md
# MAGIC ## Before a live run
# MAGIC Review the unique-ID count and current Place Details Enterprise pricing. Website
# MAGIC and regular opening hours trigger that tier. Do not assume that account credits
# MAGIC or monthly free allowances are unused. The pilot is capped at one request.
# MAGIC Set RUN_LIVE_ENRICHMENT to True only deliberately; restore the source value to
# MAGIC False before committing. This does not modify any existing discovery responses.
# MAGIC Source: https://developers.google.com/maps/documentation/places/web-service/place-details
# MAGIC Pricing: https://developers.google.com/maps/billing-and-pricing/pricing

# COMMAND ----------

# Measure field availability across persisted enrichment responses.

completed, errors = place_details.load_details_inventory(
    discovery_config["landing_path"],
    details_config
)

if errors:
    raise RuntimeError("Review invalid enrichment files first.")

responses = []

for saved_path in completed.values():
    with open(saved_path, "r", encoding="utf-8") as file:
        document = json.load(file)

    responses.append(document["extraction"]["response_data"])

print(f"Responses inspected: {len(responses)}")

for field in details_config["field_mask"]:
    reported_count = sum(
        field in response and response[field] is not None
        for response in responses
    )

    print(f"{field}: {reported_count}/{len(responses)}")

review_links = sum(
    bool(
        (response.get("googleMapsLinks") or {}).get("writeAReviewUri")
    )
    for response in responses
)

print(f"writeAReviewUri: {review_links}/{len(responses)}")