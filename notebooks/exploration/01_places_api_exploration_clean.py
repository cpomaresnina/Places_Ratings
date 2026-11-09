# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Google Places API — Exploration
# MAGIC
# MAGIC ## Purpose
# MAGIC Inspect partial business search results and evaluate extraction coverage.
# MAGIC This is an exploratory notebook, not the final production pipeline.
# MAGIC
# MAGIC ## Execution order
# MAGIC Run the notebook from top to bottom to load saved files and calculate diagnostics.
# MAGIC **Run all does not call Google, retrieve the API key, or write files.**
# MAGIC Live extraction is a separate manual action described below.
# MAGIC
# MAGIC ## Current scope
# MAGIC - Sopocachi: 600-meter study radius around the candidate Plaza Abaroa center.
# MAGIC - Nine search centers: 300-meter spacing and 350-meter query radius.
# MAGIC - Categories implemented here: cafés and restaurants.
# MAGIC - Calacoto, Ciudad Satélite and other categories await verified configuration.
# MAGIC
# MAGIC ## Storage
# MAGIC Observed responses are stored outside Git in an external Unity Catalog Volume.
# MAGIC Changing the landing path does not migrate existing data; an independent backup
# MAGIC and a migration check are still pending. No Google API key belongs in this file.

# COMMAND ----------

# Import dependencies and define explicit exploration settings.
import json
import math
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from statistics import median
from uuid import uuid4

import requests

LANDING_PATH = Path('/Volumes/places_ratings/landing/source_files')
ENDPOINT = 'https://places.googleapis.com/v1/places:searchNearby'
FIELD_MASK = (
    'places.id,places.displayName,places.primaryType,places.types,'
    'places.formattedAddress,places.location,places.businessStatus,'
    'places.rating,places.userRatingCount'
)
SECRET_SCOPE = 'places-explorer'
SECRET_KEY = 'google-places-api-key'

STUDY_AREA = 'sopocachi'
STUDY_CENTER = {'latitude': -16.5108331, 'longitude': -68.1265597}
STUDY_RADIUS_METERS = 600
GRID_SPACING_METERS = 300
QUERY_RADIUS_METERS = 350
CATEGORY_TYPES = {'cafes': ['cafe'], 'restaurants': ['restaurant']}
SELECTED_CATEGORY = 'restaurants'

# Restrict resume matching to the UTC extraction date, not all historical files.
# To start a new snapshot on another date, update this value deliberately.
SNAPSHOT_DATE_UTC = '2026-11-09'
MAX_CALLS_PER_RUN = 9
SECONDS_BETWEEN_CALLS = 12

# COMMAND ----------

# Build the geographic search grid and a shared request payload.
def build_grid(center, area, spacing_meters, radius_meters):
    points = []
    earth_radius = 6_371_000
    for north in [-spacing_meters, 0, spacing_meters]:
        for east in [-spacing_meters, 0, spacing_meters]:
            points.append({
                'point_id': f'{area}_{len(points) + 1:02d}',
                'latitude': center['latitude'] + math.degrees(north / earth_radius),
                'longitude': center['longitude'] + math.degrees(
                    east / (earth_radius * math.cos(math.radians(center['latitude'])))
                ),
                'radius_meters': radius_meters
            })
    return points


def build_payload(included_types, point):
    # maxResultCount remains omitted to preserve the tested default behavior.
    return {
        'includedTypes': list(included_types),
        'rankPreference': 'DISTANCE',
        'locationRestriction': {'circle': {
            'center': {'latitude': point['latitude'], 'longitude': point['longitude']},
            'radius': point['radius_meters']
        }}
    }


def query_signature(area, category, parameters, field_mask):
    # Normalize only known equivalent defaults; retain other request options.
    normalized = json.loads(json.dumps(parameters))
    normalized.setdefault('maxResultCount', 20)
    normalized.setdefault('rankPreference', 'POPULARITY')
    normalized['includedTypes'] = sorted(normalized.get('includedTypes', []))
    center = normalized['locationRestriction']['circle']['center']
    center['latitude'] = round(center['latitude'], 7)
    center['longitude'] = round(center['longitude'], 7)
    fields = ','.join(sorted(field_mask.split(',')))
    return (area, category, fields, json.dumps(normalized, sort_keys=True))


search_points = build_grid(
    STUDY_CENTER, STUDY_AREA, GRID_SPACING_METERS, QUERY_RADIUS_METERS
)
print(f'Grid points: {len(search_points)}')

# COMMAND ----------

# Read persisted responses and reject invalid files before resume decisions.
def load_saved_queries(landing_path):
    observed_path = Path(landing_path) / 'observed'
    if not observed_path.is_dir():
        raise FileNotFoundError(f'Observed directory not accessible: {observed_path}')
    queries, errors = [], []
    for file_path in sorted(observed_path.rglob('*.json')):
        try:
            with file_path.open('r', encoding='utf-8') as file:
                document = json.load(file)
            if document.get('data_kind') != 'observed':
                raise ValueError('Unexpected data kind')
            extraction = document['extraction']
            parameters = extraction['request_parameters']
            circle = parameters['locationRestriction']['circle']
            float(circle['center']['latitude'])
            float(circle['center']['longitude'])
            float(circle['radius'])
            extracted_at = datetime.fromisoformat(extraction['extracted_at_utc'])
            if extracted_at.tzinfo is None:
                raise ValueError('Extraction timestamp has no timezone')
            response_data = extraction['response_data']
            if not isinstance(response_data, dict) or 'error' in response_data:
                raise ValueError('Invalid response body')
            places = response_data.get('places', [])
            if not isinstance(places, list) or not all(isinstance(p, dict) for p in places):
                raise ValueError('Invalid places list')
            queries.append({
                'file_path': str(file_path),
                'study_area': document['study_area'],
                'category': document['requested_category'],
                'extracted_at_utc': extracted_at.astimezone(timezone.utc).isoformat(),
                'request_parameters': parameters,
                # Compatibility: the first ten saved files used this exact mask.
                # Do not generalize this fallback to files from other collectors.
                'field_mask': extraction.get('field_mask', FIELD_MASK),
                'endpoint': extraction.get('endpoint', ENDPOINT),
                'document': document,
                'returned_count': len(places),
                'potentially_truncated': len(places) == 20
            })
        except (OSError, ValueError, KeyError, TypeError, AttributeError) as error:
            errors.append({'file_path': str(file_path), 'error_type': type(error).__name__})
    return queries, errors


def select_snapshot(queries, area, category, snapshot_date):
    return [q for q in queries if
            q['study_area'] == area and q['category'] == category
            and q['extracted_at_utc'][:10] == snapshot_date
            and q['endpoint'] == ENDPOINT]


def pending_grid_points(queries, area, category, snapshot_date, points):
    completed = {
        query_signature(area, category, q['request_parameters'], q['field_mask'])
        for q in select_snapshot(queries, area, category, snapshot_date)
    }
    return [p for p in points if query_signature(
        area, category, build_payload(CATEGORY_TYPES[category], p), FIELD_MASK
    ) not in completed]

# COMMAND ----------

# Define response persistence and verify each newly written file.
def save_extraction(extraction, category, study_area, data_kind):
    extraction_id = uuid4().hex
    saved_at = datetime.now(timezone.utc)

    output_directory = (
        LANDING_PATH
        / data_kind
        / study_area
        / category
        / saved_at.strftime("%Y-%m-%d")
    )
    output_directory.mkdir(parents=True, exist_ok=True)

    output_path = output_directory / f"{extraction_id}.json"

    document = {
        "extraction_id": extraction_id,
        "saved_at_utc": saved_at.isoformat(),
        "data_kind": data_kind,
        "study_area": study_area,
        "requested_category": category,
        "extraction": extraction
    }

    # Exclusive creation prevents overwriting an existing file.
    with output_path.open("x", encoding="utf-8") as file:
        json.dump(document, file, ensure_ascii=False, indent=2)

    with output_path.open("r", encoding="utf-8") as file:
        recovered = json.load(file)
    if recovered != document:
        raise ValueError("Saved response verification failed.")
    return str(output_path)

# COMMAND ----------

# Define guarded extraction with bounded calls and disk-based resume checks.
def run_pending_grid(*, confirm_live_calls=False):
    if not confirm_live_calls:
        print('No API calls made. Live extraction requires explicit confirmation.')
        return
    if datetime.now(timezone.utc).date().isoformat() != SNAPSHOT_DATE_UTC:
        raise ValueError('Set SNAPSHOT_DATE_UTC to today in UTC before a live run.')

    queries, errors = load_saved_queries(LANDING_PATH)
    if errors:
        raise RuntimeError('Review unreadable files before running extraction.')
    pending = pending_grid_points(
        queries, STUDY_AREA, SELECTED_CATEGORY, SNAPSHOT_DATE_UTC, search_points
    )
    if not pending:
        print('Pending queries: 0. No API calls made.')
        return

    # Load credentials only when an explicitly requested live run needs them.
    api_key = dbutils.secrets.get(scope=SECRET_SCOPE, key=SECRET_KEY)
    attempted, saved = 0, 0
    try:
        for point in pending[:MAX_CALLS_PER_RUN]:
            if attempted:
                time.sleep(SECONDS_BETWEEN_CALLS)
            payload = build_payload(CATEGORY_TYPES[SELECTED_CATEGORY], point)
            attempted += 1
            extracted_at = datetime.now(timezone.utc).isoformat()
            if extracted_at[:10] != SNAPSHOT_DATE_UTC:
                print('Stopped: UTC date changed during the run.')
                break
            try:
                response = requests.post(
                    ENDPOINT,
                    headers={
                        'Content-Type': 'application/json',
                        'X-Goog-Api-Key': api_key,
                        'X-Goog-FieldMask': FIELD_MASK
                    },
                    json=payload, timeout=30
                )
                if not response.ok:
                    print(f"Stopped at {point['point_id']} | HTTP {response.status_code}")
                    break
                response_data = response.json()
                places = response_data.get('places', [])
                if 'error' in response_data or not isinstance(places, list):
                    raise ValueError('Invalid response structure')
                extraction = {
                    'extracted_at_utc': extracted_at,
                    'request_parameters': payload,
                    'endpoint': ENDPOINT,
                    'field_mask': FIELD_MASK,
                    'http_status': response.status_code,
                    'search_context': {
                        'point_id': point['point_id'],
                        'study_center': STUDY_CENTER.copy(),
                        'study_radius_meters': STUDY_RADIUS_METERS,
                        'grid_spacing_meters': GRID_SPACING_METERS,
                        'snapshot_date_utc': SNAPSHOT_DATE_UTC
                    },
                    'response_data': response_data
                }
                path = save_extraction(
                    extraction, SELECTED_CATEGORY, STUDY_AREA, 'observed'
                )
            except Exception as error:
                # Do not print request headers, credentials, or full exceptions.
                print(f"Stopped at {point['point_id']} | {type(error).__name__}")
                print('Inspect storage before retrying; a response may already exist.')
                break
            saved += 1
            print(f"{point['point_id']} | Returned: {len(places)} | Saved: {path}")
    finally:
        del api_key
    print(f'Calls attempted: {attempted}; responses saved and verified: {saved}')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Manual extraction — disabled by default
# MAGIC
# MAGIC Run all only defines `run_pending_grid`; it never invokes it.
# MAGIC To deliberately extract data, select the category and current UTC snapshot date
# MAGIC in configuration, then add and execute a temporary cell manually:
# MAGIC
# MAGIC ```python
# MAGIC # Explicitly execute a bounded live extraction and persist every response.
# MAGIC run_pending_grid(confirm_live_calls=True)
# MAGIC ```
# MAGIC
# MAGIC **Remove that temporary execution cell afterwards.** Do not leave it in the
# MAGIC saved notebook or a subsequent Run all could invoke it. The function refreshes
# MAGIC the disk inventory each time, skips matching saved queries for the selected date,
# MAGIC and makes at most nine calls without automatic retries. This is a single-user
# MAGIC exploration runner, not a concurrency-safe workflow or an exactly-once guarantee.
# MAGIC
# MAGIC A timeout or failed save can still require a repeated billable request. Inspect
# MAGIC the saved files and reported error before retrying. New dates intentionally
# MAGIC create new snapshots; an old snapshot does not satisfy a future refresh.

# COMMAND ----------

# Refresh the disk inventory and show pending searches without API calls.
saved_queries, unreadable_files = load_saved_queries(LANDING_PATH)
print(f'Saved queries found: {len(saved_queries)}')
print(f'Files requiring review: {len(unreadable_files)}')
for error in unreadable_files:
    print(error)
pending_points = pending_grid_points(
    saved_queries, STUDY_AREA, SELECTED_CATEGORY, SNAPSHOT_DATE_UTC, search_points
)
print(f'Selected category: {SELECTED_CATEGORY}')
print(f'Snapshot date (UTC): {SNAPSHOT_DATE_UTC}')
print(f'Grid points: {len(search_points)}; pending queries: {len(pending_points)}')

# COMMAND ----------

# Define straight-line distance for exploratory geographic checks.
def distance_meters(lat1, lon1, lat2, lon2):
    earth_radius = 6_371_000

    lat1, lon1, lat2, lon2 = map(
        math.radians, [lat1, lon1, lat2, lon2]
    )

    a = (
        math.sin((lat2 - lat1) / 2) ** 2
        + math.cos(lat1)
        * math.cos(lat2)
        * math.sin((lon2 - lon1) / 2) ** 2
    )

    return 2 * earth_radius * math.asin(math.sqrt(min(1, a)))

# COMMAND ----------

# Summarize saved snapshot data while retaining original response documents.
def summarize_snapshot(queries, area, category, snapshot_date):
    selected = select_snapshot(queries, area, category, snapshot_date)
    # Keep one latest response per equivalent query if files were saved twice.
    latest_queries = {}
    for query in sorted(selected, key=lambda q: (q['extracted_at_utc'], q['file_path'])):
        signature = query_signature(area, category, query['request_parameters'], query['field_mask'])
        latest_queries[signature] = query
    selected = list(latest_queries.values())
    unique, total, missing_ids = {}, 0, 0
    for query in sorted(selected, key=lambda q: (q['extracted_at_utc'], q['file_path'])):
        for place in query['document']['extraction']['response_data'].get('places', []):
            total += 1
            if not place.get('id'):
                missing_ids += 1
            else:
                unique[place['id']] = place
    inside, outside, missing_location = [], 0, 0
    for place in unique.values():
        location = place.get('location') or {}
        lat, lon = location.get('latitude'), location.get('longitude')
        if lat is None or lon is None:
            missing_location += 1
        elif distance_meters(STUDY_CENTER['latitude'], STUDY_CENTER['longitude'], lat, lon) <= STUDY_RADIUS_METERS:
            inside.append(place)
        else:
            outside += 1
    counts = [p['userRatingCount'] for p in inside if p.get('userRatingCount') is not None]
    return {
        'category': category,
        'matching_files': len(select_snapshot(queries, area, category, snapshot_date)),
        'distinct_queries': len(selected),
        'total_appearances': total,
        'unique_place_ids': len(unique),
        'repeated_appearances': total - missing_ids - len(unique),
        'appearances_without_id': missing_ids,
        'inside_study_area': len(inside),
        'outside_study_area': outside,
        'missing_location': missing_location,
        'potentially_truncated_queries': sum(q['potentially_truncated'] for q in selected),
        'missing_rating': sum(p.get('rating') is None for p in inside),
        'missing_review_count': len(inside) - len(counts),
        'median_reported_review_count': median(counts) if counts else None,
        'primary_types': dict(Counter(p.get('primaryType') or 'NOT_REPORTED' for p in inside))
    }


for category in CATEGORY_TYPES:
    print(json.dumps(summarize_snapshot(
        saved_queries, STUDY_AREA, category, SNAPSHOT_DATE_UTC
    ), indent=2, ensure_ascii=False))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Historical exploration — User-reported findings
# MAGIC
# MAGIC ### Scope
# MAGIC - Area: within 600 meters of Plaza Abaroa, La Paz.
# MAGIC - Categories tested: cafés and restaurants.
# MAGIC - Method: Nearby Search ranked by distance, using a 3 × 3 grid.
# MAGIC - Grid spacing: 300 meters; search radius per grid point: 350 meters.
# MAGIC - Cafés also included an initial query centered on Plaza Abaroa.
# MAGIC
# MAGIC ### Observed results
# MAGIC
# MAGIC | Metric | Cafés | Restaurants |
# MAGIC |---|---:|---:|
# MAGIC | API responses | 10 | 9 |
# MAGIC | Returned appearances | 198 | 180 |
# MAGIC | Unique places within the study area | 111 | 172 |
# MAGIC | Places with a reported review count | 82 | 149 |
# MAGIC | Places missing a review count | 29 | 23 |
# MAGIC | Median reported review count | 11.5 | 11 |
# MAGIC | Minimum reported review count | 1 | 1 |
# MAGIC | Maximum reported review count | 2,852 | 10,634 |
# MAGIC | Responses reaching the 20-result limit | 9 | 9 |
# MAGIC
# MAGIC Six places appeared in both categories, giving 277 unique places
# MAGIC within the study area across the two categories.
# MAGIC
# MAGIC ### Data handling decisions
# MAGIC - Preserve original place types and query categories separately.
# MAGIC - Identify repeated appearances using place_id.
# MAGIC - Keep missing review counts as null, rather than replacing them with zero.
# MAGIC - Separate records outside the study radius from the analytical sample.
# MAGIC - Preserve query parameters and extraction timestamps for traceability.
# MAGIC
# MAGIC ### Limitations
# MAGIC These are partial API search results, not a census of businesses.
# MAGIC Reaching the result limit indicates potentially incomplete coverage.
# MAGIC The observed medians describe only records with reported review counts.
# MAGIC The results do not yet test the hypothesis against other business categories
# MAGIC or establish demand for an NFC solution.
# MAGIC
# MAGIC ### Next stage
# MAGIC Prepare reusable extraction configuration, response persistence,
# MAGIC and execution controls before expanding to other categories and areas.
# MAGIC
# MAGIC
# MAGIC
# MAGIC These numbers were reported before the session lost its in-memory responses.
# MAGIC The original restaurant JSON responses were not persisted. The later café
# MAGIC extraction was saved and reloaded: 10 files, 198 appearances and 115 unique IDs.
# MAGIC Matching aggregate counts do not prove identical underlying records.
# MAGIC This table is historical documentation, not a result recomputed from current files.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Remaining work
# MAGIC - Persist restaurant queries before further comparison.
# MAGIC - Verify centers for Calacoto and Ciudad Satélite.
# MAGIC - Resolve other category mappings, especially dog grooming.
# MAGIC - Review additional context fields and pricing before changing the field mask.
# MAGIC - Define permission-compatible retention and an independent backup.
# MAGIC - Move reusable code into ingestion modules and separate Bronze/Silver/Gold later.
# MAGIC - Keep API/storage setup in a separate setup notebook; do not prompt for or overwrite secrets here.
# MAGIC
# MAGIC No live Google or S3 connection was exercised while preparing this revised file.