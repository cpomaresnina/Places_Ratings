# Execute bounded Google Places requests and persist each successful response.
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

from .search_plan import build_grid, build_payload, pending_grid_points
from .storage import load_saved_queries, save_extraction

def run_pending_grid(config, area, category, snapshot_date, api_key_provider, *, confirm_live_calls=False):
    """Execute one bounded, single-user extraction; resume from verified files."""
    LANDING_PATH = Path(config['landing_path'])
    ENDPOINT = config['endpoint']
    FIELD_MASK = ','.join(config['field_mask'])
    mapping_version = config['mapping_version']
    if not isinstance(mapping_version, str) or not mapping_version.strip():
        raise ValueError('mapping_version must be a non-empty string.')
    STUDY_AREA = area
    SELECTED_CATEGORY = category
    SNAPSHOT_DATE_UTC = snapshot_date
    STUDY_CENTER = config['areas'][area]['center']
    STUDY_RADIUS_METERS = config['areas'][area]['study_radius_meters']
    GRID_SPACING_METERS = config['grid_spacing_meters']
    CATEGORY_TYPES = config['categories']
    MAX_CALLS_PER_RUN = config['max_calls_per_run']
    SECONDS_BETWEEN_CALLS = config['seconds_between_calls']
    if not isinstance(MAX_CALLS_PER_RUN, int) or not 1 <= MAX_CALLS_PER_RUN <= 9:
        raise ValueError('max_calls_per_run must be an integer between 1 and 9.')
    if SECONDS_BETWEEN_CALLS < 0:
        raise ValueError('seconds_between_calls must not be negative.')
    search_points = build_grid(STUDY_CENTER, area, GRID_SPACING_METERS, config['query_radius_meters'])
    if not confirm_live_calls:
        print('No API calls made. Live extraction requires explicit confirmation.')
        return
    if datetime.now(timezone.utc).date().isoformat() != SNAPSHOT_DATE_UTC:
        raise ValueError('Set SNAPSHOT_DATE_UTC to today in UTC before a live run.')

    queries, errors = load_saved_queries(LANDING_PATH)
    if errors:
        raise RuntimeError('Review unreadable files before running extraction.')
    pending = pending_grid_points(
        queries, STUDY_AREA, SELECTED_CATEGORY, SNAPSHOT_DATE_UTC, search_points,
        CATEGORY_TYPES[category], FIELD_MASK, ENDPOINT, mapping_version
    )
    if not pending:
        print('Pending queries: 0. No API calls made.')
        return

    # Load credentials only when an explicitly requested live run needs them.
    api_key = api_key_provider()
    attempted, saved = 0, 0
    try:
        for point in pending[:MAX_CALLS_PER_RUN]:
            if attempted:
                time.sleep(SECONDS_BETWEEN_CALLS)
            payload = build_payload(CATEGORY_TYPES[SELECTED_CATEGORY], point)
            extracted_at = datetime.now(timezone.utc).isoformat()
            if extracted_at[:10] != SNAPSHOT_DATE_UTC:
                print('Stopped: UTC date changed during the run.')
                break
            attempted += 1
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
                if not isinstance(response_data, dict):
                    raise ValueError('Invalid response structure')
                places = response_data.get('places', [])
                if 'error' in response_data or not isinstance(places, list) or not all(isinstance(p, dict) for p in places):
                    raise ValueError('Invalid response structure')
                extraction = {
                    'mapping_version': mapping_version,
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
                    extraction, SELECTED_CATEGORY, STUDY_AREA, 'observed', LANDING_PATH
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
