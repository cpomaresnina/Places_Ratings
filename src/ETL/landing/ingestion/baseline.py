# Plan, persist and resume a complete Places baseline without changing earlier runs.
import hashlib
import json
import re
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

from .search_plan import build_grid, build_payload
from .storage import save_extraction


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False)


def definition(config):
    """Execution limits may change; the data selection cannot change within a run."""
    return {key: config[key] for key in [
        'run_id', 'mapping_version', 'field_set_version', 'endpoint',
        'field_mask', 'areas', 'categories', 'grid_spacing_meters', 'query_radius_meters'
    ]}


def fingerprint(config):
    return hashlib.sha256(canonical(definition(config)).encode('utf-8')).hexdigest()


def run_directory(config):
    run_id = config['run_id']
    if not re.fullmatch(r'[A-Za-z0-9_-]+', run_id):
        raise ValueError('run_id must contain only letters, numbers, hyphens or underscores.')
    return Path(config['landing_path']) / 'baselines' / run_id


def build_plan(config):
    plan = []
    for area, area_config in config['areas'].items():
        points = build_grid(area_config['center'], area,
                            config['grid_spacing_meters'], config['query_radius_meters'])
        for category, types in config['categories'].items():
            for point in points:
                payload = build_payload(types, point)
                identity = {'area': area, 'category': category, 'payload': payload}
                key = hashlib.sha256(canonical(identity).encode('utf-8')).hexdigest()
                plan.append({'query_key': key, 'area': area, 'category': category,
                             'point': point, 'payload': payload})
    return plan


def inspect_run(config):
    """Read the manifest and JSON files; never create files or call Google."""
    base = Path(config['landing_path'])
    if not base.is_dir():
        raise FileNotFoundError('Landing Volume is not accessible.')
    directory = run_directory(config)
    manifest_path = directory / 'run_manifest.json'
    observed = directory / 'observed'
    plan = build_plan(config)
    expected = {q['query_key']: q for q in plan}
    current_fingerprint = fingerprint(config)
    if manifest_path.exists():
        with manifest_path.open(encoding='utf-8') as file:
            manifest = json.load(file)
        if (manifest['config_fingerprint'] != current_fingerprint
                or manifest['definition'] != definition(config)):
            raise ValueError('Run definition changed. Restore it or select a new run_id.')
    elif observed.exists() and any(observed.rglob('*.json')):
        raise ValueError('Response files exist without a manifest. Review before resuming.')

    completed, errors, duplicate_responses = {}, [], 0
    places_seen = set()
    reported_fields = {field.removeprefix('places.'): 0 for field in config['field_mask']}
    appearances = saturated = 0
    files = sorted(observed.rglob('*.json')) if observed.exists() else []
    for path in files:
        try:
            with path.open(encoding='utf-8') as file:
                document = json.load(file)
            extraction = document['extraction']
            key = extraction['query_key']
            query = expected[key]
            timestamp = datetime.fromisoformat(extraction['extracted_at_utc'])
            if timestamp.tzinfo is None:
                raise ValueError('Missing extraction timezone.')
            if (document['data_kind'] != 'observed'
                    or document['study_area'] != query['area']
                    or document['requested_category'] != query['category']
                    or extraction['run_id'] != config['run_id']
                    or extraction['config_fingerprint'] != current_fingerprint
                    or extraction['http_status'] != 200
                    or extraction['field_mask'] != ','.join(config['field_mask'])
                    or extraction['endpoint'] != config['endpoint']
                    or extraction['request_parameters'] != query['payload']):
                raise ValueError('Unexpected extraction metadata.')
            body = extraction['response_data']
            if not isinstance(body, dict) or 'error' in body:
                raise ValueError('Invalid response body.')
            places = body.get('places', [])
            if not isinstance(places, list) or not all(isinstance(p, dict) for p in places):
                raise ValueError('Invalid places array.')
            if key in completed:
                duplicate_responses += 1
            completed[key] = str(path)
            saturated += len(places) == 20
            appearances += len(places)
            for place in places:
                if place.get('id'):
                    places_seen.add(place['id'])
                for field in reported_fields:
                    reported_fields[field] += field in place and place[field] is not None
        except (OSError, ValueError, KeyError, TypeError, AttributeError) as error:
            errors.append({'file_path': str(path), 'error_type': type(error).__name__})
    return {
        'run_id': config['run_id'], 'planned_queries': len(plan),
        'completed_queries': len(completed),
        'pending': [q for q in plan if q['query_key'] not in completed],
        'saved_files': len(files), 'duplicate_response_files': duplicate_responses,
        'errors': errors, 'potentially_truncated_responses': saturated,
        'business_appearances': appearances, 'unique_place_ids': len(places_seen),
        'reported_fields': reported_fields
    }


def execute_baseline(config, api_key_provider, *, enabled=False):
    """Run one bounded batch and persist every response before advancing."""
    if not enabled:
        print('Plan only: no Google calls and no files written.')
        return {'attempted': 0, 'saved': 0}
    limit = config['max_calls_per_run']
    if type(limit) is not int or not 1 <= limit <= 315:
        raise ValueError('max_calls_per_run must be between 1 and 315.')
    delay = config['seconds_between_calls']
    if delay < 0:
        raise ValueError('seconds_between_calls must not be negative.')
    status = inspect_run(config)
    if status['errors'] or status['duplicate_response_files']:
        raise RuntimeError('Review invalid or duplicate response files before continuing.')
    if not status['pending']:
        print('Pending queries: 0. No Google calls made.')
        return {'attempted': 0, 'saved': 0}
    directory = run_directory(config)
    directory.mkdir(parents=True, exist_ok=True)
    manifest_path = directory / 'run_manifest.json'
    if not manifest_path.exists():
        manifest = {
            'created_at_utc': datetime.now(timezone.utc).isoformat(),
            'config_fingerprint': fingerprint(config), 'definition': definition(config)
        }
        with manifest_path.open('x', encoding='utf-8') as file:
            json.dump(manifest, file, indent=2, ensure_ascii=False)
        with manifest_path.open(encoding='utf-8') as file:
            if json.load(file) != manifest:
                raise ValueError('Manifest verification failed.')
    api_key = api_key_provider()
    attempted = saved = 0
    try:
        for query in status['pending'][:limit]:
            if attempted:
                time.sleep(delay)
            attempted += 1
            timestamp = datetime.now(timezone.utc).isoformat()
            try:
                response = requests.post(
                    config['endpoint'],
                    headers={'Content-Type': 'application/json',
                             'X-Goog-Api-Key': api_key,
                             'X-Goog-FieldMask': ','.join(config['field_mask'])},
                    json=query['payload'], timeout=30
                )
                if response.status_code != 200:
                    print(f"Stopped: {query['point']['point_id']} | HTTP {response.status_code}")
                    break
                body = response.json()
                if not isinstance(body, dict) or 'error' in body:
                    raise ValueError('Invalid response body.')
                places = body.get('places', [])
                if not isinstance(places, list) or not all(isinstance(p, dict) for p in places):
                    raise ValueError('Invalid places array.')
                area_config = config['areas'][query['area']]
                extraction = {
                    'run_id': config['run_id'], 'query_key': query['query_key'],
                    'config_fingerprint': fingerprint(config),
                    'mapping_version': config['mapping_version'],
                    'field_set_version': config['field_set_version'],
                    'extracted_at_utc': timestamp,
                    'request_parameters': query['payload'],
                    'endpoint': config['endpoint'],
                    'field_mask': ','.join(config['field_mask']), 'http_status': 200,
                    'search_context': {
                        'point_id': query['point']['point_id'],
                        'study_center': area_config['center'],
                        'study_radius_meters': area_config['study_radius_meters'],
                        'parent_area': area_config.get('parent_area', query['area']),
                        'city': area_config.get('city', 'la_paz'),
                        'grid_spacing_meters': config['grid_spacing_meters']
                    },
                    'response_data': body
                }
                path = save_extraction(extraction, query['category'], query['area'],
                                       'observed', directory)
            except Exception as error:
                print(f"Stopped: {query['area']} / {query['category']} | {type(error).__name__}")
                print('Inspect stored files before retrying. No automatic retries performed.')
                break
            saved += 1
            print(f"{query['point']['point_id']} | {query['category']} | Returned: {len(places)} | Saved: {path}")
    finally:
        del api_key
    print(f'Calls attempted: {attempted}; responses saved and verified: {saved}')
    return {'attempted': attempted, 'saved': saved}
