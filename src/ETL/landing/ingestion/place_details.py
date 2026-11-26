# Enrich unique place IDs, preserving raw responses separately from discovery files.
import json
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote
from uuid import uuid4

import requests


def details_directory(landing_path, version):
    if not re.fullmatch(r'[A-Za-z0-9_-]+', version):
        raise ValueError('Invalid enrichment version.')
    return Path(landing_path) / 'enrichment' / 'place_details' / version


def normalized_mask(fields):
    return ','.join(sorted(fields))


def load_details_inventory(landing_path, settings):
    """Only verified responses with a matching request definition count as completed."""
    directory = details_directory(landing_path, settings['enrichment_version'])
    completed, errors = {}, []
    if not directory.exists():
        return completed, errors
    for path in sorted(directory.rglob('*.json')):
        try:
            with path.open(encoding='utf-8') as file:
                document = json.load(file)
            extraction = document['extraction']
            request = extraction['request_parameters']
            response = extraction['response_data']
            place_id = request['place_id']
            if (document['source_kind'] != 'place_details'
                    or extraction['enrichment_version'] != settings['enrichment_version']
                    or extraction['http_status'] != 200
                    or response.get('id') != place_id or 'error' in response):
                raise ValueError('Invalid details document.')
            if (extraction['endpoint'] != settings['endpoint']
                    or normalized_mask(request['fields']) != normalized_mask(settings['field_mask'])):
                continue
            completed[place_id] = str(path)
        except (OSError, ValueError, KeyError, TypeError, AttributeError) as error:
            errors.append({'file_path': str(path), 'error_type': type(error).__name__})
    return completed, errors


def save_details(landing_path, settings, place_id, response_data, extracted_at):
    """Write the response and compare the recovered file before reporting success."""
    directory = details_directory(landing_path, settings['enrichment_version']) / extracted_at[:10]
    directory.mkdir(parents=True, exist_ok=True)
    extraction_id = uuid4().hex
    path = directory / f'{extraction_id}.json'
    document = {
        'extraction_id': extraction_id,
        'source_kind': 'place_details',
        'saved_at_utc': datetime.now(timezone.utc).isoformat(),
        'extraction': {
            'enrichment_version': settings['enrichment_version'],
            'extracted_at_utc': extracted_at,
            'endpoint': settings['endpoint'],
            'http_status': 200,
            'request_parameters': {'place_id': place_id, 'fields': settings['field_mask']},
            'discovery_mapping_version': settings['discovery_mapping_version'],
            'discovery_date_utc': settings['discovery_date_utc'],
            'response_data': response_data
        }
    }
    with path.open('x', encoding='utf-8') as file:
        json.dump(document, file, ensure_ascii=False, indent=2)
    with path.open(encoding='utf-8') as file:
        if json.load(file) != document:
            raise ValueError('Persisted details verification failed.')
    return str(path)


def run_details_enrichment(place_ids, landing_path, settings, api_key_provider, *, enabled=False):
    """Resume a one-time campaign; the live request limit includes failed attempts."""
    if not enabled:
        print('Plan only: no Google calls and no files written.')
        return {'attempted': 0, 'saved': 0}
    if not Path(landing_path).is_dir():
        raise FileNotFoundError('Landing Volume is not accessible.')
    limit = settings['max_calls_per_run']
    if not isinstance(limit, int) or not 1 <= limit <= 500:
        raise ValueError('max_calls_per_run must be between 1 and 500.')
    delay = settings['seconds_between_calls']
    if delay < 0:
        raise ValueError('seconds_between_calls must not be negative.')
    if 'id' not in settings['field_mask']:
        raise ValueError('The id field is required for response validation.')
    completed, errors = load_details_inventory(landing_path, settings)
    if errors:
        raise RuntimeError('Review invalid enrichment files before retrying.')
    pending = sorted({p for p in place_ids if isinstance(p, str) and p.strip()} - set(completed))
    if not pending:
        print('Pending places: 0. No Google calls made.')
        return {'attempted': 0, 'saved': 0}
    key = api_key_provider()
    attempted = saved = 0
    try:
        for place_id in pending[:limit]:
            if attempted:
                time.sleep(delay)
            attempted += 1
            timestamp = datetime.now(timezone.utc).isoformat()
            try:
                response = requests.get(
                    settings['endpoint'].rstrip('/') + '/' + quote(place_id, safe=''),
                    headers={'X-Goog-Api-Key': key, 'X-Goog-FieldMask': ','.join(settings['field_mask'])},
                    timeout=30
                )
                if response.status_code != 200:
                    print(f'Stopped: {place_id} | HTTP {response.status_code}. No automatic retry.')
                    break
                data = response.json()
                if not isinstance(data, dict) or data.get('id') != place_id or 'error' in data:
                    raise ValueError('Invalid Place Details response.')
                path = save_details(landing_path, settings, place_id, data, timestamp)
            except Exception as error:
                print(f'Stopped: {place_id} | {type(error).__name__}. Inspect files before retrying.')
                break
            saved += 1
            print(f'{place_id} | Saved and verified: {path}')
    finally:
        del key
    print(f'Calls attempted: {attempted}; responses saved and verified: {saved}')
    return {'attempted': attempted, 'saved': saved}
