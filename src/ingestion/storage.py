# Read and persist extraction documents without importing Databricks utilities.
import json
from pathlib import Path
from uuid import uuid4
from datetime import datetime, timezone

# Frozen compatibility values for the original exploration collector.
LEGACY_FIELD_MASK = 'places.id,places.displayName,places.primaryType,places.types,places.formattedAddress,places.location,places.businessStatus,places.rating,places.userRatingCount'
LEGACY_ENDPOINT = 'https://places.googleapis.com/v1/places:searchNearby'

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
                'mapping_version': extraction.get('mapping_version', 'legacy'),
                'extracted_at_utc': extracted_at.astimezone(timezone.utc).isoformat(),
                'request_parameters': parameters,
                # Compatibility: the first ten saved files used this exact mask.
                # Do not generalize this fallback to files from other collectors.
                'field_mask': extraction.get('field_mask', LEGACY_FIELD_MASK),
                'endpoint': extraction.get('endpoint', LEGACY_ENDPOINT),
                'document': document,
                'returned_count': len(places),
                'potentially_truncated': len(places) == 20
            })
        except (OSError, ValueError, KeyError, TypeError, AttributeError) as error:
            errors.append({'file_path': str(file_path), 'error_type': type(error).__name__})
    return queries, errors

def save_extraction(extraction, category, study_area, data_kind, landing_path):
    extraction_id = uuid4().hex
    saved_at = datetime.now(timezone.utc)

    output_directory = (
        Path(landing_path)
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
