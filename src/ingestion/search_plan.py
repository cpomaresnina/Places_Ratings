# Generate search requests and identify completed queries.
import json
import math

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

def select_snapshot(queries, area, category, snapshot_date, endpoint):
    return [q for q in queries if
            q['study_area'] == area and q['category'] == category
            and q['extracted_at_utc'][:10] == snapshot_date
            and q['endpoint'] == endpoint]

def pending_grid_points(queries, area, category, snapshot_date, points, included_types, field_mask, endpoint):
    completed = {
        query_signature(area, category, q['request_parameters'], q['field_mask'])
        for q in select_snapshot(queries, area, category, snapshot_date, endpoint)
    }
    return [p for p in points if query_signature(
        area, category, build_payload(included_types, p), field_mask
    ) not in completed]
