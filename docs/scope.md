
# Project scope

## Objective

Build a reproducible data foundation for exploring business segments for a potential NFC review-capture service. The initial hypothesis was that cafés and restaurants have the largest review counts per establishment and would therefore be promising segments to investigate.

Review counts are an observable signal, not a direct measurement of customer demand, loyalty, purchase intent or willingness to pay. These require separate commercial validation.

## Collection design

- Source: Google Places API (New), Nearby Search. Collection uses an API, not HTML scraping.
- Areas: Sopocachi and Calacoto in La Paz; Ciudad Satélite in El Alto.
- Five search nuclei: Plaza Abaroa; El Bosque; San Miguel; Montenegro; Plaza del Tinku.
- Seven requested categories: cafés, restaurants, hair salons/barbers, gyms, veterinary/pet care, nightclubs, bars/pubs.
- Nine query points per nucleus, with 300 m spacing and a 350 m query radius.
- Total baseline design: 5 × 7 × 9 = 315 queries.
- The 600 m study radius remains context; establishments are not excluded for falling outside it.
- Repeated detections from overlapping categories or query circles are intentional.

Centers and type lists live in `config/places_baseline.json`. Adding a category requires updating collection, the Gold category lookup and the classification CASE; adding a nucleus also requires its Gold zone mapping.

## Unit of analysis

A business is an establishment identified by a Google `place_id`, not a brand. Shared names, coordinates, websites or telephone numbers are not sufficient to merge establishments. The persistent analytical grain is one establishment per extraction run.

## Fields

The baseline requests 18 fields: ID, display name, primary type, type array, address, location, status, rating, review count, website, regular hours, Maps URI, attributions, Maps action links, telephone, containing places, service-area flag and price level.

Review text, reviewer identities and an asserted latest-review date are outside this baseline. No customer transaction, repeat-visit, loyalty or NFC interaction data has been collected.

## Classification and zone attribution

Gold assigns one main analytical category from the primary type. Ambiguous types remain `Unclassified`; this does not invalidate the record. A separate bridge preserves every requested category in which an establishment was detected.

Zone assignment uses the nearest **detected nucleus**, grouped into the three study areas. El Bosque, San Miguel and Montenegro map to Calacoto. This is attribution for analysis, not a verified neighborhood boundary.

## Success criteria for this version

1. Preserve every verified query response and its execution context.
2. Consolidate repeated appearances without silently dropping establishments or imputing absent data.
3. Load Bronze, Silver and Gold reproducibly without duplicate rows on reruns.
4. Explain review coverage, volume, reputation and digital-information availability with explicit denominators.
5. Provide an interactive business explorer and document sampling and interpretation limits.

The current result is a manually executed end-to-end pipeline and a published dashboard. Scheduled operation, independent raw backup and a second baseline remain future work.
