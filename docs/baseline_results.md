# Baseline results — main analytical categories

Snapshot: `baseline_001`, collected November 9, 2026. Evidence: executed Databricks outputs supplied during development. The review on November 10 inspected repository code and exported dashboard configuration; it did not independently query live tables.

## Reconciliation

315 query responses, including 7 valid empty responses, expand into 3,272 appearances. Consolidation produces 1,314 distinct establishment snapshots. Of those, 1,011 have a reported count greater than zero; 303 (23.06%) have no reported count. No explicit zero was observed.

## Main-category comparison

| Category | Businesses | With reviews | Missing count | With reviews / all | Median count | Mean count |
|---|---:|---:|---:|---:|---:|---:|
| Bars & Pubs | 91 | 76 | 15 | 83.52% | 14.5 | 158.01 |
| Restaurants | 598 | 481 | 117 | 80.43% | 11 | 150.14 |
| Gyms | 49 | 38 | 11 | 77.55% | 12.5 | 35.63 |
| Cafes | 280 | 213 | 67 | 76.07% | 11 | 117.24 |
| Veterinary & Pet Care | 49 | 37 | 12 | 75.51% | 12 | 28.19 |
| Unclassified | 62 | 46 | 16 | 74.19% | 9.5 | 82.26 |
| Nightclubs | 36 | 25 | 11 | 69.44% | 13 | 180.80 |
| Hair Salons & Barbers | 149 | 95 | 54 | 63.76% | 5 | 22.43 |
| **Total** | **1,314** | **1,011** | **303** | **76.94%** | — | — |

These categories are mutually exclusive. Earlier [EDA findings](EDA%20Findings.md) grouped retrieval memberships and therefore have different, overlapping category counts. That is a change in analytical definition, not missing data.

## Interpretation

- The initial claim that cafés and restaurants lead review counts per establishment is not supported by their medians in this sample.
- Restaurants offer the largest observed pool of establishments with reviews. Together, restaurants and cafés account for 694 / 1,011 = 68.64% of establishments with reported reviews. This is sample composition, not market share.
- Main-category and zone coverage vary. Restaurants report reviews for 86.50% of sampled Sopocachi establishments, versus 74.40% in Ciudad Satélite. The Calacoto search design covers three nuclei, so raw zone counts must not be read as comparable market sizes.
- Website is reported for 301 businesses, hours for 931, phone for 914 and price for 404. Absence in the extraction is not verified absence in the real world.
- Café median rating is 4.8 in the 1–10 review band versus 4.3 in the 51–200 band. Scores should be displayed with their supporting counts; the pattern is not evidence of fake reviews or a causal rating decline.
- Restaurants with moderate reported prices have a median count of 74 (225 businesses), versus 3 among those without a reported price (241 with counts out of 358 businesses). Missing-price groups and small expensive-price groups limit interpretation.

## What this does not establish

The data does not establish complete local business coverage, willingness to buy NFC, customer retention, business revenue, or the impact of an NFC intervention. A second extraction would permit descriptive change comparisons; attribution to NFC would need a separate measurement design and usage evidence.
