EDA findings — baseline_001

Evidence: user-provided executed EDA outputs. These results were not independently
queried from the remote Databricks workspace while preparing this document.
The EDA uses a provisional latest-whole-observation selection per run and place ID.

## Dataset and repeated observations

- 315 source responses; 7 valid empty responses.
- 3,272 business appearances and 1,314 distinct place IDs.
- No missing place IDs or coordinates; one appearance lacks a primary type.
- No invalid ratings, negative review counts, invalid coordinate ranges or
  unrecognized price/status values were reported by the implemented checks.
- No business had multiple distinct non-null values for name, primary type,
  rating, review count, website, phone, price level or serialized regular hours.
- No business mixed null and reported review counts across observations.
- These checks do not establish complete equality across all response fields.
  COUNT(DISTINCT) ignores NULL. Mixed missingness for website, hours, phone and
  price has not yet been reported; maps links, address and coordinates were not
  included in the supplied cross-observation variation summary.

## Business-level completeness

| Field | Reported businesses | Share of 1,314 |
|---|---:|---:|
| Website | 301 | 22.91% |
| Regular opening hours | 931 | 70.85% |
| Phone | 914 | 69.56% |
| Price level | 404 | 30.75% |
| Write-review link | 1,314 | 100.00% |
| Containing places | 110 | 8.37% |
| Pure service-area status | 0 | 0.00% |

Absent fields mean not reported by this extraction, not confirmed absence of the
service or attribute. The absent service-area flag must not be converted to false.
Reported attributes are not independently verified.

## Review-count distribution

303 businesses (23.06%) have no reported review count; 1,011 have a reported count.
502 businesses have 1–10 reviews: 38.20% of all sampled businesses, or 49.65% of
businesses with a reported count. No zero-count or negative-count band appeared
in the supplied distribution; missing counts are not interpreted as zero.

## Requested-category comparison

| Requested category | Businesses | Valid reported count | Missing count | Median count | Mean count |
|---|---:|---:|---:|---:|---:|
| Bars and pubs | 104 | 89 | 15 | 26 | 190.33 |
| Nightclubs | 49 | 33 | 16 | 15 | 155.27 |
| Gyms | 49 | 38 | 11 | 12 | 35.00 |
| Cafés | 294 | 226 | 68 | 11 | 114.79 |
| Restaurants | 627 | 507 | 120 | 11 | 159.55 |
| Veterinary care and grooming | 66 | 49 | 17 | 10 | 29.90 |
| Hair salons and barbers | 170 | 108 | 62 | 5.5 | 26.27 |

Category membership is based on retrieval, not independently verified business
classification. Category counts total 1,359 memberships across 1,314 businesses.
The 45 additional memberships do not imply exactly 45 multi-category businesses.
Category samples overlap and valid-count denominators differ.

Means exceed medians substantially in several categories, consistent with a
right-skewed distribution; inspect records before treating high values as errors.

## Hypothesis assessment

The proposition that cafés and restaurants have the highest typical review counts
is not supported by the observed medians: both have a median of 11, below bars/pubs
(26), nightclubs (15) and gyms (12). This is descriptive evidence for this sample,
not a population-level hypothesis test. Restaurants and cafés have many recovered
businesses, which is different from having more reviews per establishment.

Review counts do not establish commercial need, willingness to pay or causal NFC
impact. Availability of a write-review link shows a possible technical destination
for an NFC tag, not evidence that people will use it or publish reviews.

## Status and classification review

1,280 businesses are reported OPERATIONAL and 34 CLOSED_TEMPORARILY (2.59%).
Keep all source observations. Preserve the status for potential-client filtering;
do not silently delete temporary closures or infer permanent closure.

Primary types include store, medical_clinic, florist, cultural_center and others
outside the expected high-level labels. These are review candidates, not automatic
classification errors: inspect source_types and requested_category together.

## Proposed next Silver decisions

1. Preserve all observations and lineage.
2. Keep one business snapshot per run/place ID, using a deterministic whole-row
   selection rule; finalize it after reviewing optional-field mixed missingness.
3. Keep distinct category and commercial-nucleus memberships separately.
4. Preserve NULL values, raw enum strings and exceptional records with review flags.
5. Apply no geographic exclusion, per the user's decision.
6. Recompute EDA coverage if the consolidation rule changes, rather than carrying
   forward provisional sample figures as final metrics.