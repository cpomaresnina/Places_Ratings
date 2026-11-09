# Transition from exploration to the full-context baseline

## Scope

`baseline_001` repeats the agreed five-nucleus, seven-category, nine-point search design.
It requests the nine discovery fields plus website, regular hours, Maps links,
provider attributions, national phone, containing places, service-area status and price level.
The field list is explicit in `config/places_baseline.json`; timeZone, reviews,
photos and the Enterprise + Atmosphere service flags are excluded.

## Execution

1. Import the additive baseline module, configuration and notebook.
2. Run in plan-only mode: expect 315 pending queries and a one-call pilot cap.
3. Execute one pilot and inspect the persisted field coverage before increasing the cap.
4. Resume the same run_id for remaining queries, including across UTC midnight.
5. Verify 315 distinct completed query keys, no invalid/duplicate response files,
   field availability, unique place IDs and per-category geographic coverage.

The manifest freezes the query definition; changing max_calls_per_run or the delay
does not create a new data definition. No independent claim of complete business
coverage or identical IDs relative to the previous extraction is made.

## Storage and processing transition

New data is under `baselines/baseline_001/observed/`, with a manifest at the run root.
This does not enter the previous root-level `observed/` inventory automatically.
The existing Bronze and temporary Silver views still describe earlier trial data.

After verification, update the SQL loader to read the selected baseline path and
include run_id, query_key and field_set_version. Rebuild the intended baseline Bronze
table with Delta replacement only after its source has passed validation. Then
rebuild Silver from that source, retaining original JSON and actual observation times.

Before removing trial files, inventory the explicitly scoped old `observed/`,
`enrichment/` and any synthetic test files, verify they are not baseline dependencies,
and confirm the new tables can be read independently. Deletion is not implemented
here and must never target the Volume root or the baselines directory.

The user authorized trial-data replacement, but no remote deletions or table resets
have been executed while preparing these files. Independent backup remains pending.
