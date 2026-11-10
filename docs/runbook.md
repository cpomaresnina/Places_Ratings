# Execution guide

## Environment and prerequisites

The pipeline runs in Databricks with Unity Catalog and access to a SQL execution environment. It is not runnable end to end from a local Python interpreter. The code uses Python standard-library modules and `requests` for collection; install `requests` in the notebook environment if it is not already available.

Required objects:

- Catalog `places_ratings`.
- Landing schema and external Volume `places_ratings.landing.source_files`.
- Accessible configured landing path, currently `/Volumes/places_ratings/landing/source_files`.
- For new collection only: a Google Cloud project with Places API enabled, its billing configuration, and an API key stored in the configured Databricks secret scope/key. Secret names in configuration are references, not credential values.
- For S3 setup: a storage credential and external location authorized for your own bucket/prefix. Adapt `00_storage_setup.ipynb` to your environment; this repository cannot grant access to the author's bootcamp bucket.

Bronze, Silver and Gold schemas/tables are created by their notebooks when absent. The storage setup notebook does not provision Google Cloud or AWS IAM.

## Choose one execution path

| Starting point | What to run |
|---|---|
| Existing verified Gold tables | Import the dashboard and query Gold; no collection required |
| Existing verified baseline files in landing | Bronze → consolidated Silver load → Gold → dashboard |
| No source responses | Provision your storage and secret, inspect the plan, collect a baseline, then run the transformations |

The repository intentionally does not include the author's landing JSON. Importing the repository alone will not recreate the sample figures.

## Current baseline order

1. **Setup, if necessary:** `notebooks/setup/00_storage_setup.ipynb`. Review resource names before execution.
2. **Collection, only if necessary:** `notebooks/ingestion/02c_places_baseline.py`. It finds `config/places_baseline.json` from the Git folder and imports `src/ingestion`. Run initially with `RUN_LIVE_EXTRACTION = False`.
3. Inspect planned/completed/pending queries, invalid files and duplicate query responses. The committed configuration currently permits **315 calls per enabled execution**, not one. For a new pilot, set `max_calls_per_run` to 1 first.
4. Enable live collection deliberately only when intending to issue API requests. Each successful response is saved under `baselines/<run_id>/observed/...` with a run manifest. Restore the source flag to False before committing.
5. Run `notebooks/bronze/03_places_bronze_clean.py`. Its current path, run ID, versions and assertions are explicitly scoped to `baseline_001` and 315 responses.
6. For exploratory review, run `notebooks/silver/EDA/05_places_eda.py`. Its standalone `%run "../04_places_silver"` builds temporary views in the same session.
7. Run `notebooks/silver/06_places_silver_load.py`. Its `%run "./04_places_silver"` prepares observations before loading `places_ratings.silver.business_snapshots`.
8. Run `notebooks/gold/07_places_gold.py`. It reads persisted Silver directly, creates six dimensions, loads facts and the bridge, and publishes four analytical views.
9. Import `Dashboards/Places Ratings.lvdash.json`, select an available SQL warehouse and execute the dataset. Review pages/filters before publishing.

Do not execute the earlier discovery collector and Details enrichment as extra mandatory steps. Full-context Nearby responses are the current baseline source.

## Expected reconciliation for baseline_001

| Stage | Expected result |
|---|---:|
| Landing / Bronze | 315 distinct saved query responses |
| Valid empty query responses | 7 |
| Temporary Silver observations | 3,272 appearances |
| Consolidated Silver / Gold fact | 1,314 establishments |
| Source appearances accounted for | 3,272 |
| Businesses with unreported review count | 303 |

Assertions return NULL on success and throw an error on failure. Stop on an error; do not skip a check merely to complete the notebook. A response with an empty places array is valid and remains represented in Bronze.

## Reruns and interruptions

- Keep the same run ID and definition when resuming collection. The manifest rejects changed selection definitions. Call limits and delays may change.
- Run one collector at a time. Persisting/read-back and resume logic reduce repeat work; they do not guarantee exactly-once billing after network or storage failures.
- No automatic API retry/backoff policy is implemented. Investigate a failure and inspect persistence before retrying.
- Repeated immutable baseline loads should insert zero Bronze, Silver, fact or bridge rows. SCD1 dimension descriptions may update if genuinely newer/different input is supplied.
- A notebook is not a transaction across all its tables. After interruption, rerun from the beginning and require reconciliation.
- Recreating a temporary view does not erase a Delta table. Do not use routine DROP or CREATE OR REPLACE TABLE to bypass immutable-content errors.

## Before a second extraction

History is supported by table grains, but the current loaders are not fully parameterized. Changing only the collection run ID is insufficient.

1. Choose a new run ID and keep old landing data.
2. Update the Bronze path, expected ID, response count and version checks for the new run.
3. Update Silver's baseline filter and assertions. Retain previous persisted snapshots.
4. Ensure UTC is consistent for timestamp serialization and hashing; review hash differences rather than rewriting prior data automatically.
5. Check Gold category/nucleus lookup coverage and calendar bounds (2026–2031).
6. Add a single-extraction dashboard filter, or explicitly scope the dashboard dataset to one run. The current exported dataset reads all runs; it must not be used unchanged after adding historical snapshots.
7. Compare like-for-like establishments and sampling definitions before calculating growth. Growth alone does not establish an NFC effect.

## Storage continuity

Landing is in the temporary bootcamp S3 allocation. The Delta tables are managed by Databricks. An independent export of verified landing responses and the manifest is still required before S3 access expires. The dashboard JSON backs up configuration, not source data or table contents.
