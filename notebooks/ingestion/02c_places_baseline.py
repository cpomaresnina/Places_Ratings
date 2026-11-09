# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Full-context baseline
# MAGIC Collect all agreed discovery and context fields in each Nearby Search response.
# MAGIC Five commercial nuclei × seven categories × nine points = 315 planned requests.
# MAGIC Output: landing/source_files/baselines/baseline_001/observed/...
# MAGIC Previous discovery files, enrichment files and Delta tables remain unchanged.
# MAGIC Default Run all only inspects the plan. Pilot cap: one call.
# MAGIC A fixed run_id supports resume across dates; timestamps reflect actual collection.
# MAGIC After validation, Bronze must load only this selected baseline, not all runs.
# MAGIC The earlier Silver temporary views and Bronze table still refer to the trial data.

# COMMAND ----------

# Locate the repository and import the baseline collector.
import importlib
import json
import sys
from pathlib import Path

working_directory = Path.cwd().resolve()
REPO_ROOT = next((p for p in [working_directory, *working_directory.parents]
                  if (p / 'config/places_baseline.json').is_file()
                  and (p / 'src/ingestion').is_dir()), None)
if REPO_ROOT is None:
    raise RuntimeError('Baseline configuration not found in the current Git folder.')
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
from src.ingestion import search_plan, storage, baseline
importlib.reload(search_plan)
importlib.reload(storage)
importlib.reload(baseline)

# COMMAND ----------

# Load the fixed baseline definition with live extraction disabled.
with (REPO_ROOT / 'config/places_baseline.json').open(encoding='utf-8') as file:
    config = json.load(file)

RUN_LIVE_EXTRACTION = False

# COMMAND ----------

# Audit saved progress and field availability without calling Google.
def show_baseline_plan():
    status = baseline.inspect_run(config)
    print(f"Run ID: {status['run_id']}")
    print(f"Fields requested: {len(config['field_mask'])}")
    print(f"Planned queries: {status['planned_queries']}")
    print(f"Completed queries: {status['completed_queries']}")
    print(f"Pending queries: {len(status['pending'])}")
    print(f"Saved response files: {status['saved_files']}")
    print(f"Files requiring review: {len(status['errors'])}")
    print(f"Duplicate response files: {status['duplicate_response_files']}")
    print(f"Maximum calls this execution: {min(len(status['pending']), config['max_calls_per_run'])}")
    print(f"Business appearances: {status['business_appearances']}")
    print(f"Unique place IDs: {status['unique_place_ids']}")
    print(f"Responses at limit: {status['potentially_truncated_responses']}")
    for field, count in status['reported_fields'].items():
        print(f"Reported {field}: {count}/{status['business_appearances']}")
    for error in status['errors']:
        print(error)
    return status

status = show_baseline_plan()

# COMMAND ----------

# Execute only a deliberately enabled batch, then recheck persisted progress.
try:
    if status['errors'] or status['duplicate_response_files']:
        raise RuntimeError('Review the baseline inventory before continuing.')
    if RUN_LIVE_EXTRACTION:
        baseline.execute_baseline(
            config,
            api_key_provider=lambda: dbutils.secrets.get(
                scope=config['secret_scope'], key=config['secret_key']
            ),
            enabled=True
        )
        show_baseline_plan()
    else:
        print('Plan only: no Google calls and no files written.')
finally:
    RUN_LIVE_EXTRACTION = False

# COMMAND ----------

# MAGIC %md
# MAGIC ## Operating notes
# MAGIC Keep run_id unchanged when resuming. Limits and delays may change, but selection
# MAGIC changes require a new run_id after collection begins. Configuration is frozen in
# MAGIC the run manifest. For a future monthly snapshot choose a new run_id and retain the old one.
# MAGIC Run only one collector at a time. A timeout or persistence failure can still require
# MAGIC repeating a billable call. No automatic retries are performed.
# MAGIC Restore the source flag to False before saving or committing.
# MAGIC An omitted field is unknown; explicit false and zero are preserved as values.
# MAGIC Field availability is counted across appearances, not deduplicated businesses.
# MAGIC Reaching the 20-result limit signals potentially incomplete search coverage.
# MAGIC No deletion or Delta-table replacement is included in this notebook.