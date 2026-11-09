# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Google Places API — Ingestion
# MAGIC This notebook loads project configuration and coordinates reusable ingestion modules.
# MAGIC Default Run all only reads storage and displays the plan; live requests are disabled.
# MAGIC Modules belong in `src/ingestion/` as Python files, not Databricks notebooks.
# MAGIC The Git repository root must contain both `config/places_search.json` and `src/`.
# MAGIC Same-day equivalent requests are skipped. A new UTC date intentionally creates a new snapshot.
# MAGIC Search coverage remains partial. This runner supports one active execution at a time.
# MAGIC Storage setup and secret provisioning remain in separate setup steps.

# COMMAND ----------

# Locate the repository and import the reusable ingestion modules.
import importlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

working_directory = Path.cwd().resolve()
REPO_ROOT = next((path for path in [working_directory, *working_directory.parents]
                  if (path / 'config/places_search.json').is_file()
                  and (path / 'src/ingestion').is_dir()), None)
if REPO_ROOT is None:
    raise RuntimeError('Repository root not found. Run this notebook inside the connected Git folder.')
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.ingestion import search_plan, storage, places_api
# Reload dependencies to pick up module edits during interactive development.
importlib.reload(search_plan)
importlib.reload(storage)
importlib.reload(places_api)
print(f'Repository: {REPO_ROOT}')

# COMMAND ----------

# Load non-secret configuration and select the current extraction scope.
with (REPO_ROOT / 'config/places_search.json').open(encoding='utf-8') as file:
    config = json.load(file)

STUDY_AREA = 'sopocachi'
SELECTED_CATEGORY = 'restaurants'
SNAPSHOT_DATE_UTC = datetime.now(timezone.utc).date().isoformat()
RUN_LIVE_EXTRACTION = False

# COMMAND ----------

# Inspect saved files and display pending requests without calling Google.
def show_plan():
    queries, errors = storage.load_saved_queries(config['landing_path'])
    points = search_plan.build_grid(
        config['areas'][STUDY_AREA]['center'], STUDY_AREA,
        config['grid_spacing_meters'], config['query_radius_meters']
    )
    pending = search_plan.pending_grid_points(
        queries, STUDY_AREA, SELECTED_CATEGORY, SNAPSHOT_DATE_UTC, points,
        config['categories'][SELECTED_CATEGORY], ','.join(config['field_mask']),
        config['endpoint']
    )
    print(f'Saved queries found: {len(queries)}')
    print(f'Files requiring review: {len(errors)}')
    print(f'Selected category: {SELECTED_CATEGORY}')
    print(f'Snapshot date (UTC): {SNAPSHOT_DATE_UTC}')
    print(f'Grid points: {len(points)}; pending queries: {len(pending)}')
    for error in errors:
        print(error)
    return errors

inventory_errors = show_plan()

# COMMAND ----------

# Execute ingestion only when deliberately enabled, then verify persisted progress.
try:
    if inventory_errors:
        raise RuntimeError('Review unreadable files before extraction.')
    if RUN_LIVE_EXTRACTION:
        places_api.run_pending_grid(
            config, STUDY_AREA, SELECTED_CATEGORY, SNAPSHOT_DATE_UTC,
            api_key_provider=lambda: dbutils.secrets.get(
                scope=config['secret_scope'], key=config['secret_key']
            ),
            confirm_live_calls=True
        )
        show_plan()
    else:
        print('Plan only: no API calls made and no files written.')
finally:
    RUN_LIVE_EXTRACTION = False

# COMMAND ----------

# MAGIC %md
# MAGIC ## Manual execution
# MAGIC Review the plan first. To run, set `RUN_LIVE_EXTRACTION = True` in configuration
# MAGIC and deliberately execute the notebook from top to bottom. Restore the source
# MAGIC flag to False before committing: resetting the variable in memory does not edit the cell.
# MAGIC At most nine calls are attempted per execution, with no automatic retries.
# MAGIC Inspect files after failures; a timeout or failed save can require a repeated request.
# MAGIC Keep existing observed files; no migration or deletion is performed here.