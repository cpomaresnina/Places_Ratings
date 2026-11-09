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
# MAGIC Requests from a different mapping version are not considered completed for this version.
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

STUDY_AREA = 'ciudad_satelite_tinku'
SELECTED_CATEGORY = 'cafes'
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
        config['endpoint'], config['mapping_version']
    )
    print(f'Saved queries found: {len(queries)}')
    print(f'Files requiring review: {len(errors)}')
    print(f'Selected category: {SELECTED_CATEGORY}')
    print(f"Mapping version: {config['mapping_version']}")
    print(f'Snapshot date (UTC): {SNAPSHOT_DATE_UTC}')
    print(f'Grid points: {len(points)}; pending queries: {len(pending)}')
    for error in errors:
        print(error)
    return errors

inventory_errors = show_plan()

# COMMAND ----------

# Plan or extract all configured categories for the selected study area.

categories_to_run = list(config["categories"])

try:
    for category in categories_to_run:
        queries, errors = storage.load_saved_queries(
            config["landing_path"]
        )

        if errors:
            raise RuntimeError(
                "Review unreadable files before continuing."
            )

        points = search_plan.build_grid(
            config["areas"][STUDY_AREA]["center"],
            STUDY_AREA,
            config["grid_spacing_meters"],
            config["query_radius_meters"]
        )

        pending = search_plan.pending_grid_points(
            queries,
            STUDY_AREA,
            category,
            SNAPSHOT_DATE_UTC,
            points,
            config["categories"][category],
            ",".join(config["field_mask"]),
            config["endpoint"],
            config["mapping_version"]
        )

        print(f"\nCategory: {category} | Pending: {len(pending)}")

        if not RUN_LIVE_EXTRACTION or not pending:
            continue

        places_api.run_pending_grid(
            config,
            STUDY_AREA,
            category,
            SNAPSHOT_DATE_UTC,
            api_key_provider=lambda: dbutils.secrets.get(
                scope=config["secret_scope"],
                key=config["secret_key"]
            ),
            confirm_live_calls=True
        )

        # Stop the batch if this category did not finish successfully.
        queries, errors = storage.load_saved_queries(
            config["landing_path"]
        )

        remaining = search_plan.pending_grid_points(
            queries,
            STUDY_AREA,
            category,
            SNAPSHOT_DATE_UTC,
            points,
            config["categories"][category],
            ",".join(config["field_mask"]),
            config["endpoint"],
            config["mapping_version"]
        )

        if errors or remaining:
            print(
                f"Batch stopped at {category}. "
                f"Remaining points: {len(remaining)}"
            )
            break

    if not RUN_LIVE_EXTRACTION:
        print("\nPlan only: no API calls made.")

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

# COMMAND ----------

# Audit saved coverage for every category in the selected area and snapshot.

queries, errors = storage.load_saved_queries(config["landing_path"])

points = search_plan.build_grid(
    config["areas"][STUDY_AREA]["center"],
    STUDY_AREA,
    config["grid_spacing_meters"],
    config["query_radius_meters"]
)

for category, included_types in config["categories"].items():
    matching_queries = search_plan.select_snapshot(
        queries,
        STUDY_AREA,
        category,
        SNAPSHOT_DATE_UTC,
        config["endpoint"],
        config["mapping_version"]
    )

    pending = search_plan.pending_grid_points(
        queries,
        STUDY_AREA,
        category,
        SNAPSHOT_DATE_UTC,
        points,
        included_types,
        ",".join(config["field_mask"]),
        config["endpoint"],
        config["mapping_version"]
    )

    print({
        "category": category,
        "saved_responses": len(matching_queries),
        "pending_points": len(pending),
        "responses_at_limit": sum(
            query["potentially_truncated"]
            for query in matching_queries
        )
    })

print(f"Files requiring review: {len(errors)}")

# COMMAND ----------

# Audit all configured areas and categories without calling Google.

queries, errors = storage.load_saved_queries(config["landing_path"])

incomplete = []
expected_queries = 0

for area, area_config in config["areas"].items():
    points = search_plan.build_grid(
        area_config["center"],
        area,
        config["grid_spacing_meters"],
        config["query_radius_meters"]
    )

    for category, included_types in config["categories"].items():
        expected_queries += len(points)

        pending = search_plan.pending_grid_points(
            queries,
            area,
            category,
            SNAPSHOT_DATE_UTC,
            points,
            included_types,
            ",".join(config["field_mask"]),
            config["endpoint"],
            config["mapping_version"]
        )

        if pending:
            incomplete.append({
                "area": area,
                "category": category,
                "pending_points": len(pending)
            })

print(f"Snapshot date (UTC): {SNAPSHOT_DATE_UTC}")
print(f"Mapping version: {config['mapping_version']}")
print(f"Expected queries: {expected_queries}")
print(f"Files requiring review: {len(errors)}")
print(f"Incomplete area/category combinations: {len(incomplete)}")

for item in incomplete:
    print(item)