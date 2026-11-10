# Project review — November 10, 2026

## Evidence and scope

Reviewed GitHub `main` after the dashboard merge at commit `94fdaee681b1f65b702bcd0bf778f2066f1b34a1`. Inspected source modules, exported notebooks, configuration, Markdown and the dashboard JSON. Baseline results are the executed outputs supplied during development. No live Google calls, Databricks table queries or AWS operations were performed during this review.

## Assessment

The project already demonstrates a real API-to-dashboard workflow: bounded resumable ingestion, original-response preservation, Spark SQL parsing and windows, one-row-per-business consolidation, Delta MERGEs, six Gold dimensions, a snapshot fact, a category bridge and an importable dashboard. The most immediate gaps were reproducibility instructions and presentation, rather than more transformations or more charts.

## Findings and changes

| Priority | Finding in the reviewed source | Treatment |
|---|---|---|
| High for portfolio | README was one paragraph and did not explain architecture, execution, limits or results | Replaced with a complete project entry point and documentation links |
| High for reproducibility | `docs/scope.md` was empty; active and superseded collection paths were not obvious | Added scope and a runbook that identifies `02c_places_baseline` as the current collector |
| High before a second extraction | Bronze/Silver hardcode baseline_001 and 315 responses; dashboard dataset reads all runs without a run selector | Documented exact future-run changes; parameterization/orchestration intentionally deferred |
| Medium for analysis | Exported third page had no category/area controls, so price comparisons could pool unrelated categories | Added local category/area filters and an interpretation note in the reviewed export; import validation pending |
| Medium for usability | Business Explorer export had no status control, despite retaining temporary closures | Added a status filter; source rows remain unchanged |
| Medium for clarity | Some widget titles contained literal backticks/HTML, one median chart had no visible title, and axis labels included `Avg. of reviews` and `& ` | Cleaned display titles and labels without changing query aggregates |
| Medium for documentation accuracy | Gold model described address-based unknown-zone handling even though the code uses nearest detected nucleus | Updated the document to match implemented attribution and the explicit migration |
| Medium for documentation accuracy | “positive reviews” remained in comments after SQL aliases were renamed to `with_reviews` | Reworded comments to mean at least one reported review, not favorable sentiment |
| Medium for operating instructions | Collection/transition notes implied a one-call cap although committed config allows 315 | Documented the actual cap and instructed a deliberate one-call pilot for new runs |
| Medium for navigation | Silver/EDA prose referred to old notebook paths | Corrected the descriptive paths; the executable `%run` path was already correct |
| Low for repository hygiene | No `.gitignore` for local caches, environments and data exports | Added focused ignore rules without hiding config or dashboard JSON |

The links omitted from the directory are an intentional product choice, not a defect. No URL filter or extra link columns were added.

## Checks performed

Local review checks cover Python syntax without executing notebook cells, JSON/notebook parseability, unique planned query IDs, the 315-query plan, configuration-fingerprint stability for operational limits, manifest changes for selection changes, persistence/read-back and resume with synthetic responses, dashboard internal references/layouts, and documentation links. API calls are mocked or disabled in these checks.

Checks do not prove Spark execution, IAM permissions, UI rendering, source completeness or production performance. Reimport the reviewed dashboard and rerun its acceptance checks before republishing.

## Work intentionally remaining

1. **Orchestration:** chain Bronze → consolidated Silver load → Gold with failure reporting. The collector should remain separately and explicitly enabled.
2. **Storage continuity:** independently export the verified landing baseline and manifest before bootcamp S3 access ends.
3. **Second-run readiness:** parameterize baseline selection and source expectations, standardize UTC serialization across transformation sessions, add the extraction selector and verify historical comparisons.
4. **Operational robustness:** define an explicit timeout/429 retry policy, validate expanded plans and introduce complete-run gating before loading partial future extractions. Current resumption is single-collector and does not claim exactly-once API execution.
5. **Business classification:** review 62 unclassified establishments using additional source types/manual mapping; do not silently force ambiguous cases into an existing category.
6. **Portfolio evidence:** capture real dashboard screenshots and record a concise demo. No invented performance, sales or revenue claims are needed.

These items do not prevent presenting the current manually operated baseline as a learning project. They prevent describing it as a fully scheduled production service or a validated NFC business.
