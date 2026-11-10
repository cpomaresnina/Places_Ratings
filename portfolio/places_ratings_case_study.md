# Places Ratings — From API Responses to a Business Explorer

**Data Engineering · Databricks · Python · Spark SQL · Delta Lake · Amazon S3**

I built a data pipeline to investigate a business idea: which types of establishments might be worth interviewing about an NFC service for collecting Google reviews?

The project covers selected commercial areas of La Paz and El Alto, Bolivia. It combines API ingestion, a Medallion architecture, dimensional modeling and an interactive Databricks dashboard.

## The challenge

The hard part was making the collected records represent the right unit of analysis. Overlapping geographic searches and category filters returned the same establishments repeatedly. A successful API response did not guarantee exhaustive discovery, and missing review counts could not be treated as zero.

Across 315 query responses, I collected 3,272 establishment appearances. These represented **1,314 distinct establishments**. Counting appearances as businesses would have biased category comparisons toward frequently detected establishments.

## What I built

1. **Python collector:** configurable search plan, secret-based authentication, bounded execution, a frozen run manifest, per-response persistence/read-back and resume behavior.
2. **S3 landing and Bronze:** original JSON responses with query, run, extraction time and source lineage.
3. **Spark SQL Silver:** nested JSON parsing, array expansion, technical checks and deterministic whole-row selection per establishment/run.
4. **Dimensional Gold:** six dimensions, an establishment snapshot fact and a bridge preserving detected-category memberships alongside a single analytical category.
5. **Databricks AI/BI dashboard:** Overview, Business Explorer, and Reputation & Digital Presence pages with interactive filters.

The pipeline uses managed Delta tables for Bronze, Silver and Gold. Transformations use Spark SQL; Python is used for API ingestion. Hash checks and MERGE identities protect against duplicate reruns and unexpected changes to existing snapshots.

## What the data showed

- **1,011** establishments had at least one reported review; **303** had an unreported count.
- Bars and pubs led main-category review coverage at **83.52%**, with a median of **14.5** reviews.
- Restaurants had the largest observed pool with reviews: **481** establishments. Cafés contributed another **213**.
- The original assumption that cafés/restaurants led review counts per establishment was not supported by their medians, both **11**.

This distinction matters: the segment with the highest rate or median is not necessarily the segment with the largest observed list of businesses to investigate.

## Decisions I can explain

- Why I retained original responses instead of flattening and discarding them at ingestion.
- Why the business key is a source establishment ID, not a shared name or phone number.
- Why I selected a latest whole observation rather than mixing maximum values across rows.
- Why missing values remain distinct from zero and false.
- Why the fact grain includes extraction identity and a category bridge retains multiple detection memberships.
- Why category coverage, review volume and observed pool size require separate metrics and denominators.

## Limits and current status

This is a working, manually executed project with one validated baseline and a published workspace dashboard. It is not a census, a production service or proof of NFC demand. Study areas are attributed from search nuclei; collection limits affect coverage. No customer loyalty, NFC interaction or transaction data has been collected.

Orchestration, an independent landing backup and full second-run parameterization are the next engineering steps. A small commercial pilot would be a separate step to validate product usage and willingness to pay.

## Explore the implementation

- [Project repository](https://github.com/cpomaresnina/Places_Ratings)
- [Architecture and execution overview](../README.md)
- [Gold model](../docs/gold_model.md)
- [Results and limitations](../docs/baseline_results.md)
- [Importable dashboard](../Dashboards/Places%20Ratings.lvdash.json)

The exported dashboard configuration can be inspected on GitHub. Running it requires a Databricks environment and the referenced Gold tables; it is not a public live-demo link.
