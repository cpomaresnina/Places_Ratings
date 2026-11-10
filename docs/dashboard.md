# Dashboard guide

The versioned export is [Places Ratings.lvdash.json](../Dashboards/Places%20Ratings.lvdash.json). It contains dataset SQL, page layout, widgets and filters. It does not contain landing responses or replace a backup of Delta tables.

## Pages

| Page | Purpose |
|---|---|
| Overview | Establishment counts, review coverage, missing review counts, median reviews and a rating/count scatter plot |
| Business Explorer | Searchable establishment directory with category, study area, review band, rating and website-availability filters |
| Reputation & Digital Presence | Reported website, phone and hours coverage, and mean business rating by price level |

The directory intentionally omits Maps and review-action links from its displayed columns. Those fields remain in the dataset for possible future use. A filter listing thousands of URLs is not needed.

## Import and access

1. From the Databricks dashboards listing, choose **Import dashboard from file** and select the `.lvdash.json` export.
2. Choose an available SQL warehouse. The importing identity needs access to the referenced Gold view.
3. Run the `Business snapshots` dataset and inspect all three pages.
4. Publish only after validating the draft. Publication and granting access are separate actions; a GitHub repository link does not give readers access to Databricks tables.

To replace an existing draft, use **File actions → Replace**. Review the imported draft before republishing. The exported file is the draft configuration, so published visuals may lag behind a newly committed export. [Official import/export guide](https://docs.databricks.com/aws/en/dashboards/automate/import-export).

## Filters and interpretation

- Category and study-area filters support multiple selections. Clear page filters and cross-selections before comparing against baseline totals.
- Review-count bands replace the numeric count slider so large outliers do not make low ranges difficult to select.
- `Website Reported = false` means the API did not report a website, not that absence was verified.
- The price chart pools categories when several categories are selected. Select one category for within-category price comparisons and inspect subgroup sizes with `gold.v_price_reputation`.
- The mean rating is equally weighted by establishment, not by number of individual reviews.
- Reported-information charts use an average of 0/1 flags and percent formatting. Do not multiply by 100 a second time.

## Review changes prepared on November 10

The source export lacked category/zone controls on the third page and a business-status filter in the explorer. The reviewed export adds those controls and a price-coverage caveat, fixes ambiguous axis labels, and gives the median chart a visible title. No row-level source data or underlying Gold calculations were changed. URL columns remain omitted by design.

The changes were checked structurally, not rendered in a live Databricks session. Import and verify before replacing the published version. No extraction filter is added yet, following the project's current one-baseline scope.

## Baseline acceptance checks

With all filters cleared and only baseline_001 present:

- Businesses Identified: **1,314**.
- Businesses with Unreported Review Counts: **303**.
- Restaurants: **598**.
- Cafes + Sopocachi: **111**.
- Bars & Pubs review coverage: approximately **83.52%**.
- Business-status filter offers OPERATIONAL / CLOSED_TEMPORARILY as returned by the dataset.
- Category/zone controls on the third page affect its charts.
- Rating slider selections can exclude rows with missing ratings; review bands retain Not reported as a selectable group.

## Before multiple baselines

The current dataset selects all runs from `gold.v_business_explorer`. Add a single-run selector or scope the query before loading another run. Otherwise distinct-business counters and snapshot-based rates can describe different grains, and the directory can repeat establishments.

## Portfolio evidence

Use actual screenshots of each published page with filters clearly visible. Do not present a recreated mockup as an executed dashboard. Include a screenshot or a short demo in the GitHub case study when available; a workspace-only URL is not a public portfolio demo.
