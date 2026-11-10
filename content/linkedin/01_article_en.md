# 3,272 Appearances, 1,314 Businesses: Why Data Grain Matters

My first baseline for a local-business data project contained 3,272 business appearances. After consolidation, I had 1,314 distinct establishments.

The pipeline had not lost more than half the businesses. Those numbers answered different questions: how many times an establishment appeared, and how many establishments I had identified.

Understanding that distinction became one of the most useful lessons from my final Data Engineering bootcamp project.

## Starting with a business question

I am building a data foundation to explore which business segments might be worth interviewing about an NFC service that makes it easier for customers to leave Google reviews.

I started with cafés, restaurants and several other categories in selected commercial areas of La Paz and El Alto, Bolivia. I wanted to compare review counts, ratings and reported information such as websites, phone numbers and opening hours.

The source is the Google Places API. I use Python for ingestion and Spark SQL in Databricks for the transformations.

## Repetition was part of the collection design

The extraction used multiple search points and categories. The same establishment could appear in searches from nearby points or under more than one requested category.

Those repeated appearances were valid observations. The mistake would have been treating each appearance as a separate business when calculating metrics.

If one café appears in five responses and another appears once, the first should not receive five times the weight in a business-level comparison simply because it was detected more often.

Before choosing charts, I needed to define what each row represented.

## I separated three grains

**Response:** the result of one API query. The baseline contains 315 responses, including valid queries that returned no businesses.

**Appearance:** one establishment within a response. Expanding the place arrays produced 3,272 appearances.

**Business snapshot:** one establishment, identified by its `place_id`, within an extraction run. Consolidation produced 1,314 rows at this grain.

Keeping these levels separate lets me trace the data back to its source while preventing overlapping searches from multiplying establishments in the analysis.

## Deduplication needed an explicit rule

I did not use the business name, phone number or coordinates as a unique identifier. Different branches can share those attributes.

For consolidation, I used the establishment ID together with the extraction identity. I selected the latest whole observation, applied deterministic tie-breakers and preserved the categories under which each establishment had been detected.

I did not take the highest review count from one row and the phone number from another. I wanted to explain which observation the selected record came from.

## What changed in the analysis

With one row per establishment and extraction, I could compare businesses without giving repeated detections extra weight. I also distinguished a main analytical category from the categories used to discover a business.

The findings did not fully support my initial assumption. Cafés and restaurants contributed many establishments, but they did not lead the median review count per business.

That is useful evidence too. Having a large observed list of businesses to investigate is different from demonstrating that a category has more reviews or greater willingness to buy a service.

## The lesson I am taking forward

In this project, defining the grain was as important as writing the query.

A dashboard can display a technically correct number and still answer the wrong question if we are unclear about what we are counting.

The project is still evolving. Bronze, Silver and Gold layers and a dashboard are in place; orchestration and an independent backup of the landing data are next. In future posts, I plan to share other decisions from the implementation, including missing-data handling and loads that can be rerun without duplicating records.

Have you encountered apparently duplicate records that were actually valid observations of the same entity?

Project repository: https://github.com/cpomaresnina/Places_Ratings
