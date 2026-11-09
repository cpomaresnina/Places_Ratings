# Business category mapping — draft v0.2

Status: veterinary care and dog grooming are grouped by user decision; bars/pubs remain separate from nightclubs. Other detailed boundaries remain proposed. Do not update the live extraction configuration until reviewed. No requests were made to validate local coverage for the expanded mappings.

## Principles

- A study category is not the same as a Google type. One category can use multiple retrieval types.
- Retrieval produces candidates. Silver assigns analytical membership with a documented reason; keep original types and query provenance.
- Do not assume that every specialized type also has its broad counterpart. Check type arrays and use bounded supplementary probes to evaluate omissions.
- Nearby Search with multiple included types shares one result cap; it does not return 20 per type. Combining types can improve vocabulary coverage while still truncating dense categories.
- Overlapping business categories are allowed. Deduplicate observations by place ID without erasing category memberships.
- Do not filter by minimum review count, rating or open-now status during collection.
- Preserve the original exploratory files. A changed request or mapping is a new extraction definition, not a correction to old source responses.

## Proposed mapping

| Study category | Intended business scope | Initial retrieval types | Supplementary candidates / queries | Silver review boundary |
|---|---|---|---|---|
| Cafés | Public-facing cafés and coffee shops, including coffee-focused takeaway outlets | cafe, coffee_shop, coffee_stand | cafeteria, tea_house; coffee_roastery only with evidence of café service | Bakeries, cake and pastry shops do not qualify solely because they sell food. Institutional cafeterias are outside the intended scope. A business may also be a restaurant. |
| Restaurants | Public-facing establishments preparing meals, including fast-food and takeaway formats | restaurant | Check observed specialized restaurant types without restaurant in their type array; add targeted types based on evidence rather than claiming the broad filter covers all subtypes | Food stores, delivery-only intermediaries and supermarkets are not automatically restaurants. A food-court container is not interchangeable with each tenant. |
| Hair salons and barbers | Haircutting, hairstyling, colouring and barber services | hair_salon, barber_shop, hair_care | beauty_salon as a supplementary candidate search | Beauty-only, nail-only and makeup-only businesses need evidence of hair services before inclusion. |
| Gyms | General physical training and fitness facilities | gym, fitness_center | sports_club as a supplementary candidate search | Yoga-only, dance-only and sport-specific facilities remain outside this definition unless they also operate a general gym. |
| Veterinary care and dog grooming | Businesses offering veterinary clinical care and/or dog washing, trimming or grooming | veterinary_care, pet_care | Optional text probes later if known grooming businesses are missed; no separate grooming category | pet_care also includes unrelated services. Boarding-only and pet-retail-only businesses do not qualify without evidence of veterinary or grooming services. Record service subtype as veterinary, grooming, both or pending; a type match alone does not establish the specific service. |
| Nightclubs | Establishments primarily offering dancing and nightclub activity | night_club | dance_hall as a supplementary candidate search | Do not automatically include event halls, dance schools or ordinary bars. |
| Bars and pubs | Public-facing bars, pubs, lounge and related drinking venues | bar, pub, cocktail_bar, wine_bar, sports_bar, lounge_bar, bar_and_grill, beer_garden, brewpub, gastropub, irish_pub | karaoke and live_music_venue only as supplementary candidate searches | Do not automatically include production-only breweries, concert venues or every restaurant serving alcohol. Keep subtypes for analysis. |

## Decisions still requiring review

1. Approve the intended business scopes above, including coffee takeaway outlets and general fitness facilities.
2. Keep bars/pubs separate from nightclubs and merge veterinary care with grooming: seven study categories remain.
3. Test supplementary candidates on a bounded pilot before promoting them to the main search set.
4. Dog grooming has no separate extraction branch in this version. veterinary_care and pet_care retrieve candidates for the combined category; coverage of grooming remains uncertain. If supplementary Text Search is later needed, document its different method.
5. Configuration now declares mapping_version 0.2. The current collector does not yet persist that field; add it to extraction metadata before running this mapping. Preserve method, types and geographic parameters as well.
6. Decide the snapshot date window across zones and categories before collecting the study dataset. Avoid mixing historical exploratory observations silently with new snapshots.

## Proposed coverage check

For each category, inspect a small sample of names, primary types and all types; identify known local businesses absent from results; probe only justified extra types or terms; measure additional unique IDs and irrelevant candidates. These checks evaluate retrieval quality, not business demand or the effectiveness of NFC.

An invalid or irrelevant returned record can be handled in Silver. A business never retrieved cannot be recovered by cleaning. Neither supplementary searches nor a non-saturated response guarantees a complete census.

## Sources

- Google Places supported types: https://developers.google.com/maps/documentation/places/web-service/place-types
- Nearby Search: https://developers.google.com/maps/documentation/places/web-service/nearby-search
- Text Search: https://developers.google.com/maps/documentation/places/web-service/text-search

Type availability is documented by Google. Study definitions, inclusion boundaries and the proposed supplementary search strategy are project design decisions, not Google's category guarantees.
