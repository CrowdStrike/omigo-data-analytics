# Publicly Available Data

**Page type:** grid page (nav-card grid, auto-fit columns min 300px)
**HTML title tag:** Publicly Available Data — What's Out There and Why It's Public

**Subtitle:** An enormous amount of data about people, places, companies, and machines is publicly readable — grouped here by why it is public: legal mandate, government publication, broadcast physics, open imagery, market disclosure, volunteered content, and research corpora.

## Cards

Each card links to a detail page under `publicly-available-data/`. The card shows a colored uppercase category label, a numbered title, a description, and a row of topic tag pills.

| # | Category | Title | Link | Description | Topics |
|---|----------|-------|------|-------------|--------|
| 1 | LEGAL MANDATE | Public-by-Legal-Mandate Records | [publicly-available-data/01-legal-mandate-records.md](publicly-available-data/01-legal-mandate-records.md) | Deeds, land registries, court records, business filings, voter rolls, licenses — public because openness is what makes the legal function work. Nordic countries even publish individual tax figures. | property, courts, registries |
| 2 | GOVERNMENT | Government Statistical & Open-Data Portals | [publicly-available-data/02-government-open-data.md](publicly-available-data/02-government-open-data.md) | Census bureaus, national statistics offices, economic series, weather, health, crime — governments publishing aggregates and portals by policy, worldwide. | census, FRED / World Bank, open portals |
| 3 | BROADCAST | Live Sensor & Broadcast Feeds | [publicly-available-data/03-broadcast-sensor-feeds.md](publicly-available-data/03-broadcast-sensor-feeds.md) | Aircraft, ships, transit, earthquakes, webcams — public because the signal is literally in the air, and anyone with an antenna or an API key can listen. | ADS-B, AIS, GTFS |
| 4 | IMAGERY | Imagery & Mapping | [publicly-available-data/04-imagery-mapping.md](publicly-available-data/04-imagery-mapping.md) | Street View and its global equivalents, free satellite programs like Landsat and Sentinel, OpenStreetMap, crowdsourced street-level photos — the surface of the planet, browsable. | street view, satellite, OSM |
| 5 | MARKET | Corporate & Market Data Exhaust | [publicly-available-data/05-corporate-market-exhaust.md](publicly-available-data/05-corporate-market-exhaust.md) | Stock prices and filings, crypto ledgers public by design, job postings, WHOIS and certificate transparency logs, prices and reviews — commerce leaves a publicly browsable trail. | filings, blockchains, cert logs |
| 6 | VOLUNTEERED | User-Generated & Crowdsourced Data | [publicly-available-data/06-user-generated-crowdsourced.md](publicly-available-data/06-user-generated-crowdsourced.md) | Public social posts, Wikipedia, reviews, public code activity, fitness heatmaps, wardriving maps — volunteered by individuals, aggregated into datasets nobody individually consented to. | social, Strava heatmap, WiGLE |
| 7 | RESEARCH | Scientific & Research Corpora | [publicly-available-data/07-scientific-research-corpora.md](publicly-available-data/07-scientific-research-corpora.md) | Paper archives, genomic databases, public DNA matching, web crawls, astronomy surveys, the archived web — published to advance science, reused far beyond it. | arXiv / PubMed, GenBank, Common Crawl |

## Regeneration instructions

To rebuild the HTML from this spec:

- **Template:** nav-grid style (see `docs/statsml/ui-templates/02-nav-grid`). Single page: h1, `.subtitle` paragraph, then one `.nav-grid` of `.nav-card` anchors. No canvases on this page.
- **Layout:** `.nav-grid` is CSS grid, `repeat(auto-fit, minmax(300px, 1fr))`, 16px gap, 15px top margin.
- **Links:** the table above links to the `.md` versions for navigation in markdown; in the regenerated HTML, each card's `href` is the same path with an `.html` extension instead.
- **Card structure:** `<a class="nav-card" href="...">` containing `<div class="card-num" style="color:CATEGORY_COLOR">CATEGORY</div>`, `<h3>N. Title</h3>` (unpadded index number matching the file index), `<p>description</p>`, then `<div class="topics">` with one `<span class="topic-tag">` per topic.
- **Category label colors:** LEGAL MANDATE and VOLUNTEERED `#8e44ad`; GOVERNMENT and RESEARCH `#2980b9`; BROADCAST `#27ae60`; IMAGERY `#e67e22`; MARKET `#e74c3c`.
- **Card style:** background `#f8f9fa`, border `1px solid #e0e0e0`, radius 4px, padding 20px; hover: border `#2980b9`, `translateY(-2px)`; transition on border-color and transform 0.2s. `.card-num` 0.75em weight 600; h3 `#1a5276` 1em; description `#555` 0.85em. `.topic-tag` pills: background `#eaf2f8`, color `#1a5276`, radius 4px, padding 2px 8px, 0.72em, weight 600; `.topics` is a flex row with 6px gap, wrap, 8px top margin.
- **Page style:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` bottom border; subtitle `#666` 0.95rem. No nav bar, no back/home links.
- **Palette:** `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- Canvases (none here) would use `window.devicePixelRatio` scaling.
