# Where to Get Data

**Page type:** grid page (nav-card grid, auto-fit columns min 300px)
**HTML title tag:** Where to Get Data — Every Acquisition Channel

**Subtitle:** A newcomer's map of every channel for getting hold of data — grouped by how you get it: earn it on the job, buy it, download it, build the instrument, harvest it, ask people, or generate it.

## Cards

Each card links to a detail page under `data-acquisition-methods/`. The card shows a colored uppercase category label, a numbered title, a one-sentence description, and a row of topic tag pills.

| # | Category | Title | Link | Description | Topics |
|---|----------|-------|------|-------------|--------|
| 1 | EARN | Proprietary Data on the Job | [data-acquisition-methods/01-proprietary-company-data.md](data-acquisition-methods/01-proprietary-company-data.md) | Join a company and inherit its logs, transactions, and telemetry — the largest datasets most people ever touch. | internal telemetry, logs, CRM |
| 2 | BUY | Commercial Data Feeds | [data-acquisition-methods/02-commercial-data-feeds.md](data-acquisition-methods/02-commercial-data-feeds.md) | Subscribe to vendor feeds — cybersecurity threat intelligence, market-data terminals, consumer panels. | threat intel, market data, panels |
| 3 | BUY | Data Brokers & Marketplaces | [data-acquisition-methods/03-data-brokers-marketplaces.md](data-acquisition-methods/03-data-brokers-marketplaces.md) | Buy datasets outright from cloud marketplaces, people-data brokers, and alternative-data sellers. | marketplaces, brokers, alt-data |
| 4 | DOWNLOAD | Curated Dataset Repositories | [data-acquisition-methods/04-dataset-repositories.md](data-acquisition-methods/04-dataset-repositories.md) | Ready-to-load datasets one download away — Kaggle, Hugging Face, UCI, OpenML. | Kaggle, Hugging Face, UCI |
| 5 | DOWNLOAD | Publicly Available Data | [data-acquisition-methods/05-publicly-available-data.md](data-acquisition-methods/05-publicly-available-data.md) | Everything publicly readable — government portals, public records, broadcast signals, imagery, research corpora. | open data, public records, government |
| 6 | DOWNLOAD | ML Competitions | [data-acquisition-methods/06-ml-competitions.md](data-acquisition-methods/06-ml-competitions.md) | Competition platforms hand you cleaned data and a scored problem — Kaggle, KDD Cup, DrivenData. | Kaggle, Netflix Prize, leaderboards |
| 7 | BUILD | Instrumenting Your Own Website | [data-acquisition-methods/07-web-product-instrumentation.md](data-acquisition-methods/07-web-product-instrumentation.md) | Embed a tracking snippet in your own pages and every visit becomes an event stream. | GA4, Segment, first-party events |
| 8 | BUILD | Mobile Apps & Phone Sensors | [data-acquisition-methods/08-mobile-apps-phone-sensors.md](data-acquisition-methods/08-mobile-apps-phone-sensors.md) | Ship an app and collect telemetry and sensor readings from millions of phones — with permission. | app telemetry, GPS, ResearchKit |
| 9 | BUILD | Hardware & Sensor Fleets | [data-acquisition-methods/09-hardware-sensor-fleets.md](data-acquisition-methods/09-hardware-sensor-fleets.md) | Deploy physical collectors — vehicle fleets, weather stations, wearables — and own a dataset nobody else has. | dashcams, IoT, wearables |
| 10 | HARVEST | Web Scraping & Crawl Archives | [data-acquisition-methods/10-scraping-web-archives.md](data-acquisition-methods/10-scraping-web-archives.md) | Extract data from pages built for humans, or start from pre-crawled archives like Common Crawl. | Scrapy, Common Crawl, legality |
| 11 | HARVEST | APIs & Web Services | [data-acquisition-methods/11-apis-web-services.md](data-acquisition-methods/11-apis-web-services.md) | Pull structured data from official endpoints — and navigate rate limits and the closing of the free-API era. | REST APIs, rate limits, keys |
| 12 | ASK | Surveys, Crowdsourcing & Annotation | [data-acquisition-methods/12-surveys-crowdsourcing-annotation.md](data-acquisition-methods/12-surveys-crowdsourcing-annotation.md) | Pay people to answer questions or label examples — survey panels, crowd platforms, annotation vendors. | surveys, crowd platforms, labeling |
| 13 | GENERATE | Synthetic Data — LLMs & Simulation | [data-acquisition-methods/13-synthetic-data-llm-simulation.md](data-acquisition-methods/13-synthetic-data-llm-simulation.md) | Generate training data from LLMs or simulators when the real thing is scarce — with known failure modes. | distillation, simulators, model collapse |
| 14 | ASK | Negotiated & Restricted Access | [data-acquisition-methods/14-negotiated-restricted-access.md](data-acquisition-methods/14-negotiated-restricted-access.md) | Sign a data-use agreement, join a research program, or file a records request for data that is never simply downloadable. | DUAs, credentialed access, FOIA |

## Regeneration instructions

To rebuild the HTML from this spec:

- **Template:** nav-grid style (see `docs/statsml/ui-templates/02-nav-grid`). Single page: h1, `.subtitle` paragraph, then one `.nav-grid` of `.nav-card` anchors. No canvases on this page.
- **Layout:** `.nav-grid` is CSS grid, `repeat(auto-fit, minmax(300px, 1fr))`, 16px gap, 15px top margin.
- **Links:** the table above links to the `.md` versions for navigation in markdown; in the regenerated HTML, each card's `href` is the same path with an `.html` extension instead.
- **Card structure:** `<a class="nav-card" href="...">` containing `<div class="card-num" style="color:CATEGORY_COLOR">CATEGORY</div>`, `<h3>N. Title</h3>` (unpadded index number matching the file index), `<p>description</p>`, then `<div class="topics">` with one `<span class="topic-tag">` per topic.
- **Category label colors:** EARN `#1a5276`; BUY `#e74c3c`; DOWNLOAD `#2980b9`; BUILD `#27ae60`; HARVEST `#e67e22`; ASK `#8e44ad`; GENERATE `#16a085`.
- **Card style:** background `#f8f9fa`, border `1px solid #e0e0e0`, radius 4px, padding 20px; hover: border `#2980b9`, `translateY(-2px)`; transition on border-color and transform 0.2s. `.card-num` 0.75em weight 600; h3 `#1a5276` 1em; description `#555` 0.85em. `.topic-tag` pills: background `#eaf2f8`, color `#1a5276`, radius 4px, padding 2px 8px, 0.72em, weight 600; `.topics` is a flex row with 6px gap, wrap, 8px top margin.
- **Page style:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` bottom border; subtitle `#666` 0.95rem. No nav bar, no back/home links.
- **Palette:** `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- Canvases (none here) would use `window.devicePixelRatio` scaling.
