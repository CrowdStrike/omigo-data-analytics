# Data Ingestion & ETL

**Page type:** grid page (card navigation grid, 4 columns, one philosophy callout, cards with category labels and topic tags)
**HTML title tag:** Data Ingestion & ETL — ML Pipeline Pitfalls

**Subtitle:** The pipeline runs green and the row count is wrong. Duplication, fanout, partial windows, and reused ids — damage done before any model sees the data.

## Callout (philosophy box)

**Why this matters:** Every pitfall here happens upstream of the first line of modeling code, and every one of them is silent. A job that succeeds is not a job that loaded the right rows: a join can multiply them, a retry can double them, a window can close early, and an id can be handed to a second entity after the first was deleted. Nothing throws. The dataset simply describes a world that never happened, and every number computed downstream is arithmetically correct about fiction.

## Cards

Each card links to a detail page under `01-data-ingestion-and-etl/`. The card shows a colored uppercase category label, a numbered title, a one-line description, and a row of small topic tags.

| # | Category | Title | Link | Description | Topic tags |
|---|----------|-------|------|-------------|------------|
| 1 | ETL | Duplicate / Near-Duplicate Records | [01-data-ingestion-and-etl/01-duplicate-near-duplicate-records.md](01-data-ingestion-and-etl/01-duplicate-near-duplicate-records.md) | Same record in train and test = memorization leakage | duplicates, dedup, memorization |
| 2 | ETL | Data Survivorship (Only Recent Records Kept) | [01-data-ingestion-and-etl/02-data-survivorship-only-recent-records-kept.md](01-data-ingestion-and-etl/02-data-survivorship-only-recent-records-kept.md) | Archived/deleted data biases what remains | survivorship, retention, bias |
| 3 | ETL | Duplicate Inflation in ETL | [01-data-ingestion-and-etl/03-duplicate-inflation-in-etl.md](01-data-ingestion-and-etl/03-duplicate-inflation-in-etl.md) | Duplicates from joins/retries/bugs inflate counts and skew analysis | ETL, fanout, inflation |
| 4 | ETL | Unquantified Spikes (Memory / Skew Failures) | [01-data-ingestion-and-etl/04-unquantified-spikes-memory-skew-failures.md](01-data-ingestion-and-etl/04-unquantified-spikes-memory-skew-failures.md) | Data spikes break processing without warning | skew, OOM, profiling |
| 5 | ETL | No Data Quality Monitoring Over Time | [01-data-ingestion-and-etl/05-no-data-quality-monitoring-over-time.md](01-data-ingestion-and-etl/05-no-data-quality-monitoring-over-time.md) | Data shifts go unnoticed for months without regular profiling | monitoring, drift, alerting |
| 6 | ETL | Silent Join Fanout (Row Multiplication) | [01-data-ingestion-and-etl/06-silent-join-fanout-row-multiplication.md](01-data-ingestion-and-etl/06-silent-join-fanout-row-multiplication.md) | A many-to-many join silently multiplies rows | joins, fanout, row-count |
| 7 | ETL | Index / ID Reuse | [01-data-ingestion-and-etl/07-index-id-reuse.md](01-data-ingestion-and-etl/07-index-id-reuse.md) | IDs get recycled or reset — joins match wrong records | IDs, UUID, joins |
| 8 | ETL | Partial Data (Incomplete ETL Window) | [01-data-ingestion-and-etl/08-partial-data-incomplete-etl-window.md](01-data-ingestion-and-etl/08-partial-data-incomplete-etl-window.md) | Analysis runs before all data arrives — biased toward early arrivals | completeness, late-arriving, SLA |

## Regeneration instructions

- **Template:** nav-grid style (see `docs/statsml/ui-templates/02-nav-grid`). Single page: h1, `.subtitle`, a `.philosophy` callout, then one `.nav-grid` of `.nav-card` anchors.
- **Layout:** `.nav-grid` is CSS grid, `repeat(4, 1fr)`, 16px gap, margin-top 15px; responsive: 3 columns below 1400px, 2 columns below 1100px, 1 column below 600px.
- **Links:** the table links to `.md` versions for markdown navigation; in the HTML each card's `href` is the same path with an `.html` extension.
- **Card structure:** `<a class="nav-card" href="...">` containing `<div class="card-num">CATEGORY</div>`, `<h3>N. Title</h3>` (unpadded index matching the file index), `<p>description</p>`, then `<div class="topics">` of `<span class="topic-tag">` per topic.
- **Category label colors:** inline script maps `.card-num` text through a `categoryColors` object and sets `el.style.color`: ETL `#16a085`. Base `.card-num` style is `#2980b9`, 0.75em, bold.
- **Page style:** body system sans-serif, background `#f5f5f0`, text `#2a2a2a`, padding 40px, line-height 1.6; h1 1.8em `#2980b9`; subtitle `#666` 1.05em. Philosophy callout background `#f0f4f8`, left border `4px solid #2980b9`. No nav bar, no back/home links.
