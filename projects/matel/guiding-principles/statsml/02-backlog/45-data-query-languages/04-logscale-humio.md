# 4. LogScale (Humio)

**Page type:** detail page (two-column layout table per section: text left 45%, viz right 55%; intro callout; closing key-point)
**HTML title tag:** 4. LogScale (Humio)

**Subtitle:** Real-time log search at scale with streaming aggregation

## Intro callout

Ingest everything, query anything, in real time. Schema-on-read with streaming aggregation — no pre-indexing needed. The bet: brute-force search on compressed data beats maintaining indexes.

## 1. How It Works

A log management and observability platform (formerly Humio, acquired by CrowdStrike) that uses index-free streaming search over compressed data for real-time analysis.

- **Index-free:** No pre-built indexes — data is compressed and stored efficiently
- **Brute-force scan:** Queries scan the compressed data in real time
- **Streaming aggregation:** Aggregation happens as events stream through — no batch step required

**Key-point callout:** **The bet:** Brute-force search on compressed data beats maintaining indexes — no schema management or index tuning, at the cost of slower historical queries on cold data.

### Visualization (canvas `c1`, 720×300)

Box-and-arrow pipeline diagram: LogScale streaming query stages with event counts shrinking left to right.

- **Title (bold 14px, `#1a5276`, top center, y=25):** "LogScale Pipeline: Streaming Scan of Compressed Data — No Index"
- **Sub-caption (13px, `#444`, center, y=80):** "events stream left to right — aggregation runs as they pass through"
- **Stages:** 5 boxes, 116×56 px, 30px gap, starting at x=10, y=110. Each box has two lines of text (bold 13px line 1, 13px line 2) and a bold red (`#e74c3c`) count label 26px below the box:
  1. `#type=` / "accesslog scan" — count "25M events" — fill `rgba(26,82,118,0.35)`
  2. `status` / ">= 500" — count "41K events" — fill white
  3. `groupBy` / "url, method" — count "580 groups" — fill white
  4. `count` / "> 50" — count "14 groups" — fill white
  5. `sort` / "count desc" — count "14 rows" — fill `#27ae60` (white text)
- All boxes stroked `#1a5276`, width 1.5. Box text `#222` except final green box (white).
- **Arrows:** orange (`#e67e22`) horizontal lines with triangular arrowheads between consecutive boxes, at box mid-height.
- **Bottom annotation (13px, `#444`, center, y=265):** "Index-free: brute-force scan of compressed segments; aggregation runs as events stream through"

## 2. Where It Fits

- **Strength:** Real-time streaming results (sub-second on recent data) with live tail of events as they arrive; handles extremely high ingest rates
- **Strength:** No schema management or index tuning; low storage cost via aggressive compression
- **Weakness:** Smaller ecosystem than Splunk or Elastic; limited community resources; less mature enrichment and correlation capabilities
- **Weakness:** CrowdStrike acquisition shifts roadmap toward security use cases; historical queries on cold data can be slower
- **Use case:** Security log analysis at scale, observability for high-throughput systems, real-time alerting on streaming data, high-volume event correlation in SOC environments

*Example: find url + method combinations with more than 50 server errors, sorted by count.*

**Code block (in viz column, above canvas `c2`):**

```
#type=accesslog
| status >= 500
| groupBy(field=[url, method], function=count())
| count > 50
| sort(count, order=desc)
```

### Visualization (canvas `c2`, 720×300)

Vertical bar chart: 5xx error counts by url + method, sorted descending.

- **Title (bold 14px, `#1a5276`, top center, y=25):** "Query Result: 5xx Count by url + method (count > 50, sorted desc)"
- **Data:** urls `['/api/payment', '/api/items', '/login', '/search', '/health', '/upload']`, methods `['POST', 'GET', 'POST', 'GET', 'GET', 'POST']`, counts `[320, 240, 180, 120, 75, 58]`.
- **Scale:** max 350; bars 74px wide, 40px gap, group centered horizontally; baseline y=248, max bar height 190px.
- **Colors:** first two bars red `#e74c3c`; remaining bars `rgba(26,82,118,0.35)`.
- **Labels:** count value in bold 13px `#222` above each bar; url label (13px `#222`) 20px below baseline; method label (13px `#444`) 38px below baseline.

## Closing key-point

**Takeaway:** The bet is that brute-force streaming search on compressed data beats maintaining indexes — ingest everything, query anything, in real time.

## Regeneration instructions

- **Template/layout:** data-query-languages detail page. h1 with 2px `#2980b9` bottom border, `.subtitle`, one `.intro` callout, then one `.lang-section` per numbered section (h2 with 2px `#2980b9` bottom border), each containing a `table.layout` with one row: left `td.text-col` (45%) for paragraph/bullets/key-point/example, right `td.viz-col` (55%) for optional `<pre>` code block and canvas. A standalone `.key-point` div at the bottom holds the takeaway.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; `.subtitle` `#666` 0.95rem; `.intro` background `#f0f4f8`, left border `3px solid #2980b9`, padding 8px 12px, 0.9rem; `.key-point` background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem; `.example` italic `#555` 0.9rem; ul 0.92rem; `pre` background `#f4f4f4`, padding 12px 16px, radius 4px, 0.85rem.
- **Canvas:** intrinsic 720×300, CSS `width: 100%`, border `1px solid #e0e0e0`, radius 4px; scaled with `window.devicePixelRatio` via a shared `setup(id)` helper (backing store = rendered width × dpr, `ctx.scale` back to logical coordinates).
- **Palette:** `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange, bar fill `rgba(26,82,118,0.35)`, text `#222`/`#444`.
- No nav bar, no back/home links. In regenerated HTML, any card links use `.html` extensions.
