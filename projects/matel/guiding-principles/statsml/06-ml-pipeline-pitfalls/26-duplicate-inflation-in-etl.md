# Pitfall: Duplicate Inflation in ETL

**Page type:** detail page (card-sections with h2 headers, two-column layout table per section: text left 45%, canvas right 55%)
**HTML title tag:** Duplicate Inflation in ETL

**Subtitle:** Event retries, at-least-once delivery, and overlapping incremental loads write the same event multiple times, inflating counts and skewing analysis

## The Problem

Tags: `the trap` (red), `duplicate events` (blue)

- **Silent re-writes** — pipelines store one event twice via retries, redelivery, or overlap
- **Client retries** — a timed-out request often succeeded, so the resend creates a second row
- **At-least-once delivery** — brokers like Kafka guarantee delivery, not uniqueness
- **Missing idempotency key** — without a unique event_id at write time, repeats are stored
- **Overlapping windows** — incremental jobs reprocessing a safety window re-insert old rows
- **Biased statistics** — every duplicate inflates counts and skews stats over raw rows

*Example:* A nightly job appends a 3-day overlap without dedup, so the conversion rate reads 28% instead of the true 2.8%.

**Impact:** Counts inflate silently, rates are biased whenever duplicates cluster in segments, and models learn distorted distributions.

### Visualization (canvas `c1`, 720×300)

Flow diagram: producer → broker → events table, with retry and redelivery paths creating duplicate rows.

- **Title (bold 14px, top center, `#1a5276`):** "One Real Event, Three Stored Rows".
- **Producer box:** at x≈90, 110×40, fill `rgba(26,82,118,0.7)`, stroke `#1a5276`; white labels bold 11px "PRODUCER" and 10px "event e-42".
- **Broker box:** at x≈320, 110×40, fill `rgba(230,126,34,0.7)`, stroke `#e67e22`; white labels bold 11px "BROKER" and 10px "at-least-once".
- **Producer → broker arrows:** solid blue (`#1a5276`, width 2) line labeled "send" (9px blue); dashed red (`#e74c3c`, dash 5/3) line labeled "retry after timeout" (9px red).
- **Broker → table arrows:** solid orange (`#e67e22`, width 2) line labeled "deliver"; dashed red line labeled "redeliver (ack lost)".
- **Events table (x≈560):** heading bold 12px `#1a5276` "EVENTS TABLE"; three stacked 130×30 rows each containing bold 10px monospace "event_id = e-42". Row 1 fill `rgba(39,174,96,0.5)` stroke `#27ae60`, annotated "real" (9px green); rows 2-3 fill `rgba(231,76,60,0.4)` stroke `#e74c3c`, each annotated "duplicate" (9px red).
- **Bottom warning (bold 11px red, centered):** "COUNT(*) = 3, but only 1 real event — no idempotency key enforced".

## Why It Happens

Tags: `root cause` (orange), `at-least-once` (blue)

- **Reliability first** — delivering twice is far cheaper than exactly-once across systems
- **At-least-once semantics** — brokers redeliver whenever an ack fails; dupes are expected
- **Retry-first SDKs** — clients retry on timeout even though the request may have succeeded
- **No uniqueness constraint** — many producers share one table with nothing rejecting repeats
- **Overlap-by-design backfills** — trailing windows catch late data but re-ingest old rows
- **Consumer's job** — dedup falls to the pipeline, and many pipelines never implement it

*Example:* A payment webhook fires up to 3 times, so about 4% of events land twice and raw-row revenue sums over-report by that share.

**Root Cause:** Exactly-once delivery is expensive to guarantee, so infrastructure settles for at-least-once and relies on downstream deduplication that many pipelines skip.

### Visualization (canvas `c2`, 720×300)

Grouped bar chart: COUNT(*) vs COUNT(DISTINCT id) by pipeline stage, showing count inflation growing per stage.

- **Title (bold 14px, top center, `#1a5276`):** "Duplicate Detection: Count(*) vs Count(Distinct)".
- **Plot area:** left=80, right=640, top=70, bottom=240; gray (`#999`) L-shaped axes with light gray (`#e0e0e0`) gridlines; x label "Pipeline Stage", rotated y label "Row Count"; y ticks 0 to 2500 in steps of 500.
- **Data (bar pairs, width 50 each, value labels bold 10px above bars):**

| Stage | COUNT(*) (red) | COUNT(DISTINCT) (green) |
|---|---|---|
| Source | 1000 | 1000 |
| After Client Retries | 1100 | 1000 |
| After Broker Redelivery | 1200 | 1000 |
| After Backfill Overlap | 1500 | 1000 |

- **Colors:** total bars fill `rgba(231,76,60,0.7)` stroke `#e74c3c`; distinct bars fill `rgba(39,174,96,0.7)` stroke `#27ae60`. Stage labels 10px `#2c3e50` below the axis (two lines where noted).
- **Legend (top-left inside plot):** red swatch "COUNT(*)", green swatch "COUNT(DISTINCT id)" (11px).
- **Bottom annotation (bold 10px red, centered):** "Ratio > 1.0 indicates duplicates".

## The Correct Approach

Tags: `the fix` (green), `idempotent writes` (blue)

- **Accept duplicates** — they are inevitable at ingestion; correctness comes from dedup
- **Idempotency keys** — assign a stable event_id at the producer and enforce it via upsert
- **Dedup on ingest** — keep ROW_NUMBER() OVER (PARTITION BY event_id) = 1 before counting
- **Watermark backfills** — merge or delete-and-insert so overlap replaces rows, not appends
- **Detect** — track COUNT(*) vs COUNT(DISTINCT event_id) per stage; ratio > 1.0 means dupes
- **Locate the source** — stage-over-stage row deltas show where duplicates enter

*Example:* One team added an upsert keyed on event_id and the duplicate ratio dropped from 1.2 to 1.0 overnight.

**Fix:** Make every write idempotent, deduplicate before the first aggregation, and monitor the COUNT(*) / COUNT(DISTINCT id) ratio per stage.

### Visualization (canvas `c3`, 720×300)

Side-by-side metric cards comparing inflated vs deduplicated metrics, with SQL snippets and a best-practice callout.

- **Title (bold 14px, top center, `#1a5276`):** "Impact on Metrics: Duplicates vs Deduplicated".
- **Left column (x=60, width 280), heading bold 13px red:** "WRONG (with duplicates)". Three 35px-tall metric rows, fill `#ffe5e5`, stroke `#e74c3c` width 2; label 11px `#2c3e50` left, value bold 14px red right:
  - Total Events — 10,000
  - Conversions — 280
  - Conversion Rate — 28%
  - Caption below (10px red): "Inflated by duplicate events".
- **Right column (x=400, width 280), heading bold 13px green:** "CORRECT (deduplicated)". Same rows, fill `#f0f8f4`, stroke `#27ae60`, values bold 14px green:
  - Total Events — 1,000
  - Conversions — 28
  - Conversion Rate — 2.8%
  - Caption below (10px green): "After dedup on event_id".
- **Delta annotation:** dashed red (`#e74c3c`, dash 5/3) horizontal connector between column centers below the rows, with bold 11px red label "10x inflation!".
- **SQL snippets (bold 11px monospace):** left in red "SELECT COUNT(*) FROM events"; right in green "SELECT COUNT(DISTINCT event_id) FROM events".
- **Best-practice callout (bottom):** box 620×50, fill `#f8f9fa`, stroke `#1a5276` width 2; bold 11px `#1a5276` "Best Practice: Enforce idempotency keys" then 10px "Unique event_id + upsert at write time. Add COUNT(*) vs COUNT(DISTINCT id) per stage to dashboards.".

## Regeneration instructions

- **Layout:** repeated `.card-section` blocks, one per section. Each has an `<h2>` (1.3rem `#1a5276`, bottom border `2px solid #2980b9`) followed by a `table.layout` (full width, border-collapse) with a single `<tr>`: left `td.text-col` (45%) containing `.tags` pills, a `<ul>` of labeled bullets, a `.example` paragraph, and a `.key-point` callout; right `td.viz-col` (55%) containing one `<canvas width="720" height="300">`.
- **Page style:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6. h1 2rem `#1a5276` with `2px solid #2980b9` bottom border. `.subtitle` `#666` 0.95rem. `ul` 0.92rem with `li b` in `#1a5276`. `.example` italic `#555` 0.9rem. `.key-point` background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem.
- **Tag pills:** `.tag` inline-block 0.72rem bold, padding 2px 10px, radius 10px. Colors: blue `rgba(26,82,118,0.12)`/`#1a5276`; green `rgba(39,174,96,0.15)`/`#27ae60`; red `rgba(231,76,60,0.12)`/`#e74c3c`; orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Canvas:** CSS `width: 100%`, border `1px solid #e0e0e0`, radius 4px; intrinsic size 720×300, scaled by `window.devicePixelRatio` via a shared `setup(id)` helper (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, bar fill `rgba(26,82,118,0.35)`.
- No nav bar, no back/home links. In regenerated HTML, any card links use `.html` extensions.
