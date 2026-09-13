# Data Patterns

**Page type:** grid page (tutorials category grid: single flat 4-column nav-grid)
**HTML title tag:** Data Patterns

**Subtitle:** How data is sized, moved, and shaped — the recurring layouts and flows every data system is built from.

## Cards

Each card links to a topic page under `data-patterns/`. The card shows a colored uppercase subcategory label (`.card-num`), a numbered title, a one-line description, and a row of topic-tag pills. All cards sit in one flat `.nav-grid`; the colored labels carry the grouping.

| # | Category | Title | Link | Description | Tags |
|---|----------|-------|------|-------------|------|
| 1 | SIZE REGIMES | Small Data | [22-data-patterns/01-small-data.md](22-data-patterns/01-small-data.md) | Data that fits in a spreadsheet or a laptop's memory — where the simplest tools are also the best ones. | fits in memory, spreadsheets, simple tools |
| 2 | SIZE REGIMES | Medium Data: One Machine Is Enough | [22-data-patterns/02-medium-data-one-machine-is-enough.md](22-data-patterns/02-medium-data-one-machine-is-enough.md) | Too big for a spreadsheet but comfortable on a single machine — a database and some patience go a long way. | single machine, databases, indexing |
| 3 | SIZE REGIMES | Big Data: Splitting Across Machines | [22-data-patterns/03-big-data-splitting-across-machines.md](22-data-patterns/03-big-data-splitting-across-machines.md) | When no single machine can hold or process the data, the work gets divided across many machines that share the job. | distributed, partitioning, clusters |
| 4 | SIZE REGIMES | When "Big Data" Is the Wrong Tool | [22-data-patterns/04-when-big-data-is-the-wrong-tool.md](22-data-patterns/04-when-big-data-is-the-wrong-tool.md) | Distributed systems add cost and complexity — often a sample, a summary, or one beefy machine answers the question faster. | overkill, sampling, cost of complexity |
| 5 | MOVEMENT PATTERNS | Batch Processing | [22-data-patterns/05-batch-processing.md](22-data-patterns/05-batch-processing.md) | Collect data over a period, then process it all at once on a schedule — like doing laundry once a week instead of per sock. | scheduled jobs, nightly runs, throughput |
| 6 | MOVEMENT PATTERNS | Streaming | [22-data-patterns/06-streaming.md](22-data-patterns/06-streaming.md) | Process each piece of data the moment it arrives, so answers stay seconds fresh instead of a day old. | real-time, low latency, continuous |
| 7 | MOVEMENT PATTERNS | Event Streams: Everything Is a Log | [22-data-patterns/07-event-streams-everything-is-a-log.md](22-data-patterns/07-event-streams-everything-is-a-log.md) | Record every change as an event in an ordered, append-only log — the full story of what happened, in the order it happened. | append-only log, ordering, replay |
| 8 | MOVEMENT PATTERNS | Pub/Sub | [22-data-patterns/08-pub-sub.md](22-data-patterns/08-pub-sub.md) | Producers publish messages to a topic and any number of subscribers pick them up — senders never need to know who is listening. | topics, decoupling, fan-out |
| 9 | MOVEMENT PATTERNS | Change Data Capture | [22-data-patterns/09-change-data-capture.md](22-data-patterns/09-change-data-capture.md) | Watch a database's own change log and forward every insert, update, and delete to other systems as it happens. | database log, sync, replication |
| 10 | SHAPE PATTERNS | Transactional Records vs Event Logs | [22-data-patterns/10-transactional-records-vs-event-logs.md](22-data-patterns/10-transactional-records-vs-event-logs.md) | A record stores the current state of a thing; an event log stores every step that led there — two views of the same reality. | current state, history, audit trail |
| 11 | SHAPE PATTERNS | Wide vs Long Tables | [22-data-patterns/11-wide-vs-long-tables.md](22-data-patterns/11-wide-vs-long-tables.md) | The same data can sit as one column per measure or one row per measurement — each shape makes different work easy. | pivot, melt, tidy data |
| 12 | SHAPE PATTERNS | Snapshots vs Deltas | [22-data-patterns/12-snapshots-vs-deltas.md](22-data-patterns/12-snapshots-vs-deltas.md) | Save a full copy of everything each time, or save only what changed since last time — a storage vs reconstruction trade-off. | full copy, incremental, point-in-time |
| 13 | SHAPE PATTERNS | Append-Only vs Update-in-Place | [22-data-patterns/13-append-only-vs-update-in-place.md](22-data-patterns/13-append-only-vs-update-in-place.md) | Add a new row for every change and keep the old ones, or overwrite the existing row — keeping history versus keeping it simple. | immutability, overwrite, versioning |

## Regeneration instructions

To rebuild the HTML from this spec:

- **Template:** tutorials category grid page (nav-grid style, see `docs/statsml/ui-templates/02-nav-grid`). Single page: h1, `.subtitle` paragraph, then one flat `.nav-grid` of `.nav-card` anchors (no h2 section headings).
- **Layout:** `.nav-grid` is CSS grid, `repeat(4, 1fr)`, 16px gap, 15px top margin; responsive: 3 columns below 1400px, 2 below 1100px, 1 below 600px.
- **Links:** the tables above link to `.md` versions for markdown navigation; in the regenerated HTML, each card's `href` is the same path with an `.html` extension instead (files zero-padded, e.g. `data-patterns/01-small-data.html`).
- **Card structure:** `<a class="nav-card" href="...">` containing `<div class="card-num">SUBCATEGORY</div>`, `<h3>N. Title</h3>` (unpadded index number matching the file index, running 1..13 across the whole page), `<p>description</p>`, and `<div class="topics">` with one `<span class="topic-tag">` per tag.
- **Category label colors** (applied by a small script mapping `.card-num` text to color): SIZE REGIMES `#2980b9`, MOVEMENT PATTERNS `#27ae60`, SHAPE PATTERNS `#8e44ad`; default `.card-num` color `#2980b9`.
- **Card style:** background `#ffffff`, border `1px solid #d8d8d8`, radius 10px, padding 20px, shadow `0 2px 4px rgba(0,0,0,0.05)`; hover: border `#2980b9`, `translateY(-2px)`. `.card-num` 0.75em bold; h3 `#1a3a4a` 1em; description `#555` 0.85em. `.topic-tag`: background `#f0f0f0`, border `1px solid #ccc`, radius 4px, padding 2px 6px, 0.7em, `#666`; `.topics` is flex with 4px gap, 8px top margin.
- **Page style:** body system sans-serif, background `#f5f5f0`, text `#2a2a2a`, padding 40px, line-height 1.6; h1 1.8em `#2980b9`; subtitle `#666` 1.05em. No nav bar, no back/home links.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- No canvases on this page; where canvases appear elsewhere in this project they use `window.devicePixelRatio` scaling.
