# Elasticsearch

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Elasticsearch

**Subtitle:** A distributed search engine built on Lucene — it answers "which documents contain this word?" instantly because it stores the index backwards: term → documents, not document → terms

## Finding "battery life" in a Million Reviews

**Tags:** `core idea` (blue), `inverted index` (green), `Lucene` (orange)

- **The store** — an electronics shop holds 1,000,000 product reviews and wants every one mentioning "battery life"
- **The slow way** — a database scan opens each review and reads it word by word: a million reads per search
- **The flip** — an inverted index is built once: for every word, the list of review IDs that contain it
- **The lookup** — "battery" and "life" are two dictionary lookups; intersect the two ID lists and you're done
- **The engine** — Elasticsearch wraps Lucene's inverted index in a distributed, JSON-over-HTTP service

*Example (italic):* The scan touches all 1,000,000 reviews; the inverted index touches 2 posting lists — the search returns in milliseconds no matter how large the data grows.

**Key point:** An inverted index stores term → documents instead of document → terms, so search cost depends on the query's words, not on how many documents exist.

### Visualization (canvas `c1`, 720×300)

Two-row flow diagram contrasting a forward scan (document → terms, reads everything) with an inverted-index lookup (term → documents, reads two lists).

- **Title (bold 15px, `#1a5276`, top center):** "Forward Scan vs Inverted Index: Same Question, Opposite Direction".
- **Row 1 (y=95), label 12px `#444` at x=20:** "forward scan"; blue `#2a78d6` rounded box at x=160 labeled "1,000,000 reviews" (12px), 3px arrow to a red `#e74c3c` box at x=400 labeled "read every word" with bold 12px red "✗ 1,000,000 reads per search" at x=400, y=140.
- **Row 2 (y=205), label:** "inverted index"; green `#008300` rounded box at x=160 labeled "battery → [R1, R3, R5, ...]", green box at x=400 labeled "life → [R1, R5, ...]", 3px arrow to a blue box at x=590 labeled "intersect: R1, R5" with bold 12px green "✓ 2 lookups" at x=590, y=250.
- **Box style:** 150–180px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(231,76,60,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=270):** "the index answers word-first, so document count stops mattering".
- **Caption (12px `#444`, bottom right):** "review counts illustrative".

## Indexing Five Reviews by Hand

**Tags:** `worked example` (blue), `relevance scoring` (green)

- **The corpus** — five reviews: R1 "battery life is great, battery lasts", R2 "screen is sharp", R3 "battery dies by noon", R4 "camera is decent", R5 "long battery life, best battery life"
- **Posting lists** — battery → [R1, R3, R5] (df=3); life → [R1, R5] (df=2); "battery life" hits R1 and R5
- **Rarity weight** — idf = ln(5/df): battery ln(5/3)=0.51, life ln(5/2)=0.92 — rarer words count more
- **The scores** — tf×idf summed: R5 = 2(0.51)+2(0.92) = 2.86; R1 = 2(0.51)+1(0.92) = 1.94; R3 = 1(0.51) = 0.51
- **The ranking** — R5 first, R1 second, R3 last; Elasticsearch's default BM25 refines this same tf/idf idea

*Example (italic):* R5 says "battery life" twice and outscores R1 (2.86 vs 1.94); the ranking is match-any (OR), so battery-only R3 trails at 0.51 instead of dropping out as in the intersection.

**Key point:** Relevance scoring rewards documents where the query's words are frequent (tf) and the words themselves are rare across the corpus (idf) — you can rank five reviews by hand.

### Visualization (canvas `c2`, 720×300)

Horizontal bar chart of tf-idf relevance scores for the three reviews matching "battery life", with the per-term breakdown written on each bar.

- **Title (bold 15px, `#1a5276`, top center):** "Ranking the Query 'battery life': tf × idf per Review".
- **Axis:** vertical 2px `#999` baseline at x=170, bars extend right, max width 460 = score 3.0; x tick labels 12px `#444` at scores 0 / 1.0 / 2.0 / 3.0 with gridlines `#e5e9ef`.
- **Rows (top to bottom at y = 80, 145, 210), each with a left-aligned 12px `#444` label at x=20:**
  - "R5 — battery×2, life×2": green `#008300` bar width 438 (score 2.86), bold 12px green "2.86" at bar end
  - "R1 — battery×2, life×1": blue `#2a78d6` bar width 297 (score 1.94), bold 12px blue "1.94" at bar end
  - "R3 — battery×1, life×0": orange `#d95926` bar width 78 (score 0.51), bold 12px orange "0.51" at bar end
- **Bar style:** 26px tall, fills `rgba(0,131,0,0.30)` / `rgba(42,120,214,0.30)` / `rgba(217,89,38,0.30)` with solid 2px borders in the row color.
- **Annotation (bold 13px magenta `#d55181`, near x=300, y=250):** "life (idf 0.92) outweighs battery (idf 0.51) — rare words rank harder".
- **Caption (12px `#444`, bottom right):** "reviews illustrative; tf-idf arithmetic exact for these counts".

## From Log Search to Analytics Store

**Tags:** `where it's used` (blue), `ELK stack` (green), `aggregations` (orange)

- **The ELK stack** — Elasticsearch + Logstash + Kibana made "grep across every server" a dashboard, not a shell loop
- **Log scale** — a mid-size service emits millions of log lines a day; full-text search over them is the native use case
- **Near real time** — new documents become searchable after a refresh (default every 1s), not instantly on write
- **Aggregations** — bucketing and metrics over the index (errors per hour, p95 latency) turned it into an analytics store
- **One index, two jobs** — the same inverted index answers "find this stack trace" and "chart errors by hour"

*Example (italic):* One aggregation query buckets 2,000,000 log lines from the last 24 hours into errors-per-hour and returns the histogram in under a second.

**Key point:** Search made Elasticsearch popular, but aggregations made it a default analytics store — the ELK stack put both behind one query API for logs.

### Visualization (canvas `c3`, 720×300)

Vertical bar chart: an errors-per-hour aggregation over one day of logs, the classic Kibana-style histogram, with a spike annotated.

- **Title (bold 15px, `#1a5276`, top center):** "One Aggregation Query: Errors per Hour over 2,000,000 Log Lines".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = hours 0–23 with 12px `#444` tick labels at 0/6/12/18/23; y = errors 0 to 800, gridlines `#e5e9ef` at 200/400/600.
- **Bars:** 24 bars, 20px wide, 5px gap, fill `rgba(42,120,214,0.35)` with 1px `#2a78d6` border; hourly counts `[120, 95, 80, 70, 60, 75, 110, 180, 240, 260, 250, 230, 245, 255, 740, 690, 310, 280, 260, 230, 200, 170, 150, 130]`.
- **Spike highlight:** bars for hours 14 and 15 (740 and 690) drawn in red fill `rgba(231,76,60,0.35)` with 1px `#e74c3c` border.
- **Annotation (bold 13px red `#e74c3c`, near hour 14, y=70):** "14:00 deploy — errors spike to 740/hr".
- **Second annotation (bold 12px green `#008300`, near hour 4, y=150):** "found by aggregation, not by reading logs".
- **Caption (12px `#444`, bottom right):** "error counts illustrative".

## A Search Index, Not a System of Record

**Tags:** `common mistake` (red), `durability` (orange)

- **The temptation** — it stores JSON, it queries fast, so teams quietly make it the only copy of the data
- **The design** — Elasticsearch optimizes for search and analytics, not for transactional guarantees
- **Reindexing** — changing a field's mapping means rebuilding the index; you need a source to rebuild from
- **Refresh lag** — a document written now is searchable ~1s later; search-your-own-write logic breaks
- **The pattern** — keep a database as the system of record and treat the index as a rebuildable projection

*Example (italic):* A mapping change forces a reindex of all 1,000,000 reviews; the team with a database of record rebuilds overnight — the team without one has nothing to rebuild from.

**Common mistake:** Using Elasticsearch as the only copy of your data. It is a rebuildable search projection — when a reindex, mapping change, or cluster incident hits, the system of record is what saves you.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: index-as-only-copy (mapping change strands the data) vs database-of-record feeding a rebuildable index.

- **Title (bold 15px, `#1a5276`, top center):** "System of Record vs Search Projection".
- **Row 1 (y=95), label 12px `#444` at x=20:** "index only"; blue `#2a78d6` rounded box at x=180 labeled "reviews live only in the index", 3px arrow to a red `#e74c3c` box at x=440 labeled "mapping change → full reindex" with bold 12px red "✗ nothing to rebuild from" at x=440, y=140.
- **Row 2 (y=205), label:** "record + projection"; blue box at x=180 labeled "database of record", 3px arrow to a green `#008300` box at x=380 labeled "reindex from source", then arrow to a green box at x=570 labeled "fresh index" with bold 12px green "✓ rebuilt overnight" at x=570, y=250.
- **Box style:** 150–180px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(231,76,60,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px orange `#d95926`, centered near y=270):** "the index is a view of your data, not the home of it".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); review counts, log-line counts, and hourly error counts are invented and labeled illustrative; the tf-idf arithmetic (idf ln(5/3)=0.51, ln(5/2)=0.92; scores 2.86 / 1.94 / 0.51) is exact for the stated five-review corpus.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
