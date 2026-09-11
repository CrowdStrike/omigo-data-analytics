# Web Search Engine

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Web Search Engine

**Subtitle:** Designing a web-scale search engine — crawl the pages, invert them into an index, and answer each query by asking every shard at once (a classic design exercise using public concepts, not any company's current internals)

## One Engine, Three Pipelines

**Tags:** `core idea` (blue), `architecture` (green), `search` (orange)

- **The goal** — type "coffee grinder" and get the ten best pages out of a million in under 200ms
- **Crawl** — a crawler fetches pages, extracts their links, and queues links it hasn't seen yet
- **Politeness** — it caps requests per site (say 1 per second) so it never hammers anyone's server
- **Index** — an indexer inverts the pages: for each term, the sorted list of docs containing it
- **Serve** — the index is split across 100 shards; a query asks all of them at the same time

*Example (italic):* Our toy engine crawls 1,000,000 pages, rebuilds its index nightly, and serves queries from 100 shards of 10,000 docs each.

**Key point:** A search engine is three loosely coupled pipelines — crawl and index run for hours in batch; serving answers in milliseconds by only reading what the batch jobs built.

### Visualization (canvas `c1`, 720×300)

Two-lane flow diagram: the batch pipeline (crawler → indexer → shards) runs left to right on top; the online query path runs along the bottom and fans out to the same shard stack.

- **Title (bold 15px, `#1a5276`, top center):** "Batch Pipelines Build the Index; the Online Path Reads It".
- **Lane labels (12px `#6b7280`):** "batch — hours" at (20, 60); "online — milliseconds" at (20, 200).
- **Batch lane (boxes 150×46, 8px radius, 12px `#2c3e50` two-line text):** blue box (border `#2a78d6`, fill `rgba(42,120,214,0.15)`) at x=40, y=75 labeled "crawler / fetch + extract links"; 3px `#6b7280` arrow to a green box (border `#008300`, fill `rgba(0,131,0,0.12)`) at x=240, y=75 labeled "indexer / term → postings"; 3px arrow into the shard stack.
- **Shard stack:** four rounded boxes 130×26 (border `#1a5276`, fill `rgba(26,82,118,0.10)`) at x=470, y = 64, 96, 128, 160, labeled 12px "shard 1", "shard 2", "…", "shard 100"; 11px `#6b7280` note "10,000 docs each" at (615, 90) rotated 0 (plain, right of the stack).
- **Online lane:** magenta box (border `#d55181`, fill `rgba(213,81,129,0.12)`) 150×46 at x=40, y=214 labeled "query: / coffee grinder"; 3px `#6b7280` arrow to a violet box (border `#4a3aa7`, fill `rgba(74,58,167,0.12)`) at x=240, y=214 labeled "root / merge + rank"; three thin 2px `#4a3aa7` arrows from the root box's top edge up-right to shard boxes 1, 2, and 100; dashed 2px `#4a3aa7` (dash 4/3) return arrows alongside them.
- **Annotation (bold 13px orange `#d95926`, near x=440, y=250):** "every query touches every shard".
- **Caption (12px `#444`, bottom right):** "counts illustrative".

## Building the Inverted Index by Hand

**Tags:** `worked example` (blue), `inverted index` (green)

- **The pages** — d1–d4 are four short pages about coffee gear; their full text sits in the chart
- **Invert** — scan every page once; for each word, append that page's id to the word's list
- **Posting lists** — coffee → [d1, d2, d4]; grinder → [d1, d3, d4]; lists stay sorted by doc id
- **The query** — "coffee grinder" intersects two lists: [d1, d2, d4] ∩ [d1, d3, d4] = [d1, d4]
- **Ranking** — d4 has 8 inbound links vs d1's 2, so a PageRank-style link score puts d4 first

*Example (italic):* A two-word query never scans the million pages — it walks two sorted lists and keeps the doc ids that appear in both.

**Key point:** An inverted index turns "which docs contain this term?" from a scan of every page into a lookup — it is the core data structure of every search engine.

### Visualization (canvas `c2`, 720×300)

Posting-list diagram: six terms on the left, each followed by a row of doc-id chips; the two query rows are highlighted and their intersection chips turn green; document texts and the intersection math sit on the right.

- **Title (bold 15px, `#1a5276`, top center):** "Four Tiny Pages, Inverted: term → sorted doc ids".
- **Term rows (y = 70, 100, 130, 160, 190, 220):** term label bold 12px `#1a5276` right-aligned at x=110; doc-id chips are 30×20 rounded rects (6px radius, fill `rgba(42,120,214,0.15)`, border `#2a78d6`, 12px `#2c3e50` centered label) starting at x=130 with 38px spacing:
  - "best" → d1, d4
  - "coffee" → d1, d2, d4
  - "grinder" → d1, d3, d4
  - "burr" → d3, d4
  - "review" → d3, d4
  - "shop" → d2
- **Stopword note (11px `#6b7280`, x=20, y=248):** `stopwords ("near", "me") dropped before indexing`.
- **Query highlight:** full-width band x=20..400, height 26, fill `rgba(201,133,0,0.12)` behind the "coffee" and "grinder" rows; in those two rows the d1 and d4 chips use green style (border `#008300`, fill `rgba(0,131,0,0.15)`).
- **Doc panel (12px `#444`, x=420, y = 62, 84, 106, 128, one per line):** `d1: "best coffee grinder"`, `d2: "coffee shop near me"`, `d3: "burr grinder review"`, `d4: "best burr coffee grinder review"`.
- **Annotation (bold 13px violet `#4a3aa7`, x=420, two lines at y=172 and y=190):** ""coffee grinder" intersects:" / "[d1,d2,d4] ∩ [d1,d3,d4] = [d1, d4]".
- **Annotation (bold 12px green `#008300`, x=420, y=222):** "link score ranks d4 (8 in-links) over d1 (2)".
- **Caption (12px `#444`, bottom right):** "pages and link counts illustrative".

## The Fan-Out Tax: Every Query, Every Shard

**Tags:** `where it's used` (blue), `tail latency` (red), `fan-out` (orange)

- **Doc-partitioned** — each shard holds complete posting lists, but only for its own 10,000 docs
- **Fan-out** — any term can live on any shard, so every query must ask all 100 shards
- **Top-k merge** — each shard returns its 10 best; the root re-ranks 1,000 candidates into 10
- **Serve-time ranking** — text match, link score, and freshness combine per query, not in the index
- **The tail** — if a shard runs slow 1% of the time, all-100-fast happens only 0.99¹⁰⁰ ≈ 37%
- **The fix** — hedged requests: re-send to a replica when a shard hasn't answered within, say, 50ms

*Example (italic):* With 100 shards, 63% of queries wait on at least one straggler — the slowest shard sets the pace for the whole query.

**Key point:** Document partitioning buys simple indexing and per-shard ranking, and pays with fan-out: query latency is the max over all shards, not the average.

### Visualization (canvas `c3`, 720×300)

Line chart: probability that a query hits at least one slow shard versus the number of shards it fans out to, with the 100-shard point called out.

- **Title (bold 15px, `#1a5276`, top center):** "Fan-Out Makes the Tail the Bottleneck".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = number of shards 0 to 200, 12px `#444` tick labels every 50; y = "% of queries hitting ≥1 slow shard" 0 to 100, gridlines `#e5e9ef` at 25/50/75 with 12px `#444` labels.
- **Curve:** magenta `#d55181` 3px line through shards `[1, 10, 25, 50, 100, 150, 200]`, percent `[1, 10, 22, 39, 63, 78, 87]`.
- **Guides:** dashed `#6b7280` (dash 4/3) vertical line from the baseline at x=100 up to the curve, and horizontal from there to the y-axis at 63%; filled 5px-radius `#d55181` dot at (100 shards, 63%).
- **Annotation (bold 13px red `#e74c3c`, near x=105 shards, pixel y≈100):** "100 shards: 63% of queries wait on a straggler".
- **Caption (12px `#444`, bottom right):** "1% slow chance per shard illustrative; curve 1 − 0.99ⁿ exact".

## The Myth of One Fresh Index

**Tags:** `common mistake` (red), `freshness` (orange)

- **The mistake** — treating the index as one always-current, instantly updated copy of the web
- **The reality** — rebuilding the 1M-page base index is a batch job; ours runs nightly at 2:00am
- **The gap** — a page published at 9:00am stays invisible for 17 hours on the base index alone
- **The tier** — a small fresh index (thousands of docs) rebuilds every 5 minutes and catches it
- **Serve both** — queries fan out to base shards and the fresh tier; the merger dedupes by doc id

*Example (italic):* The 9:00am page is searchable by 9:05 through the fresh tier, then folds into the base index at the 2:00am rebuild.

**Common mistake:** Assuming freshness is free. A read-optimized index is expensive to update in place, so real engines layer a fast small tier over a slow big one and merge results at serve time.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: the same newly published page flowing through a base-only design (long red gap) versus a design with a fresh tier (searchable in minutes).

- **Title (bold 15px, `#1a5276`, top center):** "Published 9:00am — Searchable When?".
- **Row 1 (y=95), label 12px `#444` at x=20:** "base index only"; blue rounded box (fill `rgba(42,120,214,0.15)`) at x=170 labeled "page live 9:00am" (12px); 3px `#6b7280` arrow to a red box (fill `rgba(231,76,60,0.12)`) at x=420 labeled "invisible until 2:00am rebuild", with bold 12px red `#e74c3c` "17-hour gap" just below the arrow.
- **Row 2 (y=205), label:** "with fresh tier"; blue box "page live 9:00am" at x=170; 3px arrow to a green box (fill `rgba(0,131,0,0.12)`) at x=360 labeled "fresh tier by 9:05am"; 3px arrow to a second green box at x=560 labeled "into base at 2:00am", with bold 12px green `#008300` "✓ 5 min" above the first green box.
- **Box style:** 140–170px wide, 40px tall, 8px radius, 12px `#2c3e50` text, borders matching each fill's hue.
- **Annotation (bold 13px aqua `#199e70`, centered near y=270):** "queries hit both tiers; the merger dedupes by doc id".
- **Caption (12px `#444`, bottom right):** "rebuild times illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); page counts, shard counts, link counts, and rebuild times are invented and labeled illustrative; the fan-out curve percentages (1/10/22/39/63/78/87) are the exact values of 1 − 0.99ⁿ at n = 1/10/25/50/100/150/200, rounded to whole percent, and must match the text's "0.99¹⁰⁰ ≈ 37%" and "63%".
- **Framing:** the page is a generic design exercise built from publicly documented, classic concepts (inverted index, PageRank-style link scoring, document-partitioned serving); make no claims about the real company's current internal systems.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
