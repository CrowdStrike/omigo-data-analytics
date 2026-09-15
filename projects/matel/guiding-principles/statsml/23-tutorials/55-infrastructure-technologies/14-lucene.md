# Lucene

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Lucene

**Subtitle:** Lucene is the Java library that made the inverted index practical — Elasticsearch, Solr, and OpenSearch are all servers wrapped around the same core

## The Index at the Back of Every Search Box

**Tags:** `core idea` (blue), `inverted index` (green), `Apache` (orange)

- **The library** — Doug Cutting wrote Lucene in Java in 1999; it has been an Apache project since 2001
- **The job** — make a pile of text searchable: here, the 8 customer reviews of a phone charger
- **The analysis chain** — an analyzer tokenizes and lowercases review #4's "Running FAST!" into running, fast
- **The inverted index** — Lucene stores term → posting list of review ids, not review → words
- **The flip** — like a book's back index: look up the word, get the page numbers, skip the reading

*Example (italic):* After analysis, review #4 is filed under two terms: running → [4] and fast → [2, 4, 7], joining the earlier reviews that also said fast.

**Key point:** Lucene's core is the inverted index made practical — analyzers normalize raw text into terms, and every term keeps a sorted posting list of the documents that contain it.

### Visualization (canvas `c1`, 720×300)

Flow diagram: raw review text passing through the analysis chain into two term-dictionary entries with posting lists.

- **Title (bold 15px, `#1a5276`, top center):** "The Analysis Chain: \"Running FAST!\" Becomes Two Index Terms".
- **Row 1 (boxes 40px tall, top edge y=70), left to right with 3px `#6b7280` arrows between:** blue `#2a78d6` rounded box at x=25 w=165 labeled "review #4: \"Running FAST!\"" (12px); box at x=230 w=130 labeled "tokenizer"; box at x=400 w=150 labeled "lowercase filter"; green `#008300` box at x=590 w=105 labeled "running, fast".
- **Row 2 (top edge y=190):** two term-dictionary boxes fed by 3px arrows dropping from the green box: green-tinted box at x=140 w=200 h=44 labeled "running → [4]" (12px `#2c3e50`); green-tinted box at x=400 w=200 h=44 labeled "fast → [2, 4, 7]".
- **Box style:** 8px radius, fills `rgba(42,120,214,0.15)` for the chain, `rgba(0,131,0,0.12)` for terms, 12px `#2c3e50` text.
- **Annotation (bold 13px green `#008300`, centered near y=270):** "the index maps terms to reviews — not reviews to words".
- **Caption (12px `#444`, bottom right):** "posting lists illustrative".

## A Query Walks the Posting Lists

**Tags:** `worked example` (blue), `posting lists` (green), `BM25` (orange)

- **The query** — a shopper's "fast charger", run as fast AND charger, becomes terms fast, charger
- **The lookup** — fast → reviews [2, 4, 7]; charger → reviews [1, 2, 7, 8]; two sorted id lists
- **The intersection** — only reviews 2 and 7 appear in both lists, so only they can match
- **The scoring** — BM25 rewards repeats in short fields: review 2 (fast ×2) scores 3.1, review 7 scores 2.4
- **The segments** — the 8 reviews landed as 3 immutable segment files; a background merge compacts them to 1
- **Hand-check** — walk both sorted lists, keep the common ids {2, 7}; no review text is ever scanned

*Example (italic):* The query never reads a review — it intersects two posting lists down to {2, 7}, then asks BM25 to put review 2 (score 3.1) above review 7 (score 2.4).

**Key point:** An inverted index answers "which documents contain this term" in one lookup; a query is a walk over sorted posting lists plus a relevance score, never a scan of the text.

### Visualization (canvas `c2`, 720×300)

Posting-list grid: 8 review columns, one row per query term with filled cells, a match row for the intersection, and BM25 scores under the survivors.

- **Title (bold 15px, `#1a5276`, top center):** "Query \"fast charger\": Intersect Two Posting Lists, Score the Survivors".
- **Columns:** reviews 1–8 at x = 140, 200, 260, 320, 380, 440, 500, 560; header labels "r1"–"r8" (12px `#444`) at y=70.
- **Row "fast" (y=100), 12px `#444` label at x=30:** filled 18px squares centered on columns `[2, 4, 7]`, blue `#2a78d6`; empty 1px `#e5e9ef` outlines elsewhere.
- **Row "charger" (y=145):** filled squares at columns `[1, 2, 7, 8]`, aqua `#199e70`.
- **Row "match" (y=195):** filled squares at columns `[2, 7]`, green `#008300`; all other cells empty.
- **Highlight:** vertical rounded rects `rgba(0,131,0,0.08)` behind columns 2 and 7 from y=85 to y=215.
- **Scores:** bold 12px `#1a5276` labels at y=238 — "BM25 3.1" under r2, "BM25 2.4" under r7.
- **Annotation (bold 13px violet `#4a3aa7`, near x=340, y=270):** "review 2 wins: fast appears twice in a short review".
- **Caption (12px `#444`, bottom right):** "ids and scores illustrative; BM25 is Lucene's default".

## A Library, Not a Server

**Tags:** `where it's used` (blue), `library vs server` (green)

- **No port** — Lucene ships no REST API, no server, no cluster; you embed it inside a JVM process
- **The wrappers** — Elasticsearch, Solr, and OpenSearch each wrap the same core with REST, shards, clustering
- **Same core** — analyzers, immutable segments, background merges, and BM25 behave the same in all three
- **The lineage** — Cutting's next project, Hadoop, grew out of scaling Lucene indexing (named for his son's toy elephant)
- **Why you care** — tuning search quality means tuning Lucene concepts, whichever wrapper you happen to run

*Example (italic):* The store outgrows one machine and moves the review index into Elasticsearch; analysis, segments, and BM25 ranking behave identically because the core is the same library.

**Key point:** The wrappers add distribution, REST, and cluster management; search behavior itself — analysis, indexing, scoring — is Lucene's in every one of them.

### Visualization (canvas `c3`, 720×300)

Layer diagram: three server products on top, all pointing down into one shared Lucene library box.

- **Title (bold 15px, `#1a5276`, top center):** "Three Servers, One Search Core".
- **Top row (top edge y=60), three boxes 200×54, 8px radius, fill `rgba(42,120,214,0.15)`:** "Elasticsearch" at x=30, "Solr" at x=260, "OpenSearch" at x=490; product name bold 13px `#1a5276`, second line 11px `#444` "REST · shards · cluster mgmt" in each.
- **Arrows:** 3px `#6b7280` vertical arrows from each box's bottom center (y=114) down to the Lucene box top (y=175).
- **Lucene box:** x=110 w=500 h=70, top edge y=175, fill `rgba(0,131,0,0.12)`, 2px `#008300` border; line 1 bold 14px `#1a5276` "Apache Lucene (Java library, 1999)"; line 2 12px `#2c3e50` "analyzers · inverted index · immutable segments · BM25".
- **Annotation (bold 13px magenta `#d55181`, centered near y=278):** "change the wrapper and the core behavior travels with you".
- **Caption (12px `#444`, bottom right):** "layer diagram; Hadoop began as Lucene-scaling work".

## The "Elasticsearch Bug" That Was an Analyzer

**Tags:** `common mistake` (red), `analyzers` (orange)

- **The blame** — "Elasticsearch can't find runs when I search running" gets filed as a search-engine bug
- **The truth** — matching happens on analyzed terms, and Lucene's standard analyzer does not stem words
- **The check** — run the text through the analyzer and look at the terms actually stored for the field
- **The fix** — an english analyzer stems running and runs to the same term run, so both sides meet
- **The scope** — analyzer choice is per-field and fixed at index time; changing it means reindexing

*Example (italic):* A search for "runs" misses review #4 ("Running FAST!") under the standard analyzer; with the english analyzer both sides stem to run and the review matches.

**Common mistake:** Attributing match behavior to the wrapper. Recall, stemming, and tokenization live in Lucene's analysis chain — inspect the stored terms before filing a bug against Elasticsearch.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: the same review and query under the standard analyzer (miss) vs the english analyzer (match).

- **Title (bold 15px, `#1a5276`, top center):** "Same Engine, Two Analyzers: Why \"runs\" Missed \"Running\"".
- **Row 1 (boxes 40px tall, top edge y=75), 12px `#444` label "standard analyzer" at x=20 (y=100):** blue `#2a78d6` rounded box at x=150 w=170 labeled "stored: running, fast" (12px), 3px arrow to box at x=365 w=150 labeled "query term: runs", 3px arrow to red `#e74c3c` box at x=560 w=130 labeled "✗ no match" with bold 12px red text.
- **Row 2 (top edge y=185), label "english analyzer" at x=20 (y=210):** blue box at x=150 w=170 labeled "stored: run, fast", arrow to box at x=365 w=150 labeled "query term: run", arrow to green `#008300` box at x=560 w=130 labeled "✓ match" with bold 12px green text.
- **Box style:** 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(231,76,60,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px orange `#d95926`, centered near y=272):** "the 'bug' was analyzer config — the engine matched exactly what it stored".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); posting lists (fast → [2, 4, 7], charger → [1, 2, 7, 8]), review counts, segment counts (3 merged to 1), and BM25 scores (3.1 / 2.4) are invented and labeled illustrative; the dates (1999, Apache since 2001), the library-vs-server layering, BM25 as Lucene's default, immutable segments with background merges, and the Hadoop lineage are exact public record.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
