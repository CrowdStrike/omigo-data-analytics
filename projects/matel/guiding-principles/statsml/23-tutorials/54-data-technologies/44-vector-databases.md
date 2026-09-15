# Vector Databases

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Vector Databases

**Subtitle:** Turn text into points in space so "refund not received" can find tickets that mean the same thing — even when they share no words

## The Ticket That Matches Without Sharing a Word

**Tags:** `core idea` (blue), `embeddings` (green), `similarity search` (orange)

- **The search** — a support agent types "refund not received" to find similar past tickets
- **The keyword failure** — "money never came back" shares zero words with the query, so keyword search misses it
- **The embedding** — a model maps each ticket to a point in high-dimensional space; similar meaning lands nearby
- **The query** — the query text becomes a point too; the answer is whatever points sit closest to it
- **The database** — a vector database stores those points and answers "give me the nearest ones" fast

*Example (italic):* "money never came back" and "still waiting on my reimbursement" land right next to "refund not received" in the space — the login and shipping tickets sit far away.

**Key point:** An embedding turns meaning into geometry: search stops being "match these words" and becomes "find the nearest points".

### Visualization (canvas `c1`, 720×300)

Scatter plot of five embedded tickets plus the query in a 2-D embedding space; the two refund-meaning tickets cluster around the query despite sharing no keywords.

- **Title (bold 15px, `#1a5276`, top center):** "Five Tickets as Points: Meaning Decides Distance, Not Words".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; both dims run 0 to 10, 12px `#444` tick labels at 0/5/10, axis titles 12px `#6b7280` "embedding dim 1" (bottom) and "dim 2" (left, rotated); gridlines `#e5e9ef` at 2.5/5/7.5. Map a point (d1, d2) to pixels as x = 60 + d1/10·600, y = 245 − d2/10·180.
- **Query point:** magenta `#d55181` filled star (or 7px circle with 2px ring) at (2.0, 8.0), bold 12px magenta label "query: refund not received" above it.
- **Ticket points (6px filled circles, 12px `#2c3e50` labels beside each):** blue `#2a78d6` "money never came back" at (2.3, 7.6); blue "still waiting on my reimbursement" at (2.6, 7.2); mute `#6b7280` "can't log into my account" at (8.0, 8.5); mute "how do I change my password" at (8.4, 7.7); mute "package arrived damaged" at (7.0, 3.0).
- **Cluster ring:** dashed `#2a78d6` (dash 4/3) circle of pixel radius 55 centered between the query and the two blue points (around (2.3, 7.6)).
- **Annotation (bold 13px green `#008300`, near (4.5, 2.0)):** "nearest neighbors share meaning, not keywords".
- **Caption (12px `#444`, bottom right):** "2-D coordinates illustrative, axes not to equal scale — real embeddings use hundreds of dimensions".

## Ranking Five Tickets by Distance

**Tags:** `worked example` (blue), `nearest neighbor` (green)

- **The setup** — using the 2-D coordinates above, score each ticket by straight-line distance to the query (2.0, 8.0)
- **Hand-check one** — "money never came back" at (2.3, 7.6): √(0.3² + 0.4²) = √0.25 = 0.50
- **The ranking** — distances come out 0.50, 1.00, 6.02, 6.41, 7.07; smallest distance = best match
- **Top-2 returned** — both refund-meaning tickets win with zero shared keywords; the rest are 6× farther
- **Real scale** — production embeddings have 768 or 1,536 dimensions; the distance formula just gains terms

*Example (italic):* "still waiting on my reimbursement" at (2.6, 7.2) scores √(0.6² + 0.8²) = 1.00 — second place, and it never says "refund".

**Key point:** A vector search is nothing but "compute distances, return the k smallest" — every number in this ranking can be redone by hand with the Pythagorean formula.

### Visualization (canvas `c2`, 720×300)

Horizontal bar chart of the five ticket-to-query distances, sorted best (shortest) to worst, top-2 highlighted as the returned matches.

- **Title (bold 15px, `#1a5276`, top center):** "Distance to the Query: Smallest Two Win".
- **Axis:** vertical 2px `#999` baseline at x=250, bars extend right, scale 60px per distance unit (max bar 424px); 12px `#444` left-aligned ticket labels at x=20 on each row.
- **Rows (top to bottom at y = 70, 110, 150, 190, 230), bars 18px tall:**
  - "money never came back": green `#008300` bar width 30 (distance 0.50), bold 12px green value label "0.50 ✓ returned" at bar end
  - "still waiting on my reimbursement": green bar width 60 (1.00), label "1.00 ✓ returned"
  - "can't log into my account": mute fill `rgba(107,114,128,0.35)` bar width 361 (6.02), 11px `#6b7280` label "6.02"
  - "how do I change my password": mute bar width 385 (6.41), label "6.41"
  - "package arrived damaged": mute bar width 424 (7.07), label "7.07"
- **Annotation (bold 13px blue `#2a78d6`, near x=300, y=95):** "top-2 share no keywords with the query".
- **Caption (12px `#444`, bottom right):** "coordinates illustrative; distances exact for those coordinates".

## Why Nobody Scans Ten Million Vectors

**Tags:** `where it's used` (blue), `approximate indexes` (green), `RAG` (orange)

- **Exact search** — comparing the query to all 10 million stored vectors gives perfect answers but costs a full scan
- **The trade** — approximate indexes skip most vectors: a little recall lost, a huge speedup gained
- **HNSW** — a graph of shortcut links; hop greedily toward the query instead of visiting everything
- **IVF** — cluster the vectors first, then search only the few clusters nearest the query
- **The 2023 wave** — RAG (retrieval-augmented generation) made this the hot product category: Pinecone, Weaviate, Milvus, Chroma

*Example (italic):* On 10M vectors, an exact scan takes ~2,000 ms per query; an HNSW index answers in ~8 ms while still finding 99 of the true top-100 (numbers illustrative).

**Key point:** Approximate nearest-neighbor indexes are the whole reason vector databases exist — at millions of vectors, exact scans are too slow to serve live queries.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart comparing per-query latency of exact scan vs IVF vs HNSW on the same 10M-vector collection, with recall noted per row.

- **Title (bold 15px, `#1a5276`, top center):** "10M Vectors, One Query: Exact Scan vs Approximate Index".
- **Axis:** vertical 2px `#999` baseline at x=230, bars extend right, max width 440; log-feel achieved by hardcoded pixel widths, not a real log axis.
- **Rows (top to bottom at y = 85, 150, 215), bars 20px tall, 12px `#444` left labels at x=20:**
  - "exact scan — recall 100%": red `#e74c3c` bar width 440, bold 12px red label "~2,000 ms" at bar end
  - "IVF — recall ~95%": orange `#d95926` bar width 175, 12px orange label "~40 ms"
  - "HNSW — recall ~99%": green `#008300` bar width 95, bold 12px green label "~8 ms"
- **Annotation (bold 13px violet `#4a3aa7`, right side near y=250):** "give up 1% recall, get a 250× speedup".
- **Caption (12px `#444`, bottom right):** "latencies illustrative; pixel widths schematic (log-feel)".

## A New Database You Didn't Need

**Tags:** `common mistake` (red), `pgvector` (orange)

- **The reflex** — "we're adding semantic search" turns into standing up a dedicated vector database cluster
- **The hidden cost** — the tickets live in Postgres, so now a sync pipeline must copy every change across
- **The drift** — two systems disagree the moment the pipeline lags; deleted tickets keep matching
- **The boring answer** — pgvector adds a vector column type plus HNSW and IVFFlat indexes inside Postgres itself
- **The fit** — 80k support tickets (illustrative) is tiny for pgvector; one SQL query joins similarity with your existing filters

*Example (italic):* `... ORDER BY embedding <-> query LIMIT 5` with a `WHERE status = 'open'` filter — one query, one database, nothing to keep in sync.

**Common mistake:** Choosing a dedicated vector database by default. If your data already lives in a database with a vector extension and your collection is modest, the extension is usually good enough — a new system is a real cost, not a free upgrade.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: the dedicated-vector-DB architecture (extra system plus sync pipeline) vs the pgvector architecture (one database, one query).

- **Title (bold 15px, `#1a5276`, top center):** "Two Architectures for the Same Ticket Search".
- **Row 1 (boxes centered at y=95), label 12px `#444` at x=20:** "dedicated vector DB"; blue `#2a78d6` rounded box at x=150 labeled "Postgres (tickets)" (12px), 3px arrow to an orange `#d95926` box at x=330 labeled "sync pipeline", arrow to an orange box at x=510 labeled "vector DB (copies)"; bold 12px red `#e74c3c` note under the row: "✗ two systems to run and keep in sync".
- **Row 2 (boxes centered at y=205), label:** "pgvector"; blue box at x=150 labeled "Postgres (tickets)", 3px arrow to a green `#008300` box at x=390 labeled "+ pgvector index, same tables"; bold 12px green note under the row: "✓ one database, one SQL query".
- **Box style:** 150–170px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(217,89,38,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px magenta `#d55181`, centered near y=270):** "add the dedicated system when you outgrow the extension — not before".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); the 2-D ticket coordinates, 10M-scale latencies (2,000 / 40 / 8 ms), recall figures, and the 80k-ticket count are invented and labeled illustrative; the five distances (0.50 / 1.00 / 6.02 / 6.41 / 7.07) are the exact Euclidean distances for those coordinates. Publicly documented facts stated as fact: HNSW and IVF as ANN index families, the 2023 vector-DB product wave (Pinecone, Weaviate, Milvus, Chroma) driven by RAG, and pgvector as a Postgres extension providing a vector type with HNSW/IVFFlat indexes.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
