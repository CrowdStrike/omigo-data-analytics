# Google Bigtable

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Google Bigtable

**Subtitle:** Bigtable stores billions of rows as one giant alphabetically sorted map — every read is a lookup of (row, column, timestamp) — and the 2006 paper describing it became the blueprint for NoSQL

## The Webtable: Every Page in One Sorted Map

**Tags:** `core idea` (blue), `sorted map` (green), `2006 paper` (orange)

- **The webtable** — Google's crawler stores billions of web pages in one table the paper calls the webtable
- **The key trick** — the row key is the URL with its hostname reversed: www.cnn.com becomes com.cnn.www
- **Sorted rows** — rows sit in strict alphabetical order, so every page of one site becomes a neighbor
- **Column families** — contents: holds the page's HTML; anchor: holds the text of links pointing at the page
- **Timestamps** — a cell keeps several dated versions; three crawls of one page are versions t3, t5, t6
- **Sparse** — most rows fill only a few columns; empty cells cost nothing, unlike a fixed-schema table

*Example (italic):* Under row com.cnn.www, the cell anchor:cnnsi.com holds the value "CNN" — the exact link text another site used to point there.

**Key point:** A Bigtable is one giant sorted map: hand it (row key, column, timestamp) and it hands back raw bytes — that single sentence is the entire data model.

### Visualization (canvas `c1`, 720×300)

Grid diagram of the webtable: four sorted row keys down the left, three column headers across the top, cells filled only where the paper's example has data — the com.cnn.www row carries versioned cells.

- **Title (bold 15px, `#1a5276`, top center):** "The Webtable: Rows Sorted by Reversed URL, Cells Stamped by Time".
- **Column headers (12px bold `#1a5276`, y=55):** "contents:" at x=260, "anchor:cnnsi.com" at x=430, "anchor:my.look.ca" at x=590.
- **Row keys (12px `#2c3e50`, left-aligned at x=20, rows at y = 80, 125, 170, 215):** `["com.aaa.www", "com.cnn.www", "com.cnn.www/tech", "org.apache.www"]` — already in sorted order; the com.cnn.www key drawn bold ink `#1a5276`.
- **Gridlines:** 1px `#e5e9ef` horizontal rules between rows and a vertical rule at x=230 separating keys from cells.
- **Filled cells (rounded 8px boxes, 40px tall):** on the com.cnn.www row — contents: box fill `rgba(42,120,214,0.15)` with three stacked 11px chip labels "t6 / t5 / t3" in blue `#2a78d6`; anchor:cnnsi.com box fill `rgba(0,131,0,0.12)` labeled `"CNN" @ t9` (12px green `#008300`); anchor:my.look.ca box fill `rgba(25,158,112,0.12)` labeled `"CNN.com" @ t8` (12px aqua `#199e70`).
- **Sparse cells:** one grey 12px `#6b7280` "…" in contents: on the org.apache.www row; every other cell left empty.
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=262):** "reversed hostnames keep all cnn pages adjacent in the sort".
- **Caption (12px `#444`, bottom right):** "rows, columns and timestamps follow the 2006 paper's webtable example".

## Walking One Lookup by Hand

**Tags:** `worked example` (blue), `tablets` (green)

- **The ask** — fetch the cell at (row=com.cnn.www, column=anchor:cnnsi.com, timestamp=t9)
- **Step 1** — binary-search the sorted row keys to find com.cnn.www; no secondary index needed
- **Step 2** — inside that row, jump to the anchor: family, then to the cnnsi.com column within it
- **Step 3** — pick the version stamped t9; the value comes back as the raw bytes "CNN"
- **Newest wins** — ask contents: with no timestamp and you get the latest crawl, the t6 version
- **Tablets** — the sorted keys are cut into ranges called tablets; each server serves a few ranges

*Example (italic):* Six row keys — com.aaa, com.bbb, com.cnn, com.cnn/tech, com.weather, org.apache — cut into three tablets of two keys each; com.cnn falls in tablet 2, so exactly one server answers.

**Key point:** Every read is the same walk — find the tablet by key range, the row by key, the cell by column and timestamp; there is no query planner and no index to choose.

### Visualization (canvas `c2`, 720×300)

Flow diagram: a query box at the top, a horizontal sorted-key line cut into three tablet brackets in the middle, and the returned value at the bottom right.

- **Title (bold 15px, `#1a5276`, top center):** "One Lookup: Key Range → Tablet 2 → Row → Cell @ t9".
- **Query box (y=62):** rounded 8px box at x=40, ~340px wide, 34px tall, fill `rgba(42,120,214,0.15)`, 12px `#2c3e50` label "get (com.cnn, anchor:cnnsi.com, t9)".
- **Sorted key line (y=165):** 2px `#999` horizontal line from x=60 to x=660; six ticks at x = `[80, 180, 280, 380, 480, 580]` with 11px `#444` labels below: `["com.aaa", "com.bbb", "com.cnn", "com.cnn/tech", "com.weather", "org.apache"]`.
- **Tablet brackets (under the labels, y=205, 18px tall rounded bands):** tablet 1 spanning x 65–225 fill `rgba(42,120,214,0.30)` labeled "tablet 1" (11px `#2a78d6`); tablet 2 spanning x 235–425 fill `rgba(0,131,0,0.30)` labeled "tablet 2" (11px bold `#008300`); tablet 3 spanning x 435–660 fill `rgba(25,158,112,0.30)` labeled "tablet 3" (11px `#199e70`).
- **Arrow:** 3px `#1a5276` arrow from the query box down to the com.cnn tick at x=280, then a short arrow right to the result box.
- **Result box (y=240):** rounded box at x=470, ~180px wide, fill `rgba(0,131,0,0.12)`, bold 12px green `#008300` label `value: "CNN" @ t9`.
- **Annotation (bold 13px green `#008300`, near x=430, y=110):** "binary search on the key finds the tablet — no index needed".
- **Caption (12px `#444`, bottom left):** "six keys and three tablets illustrative; real tablets hold ~100–200 MB of rows".

## The 2006 Paper That Launched NoSQL

**Tags:** `where it's used` (blue), `lineage` (green), `NoSQL` (orange)

- **The paper** — Chang et al. published the Bigtable design at OSDI 2006, built on Google's GFS
- **HBase** — the open-source rebuild of the paper became an Apache Hadoop subproject in 2008
- **Cassandra** — open-sourced by Facebook in 2008, it borrowed Bigtable's column-family data model
- **Wide-column** — the whole "wide-column store" category of databases descends from this one design
- **Key design** — the sorted-map habit survives everywhere: rows you scan together must sort together
- **Full circle** — Google opened Bigtable itself as a public cloud service in 2015

*Example (italic):* The reversed-URL trick is now standard key design in every wide-column store: prefix the key with whatever you want to scan by.

**Key point:** The paper's deepest legacy is a habit of thought — model the access pattern first, then design the row key so one sorted-range scan answers it.

### Visualization (canvas `c3`, 720×300)

Timeline chart from 2005 to 2016 with four dated markers showing the paper and its descendants.

- **Title (bold 15px, `#1a5276`, top center):** "One Paper, Ten Years of Descendants".
- **Axis:** 2px `#999` horizontal baseline at y=170 from x=60 to x=660; year ticks at 2006, 2008, 2010, 2012, 2014, 2016 with 12px `#444` labels below, x mapped linearly (2005 → 60, 2016 → 660, ≈54.5 px/year).
- **Markers (8px radius dots on the baseline, 3px stems to their labels):**
  - x=115 (2006): blue `#2a78d6` dot, bold 12px blue label above at y=120 "Bigtable paper (OSDI 2006)"
  - x=224 (early 2008): green `#008300` dot, 12px green label below at y=215 "HBase joins Apache Hadoop"
  - x=252 (mid 2008): orange `#d95926` dot, 12px orange label above at y=95 "Cassandra open-sourced"
  - x=605 (2015): violet `#4a3aa7` dot, bold 12px violet label above at y=120 "Cloud Bigtable public"
- **Stagger:** alternate labels above/below the line exactly as listed so the two 2008 events do not collide.
- **Annotation (bold 13px magenta `#d55181`, right side near y=45):** "one paper → a whole database category".
- **Caption (12px `#444`, bottom right):** "dates are publication and release years".

## It Looks Like a Table — It Isn't One

**Tags:** `common mistake` (red), `no joins` (orange)

- **The trap** — rows and columns look relational, so newcomers reach for JOIN and WHERE on values
- **No joins** — the original Bigtable joins nothing; the API is get, put, delete, and scan by key range
- **No secondary indexes** — you can find a row by its key, never by a value stored inside a cell
- **Single-row atomicity** — updates are atomic within one row only; there are no multi-row transactions
- **The fix** — bake the question into the key, or keep a second copy of the data keyed the other way

*Example (italic):* "Which pages does cnnsi.com link to?" has no index in the webtable — you either scan every row or maintain a second table keyed by the linking site.

**Common mistake:** Treating Bigtable as SQL minus a few features. It is a different machine: if a question cannot be answered by one key-range scan, the data model — not the query — has to change.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: a lookup by row key (one seek, green) vs a search by cell value (full scan, red, fixed by a second table).

- **Title (bold 15px, `#1a5276`, top center):** "By Key: One Seek. By Value: Scan Everything".
- **Row 1 (y=95), label 12px `#444` at x=20:** "query the key"; blue `#2a78d6` rounded box at x=170 labeled "get row com.cnn" (12px), 3px arrow to a green `#008300` box at x=430 labeled "tablet 2 → one row read" with bold 12px green "✓ one server, one seek" to its right.
- **Row 2 (y=205), label:** "query a value"; blue box at x=170 labeled "rows with anchor:cnnsi.com?", 3px arrow to a red `#e74c3c` box at x=390 labeled "no index → scan every tablet" with bold 12px red "✗ full scan", then arrow to a green box at x=575 labeled "fix: 2nd table keyed by linker".
- **Box style:** 130–170px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(231,76,60,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px orange `#d95926`, centered near y=270):** "if it isn't a key-range scan, redesign the key — not the query".
- **Caption (12px `#444`, bottom right):** "API verbs per the 2006 paper; box layout schematic".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all values are the hardcoded arrays and pixel positions above (no randomness); the webtable rows, reversed-URL keys, timestamps (t3/t5/t6, t9, t8), anchor values ("CNN", "CNN.com") and API limits follow the 2006 OSDI paper's published example; the six-key/three-tablet layout is invented and labeled illustrative, while the ~100–200 MB tablet size is the paper's stated default; timeline years (2006, 2008, 2008, 2015) are real publication/release dates.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
