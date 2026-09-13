# HNSW & Approximate Nearest Neighbors

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** HNSW & Approximate Nearest Neighbors

**Subtitle:** (two lines, `<br>` between them; the acronym words in `<b>`)

1. **HNSW** = **H**ierarchical (stacked layers, giant hops on top) **N**avigable (each step simply moves closer to the target) **S**mall **W**orld (mostly links to close neighbors plus a few long shortcuts); **ANN** = **A**pproximate **N**earest **N**eighbors — usually the true closest item, not always.
2. To find the closest match among a million vectors, HNSW hops through a graph of neighbors — long express hops first, short local hops last — checking a few hundred items instead of all of them

## One Million Songs, One Query

**Tags:** `core idea` (blue), `graph hopping` (green), `layers` (orange)

- **The app** — a music app stores 1,000,000 songs as points in space; similar songs sit close together
- **The query** — a listener picks one song and asks: which songs sit nearest to this one?
- **Brute force** — measuring the distance to all 1,000,000 songs gives the exact answer, painfully slowly
- **The shortcut** — link every song to a few close neighbors, then hop greedily toward the query
- **The layers** — a thin top layer holds huge hops between far-apart songs, like express trains before local ones
- **The payoff** — about 230 distance checks instead of 1,000,000, and the answer is almost always right

*Example (italic):* It works like reaching a street address: fly to the city, take a train to the district, walk the last block — nobody checks every house on Earth.

**Key point:** HNSW answers "what's nearest?" by hopping neighbor-to-neighbor through layered shortcuts — express layers cover distance fast, the bottom layer finishes the job.

### Visualization (canvas `c1`, 720×300)

Three-layer hop diagram: three horizontal bands of dots (sparse on top, dense at the bottom), with one blue arrow path entering at the top left, making long hops across the top layer, dropping down a layer at a time, and ending on a green dot next to an orange query marker at the bottom right.

- **Title (bold 15px, `#1a5276`, top center):** "Three Layers: Express Hops First, Local Hops Last".
- **Layer bands:** three rounded rects filled `rgba(26,82,118,0.06)`, x=60 to x=660 — layer 2 at y=55 (height 44), layer 1 at y=125 (height 44), layer 0 at y=195 (height 56); left-aligned 12px `#6b7280` labels just above each band: "layer 2 — a handful of songs", "layer 1 — more songs", "layer 0 — all 1,000,000 songs".
- **Layer 2 nodes:** 6px `#1a5276` dots at x = `[140, 380, 600]`, y=77.
- **Layer 1 nodes:** 6px `#1a5276` dots at x = `[110, 200, 300, 400, 500, 600]`, y=147.
- **Layer 0 nodes:** 5px `#1a5276` dots at x = `[80, 125, 170, 215, 260, 305, 350, 395, 440, 485, 575, 620]`, y=223, plus one green `#008300` 7px dot at x=530, y=223 (the answer).
- **Query marker:** orange `#d95926` 8px diamond at x=530, y=250, bold 12px orange label "query" to its right.
- **Hop path (blue `#2a78d6`, 3px, arrowheads):** (140,77) → (380,77) → drop to (400,147) → (500,147) → drop to (485,223) → (530,223); the two drops drawn as dashed (dash 4/3) vertical-ish arrows.
- **Entry label:** bold 12px `#2a78d6` "start here" above the (140,77) dot.
- **Annotation (bold 12px orange `#d95926`, near x=180, y=118):** "long hops shrink the search fast".
- **Caption (12px `#444`, bottom right):** "illustrative — node counts shrunk to fit the page".

## A Greedy Walk Across Twelve Songs

**Tags:** `worked example` (blue), `greedy search` (green)

- **The map** — 12 songs placed by tempo (x) and energy (y); the query song Q sits at (7, 6)
- **Entry point** — the walk starts at song A(2, 2), whose distance to Q is 6.4
- **Hop 1** — A's neighbors: B(5, 3) at 3.6, C(3, 6) at 4.0, K at 6.3, L at 5.1 — step to B
- **Hop 2** — B's unvisited neighbors: D(6, 6) at 1.0 and F(7, 4) at 2.0 — step to D
- **Stop** — D's unvisited neighbors E(8, 7) at 1.4 and I(4, 9) at 4.2 can't beat D's 1.0 — D wins
- **The bill** — 9 distance checks instead of 12; on a million songs the same walk stays a few hundred

*Example (italic):* Two hops — A to B to D — and the walk stops because no neighbor of D is closer to Q than D's own distance of 1.0.

**Key point:** The greedy rule is: measure your neighbors, step to whichever is closest to the query, stop when none beats you — here 9 checks found the winner among 12 songs.

### Visualization (canvas `c2`, 720×300)

Scatter map of the 12 songs on tempo/energy axes with light neighbor links, the query Q as an orange diamond, and the greedy path A → B → D drawn as thick blue arrows with the distance to Q labeled at each stop.

- **Title (bold 15px, `#1a5276`, top center):** "Greedy Walk: A (6.4) → B (3.6) → D (1.0) — stop".
- **Axes:** origin x=60, baseline y=262, plot width 560, plot height 222; x = tempo 0 to 10, y = energy 0 to 10; 12px `#444` tick labels every 2; 12px `#444` axis titles "tempo" (bottom center) and "energy" (rotated, left).
- **Pixel mapping:** x_px = 60 + tempo × 56, y_px = 262 − energy × 22.
- **Songs (5px `#1a5276` dots with 11px `#444` letter labels):** A(2,2)→(172,218), B(5,3)→(340,196), C(3,6)→(228,130), D(6,6)→(396,130), E(8,7)→(508,108), F(7,4)→(452,174), G(1,8)→(116,86), H(9,2)→(564,218), I(4,9)→(284,64), J(9,9)→(564,64), K(1,4)→(116,174), L(6,1)→(396,240).
- **Query:** orange `#d95926` 8px diamond at Q(7,6)→(452,130), bold 13px orange label "Q" above it.
- **Neighbor links (1px `#e5e9ef` lines):** A–B, A–C, A–K, A–L, B–D, B–F, B–L, C–G, C–I, D–E, D–F, D–I, E–J, E–H, F–H, F–L, G–K, I–J.
- **Greedy path:** 3px blue `#2a78d6` arrows A→B and B→D; D redrawn as a green `#008300` 8px dot.
- **Distance labels (bold 12px, next to each visited stop):** blue "6.4" by A, blue "3.6" by B, green "1.0" by D.
- **Annotation (bold 12px green `#008300`, near (500, 230)):** two lines: "9 of 12 songs checked —" / "the rest never touched".

## Why Every Vector Database Uses This

**Tags:** `where it's used` (blue), `scale` (green), `speed` (orange)

- **Embeddings everywhere** — search, recommendations, and chatbots all turn text or images into vectors
- **The scale wall** — brute force cost grows with the library: 1,000,000 items means 1,000,000 checks
- **Logarithmic cost** — HNSW needs about 150 checks at 10 thousand songs and only about 270 at 10 million
- **Behind the curtain** — vector databases and RAG pipelines run HNSW (or a close cousin) on every query
- **The trade** — you accept a small chance of missing the true nearest in exchange for thousand-fold speed

*Example (italic):* A 1,000× bigger library costs brute force 1,000× more work, but costs HNSW only about 120 extra checks.

**Key point:** Brute force scales with the library size; HNSW scales roughly with the number of digits in it — that gap is why vector search over millions of items feels instant.

### Visualization (canvas `c3`, 720×300)

Horizontal grouped bar chart: four library sizes, each with a magenta brute-force bar and a blue HNSW bar; bar lengths are on a log scale so both fit, with the raw check counts printed at each bar's end.

- **Title (bold 15px, `#1a5276`, top center):** "Distance Checks per Query — bar length on a log scale".
- **Layout:** bars start at x=200; group rows top-aligned at y = 62, 112, 162, 212; in each group the brute bar (height 14) sits above the HNSW bar (height 14) with a 4px gap; 12px `#444` row labels at x=20, vertically centered per group: "10 thousand songs", "100 thousand songs", "1 million songs", "10 million songs".
- **Brute-force bars (fill `#d55181`):** widths = log10(checks) × 60 → `[240, 300, 360, 420]` for checks `[10,000, 100,000, 1,000,000, 10,000,000]`; 12px `#d55181` end labels "10,000 checks", "100,000 checks", "1,000,000 checks", "10,000,000 checks".
- **HNSW bars (fill `#2a78d6`):** widths `[131, 137, 142, 146]` for checks `[150, 190, 230, 270]`; 12px `#2a78d6` end labels "~150", "~190", "~230", "~270".
- **Legend (top right, 12px):** magenta swatch "brute force", blue swatch "HNSW".
- **Annotation (bold 12px `#008300`, near x=420, y=252):** "1,000× more songs → only ~120 more HNSW checks".
- **Caption (12px `#444`, bottom right):** "illustrative check counts".

## Approximate Means It Can Miss

**Tags:** `common mistake` (red), `recall` (orange)

- **The A in ANN** — approximate: the greedy walk can settle on a very good song that isn't the best one
- **Recall** — the share of queries where the true nearest neighbor actually makes it into the results
- **The dial** — a search-width setting called ef keeps more candidate songs alive during the walk
- **Cheap start** — widening ef from 10 to 50 lifts recall from 71% to 96% for 80 → 300 checks
- **Costly finish** — the last stretch, 96% to 99.5%, needs 300 → 1,000 checks — the price triples
- **The mistake** — treating results as exact; a 96%-recall index silently misses 4 queries in every 100

*Example (italic):* A shopping site settles on ef = 50: recall 96% at 300 checks per query — pages load fast and shoppers never notice the rare miss.

**Common mistake:** Assuming vector search returns THE nearest neighbor. It returns A near neighbor; recall says how often the two coincide, and each extra point of recall is bought with a wider, slower search.

### Visualization (canvas `c4`, 720×300)

Single line chart of recall versus distance checks per query: five dots for ef = 10, 20, 50, 100, 200 joined by a blue curve that rises steeply then flattens, with the ef = 50 sweet spot highlighted in green.

- **Title (bold 15px, `#1a5276`, top center):** "The Recall Dial: More Checks Buy Less and Less".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 190; x = distance checks 0 to 1,000 with 12px `#444` tick labels "0", "200", "400", "600", "800", "1,000"; y = recall 60% to 100% with 12px `#444` tick labels "60%", "70%", "80%", "90%", "100%" and light `#e5e9ef` gridlines.
- **Pixel mapping:** x_px = 60 + checks × 0.6, y_px = 245 − (recall − 60) × 4.75.
- **Curve:** blue `#2a78d6` 3px line through (checks, recall) = `[(80, 71), (140, 88), (300, 96), (550, 99), (1000, 99.5)]` → pixels `[(108, 193), (144, 112), (240, 74), (390, 60), (660, 57)]`.
- **Dots:** 6px blue dots at the five points, each with an 11px `#6b7280` label below: "ef=10", "ef=20", "ef=50", "ef=100", "ef=200"; the ef=50 dot drawn 8px green `#008300` with bold 12px green label "sweet spot: 96% at 300 checks" above it.
- **Ceiling line:** horizontal dashed `#6b7280` (dash 4/3) line at recall 100% with 11px `#6b7280` label "exact (brute force)" at its right end.
- **Annotation (bold 12px orange `#d95926`, near x=430, y=140):** two lines: "96% → 99.5% recall" / "triples the checks (300 → 1,000)".
- **Caption (12px `#444`, bottom right):** "illustrative recall figures".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all node positions, song coordinates, greedy-path distances, bar widths, and recall points are the hardcoded values above (no randomness); distances in c2 are true straight-line distances from Q(7, 6) rounded to 1 decimal; check counts and recall figures are illustrative and labeled so in the captions.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
