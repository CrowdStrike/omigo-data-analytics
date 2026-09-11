# Head, Torso & Tail Queries

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Head, Torso & Tail Queries

**Subtitle:** A handful of queries repeat millions of times while millions of queries appear only once — and each zone needs a different method

## A Week of Searches, Sorted by Count

**Tags:** `core idea` (blue), `three zones` (green)

- **Sort the log** — take one online store's week of searches, count each distinct query, sort by count
- **The head** — a tiny set typed constantly: "iphone", "shoes" — each repeats thousands of times
- **The torso** — moderately common queries: "waterproof hiking boots women" shows up every day
- **The tail** — a huge crowd seen once or twice: "hoodie with thumb holes size xxl blue"
- **Extreme shape** — counts fall off a cliff after the first few ranks; the tail stretches for miles
- **Never-seen** — on web search, roughly 15% of a day's queries have never been seen before

*Example (italic):* Rank 1 might be typed thousands of times in the week; a query out at rank 20,000 was typed exactly once.

**Key point:** Sorted by how often each was typed, a query log is a cliff: a few giants, a modest middle, and an enormous floor of one-offs.

### Visualization (canvas `c1`, 720×300)

Rank-frequency bar chart: 64 bars of steeply decaying height, colored by zone (head blue, torso green, tail orange), with zone labels, three example-query callouts, and the never-seen annotation.

- **Title (bold 15px ink `#1a5276`, top center, y=22):** "One Week of Queries, Sorted by How Often Typed (illustrative)".
- **Plot area:** baseline y=250 (1px `#999`, x=55 to x=695); 64 bars, slot width 10px starting at x=55, drawn width 9px; bar height `195 * (i+1)^-0.62` (min 6px), deterministic — no randomness.
- **Zones:** bars 0–3 head (blue `#2a78d6`), bars 4–17 torso (green `#008300`), bars 18–63 tail (orange `#d95926`); plain filled bars in tint fills (`rgba(42,120,214,0.35)` / `rgba(0,131,0,0.25)` / `rgba(217,89,38,0.30)`), plus bold 13px zone labels "HEAD" (x=75), "TORSO" (x=165), "TAIL" (x=465) in the solid zone color at y=40.
- **Callouts (11px, zone color, left-aligned, thin 1px leader line down to a bar top):** '"iphone", "shoes"' at (95, 70) pointing to bar 0; '"waterproof hiking boots women"' at (170, 120) pointing to bar 8; '"hoodie with thumb holes size xxl blue"' at (300, 160) pointing to bar 24.
- **Annotation (bold 12px magenta `#d55181`, centered x=490, y=210):** "web search: ~15% of a day's queries were never seen before".
- **Axis labels (11px `#6b7280`):** "bar height = times typed" left-aligned at (55, 268); "query rank (most typed → typed once)" centered at (375, 288).

## Splitting 100,000 Searches by Hand

**Tags:** `worked example` (blue), `redoable numbers` (orange)

- **The log** — 100,000 searches in one week, spread over 26,020 distinct queries (illustrative)
- **Head** — the top 20 distinct queries cover 32,000 searches: 32,000 / 20 = 1,600 each on average
- **Torso** — the next 2,000 queries cover 41,000 searches: 41,000 / 2,000 ≈ 20 each
- **Tail** — the remaining 24,000 queries cover just 27,000 searches: 27,000 / 24,000 ≈ 1.1 each
- **Check the sums** — 32,000 + 41,000 + 27,000 = 100,000; 20 + 2,000 + 24,000 = 26,020
- **1,600 vs 1.1** — the average head query is typed about 1,500 times more often than a tail one

*Example (illustrative, italic):* The single query "iphone" alone could account for 4,000 searches — more than thousands of tail queries combined.

**Key point:** Three cuts of one log give three wildly different averages — 1,600, 20, and 1.1 searches per query — and that spread is the whole story.

### Visualization (canvas `c2`, 720×300)

Three-column zone summary: a solid zone pill on top, a bar sized by searches covered, and the distinct-query count and per-query average beneath each column.

- **Title (bold 15px ink, top center, y=22):** "100,000 Searches Split Into Three Zones (illustrative)".
- **Columns centered at x = 140, 360, 580.** Zone pill (w=120 h=20, y=38): solid zone color fill, bold 12px white label "HEAD" / "TORSO" / "TAIL".
- **Bars:** baseline y=214 (1px `#999`, x=60 to x=660); width 90 centered on the column; heights proportional to searches, 41,000 → 116px: head 91px, torso 116px, tail 76px; tint fill (`rgba(42,120,214,0.15)` / `rgba(0,131,0,0.10)` / `rgba(217,89,38,0.15)`) with 2px zone-color border.
- **Value labels above bars (centered per column):** bold 14px zone color "32,000" / "41,000" / "27,000" at bar top − 20; 11px `#6b7280` "searches" at bar top − 6.
- **Below baseline (centered per column):** bold 12px zone color at y=236: "20 distinct queries" / "2,000 distinct queries" / "24,000 distinct queries"; 11px `#6b7280` at y=254: "≈ 1,600 searches each" / "≈ 20 searches each" / "≈ 1.1 searches each".
- **Caption (11px `#6b7280`, centered, y=286):** "sums check out: 32,000 + 41,000 + 27,000 = 100,000 searches · 20 + 2,000 + 24,000 = 26,020 queries".

## Each Zone Needs Its Own Method

**Tags:** `where it's used` (blue), `rule of thumb` (orange)

- **Head: memorize** — click history is deep; the best results can be hand-tuned and cached per query
- **Torso: generalize** — some history exists; models learn patterns shared across similar queries
- **Tail: understand** — no history at all; the engine must read meaning — synonyms, semantics
- **Catalog gaps** — zero-result tail queries map missing words: "hoodie" vs a catalog's "hooded sweatshirt"
- **Report separately** — a volume-weighted average hides tail failure; track head and tail metrics apart

*Example (italic):* A team that hand-tunes its top 20 queries has fixed 32% of searches — and 0.1% of its distinct queries.

**Key point:** One relevance method cannot serve all three zones — deep history at the head, none at the tail — so both the method and the metrics must split by zone.

### Visualization (canvas `c3`, 720×300)

Three horizontal rows, one per zone: solid zone pill → arrow → method box → one-line reason; below them a highlighted catalog-gap callout band.

- **Title (bold 15px ink, top center, y=22):** "Three Zones, Three Methods".
- **Rows at y = 72, 132, 192** (row center). Zone pill (x=30, w=90, h=24, centered on row): solid zone color, bold 12px white "HEAD" / "TORSO" / "TAIL". Arrow right (zone color, 2px, arrowhead) from x=126 to x=152.
- **Method box (x=158, w=200, h=30, centered on row):** tint fill + 2px zone-color border, bold 12px zone-color text centered: "memorize & hand-tune" / "generalize with models" / "understand meaning".
- **Reason (12px `#6b7280`, left-aligned at x=378, on the row center + 4):** "click history is deep — cache the answer" / "some history — learn shared patterns" / "no history — synonyms and semantics".
- **Callout band:** rect (x=30, y=228, w=660, h=48) fill `rgba(201,133,0,0.10)`; bold 12px yellow `#c98500` centered at y=248: "zero-result tail queries map catalog gaps"; 11px `#6b7280` centered at y=266: 'shopper types "hoodie" — the catalog only says "hooded sweatshirt" — zero results, one missing synonym'.

## Most Searches vs Most Queries

**Tags:** `common mistake` (red), `two populations` (orange)

- **Two populations** — "most searches" counts typed events; "most queries" counts distinct strings
- **Searches lean head** — 32% of all searches hit just 20 queries; head plus torso take 73%
- **Queries lean tail** — 92% of distinct queries live in the tail, typed once or twice all week
- **The trap** — a metric averaged over searches can look great while failing most distinct intents
- **Same log, two answers** — "where is the traffic?" says head; "what do people ask?" says tail

*Example (italic):* "We are good on 73% of searches" and "we fail 92% of the queries we see" can both be true of the same engine.

**Key point:** Always say which population a number is over — searches or distinct queries — because the head dominates one and the tail dominates the other.

### Visualization (canvas `c4`, 720×300)

Two 100% stacked horizontal bars over the same log: the split of searches (head-heavy) vs the split of distinct queries (tail-heavy), with a legend and a trap annotation.

- **Title (bold 15px ink, top center, y=22):** "Same Log, Two Very Different Splits (illustrative)".
- **Legend (y=50, centered as a group around x=360):** three 12×12 swatches in the zone tints with 2px zone-color borders, bold 11px zone-color labels "HEAD", "TORSO", "TAIL"; swatch groups at x = 250, 330, 415.
- **Bars:** x=185, w=490, h=36. Segment tint fills + 2px zone-color borders.
  - *Row 1 (y=88):* right-aligned bold 12px ink label "share of searches" at x=175 (baseline y=110); segments head 32% (157px), torso 41% (201px), tail 27% (132px); bold 13px zone-color "%" labels centered inside each segment (y=110).
  - *Row 2 (y=178):* label "share of distinct queries"; segments head 0.1% (2px sliver), torso 7.7% (38px), tail 92.2% (450px); "92.2%" bold 13px orange centered inside the tail segment (y=200); "7.7%" bold 12px green centered below the torso segment (y=232) with a 1px leader tick from (206, 214) to (206, 222); "head: just 0.1%" bold 12px blue left-aligned at (240, 152) with a 1px leader line down to the 2px sliver.
- **Annotation (bold 12px red `#e74c3c`, centered, y=252):** "great on most searches can still mean failing most queries".
- **Caption (11px `#6b7280`, centered, y=285):** "same 100,000-search log as the worked example — numbers illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border (no index number), `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links, no cross-page links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object `P`:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Zone color mapping used on every chart: head = blue, torso = green, tail = orange; red `#e74c3c` only for the trap annotation in c4; magenta only for the never-seen annotation in c1.
- **Data:** everything is hardcoded or deterministic (no `Math.random()`): c1 bar heights come from the closed-form decay `195 * (i+1)^-0.62`; the worked-example numbers 32,000 / 41,000 / 27,000 searches over 20 / 2,000 / 24,000 distinct queries (averages 1,600 / ≈20 / ≈1.1) appear in both text and c2; the c4 splits 32 / 41 / 27 (searches) and 0.1 / 7.7 / 92.2 (distinct queries) derive from the same log. All invented numbers labeled "illustrative"; the ~15% never-seen figure is stated as rough.
- Text numbers match chart numbers everywhere (32,000 / 41,000 / 27,000; 1,600 / 20 / 1.1; 32 / 73 / 92; 0.1).
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
