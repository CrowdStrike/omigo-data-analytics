# Pigeonhole Principle

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Pigeonhole Principle

**Subtitle:** Put more items into fewer boxes and some box must hold at least two — a one-line argument that rules out collision-free hashing, shrink-everything compression, and all-distinct data

## Thirteen Coworkers, Twelve Months

**Tags:** `core idea` (blue), `one-line proof` (green), `guarantee` (orange)

- **The office** — 13 coworkers write their birth months on a whiteboard with only 12 month boxes
- **Forced overlap** — with 13 names and 12 boxes, every possible arrangement doubles up somewhere
- **No data needed** — you can promise a shared month before anyone writes a single name down
- **The principle** — put n items into k boxes with n > k, and some box must hold at least two
- **One line** — the whole proof: if every box held at most one name, at most 12 people would fit

*Example (italic):* Even an adversary assigning months to dodge overlaps fails — the 13th coworker has nowhere new to go.

**Key point:** The pigeonhole principle costs one line: more items than boxes forces a shared box. It is a guarantee about existence — no probability, no data collection involved.

### Visualization (canvas `c1`, 720×300)

Schematic: a row of 13 person dots dropped into 12 month boxes; one box is forced to hold two.

- **Title (bold 15px, `#1a5276`, top center):** "13 Coworkers Dropped Into 12 Birth-Month Boxes".
- **Data:** months `["J","F","M","A","M","J","J","A","S","O","N","D"]`; box counts `[1,1,2,1,1,1,1,1,1,1,1,1]` (March holds 2).
- **Top row:** 13 blue `#2a78d6` 8px dots evenly spaced from x=95 to x=625 at y=64; bold 12px `#6b7280` label "13 coworkers" centered above at y=46.
- **Boxes:** 12 rectangles, width 44, gap 4, starting x=70, from y=120 to y=205, 1.5px `#1a5276` border, no fill; month letter 12px `#444` centered below each box at y=222.
- **Dots in boxes:** one blue `#2a78d6` 7px dot centered at y=162 in every box except March; the March box holds two magenta `#d55181` 7px dots at y=148 and y=178.
- **Annotation (bold 13px magenta `#d55181`):** "two share March — forced" at x=175, y=105, with a 1.5px magenta line down to the top edge of the March box.
- **Caption (12px `#444`, centered, y=262):** "13 items into 12 boxes → some box holds at least 2".

## How Crowded Must the Fullest Month Be?

**Tags:** `worked example` (blue), `rule of thumb` (green)

- **More people** — the office grows to 40 coworkers, still spread across the same 12 birth months
- **Even spread** — the flattest possible split of 40 over 12 is eight months of 3 and four months of 4
- **The floor** — some month must hold at least ceil(40/12) = 4 people, no matter the arrangement
- **This office** — the real counts run 3, 2, 4, 3, 5, 3, 4, 2, 3, 4, 3, 4, so May tops out at 5
- **General rule** — n items in k boxes always force some box holding at least ceil(n/k) items

*Example (italic):* Before seeing any list, you can promise the fullest month has at least 4 birthdays — May's 5 clears it.

**Key point:** The general form: n items in k boxes force some box with at least ceil(n/k). It is a floor on the fullest box, not an average — real data usually beats it.

### Visualization (canvas `c2`, 720×300)

Bar chart of 40 birthdays by month with a dashed line marking the guaranteed floor of 4 for the fullest month.

- **Title (bold 15px, `#1a5276`, top center):** "40 Coworkers by Birth Month vs the Guaranteed Floor".
- **Data:** months `["J","F","M","A","M","J","J","A","S","O","N","D"]`; counts `[3, 2, 4, 3, 5, 3, 4, 2, 3, 4, 3, 4]` (sum 40).
- **Axes:** origin x=60, baseline y=240, plot width 600, chart height 175, y scale 0–6; y ticks 0/2/4/6 labeled 12px `#444`; light grid lines `#e5e9ef`.
- **Bars:** width 34, gap 16, fill `rgba(42,120,214,0.45)`, 1px `#2a78d6` stroke; count labels bold 12px `#2a78d6` above each bar; month letters 12px `#444` below baseline.
- **Floor line:** dashed green `#008300` (dash 5/4) horizontal line at count 4, with bold 12px green label "guaranteed floor for the fullest month: ceil(40/12) = 4" above it at the right end.
- **Annotation (bold 13px magenta `#d55181`):** "May: 5 — beats the floor" above the May bar, with a short magenta tick to the bar top.
- **Caption (12px `#444`, centered, y=290):** "counts sum to 40; some bar must reach 4 in every possible arrangement".

## Hash Buckets and the Compression That Can't Exist

**Tags:** `where it's used` (blue), `collisions` (orange), `impossibility` (red)

- **Hash buckets** — the office app hashes 1,000 customer IDs into 256 buckets, so collisions are certain
- **The floor again** — some bucket must hold at least ceil(1000/256) = 4 IDs, whatever hash you pick
- **No escape** — a "better hash function" cannot help; only 1,000 or more buckets removes the guarantee
- **Compression** — a compressor that shrinks every file is impossible: there are fewer short files than long ones
- **Tiny case** — there are 8 three-bit files but only 4 two-bit files, so two inputs must share an output
- **Duplicates** — 1,000 customers with whole-number ages 0–99 must include at least 10 sharing one age

*Example (italic):* A vendor promising a zero-collision hash for 1,000 keys in 256 slots is selling arithmetic that cannot exist.

**Key point:** When a claim says collision-free, always-smaller, or all-distinct, count the boxes first — if items outnumber boxes, pigeonhole kills the claim in one line.

### Visualization (canvas `c3`, 720×300)

Dual panel split by a vertical dashed divider at x=360: sample hash-bucket bars with the guaranteed floor (left), and the 8-files-into-4-slots compression funnel (right).

- **Title (bold 15px, `#1a5276`, top center):** "Two Guaranteed Collisions: Hashing and Compression".
- **Left panel (hash buckets):** heading bold 12px `#444` "1,000 IDs → 256 buckets" at x=60, y=48; bars for 10 sample buckets with counts `[4, 3, 5, 4, 3, 4, 6, 3, 4, 4]`; axis origin x=50, plot width 290, baseline y=240, chart height 155, y scale 0–7; bar width 20, gap 9, fill `rgba(42,120,214,0.45)`; dashed ink `#1a5276` (dash 5/4) line at count 4 labeled bold 12px ink "floor: ceil(1000/256) = 4"; magenta `#d55181` bold 12px annotation "some bucket ≥ 4 — guaranteed" above the tallest bar; caption 12px `#444` "10 of the 256 buckets shown (illustrative)".
- **Right panel (compression):** heading bold 12px `#444` "all 3-bit files (8)" above x=420 and "all 2-bit files (4)" above x=610, both at y=48; left column of 8 blue `#2a78d6` 6px dots at x=420, y from 70 to 245 in steps of 25; right column of 4 green `#008300` 7px dots at x=610, y at 95, 137, 179, 221; gray `#999` 1.5px arrows pairing consecutive left dots into each right dot (two arrows per output); the top pair's two arrows drawn magenta `#d55181` 2.5px; magenta bold 12px annotation, two lines, at x=470, y=270: "two inputs, one output —" / "decompression can't tell them apart".
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=360 from y=38 to h-12.

## A Guarantee Is Not a Probability

**Tags:** `common mistake` (red), `birthday paradox` (orange)

- **Two questions** — "must two share?" is pigeonhole; "how likely is it two share?" is the birthday paradox
- **The guarantee** — with 365 possible birthdays, a shared day is only certain once you have 366 people
- **The surprise** — yet at just 23 people, the chance of a shared day already reaches 50.7%
- **Don't mix them** — 23 people can still all differ; 366 people cannot, and that gap is the whole point
- **Existence only** — pigeonhole says a crowded box exists; it never says which box, or more than the floor

*Example (italic):* A quiz answer of "23 people guarantee a shared birthday" confuses a coin worth flipping with a certainty.

**Common mistake:** Quoting the birthday paradox as if 23 people guarantee a match. Probability climbs fast, but pigeonhole certainty only arrives when the boxes run out — at 366 people, not 23.

### Visualization (canvas `c4`, 720×300)

Line chart of the birthday-paradox probability as the group grows, with the 23-person likely point marked and the 366-person certainty called out off-chart.

- **Title (bold 15px, `#1a5276`, top center):** "Likely vs Guaranteed: Chance of a Shared Birthday".
- **Data:** people `[5, 10, 15, 20, 23, 30, 40, 50, 60, 70, 80]`; probability % `[2.7, 11.7, 25.3, 41.1, 50.7, 70.6, 89.1, 97.0, 99.4, 99.9, 99.99]`.
- **Axes:** origin x=60, baseline y=245, plot width 560, chart height 185; x scale 0–80 people with ticks at 0/20/40/60/80; y scale 0–100% with ticks at 0/25/50/75/100; tick labels 12px `#444`; light grid lines `#e5e9ef`; axis titles 12px `#6b7280` "people in the group" (below) and "% chance" (left, rotated or above axis).
- **Curve:** blue `#2a78d6` 3px polyline through the data points with 4px dots.
- **Marker at 23:** dashed magenta `#d55181` (dash 4/3) crosshair lines from (23, 50.7) to both axes; 6px magenta dot; bold 13px magenta annotation "23 people: 50.7% — likely, not certain" placed right of the point.
- **Certainty callout (bold 13px green `#008300`, right edge near the 100% line):** "certain only at 366 people (pigeonhole) →".
- **Caption (12px `#444`, centered, y=292):** "365 possible birthdays; chance that at least two people share a day".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (here it hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
