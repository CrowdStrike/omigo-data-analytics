# Map, Filter, Reduce

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Map, Filter, Reduce

**Subtitle:** Almost every loop is secretly doing three small jobs — change each item, keep some items, combine what's left — and giving each job its own name makes code shorter, clearer, and parallel-ready

## One Stack of Receipts, Three Small Jobs

**Tags:** `core idea` (blue), `the loop decomposed` (green), `named steps` (orange)

- **Closing time** — a coffee shop owner has 8 delivery receipts and wants tonight's big-order total
- **The one big loop** — for each receipt: add the $2 delivery fee, skip it if small, add it to a total
- **Map** — the first job: do the same thing to every receipt (add the $2 fee to each one)
- **Filter** — the second job: keep only receipts that pass a test (at least $10 after the fee)
- **Reduce** — the third job: squash whatever is left into one answer (the evening total, $64)
- **The names** — map, filter, reduce are just those three jobs pulled apart and given names

*Example (italic):* Add $2 to all 8 receipts, keep the 4 that reach $10, add those up to $64 — the same loop, told as three named steps.

**Key point:** Map changes each item, filter drops some items, reduce combines the rest — most loops are some stack of these three moves.

### Visualization (canvas `c1`, 720×300)

Left-to-right pipeline diagram: a column of 8 receipt squares flows through three labeled stage boxes (map, filter, reduce), the item count shrinking 8 → 8 → 4 → 1.

- **Title (bold 15px, `#1a5276`, top center):** "One Loop, Three Jobs: 8 receipts in, 1 answer out".
- **Stage columns (item squares 20×20, 4px vertical gap, columns centered at x = 90, 265, 440, 615):** column 1 "receipts" — 8 blue `#2a78d6` squares (top square at y=55); column 2 "after map (+$2 fee)" — 8 green `#008300` squares; column 3 "after filter (≥ $10)" — 4 orange `#d95926` squares vertically centered; column 4 — one violet `#4a3aa7` circle (radius 26, center y≈160) with bold 15px white "$64" inside.
- **Stage labels (bold 13px `#1a5276`, above each of columns 2–4):** "map", "filter", "reduce"; 11px `#6b7280` sub-labels beneath: "same thing to every item", "keep items passing a test", "combine into one answer".
- **Arrows:** 3px `#6b7280` horizontal arrows with arrowheads between columns at y≈160.
- **Count labels (12px `#444`, below each column, y=272):** "8 items", "8 items", "4 items", "1 answer".
- **Annotation (bold 12px violet `#4a3aa7`, near x=560, y=70):** two lines: "each stage has one job —" / "the loop did all three at once".
- **Caption (11px `#444`, bottom left):** "illustrative — a coffee shop's evening delivery receipts".

## Eight Receipts, By Hand

**Tags:** `worked example` (blue), `pencil and paper` (green)

- **The receipts** — tonight's delivery amounts in dollars: 4, 12, 7, 15, 3, 9, 20, 6
- **Map (+$2 fee)** — every receipt rises by 2: 6, 14, 9, 17, 5, 11, 22, 8 — still eight numbers
- **Filter (≥ $10)** — only 14, 17, 11, 22 pass the test; four survive, none of them changed
- **Reduce (sum)** — keep a running total: 14, then 14+17=31, then 31+11=42, then 42+22=64
- **Check the shape** — 8 numbers in, 8 mapped, 4 kept, 1 out: the answer is $64

*Example (italic):* Two minutes with pencil and paper reproduces the whole pipeline — eight receipts collapse to the single answer $64.

**Key point:** Reduce is a running answer that folds in one item at a time: 0 → 14 → 31 → 42 → 64.

### Visualization (canvas `c2`, 720×300)

Three mini bar panels side by side sharing one dollar scale, showing the same receipts before mapping, after mapping, and after filtering, with the reduce result as a bold total on the right.

- **Title (bold 15px, `#1a5276`, top center):** "4, 12, 7, 15, 3, 9, 20, 6 → map → filter → sum = $64".
- **Shared scale:** dollars 0 to 24 mapped to bar height 0–170px; each panel baseline at y=250; light `#e5e9ef` gridlines at $10 and $20 across each panel with 11px `#6b7280` labels "$10", "$20" at the left edge of panel 1.
- **Panel 1 (x=60–240, bold 12px `#444` header "start"):** 8 bars width 16, gap 6, values `[4, 12, 7, 15, 3, 9, 20, 6]`, fill `rgba(42,120,214,0.35)`, 1px `#2a78d6` stroke; 11px `#444` value labels above each bar.
- **Panel 2 (x=270–450, header "map: +$2 fee"):** 8 bars, values `[6, 14, 9, 17, 5, 11, 22, 8]`, fill `rgba(0,131,0,0.30)`, 1px `#008300` stroke; 11px value labels; dashed `#6b7280` (dash 4/3) horizontal cut line at $10 with 11px label "keep ≥ $10".
- **Panel 3 (x=480–600, header "filter: 4 survive"):** 4 bars, values `[14, 17, 11, 22]`, fill `rgba(217,89,38,0.35)`, 1px `#d95926` stroke; 12px `#444` value labels.
- **Reduce block (x=615–700):** bold 13px `#1a5276` header "reduce"; 12px `#444` running-total lines stacked at y = 130, 150, 170, 190: "14", "31", "42", "64"; below them a bold 16px violet `#4a3aa7` "$64" at y=225.
- **Annotation (bold 12px green `#008300`, above panel 2 near y=60):** "still 8 bars — map never drops items".
- **Caption (11px `#444`, bottom right):** "illustrative — amounts match the worked example in the text".

## The Same Three Jobs Everywhere

**Tags:** `where it's used` (blue), `parallel work` (green), `big data` (orange)

- **SQL** — SELECT computes a map, WHERE is a filter, SUM or COUNT is a reduce; every query is the trio
- **Spreadsheets** — a formula column maps, hiding rows filters, the =SUM() cell at the bottom reduces
- **Pandas and Spark** — column arithmetic maps, boolean masks filter, `.sum()` reduces, at any scale
- **Split the stack** — map and filter touch one item at a time, so two workers can share the receipts
- **Meet in the middle** — worker A totals $31, worker B totals $33; one final add gives the same $64

*Example (italic):* Hand half the receipts to each of two baristas: each maps, filters, and sums their own half (31 and 33), and one last addition finishes the night.

**Key point:** This decomposition is why big-data systems work — map and filter run anywhere in parallel, and only the tiny reduce step has to meet in the middle.

### Visualization (canvas `c3`, 720×300)

Two-branch flow diagram: the receipt stack splits between two workers, each runs its own map + filter + sum, and the two partial totals merge into the same $64 as the by-hand version.

- **Title (bold 15px, `#1a5276`, top center):** "Two Workers, Same Answer: 31 + 33 = 64".
- **Input box (x=30–130, centered at y=150):** rounded rect, fill `rgba(42,120,214,0.12)`, 1px `#2a78d6` border, bold 13px `#1a5276` "8 receipts" with 11px `#6b7280` second line "4, 12, 7, 15, 3, 9, 20, 6".
- **Branch arrows:** 3px `#6b7280` arrows from the input box to two worker lanes at y=95 (worker A) and y=205 (worker B).
- **Worker A lane (y=95), three rounded boxes left to right at x = 170, 330, 490 (each ~130 wide):** "map: 6, 14, 9, 17" (green `#008300` border), "filter: 14, 17" (orange `#d95926` border), "sum: 31" (bold 13px `#1a5276`); 3px `#6b7280` arrows between boxes; 12px `#444` lane label "worker A: 4, 12, 7, 15" above at y=62.
- **Worker B lane (y=205), same layout:** "map: 5, 11, 22, 8", "filter: 11, 22", "sum: 33"; lane label "worker B: 3, 9, 20, 6" below at y=240; box text 12px `#2c3e50`.
- **Merge:** arrows from both sum boxes converging to a violet `#4a3aa7` circle (radius 26) at x=655, y=150 with bold 15px white "$64" inside; 11px `#6b7280` label "one final add" beneath it.
- **Annotation (bold 12px orange `#d95926`, near x=330, y=150, between the lanes):** "map and filter never look at other items — safe to split".
- **Caption (11px `#444`, bottom left):** "illustrative — same receipts as the worked example, split in half".

## Which Job Changes What

**Tags:** `common mistake` (red), `shape of the data` (orange)

- **Map** — changes every value but never the count: 8 receipts in, 8 receipts out
- **Filter** — changes the count but never the values: 8 in, 4 out, each survivor untouched
- **Reduce** — collapses the shape entirely: 4 numbers in, 1 answer out
- **Reduce is not just sum** — max, min, count, and "join names into one line" are all reduces
- **The muddle** — a map that quietly drops items, or a filter that edits them, hides bugs in plain sight

*Example (italic):* If the receipt count changes after your "map" step, it wasn't a map — something filtered, and now the totals won't reconcile.

**Common mistake:** Blending the jobs — one step that edits values AND drops items is hard to test and hard to parallelize; keep "change", "keep", and "combine" as three separate, named moves.

### Visualization (canvas `c4`, 720×300)

Three-row shape chart: each row shows one operation as squares-in → squares-out, making visible that map preserves count, filter shrinks count, and reduce collapses to a single value.

- **Title (bold 15px, `#1a5276`, top center):** "What Each Job Does to the Shape".
- **Rows (centered at y = 90, 160, 230), each with a bold 13px `#1a5276` label at x=30:** "map", "filter", "reduce"; 11px `#6b7280` sub-label beneath each: "8 in → 8 out", "8 in → 4 out", "4 in → 1 out".
- **Row 1 (map):** 8 blue `#2a78d6` 20×20 squares starting at x=170 (gap 5); 3px `#6b7280` arrow at x≈390; 8 green `#008300` squares starting at x=440 — same count, new color for "values changed".
- **Row 2 (filter):** 8 green squares starting at x=170, the four at positions 1, 3, 5, 8 (the dropped mapped values 6, 9, 5, 8) drawn faded `rgba(0,131,0,0.20)` with 1px dashed border; arrow; 4 solid orange `#d95926` squares starting at x=440 — fewer items, same values.
- **Row 3 (reduce):** 4 orange squares starting at x=170; arrow; one violet `#4a3aa7` circle (radius 16) at x≈460 with bold 12px white "1" inside; 12px `#444` note at x=500: "sum, max, count, join — all reduces".
- **Annotation (bold 12px red `#e74c3c`, below row 1 near x=500, y=118):** two lines: "count changed after a map?" / "something else snuck in".
- **Caption (11px `#444`, bottom right):** "illustrative — square counts match the worked example (8, 8, 4, 1)".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all bar values, box labels, and partial sums are the hardcoded literal arrays above (no randomness); the receipt amounts `[4, 12, 7, 15, 3, 9, 20, 6]`, mapped values `[6, 14, 9, 17, 5, 11, 22, 8]`, filtered survivors `[14, 17, 11, 22]`, partial sums 31 and 33, and total 64 must agree across all four charts and the text.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
