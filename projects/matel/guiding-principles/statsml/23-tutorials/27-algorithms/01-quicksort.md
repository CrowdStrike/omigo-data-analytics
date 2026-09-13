# Quicksort

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Quicksort

**Subtitle:** Quicksort sorts a pile by picking one item as a pivot, splitting everything else into a smaller pile and a bigger pile, then doing the same to each pile — every split leaves the pivot in its final place

## One Slip Splits the Whole Stack

**Tags:** `core idea` (blue), `pivot` (green), `partition` (orange)

- **The stack** — a barista has seven order slips in random order: $7, $3, $9, $2, $8, $4, $6
- **The pivot** — she grabs the last slip, $6, and calls it the pivot: the yardstick for one pass
- **The split** — every other slip goes into two piles: cheaper than $6 to the left, pricier to the right
- **One pass** — after checking all 6 slips she has [$3, $2, $4] on the left and [$7, $9, $8] on the right
- **Pivot placed** — $6 sits between the piles in position 4, exactly where it belongs in the final order
- **Repeat** — each pile is a smaller version of the same problem, so she splits each pile the same way

*Example (italic):* The $6 slip never moves again — three slips are cheaper, three are pricier, so fourth place is its final home even though nothing else is sorted yet.

**Key point:** One partition pass does two jobs at once: it puts the pivot in its final position and splits the rest into two smaller sorting problems.

### Visualization (canvas `c1`, 720×300)

Two-row box diagram: the unsorted stack on top, the partitioned stack below, with arrows showing each slip flowing into the cheaper pile, the pivot slot, or the pricier pile.

- **Title (bold 15px, `#1a5276`, top center):** "One Pass Around the $6 Pivot".
- **Top row (y=75, boxes 56×40, gap 14, starting x=115):** seven boxes labeled "$7", "$3", "$9", "$2", "$8", "$4", "$6" in bold 14px `#2c3e50`; fill `#f8f9fa`, 2px `#6b7280` border; the "$6" box gets a 3px blue `#2a78d6` border and a 12px blue label "pivot" above it.
- **Bottom row (y=200, same box size and spacing, starting x=115):** boxes "$3", "$2", "$4" with green `#008300` 2px borders and fill `rgba(0,131,0,0.12)`; box "$6" with 3px blue border and fill `rgba(42,120,214,0.15)`; boxes "$7", "$9", "$8" with orange `#d95926` 2px borders and fill `rgba(217,89,38,0.12)`.
- **Arrows:** 2px `#6b7280` lines with small arrowheads from each top box center-bottom to its bottom box center-top (e.g. top "$7" at slot 1 down to bottom slot 5, top "$3" to slot 1, "$9" to slot 6, "$2" to slot 2, "$8" to slot 7, "$4" to slot 3, "$6" straight down to slot 4).
- **Pile labels (12px, under bottom row at y=258):** green "cheaper than $6" centered under slots 1–3, blue "pivot" under slot 4, orange "pricier than $6" under slots 5–7.
- **Annotation (bold 13px blue `#2a78d6`, right side near x=560, y=140):** two lines: "$6 lands in its final spot —" / "after just one pass".
- **Caption (12px `#444`, bottom right):** "illustrative order slips from one coffee-shop morning".

## Sorting Seven Slips by Hand

**Tags:** `worked example` (blue), `recursion` (green)

- **Level 0** — start with [$7, $3, $9, $2, $8, $4, $6]; pivot $6 costs 6 comparisons to place
- **Level 1 left** — [$3, $2, $4] with pivot $4: 2 comparisons give [$3, $2] left, nothing right
- **Level 1 right** — [$7, $9, $8] with pivot $8: 2 comparisons give [$7] left, [$9] right
- **Level 2** — [$3, $2] with pivot $2: 1 comparison sends $3 right; single slips need no work
- **Read it off** — the pivots and singletons line up as $2, $3, $4, $6, $7, $8, $9 — fully sorted
- **The bill** — 6 + 2 + 2 + 1 = 11 comparisons total to sort all seven slips

*Example (italic):* Anyone can redo this on paper: write [7, 3, 9, 2, 8, 4, 6], circle the last number, split, and repeat — three rounds of splitting finish the job in 11 comparisons.

**Key point:** Quicksort never merges anything back — once every pile is a pivot or a single slip, the sorted order is just the piles read left to right.

### Visualization (canvas `c2`, 720×300)

Recursion-tree diagram: four levels of boxes showing each pile splitting around its pivot, with the comparison count per level on the right margin.

- **Title (bold 15px, `#1a5276`, top center):** "The Full Split Tree: 11 Comparisons in Three Rounds".
- **Levels at y = 70, 130, 190, 250; boxes 22px tall, 13px bold `#2c3e50` digits, 6px corner radius.**
- **Level 0 (centered x=330):** one wide box "7 3 9 2 8 4 [6]", fill `#f8f9fa`, 2px `#6b7280` border; the pivot digit 6 drawn in blue `#2a78d6`.
- **Level 1:** left box "3 2 [4]" centered x=180, right box "7 9 [8]" centered x=480; pivot digits 4 and 8 in blue; fill `#f8f9fa`, 2px `#6b7280` border; 2px `#6b7280` connector lines from the level-0 box.
- **Level 2:** box "[2] 3" centered x=120 (pivot 2 blue), box "7" centered x=430, box "9" centered x=530; singletons get green `#008300` 2px borders and fill `rgba(0,131,0,0.12)`; connectors from their parents.
- **Level 3:** box "3" centered x=155, green singleton style; connector from "[2] 3".
- **Placed pivots:** once a pivot is placed, repeat it at its level as a small blue-bordered box with fill `rgba(42,120,214,0.15)` ("6" at level 1 center x=330, "4" at level 2 x=205, "8" at level 2 x=480, "2" at level 3 x=105).
- **Right margin (12px `#444`, x=620, one per level):** "6 comparisons", "2 + 2", "1", "0" — with a bold 13px `#1a5276` total "= 11" below the last.
- **Annotation (bold 12px green `#008300`, bottom center y=285):** "read the leaves and pivots left to right: 2 3 4 6 7 8 9".

## Why Splitting Beats Scanning

**Tags:** `where it's used` (blue), `speed` (green), `divide & conquer` (orange)

- **Everywhere** — sorting powers rankings, medians, percentiles, joins; data work sorts constantly
- **The slow way** — repeatedly scanning the pile for the next-smallest slip costs about n²/2 checks
- **The fast way** — good splits halve the piles each round, so total work is about n × log₂(n)
- **Small case** — 8 slips: roughly 24 comparisons with halving splits versus 28 with repeated scans
- **Big case** — 128 slips: about 896 comparisons with halving splits versus 8,128 with scans
- **The gap grows** — every doubling of the pile roughly doubles quicksort's work but quadruples the scan's

*Example (italic):* At 128 slips the split-based sort does 896 comparisons while the scan-everything approach does 8,128 — nine times the work for the same sorted stack.

**Key point:** Halving the pile each round is the whole trick: log₂(128) = 7 rounds of cheap passes replaces 127 expensive full scans.

### Visualization (canvas `c3`, 720×300)

Two-line growth chart: comparisons versus number of slips for balanced quicksort (n·log₂n) and repeated scanning (n(n−1)/2), on a shared linear axis.

- **Title (bold 15px, `#1a5276`, top center):** "Comparisons Needed: Splitting vs Scanning".
- **Axes:** origin x=80, baseline y=245, plot width 580, plot height 185; x = slips with tick labels "8", "16", "32", "64", "128" (12px `#444`) evenly spaced; y = comparisons 0 to 9,000 with light `#e5e9ef` gridlines and 12px `#444` labels at 2,000, 4,000, 6,000, 8,000.
- **Scan line (n(n−1)/2):** orange `#d95926` 3px line with 5px dots through hardcoded points x = `[8, 16, 32, 64, 128]`, y = `[28, 120, 496, 2016, 8128]`; 12px orange label "scan for next smallest" above the line near x=64.
- **Quicksort line (n·log₂n):** blue `#2a78d6` 3px line with 5px dots through the same x grid, y = `[24, 64, 160, 384, 896]`; 12px blue label "quicksort, balanced splits" below the line near x=96.
- **Gap marker:** vertical dashed `#6b7280` (dash 4/3) line at x=128 between the two endpoints.
- **Annotation (bold 13px blue `#2a78d6`, near x=430, y=85):** two lines: "128 slips: 896 vs 8,128 —" / "9× less work, and the gap keeps growing".
- **Caption (12px `#444`, bottom right):** "counts from the formulas n·log₂n and n(n−1)/2".

## The Sorted-Pile Trap

**Tags:** `common mistake` (red), `pivot choice` (orange)

- **The trap** — hand quicksort an already-sorted stack [$2, $3, $4, $6, $7, $8, $9] with last-slip pivots
- **Lopsided splits** — pivot $9 puts all 6 slips on one side and none on the other; no halving happens
- **Every round repeats it** — the pile shrinks by only one slip per pass: 6, 5, 4, 3, 2, 1 comparisons
- **The bill** — 21 comparisons for the same seven slips that balanced splits sorted in 11
- **The mistake** — assuming quicksort is always fast; a bad pivot rule turns it into the slow scan
- **The fix** — pick pivots randomly or take the median of first, middle, and last slip

*Example (italic):* The tidiest input is the worst case here — a stack already in order makes every last-slip pivot the maximum, so each pass places one slip and re-scans the rest.

**Common mistake:** Judging quicksort by its best case. Its speed comes from balanced splits, not from the algorithm's name — the pivot rule decides which sort you actually get.

### Visualization (canvas `c4`, 720×300)

Horizontal staircase bar chart: one bar per pass on the already-sorted stack, each bar one slip shorter than the last, with per-pass comparison counts and the 21-vs-11 total called out.

- **Title (bold 15px, `#1a5276`, top center):** "Already Sorted + Last-Slip Pivot: the Pile Shrinks by One".
- **Bars (left edge x=180, rows at y = 62, 95, 128, 161, 194, 227; height 22px):** widths proportional to pile size 7, 6, 5, 4, 3, 2 slips (64px per slip, so 448, 384, 320, 256, 192, 128); fill `rgba(217,89,38,0.25)`, 2px orange `#d95926` border; the pivot cell (rightmost 64px of each bar) filled `rgba(217,89,38,0.55)` with bold 12px white pivot labels "$9", "$8", "$7", "$6", "$4", "$3".
- **Row labels (12px `#444`, right-aligned at x=170):** "pass 1 — 7 slips", "pass 2 — 6 slips", "pass 3 — 5 slips", "pass 4 — 4 slips", "pass 5 — 3 slips", "pass 6 — 2 slips".
- **Comparison counts (bold 12px `#d95926`, just right of each bar):** "6", "5", "4", "3", "2", "1".
- **Total line:** 2px `#999` horizontal rule at y=262 from x=180 to x=660; bold 13px `#d95926` label below at y=280: "total 21 comparisons — the balanced tree above needed 11".
- **Annotation (bold 13px violet `#4a3aa7`, near x=470, y=180):** two lines: "no halving, just shaving —" / "each pass places only the pivot".
- **Caption (12px `#444`, bottom right):** "same seven slips as above, fed in sorted order".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (here it hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all slip values, tree contents, and curve points are the hardcoded arrays above (no randomness); the c3 growth lines are exact values of n·log₂n and n(n−1)/2 at n = 8, 16, 32, 64, 128; comparison totals (11 balanced, 21 worst-case) must match between text and charts.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
