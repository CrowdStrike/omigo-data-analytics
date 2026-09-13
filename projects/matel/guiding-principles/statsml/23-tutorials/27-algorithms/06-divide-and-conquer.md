# Divide and Conquer

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Divide and Conquer

**Subtitle:** Divide and conquer means splitting one big job into small copies of the same job, solving the tiny ones, and combining the answers back up — most of the real work hides in the combining

## Sorting a Shoebox of Receipts

**Tags:** `core idea` (blue), `split-solve-combine` (green), `running example` (orange)

- **The shoebox** — a shop owner dumps a shoebox of receipts on the table and wants them in date order
- **Too big at once** — eyeballing the whole messy pile is hopeless, but sorting a tiny pile is easy
- **Divide** — split the pile in half, split each half again, and keep halving down to piles of one
- **Conquer** — a pile of one receipt is already sorted; nothing to do at the bottom
- **Combine** — merge sorted piles back together two at a time until one fully sorted stack remains
- **The paradigm** — this split-solve-combine pattern is called divide and conquer

*Example (italic):* Eight receipts dated 23, 4, 17, 9, 30, 2, 12, 26 become two piles of four, then four piles of two, then eight piles of one — and get merged back in order.

**Key point:** Divide and conquer turns one impossible-looking job into many trivial ones plus a combine step — split until easy, then merge the answers back up.

### Visualization (canvas `c1`, 720×300)

Top-down split tree: the eight-receipt pile halving level by level, with a SPLIT label on the way down and the stopping rule called out at the bottom.

- **Title (bold 15px, `#1a5276`, top center):** "Divide: Keep Halving Until Each Pile Is One Receipt".
- **Level rows (box centers at y = 75, 135, 195, 255):** rounded boxes (6px radius, 1.5px `#2a78d6` border, fill `rgba(42,120,214,0.10)`, bold 12px `#2c3e50` centered text):
  - Level 0, one box (width 300) centered at x=360: "23 4 17 9 30 2 12 26"
  - Level 1, two boxes (width 150) at x=210 and x=510: "23 4 17 9" and "30 2 12 26"
  - Level 2, four boxes (width 84) at x=120, 300, 460, 640: "23 4", "17 9", "30 2", "12 26"
  - Level 3, eight boxes (width 34) at x=95, 145, 275, 325, 435, 485, 615, 665: "23", "4", "17", "9", "30", "2", "12", "26"
- **Arrows:** 1.5px `#6b7280` lines with small arrowheads from each box's bottom edge to its two children's top edges.
- **Side label (bold 13px `#d95926`, rotated or stacked at x=25, y≈160):** "SPLIT" with a downward arrow beneath it.
- **Annotation (bold 12px green `#008300`, bottom right near x=540, y=290):** "a pile of one is already sorted — stop here".
- **Caption (11px `#444`, bottom left):** "receipt dates are day-of-month, illustrative".

## Eight Receipts, Sixteen Comparisons

**Tags:** `worked example` (blue), `merge step` (green)

- **Merge pairs** — each pile of two needs one look: [4 23], [9 17], [2 30], [12 26] — 4 comparisons
- **Merge fours** — [4 23]+[9 17] takes 3 looks, [2 30]+[12 26] takes 3 — piles [4 9 17 23], [2 12 26 30]
- **Final merge** — walk both piles front-to-front, always taking the smaller date: 6 comparisons
- **Grand total** — 4 + 6 + 6 = 16 comparisons and the stack reads 2, 4, 9, 12, 17, 23, 26, 30
- **Brute force** — checking every receipt against every other is 28 comparisons for the same 8 receipts

*Example (italic):* In the final merge, 4 vs 2 takes 2, then 4 vs 12 takes 4, then 9, 12, 17, 23 — and 26, 30 just slide in for free.

**Key point:** Merging two sorted piles is cheap because you only ever compare the two front receipts — 16 comparisons sort the shoebox instead of 28.

### Visualization (canvas `c2`, 720×300)

Bottom-up merge diagram of the final step: the two sorted piles of four flowing into one sorted row of eight, with the comparison sequence written out.

- **Title (bold 15px, `#1a5276`, top center):** "Combine: Merge Two Sorted Piles Front-to-Front".
- **Input piles (y=90):** two rows of four rounded boxes (width 52, height 30, 6px radius, bold 13px centered text): left pile at x = 80, 140, 200, 260 with values "4", "9", "17", "23" (border `#2a78d6`, fill `rgba(42,120,214,0.12)`); right pile at x = 420, 480, 540, 600 with values "2", "12", "26", "30" (border `#199e70`, fill `rgba(25,158,112,0.12)`); 12px `#6b7280` labels "left pile (sorted)" and "right pile (sorted)" above each at y=60.
- **Output row (y=215):** eight boxes (width 52, height 30) at x = 100, 165, 230, 295, 360, 425, 490, 555 with values "2", "4", "9", "12", "17", "23", "26", "30"; each colored to match its source pile (blue fill for 4, 9, 17, 23; aqua fill for 2, 12, 26, 30); 12px `#444` label "one sorted stack" below at y=255.
- **Arrows:** 1.5px lines (blue from left-pile boxes, aqua from right-pile boxes) from each input box bottom to its output box top, small arrowheads.
- **Comparison ticker (12px `#444`, single line at y=280, left-aligned at x=80):** "compares: 4v2, 4v12, 9v12, 17v12, 17v26, 23v26 — then 26, 30 slide in".
- **Annotation (bold 13px orange `#d95926`, near x=360, y=155):** "6 comparisons here — 16 for the whole sort".

## Why Halving Beats Brute Force

**Tags:** `where it's used` (blue), `scaling` (green), `rule of thumb` (orange)

- **Small stakes** — at 8 receipts the gap is 16 vs 28 looks; nobody cares either way
- **Big stakes** — at 800 receipts brute force needs 319,600 comparisons; halving needs about 7,715
- **The ratio** — that is roughly 41 times less work, and the gap keeps widening as the pile grows
- **Why** — every merge level touches each receipt once, and halving 800 gives only about 10 levels
- **Everywhere** — fast sorting, searching, multiplying big numbers, and map-reduce jobs all halve like this

*Example (italic):* Doubling the pile from 400 to 800 receipts quadruples brute-force work (79,800 to 319,600) but barely doubles the halving approach (3,458 to 7,715).

**Key point:** Brute force grows with the square of the pile; divide and conquer grows with the pile times its number of halvings — at 800 receipts that is 319,600 vs 7,715 looks.

### Visualization (canvas `c3`, 720×300)

Two-line growth chart: comparisons needed vs pile size for brute force and for divide and conquer, on a shared linear axis, with every point labeled.

- **Title (bold 15px, `#1a5276`, top center):** "Comparisons Needed: Brute Force vs Divide and Conquer".
- **Axes:** origin x=90, baseline y=245, plot width 570, plot height 185; x = pile size with tick labels "100", "200", "400", "800" (12px `#444`) at evenly spaced positions x = 160, 300, 440, 580; y = comparisons 0 to 320,000, light `#e5e9ef` gridlines at 80,000 / 160,000 / 240,000 / 320,000 with 11px `#6b7280` labels "80k", "160k", "240k", "320k".
- **Brute force line:** orange `#d95926` 3px line with 6px dots through points (pile, comparisons) = `[100, 200, 400, 800]` vs `[4950, 19900, 79800, 319600]`; 12px orange value labels "4,950", "19,900", "79,800", "319,600" beside each dot; bold 12px orange series label "check every pair" near x=470, y=110.
- **Divide-and-conquer line:** green `#008300` 3px line with 6px dots through the same pile sizes vs `[664, 1529, 3458, 7715]`; 12px green value label "7,715" beside the last dot; bold 12px green series label "halve and merge" near x=470, y=225.
- **Annotation (bold 13px `#1a5276`, near x=180, y=75):** two lines: "800 receipts: 319,600 vs 7,715" / "about 41x less work".
- **Caption (11px `#444`, bottom right):** "counts are formula-based: n(n-1)/2 vs n log2 n, rounded".

## The Split Is Free — the Merge Does the Work

**Tags:** `common mistake` (red), `combine step` (orange)

- **The illusion** — splitting the pile feels like progress, but no receipt moves toward its place
- **Count it** — in the 8-receipt sort, splitting costs 0 comparisons; merging costs all 16
- **The lesson** — divide and conquer only wins when small answers combine cheaply into big ones
- **Bad fit** — if merging two answers is as hard as the original job, halving buys you nothing
- **Not just halving** — cutting a pile into random uneven chunks with no combine plan is not the paradigm

*Example (italic):* Splitting the shoebox into "left half, right half" over and over produces eight tidy piles of one — and the receipts are exactly as unsorted as when you started.

**Common mistake:** Thinking the splitting sorts the receipts. It never does — every one of the 16 comparisons happens on the way back up, so always ask "how do two solved halves combine?" before halving anything.

### Visualization (canvas `c4`, 720×300)

Bar chart of comparisons spent at each stage of the eight-receipt sort, showing the split stage flat at zero and all effort stacked in the three merge stages.

- **Title (bold 15px, `#1a5276`, top center):** "Where the 16 Comparisons Actually Happen".
- **Axes:** origin x=90, baseline y=235, plot width 570, plot height 165; y = comparisons 0 to 8, light `#e5e9ef` gridlines at 2, 4, 6, 8 with 11px `#6b7280` labels.
- **Bars (width 90, centered at x = 165, 305, 445, 585):** stage vs comparisons = `["split (all levels)", "merge pairs", "merge fours", "final merge"]` vs `[0, 4, 6, 6]`; split bar drawn as a 2px `#6b7280` flat dash on the baseline, merge bars filled `rgba(0,131,0,0.30)` with 2px `#008300` border; bold 13px value labels above each bar: "0", "4", "6", "6" (green for the merges, `#6b7280` for the zero).
- **X labels:** 12px `#444`, two lines where needed, centered under each bar at y=255/270.
- **Annotation (bold 13px magenta `#d55181`, near x=165, y=95):** two lines: "splitting: zero comparisons —" / "all 16 live in the combine step".
- **Caption (11px `#444`, bottom right):** "counts from the eight-receipt example above".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all box values, comparison counts, and curve points are the hardcoded arrays above (no randomness); c3 values are n(n-1)/2 and round(n·log2(n)) for n in [100, 200, 400, 800]; the 16-comparison breakdown (4 + 3 + 3 + 6) is the true merge-sort trace of [23, 4, 17, 9, 30, 2, 12, 26].
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
