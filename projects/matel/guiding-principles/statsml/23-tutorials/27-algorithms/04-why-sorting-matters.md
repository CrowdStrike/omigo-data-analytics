# Why Sorting Matters

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Why Sorting Matters

**Subtitle:** Sorting 1,000 items costs only about 10,000 steps — cheap enough that "sort first, then scan" turns many compare-everything problems into a single easy pass

## Finding the Double Charge in a Shoebox

**Tags:** `core idea` (blue), `sort first` (green), `duplicates` (orange)

- **The shoebox** — a coffee shop owner has 1,000 paper receipts and suspects someone was charged twice
- **The slow way** — comparing every receipt against every other one means 499,500 pair checks
- **Sort first** — order the pile by amount, and any duplicate charge lands right next to its twin
- **One pass** — after sorting, a single flip through the pile (999 neighbor checks) catches every double
- **The bill** — the sort itself takes about 9,966 steps, so the total is ~10,965 vs 499,500 — 45× less

*Example (italic):* Instead of half a million comparisons, the owner sorts once and flips through once — about 11,000 steps for the same answer.

**Key point:** Many hard questions — duplicates, closest pair, "have I seen this before?" — become one cheap scan the moment the data is sorted.

### Visualization (canvas `c1`, 720×300)

Two-bar comparison chart: total steps to find duplicates in 1,000 receipts by checking every pair vs sorting first and scanning once, with the 45× gap called out.

- **Title (bold 15px, `#1a5276`, top center):** "Two Ways to Find a Double Charge in 1,000 Receipts".
- **Axes:** origin x=210, baseline y=250, plot width 450, plot height 185; horizontal bars; x axis = total steps 0 to 500,000 with 12px `#444` tick labels "0", "100k", "200k", "300k", "400k", "500k"; light `#e5e9ef` gridlines at each tick.
- **Bar 1 (y=95, 46px tall):** "check every pair" — length 499,500 steps, fill `rgba(42,120,214,0.35)`, 2px `#2a78d6` border; 12px `#444` row label at x=20; bold 13px blue `#2a78d6` value label "499,500 checks" just left of the bar's right end.
- **Bar 2 (y=185, 46px tall):** "sort, then scan once" — length 10,965 steps (a thin sliver), fill `rgba(0,131,0,0.35)`, 2px `#008300` border; 12px `#444` row label at x=20; bold 13px green `#008300` value label "10,965 steps" to the right of the bar.
- **Annotation (bold 13px green `#008300`, near x=380, y=170):** "sort first: ~45× fewer steps".
- **Caption (12px `#444`, bottom right):** "steps counted as comparisons; sort cost 1,000 × log₂1,000 ≈ 9,966 — illustrative".

## Eight Receipts, Sorted by Hand

**Tags:** `worked example` (blue), `by hand` (green)

- **Eight receipts** — take a small pile with dollar amounts 7, 3, 9, 3, 5, 8, 2, 7
- **All pairs** — checking every pair for a match takes 8×7/2 = 28 comparisons
- **Sort them** — ordered, the pile reads 2, 3, 3, 5, 7, 7, 8, 9
- **Scan once** — 7 neighbor checks, and the two 3s and the two 7s are sitting side by side
- **Redo it** — you can verify all 28 pair checks and all 7 neighbor checks yourself in a minute

*Example (italic):* The duplicate $3 receipts hide at positions 2 and 4 in the messy pile, but land touching at positions 2 and 3 once sorted.

**Key point:** Sorting doesn't find the duplicates itself — it moves every duplicate next to its twin, so one cheap pass of 7 checks can.

### Visualization (canvas `c2`, 720×300)

Two-row box diagram: the eight receipt amounts before and after sorting, with the duplicate pairs highlighted so the reader sees them scattered on top and adjacent on the bottom.

- **Title (bold 15px, `#1a5276`, top center):** "Sorting Pulls Every Duplicate Next to Its Twin".
- **Layout:** two rows of 8 boxes, each box 62px wide × 46px tall with 10px gaps, row starting at x=105; top row at y=80, bottom row at y=190; 12px `#444` row labels at x=20: "messy pile" and "sorted pile".
- **Top row values (left to right):** `[7, 3, 9, 3, 5, 8, 2, 7]`; bottom row values: `[2, 3, 3, 5, 7, 7, 8, 9]`; amounts drawn as bold 15px centered text with a "$" prefix.
- **Box style:** default fill `#ffffff`, 2px `#6b7280` border, 4px corner radius, text `#2c3e50`; duplicate boxes ($3s and $7s) get fill `rgba(0,131,0,0.15)`, 2px `#008300` border, text `#008300`.
- **Adjacency brackets:** below the bottom row, a 3px green `#008300` horizontal bracket under boxes 2–3 (the 3s) and another under boxes 5–6 (the 7s), each with a bold 12px green label "touching" beneath.
- **Scan arrow:** thin 2px `#6b7280` arrow above the bottom row from the first box to the last, 11px `#6b7280` label "one pass: 7 neighbor checks" above it.
- **Annotation (bold 13px orange `#d95926`, right side near y=145):** two lines: "28 pair checks before —" / "7 neighbor checks after".

## The n log n Barrier — and Why It's Good News

**Tags:** `where it's used` (blue), `n log n` (green), `rule of thumb` (orange)

- **The barrier** — no method that sorts by comparing items can beat roughly n log n steps; that's the floor
- **Good news** — the floor is low: for 1,000 items n log n is ~9,966 steps, while pair-checking needs 499,500
- **Everywhere** — databases sort to join tables, find medians and percentiles, group rows, and dedupe
- **Binary search** — a sorted list answers "is this amount here?" in ~10 checks instead of up to 1,000
- **The habit** — when a problem feels like it needs "compare everything", try "sort first" instead

*Example (italic):* A group-by in a database engine is often a sort in disguise — identical keys end up adjacent, then one pass totals each group.

**Key point:** n log n grows barely faster than n itself, so "sort first" turns many n-squared problems into nearly-linear ones.

### Visualization (canvas `c3`, 720×300)

Growth-curve chart: steps needed vs number of items for three costs — a straight scan, a sort, and all-pairs checking — showing the pair-checking curve rocketing off while the sort hugs the floor.

- **Title (bold 15px, `#1a5276`, top center):** "The Sort Hugs the Floor While Pair-Checking Explodes".
- **Axes:** origin x=75, baseline y=245, plot width 590, plot height 185; x = number of items 0 to 1,000 with 12px `#444` tick labels "0", "200", "400", "600", "800", "1,000"; y = steps 0 to 500,000 with 12px `#444` tick labels "0", "125k", "250k", "375k", "500k" and light `#e5e9ef` gridlines.
- **Shared x grid for all three curves:** `[100, 200, 300, 400, 500, 600, 700, 800, 900, 1000]`.
- **All-pairs n(n−1)/2 ≈ n²/2:** orange `#d95926` 3px line, values `[5000, 20000, 45000, 80000, 125000, 180000, 245000, 320000, 405000, 500000]`; bold 12px orange label "check every pair" near x=780, above the curve.
- **Sort n·log₂n:** green `#008300` 3px line, values `[664, 1529, 2469, 3458, 4483, 5541, 6621, 7715, 8827, 9966]`; 12px green label "sort: n log n" just above its right end.
- **One scan n:** mute `#6b7280` 2px dashed (dash 4/3) line, values `[100, 200, 300, 400, 500, 600, 700, 800, 900, 1000]`; 11px `#6b7280` label "one scan: n" below the green label, staggered to avoid overlap.
- **Marker:** 6px orange dot at (1,000, 500,000) and 6px green dot at (1,000, 9,966), each with a thin 1px `#e5e9ef` vertical guide to the baseline.
- **Annotation (bold 13px `#1a5276`, near x=340, y=95):** two lines: "at 1,000 items:" / "499,500 vs 9,966 steps".
- **Caption (12px `#444`, bottom right):** "step counts are comparison counts — illustrative".

## Sorting Isn't the Cost, It's the Investment

**Tags:** `common mistake` (red), `break-even` (orange)

- **The worry** — "sorting is an extra step, so it must slow things down" is the common first instinct
- **Pay once** — the ~9,966-step sort is paid one time; every later lookup then costs ~10 binary-search checks
- **Scan forever** — skipping the sort means every single lookup rescans all 1,000 receipts from the top
- **Break-even** — at 10 lookups it's 10,066 vs 10,000; from lookup 11 on, sort-first is cheaper
- **One-shot caveat** — for a single lookup you'll never repeat, one 1,000-check scan beats sorting first

*Example (italic):* After 50 lookups the unsorted pile has cost 50,000 checks; the sorted one has cost 10,466 in total, sort included.

**Common mistake:** Treating the sort as overhead. It's an investment — compare the total cost over all the lookups you'll ever do, not the cost of the first one.

### Visualization (canvas `c4`, 720×300)

Two-line cumulative-cost chart: total checks vs number of lookups on 1,000 receipts, never-sorted rising steeply and sort-first nearly flat, with the break-even point marked.

- **Title (bold 15px, `#1a5276`, top center):** "Total Cost of Lookups: the Sort Pays for Itself by Lookup 10".
- **Axes:** origin x=75, baseline y=245, plot width 590, plot height 185; x = number of lookups 0 to 50 with 12px `#444` tick labels "0", "10", "20", "30", "40", "50"; y = total checks 0 to 50,000 with 12px `#444` tick labels "0", "10k", "20k", "30k", "40k", "50k" and light `#e5e9ef` gridlines.
- **Shared x grid for both lines:** `[0, 10, 20, 30, 40, 50]`.
- **Never sorted (1,000 checks per lookup):** orange `#d95926` 3px line, values `[0, 10000, 20000, 30000, 40000, 50000]`; bold 12px orange label "never sorted" above the line near x=38.
- **Sort first (9,966 up front + 10 per lookup):** green `#008300` 3px line, values `[9966, 10066, 10166, 10266, 10366, 10466]`; bold 12px green label "sort first" just above its right end.
- **Break-even marker:** vertical dashed `#6b7280` (dash 4/3) line at x=10 from the baseline to y=75, 6px `#1a5276` dot where the two lines cross (10, ~10,000); bold 12px `#6b7280` label "break-even: 10 lookups" at the top of the dashed line.
- **Annotation (bold 13px green `#008300`, near x=32, y=190):** two lines: "after 50 lookups:" / "10,466 vs 50,000 checks".
- **Caption (12px `#444`, bottom right):** "1,000 receipts; lookup = binary search ≈ 10 checks — illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all bar lengths, box values, and curve points are the hardcoded arrays above (no randomness); step counts use n(n−1)/2 for pairs and n·log₂n rounded for sorting (log₂1,000 ≈ 9.966), and text numbers match chart numbers exactly.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
