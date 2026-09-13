# Segment Trees

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Segment Trees

**Subtitle:** A segment tree keeps pre-added sums of pairs, quads, and bigger blocks stacked above the raw numbers — so any range total, and any single-value fix, takes a handful of steps instead of a full walk

## Eight Hours of Coffee Sales, Stacked Into a Pyramid

**Tags:** `core idea` (blue), `pre-computed sums` (green), `block totals` (orange)

- **The shop** — a coffee shop logs cups sold each hour, 9am to 4pm: 5, 3, 8, 6, 2, 7, 4, 9
- **The question** — the manager keeps asking for slices of the day: "how many cups from 10am to 2pm?"
- **Naive way** — walk the slice and add every hour one by one; long slices mean long walks
- **The pyramid** — pre-add neighbors into pairs (8, 14, 9, 13), pairs into halves (22, 22), halves into the day (44)
- **The tree** — that pyramid is the segment tree: every node stores the sum of all the hours below it

*Example (italic):* The node sitting above 11am and 12pm permanently stores 8 + 6 = 14, so those two hours never need re-adding.

**Key point:** A segment tree is the raw numbers plus every block sum above them — here 15 stored numbers — so any slice can be assembled from a few ready-made blocks.

### Visualization (canvas `c1`, 720×300)

Node-and-link tree diagram: the 8 hourly counts as leaves, block sums stacked above them, root at the top holding the whole day.

- **Title (bold 15px, `#1a5276`, top center):** "The Sales Pyramid: 8 Hours, 15 Stored Sums".
- **Leaves (y=235):** 8 rounded rects (34×24, 1.5px `#2a78d6` border, fill `rgba(42,120,214,0.15)`) centered at x = `[90, 170, 250, 330, 410, 490, 570, 650]`, bold 13px `#1a5276` values `[5, 3, 8, 6, 2, 7, 4, 9]` inside; 11px `#6b7280` hour labels below at y=268: "9am", "10am", "11am", "12pm", "1pm", "2pm", "3pm", "4pm".
- **Pair level (y=170):** 4 rects same style but border `#199e70`, fill `rgba(25,158,112,0.12)`, at x = `[130, 290, 450, 610]`, values `[8, 14, 9, 13]`.
- **Half level (y=110):** 2 rects, border `#c98500`, fill `rgba(201,133,0,0.12)`, at x = `[210, 530]`, values `[22, 22]`.
- **Root (y=55):** 1 rect (40×26), border 2px `#1a5276`, fill `rgba(26,82,118,0.12)`, at x=370, bold value `44`.
- **Links:** 1.5px `#e5e9ef` lines from each parent's bottom edge to both children's top edges (drawn before nodes).
- **Annotation (bold 12px `#1a5276`, right of root near x=430, y=55):** "whole day = 44 cups".
- **Caption (12px `#444`, bottom right):** "illustrative — one day's hourly cup counts".

## The 10am-to-2pm Answer in Three Grabs

**Tags:** `worked example` (blue), `range query` (green)

- **The ask** — cups from 10am through 2pm; the honest hour-by-hour sum is 3 + 8 + 6 + 2 + 7 = 26
- **Blocks that fit** — the tree already stores 11am+12pm as 14 and 1pm+2pm as 9, both fully inside the range
- **Three grabs** — 10am's own 3, plus block 14, plus block 9: 3 + 14 + 9 = 26, same answer
- **Fewer touches** — three stored numbers replace five additions; wider ranges save far more
- **A fix, too** — if 1pm's count corrects from 2 to 6, only its path changes: leaf 2→6, then 9→13, 22→26, 44→48

*Example (italic):* For a 6-month slice of daily sales the tree grabs about 9 blocks; adding day by day means about 180 additions.

**Key point:** A range query keeps every stored block that fits entirely inside the range and only splits the edges — 3 + 14 + 9 = 26 for 10am–2pm.

### Visualization (canvas `c2`, 720×300)

The same tree as c1 with the 10am–2pm query highlighted: the three grabbed nodes in green, the covered leaves bracketed, everything untouched in gray.

- **Title (bold 15px, `#1a5276`, top center):** "10am–2pm: Three Stored Sums Do the Work of Five".
- **Layout:** identical node positions, sizes, and values as c1 (leaves y=235 at x = `[90, 170, 250, 330, 410, 490, 570, 650]` with values `[5, 3, 8, 6, 2, 7, 4, 9]`; pairs y=170 at `[130, 290, 450, 610]` = `[8, 14, 9, 13]`; halves y=110 at `[210, 530]` = `[22, 22]`; root y=55 at 370 = `44`), links 1.5px `#e5e9ef`.
- **Grabbed nodes (3):** the 10am leaf (value 3), the pair node 14, and the pair node 9 get border 2.5px `#008300`, fill `rgba(0,131,0,0.18)`, bold value text `#008300`.
- **Untouched nodes:** border 1.5px `#c9ced6`, fill `#f4f6f8`, value text `#6b7280` (including root and both halves — neither half fits inside the range).
- **Range bracket:** 2px `#008300` horizontal bracket under the hour labels from x=150 to x=510 (leaves 10am through 2pm), 12px `#008300` label centered below: "the asked range".
- **Annotation (bold 13px `#008300`, upper right near x=560, y=60):** two lines: "3 grabs: 3 + 14 + 9" / "= 26 cups".
- **Caption (12px `#444`, bottom right):** "illustrative — grabbed blocks in green".

## Why Log Time Changes the Game

**Tags:** `where it's used` (blue), `log time` (green), `live updates` (orange)

- **Doubling is cheap** — doubling the hours adds one level to the pyramid, so one extra step per query
- **The count** — 8 values need about 3 steps, 1,024 need about 10, a million need about 20
- **Both directions** — fixes ride one root-to-leaf path; queries grab a few nodes per level: log time both ways
- **Where it lives** — live dashboards, leaderboards, and time-series stores answering windowed totals
- **Beyond sums** — swap + for min, max, or count and the identical tree answers those range questions too

*Example (italic):* A metrics store holding 1,000,000 minute-buckets answers "total over any window" in about 20 steps, not a million.

**Key point:** Cost grows with the number of tree levels, not the number of values — log2 of n — so a million values still take only about 20 steps.

### Visualization (canvas `c3`, 720×300)

Horizontal paired-bar chart on a log-scale axis: steps needed by a plain scan vs a segment tree, for three data sizes.

- **Title (bold 15px, `#1a5276`, top center):** "Steps per Range Query: Scan Every Value vs Segment Tree".
- **Axis:** horizontal 2px `#999` line at y=245 from x=170 to x=680; log10 scale mapping steps 1 → x=170 and 1,000,000 → x=680 (85px per decade); 12px `#444` tick labels below at 1, 10, 100, "1k", "10k", "100k", "1M"; light `#e5e9ef` vertical gridlines at each tick.
- **Rows (three groups, top to bottom), each with a 12px `#444` label at x=20:** "8 values" (y=85), "1,024 values" (y=145), "1,000,000 values" (y=205).
- **Bars (12px tall, 4px gap within a pair):** per group, top bar = plain scan in orange `#d95926` with lengths for `[8, 1024, 1000000]` steps; bottom bar = segment tree in green `#008300` with lengths for `[3, 10, 20]` steps; bold 12px value label just right of each bar end ("8", "3", "1,024", "10", "1,000,000", "20"), matching bar color.
- **Legend (12px, top left under title):** orange swatch "plain scan", green swatch "segment tree".
- **Annotation (bold 13px `#008300`, near x=430, y=195):** "a million values: 20 steps, not 1,000,000".
- **Caption (12px `#444`, bottom right):** "x axis is log-scale; step counts approximate, illustrative".

## Not the Same as a Running Total

**Tags:** `common mistake` (red), `prefix sums` (orange)

- **The rival** — a running-total list (prefix sums) answers any range sum with one subtraction
- **Static win** — if the numbers never change, prefix sums beat the tree: 2 lookups and no pyramid to build
- **The catch** — correct one early hour and every running total after it becomes wrong
- **Update cost** — at 1,024 values a prefix fix can rewrite up to 1,024 entries; the tree patches about 10
- **The rule** — frozen data: use prefix sums; data that keeps changing: use the segment tree

*Example (italic):* A leaderboard whose scores change every second would rebuild its running totals constantly; the tree just patches one root-to-leaf path.

**Common mistake:** Reaching for a segment tree when the data never changes — prefix sums are simpler and faster there; the tree earns its keep only when updates and range queries interleave.

### Visualization (canvas `c4`, 720×300)

Grouped vertical bar chart on a log-scale y axis: worst-case steps for a range query and for a single update, prefix sums vs segment tree, at 1,024 values.

- **Title (bold 15px, `#1a5276`, top center):** "At 1,024 Values: Who Wins Which Job?".
- **Axes:** origin x=90, baseline y=245; y is log10 scale mapping 1 → y=245 and 1,024 → y=70; 12px `#444` tick labels at 1, 10, 100, "1k" with light `#e5e9ef` horizontal gridlines.
- **Groups (centered at x=260 and x=520), 13px `#2c3e50` labels below the baseline:** "range query" and "one update".
- **Bars (46px wide, 16px gap within a group):** prefix sums in magenta `#d55181` with heights for `[2, 1024]` steps; segment tree in green `#008300` with heights for `[10, 10]` steps; bold 13px value label above each bar top ("2", "10", "1,024", "10"), matching bar color.
- **Legend (12px, top right under title):** magenta swatch "prefix sums", green swatch "segment tree".
- **Annotation (bold 13px `#008300`, near x=520, y=55):** "updates are the tree's win: 10 vs 1,024".
- **Caption (12px `#444`, bottom right):** "worst-case step counts, illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all node values, bar lengths, and coordinates are the hardcoded literals above (no randomness); the tree in c1 and c2 uses the same array `[5, 3, 8, 6, 2, 7, 4, 9]` and its true pairwise sums; log-scale positions in c3/c4 are computed from the literal step counts with log10.
- **Shared tree drawing:** c1 and c2 use identical node coordinates; write one draw helper taking a per-node style map so the two canvases cannot drift apart.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
