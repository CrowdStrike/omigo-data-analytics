# Box Plots

**Page type:** detail page (tutorial page: h1 + subtitle, then card-sections each with a two-column table layout — text left 50%, canvas right 50%; last section uses a 3-column 50/25/25 layout with two canvases)
**HTML title tag:** Box Plots

**Subtitle:** One small box summarizes a whole pile of numbers — so you can compare four cities' delivery times in a single glance.

## Four cities of delivery times in one picture

Tags: `core idea` (blue), `running example` (green)

- **One picture** — delivery times for four cities side by side, no tables of numbers
- **The middle line** — each box's bar is the median: half of deliveries were faster, half slower
- **The box** — spans the middle half of deliveries, from the 25th to the 75th percentile
- **Whiskers** — the thin lines stretch out to the typical fastest and slowest deliveries
- **Lone dots** — deliveries so unusual they get drawn separately, flagged for a closer look

*Example (italic):* Summit's median is 19 minutes and Georgetown's is 41 — read straight off the boxes.

**Key point:** A box plot compresses each group to five numbers plus flagged points — small enough to line up many groups and compare them at once.

### Visualization (canvas `c1`, 720×300)

Four vertical box plots (one per city) with annotated parts.

- **Shared data (five-number summaries in minutes, illustrative, fixed for this page; used by c1 and c3):**
  - Summit: lo 12, Q1 16, median 19, Q3 22, hi 26, outliers none — color green `#008300`
  - Oakville: lo 18, Q1 24, median 28, Q3 33, hi 36, outliers [58] — color blue `#2a78d6`
  - Riverton: lo 11, Q1 23, median 34, Q3 52, hi 64, outliers none — color orange `#d95926`
  - Georgetown: lo 30, Q1 36, median 41, Q3 46, hi 54, outliers [75, 82] — color violet `#4a3aa7`
- **Title (bold 15px `#1a5276`, top center):** "Delivery times in four cities — one box per city".
- **Axes:** y axis 0–90 minutes, gridlines (`#e5e9ef`) and mute labels at 0/30/60/90; rotated y label "delivery time (minutes)". Padding top 46 / bottom 42 / left 56 / right 24. Four equal horizontal slots, boxes 56px wide.
- **Box drawing (per city, in the city's color):** whisker stems from lo→Q1 and Q3→hi with 28px-wide caps; box from Q1 to Q3, stroke 2px in city color, fill same hue at 0.18 alpha; median as a 3px horizontal line, its value printed to the right of the box in bold 12px; outliers as 4px-radius solid red `#e74c3c` dots; city name in bold 13px city color below the axis.
- **Annotations (near the Oakville box):** bold 12px `#2c3e50` "median" beside the median line; red `#e74c3c` "flagged outlier" beside the 58 dot; mute `#6b7280` "box = middle half" beside the box.

## Building Oakville's box from 11 deliveries

Tags: `worked example` (green), `do it yourself` (blue)

- **Sort** — 11 delivery times in minutes: 18, 22, 24, 25, 26, 28, 30, 31, 33, 36, 58
- **Median** — the middle (6th) value is 28: that is the bar inside the box
- **Box edges** — 3rd value 24 and 9th value 33 bracket the middle half of the list
- **Fence** — box width 33 − 24 = 9; anything past 33 + 1.5×9 = 46.5 is suspicious
- **Whiskers** — reach the last values inside the fence: 18 on the left, 36 on the right
- **Flag** — 58 sits past 46.5, so it is drawn as a lone dot: an outlier worth investigating

*Example (italic):* That 58-minute delivery turned out to be a wrong address — the flag did its job.

**The recipe:** sort, take the middle value and the two quarter-points, fence at 1.5 box-widths, flag whatever falls outside.

### Visualization (canvas `c2`, 720×300)

Horizontal box plot built above a strip of the 11 raw data dots.

- **Data:** values `[18, 22, 24, 25, 26, 28, 30, 31, 33, 36, 58]`; Q1 24, median 28, Q3 33, fence 46.5, whiskers 18 and 36.
- **Title:** "Oakville: 11 deliveries become one box".
- **Axis:** horizontal minute axis at y=250, scale 10–65, ticks $-less labels at 10–60 step 10, label "minutes". Left/right padding 50/30.
- **Raw dots (row at y=220):** 5px-radius dots at each value, blue `#2a78d6` except values past the fence (58) in red `#e74c3c`; mute 12px label "the 11 raw times" near the right.
- **Box (rows y=92–152):** blue stroke 2px, fill `rgba(42,120,214,0.15)` from 24 to 33; median as a 3px vertical line at 28; whiskers as 2px horizontal lines from 18→24 and 33→36 with vertical end caps; the outlier 58 as a solid red dot on the box's mid-line.
- **Fence line:** vertical dashed red `#e74c3c` (dash 5/4, width 2) at x=46.5 from y=62 down to the axis.
- **Labels:** bold 12px blue "Q1 = 24", "median = 28", "Q3 = 33" above the box; mute 12px "whisker: 18" and "whisker: 36" beside the whisker caps; bold 13px red "fence: 33 + 1.5×9 = 46.5" and "58 is past the fence → flagged" to the right of the fence line.

## Reading decisions straight off the boxes

Tags: `where it's used` (blue), `spread = risk` (orange)

- **Fast and steady** — Summit: low median (19 min) and a short box — predictable service
- **Slow but steady** — Georgetown: high median (41), short box, two extreme flagged dots
- **The gamble** — Riverton: the tallest box; a delivery may take 15 minutes or an hour
- **Promise check** — against a 45-minute promise, Riverton fails often; Summit never did
- **Close medians, far risk** — Oakville (28) and Riverton (34) look close; their spreads don't

*Example (italic):* Marketing wants one citywide delivery promise; the boxes show why that cannot work.

**Key point:** The median line picks the winner; the height of the box tells you the risk. Averages alone show neither.

### Visualization (canvas `c3`, 720×300)

Same four city box plots as c1 (same data and drawing routine) with a promise threshold added.

- **Title:** "Which city can promise \"45 minutes or free\"?".
- **Promise line:** horizontal dashed red `#e74c3c` (dash 6/4, width 2) across the plot at y=45 minutes, labeled bold 13px red "45-min promise" at the left.
- **Annotations (bold 12px, centered over the city slots):** orange `#d95926` "breaks it often" above Riverton; green `#008300` "never breaks it" above Summit.

## What the box hides: two humps look like one box

Tags: `common mistake` (red), `hidden shape` (orange)

- **Riverton's secret** — downtown drops take ~20 min; bridge-crossing runs take ~50
- **The histogram** — 74 deliveries form two clear humps with a valley between
- **The box** — the same 74 numbers make one ordinary-looking box, humps invisible
- **The lie in the middle** — median 34 lands in the valley: only 5 of 74 took 30–40 min
- **The tell** — an unusually tall box is a hint to go draw the histogram

*Example (italic):* "Typical Riverton delivery: 34 minutes" describes almost no actual delivery.

**Common confusion:** a box plot summarizes a shape it never shows. Two humps, one hump, or a flat smear can all produce the same box.

This section uses the 3-column layout: text 50%, two canvases at 25% each, so the text/viz split stays 50/50.

### Visualization (canvas `c4a`, 360×340)

Histogram of Riverton's 74 deliveries showing two humps.

- **Data:** 5-minute buckets from 10 to 65 minutes, counts `[2, 8, 14, 10, 4, 1, 3, 9, 13, 8, 2]` (74 deliveries), max count 14.
- **Title:** "Riverton histogram: two humps".
- **Chart type:** vertical bar histogram, fill `rgba(217,89,38,0.5)` (orange), each bar's count in 11px `#2c3e50` above it. L-shaped `#999` axis, padding top 50 / bottom 48 / left 46 / right 16; x ticks 10–60 step 10, label "minutes".
- **Median marker:** vertical dashed ink `#1a5276` line (dash 5/4, width 2) at x=34, labeled in bold 12px ink over two lines: "median 34 —" / "in the valley".
- **Annotations (bold 12px):** green `#008300` "downtown ~20" over the left hump (x≈21); violet `#4a3aa7` "over the bridge ~50" over the right hump (x≈50).

### Visualization (canvas `c4b`, 360×340)

The same 74 deliveries drawn as one horizontal box plot.

- **Data:** lo 11, Q1 23, median 34, Q3 52, hi 64 (same x scale 10–65).
- **Title:** "Same 74 deliveries: one box".
- **Axis:** horizontal minute axis at the bottom, ticks 10–60 step 10, label "minutes"; same padding as c4a.
- **Box (rows y=120–190):** orange `#d95926` stroke 2px, fill `rgba(217,89,38,0.15)` from 23 to 52; median as a 3px vertical line at 34, labeled bold 12px orange "median 34" above; 2px whiskers 11→23 and 52→64 with vertical caps.
- **Annotation:** magenta `#d55181` bold 13px, two centered lines below the box: "the two humps are invisible —" / "only the tall box hints at trouble".

## Regeneration instructions

- **Template:** tutorials topic-page layout (see `tutorials/CLAUDE.md`). `<h1>` + `.subtitle`, then four `.card-section` blocks, each an `<h2>` with a bottom border and a `table.layout` row: `.text-col` (50%) with `.tags` pills, one-line `<ul>` bullets opening with `<b>` terms, an italic `.example` line, and a `.key-point` callout; `.viz-col` (50%) holds one canvas. Section 4 uses the 3-column variant: `.text-col3` (50%) plus two `.viz-col3` (25%) cells each holding a 360×340 canvas.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6. h1 2rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem; h2 1.3rem `#1a5276` with 2px `#2980b9` bottom border. `.key-point` background `#f8f9fa`, left border 3px solid `#e74c3c`, 0.9rem; `.example` italic `#555` 0.9rem; `ul` 0.92rem, `li b` in `#1a5276`. Canvas CSS `width:100%`, 1px `#e0e0e0` border, 4px radius.
- **Tag pills:** `.tag` inline-block 0.72rem bold, padding 2px 10px, radius 10px; blue = bg `rgba(26,82,118,0.12)` / `#1a5276`, green = bg `rgba(39,174,96,0.15)` / `#27ae60`, red = bg `rgba(231,76,60,0.12)` / `#e74c3c`, orange = bg `rgba(230,126,34,0.15)` / `#e67e22`.
- **Chart palette (JS object `P`):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette anchors: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- **Canvases:** intrinsic `width`/`height` attributes as specified per chart; shared `setup(id)` helper scales the backing store by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates). c1 and c3 share a `drawCityBoxes` helper over the fixed `CITIES` five-number summaries; box fills are the stroke color at 0.18 alpha. All data hardcoded (no `Math.random()`). Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- No nav bar, no back/home links, no cross-page links. In regenerated HTML any card links would use `.html` extensions (this page has none).
