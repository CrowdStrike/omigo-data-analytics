# When the Mean Lies

**Page type:** detail page (tutorial page: h1 + subtitle, then four card-sections each with a two-column table layout — text left 50%, canvas right 50%)
**HTML title tag:** When the Mean Lies

**Subtitle:** "The average customer spends $38" — but no customer spends $38. Averages can point at empty space.

## The $38 customer who does not exist

Tags: `core idea` (blue), `running example` (green)

- **The report** — "our average customer spends $38" sounds like a solid, usable fact
- **The mix** — 100 customers: 80 casual browsers near $12 and 20 loyal regulars near $142
- **The math** — 80×$12 + 20×$142 = $3,800; divide by 100 customers: exactly $38
- **The problem** — not one of the 100 customers spends anywhere near $38
- **The habit** — look at the histogram before trusting any average

*Example (italic):* A $38 free-shipping threshold is tuned for a customer who does not exist.

**Key point:** The mean is a fine summary of one hump. With two groups, it points at the empty valley between them.

### Visualization (canvas `c1`, 720×300)

Spend histogram with the mean line landing in the empty valley.

- **Data:** $10 buckets from $0 to $170, 100 customers (illustrative), counts `[30, 40, 10, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4, 5, 6, 4, 1]`; max count 40; zero bars skipped.
- **Title (bold 15px `#1a5276`, top center):** "What 100 customers actually spend (illustrative)".
- **Chart type:** vertical bar histogram; first 6 buckets filled `rgba(42,120,214,0.55)` (blue), remaining buckets `rgba(25,158,112,0.55)` (aqua); each nonzero bar's count in 12px `#2c3e50` above it. L-shaped `#999` axis, padding top 52 / bottom 48 / left 52 / right 20; x ticks "$0"–"$160" step $40, x label "spend per customer".
- **Group labels (bold 13px):** blue `#2a78d6` "80 browsers ~$12" over the left hump (x≈$28); aqua `#199e70` "20 regulars ~$142" over the right hump (x≈$140).
- **Mean line:** vertical dashed orange `#d95926` (dash 6/4, width 3) at x=$38, labeled to its right in bold 14px orange: "mean = $38: nobody is here".

## Check it yourself with ten receipts

Tags: `worked example` (green), `do it yourself` (blue)

- **Ten receipts** — $12, $12, $12, $12, $12, $12, $12, $12, $142, $142
- **Total** — 8×12 = 96, plus 2×142 = 284; grand total $380
- **Mean** — 380 ÷ 10 = $38, the number the report quotes
- **Median** — sort, take the middle: the 5th and 6th receipts are both $12
- **Compare** — the median says "typical customer: $12"; the mean says $38

*Example (italic):* Say it out loud — "half our customers spend $12 or less" — and the meeting changes.

**Two different questions:** the mean answers "total ÷ heads"; the median answers "what is typical". They agree only when the shape is one tight hump.

### Visualization (canvas `c2`, 720×300)

Dot plot on a dollar line: stacked receipt dots, median marker vs mean fulcrum.

- **Data:** eight $12 receipts and two $142 receipts; x scale $0–$160.
- **Title:** "Ten receipts: mean $38, median $12".
- **Axis:** horizontal dollar line at y=200, ticks "$0"–"$160" step $40, label "receipt amount" (mute 12px). Left/right padding 52/28.
- **Dots:** 6px-radius dots stacked vertically (15px apart) above the axis — 8 blue `#2a78d6` dots at $12, 2 aqua `#199e70` dots at $142. Labels bold 12px: blue "8 receipts of $12" above the $12 stack, aqua "2 receipts of $142" above the $142 stack.
- **Median marker:** green `#008300` tick at $12 with a 2px green line extending below the axis; bold 13px green label "median $12: the middle receipt".
- **Mean fulcrum:** solid orange `#d95926` triangle (10px half-width) under the axis at $38; bold 13px orange label "mean $38: where the line balances", followed by 12px `#2c3e50` text "(96 + 284) ÷ 10 = 38".

## Three different ways the mean breaks

Tags: `skew` (orange), `mixture` (blue), `outlier` (red)

- **Skew** — a long tail of large values drags the mean above what is typical
- **Mixture** — two groups park the mean in the valley where nobody sits (our $38)
- **Outlier** — one $1,500 corporate order lifts the mean from $38 to $52 by itself
- **The median** — barely moves in all three cases; it only looks at the middle rank
- **Different fixes** — trim or transform for skew, split the groups, investigate the outlier

*Example (italic):* Same misleading headline number, three different diseases — the histogram says which.

**Key point:** "the mean lies" is three separate failure modes. Diagnose which one you have before you fix anything.

### Visualization (canvas `c3`, 720×300)

Three side-by-side panels (230px wide each, 10px gap, bordered in grid gray `#e5e9ef`), each showing a shape with a solid green median line and a dashed orange mean line ("median" label bold 11px green left of its line, "mean" bold 11px orange right of its line).

- **Overall title (bold 15px `#1a5276`):** "Three shapes, one symptom: mean pulled away from median".
- **Panel 1 — "Skew: a long tail":** declining bars `[30, 22, 15, 10, 7, 5, 3, 2]`, fill `rgba(201,133,0,0.5)` (yellow); median line at 17% of panel width, mean line at 34%; caption (mute 11px): "tail drags the mean up".
- **Panel 2 — "Mixture: two groups":** the spend data in $20 buckets $0–$180, bars `[70, 10, 0, 0, 0, 0, 9, 10, 1]`; first 4 buckets `rgba(42,120,214,0.55)` (blue), rest `rgba(25,158,112,0.55)` (aqua); median line at 7% (≈$12 of $0–180), mean at 21% (≈$38); caption: "mean sits in the empty valley".
- **Panel 3 — "Outlier: one whale":** scatter of 10 blue `#2a78d6` dots (4.5px radius) clustered near the left at x-fractions `[0.06, 0.08, 0.10, 0.07, 0.11, 0.09, 0.12, 0.08, 0.10, 0.13]` (stacked in rows of 5), plus one 6px red `#e74c3c` dot at x-fraction 0.93 labeled bold 11px red "$1,500"; median line at 9%, mean at 30%; caption: "one point carries the mean".

## What to report instead

Tags: `best practice` (green), `rule of thumb` (blue)

- **Typical customer** — report the median: "$12" describes real people
- **Better still** — report the mix: "80% spend about $12, 20% spend about $142"
- **Keep the mean for totals** — revenue = mean × customers; that job it does honestly
- **Quick sanity check** — mean far from median means skew, mixture, or outliers
- **Fragility test** — recompute without the top value; a mean that jumps was being carried

*Example (italic):* One $1,500 order arrives Monday; every "average customer" chart moves — the median doesn't.

**Common confusion:** the mean is not wrong — it faithfully reports total ÷ heads. The lie happens when you read it as "a typical customer".

### Visualization (canvas `c4`, 720×300)

Grouped before/after bar chart: the effect of adding one $1,500 order on mean vs median.

- **Data:** mean before $38 / after $52 (orange `#d95926`); median before $12 / after $12 (green `#008300`). "Before" bars drawn at 0.35 alpha of the group color, "after" bars at 0.75 alpha. Bars 72px wide, two per group.
- **Title:** "Add one $1,500 order to the 100 customers".
- **Axes:** L-shaped `#999` axis, padding top 60 / bottom 56 / left 60 / right 30; y scale $0–$60 with labels "$0"/"$20"/"$40"/"$60" (mute 12px, right-aligned).
- **Labels:** each bar's dollar value in bold 13px `#2c3e50` above it; "before"/"after" in mute 12px under each bar; group names "mean" and "median" in bold 13px ink `#1a5276` below.
- **Annotation (magenta `#d55181`, bold 13px, centered above the plot):** "one order moved the mean $14; the median did not budge".

## Regeneration instructions

- **Template:** tutorials topic-page layout (see `tutorials/CLAUDE.md`). `<h1>` + `.subtitle`, then four `.card-section` blocks, each an `<h2>` with a bottom border and a `table.layout` row: `.text-col` (50%) with `.tags` pills, one-line `<ul>` bullets opening with `<b>` terms, an italic `.example` line, and a `.key-point` callout; `.viz-col` (50%) holds one 720×300 canvas. (This page has no 3-column sections.)
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6. h1 2rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem; h2 1.3rem `#1a5276` with 2px `#2980b9` bottom border. `.key-point` background `#f8f9fa`, left border 3px solid `#e74c3c`, 0.9rem; `.example` italic `#555` 0.9rem; `ul` 0.92rem, `li b` in `#1a5276`. Canvas CSS `width:100%`, 1px `#e0e0e0` border, 4px radius.
- **Tag pills:** `.tag` inline-block 0.72rem bold, padding 2px 10px, radius 10px; blue = bg `rgba(26,82,118,0.12)` / `#1a5276`, green = bg `rgba(39,174,96,0.15)` / `#27ae60`, red = bg `rgba(231,76,60,0.12)` / `#e74c3c`, orange = bg `rgba(230,126,34,0.15)` / `#e67e22`.
- **Chart palette (JS object `P`):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette anchors: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- **Canvases:** intrinsic 720×300 `width`/`height` attributes; shared `setup(id)` helper scales the backing store by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates). All data hardcoded (no `Math.random()`); histogram counts labeled "illustrative" where invented. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- No nav bar, no back/home links, no cross-page links. In regenerated HTML any card links would use `.html` extensions (this page has none).
