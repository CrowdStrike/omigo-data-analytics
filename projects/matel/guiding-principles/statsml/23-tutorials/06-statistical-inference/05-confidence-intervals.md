# Confidence Intervals

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table row, text left 50% / canvas right 50%)
**HTML title tag:** Confidence Intervals

**Subtitle:** Instead of one guess, give a range that would capture the true answer most of the time

## A Net Instead of a Pin

Tags: `core idea` (blue), `running example` (green)

- **The shop** — a coffee shop wants to know its true average spend per customer
- **The pin** — one day's 100 receipts average $6.40; quoting "$6.40" alone is a pin drop
- **The problem** — tomorrow's 100 receipts will average $6.20 or $6.55 — the pin moves
- **The net** — quote a range instead: "$6.01 to $6.79" — built to catch the true value
- **The promise** — nets built this way catch the truth about 95 times in 100

*Example (italic):* "Average spend is $6.40, give or take $0.39" says both the guess and how far it might be off.

**Key point:** a confidence interval trades false precision for honest coverage — a range with a track record, not a single number.

### Visualization (canvas `c1`, 720×300)

Number-line diagram: a point estimate "pin" vs an interval "net" on a dollar scale.

- **Title (bold 15px, `#1a5276`, top center):** "One Guess (pin) vs a Range With a Track Record (net)".
- **Number line:** at y=235 from x=80 to x=650 mapping $5.60–$7.20; ticks and labels at $5.80, $6.00, $6.20, $6.40, $6.60, $6.80, $7.00, gray `#999` axis.
- **The pin:** magenta `#d55181` vertical line (width 2.5) from y=110 down to the axis at $6.40, topped with a radius-7 magenta dot; bold label above: 'the pin: "$6.40" — sounds exact, moves every day'.
- **The net:** thick green `#008300` rounded segment (width 6) at y=175 from $6.01 to $6.79; bold green label below: "the net: $6.01 – $6.79 — built to catch the truth 95% of the time"; endpoint value labels "$6.01" and "$6.79" above the segment ends.
- **Caption (muted `#6b7280`, 12px, bottom center):** "average spend per customer".

## Building the Net From 100 Receipts

Tags: `worked example` (green), `by hand` (blue)

- **Step 1** — sample: n = 100 receipts, mean $6.40, standard deviation $2.00
- **Step 2** — standard error: SE = 2.00 ÷ √100 = $0.20
- **Step 3** — for 95%, take about 2 SEs: 1.96 × 0.20 = $0.39
- **Step 4** — interval: 6.40 − 0.39 to 6.40 + 0.39 → $6.01 to $6.79
- **Tighter net** — want ±$0.20 instead? You need 4x the receipts (n = 400)

*Example (italic):* Mean, one division, one multiplication — the whole 95% interval fits on a napkin.

**Key point:** interval = estimate ± 2 × SE (roughly). The width is driven by spread and √n — nothing mysterious.

### Visualization (canvas `c2`, 720×300)

Bell curve of possible sample means with the central 95% region shaded.

- **Title (bold 15px, `#1a5276`, top center):** "Sample Mean $6.40, SE $0.20 → 95% Interval $6.01 – $6.79".
- **Curve:** Gaussian bell, mean 6.40, sd 0.20, peak height 0.9 of chart height, stroked `#2a78d6` width 3, over dollar scale $5.70–$7.10. Padding: top 52, bottom 56, left 70, right 40.
- **Shaded region:** area under the curve from $6.01 to $6.79 filled `rgba(0,131,0,0.15)`.
- **Center line:** dashed `#1a5276` vertical (width 2, dash 5/4) at $6.40; bold label at top: "mean $6.40".
- **X ticks:** $5.80, $6.01, $6.20, $6.40, $6.60, $6.79, $7.00.
- **Annotations:** bold green mid-chart: "±1.96 × SE = ±$0.39 covers 95%"; bold orange `#d95926` labels in both tails: "2.5% miss" (right, near $6.86) and "2.5% miss" (left, near $5.94).
- **Caption (muted, 12px, bottom center):** "where the sample mean could land (SE = $0.20)".

## What "95%" Actually Promises

Tags: `core idea` (blue), `subtle point` (orange)

- **Rerun it** — imagine 20 different days, each with its own 100 receipts and its own interval
- **Each differs** — every day gets a different mean, so a different net position
- **The score** — about 19 of the 20 nets will contain the true average; ~1 will miss
- **The 95%** — describes the netting procedure's hit rate, not any single net
- **Unlucky days** — day 13 below misses with a perfectly executed method; that's the 5%

*Example (italic):* In the chart, 19 of 20 simulated day-intervals cross the true $6.25 line — day 13 does not.

**Key point:** the confidence lives in the method: build nets this way forever and ~95% of them will hold the truth.

### Visualization (canvas `c3`, 720×300)

Forest-style plot: 20 stacked horizontal 95% intervals against a vertical true-value line; one interval misses.

- **Title (bold 15px, `#1a5276`, top center):** "20 Days, 20 Intervals: 19 Catch the True $6.25, 1 Misses".
- **Scale:** dollars $5.50–$7.30 mapped to x; padding top 52, bottom 50, left 90, right 40. TRUE value = $6.25, interval half-width = $0.39.
- **True line:** dashed `#1a5276` vertical (width 2, dash 6/4) at $6.25, bold label above: "true average $6.25".
- **Interval centers (day 1–20):** `[6.31, 6.18, 6.44, 6.09, 6.27, 6.52, 6.13, 6.35, 6.02, 6.40, 6.21, 6.48, 6.72, 6.16, 6.30, 5.98, 6.38, 6.24, 6.55, 6.11]`; each drawn as a horizontal bar center ± 0.39 with a center dot (radius 3.5). Hits: stroke `rgba(42,120,214,0.75)` width 3, dot `#2a78d6`. Miss (day 13, center 6.72, interval 6.33–7.11 excludes 6.25): stroke `#e74c3c` width 4, red dot, bold red label to its right: "day 13: an honest miss — the 5%".
- **Row labels (right-aligned, left of plot):** "day 1" at top, "day 20" at bottom.
- **Caption (muted, 12px, bottom center):** "each bar = that day's 95% interval, mean ± $0.39 (illustrative)".

## Reading Intervals Wrong

Tags: `common mistake` (red), `where it's used` (orange)

- **Bare points** — dashboards quoting "$6.40" vs "$6.70" invite fights over pure noise
- **Overlap check** — old menu $6.01–$6.79 vs new menu $6.31–$7.09: heavy overlap
- **No verdict yet** — the $0.30 gap is well inside the wobble; don't crown a winner
- **Wrong reading** — "95% of customers spend $6.01–$6.79" — no, it bounds the mean only
- **Also wrong** — treating the interval as a probability statement about one fixed truth

*Example (italic):* A manager celebrating "$6.70 beats $6.40" is celebrating a difference the nets cannot separate.

**Common mistake:** comparing two point estimates without their intervals — if the nets overlap this much, the honest answer is "collect more data."

### Visualization (canvas `c4`, 720×300)

Two overlapping intervals on a shared dollar number line with the overlap zone highlighted.

- **Title (bold 15px, `#1a5276`, top center):** "Old Menu vs New Menu: the Nets Overlap Too Much to Call It".
- **Scale:** dollars $5.70–$7.40 mapped to x between padding left 90 / right 40; number line at y=245 with ticks at $5.80–$7.20 in $0.20 steps.
- **Overlap band:** rectangle from $6.31 to $6.79, y 60–220, filled `rgba(201,133,0,0.15)`.
- **Nets** (thick rounded segments, width 6, center dot radius 7, endpoint/mid dollar labels above, name label right-aligned left of the plot):
  - "old menu" at y=105: $6.01 – $6.40 – $6.79 in `#2a78d6`.
  - "new menu" at y=185: $6.31 – $6.70 – $7.09 in `#199e70`.
- **Annotation (bold yellow `#c98500`, top center of overlap):** "overlap zone: both nets allow the same truth".
- **Caption (muted, 12px, bottom center):** "average spend per customer (illustrative)".

## Regeneration instructions

- **Layout:** tutorial detail page. h1 (2rem, `#1a5276`, 2px `#2980b9` bottom border) + `.subtitle` (`#666`, 0.95rem), then four `.card-section` blocks. Each section: `<h2>` (1.3rem, `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one `<tr>`: left `<td class="text-col">` (50%) holds `.tags` pill row, `<ul>` bullets (each starting with `<b>` term in `#1a5276`), one italic `.example` paragraph, one `.key-point` callout; right `<td class="viz-col">` (50%) holds one canvas.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6. Bullets 0.92rem; `.example` italic `#555` 0.9rem; `.key-point` background `#f8f9fa`, left border 3px solid `#1a5276`, padding 8px 12px, 0.9rem.
- **Tag pills:** `.tag` inline-block 0.72rem bold, padding 2px 10px, radius 10px. Colors — blue: bg `rgba(26,82,118,0.12)` / `#1a5276`; green: bg `rgba(39,174,96,0.15)` / `#27ae60`; red: bg `rgba(231,76,60,0.12)` / `#e74c3c`; orange: bg `rgba(230,126,34,0.15)` / `#e67e22`.
- **Canvas:** 720×300 intrinsic attributes, CSS `width:100%`, 1px `#e0e0e0` border, radius 4px; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object `P`:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- No nav bar, no back/home links. In regenerated HTML any card links use `.html` extensions (this page has none).
