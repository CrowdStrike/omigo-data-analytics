# Snapchat Live World Map — When the Heatmap Is the News

**Page type:** detail page (two-column obj-table layout: text left 50%, canvas right 50%, one h2 + table per aspect, plus philosophy callouts and a summary table)
**HTML title tag:** Snapchat Live World Map — When the Heatmap Is the News

**Subtitle:** Millions of expiring, geotagged clips streaming onto one zoomable map. The engineering is hard, but the statistics are harder: every design choice — grid size, sample rate, window length, colour scale — silently changes what the map appears to say.

## Callout (philosophy box, top)

**The question:** A map that glows where activity concentrates is an anomaly detector with no detector in it — a human eye reads the bright patch. So what is the map actually measuring, and when is the bright patch a real event rather than an artifact of how the map was built?

**The answer:** An absolute-count heatmap measures *population*, not news. It lights up where people are, every night, forever. To surface events you must divide by a local baseline — and once you do, the map becomes a ratio estimator with a moving denominator, an arbitrary spatial grid, and a sample size that shrinks exactly when the story breaks.

## 1. The Load Is Anti-Correlated With Your Ability To Serve It

**Obj-title:** Sized for the Median, Fails at the Only Interesting Moment

Every other system gets to treat its spikes as a nuisance to be shed. Here the spike *is* the product. The minute the map most needs to be right is the minute the ingest path, the clustering pass, and the moderation queue all simultaneously fall behind — because all three are sized against a volume that no longer applies.

Math-box:

**Illustrative Example — one cell, 61 minutes** (posts/min, thousands)

Median volume: `20k/min` — Peak volume: `140k/min` — Ratio: `7.0x`
Render + ingest capacity provisioned at `40k/min` (2x the median)

Minutes above capacity: `16` (minute 32 → 47)
Posts arriving in those minutes: `1,515k` — servable: `16 × 40k = 640k` = `42.2%`
Shed or delayed: `1,515k − 640k = 875k`

Provision at the peak instead and the median hour runs at `20 / 140 = 14.3%` utilisation — you pay 7x for 45 minutes of correctness per hour.

- **The inversion:** capacity is cheapest to justify at the median and most needed at the tail — the two never coincide
- **Newsworthiness correlates with overload:** an unremarkable evening never stresses the pipeline, so load is a proxy for interest
- **Shedding is not neutral:** dropping the overflow biases the map toward whatever arrived first, not toward a fair sample
- **Latency is part of correctness:** a map that is right about 11:04 but renders at 11:19 has answered a question nobody asked
- **Autoscaling has a lag too:** new capacity arrives minutes after the spike, i.e. roughly when the spike is subsiding
- **The honest option:** degrade gracefully to a *labelled* sample rather than silently serve a truncated count

### Visualization (canvas `canvas1`, 720×360)

Line chart: one hour of per-minute volume against a flat capacity line, overload region shaded, all statistics computed from the plotted array.

- **Layout:** origin at (72, 296), plot width 596, plot height 236. Axes `#1a5276`, width 2.
- **Data (hardcoded literal array `vol`, 61 values, thousands of posts/min — the shape carries the lesson, no PRNG):**
  `18,20,19,20,20,17,20,20,19,20,18,19,20,19,20,20,20,18,20,20,19,21,23,20,18,22,20,21,19,20,24,` then the ramp `30,44,62,85,106,124,135,140,` then the decay `138,130,118,104,90,76,64,54,45,38,33,29,26,24,22,21,20,19,21,20,19,20`
- **Scales:** x = index 0..60 across plot width; y value scale 0–150.
- **Gridlines / y labels:** at 0, 30, 60, 90, 120, 150 in gray `#666` 11px, gridlines `#eee`.
- **Axis labels:** x: "Minute of the hour"; y (rotated): "Posts per minute (thousands)" — `#1a5276`, 13px.
- **Volume curve:** red `#e74c3c`, width 2.5, filled below with `rgba(26,82,118,0.35)` only where `vol ≤ 40`.
- **Overload shading:** region between the capacity line and the curve where `vol > 40` filled `rgba(231,76,60,0.18)`.
- **Capacity line:** green `#27ae60`, width 2.5, dashed (6/4), horizontal at 40; bold green 12px label "render + ingest capacity: 40k/min" at the left end, above the line.
- **Computed labels (bold 12px, drawn at render time from `vol`):** red, two lines near minute 30 above the peak — `"peak " + max + "k/min = " + (max/median).toFixed(1) + "x median " + median + "k/min"` and `"only " + pct + "% of arrivals servable in the " + n + " overloaded minutes"`, where `median` is the middle of the sorted array, `n` counts minutes with `vol > 40`, and `pct = (n*40 / sum of vol over those minutes * 100).toFixed(1)`.
- **Baseline note (12px `#999`, near minute 4 at value 58):** "45 quiet minutes fund the 16 that matter".
- **Title (bold 14px `#1a5276`, top center):** "The Spike Is the Product — and the Pipeline's Worst Case".

## 2. The Grid Is a Choice, and the Choice Changes the Pattern

**Obj-title:** Modifiable Areal Unit Problem, Rendered at 60fps

A million points cannot be drawn, so nearby posts are merged into a cluster and the cluster gets a count. But the merge needs boundaries, and boundaries are arbitrary: shift the grid half a cell, or zoom out one level, and the *same* posts produce a different hottest cluster in a different place. This is the modifiable areal unit problem — the apparent spatial pattern is partly a property of the bins, not the data.

Math-box:

**Illustrative Example — one street, eight fine cells, 120 posts**

Fine counts: `4, 30, 28, 5, 20, 22, 6, 5` → total `120`, hottest fine cell `30`

Group in aligned pairs: `4+30=34`, `28+5=33`, `20+22=42`, `6+5=11` → hottest cluster `42`
Shift the grid by one cell: `4`, `30+28=58`, `5+20=25`, `22+6=28`, `5` → hottest cluster `58`

Both partitions total `120`. Yet `58 / 42 = 1.38` — a `38.1%` swing in the headline number, and the hotspot *moves*: the aligned grid says the peak is on the right half of the street, the shifted grid says it is on the left.

- **Same data, two answers:** the count you display is a function of the offset, and the offset is an implementation detail
- **Zoom is re-binning:** each zoom level is a different areal unit, so a cluster can appear, split, or vanish on zoom
- **Cluster counts are not additive across levels:** children of a parent cluster need not partition it if the grids differ
- **Edge splitting hides events:** an event straddling a boundary is halved into two unremarkable clusters
- **Ranking is fragile:** "top 10 hottest places" reorders under a grid shift even with identical underlying posts
- **Mitigations, not fixes:** fixed hierarchical cells, density-based clustering, or reporting counts with the cell size shown

### Visualization (canvas `canvas2`, 720×360)

Three stacked bar rows over the same eight fine cells: raw cells, aligned grouping, shifted grouping — all totals and maxima computed at render.

- **Layout:** plot area x from 72 to 668. Three rows with baselines at y = 130 (fine), y = 232 (aligned), y = 320 (shifted); bars drawn upward from each baseline, max bar height 78 for row 1 and 62 for rows 2–3, scaled by `value / 58` so all three rows share one scale.
- **Data (hardcoded literals):** fine = `[4,30,28,5,20,22,6,5]`; aligned = `[34,33,42,11]` spanning 2 fine cells each; shifted = `[4,58,25,28,5]` spanning `[1,2,2,2,1]` fine cells.
- **Row 1 bars:** fill `rgba(26,82,118,0.35)`, border `#1a5276` width 1; value printed above each bar in `#1a5276` 11px.
- **Row 2 bars:** fill `rgba(39,174,96,0.30)`, border `#27ae60` width 1.5, drawn across the full width of the fine cells they cover; value in bold green 12px above the bar; the maximum bar's border thickened to 2.5.
- **Row 3 bars:** fill `rgba(230,126,34,0.30)`, border `#e67e22` width 1.5, same treatment; the maximum bar's border thickened to 2.5.
- **Row labels (bold 12px, left-aligned at x = 8, on the baseline):** `#1a5276` "fine cells"; `#27ae60` "aligned grid"; `#e67e22` "grid shifted one cell".
- **Cell boundary ticks:** light gray `#eee` vertical lines at every fine-cell edge, from y = 40 to y = 328.
- **Computed caption (bold 12px, drawn at render, centered at y = 346):** `"both partitions total " + sum + " posts — hottest cluster " + maxAligned + " vs " + maxShifted + " (+" + ((maxShifted/maxAligned-1)*100).toFixed(1) + "%)"` in `#e74c3c`, asserting the two sums are equal by computing both.
- **Title (bold 14px `#1a5276`, top center):** "Illustrative Example — Move the Grid, Move the Hotspot".

## 3. A Sample of a Spike Is Honest; a Sample Labelled a Count Is Not

**Obj-title:** Coverage Falls as 1/Volume

Under a fixed latency budget the map processes a roughly fixed number of items per minute. That is a sample, and its *fraction* is inversely proportional to volume — so precision degrades exactly where the interesting cell is. The moderation queue has the identical structure: a fixed review rate means review coverage also falls as 1/volume, so the review backlog is created by the same event that makes review urgent.

Math-box:

**Illustrative Example — fixed budgets, variable volume**

Render budget: `10k posts/min` sampled. At the median `20k/min` that is `10/20 = 50%`; at the peak `140k/min` it is `10/140 = 7.1%`.

Scale-up error: a cell showing `40` sampled posts at `f = 1/14` implies `40 × 14 = 560` posts. The count is Poisson-ish, so the half-width is `1.96 × √40 / f = 1.96 × 6.32 × 14 ≈ 174` — that is `±31%` on a number the UI prints as "560".

Review queue: `2%` of posts need review, capacity `500 items/min`. At the median, `0.02 × 20,000 = 400/min` — clears. At the peak, `0.02 × 140,000 = 2,800/min`, so coverage is `500 / 2,800 = 17.9%`. Across the 16 overloaded minutes: `0.02 × 1,515,000 = 30,300` flagged, `16 × 500 = 8,000` reviewed = `26.4%`; backlog `22,300`.

- **The label is the defect:** sampling is fine, printing the scaled-up number without its interval is not
- **Precision is worst where it matters:** the spike cell has the smallest sample fraction on the whole map
- **Poisson noise scales as √n:** small sampled counts imply wide intervals after multiplying by a large weight
- **Fixed rate, falling coverage:** review capacity is constant, so coverage traces `1/volume` with no floor
- **Backlog is an event, not a trend:** it appears in minutes and drains over hours, long after the map has moved on
- **Sampling under a moving frame:** the weight `1/f` itself must be estimated from the same overloaded minute

### Visualization (canvas `canvas3`, 720×360)

Two `1/volume` coverage curves against volume, with the median and peak operating points marked; every printed percentage computed from the plotted functions.

- **Layout:** origin at (76, 296), plot width 588, plot height 236. Axes `#1a5276`, width 2.
- **Data (functions of `v` = thousands of posts/min, 20 → 140):** render coverage `r(v) = min(100, 100 × 10 / v)`; review coverage `q(v) = min(100, 100 × 500 / (0.02 × 1000 × v))` = `min(100, 2500 / v)`.
- **Scales:** x = 20..140 across plot width; y = 0..100 percent.
- **Gridlines / labels:** y at 0, 25, 50, 75, 100 with a "%" suffix, `#666` 11px, gridlines `#eee`; x ticks at 20, 50, 80, 110, 140.
- **Axis labels:** x: "Volume in the cell (thousand posts/min)"; y (rotated): "Coverage of arriving items (%)" — `#1a5276`, 13px.
- **Render coverage curve:** blue `#1a5276`, width 2.5. **Review coverage curve:** orange `#e67e22`, width 2.5, dashed (7/5).
- **Operating points:** filled dots radius 5 at `(20, r(20))`, `(140, r(140))` in `#1a5276` and at `(20, q(20))`, `(140, q(140))` in `#e67e22`; each annotated at render with `v + "k → " + value.toFixed(1) + "%"` in bold 11px of the matching colour.
- **Curve labels (bold 12px):** blue "sampled for render" near v = 46 above its curve; orange "reviewed by the queue" near v = 60 below its curve.
- **Danger band:** region x from 100 to 140 filled `rgba(231,76,60,0.10)`, with red 11px label "the newsworthy minutes" at its top.
- **Note (12px `#999`, at v = 70, y value 12):** "both curves are 1/volume — a fixed budget cannot hold coverage flat".
- **Title (bold 14px `#1a5276`, top center):** "Fixed Budget, Variable Volume — Coverage Traces 1/Volume".

## 4. Ephemerality Means the Denominator Is Moving

**Obj-title:** Every Statistic Is a Windowed Estimate

Content that expires turns the corpus into a sliding window. Nothing on the map is a count over a fixed population — it is a count over whatever has not yet expired, and that set changes between two page loads. Two people comparing screenshots are not disagreeing about the data; they are holding two different denominators.

Math-box:

**Illustrative Example — a 20-minute window, three minutes apart**

Alice loads at minute 42; her window covers minutes `22–41`: `1,299k` posts
Bob loads at minute 45; his window covers minutes `25–44`: `1,508k` posts
Shared minutes `25–41`: `1,238k` — that is `95.3%` of Alice's window and `82.1%` of Bob's

Bob's cell reads `1,508 / 1,299 = 1.161`, i.e. `16.1%` hotter, from a `3-minute` clock difference and *zero* change in anyone's behaviour. Neither number is wrong; they answer different questions.

- **No stable population:** the denominator is defined by expiry time, not by any question the analyst asked
- **Screenshots do not reproduce:** the window that produced a figure is gone before anyone can re-derive it
- **Window length is a smoother:** a long window damps real spikes, a short one amplifies arrival noise
- **Trends confound with retention:** an expiry-policy change moves every metric without moving any behaviour
- **Reconciliation needs the frame:** publishing a number requires publishing its window start, end, and expiry rule
- **Backfill is impossible:** a late-arriving post may already have expired, so the window can never be repaired

### Visualization (canvas `canvas4`, 720×360)

The same 61-minute volume series with two overlapping 20-minute windows shaded; all three sums computed from the array at render.

- **Layout:** origin at (72, 288), plot width 596, plot height 210. Axes `#1a5276`, width 2.
- **Data:** the identical hardcoded `vol` array from canvas 1 (define it once at script scope so the two charts cannot drift). Value scale 0–150.
- **Volume curve:** gray `#999`, width 2, so the window shading dominates.
- **Alice's window:** minutes 22–41 filled under the curve `rgba(26,82,118,0.30)`; dashed blue `#1a5276` verticals (5/5) at 22 and 42.
- **Bob's window:** minutes 25–44 outlined and filled `rgba(230,126,34,0.28)`; dashed orange `#e67e22` verticals (5/5) at 25 and 45.
- **Overlap marker:** solid `#1a5276` width 1 bracket under the axis spanning minutes 25–41, labelled at render `"shared: " + shared + "k"` in bold 11px `#1a5276`.
- **Window bars:** two thin horizontal bars above the plot at y = 44 (blue, minutes 22→42, label "Alice — loads at minute 42") and y = 62 (orange, minutes 25→45, label "Bob — loads at minute 45"), bold 11px labels in the matching colour.
- **Computed readout (bold 12px, two lines at x = 82, y = 96 and 112):** `"Alice " + a + "k · Bob " + b + "k · " + ((b/a-1)*100).toFixed(1) + "% hotter"` in `#e74c3c`, and `"shared " + shared + "k = " + (shared/a*100).toFixed(1) + "% of Alice, " + (shared/b*100).toFixed(1) + "% of Bob"` in `#666` — `a`, `b`, `shared` all summed from `vol`.
- **Axis labels:** x: "Minute of the hour"; y (rotated): "Posts per minute (thousands)" — `#1a5276`, 13px. Y gridlines at 0, 50, 100, 150 in `#eee`.
- **Title (bold 14px `#1a5276`, top center):** "Three Minutes Apart, 16% Apart — the Window Is the Denominator".

## 5. An Absolute Heatmap Just Draws a Population Map

**Obj-title:** The Base-Rate Problem, in Colour

The map is an anomaly detector whose only classifier is the human eye, so it inherits the base-rate problem. Colour a cell by raw count and the brightest cells are simply the most populous ones — every night, whether or not anything happened. "Hot" has to mean hot *relative to that cell's own baseline*, and the moment you divide, the ranking changes.

Math-box:

**Illustrative Example — four cells, tonight vs their own typical hour** (posts/hour)

| Cell | Baseline | Tonight | Tonight − baseline | Ratio |
|---|---|---|---|---|
| City A | 900 | 990 | +90 | 1.10 |
| City B | 400 | 520 | +120 | 1.30 |
| City C | 120 | 480 | +360 | 4.00 |
| City D | 60 | 300 | +240 | 5.00 |

Absolute rank: `A (990) > B (520) > C (480) > D (300)` — identical to the baseline order `900 > 400 > 120 > 60`. The absolute map is a population map with a different legend.

Ratio rank: `D (5.00) > C (4.00) > B (1.30) > A (1.10)` — the exact reverse. The two rankings agree on nothing.

Excess rank: `C (+360) > D (+240) > B (+120) > A (+90)` — a third ordering, because "biggest surprise" and "biggest multiple" are different questions.

- **Absolute colouring is a tautology:** it ranks cells by how many people live there, which nobody needed a map for
- **Ratio needs a per-cell baseline:** the same-hour-same-weekday history of *that* cell, not a global average
- **Small denominators explode:** a cell with a baseline of 3 hits 10x from seven extra posts — variance-stabilise or floor it
- **Ratio and excess disagree by construction:** pick the one matching the decision, and say which you picked
- **Baselines drift:** yesterday's spike enters tomorrow's baseline, so a sustained event stops looking anomalous
- **Two failure modes, both visible:** false positives in sparse cells, false negatives in dense ones already near saturation

### Visualization (canvas `canvas5`, 720×360)

Two side-by-side panels over the same four hardcoded cells: absolute counts vs ratio to baseline, with each panel's rank order computed at render.

- **Layout:** left panel x from 60 to 350, right panel x from 400 to 690; both with baseline at y = 288 and max bar height 180. Panel axes `#1a5276`, width 2.
- **Data (hardcoded literals, the numbers are the lesson):** labels `["City A","City B","City C","City D"]`, baseline `[900,400,120,60]`, tonight `[990,520,480,300]`. Ratios and ranks are computed in JS, never hardcoded.
- **Left panel:** paired bars per cell — baseline in `rgba(26,82,118,0.35)` with `#1a5276` border, tonight in `#e74c3c`; y scale 0–1000, gridlines at 0, 250, 500, 750, 1000 in `#eee` with `#666` 10px labels. Value printed above each red bar in `#e74c3c` 10px. Panel heading bold 12px `#1a5276`: "absolute count tonight".
- **Right panel:** single bars per cell of height `ratio / 5 × 180`, filled `#27ae60`; y scale 0–5 with gridlines at 1, 2, 3, 4, 5 in `#eee`; a solid `#e67e22` reference line at ratio 1.0 labelled "baseline = 1.0x" in `#e67e22` 10px. Each bar labelled at render with `ratio.toFixed(2) + "x"` computed as `tonight[i] / baseline[i]`, in bold `#27ae60` 11px. Panel heading bold 12px `#1a5276`: "count ÷ that cell's own baseline".
- **Rank strips (bold 11px, drawn at render under each panel at y = 310):** left `"rank: " + labelsSortedByTonightDesc.join(" > ")` in `#e74c3c`; right `"rank: " + labelsSortedByRatioDesc.join(" > ")` in `#27ae60` — both produced by sorting in JS so the reversal is demonstrated, not asserted.
- **Verdict line (bold 12px `#1a5276`, centered at y = 336):** computed by comparing the two rank arrays — prints "the two rankings are exact reverses of each other" when that holds, otherwise "the two rankings differ".
- **X labels:** cell names in `#666` 10px under each group in both panels.
- **Title (bold 14px `#1a5276`, top center):** "Illustrative Example — Absolute Ranks by Population, Ratio Ranks by Anomaly".

## 6. The Complete Picture

Summary table (`.summary-table`, header row + 6 rows):

| Design choice | Looks like an engineering knob | Is actually a statistical claim | Failure it produces |
|---|---|---|---|
| **Capacity ceiling** | Cost/throughput tradeoff | Which arrivals enter the estimate | Load shedding biases the map at the peak |
| **Cluster grid** | Rendering optimisation | The areal unit of every count | Hotspot moves when the grid shifts (MAUP) |
| **Sample rate** | Latency budget | A weight `1/f` applied to every cell | Scaled counts shown without their intervals |
| **Expiry window** | Storage policy | The denominator of every rate | Two viewers, two numbers, neither wrong |
| **Colour scale** | Visual design | The anomaly-detection threshold | Absolute colouring draws a population map |
| **Review rate** | Staffing plan | Coverage of the flagged stream | Coverage falls as 1/volume, backlog at the spike |

## Callout (philosophy box, bottom)

**One sentence:** The live map is an anomaly detector with no detector in it — so its grid, sample rate, window, and colour scale are not implementation details but the estimator itself, and the only defensible version divides by a local baseline and shows the interval it earned.

## Regeneration instructions

- **Layout:** detail page. h1 (no index number), `.subtitle`, opening `.philosophy` callout, then per aspect: `<h2>N. Title</h2>` (h2 1.4em `#1a5276`, bottom border `2px solid #2980b9`, padding-bottom 8px) followed by a one-row `.obj-table` — left `<td>` (50%) holds `.obj-title`, paragraph, `.math-box`, bullets; right `<td>` (50%, centered) holds the canvas. Section 6 is a `.summary-table`; page closes with a `.philosophy` callout.
- **Page style:** body system sans-serif (-apple-system, BlinkMacSystemFont, 'Segoe UI'), white background, text `#2a2a2a`, padding 40px 20px, line-height 1.6; h1 1.8em `#1a5276`; subtitle `#666` 1.05em; table cell borders `1px solid #e0e0e0`, padding 20px 24px, vertical-align middle; `.obj-title` 1.05em weight 600 `#1a5276`; `strong` `#1a5276`; ul 0.9em `#333`. No nav bar, no back/home/cross-reference links of any kind.
- **Callout style:** `.philosophy` — background `#f0f4f8`, left border `4px solid #2980b9`, padding 12px 16px, 0.9em.
- **Math box:** `.math-box` — background `#f8fafb`, border `1px solid #e0e0e0`, radius 6px, padding 16px 20px, 0.9em; `code` background `#eef2f7`, padding 2px 6px, radius 3px. The section 5 math-box contains an inner `.inline-table` (0.88em, `#f0f4f8` header, `1px solid #e0e0e0` cells).
- **Summary table:** `.summary-table` — 0.9em, th background `#f0f4f8` `#1a5276` padding 10px 14px left-aligned, td padding 10px 14px, borders `1px solid #e0e0e0`.
- **Canvas:** intrinsic 720×360 each; a shared `setupCanvas(id, w, h)` sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Data discipline:** no `Math.random()` anywhere. All five charts use hardcoded literal arrays or closed-form functions, because the counts and the rankings *are* the lesson. If any future chart on this page needs generated data, use an inline seeded LCG: `function lcg(seed){var s=seed;return function(){s=(s*16807)%2147483647;return s/2147483647;};}` with its own fixed seed per chart. Every statistic printed beside a chart — medians, peaks, percentages, ratios, rank orders — is computed in JS from the plotted values at render time, never typed in as a literal.
- **Naming:** no real company or city names in the body; the platform is "an ephemeral-media platform", places are "City A"–"City D", people are Alice and Bob. Moderation is described only as "items requiring review".
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, bar fill `rgba(26,82,118,0.35)`, gray text `#666`/`#999`, accent `#2980b9`.
