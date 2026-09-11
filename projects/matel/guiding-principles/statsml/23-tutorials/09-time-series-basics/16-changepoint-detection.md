# Changepoint Detection

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Changepoint Detection

**Subtitle:** A changepoint is the moment a process shifts to a new normal — split the series at every candidate day, and the split with the biggest before/after gap marks the change

## The Day the Office Opened

**Tags:** `core idea` (blue), `level shift` (green), `before/after` (orange)

- **The shop** — a coffee shop sells about 120 cups a day for three weeks, steady with small wiggles
- **The shift** — on day 21 a new office opens across the street; sales jump to about 155 a day
- **It stays** — the jump is not one busy day; every day from 21 onward sits near the new level
- **The question** — given only the numbers, could you recover that day 21 is when things changed?
- **The definition** — a changepoint is the time index where the data's underlying level (or trend) shifts

*Example (italic):* Day 20 sold 120 cups and day 21 sold 152 — but only the next 19 days prove it was a real shift, not a fluke.

**Key point:** A changepoint splits a series into a "before" and an "after" that behave differently. Detection means finding that split from the data alone.

### Visualization (canvas `c1`, 720×300)

Single line chart of 40 days of cup sales with a level shift at day 21, segment mean lines, and a dashed vertical marker at the changepoint.

- **Title (bold 15px, `#1a5276`, top center):** "Daily Cups Sold: 40 Days, One Changepoint".
- **Data (days 1–40):** `[118, 124, 116, 121, 120, 119, 113, 122, 122, 120, 115, 123, 118, 126, 121, 117, 124, 119, 122, 120, 152, 158, 150, 156, 156, 154, 148, 157, 160, 155, 151, 159, 153, 157, 156, 149, 158, 154, 157, 160]` (days 1–20 mean exactly 120, days 21–40 mean exactly 155).
- **Axes:** origin x=55, plot width 610 (x from 55 to 665), baseline y=245, chart height 185, y scale 100–170; y ticks 100/120/140/160 (12px `#444`); x labels "day 1", "10", "20", "30", "40" below baseline (12px `#444`).
- **Line:** blue `#2a78d6` 2.5px with 3px dots for all 40 points.
- **Segment means:** dashed blue line at y-value 120 spanning days 1–20 labeled "mean 120" (bold 12px blue); dashed green `#008300` line at y-value 155 spanning days 21–40 labeled "mean 155" (bold 12px green).
- **Changepoint marker:** vertical dashed magenta `#d55181` line (dash 4/3) between day 20 and day 21, from y=38 to baseline; bold 13px magenta annotation "day 21: office opens, +35 cups/day".
- **Caption (12px `#444`, bottom):** "illustrative daily sales — the level jumps once and stays".

## Scoring Every Possible Split Day

**Tags:** `worked example` (blue), `rule of thumb` (green)

- **The recipe** — for each candidate day k, compute the mean of days 1..k and the mean of days k+1..40
- **The score** — score(k) is the gap |mean before − mean after|; a big gap means a convincing split
- **Try day 10** — before mean 119.5, after mean 143.5, gap 24.0 (the "after" side still mixes old days)
- **Try day 20** — before mean 120, after mean 155, gap 35.0 — the biggest of any candidate split
- **The answer** — the score peaks at day 20, so the detected changepoint is the start of day 21

*Example (italic):* Splitting at days 5, 10, 15, 18, 20, 22, 25, 30, 35 gives gaps 20.2, 24.0, 28.2, 32.0, 35.0, 31.8, 28.3, 23.9, 20.7 — a clean peak at 20.

**Key point:** Changepoint detection is a search: score every split, keep the best one. Real libraries use fancier scores (CUSUM, likelihood), but the peak-at-the-split logic is the same.

### Visualization (canvas `c2`, 720×300)

Bar chart of the before/after gap score for nine candidate split days, peaking at day 20.

- **Title (bold 15px, `#1a5276`, top center):** "Gap Score for Each Candidate Split Day".
- **Data:** candidate days `[5, 10, 15, 18, 20, 22, 25, 30, 35]` with scores `[20.2, 24.0, 28.2, 32.0, 35.0, 31.8, 28.3, 23.9, 20.7]` (computed from the c1 series).
- **Axes:** origin x=55, plot width 610, baseline y=240, chart height 175, y scale 0–40; y ticks 0/10/20/30/40 (12px `#444`); candidate-day labels 12px `#444` under each bar ("day 5" ... "day 35").
- **Bars:** width ~44px, fill `rgba(42,120,214,0.45)`, 1px `#2a78d6` stroke; each bar's score printed 12px `#444` above it.
- **Winner bar (day 20):** fill `rgba(0,131,0,0.5)`, 1.5px `#008300` stroke; bold 13px green annotation above it "35.0 — biggest gap → change after day 20".
- **Caption (12px `#444`, bottom):** "score(k) = |mean of days 1..k − mean of days k+1..40|".

## Why One Average Fits Neither Half

**Tags:** `where it's used` (blue), `failure mode` (red)

- **The trap** — averaging all 40 days gives 137.5 cups, a level the shop never actually sold on any day
- **Both wrong** — 137.5 overshoots the first 20 days by 17.5 and undershoots the last 20 by 17.5
- **Forecasts break** — a forecast anchored at 137.5 orders too many beans before and runs out after
- **Where it appears** — code deploys shifting error rates, price changes, sensor recalibration, policy starts
- **The fix** — detect the changepoint first, then summarize or forecast using only the "after" segment

*Example (italic):* Ordering supplies for 137.5 cups a day wasted stock for three weeks, then left customers unserved after day 21.

**Key point:** Any statistic computed across a changepoint describes a process that no longer exists. Find the break first; summarize each side separately.

### Visualization (canvas `c3`, 720×300)

The same 40-day series with a single global-mean line overlaid, showing it misses both halves; error gaps annotated on each side.

- **Title (bold 15px, `#1a5276`, top center):** "One Global Average (137.5) Fits Neither Half".
- **Data:** same 40-value array as c1: `[118, 124, 116, 121, 120, 119, 113, 122, 122, 120, 115, 123, 118, 126, 121, 117, 124, 119, 122, 120, 152, 158, 150, 156, 156, 154, 148, 157, 160, 155, 151, 159, 153, 157, 156, 149, 158, 154, 157, 160]`.
- **Axes:** identical to c1 (origin x=55, plot width 610, baseline y=245, chart height 185, y scale 100–170, same tick labels).
- **Line:** the series in muted blue `rgba(42,120,214,0.55)` 2px with 2.5px dots (background role in this chart).
- **Global mean:** solid orange `#d95926` 3px horizontal line at y-value 137.5 across the full plot, bold 13px orange label "global mean 137.5" above its left end.
- **Error annotations:** magenta `#d55181` vertical double-arrow near day 10 from y-value 120 up to 137.5 with bold 12px magenta label "17.5 too high"; second magenta double-arrow near day 30 from 137.5 up to 155 labeled "17.5 too low".
- **Caption (12px `#444`, bottom):** "the average of two different regimes describes neither one".

## Spike, Drift, or Step?

**Tags:** `common mistake` (red), `outlier vs changepoint` (orange)

- **Spike** — one wild day (a bus tour buys 168 cups) that returns to normal is an outlier, not a changepoint
- **Drift** — sales creeping up a few cups every single day is a trend; there is no one moment to point at
- **Step** — a jump that arrives and stays at the new level is the changepoint signature
- **The test** — cover the suspect day and ask: do the days after it still look different from the days before?
- **Noise discipline** — small wiggles around a steady mean are neither; do not re-plan on every bump

*Example (italic):* The 168-cup bus-tour day looks dramatic, but days either side of it all sit near 120 — nothing changed.

**Common mistake:** Flagging a single spike as a changepoint. An outlier changes one observation; a changepoint changes every observation that follows.

### Visualization (canvas `c4`, 720×300)

Three mini line panels side by side — outlier, trend, changepoint — each 12 points, with a verdict label under each.

- **Title (bold 15px, `#1a5276`, top center):** "Three Shapes That Get Confused".
- **Panel layout:** three panels with origins x=55, x=280, x=505, each plot width 180, baseline y=230, chart height 150, shared y scale 100–175; panel headings bold 12px `#1a5276` above each ("spike", "drift", "step").
- **Panel 1 (spike):** data `[120, 122, 119, 121, 168, 120, 118, 123, 121, 119, 122, 120]`; blue `#2a78d6` 2.5px line, 3px dots; the 168 point circled in magenta `#d55181` with bold 12px magenta label "one day, back to normal".
- **Panel 2 (drift):** data `[120, 123, 126, 130, 133, 137, 140, 144, 147, 151, 154, 158]`; yellow `#c98500` 2.5px line, 3px dots; bold 12px `#c98500` label "no single moment".
- **Panel 3 (step):** data `[120, 122, 119, 121, 118, 123, 155, 153, 157, 154, 156, 158]`; green `#008300` 2.5px line, 3px dots; dashed magenta vertical line between points 6 and 7; bold 12px green label "jumps and stays".
- **Verdict labels (bold 13px, centered under each panel, y=262):** "outlier — ignore it" (magenta `#d55181`), "trend — model the slope" (`#c98500`), "changepoint — split here" (green `#008300`).
- **Caption (12px `#444`, bottom center):** "only the step pattern has a 'moment the process changed'".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (here it hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
