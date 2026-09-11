# Backtesting: No Peeking at the Future

**Page type:** detail page (tutorial layout: `.card-section` blocks, each with an h2 and a two-column `table.layout` — text left 50%, canvas right 50%)
**HTML title tag:** Backtesting: No Peeking at the Future

**Subtitle:** Testing a forecast the honest way — train on the past, score on what came after, never mix the two

## Train on January-September, Score on October-December

**Tags:** `core idea` (blue), `time-ordered split` (green)

- **The shop** — a coffee shop has a year of daily cup sales and a forecast to test
- **The honest split** — fit the model on Jan-Sep only, then score it on Oct-Dec
- **Why in order** — in real life the model will only ever know the past; the test must too
- **The wrong way** — a random shuffle scatters October days into training: peeking
- **The name** — replaying history this way is called a backtest

*Example (italic):* Shuffling put Oct 14 in training and Oct 15 in test — the model "predicted" a day it had practically seen.

**Key point:** A backtest replays real life — everything the model learns from must come strictly before everything it is scored on.

### Visualization (canvas `c1`, 720×300)

Two horizontal month-segmented timelines comparing an honest time split against a shuffled split.

- **Title (bold 15px, `#1a5276`, top center):** "One year of sales, two ways to split it".
- **Layout:** padding left 58, right 24; 12 equal segments across the content width, each labeled J F M A M J J A S O N D (12px `#2c3e50`, centered); bar height 44px.
- **Timeline 1 (y=58):** label above in bold 13px green `#008300`: "HONEST: train Jan-Sep (blue), test Oct-Dec (green) — time flows left to right". Test months are indices 9, 10, 11 (Oct-Dec). Train segments filled `rgba(42,120,214,0.4)` stroked `#2a78d6`; test segments filled `rgba(0,131,0,0.45)` stroked `#008300`.
- **Time arrow:** under timeline 1 at y=116, a 2px `#1a5276` horizontal line across the content width ending in a filled right-pointing triangle arrowhead, with bold 12px centered label "time" at y=132.
- **Timeline 2 (y=160):** label above in bold 13px red `#e74c3c`: "SHUFFLED: test days (green) scattered everywhere — the future leaks into training". Test months are indices 1, 4, 6, 9, 11 (F, M(ay), J(ul), O, D), same fill/stroke scheme.
- **Big X:** a 4px `rgba(231,76,60,0.8)` X drawn diagonally across timeline 2 (corners at roughly x=pad+8 to x=pad+cw-8, y=158 to y=208).
- **Annotations (centered):** bold 14px `#e74c3c` at y=250: "shuffling a time series = grading with the answer key open"; bold 13px green `#008300` at y=274: "everything scored on comes strictly after everything trained on".

## Ten Days by Hand: the Rolling Origin

**Tags:** `worked example` (green), `rolling origin` (blue)

- **The data** — ten days of cups: 240, 244, 250, 248, 255, 260, 262, 266, 270, 268
- **Round 1** — know days 1-7, forecast day 8: predict 262, actual 266, miss 4
- **Round 2** — now know days 1-8, forecast day 9: predict 266, actual 270, miss 4
- **Round 3** — know days 1-9, forecast day 10: predict 270, actual 268, miss 2
- **The score** — average miss = (4 + 4 + 2) / 3 ≈ 3.3 cups; the origin rolled forward

*Example (italic):* Each round the "today" line moved one day right and the model re-forecast — exactly how it will live in production.

**Key point:** Rolling the origin gives many honest test days instead of one — every forecast still uses only what was known at that moment.

### Visualization (canvas `c2`, 720×300)

Line chart of 10 days of actual sales with three rolling-origin forecast rounds marked.

- **Title (bold 15px, `#1a5276`, top center):** "Rolling origin: forecast day 8, then 9, then 10 — misses 4, 4, 2".
- **Data (actuals):** `[240, 244, 250, 248, 255, 260, 262, 266, 270, 268]` for days d1–d10.
- **Axes:** padding top 46, bottom 46, left 58, right 24; y range 230–285 with gridlines and right-aligned 12px `#6b7280` labels at 240, 260, 280 (grid color `#e5e9ef`, axis stroke `#999`); x labels d1…d10 centered under each point.
- **Actual series:** connected line in blue `#2a78d6`, width 2.5, 3.5px-radius blue dots at each point, each labeled with its value in 11px `#2c3e50` above the dot.
- **Forecast rounds:** three rounds as [target index, forecast, actual] = [d8, 262, 266], [d9, 266, 270], [d10, 270, 268]. Each: a 5px-radius orange `#d95926` dot at the forecast value, a vertical red `#e74c3c` 2px line from forecast to actual, and a bold 12px red label to the right: "miss 4", "miss 4", "miss 2".
- **Legend/annotations (top left, left-aligned):** bold 12px orange `#d95926`: "orange = forecast (last known value)"; below it bold 13px violet `#4a3aa7`: "average miss = (4+4+2) / 3 ≈ 3.3 cups".

## The Lab Star That Failed on Monday

**Tags:** `why it matters` (orange), `leakage` (red)

- **Shuffled test** — the same model scored a 4-cup average miss: looked brilliant
- **Time-ordered test** — scored honestly, the miss was 12 cups: three times worse
- **Live reality** — deployed for a month, the miss came in at 13 cups a day
- **The verdict** — only the time-ordered backtest predicted live performance
- **The cost** — the shop staffed and stocked for a 4-cup model that never existed

*Example (italic):* The launch review quoted the shuffled 4-cup score; the post-mortem quoted the live 13.

**Key point:** Leaking future rows into training inflates the score in the lab and evaporates on day one — the honest backtest is your live-performance preview.

### Visualization (canvas `c3`, 720×300)

Three-bar comparison of the same model's average daily miss under three evaluation regimes.

- **Title (bold 15px, `#1a5276`, top center):** "Same model, three scores: average daily miss in cups (illustrative)".
- **Data:** shuffled split (lab) = 4 (red `#e74c3c`), time-ordered backtest = 12 (green `#008300`), live, first month = 13 (blue `#2a78d6`).
- **Axes:** padding top 56, bottom 46, left 58, right 24; y range 0–16 with gridlines (`#e5e9ef`) and 12px `#6b7280` labels at 4, 8, 12, 16; axis stroke `#999`.
- **Bars:** 140px wide, centered at 18%, 50%, 82% of content width; value labels bold 14px above each bar ("4 cups", "12 cups", "13 cups" — value label drawn in the bar color); category labels 12px `#2c3e50` below the baseline: "shuffled split (lab)", "time-ordered backtest", "live, first month".
- **Bracket:** violet `#4a3aa7` 2px bracket spanning from the backtest bar center (50%) to the live bar center (82%) near y-value 14.6–15.4, with bold 13px violet centered label above: "honest backtest ≈ live reality".
- **Annotation:** bold 13px red `#e74c3c` centered over the first bar at y-value 6: "3x too optimistic".

## Peeking Sneaks in Through the Side Door

**Tags:** `common mistake` (red), `hidden leakage` (orange)

- **Centered averages** — a "7-day average" centered on today quietly uses 3 future days
- **Full-data scaling** — normalizing by the whole year's mean bakes December into March
- **Repeated tuning** — tweak, re-score on the test months, repeat: the test leaks by iteration
- **Late-arriving data** — using corrected figures that only existed weeks after the day
- **The check** — for every feature ask: was this exact value knowable that morning?

*Example (italic):* The model's best feature was a centered moving average — its "signal" was tomorrow's sales in disguise.

**Key point:** Splitting by time is not enough — any feature computed from the full series can smuggle the future into the past.

### Visualization (canvas `c4`, 720×300)

Day-cell diagram showing a centered 7-day window reaching 3 days into the future, plus the honest trailing-window fix.

- **Title (bold 15px, `#1a5276`, top center):** "A \"7-day average\" centered on today secretly reads 3 future days".
- **Day cells:** 15 cells in a row (padding left 58, right 24), row at y=92, box height 46px, labeled d-7…d-1, d+0, d+1…d+7 in 11px `#2c3e50` ("today" = index 7, label format "d+0"). Past/today cells (index ≤ 7) filled `rgba(42,120,214,0.12)` stroked `#2a78d6`; future cells (index > 7) filled `rgba(231,76,60,0.10)` stroked `#e74c3c`.
- **Today marker:** vertical dashed `#1a5276` 2px line (dash 5/4) through the d+0 cell from y=46 to below the row, with bold 12px centered label "today (d+0)" at y=42.
- **Centered window bracket:** orange `#d95926` 3px rectangle around cells d-3 to d+3 (extending 8px above/below the row), with bold 12px orange left-aligned label above the row: "centered 7-day window: d-3 to d+3".
- **Leak highlight:** cells d+1, d+2, d+3 overlaid with `rgba(231,76,60,0.28)`; bold 13px red centered label below the row: "d+1, d+2, d+3: not knowable this morning — leakage".
- **Fix:** green `#008300` 3px rectangle below the row (y = row bottom + 38, height 30) spanning cells d-6 to d+0, with bold 12px green centered label: "the honest version: trailing window, d-6 to d+0".
- **Bottom annotation (bold 13px violet `#4a3aa7`, centered, y=272):** "ask of every feature: \"was this exact value knowable that morning?\"".

## Regeneration instructions

- **Template:** tutorials topic-page layout. Each section is a `.card-section` (`margin-bottom: 40px`) with an `<h2>` (1.3rem `#1a5276`, `border-bottom: 2px solid #2980b9`, padding-bottom 4px) followed by a `table.layout` with one `<tr>`: left `<td class="text-col">` (50%) and right `<td class="viz-col">` (50%), cells padded 12px, vertical-align top.
- **Text column structure:** `.tags` row of pill spans first, then a `<ul>` of one-line bullets each opening with `<b>bold term</b>` (li b color `#1a5276`), then an italic `.example` paragraph (`#555`, 0.9rem), then a `.key-point` callout (background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem) beginning with `<strong>Key point:</strong>`.
- **Tag pills:** inline-block, 0.72rem, weight 600, padding 2px 10px, border-radius 10px. Colors — blue: bg `rgba(26,82,118,0.12)` text `#1a5276`; green: bg `rgba(39,174,96,0.15)` text `#27ae60`; red: bg `rgba(231,76,60,0.12)` text `#e74c3c`; orange: bg `rgba(230,126,34,0.15)` text `#e67e22`.
- **Page style:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with `border-bottom: 2px solid #2980b9`; `.subtitle` `#666` 0.95rem. No nav bar, no back/home links.
- **Canvases:** each declared 720×300 with `width: 100%` CSS, `1px solid #e0e0e0` border, 4px radius; scaled via a shared `setup(id)` helper using `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates). All data hardcoded as literal arrays (no `Math.random()`). Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`; alarm red `#e74c3c`. Site palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- In regenerated HTML, any card links use `.html` extensions (this page has no outbound links).
