# Markov Chains

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Markov Chains

**Subtitle:** Predict tomorrow from today alone — a transition matrix stores the odds of every hop, and running it forward settles into a steady state no matter where you start

## One Kiosk, Twenty-One Days of Weather

**Tags:** `core idea` (blue), `memoryless` (green), `transition counts` (orange)

- **The kiosk** — a beach ice-cream kiosk logs 21 days of weather, each day sunny (S) or rainy (R)
- **Count from sun** — 15 sunny days had a next day; 12 of those next days were sunny: 12/15 = 0.8
- **Count from rain** — 5 rainy days had a next day; 3 stayed rainy (3/5 = 0.6), 2 turned sunny (0.4)
- **Memoryless** — tomorrow's odds depend only on today's sky, not on the whole history before it
- **Markov chain** — a system hopping between states where each hop looks only at the current state

*Example (italic):* Predicting day 9, the kiosk ignores days 1–7 entirely — day 8 was rainy, so tomorrow is sunny with chance 0.4.

**Key point:** A Markov chain compresses all history into the current state. Once you know today is sunny, knowing last week adds nothing to the forecast.

### Visualization (canvas `c1`, 720×300)

A 21-square weather strip at the top, with two small "what follows what" count panels below tallying the transitions the reader can verify by hand.

- **Title (bold 15px, `#1a5276`, top center):** "21 Days of Kiosk Weather: Counting What Follows What".
- **Data:** sequence `["S","S","S","S","S","S","R","R","S","S","S","S","S","R","R","S","S","S","S","R","R"]` (runs: 6 sunny, 2 rainy, 5 sunny, 2 rainy, 4 sunny, 2 rainy).
- **Strip:** 21 squares 24×24px starting x=90 y=55, 3px gap; sunny fill yellow `#c98500`, rainy fill blue `#2a78d6`; letter "S"/"R" bold 12px white centered in each square; day numbers 1, 7, 14, 21 in 11px `#6b7280` below their squares; legend 12px `#444` at top right "S = sunny, R = rainy".
- **Left count panel (from x=90, y=140):** heading bold 13px `#c98500` "after a sunny day (15 cases):"; two horizontal bars 18px tall, max width 220px scaled to count/15 — "next day sunny 12" fill `rgba(201,133,0,0.5)` and "next day rainy 3" fill `rgba(42,120,214,0.5)`, counts bold 12px at bar ends; caption bold 13px `#1a5276` "12/15 = 0.8 stay sunny".
- **Right count panel (from x=420, y=140):** heading bold 13px `#2a78d6` "after a rainy day (5 cases):"; bars scaled to count/5 over the same 220px — "next day rainy 3" blue fill, "next day sunny 2" yellow fill; caption bold 13px `#1a5276` "3/5 = 0.6 stay rainy".
- **Takeaway (bold 13px green `#008300`, bottom center y=285):** "four counted fractions are the whole model".

## The Transition Matrix Does the Forecasting

**Tags:** `worked example` (blue), `transition matrix` (green)

- **Two rows** — the matrix has one row per today-state, and each row's probabilities sum to exactly 1
- **Sunny row** — [0.8, 0.2]: from a sunny today, tomorrow is sunny 0.8 and rainy 0.2
- **Rainy row** — [0.4, 0.6]: from a rainy today, tomorrow is sunny 0.4 and rainy 0.6
- **One step** — today is sunny, so tomorrow's forecast is read straight off the sunny row: 0.8 sunny
- **Two steps** — P(sunny in 2 days | sunny today) = 0.8×0.8 + 0.2×0.4 = 0.64 + 0.08 = 0.72
- **Chain it** — multiplying the matrix by itself gives every two-day-ahead probability at once

*Example (italic):* Today is rainy, so two days out the sunny chance is 0.4×0.8 + 0.6×0.4 = 0.32 + 0.24 = 0.56.

**Key point:** The matrix is the machine: one row-lookup forecasts one day ahead, and repeated multiplication forecasts any number of days ahead.

### Visualization (canvas `c2`, 720×300)

A two-state diagram with labeled arrows on the left, and the 2×2 transition matrix drawn as a grid on the right.

- **Title (bold 15px, `#1a5276`, top center):** "The Kiosk's Weather Chain as Diagram and Matrix".
- **State circles:** "SUNNY" circle radius 42 centered (150, 165), fill `rgba(201,133,0,0.18)`, border 3px `#c98500`; "RAINY" circle radius 42 centered (380, 165), fill `rgba(42,120,214,0.18)`, border 3px `#2a78d6`; state names bold 13px `#1a5276` centered.
- **Arrows:** curved arrow (quadratic, control point above at y=95) from SUNNY to RAINY, 2.5px `#d95926`, arrowhead, bold 13px `#d95926` label "0.2" above; curved arrow below (control point y=235) from RAINY to SUNNY, 2.5px `#008300`, bold 13px `#008300` label "0.4" below; self-loop arc on SUNNY's left labeled bold 13px `#c98500` "0.8"; self-loop arc on RAINY's right labeled bold 13px `#2a78d6` "0.6".
- **Matrix grid (from x=500, y=90):** heading bold 13px `#1a5276` "transition matrix (rows sum to 1)"; 2×2 grid of 80×48px cells with 1px `#e5e9ef` borders; column headers "→ sunny", "→ rainy" and row headers "sunny", "rainy" in 12px `#444`; cell values bold 14px — 0.8 `#c98500`, 0.2 `#d95926`, 0.4 `#008300`, 0.6 `#2a78d6` (colors matching the arrows).
- **Caption (bold 12px `#6b7280`, under the grid):** "each cell = one arrow; read a row to forecast tomorrow".

## Every Forecast Settles at the Same Place

**Tags:** `steady state` (blue), `worked example` (green), `where it's used` (orange)

- **Run it forward** — start rainy: the sunny chance climbs 0 → 0.40 → 0.56 → 0.624 → 0.650 → 0.660
- **Start sunny** — the sunny chance falls 1 → 0.80 → 0.72 → 0.688 → 0.675 → 0.670 instead
- **Same limit** — both starting points land on 0.667 sunny; the chain forgets where it began
- **Solve it** — the steady state solves π = 0.8π + 0.4(1−π), which gives π = 2/3 sunny
- **Meaning** — the long-run share of days: about 2 of every 3 kiosk days will be sunny
- **Where it's used** — PageRank, churn models, and queue models are all read off a steady state

*Example (italic):* Whether the season opens sunny or rainy, by day 7 the forecast is within half a point of 66.7% sunny either way.

**Key point:** The steady state is the distribution the chain settles into on its own. It is a property of the matrix, not of the starting weather.

### Visualization (canvas `c3`, 720×300)

Line chart of P(sunny) over days 0–7 for both starting states, converging onto a dashed steady-state line at 2/3.

- **Title (bold 15px, `#1a5276`, top center):** "Two Starts, One Destination: P(sunny) Day by Day".
- **Data:** days `[0,1,2,3,4,5,6,7]`; sunny-start `[1, 0.8, 0.72, 0.688, 0.675, 0.670, 0.668, 0.667]`; rainy-start `[0, 0.4, 0.56, 0.624, 0.650, 0.660, 0.664, 0.666]`.
- **Axes:** origin x=70, plot width 560, baseline y=245, chart height 185; y from 0 to 1 with gridlines `#e5e9ef` and 12px `#444` labels at 0, 0.25, 0.5, 0.75, 1; x labels "day 0" … "day 7" 12px `#444` below baseline.
- **Sunny-start line:** yellow `#c98500`, 3px with 4px dots; bold 12px `#c98500` label "start sunny" near its first point.
- **Rainy-start line:** blue `#2a78d6`, 3px with 4px dots; bold 12px `#2a78d6` label "start rainy" near its first point.
- **Steady-state line:** dashed green `#008300` (dash 5/4) horizontal at y for 0.667, bold 13px `#008300` label "steady state = 2/3 sunny" above its right end.
- **Annotation (bold 13px violet `#4a3aa7`, near day 5–7):** "by day 7 the start no longer matters".
- **Caption (12px `#444`, bottom center):** "each point = previous point pushed through the matrix once".

## A Sunny Streak Doesn't Make Rain Due

**Tags:** `common mistake` (red), `memoryless` (green)

- **The streak trap** — after 5 sunny days in a row, rain tomorrow is still 0.2, not "overdue"
- **Only today counts** — a 1-day and a 5-day sunny streak give the identical forecast: 0.8 sunny
- **Not a coin** — the sky isn't independent either: sun follows sun (0.8) far more than rain (0.4)
- **Past not useless** — history matters, but only through the single state it left you in today
- **Quick test** — if the from-sunny and from-rainy rows matched, you'd have independence, not a chain

*Example (italic):* A regular customer bet the kiosk owner that rain was "overdue" after six straight sunny days — the rain chance was still 0.2.

**Common mistake:** Reading a Markov chain with gambler's-fallacy eyes — expecting long runs to raise the switch probability. The switch chance stays flat at 0.2 no matter how long the sunny run is.

### Visualization (canvas `c4`, 720×300)

Bar chart of the true P(rain tomorrow) after sunny streaks of length 1–6 (flat), with a dashed line showing the rising "feels overdue" intuition.

- **Title (bold 15px, `#1a5276`, top center):** "P(rain tomorrow) After a Sunny Streak of Length 1–6".
- **Data:** streak lengths `[1,2,3,4,5,6]`; true chain values `[0.2, 0.2, 0.2, 0.2, 0.2, 0.2]`; gambler's-fallacy intuition `[0.2, 0.3, 0.4, 0.5, 0.6, 0.7]` (illustrative).
- **Axes:** origin x=70, plot width 560, baseline y=245, chart height 185; y from 0 to 1 with 12px `#444` labels at 0, 0.5, 1 and gridlines `#e5e9ef`; x labels "1 day" … "6 days" 12px `#444` below each bar.
- **Bars:** six bars 55px wide, fill `rgba(42,120,214,0.45)`, 2px `#2a78d6` top edge; bold 12px `#2a78d6` value "0.2" above each bar.
- **Intuition line:** dashed magenta `#d55181` (dash 5/4), 2.5px with 4px dots through the six intuition values; bold 12px `#d55181` label at its right end, two lines: "'rain feels overdue'" / "(illustrative)".
- **Annotation (bold 13px green `#008300`, above the flat bars, centered):** "the chain is flat — only today's state matters".
- **Caption (12px `#444`, bottom center):** "memoryless: run length changes nothing once you know today is sunny".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- All data is hardcoded literal arrays — no `Math.random()`. In regenerated HTML, any card links would use `.html` extensions (this page has no links).
