# Percentage Errors & MAPE

**Page type:** detail page (tutorial layout: `.card-section` blocks, each with a two-column `table.layout` — text left 50%, canvas right 50%)
**HTML title tag:** Percentage Errors & MAPE

**Subtitle:** Errors as percentages feel natural, but they blow up near zero and favor under-forecasting

## Ten Umbrellas Off Is Not Always the Same Miss

**Tags:** `core idea` (blue), `relative error` (blue)

- **The setup** — a store forecasts daily umbrella demand; some days sell 200, some sell 10
- **The week** — actual sales 100, 50, 200, 10, 40; forecasts 90, 60, 180, 20, 44
- **Unit errors** — the misses are 10, 10, 20, 10 and 4 umbrellas: all look modest
- **Percent errors** — divide each miss by that day's actual: 10%, 20%, 10%, 100%, 10%
- **The twist** — the same 10-umbrella miss is a shrug on a 100-day and a disaster on a 10-day

*Example:* Thursday sold 10 umbrellas; the forecast said 20 — off by 10 units, but off by 100%.

**Key point:** A percentage error re-weighs every miss by the size of the day it landed on — small days magnify, big days forgive.

### Visualization (canvas `c1`, 720×300)

Two side-by-side bar panels — misses in units vs misses in percent — split by a vertical dashed divider (`#bdc3c7`, dash 4/3) at x=360 from y=40 to h−15.

- **Title (bold 15px, `#1a5276`, top center):** "Same Week, Two Rulers: Umbrellas Off vs Percent Off"
- **Panels (each x-width 280, bars 36px wide, plot from y=66 to baseline y=240, y scale max 110, panel titles bold 13px `#1a5276` at y=54):**
  - Left (x=50): unit misses `[10, 10, 20, 10, 4]` for days `Mon, Tue, Wed, Thu, Fri`, bars in blue `#2a78d6` (Thursday also blue), title "miss in umbrellas", value labels without suffix.
  - Right (x=400): percent misses `[10, 20, 10, 100, 10]`, bars in aqua `#199e70` with Thursday highlighted in orange `#d95926`, title "miss in percent of that day", value labels with "%" suffix.
- Bold 12px `#444` value labels above bars; 12px `#222` day labels below.
- **Annotation (bold 13px orange, bottom center y=280):** "Thursday: 10 umbrellas off on a 10-umbrella day = 100%"

## Averaging to MAPE = 30% — Driven by One Tiny Day

**Tags:** `worked example` (green), `MAPE` (blue)

- **The recipe** — MAPE = the plain average of the percent errors, ignoring signs
- **The sum** — (10 + 20 + 10 + 100 + 10) / 5 = 30%: that is the week's MAPE
- **The driver** — Thursday alone contributes 20 of those 30 points: two-thirds of the score
- **Without it** — the other four days average to 12.5%: less than half the headline
- **The irony** — Thursday's miss was 10 umbrellas, tied for the smallest miss of the week

*Example:* A stakeholder reading "MAPE 30%" pictures a bad week; four of five days were within 20%.

**Key point:** MAPE hands its biggest weight to the lowest-volume days — exactly the days that matter least for revenue.

### Visualization (canvas `c2`, 720×300)

Bar chart of each day's contribution to the 30-point MAPE, with a summary panel on the right.

- **Title (bold 15px, `#1a5276`):** "Who Built the 30% MAPE? Points Contributed per Day"
- **Padding:** top 56, bottom 60, left 66, right 210. Gray `#999` L-frame axes. Y max 22.
- **Data:** days `Mon, Tue, Wed, Thu, Fri`, contributions `[2, 4, 2, 20, 2]` points (each day's % error ÷ 5). Bars 48px wide, Thursday in orange `#d95926`, others blue `#2a78d6`. Bold 12px `#444` labels above bars formatted "2 pts", "4 pts", "2 pts", "20 pts", "2 pts".
- **Axis titles 12px `#444`:** "day (contribution = its % error ÷ 5 days)" (bottom), rotated "points of the 30% MAPE" (left).
- **Right panel (x = w−195):** bold 13px `#1a5276` "MAPE = 30%"; bold 13px orange "Thursday: 20 of 30 pts" / "= two-thirds of the score"; 12px gray `#6b7280` "drop Thursday and the" / "week averages 12.5%".

## MAPE Quietly Rewards Forecasting Low

**Tags:** `common mistake` (red), `asymmetry` (orange)

- **A toy world** — demand alternates: 50 umbrellas half the days, 150 the other half
- **Honest guess** — forecast the mean, 100: percent errors 100% and 33% → MAPE 66.7%
- **Guess lower** — forecast 75: errors 50% and 50% → MAPE 50%: lower already
- **Lowest wins** — forecast 50: errors 0% and 66.7% → MAPE 33.3%, the minimum
- **Why** — under-forecasting caps at 100%, over-forecasting is unbounded, so low guesses are safe

*Example:* A team bonused on MAPE learns to lowball demand — and the store quietly runs out of stock.

**Key point:** Optimizing MAPE pushes forecasts below the average demand — the metric itself has a thumb on the scale.

### Visualization (canvas `c3`, 720×300)

Curve of MAPE as a function of the chosen constant forecast, with marker points and a mean-demand reference line.

- **Title (bold 15px, `#1a5276`, top center):** "Demand Alternates 50 / 150: MAPE of Each Constant Forecast"
- **Padding:** top 52, bottom 56, left 66, right 40. Gray `#999` L-frame axes. X 30–170 (forecast), y 0–130 (MAPE %).
- **Curve (violet `#4a3aa7`, width 3):** MAPE(f) = (|f−50|/50 + |f−150|/150) / 2 × 100, plotted for f = 30…170 step 2 — a V shape with minimum at f=50.
- **Markers (6px dots with bold 12px labels to the right):** forecast 50 → 33.3% in green `#008300`, labeled "forecast 50 → 33.3%" (green); forecast 75 → 50% in orange `#d95926`, labeled "forecast 75 → 50%" (`#444`); forecast 100 → 66.7% in orange, labeled "forecast 100 → 66.7%" (`#444`).
- **Reference line:** dashed `#bbb` vertical (dash 5/4, width 1.5) at x=100 with 12px `#888` label "true average demand".
- **X ticks:** 30, 50, 75, 100, 125, 150, 170. Axis titles 12px `#444`: "the constant forecast you commit to, umbrellas" (bottom), rotated "resulting MAPE, %" (left).
- **Annotation (bold 13px green, near y=20 level at x≈38):** "best MAPE = lowball at 50, half the true average"

## The Zero-Demand Day Breaks the Formula

**Tags:** `division by zero` (red), `rule of thumb` (green)

- **The crash** — a dry Saturday sells 0 umbrellas: percent error divides by zero, MAPE is undefined
- **Bad fix** — silently dropping zero days biases the score and hides the hardest days
- **Better fix** — WMAPE: total miss over total demand = 54 / 400 = 13.5% for our week
- **Why it differs** — WMAPE weighs days by volume, so tiny Thursday no longer dominates
- **Simplest fix** — when volumes vary wildly or touch zero, report MAE in plain units instead

*Example:* Same five days: MAPE says 30%, WMAPE says 13.5% — one tiny day explains the whole gap.

**Common mistake:** Using MAPE on data where demand can be tiny or zero — the metric fails exactly where forecasting is hardest.

### Visualization (canvas `c4`, 720×300)

Split panel: MAPE-vs-WMAPE bars on the left, a zero-day failure callout box on the right; vertical dashed divider (`#bdc3c7`, dash 4/3) at x=400 from y=40 to h−15.

- **Title (bold 15px, `#1a5276`, top center):** "Same Five Days, Two Scores — and the Day That Breaks Both?"
- **Left panel (x=80, width 260, baseline y=226, height 145, y max 35):** two bars 80px wide at 28% and 72% of panel width — MAPE = 30% in orange `#d95926`, WMAPE = 13.5% in green `#008300`. Bold 15px value labels ("30%", "13.5%") in bar color above; bold 12px `#222` labels "MAPE" / "WMAPE" below; 11px `#666` sub-captions "average of daily %" / "total miss ÷ total demand"; bold 12px green line below (y ≈ baseline+56): "WMAPE = 54 / 400 = 13.5%".
- **Right panel (box at x=430, 250×130, y=62):** background `#f8f9fa` with 2px red `#e74c3c` border. Text centered: bold 13px `#1a5276` "dry Saturday"; 13px `#444` "actual sold: 0 · forecast: 5"; bold 15px red "% error = 5 ÷ 0 → undefined"; 12px gray `#6b7280` "one such day poisons the whole MAPE". Below the box, bold 12px green: "WMAPE still works: it only divides once," / "by the week's total demand (400)"; then bold 12px red (y=276): "MAPE fails exactly where forecasting is hardest".

## Regeneration instructions

- **Layout:** tutorial detail page — h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks. Each section: `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one `<tr>`: left `td.text-col` (50%) holding `.tags` pill row, a `<ul>` of bold-term bullets (`li b` in `#1a5276`), an italic `.example` line, and a `.key-point` callout; right `td.viz-col` (50%) holding one 720×300 canvas.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; ul 0.92rem; `.example` italic `#555` 0.9rem; `.key-point` background `#f8f9fa`, left border `3px solid #e74c3c`, 0.9rem. No nav bar, no back/home links.
- **Tag pills:** `.tag` inline-block, 0.72rem, weight 600, padding 2px 10px, radius 10px. Colors: blue `rgba(26,82,118,0.12)`/`#1a5276`; green `rgba(39,174,96,0.15)`/`#27ae60`; red `rgba(231,76,60,0.12)`/`#e74c3c`; orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Canvas:** intrinsic 720×300, CSS `width:100%`, border `1px solid #e0e0e0` radius 4px; scaled via `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) in a shared `setup(id)` helper. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette (JS object `P`):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Doc palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- **Shared data:** umbrella demand week — actual `100, 50, 200, 10, 40`; forecast `90, 60, 180, 20, 44`; absolute errors `10, 10, 20, 10, 4` (total 54; total actual 400); percent errors `10%, 20%, 10%, 100%, 10%` → MAPE 30%, WMAPE 13.5%.
- This page has no card links; in regenerated HTML any links would use `.html` extensions.
