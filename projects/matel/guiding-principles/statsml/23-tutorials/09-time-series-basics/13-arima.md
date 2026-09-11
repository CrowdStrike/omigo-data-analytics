# ARIMA

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + `table.layout` two-column row, text left 50% / canvas right 50%)
**HTML title tag:** ARIMA

**Subtitle:** ARIMA forecasts a series with three moves — difference away the climb, lean on the last value, and correct for the last miss

## One Café, Three Letters

**Tags:** `core idea` (blue), `running example` (green)

- **The café** — a café logs daily cups sold for 21 days, climbing from 106 to 175 with wobble
- **The job** — forecast day 22 from the history alone; ARIMA is the classic recipe for this
- **AR (autoregression)** — busy days follow busy days: today's cups echo yesterday's cups
- **I (integrated)** — the steady climb is removed by differencing: model day-to-day changes
- **MA (moving average)** — yesterday's forecast miss still carries a hint, so reuse part of it
- **The label** — ARIMA(1,1,1) means 1 past value, 1 differencing pass, 1 past miss

*Example (italic):* Feeding the 21 days into ARIMA(1,1,1) yields a day-22 forecast of about 183 cups.

**Key point:** ARIMA is three moves in one model: difference away the drift (I), lean on the last value (AR), and correct with the last miss (MA).

### Visualization (canvas `c1`, 720×300)

Line chart of 21 days of climbing cup sales with a dashed forecast step to day 22 and three ingredient callouts.

- **Title (bold 15px, `#1a5276`, top center):** "21 days of cups sold, plus the day-22 ARIMA forecast"
- **Data (`cups21`, comment: "daily cups sold (illustrative); day-22 forecast ≈ 183"):** `[106,111,109,117,120,118,126,130,127,133,138,134,137,140,148,145,154,160,158,167,175]`
- **Axes:** y from 90 to 200, tick labels 100, 140, 180; x labeled "d1", "d7", "d14", "d21"; L-shaped `#999` axes; padding top 46, bottom 44, left 58, right 60.
- **Series:** blue `#2a78d6` line, width 2.5; radius-4 blue dot on day 21 with bold 12px blue label "175".
- **Forecast:** dashed (5/4) orange `#d95926` segment width 2.5 from day 21 (175) to day 22 (183); open orange circle (radius 5, 2.5px stroke, white fill) at day 22; bold 13px orange label "day 22: ≈183" centered above it.
- **Callouts (stacked upper-left plot area, bold 12px, one per line):** green `#008300` "AR: busy days follow busy days"; violet `#4a3aa7` "I: difference away the climb"; magenta `#d55181` "MA: reuse yesterday's miss".
- **Caption (12px gray `#6b7280`, bottom center):** "daily cups sold (illustrative)".

## The I: Difference Away the Climb

**Tags:** `worked example` (green), `differencing` (blue)

- **Eight days** — the café's last eight days: 140, 148, 145, 154, 160, 158, 167, 175 cups
- **Subtract** — each day minus the day before gives: +8, −3, +9, +6, −2, +9, +8
- **Drift gone** — the levels climb 35 cups in a week; the changes just wobble around +5
- **Why bother** — AR and MA assume a series with a stable level; raw climbing levels break them
- **The d** — one subtraction pass is d = 1; a series still drifting can be differenced again

*Example (italic):* The question shifts from "how many cups?" (drifting) to "how many more than yesterday?" (stable).

**Key point:** The I is this differencing step — ARIMA models the changes, then adds them back up ("integrates") to forecast levels.

### Visualization (canvas `c2`, 720×300)

Dual panel split by a dashed divider at x=360: the 8 daily levels drifting up (left) and the 7 day-over-day changes as bars (right).

- **Title (bold 15px, `#1a5276`, top center):** "Levels drift, changes sit still"
- **Divider:** dashed (4/3) vertical line `#bdc3c7` at x=360 from y=38 to h−12.
- **Data:** `levels8 = [140, 148, 145, 154, 160, 158, 167, 175]` (days d14–d21); `diffs7 = [8, -3, 9, 6, -2, 9, 8]` (days d15–d21).
- **Left panel (x0=50, width 285, plot top 62, height 168, y 130–185, ticks 140, 160, 180):** bold 13px `#1a5276` panel title "the levels"; blue `#2a78d6` line width 2.5 with radius-4 dots; each value printed above its dot (11px `#2c3e50`); x labels "d14"…"d21" 11px `#444`; caption 12px gray "cups per day — still climbing".
- **Right panel (x0=400, width 285, same plot top/height, y −5 to 12, ticks 0, 5, 10, gray `#999` zero line):** bold 13px `#1a5276` panel title "the changes"; bars width 26px, positive fill `rgba(25,158,112,0.55)`, negative fill `rgba(213,81,129,0.55)`; signed labels "+8", "−3", … 11px `#2c3e50` above/below bars; dashed (6/4) green `#008300` width 2.5 line at y=5 with bold 12px green label "mean change +5"; caption 12px gray "day-over-day change — one stable level".

## AR + MA: Tomorrow's Forecast by Hand

**Tags:** `worked example` (green), `autoregression` (blue), `moving average` (orange)

- **The recipe** — next change = 5 + 0.6 × (last change − 5) + 0.4 × last miss
- **AR term** — the last change was +8, which is 3 above the mean, so AR adds 0.6 × 3 = +1.8
- **MA term** — the model predicted +6 for day 21 but got +8, a +2 miss: MA adds 0.4 × 2 = +0.8
- **Total** — 5 + 1.8 + 0.8 = +7.6 more cups, so day 22 is 175 + 7.6 ≈ 183 cups
- **The weights** — 0.6 is the AR(1) coefficient, 0.4 the MA(1) coefficient; fitting picks them

*Example (italic):* One subtraction, two multiplications, one addition — the whole day-22 forecast fits on a napkin.

**Key point:** AR reuses the last change, MA reuses the last error; on the differenced series both are one-line arithmetic.

### Visualization (canvas `c3`, 720×300)

Waterfall chart assembling the day-22 change forecast from its three pieces, ending with the level arithmetic.

- **Title (bold 15px, `#1a5276`, top center):** "Building the day-22 forecast: 5 + 1.8 + 0.8 = +7.6 cups"
- **Data (comment: "waterfall pieces (illustrative): base 5.0, AR +1.8, MA +0.8, total +7.6"):** four bars — "mean change" 0→5.0, "AR: 0.6×(8−5)" 5.0→6.8, "MA: 0.4×(+2)" 6.8→7.6, "forecast change" 0→7.6.
- **Axes:** y from 0 to 9, tick labels 0, 2, 4, 6, 8; L-shaped `#999` axes; padding top 52, bottom 60, left 58, right 24.
- **Bars:** width 90px, evenly spaced across the plot; fills — mean change `rgba(42,120,214,0.55)` (blue), AR piece `rgba(0,131,0,0.5)` (green), MA piece `rgba(74,58,167,0.5)` (violet), forecast change `rgba(217,89,38,0.55)` (orange); dashed (4/3) gray `#bdc3c7` connector lines linking each bar's top to the next bar's start.
- **Bar value labels (bold 12px, matching each bar's solid color, above each bar):** "+5.0", "+1.8", "+0.8", "+7.6".
- **Bar names (12px `#444`, two lines allowed, below baseline):** "mean change", "AR: 0.6×(8−5)", "MA: 0.4×(+2)", "forecast change".
- **Annotation (bold 13px ink `#1a5276`, upper right):** "175 cups + 7.6 ≈ 183 cups tomorrow".
- **Caption (12px gray `#6b7280`, bottom center):** "AR(1) weight 0.6, MA(1) weight 0.4 (illustrative)".

## Two Things Called "Moving Average"

**Tags:** `common mistake` (red), `rule of thumb` (blue)

- **Two "MA"s** — the moving-average smoother and the MA term in ARIMA share a name, nothing else
- **The smoother** — averages the last few observed values to draw a calmer line through the data
- **The MA term** — feeds the model's own recent forecast misses back into the next forecast
- **Notation** — ARIMA(1,1,1) reads: 1 past change (AR), 1 differencing pass (I), 1 past miss (MA)
- **Quick check** — a smoother needs only the data; the MA term needs a model that makes errors

*Example (italic):* An analyst applied a 7-day rolling average thinking it was "the MA part" — it just blurred the series.

**Common mistake:** Assuming the MA in ARIMA smooths the data. It never touches the raw values — it recycles the model's own forecast errors.

### Visualization (canvas `c4`, 720×300)

Dual panel split by a dashed divider at x=360: a 3-day smoother over cup counts (left) vs bars of forecast misses feeding the next forecast (right).

- **Title (bold 15px, `#1a5276`, top center):** "The smoother averages data; the MA term recycles misses"
- **Divider:** dashed (4/3) vertical line `#bdc3c7` at x=360 from y=38 to h−12.
- **Left panel (x0=50, width 285, plot top 66, height 160, y 145–175, ticks 150, 160, 170):** bold 13px `#1a5276` panel title "moving-average smoother"; data `cups10 = [150,158,152,161,155,164,159,168,162,170]` as blue `#2a78d6` line width 2 with radius-3 dots; 3-day averages `ma3 = [153.3,157,156,160,159.3,163.7,163,166.7]` (comment: "3-day averages of cups10, hardcoded") plotted from the 3rd point as aqua `#199e70` line width 3; bold 12px aqua label "3-day average" near its line; caption 12px gray "a calmer line drawn from the data itself".
- **Right panel (x0=400, width 285, plot top 66, height 160, y −5 to 6, ticks −4, 0, 4, gray `#999` zero line):** bold 13px `#1a5276` panel title "MA term in ARIMA"; data `misses5 = [3, -2, 4, -1, 2]` labeled "d17"…"d21" (11px `#444`); bars width 30px, positive fill `rgba(74,58,167,0.55)` (violet), negative fill `rgba(213,81,129,0.55)` (magenta); signed labels "+3", "−2", "+4", "−1", "+2" 11px `#2c3e50` above/below bars; bold 12px violet `#4a3aa7` annotation, two lines: "next forecast adds" / "0.4 × the latest miss"; caption 12px gray "forecast misses (illustrative) — data untouched".

## Regeneration instructions

- **Template/layout:** tutorials topic page. `<h1>` (no index number) with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one `<tr>`: `td.text-col` (50%) and `td.viz-col` (50%, one canvas).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** `.tag` inline-block 0.72rem weight 600, padding 2px 10px, radius 10px; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; ul 0.92rem; `li b` `#1a5276`; `.example` italic `#555` 0.9rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links, no cross-page links.
- **Canvas:** all four intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates; all data arrays hardcoded (no randomness). Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette (JS object `P`):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette anchors: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
