# Robust Regression

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Robust Regression

**Subtitle:** When one wild data point tries to drag your fitted line, Huber loss lets the ordinary points outvote it — you fit the crowd, not the outlier

## One Flat Tire Bends the Whole Line

**Tags:** `core idea` (blue), `outliers` (red), `least squares` (orange)

- **The pizzeria** — ten deliveries; times rise about 5 min per km, from 13 min at 1 km to 33 min at 5 km
- **The flat tire** — one 4 km run took 58 min instead of the usual ~28 because the driver changed a tire
- **Ordinary fit** — least squares chases that one point: the line tilts up to 5.8 + 6.7×km
- **Robust fit** — Huber regression stays with the crowd at roughly 7.9 + 5.1×km
- **Definition (after the example)** — robust regression fits a line that one weird row cannot drag

*Example (italic):* Quote a customer 6 km away: the dragged line predicts 46 min, the robust line about 38 — the crowd's answer.

**Key point:** Least squares treats every point as equally trustworthy; robust regression lets the nine ordinary deliveries outvote the one disaster.

### Visualization (canvas `c1`, 720×300)

Scatter of the ten deliveries with two fitted lines: the least-squares line dragged toward the flat-tire point, and the Huber line hugging the crowd.

- **Title (bold 15px, `#1a5276`, top center):** "Ten Pizza Deliveries: Least Squares vs Huber".
- **Data (crowd, blue `#2a78d6` 5px dots):** distances km `[1, 1.5, 2, 2.5, 3, 3.5, 4, 4.5, 5]`, times min `[13, 16, 18, 20, 23, 26, 28, 31, 33]`.
- **Outlier (magenta `#d55181` 7px dot):** (4 km, 58 min), with magenta bold 12px annotation above-left: "flat tire: 4 km, 58 min".
- **Axes:** origin x=60, baseline y=250, plot width 620, plot height 205; x scale 0–6 km with ticks at 1..6 labeled "1 km".."6 km" (12px `#444`); y scale 0–65 min with ticks 0/20/40/60 and light grid lines `#e5e9ef`; axis lines 2px ink `#1a5276`; y-axis label 12px `#444` "delivery time (min)".
- **Least-squares line:** orange `#d95926` 3px dashed (dash 6/4), y = 5.8 + 6.7×x drawn from x=0.5 to x=5.5; orange bold 12px label near its right end: "least squares: dragged up".
- **Huber line:** green `#008300` 3px solid, y = 7.9 + 5.1×x drawn from x=0.5 to x=5.5; green bold 13px label near its right end: "Huber: stays with the crowd".
- **Caption (12px `#444`, bottom left):** "same ten rows, two lines — only the loss function differs (illustrative)".

## Why Squaring Makes One Point a Bully

**Tags:** `worked example` (blue), `Huber loss` (green), `rule of thumb` (orange)

- **Crowd misses** — against the crowd's line 8 + 5×km, nine deliveries miss by at most 0.5 min
- **The big miss** — the flat-tire run is predicted at 28 min but took 58: a 30-min residual
- **Squared loss** — charges that row 30² = 900 while the other nine total 1.0 — one row is 99.9%
- **Huber loss** — squares small misses but switches to a straight line past a cutoff δ (here δ = 5 min)
- **The bill** — Huber charges the outlier 275 instead of 900, so its pull on the line is capped

*Example (italic):* Tilt the line 1 min closer to the outlier: squared loss saves ~59, Huber saves only 10 — not worth abandoning the crowd.

**Key point:** Under squared loss a point's pull grows with its distance; Huber caps the pull at the cutoff, so a 30-min miss argues no louder than a 6-min one.

### Visualization (canvas `c2`, 720×300)

Two loss curves over residual r from −30 to +30 min: the squared-loss parabola and the Huber curve (squared inside ±δ, straight lines outside), with the flat-tire residual marked on both.

- **Title (bold 15px, `#1a5276`, top center):** "What One Residual Costs: Squared vs Huber (δ = 5)".
- **Curves (evaluate analytically, no random data):** squared loss `L = r*r`; Huber loss `L = r*r` for |r| ≤ 5, else `L = 2*5*|r| − 25`; plot both for r in [−30, 30] (step 0.5).
- **Axes:** origin x=70, baseline y=250, plot width 580, plot height 200; x scale −30..30 with ticks at −30/−20/−10/0/10/20/30 (12px `#444`), x-axis label "residual (min)"; y scale 0..900 with ticks 0/300/600/900 and light grid `#e5e9ef`; axis lines 2px ink `#1a5276`.
- **Squared curve:** blue `#2a78d6` 3px; blue 6px dot at (30, 900) with blue bold 13px label "900".
- **Huber curve:** green `#008300` 3px; green 6px dot at (30, 275) with green bold 13px label "275".
- **Cutoff markers:** vertical dashed `#6b7280` (dash 4/3) lines at r = −5 and r = +5 from baseline to y of 120px above it, each labeled 12px `#6b7280` "δ = 5"; inside the band a mute 11px note "small misses: identical".
- **Annotation (orange `#d95926` bold 13px, mid-right):** "same 30-min miss, a 3.3× smaller bill".
- **Caption (12px `#444`, bottom left):** "Huber = squared for |r| ≤ δ, straight line beyond — the pull stops growing".

## One Cell Edit Should Not Move Your Forecast

**Tags:** `where it's used` (blue), `influence` (orange)

- **The experiment** — keep nine deliveries fixed and let the flat-tire time grow: 28, 38, 48, 58, 78 min
- **Least squares** — the slope climbs 5.0 → 5.6 → 6.2 → 6.7 → 7.9 min/km, tracking that one cell
- **Huber** — the slope holds near 5.1 min/km no matter how bad the single delivery gets
- **Where it bites** — sensor glitches, fat-finger data entry, one whale customer, one viral day
- **Median cousin** — Huber sits between the mean (all squares) and the median (all absolute values)

*Example (italic):* A typo turning 58 into 580 would multiply the least-squares slope several times over; the Huber line would not visibly move.

**Key point:** If a single row can move your forecast as far as it likes, you have a model of your worst row, not of the crowd — robust losses are insurance for rows nobody has inspected yet.

### Visualization (canvas `c3`, 720×300)

Line chart of the fitted slope as the one outlier's recorded time grows, for least squares vs Huber.

- **Title (bold 15px, `#1a5276`, top center):** "Fitted Slope as One Delivery's Time Grows (illustrative)".
- **Data:** x values (outlier time, min) `[28, 38, 48, 58, 78]`; least-squares slopes `[5.0, 5.6, 6.2, 6.7, 7.9]`; Huber slopes `[5.0, 5.1, 5.1, 5.1, 5.1]`.
- **Axes:** origin x=70, baseline y=245, plot width 580, plot height 185; x scale 25–80 with ticks at the five data values (12px `#444`), x-axis label "recorded time of the one outlier (min)"; y scale 4.5–8.5 min/km with ticks 5/6/7/8 and light grid `#e5e9ef`; axis lines 2px ink `#1a5276`; y-axis label 12px `#444` "fitted slope (min/km)".
- **Least-squares series:** orange `#d95926` 3px line with 5px dots; orange bold 13px annotation near (78, 7.9): "one cell drags the whole model".
- **Huber series:** green `#008300` 3px line with 5px dots; green bold 13px annotation near (78, 5.1): "Huber holds at ~5.1".
- **Marker:** vertical dashed `#6b7280` (dash 4/3) line at x=58 with 12px `#6b7280` label "the actual flat-tire value".
- **Caption (12px `#444`, bottom left):** "nine rows fixed; only one cell changes between points".

## Robust Means Resist, Not Erase

**Tags:** `common mistake` (red), `rule of thumb` (green)

- **Not deletion** — Huber keeps the flat-tire row in the fit; it just refuses to be dragged by it
- **Read the residual** — the robust fit leaves a 29.7-min residual flagging exactly which row is odd
- **The story row** — the outlier may be the finding: a route problem, a fraud case, a broken sensor
- **Leverage caveat** — Huber tames wild y values; a lone point far out in x can still tilt the line
- **Picking δ** — the usual default is 1.35 × a robust spread estimate, chosen to lose almost nothing on clean data

*Example (italic):* The pizzeria kept all ten rows, quoted customers off the Huber line, and separately fixed the route where tires kept failing.

**Common mistake:** Treating the robust fit as the end of the job. The line now describes the crowd — but the 29.7-min residual it exposes is a lead to investigate, not noise to forget.

### Visualization (canvas `c4`, 720×300)

Bar chart of each delivery's residual from the Huber line (7.9 + 5.1×km): nine tiny bars inside the δ band, one huge magenta bar begging to be investigated.

- **Title (bold 15px, `#1a5276`, top center):** "Residuals from the Huber Fit: Nine Whispers, One Shout (illustrative)".
- **Data:** ten bars, x labels `["1 km", "1.5", "2", "2.5", "3", "3.5", "4", "4.5", "5", "flat tire (4 km)"]` (12px `#444`, last label bold magenta), residuals min `[0.0, 0.5, -0.1, -0.7, -0.2, 0.3, -0.3, 0.2, -0.4, 29.7]`.
- **Axes:** origin x=70, zero line at y=215 (2px ink `#1a5276`), plot width 580; y scale −5 to +32 min with ticks at −5/0/5/15/30 and light grid `#e5e9ef`; y-axis label 12px `#444` "actual − predicted (min)".
- **Bars:** width 40px, evenly spaced; nine crowd bars fill `rgba(0,131,0,0.45)` with 1px `#008300` stroke; flat-tire bar fill `rgba(213,81,129,0.55)` with 2px `#d55181` stroke and bold 13px magenta value label "29.7" above it.
- **δ band:** horizontal dashed `#6b7280` (dash 4/3) lines at +5 and −5 across the plot, right-edge label 12px `#6b7280` "δ = ±5 min".
- **Annotation (magenta `#d55181` bold 13px, above the last bar):** "the row to investigate, not erase".
- **Caption (12px `#444`, bottom left):** "the robust fit doesn't hide the outlier — it isolates it".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (here it hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
