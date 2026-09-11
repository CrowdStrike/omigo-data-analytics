# Gauss-Markov & BLUE

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Gauss-Markov & BLUE

**Subtitle:** Many fair recipes can fit a straight line — Gauss-Markov says that under four fine-print conditions, ordinary least squares is the fair recipe whose answer wiggles least

## Three Fair Ways to Draw One Line

**Tags:** `core idea` (blue), `unbiased` (green), `estimator` (orange)

- **The courier** — a bike courier logs 8 deliveries: minutes vs distance, roughly 4 min per km
- **Three recipes** — endpoints-only line, first-half vs second-half line, and least squares
- **All fair** — averaged over many days, each recipe centers on the true 4.0 min/km (unbiased)
- **Today's answers** — endpoints say 3.87, halves say 4.00, least squares says 3.97 min/km
- **The question** — if all three are fair on average, which one should the courier trust daily?

*Example (italic):* On today's log the three fitted lines nearly overlap — the difference between recipes only shows up when you repeat the estimate across many days.

**Key point:** Being right on average is cheap — lots of recipes are unbiased. The real contest is which fair recipe wanders least from day to day.

### Visualization (canvas `c1`, 720×300)

Scatter of the 8 deliveries with three fitted lines through the same points, slopes labeled in a legend.

- **Title (bold 15px, `#1a5276`, top center):** "8 Deliveries, 3 Fair Lines: Endpoints, Halves, Least Squares".
- **Data:** km `[1, 2, 3, 4, 5, 6, 7, 8]`; minutes `[9.5, 12.4, 17.8, 20.6, 25.5, 28.4, 33.8, 36.6]`.
- **Axes:** origin x=60, width 480, baseline y=250, chart height 195; x range 0–9 km, y range 0–40 min; ink `#1a5276` 1.5px axes; x tick labels "1".."8" and y ticks 0/10/20/30/40, 12px `#444`; axis captions 12px `#6b7280` "distance (km)" and "minutes".
- **Scatter:** 5px dots, fill `#2c3e50`.
- **Lines (each 2.5px, drawn from x=0.5 to x=8.5):** OLS blue `#2a78d6` y = 5.21 + 3.97x; endpoints orange `#d95926` y = 5.63 + 3.87x; halves green `#008300` y = 5.075 + 4.00x.
- **Legend (top-left inside plot, from x=80 y=55, 12px bold):** blue "least squares: 3.97 min/km", green "halves: 4.00 min/km", orange "endpoints: 3.87 min/km", each with a 22px color swatch line.
- **Annotation (bold 13px `#d55181`, right-aligned at x=710, y=150, two lines):** "three fair recipes," / "nearly the same line today".

## Replaying the Estimate for 200 Days

**Tags:** `worked example` (blue), `sampling variance` (orange), `rule of thumb` (green)

- **OLS by hand** — slope = Σ(x−x̄)(y−ȳ) / Σ(x−x̄)² = 166.7 / 42 = 3.97 min per km
- **Endpoints by hand** — (36.6 − 9.5) / (8 − 1) = 27.1 / 7 = 3.87 min per km
- **Halves by hand** — (31.075 − 15.075) / (6.5 − 2.5) = 16.0 / 4 = 4.00 min per km
- **Replay** — with noise SD 0.6 min, day-to-day slope SD: OLS 0.093, halves 0.106, endpoints 0.121
- **Best = tightest** — endpoints has 1.7× the variance of OLS, so it needs ~70% more data to match

*Example (italic):* All three bells sit on the true 4.0, but the endpoints bell is a third wider than the OLS bell — its daily answer strays further from the truth.

**Key point:** This picture IS the Gauss-Markov theorem: among all fair straight-line recipes, least squares has the smallest day-to-day variance — the Best Linear Unbiased Estimator.

### Visualization (canvas `c2`, 720×300)

Three sampling-distribution bell curves for the slope estimate, all centered on 4.0, widths matching the three recipes.

- **Title (bold 15px, `#1a5276`, top center):** "Day-to-Day Slope Estimates: Three Bells on the Same Truth".
- **Axis:** horizontal line at y=250 from x=70, width 580; slope scale 3.6–4.4; ticks and 12px `#444` labels at 3.6, 3.8, 4.0, 4.2, 4.4; caption 12px `#6b7280` "estimated slope (min/km)".
- **Curves (deterministic, no randomness):** for each recipe draw y(s) = peak · exp(−(s−4.0)² / (2·SD²)) above the baseline, 2.5px stroke, sampled at 1px steps: OLS blue `#2a78d6` SD 0.093 peak 195px; halves green `#008300` SD 0.106 peak 171px; endpoints orange `#d95926` SD 0.121 peak 150px (peaks proportional to 1/SD).
- **True-slope marker:** dashed `#1a5276` (dash 4/3) vertical line at slope 4.0 from y=45 to y=250, bold 12px ink label "true slope 4.0" above.
- **Legend (top-right from x=520 y=60, 12px bold, color swatches):** "OLS  SD 0.093", "halves  SD 0.106", "endpoints  SD 0.121".
- **Annotation (bold 13px blue `#2a78d6`, near the OLS peak, x≈190 y≈75):** "narrowest bell = least squares".
- **Caption (12px `#6b7280`, bottom center):** "noise SD 0.6 min, 8 deliveries at 1–8 km (illustrative)".

## The Fine Print: Four Conditions

**Tags:** `where it's used` (blue), `assumptions` (orange), `failure mode` (red)

- **Linear truth** — the real relationship is a straight line in the coefficients (4 min per km)
- **Fair errors** — delays average zero at every distance; noise carries no signal about km
- **Equal spread** — the noise SD is the same 0.6 min whether the trip is 1 km or 8 km
- **No echoes** — one delivery's delay says nothing about the next one's (uncorrelated errors)
- **Break one** — funnel noise keeps OLS fair, but it is no longer tightest; weighting beats it

*Example (italic):* Long trips cross more downtown traffic, so their delays get wilder — the residual funnel on the right panel is the classic equal-spread violation.

**Key point:** Normality is NOT on the list — Gauss-Markov never asks for bell-shaped errors, only zero-mean, equal-spread, uncorrelated ones.

### Visualization (canvas `c3`, 720×300)

Dual-panel residual plot: equal-spread residuals (left, conditions hold) vs funnel-shaped residuals (right, equal spread broken), split by a dashed divider at x=360.

- **Title (bold 15px, `#1a5276`, top center):** "Residuals by Distance: Conditions Hold vs Funnel Violation".
- **Shared scale:** residual range −3 to +3 min maps to y=65 (top) through y=235 (bottom), zero line at y=150; km 1–8 spread across each panel width; solid 1.5px `#999` zero line in both panels; x tick labels "1".."8" 11px `#444`, caption 12px `#6b7280` "distance (km)" under each panel.
- **Left panel (holds):** origin x=55, width 280; heading bold 12px `#008300` "equal spread — Gauss-Markov happy"; residuals `[0.5, -0.6, 0.8, -0.4, 0.5, -0.6, 0.8, -0.4]` as 5px green `#008300` dots; aqua band fill `rgba(25,158,112,0.12)` between residual +1.2 and −1.2 across the panel with dashed `#199e70` (dash 4/3) edges; green bold 12px annotation "same ±1.2 band at every km".
- **Right panel (broken):** origin x=400, width 280; heading bold 12px `#d95926` "funnel — OLS fair but no longer best"; residuals `[0.2, -0.3, 0.6, -0.8, 1.2, -1.5, 2.1, -2.6]` as 5px orange `#d95926` dots; two dashed `#d95926` guide lines from ±0.3 at km 1 widening to ±2.7 at km 8; magenta `#d55181` bold 12px annotation, two lines: "spread grows with distance —" / "weighted LS becomes the new best".
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=360 from y=38 to h-12.

## What "Best" Does and Doesn't Mean

**Tags:** `common mistake` (red), `BLUE` (blue), `rule of thumb` (green)

- **B-L-U-E** — Best (lowest variance) Linear (weighted sum of the y's) Unbiased Estimator
- **Small pond** — "best" only compares against linear unbiased recipes, not every estimator
- **Biased rivals** — ridge/shrinkage accept a little bias to cut variance and can win on MSE
- **Broken spread** — under funnel noise the weighted least-squares line is the new BLUE
- **Not normality** — normal errors buy t-tests and stronger claims, but BLUE never needs them

*Example (italic):* Calling OLS "best" is like calling a cyclist the fastest commuter — fastest among the bikes, not measured against the cars.

**Common mistake:** Reading BLUE as "OLS beats everything." It wins a fenced contest; allow bias or unequal spread outside the fence, and other estimators beat it.

### Visualization (canvas `c4`, 720×300)

Nested-boxes diagram showing the fenced contest OLS wins, with the rivals that live outside the fence listed on the right.

- **Title (bold 15px, `#1a5276`, top center):** "BLUE Wins a Fenced Contest".
- **Outer box:** rounded rect x=60 y=55 w=420 h=215, 2px `#6b7280` border, label bold 12px `#6b7280` at top-left inside: "all estimators of the slope".
- **Middle box:** rounded rect x=90 y=90 w=360 h=160, 2px violet `#4a3aa7` border, label bold 12px `#4a3aa7`: "linear (weighted sums of the y's)".
- **Inner box:** rounded rect x=120 y=125 w=300 h=105, 2px green `#008300` border, fill `rgba(0,131,0,0.06)`, label bold 12px `#008300`: "linear + unbiased".
- **OLS marker:** 7px blue `#2a78d6` dot at (270, 195) with bold 13px blue label centered below: "OLS — lowest variance in here (BLUE)".
- **Outside-the-fence list (right side, from x=505 y=95, 12px):** heading bold 12px `#d55181` "outside the fence:"; two entries, each a bold colored term + one plain 11–12px `#444` line: orange `#d95926` "ridge / shrinkage" — "a little bias, less variance"; violet `#4a3aa7` "nonlinear estimators" — "not weighted sums at all". Inside the green box, magenta `#d55181` bold 12px centered two-line note at (270, 162)/(270, 177): "weighted LS is in here too —" / "it takes over when spread is unequal".
- **Caption (12px `#6b7280`, bottom center):** "Gauss-Markov compares OLS only against the green box".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (here it hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
