# Polynomial Regression & Splines

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Polynomial Regression & Splines

**Subtitle:** A straight-line fitter can draw curves — add a squared column and the same linear machinery bends; when the shape has several bends, splines cut the axis into pieces at knots

## One Smoothie Truck, One Bend the Line Can't Make

**Tags:** `core idea` (blue), `curvature` (orange), `linear model` (green)

- **The truck** — a smoothie truck logs daily cups sold against that day's high temperature, 50–95 °F
- **The shape** — cups climb from 34 at 50 °F to a peak of 302 at 85 °F, then slip back to 270 at 95 °F
- **Straight line** — the best line, cups ≈ 5.4·temp − 169, keeps climbing forever and misses the turn
- **The bend** — a rise-then-fall shape needs the effect of temperature to change sign; a line cannot turn
- **Still linear** — "linear regression" means linear in the coefficients, so the same tool can fit curves

*Example (italic):* At 95 °F the straight line predicts 344 cups but the truck sold 270 — the hottest customers stay home.

**Key point:** A straight line assumes each extra degree adds the same number of cups everywhere; the truck's data says the effect flips sign near 85 °F.

### Visualization (canvas `c1`, 720×300)

Single-panel scatter of the 10 daily observations with the straight-line fit (misses the turn) and the quadratic fit (follows it) overlaid.

- **Title (bold 15px, `#1a5276`, top center):** "Daily Cups Sold vs High Temperature: Line vs Curve (illustrative)".
- **Data:** temps `[50, 55, 60, 65, 70, 75, 80, 85, 90, 95]`, cups `[34, 98, 166, 214, 248, 281, 296, 302, 291, 270]`.
- **Axes:** origin x=70, plot width 600, baseline y=250, plot height 195; x maps 45–100 °F, y maps 0–360 cups; y ticks 0/100/200/300 (12px `#444`), x tick labels at each temp; axis lines 2px `#1a5276`; x axis title 12px `#444` "high temperature (°F)".
- **Scatter:** blue `#2a78d6` 5px dots at the 10 points.
- **Straight line:** orange `#d95926` 2.5px line, cups = 5.4·t − 169, drawn from t=48 to t=97.
- **Quadratic:** green `#008300` 3px curve, cups = −0.22·t² + 37.4·t − 1290, drawn from t=48 to t=97.
- **Annotations:** orange bold 12px near top right "line says 344 at 95 °F — off by 74"; green bold 13px above the peak "curve turns at 85 °F".
- **Caption (12px `#444`, bottom left):** "one dot per day; same 10 days, two fits".

## The Trick: A New Column Called temp²

**Tags:** `worked example` (blue), `rule of thumb` (green)

- **New column** — copy the temp column and square it: 60 → 3600, 70 → 4900, 85 → 7225, 95 → 9025
- **Same machinery** — ordinary linear regression on the two columns fits cups = b0 + b1·temp + b2·temp²
- **The fit** — the truck's 10 days give cups ≈ −0.22·temp² + 37.4·temp − 1290
- **Check by hand** — at 70 °F: −0.22·4900 + 37.4·70 − 1290 = 250 cups, right next to the observed 248
- **The peak** — the curve tops out at temp = 37.4 / (2·0.22) = 85 °F, exactly the truck's best day

*Example (italic):* One extra spreadsheet column turns the line-fitter into a curve-fitter — no new algorithm needed.

**Key point:** Polynomial regression is plain linear regression on manufactured columns (temp², temp³, ...); "linear" refers to the coefficients, not to the picture the fit draws.

### Visualization (canvas `c2`, 720×300)

Two-panel: the design matrix drawn as a small table (left) and the fitted parabola with a hand-check drop line at 70 °F (right), split by a vertical dashed divider at x=360.

- **Title (bold 15px, `#1a5276`, top center):** "One New Column, Same Line-Fitter".
- **Left panel (design matrix table):** heading bold 12px `#444` at (55, 55) "the design matrix: one manufactured column"; table from x=55 to x=320, header row bold 12px `#1a5276` `["temp", "temp²", "cups"]` at y=85 with a 1.5px `#1a5276` rule under it; four data rows 12px `#2c3e50` at y=115/145/175/205: `(60, 3600, 166)`, `(70, 4900, 248)`, `(85, 7225, 302)`, `(95, 9025, 270)`; the temp² column values in bold violet `#4a3aa7`; violet bold 12px note at y=235 "temp² is just temp × temp — a copied, squared column".
- **Right panel (fit):** axis origin x=400, plot width 290, baseline y=245, plot height 180; x maps 45–100, y maps 0–360; green `#008300` 3px parabola cups = −0.22·t² + 37.4·t − 1290 from t=48 to 97; blue `#2a78d6` 5px dots at the four table rows (60,166), (70,248), (85,302), (95,270); violet `#4a3aa7` dashed (dash 4/3) vertical line from the baseline up to the curve at t=70; violet bold 12px annotation, two lines: "−0.22·4900 + 37.4·70 − 1290" / "= 250 cups (observed 248)".
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=360 from y=38 to h-12.

## Two Rushes Need Splines, Not Higher Powers

**Tags:** `splines` (blue), `knots` (green), `where it's used` (orange)

- **New question** — the same truck's cups by hour of day: a 10 am rush, a 1 pm lull, a 5 pm rush
- **Two humps** — one quadratic can only draw one dome, so it splits the difference and misses both
- **The miss** — the dome predicts 93 cups at the 1 pm lull (actual 42) and 80 at the 10 am rush (actual 130)
- **Splines** — fit a separate cubic piece between chosen "knots" and force the pieces to join smoothly
- **Low degree, many pieces** — splines bend often without high powers, so they stay tame between points

*Example (italic):* With knots at 11 am, 2 pm, and 5 pm, each stretch of the day gets its own gentle cubic.

**Key point:** When the shape has several bends, do not raise the polynomial degree — cut the x-axis into pieces at knots and fit a low-degree curve per piece.

### Visualization (canvas `c3`, 720×300)

Single-panel hourly scatter with the one-dome quadratic (misses both rushes) and a fitted cubic regression spline, knots marked by dashed verticals.

- **Title (bold 15px, `#1a5276`, top center):** "Cups by Hour: One Quadratic vs a Spline with 3 Knots (illustrative)".
- **Data:** hours `[8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20]`, cups `[40, 95, 130, 100, 60, 42, 48, 80, 125, 150, 120, 70, 30]`.
- **Axes:** origin x=70, plot width 600, baseline y=250, plot height 190; x maps hours 7.5–20.5, y maps 0–180; x labels 12px `#444` "8am", "10am", "12pm", "2pm", "4pm", "6pm", "8pm" at hours 8/10/12/14/16/18/20; y ticks 0/60/120/180.
- **Scatter:** blue `#2a78d6` 5px dots at the 13 points.
- **Quadratic:** orange `#d95926` 2.5px dashed (dash 6/4) dome, cups = 94.3 + 0.51·(h−14) − 0.75·(h−14)², drawn h=8 to 20.
- **Spline:** green `#008300` 3px curve, the least-squares cubic regression spline with knots at 11/14/17 (truncated power basis): spl(h) = −4869.4857 + 1343.7044h − 118.4946h² + 3.4005h³ − 0.1882(h−11)³₊ − 9.1075(h−14)³₊ + 12.0004(h−17)³₊, drawn h=8 to 20 in 0.25 steps — it smooths the points (e.g. 118.6 at the 10 am rush vs actual 130), it does not interpolate them.
- **Knots:** violet `#4a3aa7` dashed (dash 4/3) vertical lines at hours 11, 14, 17 from baseline to y=45, each with bold 12px violet label "knot" at the top.
- **Annotations:** orange bold 12px near the dome apex "one dome: 93 at 1 pm, actual 42"; green bold 13px above the 5 pm point "spline follows both rushes".

## The Edges Are Where Polynomials Lie

**Tags:** `common mistake` (red), `extrapolation` (orange), `rule of thumb` (green)

- **Too much power** — a degree-8 curve nearly threads all 10 days but wiggles between them
- **The wiggle** — between observed days it overshoots to 322 cups near 88 °F, above every real day
- **Off the edge** — just past the data, at 100 °F, the degree-8 fit dives to −180 cups (illustrative)
- **The quadratic** — the humble temp² model says 250 cups at 100 °F: not certain, but at least sane
- **Natural splines** — splines built to go straight beyond the last knot fail gently at the edges

*Example (italic):* Asked to forecast a 100 °F heat-wave day, the degree-8 model answers −180 smoothie cups.

**Common mistake:** Trusting a high-degree polynomial outside — or even between — its data points; the tails of a polynomial are pure invention, so keep the degree low or switch to splines.

### Visualization (canvas `c4`, 720×300)

Single-panel comparison of a wiggly degree-8 fit and the stable quadratic on the same 10 points, with a shaded extrapolation zone past 95 °F where the degree-8 curve dives below zero.

- **Title (bold 15px, `#1a5276`, top center):** "Degree-8 Fit vs Quadratic: the Edges Invent Data (illustrative)".
- **Data (dots):** temps `[50, 55, 60, 65, 70, 75, 80, 85, 90, 95]`, cups `[34, 98, 166, 214, 248, 281, 296, 302, 291, 270]`; blue `#2a78d6` 5px dots.
- **Axes:** origin x=70, plot width 600, baseline at y for cups = −200, plot height 200; x maps 45–103 °F, y maps −200 to 360; y ticks −200/0/200/300 (12px `#444`); dashed 1px `#bdc3c7` horizontal zero line at cups = 0 labeled "0 cups" 11px `#6b7280`.
- **Extrapolation zone:** rect fill `rgba(107,114,128,0.10)` from t=95 to t=103 over the full plot height, bold 12px `#6b7280` label "beyond the data" at its top.
- **Degree-8 curve:** magenta `#d55181` 2.5px polyline through hardcoded points (t, cups): `(48, 90), (50, 34), (52, 10), (55, 98), (58, 190), (60, 166), (62, 150), (65, 214), (68, 268), (70, 248), (72, 240), (75, 281), (78, 310), (80, 296), (82, 290), (85, 302), (88, 322), (90, 291), (92, 255), (95, 270), (97, 150), (99, −80), (100, −180)` — draw with a smooth quadratic-curve join between points.
- **Quadratic:** green `#008300` 3px curve, cups = −0.22·t² + 37.4·t − 1290, drawn from t=48 to t=102 (value 250 at t=100).
- **Annotations:** magenta bold 13px inside the shaded zone, two lines: "degree-8:" / "−180 cups at 100 °F"; green bold 12px near t=100 on the green curve "quadratic: 250"; magenta bold 12px near (88, 322) "overshoots to 322".
- **Caption (12px `#444`, bottom left):** "same 10 days as above; degree-8 path is illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (here it hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- All curves are deterministic closed-form functions or hardcoded point lists — no `Math.random()`.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
