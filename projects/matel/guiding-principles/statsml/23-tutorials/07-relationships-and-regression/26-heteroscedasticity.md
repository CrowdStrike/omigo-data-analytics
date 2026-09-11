# Heteroscedasticity

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Heteroscedasticity

**Subtitle:** When the scatter around a regression line grows with x, the slope is still right but its standard error is a lie — robust SEs restore honest uncertainty

## One Courier, Thirty Deliveries

**Tags:** `core idea` (blue), `running example` (green), `fan shape` (orange)

- **The courier** — a bike courier logs 30 deliveries: distance in km and door-to-door minutes
- **The fit** — a regression gives minutes ≈ 10 + 4 × km: a 10-minute pickup plus 4 min per km
- **Short trips** — at 1 km, actual times land in 12–16 min, only ±2 around the line
- **Long trips** — at 10 km, times land anywhere in 35–65 min, ±15 around the same line
- **The name** — scatter that changes with x is heteroscedasticity; equal scatter is homoscedastic

*Example (italic):* A 1 km lunch run is predictable to 2 minutes either way; a 10 km run across town can be 15 minutes early or late.

**Key point:** Heteroscedasticity means the spread around the line is not constant — here it fans out with distance. Ordinary least squares quietly assumes it stays the same everywhere.

### Visualization (canvas `c1`, 720×300)

Scatter of 30 deliveries with the fitted line, showing a fan: tight at short distances, wide at long ones.

- **Title (bold 15px, `#1a5276`, top center):** "30 Bike Deliveries: Minutes vs Distance (illustrative)".
- **Data (hardcoded):** distances `[1,1,1,2,2,2,3,3,3,4,4,4,5,5,5,6,6,6,7,7,7,8,8,8,9,9,9,10,10,10]`; minutes `[12,14,16, 15,18,21, 17,22,27, 20,26,32, 22,30,38, 25,34,43, 27,38,49, 30,42,54, 32,46,60, 35,50,65]`.
- **Axes:** origin x=60, baseline y=250, plot width 600, plot height 195; x scale 0–11 km with ticks 0..10 (12px `#444`, axis label "distance (km)"); y scale 0–70 min with ticks 0, 10, ... 70 and label "minutes".
- **Points:** blue `#2a78d6` filled 5px dots.
- **Fit line:** green `#008300` 2.5px solid from (0, 10) to (10.5, 52) following y = 10 + 4x; green bold 12px label "fit: minutes = 10 + 4×km" along the line's upper end.
- **Brackets:** aqua `#199e70` 2px vertical bracket at x=1 spanning y-values 12–16 with bold 12px label "±2 min"; orange `#d95926` 2px vertical bracket at x=10 spanning 35–65 with bold 13px label "±15 min".
- **Annotation (bold 13px magenta `#d55181`, upper left area):** "spread grows with distance — a fan, not a tube".

## Spotting the Fan in the Residuals

**Tags:** `worked example` (blue), `residual plot` (green)

- **Residual** — actual minus predicted; the three 1 km trips leave residuals −2, 0, and +2 min
- **Growing** — at 5 km the residuals reach ±8 min; at 10 km they reach ±15 min
- **The rate** — in this example the spread grows by roughly ±1.5 min per extra km
- **Healthy** — a homoscedastic fit leaves a flat band, staying near ±5 at every distance
- **The check** — always plot residuals against the predictor; the fan is invisible in R² alone

*Example (italic):* Both panels have residuals averaging zero — only the plot reveals that one band is flat and the other fans open.

**Key point:** The diagnostic is one picture: residuals vs x. A flat band is fine; a fan (or funnel) means the equal-spread assumption is broken.

### Visualization (canvas `c2`, 720×300)

Dual-panel residual plot: a healthy flat band (left) vs this courier's fan (right), split by a vertical dashed divider at x=360.

- **Title (bold 15px, `#1a5276`, top center):** "Residuals vs Distance: Flat Band vs Fan".
- **Shared y scale:** residual range −18 to +18 min; plot top y=55, bottom y=245, zero line at y=150 drawn 1.5px `#999` in both panels.
- **Left panel (healthy, illustrative):** origin x=55, width 280; x scale 0–11 km; residuals at the same 30 distances as c1: `[3,-2,4, -5,1,-3, 5,-4,2, -1,4,-3, 2,-5,3, -2,5,-1, -4,2,3, -5,1,4, -2,-3,5, -1,2,-4]`; green `#008300` filled 4px dots; dashed `#6b7280` (dash 4/3) guide lines at +5 and −5; heading bold 12px `#444` "healthy: even ±5 band"; caption 12px `#444` "spread constant at every distance".
- **Right panel (the courier):** origin x=400, width 280, same scales; residuals `[-2,0,2, -3,0,3, -5,0,5, -6,0,6, -8,0,8, -9,0,9, -11,0,11, -12,0,12, -14,0,14, -15,0,15]` at the same 30 distances; magenta `#d55181` filled 4px dots; orange `#d95926` dashed (dash 4/3) envelope lines y = +1.5·km and y = −1.5·km from x=0 to 10.5; orange bold 13px annotation "fan: spread ≈ ±1.5 min per km"; caption "same fit — unequal spread".
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=360 from y=38 to h−12.
- **X labels:** ticks 0..10 (12px `#444`) under both panels.

## Coefficients Survive, Standard Errors Lie

**Tags:** `where it's used` (blue), `standard errors` (orange), `robust SEs` (green)

- **Slope stands** — heteroscedasticity does not bias OLS: the 4.0 min/km slope is still trustworthy
- **SE lies** — the naive slope SE is 0.30 but the robust SE is 0.52 — robust is 73% larger
- **Fake finding** — a weekend bump of 1.2 min gets naive CI [0.1, 2.3], which excludes zero
- **Honest read** — its robust CI is [−0.5, 2.9]: it crosses zero, so the bump is not established
- **The fix** — robust (heteroscedasticity-consistent) SEs are one option flag in any stats package

*Example (italic):* The analyst reported "weekends are significantly slower" from the naive CI [0.1, 2.3]; the robust CI [−0.5, 2.9] says the data cannot tell.

**Key point:** The fan does not move the line — it corrupts the uncertainty around it. Naive SEs are too small, p-values too eager; robust SEs widen intervals back to honest.

### Visualization (canvas `c3`, 720×300)

Horizontal 95% confidence intervals for two coefficients, naive vs robust, sharing one value axis.

- **Title (bold 15px, `#1a5276`, top center):** "Same Coefficients, Naive vs Robust 95% Intervals".
- **Value axis:** horizontal line at y=245 from x=90 to x=680, mapping values −1 to 5.5; ticks with 12px `#444` labels at −1, 0, 1, 2, 3, 4, 5.
- **Zero line:** dashed `#6b7280` (dash 4/3) vertical line at value 0 from y=55 to y=245, 12px `#6b7280` label "0" at top.
- **Data (hardcoded):** distance slope estimate 4.0, naive CI [3.4, 4.6], robust CI [3.0, 5.0]; weekend bump estimate 1.2, naive CI [0.1, 2.3], robust CI [−0.5, 2.9].
- **Rows:** "distance slope (min/km)" label bold 12px `#1a5276` left of x=90 area — naive interval at y=95, robust at y=125; "weekend bump (min)" — naive at y=185, robust at y=215.
- **Interval style:** 3px horizontal line with 8px vertical end caps and a 5px filled dot at the estimate; naive intervals magenta `#d55181` with 11px "naive" label at right end; robust intervals green `#008300` with 11px "robust" label.
- **Annotations:** magenta bold 12px near the naive weekend interval "excludes 0 — looks significant"; green bold 12px near the robust weekend interval "crosses 0 — not established"; green bold 13px right-aligned at x=710 below the robust slope interval "same estimate, honest width".

## The Wrong Fix and the Right One

**Tags:** `common mistake` (red), `rule of thumb` (green)

- **Not bias** — refit OLS on the fan data and the slope comes out 3.98 vs the true 4.0
- **Don't refit** — people re-estimate, trim, or delete the long trips; the line was never broken
- **Not outliers** — the wide 10 km points are honest data; removing them fakes precision
- **Do fix SEs** — report robust SEs (0.52, not 0.30); the coefficient column stays as is
- **Upgrades** — weighted least squares if you know the variance pattern; a log target if spread is multiplicative

*Example (italic):* A teammate deleted the six widest long-distance rows to "clean the data" — the slope barely moved, but the fake-narrow CI got even narrower.

**Common mistake:** Treating heteroscedasticity as a biased-slope problem and attacking the points. The fit survives; only the inference is broken — fix the standard errors, not the data.

### Visualization (canvas `c4`, 720×300)

Dual panel: what the fan does NOT change (the slope, left) vs what it DOES change (the SE, right), split by a dashed divider at x=360.

- **Title (bold 15px, `#1a5276`, top center):** "What the Fan Breaks — and What It Doesn't".
- **Left panel (slope survives):** origin x=55, width 280, baseline y=245, plot height 175; same 30 points as c1 drawn as 3px `rgba(42,120,214,0.5)` dots, x scale 0–11 km, y scale 0–70 min; true line dashed green `#008300` (dash 5/4) y = 10 + 4x; OLS line solid blue `#2a78d6` 2.5px y = 10.2 + 3.98x (visually almost on top); heading bold 12px `#444` "the slope survives"; blue bold 12px annotation "OLS 3.98 vs true 4.0".
- **Right panel (SE breaks):** two vertical bars on baseline y=245, bar width 70; x centers at 470 and 590; y scale 0–0.6 mapped over 175px; naive SE bar height for 0.30, fill `rgba(213,81,129,0.55)`, bold 13px magenta value label "0.30" above, 12px `#444` label "naive SE" below; robust SE bar for 0.52, fill `rgba(0,131,0,0.45)`, bold 13px green label "0.52", label "robust SE"; heading bold 12px `#444` "the standard error breaks"; magenta bold 12px annotation "robust is 73% larger".
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=360 from y=38 to h−12.
- **Takeaway (bold 13px `#1a5276`, bottom center):** "keep the coefficients, replace the standard errors".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (here it hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- All data arrays are hardcoded literals (no `Math.random()`); the delivery numbers are labeled "illustrative" in the c1 title.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
