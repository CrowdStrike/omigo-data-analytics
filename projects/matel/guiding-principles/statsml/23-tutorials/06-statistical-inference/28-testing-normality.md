# Testing Normality

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Testing Normality

**Subtitle:** Before trusting a t-test or a control chart, check the "roughly bell-shaped" assumption — a QQ plot shows where the data bends away from normal, and Shapiro-Wilk turns the check into one number

## Twenty Loaves on a Scale

**Tags:** `core idea` (blue), `normality check` (green), `histograms lie at small n` (orange)

- **The bakery** — a baker weighs 20 sourdough loaves: they range 477g to 524g, averaging about 500g
- **The plan** — she wants a t-test comparing two ovens, and the t-test leans on roughly normal data
- **The other column** — the same notebook logs 20 customer wait times: 1.2 to 19.5 minutes
- **Eyeball test** — at 20 points a histogram is lumpy; the weights and waits both look "sort of okay"
- **The tell** — weights spread evenly around 500g; waits pile up under 6 min with a long right tail

*Example (italic):* Loaf weights fill bins 2, 4, 7, 5, 2 — a rough bell; wait times fill 7, 7, 2, 2, 1, 0, 1 — a slide with a tail.

**Key point:** "Is this normal?" is a real question you must answer before using normal-based tools, and a lumpy 20-point histogram is too weak an instrument to answer it alone.

### Visualization (canvas `c1`, 720×300)

Dual-panel histogram: the 20 loaf weights (left, rough bell) vs the 20 wait times (right, right-skewed), split by a vertical dashed divider at x=360.

- **Title (bold 15px, `#1a5276`, top center):** "Two Columns from One Bakery Notebook: Weights vs Waits".
- **Weights data (sorted, grams):** `[477, 482, 487, 489, 492, 493, 495, 497, 498, 499, 501, 503, 504, 506, 507, 510, 512, 514, 518, 524]`.
- **Waits data (sorted, minutes):** `[1.2, 1.5, 1.8, 2.0, 2.3, 2.5, 2.8, 3.0, 3.3, 3.6, 4.0, 4.4, 5.0, 5.6, 6.5, 7.5, 9.0, 11.0, 14.5, 19.5]`.
- **Left panel (weights):** bin counts `[2, 4, 7, 5, 2]` for edges 475–525 in 10g steps; axis origin x=55, width 280, baseline y=240, chart height 175, y scale 0–8; bars fill `rgba(42,120,214,0.45)`, 1px `#2a78d6` stroke; bin edge labels "475"…"525" 11px `#444` below; blue `#2a78d6` bold 12px annotation "roughly a bell?"; caption 12px `#444` "20 loaf weights (g)".
- **Right panel (waits):** bin counts `[7, 7, 2, 2, 1, 0, 1]` for edges 0–21 in 3-min steps; axis origin x=400, width 280, same baseline/height, y scale 0–8; bars fill `rgba(213,81,129,0.4)`, 1px `#d55181` stroke; edge labels "0", "3", "6", "9", "12", "15", "18", "21"; magenta `#d55181` bold 12px annotation "long right tail"; caption "20 wait times (min)".
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=360 from y=38 to h-12.
- **Footer note (11px `#6b7280`, bottom left):** "illustrative data".

## The QQ Plot: Your Data vs a Normal Ruler

**Tags:** `worked example` (blue), `QQ plot` (green), `read the bend` (orange)

- **The recipe** — sort your 20 values, then pair each with where a perfect normal would put it
- **Normal ruler** — for 20 points the ruler marks sit at z = −1.96, −1.44, ... 1.44, 1.96
- **By hand** — the smallest weight 477g pairs with z = −1.96; the largest 524g pairs with z = 1.96
- **Straight = normal** — weights hug the line 500 + 12z, so a normal with mean 500, sd 12 fits
- **Bend = skew** — the biggest wait, 19.5 min, sits far above its ruler spot of about 11.4 min

*Example (italic):* Every weight lands within a few grams of the line, but the top three waits (11.0, 14.5, 19.5) climb further above it each step.

**Key point:** A QQ plot is just sorted data against a normal template — points on a straight line mean normal, and the direction of the bend tells you how it fails (an upward right tail = right skew).

### Visualization (canvas `c2`, 720×300)

Dual-panel QQ plot: loaf weights on the line (left) vs wait times bending upward (right), split by a dashed divider at x=360.

- **Title (bold 15px, `#1a5276`, top center):** "QQ Plots: Weights Hug the Line, Waits Bend Away".
- **Theoretical quantiles (both panels, x values):** `[-1.96, -1.44, -1.15, -0.93, -0.76, -0.60, -0.45, -0.32, -0.19, -0.06, 0.06, 0.19, 0.32, 0.45, 0.60, 0.76, 0.93, 1.15, 1.44, 1.96]`.
- **Left panel (weights):** y values are the sorted weights array from c1; axis origin x=55, width 280, baseline y=240, chart height 180; x range −2.2 to 2.2, y range 470–530; reference line ink `#1a5276` 2px from (−1.96, 476.5) to (1.96, 523.5) (y = 500 + 12z); points blue `#2a78d6` 4px dots; x ticks at −2, −1, 0, 1, 2 (12px `#444`), y ticks 480/500/520; green `#008300` bold 12px annotation "points on the line = normal fits"; caption 12px `#444` "weights vs z".
- **Right panel (waits):** y values are the sorted waits array from c1; axis origin x=400, width 280, same baseline/height; x range −2.2 to 2.2, y range 0–21; reference line ink 2px, y = 4.7 + 3.4z (through the quartiles), from (−1.96, −1.96→clip at y=0) to (1.96, 11.4); points magenta `#d55181` 4px dots; magenta bold 12px annotation with arrow to the top point: "19.5 vs 11.4 expected"; caption "waits vs z".
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=360 from y=38 to h-12.
- **Takeaway (bold 13px `#008300`, bottom center):** "straight line = normal; upward bend on the right = right skew".

## Shapiro-Wilk: The Same Check as One Number

**Tags:** `where it's used` (blue), `Shapiro-Wilk` (green), `W statistic` (orange)

- **What it does** — Shapiro-Wilk scores how straight the QQ plot is; W = 1 means perfectly straight
- **The weights** — W = 0.98, p = 0.93: no evidence against normal, the t-test can proceed
- **The waits** — W = 0.78, p = 0.0004: the straight-line fit fails, normal tools are out
- **Reading p** — small p says "a true normal almost never produces a QQ plot this bent"
- **Pipelines** — automated checks use it because a number, unlike a picture, can gate a pipeline

*Example (italic):* The baker's report reads one line per column: weights W = 0.98 (keep the t-test), waits W = 0.78 (switch to a rank test or log the data).

**Key point:** Shapiro-Wilk is the QQ plot's straightness packaged as a test statistic — use the plot to see how normality fails and the test to decide, not one without the other.

### Visualization (canvas `c3`, 720×300)

Horizontal W-scale from 0.70 to 1.00 with both columns' W values marked, plus a two-row decision table beneath.

- **Title (bold 15px, `#1a5276`, top center):** "Shapiro-Wilk W: How Straight Was the QQ Plot? (illustrative)".
- **W scale:** horizontal 2px `#999` line at y=110 from x=90, width 540, mapping W 0.70 (left) to 1.00 (right); ticks with 12px `#444` labels at 0.70, 0.80, 0.90, 1.00; label bold 12px `#444` "W = 1 means perfectly straight" right-aligned above the 1.00 end.
- **Markers:** green `#008300` 7px dot at W = 0.98 with bold 13px label above "weights W = 0.98"; magenta `#d55181` 7px dot at W = 0.78 with bold 13px label above "waits W = 0.78".
- **Decision rows (from y=170, two rows 34px apart, x from 90):** row 1 — green 12px bold "weights:" then 12px `#444` "W = 0.98, p = 0.93 → no evidence against normal → t-test OK"; row 2 — magenta 12px bold "waits:" then "W = 0.78, p = 0.0004 → reject normality → rank test or log transform".
- **Takeaway (bold 13px `#1a5276`, centered at y=265):** "the plot shows HOW it fails; the test gives the yes/no".

## A p Above 0.05 Doesn't Prove Normal

**Tags:** `common mistake` (red), `sample size trap` (orange), `rule of thumb` (green)

- **The trap** — "p = 0.93, therefore normal" is wrong; the test only failed to find evidence against
- **Small n** — at n = 20 even clearly skewed data can pass: the test has little power to see it
- **Big n** — the same mild skew gives p = 0.31 at n = 20, p = 0.04 at n = 200, p < 0.001 at n = 5,000
- **The irony** — at huge n the test rejects harmless wiggles exactly when the t-test needs them least
- **Rule** — always look at the QQ plot; ask "is the bend big enough to matter?", not just "p < 0.05?"

*Example (italic):* The same slightly skewed process fails Shapiro-Wilk at n = 5,000 and sails through at n = 20 — the data's shape never changed, only the sample size did.

**Common mistake:** Treating the Shapiro-Wilk p-value as a normality certificate. It measures evidence, which scales with n — tiny samples pass everything and huge samples fail everything, so the QQ plot's bend size is the judgment call.

### Visualization (canvas `c4`, 720×300)

Bar chart of Shapiro-Wilk p-values for the same mildly skewed shape at three sample sizes, with the 0.05 line crossing the bars.

- **Title (bold 15px, `#1a5276`, top center):** "Same Mild Skew, Three Sample Sizes: Only n Changed (illustrative)".
- **Data:** labels `["n = 20", "n = 200", "n = 5,000"]`, p-values `[0.31, 0.04, 0.001]` (draw the last bar at a 4px minimum height, value label "< 0.001").
- **Layout:** axis origin x=90, width 480, baseline y=245, chart height 180; y scale 0–0.35 with ticks 0, 0.1, 0.2, 0.3 (12px `#444`); three bars 90px wide, 70px gaps; fills: n=20 `rgba(0,131,0,0.45)`, n=200 `rgba(217,89,38,0.55)`, n=5,000 `rgba(213,81,129,0.55)`; bold 13px value labels above each bar: "p = 0.31", "p = 0.04", "p < 0.001"; x labels 13px `#444` below each bar.
- **Threshold line:** dashed `#e74c3c` (dash 5/4) horizontal line at p = 0.05 across the plot area, red bold 12px label "p = 0.05" at its right end.
- **Annotations:** green bold 12px above the first bar's label "passes — low power"; magenta bold 12px above the third bar's label "fails — evidence, not size of bend".
- **Takeaway (bold 13px `#d55181`, centered at y=290):** "p measures evidence against normal, not how normal the data is".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (here it hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- All data arrays are hardcoded literals (no `Math.random()`); the weights and waits arrays are shared between c1 and c2.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
