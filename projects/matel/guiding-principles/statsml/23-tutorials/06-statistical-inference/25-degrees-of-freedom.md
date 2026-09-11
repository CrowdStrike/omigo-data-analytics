# Degrees of Freedom

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Degrees of Freedom

**Subtitle:** Once a total or an average is fixed, not every value is free anymore — degrees of freedom counts how many still are, and that count sets the shape of every t table and chi-square test

## Five Coffees, One Locked Order

**Tags:** `core idea` (blue), `constraint` (green), `df = n − 1` (orange)

- **The coffee run** — five friends order coffee and the receipt shows the total is exactly $20
- **The average** — a $20 total across 5 people fixes the average order at $4 before anyone speaks
- **Four free picks** — Ana $3, Ben $5, Cam $2, Dee $6 can be anything; that is $16 of the $20
- **One forced value** — Eli's order must be $20 − $16 = $4; the fixed total leaves him no choice
- **The definition** — df counts the values still free to vary after the constraints: 5 − 1 = 4

*Example (italic):* Tell me any four of the five orders plus the $20 total, and I can always name the fifth — it was never free.

**Key point:** One fixed total (one constraint) removes exactly one degree of freedom: n values with a known mean have only n − 1 free.

### Visualization (canvas `c1`, 720×300)

Bar chart of the five coffee orders; the first four bars are free choices, the fifth is forced by the fixed $20 total, with a dashed average line at $4.

- **Title (bold 15px, `#1a5276`, top center):** "Five Coffee Orders, Total Fixed at $20".
- **Data:** names `["Ana", "Ben", "Cam", "Dee", "Eli"]`; orders `[3, 5, 2, 6, 4]` dollars.
- **Axes:** origin x=70, baseline y=245, chart height 185, plot width 540; y scale 0–7 dollars with gridlines `#e5e9ef` and 12px `#444` tick labels at $0, $2, $4, $6.
- **Bars:** width 62px, evenly spaced; Ana–Dee filled `rgba(42,120,214,0.45)` with 2px blue `#2a78d6` border; Eli filled `rgba(217,89,38,0.35)` with 2px dashed orange `#d95926` border; dollar value bold 13px above each bar (blue for the four free, orange for Eli).
- **Free labels:** 12px `#6b7280` "free" under each of the first four names; bold 12px orange "forced" under Eli's name.
- **Average line:** dashed (dash 5/4) green `#008300` horizontal line at $4 across the plot, right-end label bold 12px green "average = $4".
- **Annotation (bold 13px orange `#d95926`, upper right):** two lines "total $20 is fixed →" / "Eli must be $20 − $16 = $4".
- **Takeaway (bold 13px `#1a5276`, bottom center):** "4 free values + 1 forced value → df = 4".

## Why the Variance Divides by n − 1

**Tags:** `worked example` (blue), `sum to zero` (green), `n − 1` (orange)

- **Deviations** — subtracting the $4 mean gives −1, +1, −2, +2, 0, and these always sum to 0
- **The constraint** — because deviations must sum to 0, the fifth is fixed once four are known
- **Squared spread** — the squared deviations are 1, 1, 4, 4, 0, which add up to 10
- **Divide by 4** — sample variance is 10 ÷ 4 = 2.5; it divides by df, not by the count of values
- **Divide by 5** — 10 ÷ 5 = 2.0 underestimates, because the mean was taken from the same data

*Example (italic):* The five coffees have sample variance 10 ÷ 4 = 2.5; dividing by 5 would claim a too-tidy 2.0.

**Key point:** Estimating the mean spends one degree of freedom, so only n − 1 = 4 independent pieces of spread information remain — dividing by 4 keeps the variance honest.

### Visualization (canvas `c2`, 720×300)

Dual panel split by a vertical dashed divider at x=400: deviations-from-the-mean lollipop chart (left) and a two-bar comparison of dividing by 5 vs by 4 (right).

- **Title (bold 15px, `#1a5276`, top center):** "Deviations Sum to Zero — So Divide the Squares by 4, Not 5".
- **Left panel (deviations):** zero line 2px `#999` at y=150 from x=55 to x=365; deviations `[-1, 1, -2, 2, 0]` for Ana, Ben, Cam, Dee, Eli; scale ±2.5 mapped to ±85px; 3px stems with 6px dots, blue `#2a78d6` for the four nonzero, orange `#d95926` for Eli's 0; signed value bold 12px at each dot tip; names 12px `#444` below the panel; green `#008300` bold 12px annotation "the five deviations always add to 0"; caption 12px `#444` "deviations from the $4 mean".
- **Right panel (two estimates):** axis origin x=445, baseline y=245, chart height 170, y scale 0–3; two bars 70px wide: "÷ 5 → 2.0" filled `rgba(213,81,129,0.45)` with 2px magenta `#d55181` border, "÷ 4 → 2.5" filled `rgba(0,131,0,0.4)` with 2px green `#008300` border; values 2.0 and 2.5 bold 13px above the bars; labels "÷ n = 5" and "÷ df = 4" 12px `#444` below; magenta bold 12px annotation "too small" by the left bar, green bold 12px "unbiased" by the right bar.
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=400 from y=38 to h-12.

## Where df Shows Up: the t Table

**Tags:** `where it's used` (blue), `t-distribution` (green), `fat tails` (orange)

- **A curve family** — the t-distribution is a family of bell curves, one curve for each df
- **Fat tails** — at df = 4 the peak is only 0.375 and the tails stay fat well past ±3
- **Near normal** — by df = 30 the peak is ≈0.396, within 1% of the normal's 0.399
- **Wider cutoffs** — the 95% cutoff is ±2.776 at df = 4 but ±2.042 at df = 30 (normal: ±1.96)
- **Practical effect** — a t-interval on the 5 coffees uses df = 4, about 42% wider than normal

*Example (italic):* A t-test on the five coffee orders looks up its cutoff in the df = 4 row of the t table, not under the normal curve.

**Key point:** df tells the test how much data really backs the estimate — fewer free values mean fatter tails, bigger cutoffs, and wider intervals.

### Visualization (canvas `c3`, 720×300)

Two overlaid t-density curves (df = 4 vs df = 30) on a shared axis, with dashed vertical 95% cutoff lines showing how low df pushes the cutoff outward.

- **Title (bold 15px, `#1a5276`, top center):** "The t-Distribution: df = 4 vs df = 30".
- **Data:** x from −4 to 4 in steps of 0.5 (17 points); df = 4 density `[0.007, 0.011, 0.020, 0.036, 0.066, 0.123, 0.215, 0.322, 0.375, 0.322, 0.215, 0.123, 0.066, 0.036, 0.020, 0.011, 0.007]`; df = 30 density `[0.001, 0.001, 0.005, 0.018, 0.055, 0.130, 0.240, 0.352, 0.396, 0.352, 0.240, 0.130, 0.055, 0.018, 0.005, 0.001, 0.001]` (values rounded to 3 decimals; label curves in-chart).
- **Axes:** x maps −4..4 to pixels 70..650; baseline y=250; density 0–0.42 maps to chart height 190; x tick labels −4, −2, 0, 2, 4 in 12px `#444`.
- **Curves:** df = 30 as 3px blue `#2a78d6` smooth polyline; df = 4 as 3px magenta `#d55181` polyline; bold 13px labels near each peak: blue "df = 30 (peak 0.396)", magenta "df = 4 (peak 0.375)".
- **Cutoff lines:** dashed (dash 5/4) orange `#d95926` verticals at x = ±2.776 with bold 12px orange label "df = 4 cutoff ±2.776"; dashed aqua `#199e70` verticals at x = ±2.042 with bold 12px aqua label "df = 30 cutoff ±2.042".
- **Annotation (bold 13px magenta `#d55181`, right tail region):** "fat tails: rare values are less surprising with df = 4".
- **Caption (12px `#444`, bottom center):** "95% two-sided cutoffs; the normal curve's is ±1.96".

## The Common Confusion: df Is Not Sample Size

**Tags:** `common mistake` (red), `contingency table` (blue), `counting constraints` (green)

- **Not the count** — df is the number of values minus the constraints, so it depends on the analysis
- **Two means** — a two-sample t-test on 5 + 5 coffees estimates two means, leaving df = 8, not 10
- **Tables too** — a 2×3 order table with fixed row and column totals has df = (2−1)(3−1) = 2
- **Fill it in** — choose weekday espresso = 20 and weekday latte = 30; the totals force the other 4 cells
- **Regression** — fitting an intercept and a slope to 5 points spends 2 df, leaving 3 for residuals

*Example (italic):* In the 2×3 table below, picking just 2 of the 6 cells locks the remaining 4 — the row and column totals do the rest.

**Common mistake:** Reading df as "how many data points I have" — every estimated parameter or fixed total removes one, and formulas like n − 1 or (r−1)(c−1) simply count what is left.

### Visualization (canvas `c4`, 720×300)

A 2×3 contingency table of coffee-shop orders drawn as a grid with row and column totals, coloring the 2 free cells green and the 4 forced cells gray.

- **Title (bold 15px, `#1a5276`, top center):** "A 2×3 Table with Fixed Totals: Only 2 Cells Are Free".
- **Data:** rows weekday/weekend, columns espresso/latte/tea; cells weekday `[20, 30, 10]`, weekend `[10, 20, 10]`; row totals `[60, 40]`; column totals `[30, 50, 20]`; grand total 100.
- **Grid:** table origin x=90, y=70; cell width 110, cell height 48; 1px `#999` cell borders; column headers "espresso", "latte", "tea", "total" bold 12px `#1a5276` above; row labels "weekday", "weekend", "total" bold 12px `#1a5276` at left.
- **Free cells:** weekday-espresso (20) and weekday-latte (30) filled `rgba(0,131,0,0.15)` with 2px green `#008300` border, values bold 14px green, small 11px green tag "free" in the cell corner.
- **Forced cells:** weekday-tea (10) and all three weekend cells (10, 20, 10) filled `#f0f2f5`, values 14px `#6b7280`, small 11px `#6b7280` tag "forced".
- **Totals:** totals row and column values bold 14px ink `#1a5276` on white; grand total 100 bold.
- **Annotation (bold 13px green `#008300`, right of the grid):** two lines "pick 2 cells, totals force the rest" / "df = (2−1) × (3−1) = 2".
- **Caption (12px `#444`, bottom center):** "orders at a coffee shop, illustrative counts".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (here it hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
