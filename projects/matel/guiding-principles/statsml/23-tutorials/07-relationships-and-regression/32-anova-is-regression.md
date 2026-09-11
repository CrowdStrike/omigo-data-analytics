# ANOVA Is Regression

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** ANOVA Is Regression

**Subtitle:** Code each group as a 0/1 dummy column, run a plain regression, and ANOVA's F-test falls out — they were the same model all along

## Three Couriers, One Question

**Tags:** `core idea` (blue), `group means` (green), `between vs within` (orange)

- **The setup** — a delivery firm times 5 deliveries for each of three couriers: A, B, and C
- **The means** — courier A averages 30 minutes, B averages 40, C averages 26; the grand mean is 32
- **The question** — are the couriers genuinely different, or is this just day-to-day noise?
- **ANOVA's answer** — compare the spread between the three means to the spread within each courier
- **The secret** — the same question can be asked as a regression, and it returns the same answer

*Example (italic):* Courier B's five times (38, 40, 42, 39, 41) don't overlap A's (28–32) at all — the gap looks too big to be luck.

**Key point:** ANOVA asks "do the group means differ by more than noise explains?" — hold that question; a regression is about to ask it too.

### Visualization (canvas `c1`, 720×300)

Dot strip plot: the 15 delivery times in three vertical columns (one per courier), each with a solid group-mean bar, plus a dashed grand-mean line across the chart.

- **Title (bold 15px, `#1a5276`, top center):** "15 Deliveries, 3 Couriers: Are the Means Really Different?".
- **Data:** courier A `[28, 30, 32, 29, 31]` (mean 30), courier B `[38, 40, 42, 39, 41]` (mean 40), courier C `[24, 26, 28, 25, 27]` (mean 26); grand mean 32.
- **Layout:** y axis "minutes" mapping 20–45 onto y=250 (bottom) to y=50 (top), 8px per minute; y ticks 20/25/30/35/40/45 at x=55 (12px `#444`); courier columns centered at x=200 (A), x=400 (B), x=600 (C) with labels "Courier A" / "Courier B" / "Courier C" bold 13px below y=250.
- **Dots:** 6px radius, jittered ±14px horizontally around each column center (fixed offsets `[-14, -7, 0, 7, 14]`); A blue `#2a78d6`, B orange `#d95926`, C aqua `#199e70`.
- **Mean bars:** 3px horizontal line, 70px wide, centered on each column at the group mean, in the group's color, labeled bold 12px "mean 30" / "mean 40" / "mean 26" to the right of each bar.
- **Grand mean:** dashed magenta `#d55181` (dash 5/4) horizontal line at 32 from x=90 to x=680, labeled bold 12px magenta "grand mean 32" near the left end.
- **Caption (12px `#444`, bottom right):** "5 timed deliveries per courier (illustrative)".

## Turning Couriers into 0/1 Columns

**Tags:** `worked example` (blue), `dummy variables` (green)

- **Dummy columns** — make two 0/1 columns, is_B and is_C; courier A is the row of all zeros
- **Why two, not three** — with A as the baseline, the pair (is_B, is_C) already covers all three cases
- **The regression** — fit minutes = b0 + b1·is_B + b2·is_C on the same 15 deliveries
- **The coefficients** — the fit returns b0 = 30, b1 = +10, b2 = −4, and nothing else
- **Read them** — b0 is A's mean; b1 is B minus A (40 − 30); b2 is C minus A (26 − 30)

*Example (italic):* Plug in a courier B row (is_B = 1, is_C = 0): the prediction is 30 + 10 = 40, exactly B's group mean.

**Key point:** A regression on dummy columns can only predict one number per group, so its best fit IS the three group means — the very model ANOVA tests.

### Visualization (canvas `c2`, 720×300)

Two-panel figure split by a dashed divider at x=360: the dummy-coded design matrix (left) and a "prediction ladder" showing how the coefficients rebuild the three group means (right).

- **Title (bold 15px, `#1a5276`, top center):** "Two 0/1 Columns Encode Three Couriers".
- **Left panel (design matrix):** table drawn on canvas from x=45 to x=330, header row at y=70 bold 12px `#1a5276`: "courier | minutes | is_B | is_C"; six data rows (22px apart, 12px `#444`): A 28 0 0, A 30 0 0, B 38 1 0, B 40 1 0, C 24 0 1, C 26 0 1; row text colored by courier (A blue `#2a78d6`, B orange `#d95926`, C aqua `#199e70`); light grid lines `#e5e9ef` between rows; caption 11px `#666` below: "… 15 rows total, 5 per courier".
- **Right panel (prediction ladder):** y maps 20–45 onto y=250 to y=60; three horizontal 3px lines from x=430 to x=560: blue at 30, orange at 40, aqua at 26; labels to the right bold 12px in matching colors: "b0 = 30 (A's mean)", "b0 + b1 = 40 (B)", "b0 + b2 = 26 (C)".
- **Arrows:** from the blue line at 30, an orange 2px arrow up to 40 at x=470 labeled bold 12px "+10", and an aqua 2px arrow down to 26 at x=520 labeled bold 12px "−4".
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=360 from y=45 to y=285.
- **Takeaway (bold 12px violet `#4a3aa7`, bottom of right panel):** "the fit can only output group means".

## The Reveal: One Computation, Two Printouts

**Tags:** `the unification` (blue), `worked example` (green), `where it's used` (orange)

- **Split the spread** — total squared spread is 550: 520 between couriers, 30 within them
- **ANOVA's F** — F = (520/2) / (30/12) = 260 / 2.5 = 104, a huge between-to-within ratio
- **Regression's F** — the regression's overall F-test on b1 and b2 computes the identical 104
- **Same everything** — model SS 520 = between SS; residual SS 30 = within SS; identical p-value
- **R² for free** — the regression also reports R² = 520/550 = 0.945: courier explains 94.5% of spread

*Example (italic):* Run `aov()` and `lm()` on the same 15 rows — both print F = 104 on 2 and 12 degrees of freedom.

**Key point:** ANOVA is not a cousin of regression — it IS regression with dummy predictors, wearing a different output format.

### Visualization (canvas `c3`, 720×300)

Two identical stacked bars — one labeled as an ANOVA table, one as a regression output — with an equals sign between them showing the decomposition is the same computation.

- **Title (bold 15px, `#1a5276`, top center):** "Same 550, Same Split, Same F".
- **Data:** total SS 550 = explained 520 + leftover 30 in both bars.
- **Bars:** two vertical stacked bars, 110px wide, baseline y=250, total height 180px scaled to SS 550; left bar centered x=200, right bar centered x=520.
- **Left bar (ANOVA):** bottom segment "between groups = 520" fill `rgba(42,120,214,0.5)` with 2px `#2a78d6` border, height 520/550 of 180 ≈ 170px; top segment "within groups = 30" fill `rgba(213,81,129,0.55)`, ~10px; segment labels bold 12px to the left of the bar in matching colors; heading below bar bold 13px `#1a5276`: "ANOVA table".
- **Right bar (regression):** identical geometry; bottom segment "model (dummies) = 520" same blue fill, top segment "residual = 30" same magenta fill; labels bold 12px to the right; heading below: "regression output".
- **Center annotation:** bold 26px `#008300` "=" at (360, 160); under it bold 13px green `#008300`, two lines: "F = 104 on 2 and 12 df" / "both ways".
- **Callout (bold 12px magenta `#d55181`, top right):** "only 30 of 550 left unexplained".
- **Caption (12px `#444`, bottom center):** "between SS = model SS, within SS = residual SS — different names, one number".

## Coefficients Are Gaps, Not Means

**Tags:** `common mistake` (red), `rule of thumb` (green)

- **The trap** — b1 = 10 is NOT courier B's mean; it is B's gap above the baseline courier A
- **Baseline choice** — rebase on C and the coefficients become 26, +4, +14; the F stays 104
- **The dummy trap** — adding a third column is_A duplicates information and the fit breaks
- **t vs F** — each coefficient's t-test compares one pair; the F tests all three groups at once
- **It scales** — two-way ANOVA is just regression with two dummy sets plus their interactions

*Example (italic):* An analyst reported "courier B takes 10 minutes" after reading b1 = 10 — B actually takes 40, which is 10 more than A.

**Common mistake:** Reading a dummy coefficient as a group's mean. Only the intercept is a mean; every other coefficient is a difference from the baseline group.

### Visualization (canvas `c4`, 720×300)

Two-panel bar chart split by a dashed divider at x=360: the three group means (left) versus the three regression numbers (right), where b2 dips below a zero line.

- **Title (bold 15px, `#1a5276`, top center):** "Group Means vs Regression Coefficients (baseline = A)".
- **Left panel (means):** baseline y=245, scale 4px per minute from 0; bars 50px wide centered at x=110, x=190, x=270: A = 30 blue `rgba(42,120,214,0.5)`, B = 40 orange `rgba(217,89,38,0.5)`, C = 26 aqua `rgba(25,158,112,0.5)`, each with a 2px solid border in the full-strength color; value labels bold 12px above each bar ("30", "40", "26"); names 12px `#444` below; heading bold 13px `#1a5276` above panel: "what each courier averages".
- **Right panel (coefficients):** zero line 2px `#999` at y=200 from x=400 to x=680 labeled "0" at its left; scale 4px per unit; bars 50px wide centered at x=460, x=540, x=620: b0 = 30 up (blue), b1 = +10 up (orange), b2 = −4 down (aqua), same fills/borders as left; labels bold 12px "b0 = 30", "b1 = +10", "b2 = −4" above (or below the b2 bar); heading bold 13px `#1a5276`: "what the regression prints".
- **Annotation (bold 12px magenta `#d55181`, right panel, two lines):** "only b0 is a mean —" / "b1, b2 are gaps from A".
- **Guide:** dashed orange (dash 4/3) 1px line linking the top of the B = 40 bar to the top of the b1 bar, labeled 11px `#d95926` "10 = 40 − 30".
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=360 from y=40 to y=288.

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data integrity:** all arrays hardcoded exactly as specified above (no `Math.random()`); the worked-example numbers in the text (means 30/40/26, grand mean 32, SS 520/30/550, F = 104, R² = 0.945, coefficients 30/+10/−4) must match the chart data.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
