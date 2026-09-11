# Dummy & Contrast Coding

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Dummy & Contrast Coding

**Subtitle:** Regression only eats numbers — dummy coding turns a category like "neighborhood" into 0/1 columns, with one level left out as the baseline the others are compared to

## Turning Neighborhoods into Numbers

**Tags:** `core idea` (blue), `dummy variables` (green), `baseline level` (orange)

- **The listings** — six apartments: two Downtown ($2,300, $2,500), two Midtown ($2,000, $2,200), two Suburbs ($1,400, $1,600)
- **The problem** — a regression predicting rent cannot multiply a slope by the word "Downtown"
- **The fix** — make a 0/1 column per neighborhood: D_downtown is 1 for Downtown rows, else 0
- **Leave one out** — with 3 neighborhoods you keep only 2 columns; Suburbs gets no column
- **The baseline** — a Suburbs row is all zeros, so Suburbs becomes the model's reference level

*Example (italic):* The $2,300 Downtown apartment becomes the row (D_downtown=1, D_midtown=0); the $1,400 Suburbs apartment becomes (0, 0).

**Key point:** Dummy coding replaces one category column with K−1 zero/one columns. The left-out level is not lost — it lives in the intercept as the baseline.

### Visualization (canvas `c1`, 720×300)

Before/after table diagram: the original 6-row listings table on the left, an arrow, and the dummy-coded design matrix on the right, with the all-zero Suburbs rows highlighted.

- **Title (bold 15px, `#1a5276`, top center):** "One Text Column Becomes Two 0/1 Columns".
- **Data (6 rows):** neighborhoods `["Downtown","Downtown","Midtown","Midtown","Suburbs","Suburbs"]`, rents `["$2,300","$2,500","$2,000","$2,200","$1,400","$1,600"]`, D_downtown `[1,1,0,0,0,0]`, D_midtown `[0,0,1,1,0,0]`.
- **Left table:** starts x=40, header row at y=60 bold 12px `#1a5276` with columns "neighborhood" (x=40) and "rent" (x=160); 6 data rows 12px `#444` at y=85 stepping +30; light grid lines 1px `#e5e9ef` under each row from x=35 to x=225.
- **Arrow:** blue `#2a78d6` 3px arrow from x=245 to x=305 at y=165, with bold 12px blue label "dummy code" above it at y=150.
- **Right table:** starts x=330, header row at y=60 bold 12px `#1a5276` with columns "rent" (x=330), "D_downtown" (x=420), "D_midtown" (x=540); same 6 rows of 12px values; 1s drawn bold green `#008300`, 0s in `#6b7280`; grid lines from x=325 to x=650.
- **Highlight:** the two Suburbs rows (y=235 and y=265) get a `rgba(217,89,38,0.12)` background rectangle from x=325 to x=650, 24px tall each.
- **Annotation (bold 12px orange `#d95926`, centered at x=490, y=295):** "Suburbs rows are all zeros — the left-out baseline".

## Reading the Fitted Coefficients

**Tags:** `worked example` (blue), `intercept` (green)

- **The fit** — least squares on the six rows gives: rent = 1500 + 900·D_downtown + 600·D_midtown
- **Intercept** — 1500 is exactly the Suburbs mean: (1400 + 1600) / 2 = 1500
- **Downtown coef** — +900 is the gap between means: Downtown 2400 minus Suburbs 1500
- **Midtown coef** — +600 is Midtown 2100 minus Suburbs 1500, not Midtown's own average
- **Predictions** — plug in (1,0): 1500 + 900 = 2400; plug in (0,0): just the intercept, 1500

*Example (italic):* A colleague reads "+600" as Midtown rent — the real Midtown average is 1500 + 600 = $2,100.

**Key point:** With dummy coding, every coefficient is a difference from the left-out baseline, and the intercept is the baseline's own mean. Nothing is "the group's value" except the intercept.

### Visualization (canvas `c2`, 720×300)

Bar chart of the three neighborhood mean rents with a dashed baseline line at the Suburbs mean and labeled offset arrows showing each coefficient as a jump from that line.

- **Title (bold 15px, `#1a5276`, top center):** "rent = 1500 + 900·Downtown + 600·Midtown".
- **Data:** groups `["Suburbs","Midtown","Downtown"]`, means `[1500, 2100, 2400]`.
- **Layout:** axis origin x=70, baseline y=245, chart height 190, y scale 0–2,600; y ticks at 0, 500, 1000, 1500, 2000, 2500 with 12px `#6b7280` labels and 1px `#e5e9ef` gridlines.
- **Bars:** 90px wide, centered at x=160, 360, 560; Suburbs fill `rgba(42,120,214,0.45)` with 2px `#2a78d6` border, Midtown fill `rgba(0,131,0,0.35)` with 2px `#008300` border, Downtown fill `rgba(74,58,167,0.3)` with 2px `#4a3aa7` border; mean value bold 13px in each bar's border color above the bar ("$1,500", "$2,100", "$2,400"); group names 12px `#444` below baseline.
- **Baseline line:** dashed `#2a78d6` (dash 5/4) horizontal line at the y of 1500 across x=70 to 660, bold 12px blue label "intercept = 1500 (Suburbs mean)" at x=75, 6px above the line.
- **Offset arrows:** vertical 3px arrows from the baseline line up to each bar top — green `#008300` at x=360 labeled bold 13px green "+600" beside it; violet `#4a3aa7` at x=560 labeled bold 13px violet "+900".
- **Caption (12px `#444`, bottom center y=295):** "each coefficient = that group's mean minus the baseline mean".

## Contrast Coding: Changing the Question

**Tags:** `where it's used` (blue), `effect coding` (orange)

- **Same fit, new labels** — recoding the 0/1 columns changes what the coefficients mean, not the fit
- **Effect coding** — use +1 / 0 / −1 instead of 0/1; the intercept becomes the grand mean of groups
- **Grand mean** — (2400 + 2100 + 1500) / 3 = 2000, the unweighted average of the three means
- **Deviations** — coefficients read as offsets from 2000: Downtown +400, Midtown +100, Suburbs −500
- **Pick by question** — "vs a control group" wants dummies; "vs the overall average" wants effects

*Example (italic):* HR comparing three offices to the company-wide average uses effect coding, so "+400" means $400 above the grand mean, not above one office.

**Key point:** Coding schemes are lenses on the same model: predictions never change, but the intercept and coefficients answer different comparisons. Choose the coding whose comparison you actually want to report.

### Visualization (canvas `c3`, 720×300)

Dual-panel view of the same three means, split by a vertical dashed divider at x=360: dummy-coding lens (left, offsets from the Suburbs baseline) vs effect-coding lens (right, deviations from the grand mean).

- **Title (bold 15px, `#1a5276`, top center):** "Same Three Means, Two Codings, Two Stories".
- **Data (both panels):** groups `["Sub","Mid","Down"]`, means `[1500, 2100, 2400]`.
- **Left panel (dummy lens):** heading bold 12px `#2a78d6` "dummy: compare to Suburbs" at x=190 centered, y=48; axis origin x=55, panel width 280, baseline y=245, chart height 175, y scale 1,200–2,600; bars 60px wide centered at x=105, 190, 275, fill `rgba(42,120,214,0.4)` with 2px `#2a78d6` border; dashed blue reference line at 1500 across the panel labeled 11px "baseline 1500"; bold 12px blue offset labels above bars: "0", "+600", "+900".
- **Right panel (effect lens):** heading bold 12px `#d95926` "effect: compare to grand mean" at x=530 centered, y=48; axis origin x=400, same width/baseline/height/scale; bars same positions shifted (+345px: x=450, 535, 620), fill `rgba(217,89,38,0.35)` with 2px `#d95926` border; dashed orange reference line at 2000 labeled 11px "grand mean 2000"; bold 12px orange deviation labels above bars: "−500", "+100", "+400".
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=360 from y=38 to h-12.
- **Caption (12px `#444`, bottom center y=295):** "identical bars and predictions — only the reference line the coefficients measure from moves".

## The Dummy Trap

**Tags:** `common mistake` (red), `multicollinearity` (orange)

- **The temptation** — keeping all three columns D_downtown, D_midtown, D_suburbs "to be fair"
- **The identity** — in every row the three dummies sum to exactly 1, same as the intercept column
- **The break** — the software hits perfect collinearity: it errors out or silently drops a column
- **No baseline, no meaning** — with all K dummies, "difference from what?" has no single answer
- **The rule** — K categories get K−1 dummies when the model has an intercept, always

*Example (italic):* Adding D_suburbs to the rent model makes D_downtown + D_midtown + D_suburbs = 1 in all six rows — an exact copy of the intercept column.

**Common mistake:** Including a dummy for every level alongside the intercept. The columns then add up to the intercept exactly, and the regression has no unique solution — leave one level out.

### Visualization (canvas `c4`, 720×300)

Design-matrix diagram: the intercept column plus all three dummy columns for the six rows, with a per-row sum column showing every row's dummies total 1, flagged as a perfect copy of the intercept.

- **Title (bold 15px, `#1a5276`, top center):** "All Three Dummies + Intercept = Perfect Collinearity".
- **Data (6 rows):** intercept `[1,1,1,1,1,1]`, D_downtown `[1,1,0,0,0,0]`, D_midtown `[0,0,1,1,0,0]`, D_suburbs `[0,0,0,0,1,1]`, sum `[1,1,1,1,1,1]`.
- **Column headers (bold 12px, y=70):** "intercept" `#1a5276` at x=110, "D_down" `#4a3aa7` at x=230, "D_mid" `#008300` at x=330, "D_sub" `#d95926` at x=430, "sum of dummies" `#e74c3c` at x=575; all centered on their columns.
- **Rows:** 6 rows of 13px values at y=100 stepping +28, centered under each header; 1s bold in the column's header color, 0s in `#6b7280`; light 1px `#e5e9ef` rule under each row from x=60 to x=650.
- **Plus/equals signs:** 14px `#6b7280` "+" between the dummy columns at x=180, 280, 380 (each row's y), "=" at x=495.
- **Highlight boxes:** 2px `#1a5276` rounded rectangle around the intercept column (x=80–140, y=82–275); 2px `#e74c3c` rounded rectangle around the sum column (x=545–605, y=82–275).
- **Annotation (bold 13px red `#e74c3c`, two lines, right side x=612, y=150/166, left-aligned):** "identical" / "columns!" with a 2px red connector line from the sum box to the intercept box across the top at y=78.
- **Caption (12px `#444`, bottom center y=295):** "the model cannot tell the intercept from the dummy total — drop one dummy to break the tie".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (here it hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- All data is hardcoded literal arrays (six apartment rows and derived means); no randomness. In regenerated HTML, any card links would use `.html` extensions (this page has no links).
