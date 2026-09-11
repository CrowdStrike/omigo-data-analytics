# Shapley Values

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Shapley Values

**Subtitle:** Split a team's payout by averaging what each member adds across every possible join order — the same math SHAP uses to give features credit for a model's prediction

## Three Friends, One Pile of Lemonade Money

**Tags:** `core idea` (blue), `marginal contribution` (green), `teamwork` (orange)

- **The stand** — Ana, Ben, and Cara run a lemonade stand; the weekend ends with $180 to split
- **Alone** — solo weekends earn Ana $60, Ben $30, Cara $30 — just $120 combined
- **Together** — pairs beat their parts: Ana+Ben $120, Ana+Cara $100, Ben+Cara $80
- **Teamwork bonus** — the trio's $180 tops the $120 solo total; who earned the extra $60?
- **Marginal contribution** — what you add by joining a group that formed before you arrived

*Example (italic):* Ben joining Ana lifts profit from $60 to $120 — his marginal contribution there is $60, double his solo $30.

**Key point:** When teamwork creates extra value, solo earnings can't split the total fairly — you need what each member adds to every group they could join.

### Visualization (canvas `c1`, 720×300)

Vertical bar chart of every coalition's weekend earnings: three solos, three pairs, and the full trio, with a dashed reference line at the $120 solo total.

- **Title (bold 15px, `#1a5276`, top center):** "What Each Group Earns in a Weekend (illustrative)".
- **Data:** labels `['Ana', 'Ben', 'Cara', 'Ana+Ben', 'Ana+Cara', 'Ben+Cara', 'All three']` with values `[60, 30, 30, 120, 100, 80, 180]`.
- **Axes:** origin x=60, plot width 630, baseline y=240, chart height 175, y scale 0–190; 1px `#999` L-shaped axis.
- **Bars:** 7 slots of width 630/7, bars 58px wide centered in each slot; solos fill `rgba(42,120,214,0.5)`, pairs fill `rgba(25,158,112,0.5)`, trio fill `rgba(74,58,167,0.55)`; bold 12px `#444` "$N" value label above each bar; 12px `#444` group label below baseline.
- **Reference line:** dashed `#bdc3c7` (dash 4/3) horizontal line at the $120 level, labeled bold 12px mute `#6b7280` "solo total $120" (left-aligned just above it).
- **Annotation (bold 13px green `#008300`, right-aligned near top right):** "teamwork adds $60 on top of the solos".
- **Caption (12px `#444`, bottom center):** "solos (blue)   ·   pairs (teal)   ·   trio (violet)".

## Walking All Six Join Orders by Hand

**Tags:** `worked example` (blue), `all 6 orders` (green)

- **Join orders** — three players can assemble in 3! = 6 orders; write one row per order
- **One row** — Ben→Ana→Cara: Ben adds $30, Ana lifts it to $120 (+$90), Cara to $180 (+$60)
- **Average** — a player's Shapley value is their marginal contribution averaged over all 6 rows
- **Result** — Ana $80, Ben $55, Cara $45 — and 80 + 55 + 45 = 180, the whole profit, exactly
- **Position matters** — Ana adds $100 joining last but only $60 joining first; averaging is fair

*Example (italic):* Redo row 4 yourself: Ben starts at $30, Cara lifts it to $80 (+$50), Ana lifts it to $180 (+$100).

**Key point:** The averages always sum to exactly the total payout — every dollar is handed out once, none invented, none lost. Each row sums to $180 too.

### Visualization (canvas `c2`, 720×300)

Canvas-drawn table: six rows (one per join order) with each player's marginal contribution, plus a highlighted averages row and sum annotations.

- **Title (bold 15px, `#1a5276`, top center):** "Marginal Contribution in Each of the 6 Join Orders ($)".
- **Columns:** order label left-aligned at x=60; number columns centered at x=385 (Ana), x=490 (Ben), x=595 (Cara). Header row at y=50: "join order" bold 13px ink; player names bold 13px in their colors — Ana blue `#2a78d6`, Ben aqua `#199e70`, Cara orange `#d95926`; 1px `#999` rule under the header from x=60 to x=660.
- **Row data (13px `#444`, rows at y=78 stepping 24px):**
  - `1. Ana → Ben → Cara` — 60, 60, 60
  - `2. Ana → Cara → Ben` — 60, 80, 40
  - `3. Ben → Ana → Cara` — 90, 30, 60
  - `4. Ben → Cara → Ana` — 100, 30, 50
  - `5. Cara → Ana → Ben` — 70, 80, 30
  - `6. Cara → Ben → Ana` — 100, 50, 30
- **Averages row:** highlight band `rgba(26,82,118,0.08)` from y=208, 616px wide, 28px tall; bold 13px ink label "average of the 6 = Shapley value"; averages "$80", "$55", "$45" bold 13px in each player's color. (Column sums 480/330/270 divided by 6; every row also sums to 180.)
- **Annotations:** bold 13px green `#008300` centered at y=262: "80 + 55 + 45 = 180 — the shares hand out the whole profit"; 12px mute `#6b7280` centered at y=284: "check any row: the three contributions in it also sum to 180".

## What Makes This the Fair Split

**Tags:** `fairness rules` (blue), `comparison` (orange)

- **Adds up** — the shares sum to the full $180; the split never leaks or mints money
- **Equal work, equal pay** — players who add the same to every group get the same share
- **No work, no pay** — a player who adds $0 to every group gets a share of exactly $0
- **Solo pay misleads** — Ben and Cara both earn $30 alone, yet Ben gets $55, Cara $45
- **Why the gap** — Ben pairs better with Ana ($120 vs $100), and the join orders reward it
- **Only split** — add one rule, consistency across games (additivity), and Shapley's is unique

*Example (italic):* An equal $60-each split overpays Cara; splitting by solo pay ($90/$45/$45) ignores Ben's teamwork with Ana.

**Key point:** Shapley values are the unique way to divide a joint payout that adds up exactly, pays equal contributors equally, and gives non-contributors nothing — once you also require consistency across games (additivity).

### Visualization (canvas `c3`, 720×300)

Grouped bar chart comparing three splits of the same $180: equal split, proportional to solo pay, and Shapley.

- **Title (bold 15px, `#1a5276`, top center):** "Three Ways to Split the Same $180".
- **Legend (y≈38–50, starting x=200, 110px apart):** 14px swatches with 12px `#444` labels — Ana `rgba(42,120,214,0.55)`, Ben `rgba(25,158,112,0.55)`, Cara `rgba(217,89,38,0.55)`.
- **Data:** methods `['equal split', 'by solo pay', 'Shapley']` with per-player shares `[60, 60, 60]`, `[90, 45, 45]`, `[80, 55, 45]` (each sums to 180).
- **Axes:** origin x=60, plot width 630, baseline y=235, chart height 155, y scale 0–100; 1px `#999` L-shaped axis.
- **Bars:** three clusters centered in thirds of the plot width; within a cluster three 44px-wide bars with 10px gaps, filled with the legend colors; bold 12px "$N" value label above each bar in the player's solid color (Ana `#2a78d6`, Ben `#199e70`, Cara `#d95926`); method name bold 13px `#444` below the baseline.
- **Takeaway (bold 13px magenta `#d55181`, centered at y=282):** "all three hand out $180 — only Shapley prices each player's teamwork".

## From Lemonade to SHAP — Credit Is Not Cause

**Tags:** `where it's used` (blue), `SHAP` (green), `common mistake` (red)

- **Same math** — SHAP treats a model's features as players and one prediction as the payout
- **The payout** — the split explains the gap between the average prediction and this one
- **House model** — base $300k, house $370k: size +$55k, neighborhood +$40k, age −$20k, garage −$5k
- **Adds up again** — +55 +40 −20 −5 = +70, exactly the gap from $300k to $370k
- **Credit, not cause** — SHAP shows what the model leaned on, not what changing it would do
- **Correlation trap** — a feature can earn big credit just by riding along with the real driver

*Example (italic):* Renovating won't add $40k — "neighborhood +$40k" is credit inside the model, likely riding on school quality.

**Common mistake:** Reading a Shapley/SHAP value as a causal effect. It is credit attribution for a prediction — a correlated feature can collect large credit while causing nothing.

### Visualization (canvas `c4`, 720×300)

SHAP-style horizontal bar chart for one house-price prediction: four feature contributions diverging from a vertical zero line that represents the base (average) prediction.

- **Title (bold 15px, `#1a5276`, top center):** "How SHAP Splits One House Prediction (illustrative)".
- **Equation line (bold 13px green `#008300`, centered at y=46):** "prediction $370k = base $300k + 55 + 40 − 20 − 5".
- **Data:** features `['size', 'neighborhood', 'house age', 'no garage']` with contributions `[+55, +40, −20, −5]` ($k).
- **Zero line:** 2px `#999` vertical line at x=380 from y=60 to y=232; label bold 12px mute `#6b7280` centered below at y=248: "base (average prediction) $300k".
- **Bars:** rows at y=70 stepping 42px, bar height 24, length 2.6px per $k; positive bars extend right filled `rgba(42,120,214,0.55)`, negative bars extend left filled `rgba(217,89,38,0.55)`; feature names 13px `#444` left-aligned at x=60; value labels ("+$55k" ... "−$5k") bold 12px just past each bar end, blue `#2a78d6` for positive, orange `#d95926` for negative.
- **Takeaway (bold 13px magenta `#d55181`, centered at y=282):** "credit, not cause — 'neighborhood' may just ride on correlated school quality".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data integrity:** coalition values are v(A)=60, v(B)=30, v(C)=30, v(AB)=120, v(AC)=100, v(BC)=80, v(ABC)=180; the six-ordering table in `c2` must be derived from these and its column averages (80/55/45) must sum to 180; the SHAP contributions in `c4` must sum to the base-to-prediction gap (+70).
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
