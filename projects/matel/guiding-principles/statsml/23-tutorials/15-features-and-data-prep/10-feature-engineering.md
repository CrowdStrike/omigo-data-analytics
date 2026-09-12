# Feature Engineering

**Page type:** detail page (tutorial page: h1 + subtitle, 4 `.card-section` blocks each with an h2 and a two-column `table.layout` — text left 50%, canvas right 50%)
**HTML title tag:** Feature Engineering

**Subtitle:** A model can only learn from the columns you hand it — turning raw records into telling numbers is often worth more than a fancier model

## The Orders Table That Couldn't Predict Churn

**Tags:** `core idea` (blue), `running example` (green)

- **The task** — predict which customers will stop ordering (churn)
- **The raw data** — an orders table: customer id, order date, amount
- **The problem** — a model needs one row of numbers per customer, and dates aren't numbers it can use
- **The move** — compute new columns: days since last order, orders per month, average basket
- **The name** — inventing these input columns is called feature engineering

*Example:* "2025-10-02" means nothing to a model — "last ordered 90 days ago" means everything.

**Key point:** The model never sees your raw table. It sees the columns you build from it — those columns are the features, and building good ones is the job.

### Visualization (canvas `c1`, 720×300)

Transformation diagram: raw orders table → arrow → three feature cards.

- **Title (bold 15px, `#1a5276`, top center):** "From Raw Rows to Features the Model Can Use".
- **Left raw table (x=45, y=55, width 250, row height 26):** header row shaded rgba(26,82,118,0.12) with bold 12px `#1a5276` text; data rows alternate `#fff`/`#f6f8fa` with 12px `#444` text; borders `#e5e9ef`. Rows: header (customer, order date, amount), then ana / 2025-05-14 / $25; ana / 2025-08-03 / $35; ana / 2025-10-02 / $30; ben / 2025-12-26 / $30; a final "…" ellipsis row in all three cells signalling truncation.
- **Table caption (12px muted `#6b7280`, centered below):** "many rows per customer — dates, not signals".
- **Arrow:** orange (`#d95926`, width 3) horizontal line from x=310 to x=375 at y=145 with filled triangular head and bold 12px label "compute" above.
- **Right feature cards (x=405, width 275, height 52, stacked):** `#f8f9fa` boxes with 2px colored borders and bold 13px colored labels + 12px `#444` values:
  - "days since last order" — "ana: 90    ben: 5" — blue `#2a78d6`
  - "orders per month" — "ana: 0.5   ben: 2.0" — green `#008300`
  - "average basket" — "ana: $30   ben: $30" — violet `#4a3aa7`
- **Caption (bold 13px orange, bottom):** "one row per customer, and every number means something".

## Ana and Ben: Same Totals, Opposite Risk

**Tags:** `worked example` (green)

- **The raw view** — Ana: 6 orders, $180 total; Ben: 6 orders, $180 total — identical
- **Days since last order** — Ana last ordered Oct 2: 29 + 30 + 31 = 90 days ago; Ben Dec 26: 5 days
- **Orders per month** — Ana joined 12 months ago: 6 ÷ 12 = 0.5; Ben joined 3 months ago: 6 ÷ 3 = 2.0
- **Average basket** — both: $180 ÷ 6 = $30 per order
- **The verdict** — Ana is drifting away; Ben is heating up — invisible in the raw totals

*Example:* Count from Oct 2 to Dec 31 by hand: 29 days left in Oct + 30 in Nov + 31 in Dec = 90.

**Key point:** Order count and total spend treat Ana and Ben as twins. The three engineered features split them apart — that separation is exactly what the model needs to learn churn.

### Visualization (canvas `c2`, 720×300)

Three mini bar panels, one per feature, each comparing Ana vs Ben.

- **Title (bold 15px, `#1a5276`, top center):** "Same 6 Orders, Same $180 — Three Features Split Them".
- **Panels (width 200, gap 30, starting x=40; baseline y=235, chart height 150, bar width 56):** each with a bold 13px `#1a5276` panel title and a thin `#999` baseline. Ana bars magenta `#d55181`, Ben bars aqua `#199e70`, both at 0.72 alpha; bold 13px value labels above bars in the bar color; 12px `#333` "Ana"/"Ben" labels below.
  - Panel 1 "days since last order": Ana 90, Ben 5 (scale max 100, labels "90d", "5d").
  - Panel 2 "orders per month": Ana 0.5, Ben 2.0 (scale max 2.4, labels "0.5", "2.0").
  - Panel 3 "average basket": Ana 30, Ben 30 (scale max 40, labels "$30", "$30").
- **Caption (bold 13px orange `#d95926`, bottom center):** `raw totals said "twins" — the features say drifting away vs heating up`.

## Better Inputs Beat Fancier Models

**Tags:** `where it's used` (blue), `rule of thumb` (blue)

- **Weak baseline** — simple model on raw columns (order count, total spend): AUC 0.61
- **Fancier model** — gradient boosting on the same raw columns barely helps: AUC 0.64
- **Better inputs** — the simple model with the 3 engineered features: AUC 0.81
- **The lesson** — the big jump came from the inputs, not the algorithm
- **Everywhere** — fraud, credit, recommendations: the winning teams mostly win on features

*Example:* Days-since-last-order alone often beats an entire raw table for churn.

**Rule of thumb:** Before reaching for a bigger model, ask what a human expert would look at — then compute that as a column. Domain knowledge enters the model through features.

### Visualization (canvas `c3`, 720×300)

Three-bar AUC comparison showing inputs matter more than the algorithm.

- **Title (bold 15px, `#1a5276`, top center):** "Churn Model AUC: Where the Jump Comes From (illustrative)".
- **Scale:** y maps AUC 0.4–0.9 onto 175px height, baseline y=235; thin `#999` baseline from x=80 to x=660.
- **Coin-flip line:** dashed red (`#e74c3c`, dash 6/4, width 1.5) horizontal line at 0.5, labeled in bold 12px red at the right: "0.5 = coin flip".
- **Bars (width 120, gap 70, starting x=110, fill at 0.72 alpha; bold 14px value label above in the bar color; 12px `#333` first line and 11px muted second line below):**
  - 0.61 — "simple model" / "raw columns" — muted gray `#6b7280`
  - 0.64 — "fancy model" / "raw columns" — violet `#4a3aa7`
  - 0.81 — "simple model" / "3 engineered features" — green `#008300`
- **Caption (bold 13px green, bottom center):** "+0.17 from inputs, +0.03 from the algorithm".

## More Columns Is Not the Goal

**Tags:** `common mistake` (red), `watch out` (orange)

- **The confusion** — "just give the model everything and it will figure it out"
- **Reality** — a model can't parse date strings, and most can't divide two columns on their own
- **Dumping columns** — 30 extra raw columns moved AUC from 0.61 to only 0.63
- **Three good ones** — the 3 engineered features moved it to 0.81
- **Watch out** — never build a feature from data that arrives after the thing you predict

*Example:* "Cancelled subscription date" predicts churn perfectly — because it IS churn, leaked into a feature.

**Common mistake:** Measuring feature work in column count. One column that encodes a real behavior beats thirty that restate the raw table.

### Visualization (canvas `c4`, 720×300)

Three-bar AUC comparison of column count vs signal, with column-count dots under each bar.

- **Title (bold 15px, `#1a5276`, top center):** "Column Count vs Signal (illustrative)".
- **Scale:** y maps AUC 0.4–0.9 onto 160px, baseline y=225; thin `#999` baseline from x=80 to x=660.
- **Bars (width 120, gap 70, starting x=110, 0.72 alpha; bold 14px "AUC 0.NN" label above in bar color; 12px `#333` label and optional 11px muted sub-label below):**
  - AUC 0.61 — "2 raw columns" — muted gray `#6b7280` — 2 dots
  - AUC 0.63 — "32 raw columns" / "everything dumped in" — yellow `#c98500` — 16 dots plus "…32" overflow note
  - AUC 0.81 — "3 engineered features" / "built with intent" — green `#008300` — 3 dots
- **Column dots:** 2.5px-radius dots in the bar color, centered under each bar at baseline+45; dots capped at 16 with an "…N" 11px note for overflow. Legend in 11px muted at top-left (x=80, y=50): "dots = columns given to the model".
- **Caption (bold 13px red `#e74c3c`, bottom center):** "30 more columns bought 0.02 — three thoughtful ones bought 0.20".

## Regeneration instructions

- **Layout:** tutorial detail page. `<h1>` + `.subtitle` paragraph, then four `.card-section` divs, each an `<h2>` (bottom border `2px solid #2980b9`) followed by a `table.layout` with one `<tr>`: left `td.text-col` (50%) holding `.tags` pills, a `<ul>` of bold-term bullets, an italic `.example` line, and a `.key-point` callout; right `td.viz-col` (50%) holding one `<canvas width="720" height="300">`. Callout bold lead-ins vary per section: "Key point:", "Rule of thumb:", "Common mistake:".
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6. h1 2rem `#1a5276` with `2px solid #2980b9` bottom border; `.subtitle` `#666` 0.95rem; h2 1.3rem `#1a5276`. `ul` 0.92rem; `li b` in `#1a5276`; `li code` ui-monospace on `#f4f6f8`. `.example` italic `#555` 0.9rem. `.key-point` background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem. Canvases `width:100%`, border `1px solid #e0e0e0`, radius 4px. No nav bar, no back/home links.
- **Tag pills:** `.tag` inline-block, 0.72rem bold, padding 2px 10px, radius 10px; `.tag.blue` background rgba(26,82,118,0.12) color `#1a5276`; `.tag.green` rgba(39,174,96,0.15) `#27ae60`; `.tag.red` rgba(231,76,60,0.12) `#e74c3c`; `.tag.orange` rgba(230,126,34,0.15) `#e67e22`.
- **Chart palette (JS object `P`):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- **Canvas:** all charts 720×300 logical, drawn via a shared `setup(id)` helper that sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- In regenerated HTML, any card links use `.html` extensions (this page has no outgoing links).
