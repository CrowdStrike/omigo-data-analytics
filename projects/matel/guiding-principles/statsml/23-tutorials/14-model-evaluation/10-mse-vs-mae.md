# MSE vs MAE

**Page type:** detail page (tutorial layout: `.card-section` blocks, each with a two-column `table.layout` — text left 50%, canvas right 50%)
**HTML title tag:** MSE vs MAE

**Subtitle:** Two ways to average your errors — one punishes big misses hard, the other treats all misses alike

## Six Small Misses and One Festival Sunday

**Tags:** `core idea` (blue), `outliers` (orange)

- **The setup** — a coffee shop forecasts daily cup sales; each day the forecast misses by a bit
- **The week** — misses of 3, 2, 1, 4, 2 and 1 cups Mon–Sat: small, ordinary, forgivable
- **Then Sunday** — a street festival nobody predicted: the forecast misses by 21 cups
- **The question** — how good was the week overall? You must average seven misses into one number
- **Two answers** — MAE averages the misses as-is; MSE squares each miss first, then averages

*Example:* Squaring turns Sunday's 21-cup miss into 441 — while Tuesday's 2-cup miss becomes just 4.

**Key point:** Both metrics summarize the same seven misses — they only disagree on how loudly the one big miss should count.

### Visualization (canvas `c1`, 720×300)

Bar chart of the seven daily forecast misses with Sunday highlighted.

- **Title (bold 15px, `#1a5276`, top center):** "One Week of Forecast Misses at the Coffee Shop"
- **Padding:** top 52, bottom 56, left 62, right 30. Gray `#999` L-frame axes. Y scale max 24.
- **Data:** days `Mon, Tue, Wed, Thu, Fri, Sat, Sun`, misses `[3, 2, 1, 4, 2, 1, 21]`. Bars 46px wide, Mon–Sat in blue `#2a78d6`, Sunday in orange `#d95926`. Bold 12px `#444` value labels above bars; 12px `#222` day labels below.
- **Axis titles 12px `#444`:** "day of the week" (bottom center), rotated "forecast miss, cups" (left).
- **Annotation (bold 13px orange `#d95926`, right-aligned near top of plot):** "street festival: one miss 5x any other"

## Averaging the Week Two Ways: 4.9 vs 8.2 Cups

**Tags:** `worked example` (green), `squared error` (blue), `absolute error` (blue)

- **MAE** — add the misses: 3+2+1+4+2+1+21 = 34; divide by 7 days: MAE ≈ 4.9 cups
- **MSE** — square first: 9+4+1+16+4+1+441 = 476; divide by 7: MSE = 68 cups²
- **RMSE** — take the square root to get back to cups: √68 ≈ 8.2 cups
- **Sunday's share** — 21 of 34 is 62% of MAE, but 441 of 476 is 93% of MSE
- **Read the gap** — RMSE 8.2 vs MAE 4.9: a big gap between them is a big-miss alarm

*Example:* A "typical miss" of 8.2 cups describes no actual day — six days missed by 4 or less.

**Key point:** Squaring hands the microphone to the biggest miss: one day out of seven ends up carrying 93% of the MSE.

### Visualization (canvas `c2`, 720×300)

Two horizontal stacked share bars showing Sunday's slice of the absolute total vs the squared total.

- **Title (bold 15px, `#1a5276`, top center):** "Sunday's Share of the Total: 62% of MAE, 93% of MSE"
- **Bars:** 460px wide, 44px tall, x=130; row 1 at y=66, row 2 at y=156. Each bar split: Mon–Sat portion in `rgba(42,120,214,0.35)`, Sunday portion in orange `#d95926`; `#1a5276` 1px outline. White bold 13px percentage centered inside the Sunday slice.
  - Row 1 — label above (bold 12px `#1a5276`): "absolute total = 34"; Sunday fraction 21/34 (shown "62%"); metric text right of bar (12px `#444`): "MAE = 34/7 ≈ 4.9 cups".
  - Row 2 — label: "squared total = 476"; Sunday fraction 441/476 (shown "93%"); metric text: "MSE = 476/7 = 68 → RMSE ≈ 8.2".
- **Legend (12px, y≈228):** blue swatch "Mon–Sat misses (3+2+1+4+2+1)"; orange swatch "Sunday miss (21; squared: 441)".
- **Takeaway (bold 13px red `#e74c3c`, bottom center y=276):** "squaring turns one bad day into almost the whole score"

## Why the Choice Changes What the Model Learns

**Tags:** `where it's used` (blue), `trade-off` (orange)

- **Training target** — most regressions minimize MSE by default; the metric shapes the model
- **Quadratic sting** — double a miss and MAE's penalty doubles, but MSE's quadruples
- **Chasing outliers** — an MSE-trained model bends toward festival days to dodge huge penalties
- **Cost check** — if a 20-cup miss truly hurts far more than ten 2-cup misses, MSE is honest
- **Robust check** — if outlier days are noise (a till glitch, one festival), MAE is the safer average

*Example:* Under MSE, one 20-cup miss (400) costs as much as one hundred 2-cup misses (100 × 4).

**Key point:** Pick the metric whose penalty curve matches your real cost of being wrong — not the one your library defaults to.

### Visualization (canvas `c3`, 720×300)

Line chart comparing the linear |e| penalty and the quadratic e² penalty curve.

- **Title (bold 15px, `#1a5276`):** "Penalty per Miss: Linear vs Quadratic"
- **Padding:** top 50, bottom 54, left 66, right 185. Gray `#999` L-frame axes. X 0–20 (miss size, cups), y 0–400 (penalty).
- **Series:** MAE penalty |e| as a straight green `#008300` line (width 3) from (0,0) to (20,20); MSE penalty e² as a violet `#4a3aa7` curve (width 3) plotted for integer e = 0…20.
- **Markers (5px dots):** at miss = 5 (violet dot at 25, green dot at 5) and miss = 20 (violet dot at 400, green dot at 20). Bold 12px violet labels: "20-cup miss → 400" and "5-cup miss → 25".
- **X ticks:** 0, 5, 10, 15, 20 (12px `#222`). Axis titles 12px `#444`: "size of the miss, cups" (bottom), rotated "penalty added to the total" (left).
- **Legend (right side, x = w−172):** green swatch "MAE penalty: |miss|"; violet swatch "MSE penalty: miss²".
- **Annotation (bold 12px violet, right panel):** "double the miss" / "→ 4x the penalty"

## Two Baristas, Two Winners

**Tags:** `common mistake` (red), `rule of thumb` (green)

- **Barista A** — misses by exactly 4 cups every day: MAE = 4.0, RMSE = 4.0
- **Barista B** — perfect six days, then one 14-cup miss: MAE = 2.0, RMSE ≈ 5.3
- **The flip** — B halves the MAE yet loses on RMSE: the rankings genuinely disagree
- **Units trap** — MSE is in cups², which nobody can picture; report RMSE or MAE, in cups
- **Equal only when** — RMSE = MAE only if every miss is the same size; otherwise RMSE > MAE

*Example:* "Which barista forecasts better?" has no answer until you say which average you mean.

**Common mistake:** Treating MAE and RMSE as interchangeable — a model can win on one and lose on the other, on the same data.

### Visualization (canvas `c4`, 720×300)

Split panel: two mini bar rows of daily misses (left) and a grouped metric-comparison chart (right), divided by a vertical dashed line (`#bdc3c7`, dash 4/3) at x=430 from y=40 to h−15.

- **Title (bold 15px, `#1a5276`, top center):** "B Wins on MAE, A Wins on RMSE — Same Week"
- **Left panel (x=60, width 330, bars 30px wide, row height 74, y max 15):**
  - Panel 1 at y0=66 — Barista A daily misses `[4, 4, 4, 4, 4, 4, 4]` in aqua `#199e70`, header (bold 12px aqua): "Barista A: off by 4 every day".
  - Panel 2 at y0=172 — Barista B daily misses `[0, 0, 0, 0, 0, 0, 14]` in magenta `#d55181`, header (bold 12px magenta): "Barista B: perfect, then one 14-cup miss".
  - 11px `#444` value labels above bars; caption 12px gray `#6b7280` (y=278): "daily misses, cups (Mon–Sun)".
- **Right panel (x=470, width 210, baseline y=232, height 150, y max 6):** two grouped-bar pairs, bars 34px wide — MAE group (A=4.0 aqua, B=2.0 magenta) and RMSE group (A=4.0 aqua, B=5.3 magenta). Bold 12px `#444` value labels ("4.0", "2.0", "4.0", "5.3") above bars; group labels "MAE" and "RMSE" below.
- **Legend (12px, y≈56):** aqua swatch "Barista A"; magenta swatch "Barista B".
- **Annotation (bold 12px red `#e74c3c`, centered below right panel):** "each metric picks a different winner"

## Regeneration instructions

- **Layout:** tutorial detail page — h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks. Each section: `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one `<tr>`: left `td.text-col` (50%) holding `.tags` pill row, a `<ul>` of bold-term bullets (`li b` in `#1a5276`), an italic `.example` line, and a `.key-point` callout; right `td.viz-col` (50%) holding one 720×300 canvas.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; ul 0.92rem; `.example` italic `#555` 0.9rem; `.key-point` background `#f8f9fa`, left border `3px solid #e74c3c`, 0.9rem. No nav bar, no back/home links.
- **Tag pills:** `.tag` inline-block, 0.72rem, weight 600, padding 2px 10px, radius 10px. Colors: blue `rgba(26,82,118,0.12)`/`#1a5276`; green `rgba(39,174,96,0.15)`/`#27ae60`; red `rgba(231,76,60,0.12)`/`#e74c3c`; orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Canvas:** intrinsic 720×300, CSS `width:100%`, border `1px solid #e0e0e0` radius 4px; scaled via `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) in a shared `setup(id)` helper. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette (JS object `P`):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Doc palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- **Shared data (all charts):** coffee-shop forecast misses Mon–Sun in cups: `3, 2, 1, 4, 2, 1, 21` (Sunday = street festival); MAE = 34/7 ≈ 4.9, MSE = 476/7 = 68, RMSE ≈ 8.2.
- This page has no card links; in regenerated HTML any links would use `.html` extensions.
