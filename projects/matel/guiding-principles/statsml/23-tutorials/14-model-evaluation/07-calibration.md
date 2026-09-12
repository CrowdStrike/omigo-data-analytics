# Calibration

**Page type:** detail page (tutorial layout: `.card-section` blocks, each with a two-column `table.layout` — text left 50%, canvas right 50%)
**HTML title tag:** Calibration

**Subtitle:** When the model says "70% fraud risk", is it right 70% of the time? — a model can sort transactions well and still lie about the odds

## The Model Said 70% — Was It?

**Tags:** `core idea` (blue), `honesty check` (orange)

- **The claim** — a fraud model scores 1,000 transactions; 30 land in 0.6–0.8: "about 70% risk"
- **The check** — look up what actually happened: 9 of those 30 turned out to be fraud
- **The verdict** — actual rate 9/30 = 30% vs claimed ~70%: the model overstates risk 2x
- **The name** — a model is calibrated when its stated probabilities match observed rates
- **Not accuracy** — this asks whether the number 0.7 means 70%, not whether flags were right

*Example:* A weather app is calibrated if, across all its "70% rain" days, it rained on about 70% of them.

**Key point:** Calibration checks the honesty of the probability itself: of everything scored p, a fraction p should turn out positive.

### Visualization (canvas `c1`, 720×300)

Split panel: dot grid of the "70%" bin on the left, claimed-vs-actual bars on the right, separated by a vertical dashed divider (`#bdc3c7`, dash 4/3) at x=390 from y=40 to h−15.

- **Title (bold 15px, `#1a5276`, top center):** "The 30 Transactions Scored \"About 70% Risk\""
- **Left panel:** 6×5 grid of 30 dots (radius 11), origin (80, 66), spacing 44px horizontal / 36px vertical. Fraud pattern by index (1 = fraud, row-major): `[1,0,0,1,0,0, 0,1,0,0,0,1, 0,0,1,0,0,0, 1,0,0,0,1,0, 0,0,1,0,0,1]` — 9 red fraud dots `#e74c3c`, 21 gray dots `#d5dbe1`. Labels under grid: bold red 12px "9 red = actually fraud" (y=252), gray `#6b7280` 12px "21 gray = turned out legit" (y=270).
- **Right panel:** axis L-frame from x=430, baseline y=232, chart height 160px, y scale max 100. Two bars 84px wide at 30% and 72% of panel width: "claimed risk" 70% in violet `#4a3aa7`, "actual rate" 30% in green `#008300`. Bold 15px value labels ("70%", "30%") above bars; bold 12px labels below; gray 12px sub-captions "~0.7 average score" and "9 / 30 were fraud" at baseline+34.
- **Annotation (bold 13px red `#e74c3c`, centered over right panel at y=52):** "the model overstates risk ~2x"

## Binning All 1,000 Transactions: The Reliability Diagram

**Tags:** `worked example` (green), `core idea` (blue)

- **Make bins** — group transactions by score: 0–0.2, 0.2–0.4, 0.4–0.6, 0.6–0.8, 0.8–1.0
- **Count each bin** — the 0.8–1.0 bin holds 20 transactions, 13 of them real fraud
- **Actual rates** — 1%, 10%, 20%, 30%, 65% against claimed ~10%, 30%, 50%, 70%, 90%
- **Plot it** — claimed rate on x, actual rate on y: that is the reliability diagram
- **Read it** — points on the diagonal are honest; below it, the model is overconfident

*Example:* The 0–0.2 bin holds 800 transactions with 8 frauds — actual 1% vs claimed ~10%, overconfident even at the bottom.

**Key point:** Every point below the diagonal is the model promising more fraud than reality delivers — this model exaggerates in every bin.

### Visualization (canvas `c2`, 720×300)

Reliability diagram (line + dots) with honest diagonal reference.

- **Title (bold 15px, `#1a5276`, top center):** "Reliability Diagram: Claimed Risk vs What Happened"
- **Padding:** top 50, bottom 52, left 62, right 30. Gray `#999` L-frame axes.
- **Axes:** x = claimed fraud risk 0–100, y = actual fraud rate 0–100; x ticks at 0, 20, 40, 60, 80, 100. Axis titles 12px `#444`: "claimed fraud risk (bin average), %" (bottom center) and rotated "actual fraud rate in the bin, %" (left).
- **Honest diagonal:** dashed `#bbb` line (dash 5/4, width 1.5) from (0,0) to (100,100), with rotated 12px `#888` label along it at (60,60): "honest line: claimed = actual".
- **Data series (magenta `#d55181`, line width 3, 6px-radius dots):** claimed `[10, 30, 50, 70, 90]` vs actual `[1, 10, 20, 30, 65]`. Below each dot (offset +22px), 12px `#555` count labels from `[800, 100, 50, 30, 20]` formatted as "800 txns", "100 txns", "50 txns", "30 txns", "20 txns".
- **Annotation (bold 13px magenta, left-aligned near top-left at data coords ~(14, 84)):** "every bin below the line = overconfident everywhere"

## Why a Lying Probability Costs Money

**Tags:** `where it's used` (blue), `common mistake` (red)

- **Decisions use p** — "block if risk × amount exceeds review cost" needs 0.7 to mean 70%
- **A $500 example** — claimed 70% risk → expected loss $350, block; true 30% → $150, review
- **Forecasts inflate** — summing claimed risks forecasts 174 frauds; only 50 exist
- **Budgets follow** — alert staffing, loss reserves, insurance pricing all multiply by p
- **The fix** — recalibrate scores on held-out data (Platt scaling or isotonic regression)

*Example:* The top bin promised 20 × 90% = 18 frauds; reality delivered 13.

**Key point:** Any decision that multiplies by the model's probability inherits its lie; recalibration fixes the numbers without touching the ranking.

### Visualization (canvas `c3`, 720×300)

Grouped bar chart: promised vs actual frauds per score bin, with legend and takeaway panel on the right.

- **Title (bold 15px, `#1a5276`):** "Frauds the Scores Promise vs Frauds That Exist"
- **Padding:** top 56, bottom 62, left 62, right 175. Gray `#999` L-frame axes. Y scale max 90.
- **Bins (x labels 12px `#222`):** `0-0.2`, `0.2-0.4`, `0.4-0.6`, `0.6-0.8`, `0.8-1.0`.
- **Bars (26px wide, paired per bin):** expected (count × claimed rate) `[80, 30, 25, 21, 18]` in violet `#4a3aa7`; actual `[8, 10, 10, 9, 13]` in green `#008300`. 12px `#444` value labels above each bar.
- **Axis titles 12px `#444`:** "score bin" (bottom center), rotated "number of frauds" (left).
- **Legend (right side, x = w−160):** violet swatch "promised: count × p"; green swatch "actual frauds".
- **Right-panel annotations:** bold 13px red `#e74c3c` "forecast: 174 frauds" / "reality: 50"; below in 12px gray `#6b7280`: "any budget built on" / "these p's is ~3.5x off".

## Good AUC, Bad Probabilities — Both at Once

**Tags:** `common mistake` (red), `rule of thumb` (green)

- **Ranking is fine** — actual fraud rate climbs 1% → 10% → 20% → 30% → 65% across bins
- **Same AUC** — squashing or stretching scores never reorders them, so AUC does not move
- **Two different skills** — sorting fraud above legit vs stating honest odds
- **Each can fail alone** — a perfectly calibrated model can still rank poorly, and vice versa
- **Check both** — report AUC and a reliability diagram; one number cannot cover both

*Example:* Divide every score by 2 and AUC is unchanged — but every stated probability just changed.

**Key point:** High AUC never certifies the probabilities — calibration is a separate check that costs one reliability diagram.

### Visualization (canvas `c4`, 720×300)

Two side-by-side panels split by a vertical dashed divider (`#bdc3c7`, dash 4/3) at x=360 from y=40 to h−15.

- **Title (bold 15px, `#1a5276`, top center):** "One Model, Two Report Cards"
- **Left panel (ranking view):** axis L-frame from x=60, width 270, baseline y=228, chart height 145, y max 70. Five bars 38px wide in blue `#2a78d6` for actual rates `[1, 10, 20, 30, 65]` labeled "1%"…"65%" above; bin labels `0-.2`, `.2-.4`, `.4-.6`, `.6-.8`, `.8-1` below. Headline (bold 13px green `#008300`, y=56): "Ranking: fine (high AUC)". Caption 12px gray `#6b7280` below baseline: "higher score bin → more fraud, always".
- **Right panel (honesty view):** mini reliability plot, origin x=420, width 240, baseline y=228, height 145. Dashed `#bbb` diagonal (dash 5/4). Magenta `#d55181` line (width 3) with 4px dots: claimed `[10, 30, 50, 70, 90]` vs actual `[1, 10, 20, 30, 65]`. Headline (bold 13px red `#e74c3c`, y=56): "Honesty: broken (sags below)". Caption 12px gray: "claimed % (x) vs actual % (y)". Below that (bold 12px `#1a5276`): "check both, always".

## Regeneration instructions

- **Layout:** tutorial detail page — h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks. Each section: `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one `<tr>`: left `td.text-col` (50%) holding `.tags` pill row, a `<ul>` of bold-term bullets (`li b` in `#1a5276`), an italic `.example` line, and a `.key-point` callout; right `td.viz-col` (50%) holding one 720×300 canvas.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; ul 0.92rem; `.example` italic `#555` 0.9rem; `.key-point` background `#f8f9fa`, left border `3px solid #e74c3c`, 0.9rem. No nav bar, no back/home links.
- **Tag pills:** `.tag` inline-block, 0.72rem, weight 600, padding 2px 10px, radius 10px. Colors: blue `rgba(26,82,118,0.12)`/`#1a5276`; green `rgba(39,174,96,0.15)`/`#27ae60`; red `rgba(231,76,60,0.12)`/`#e74c3c`; orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Canvas:** intrinsic 720×300, CSS `width:100%`, border `1px solid #e0e0e0` radius 4px; scaled via `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) in a shared `setup(id)` helper. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette (JS object `P`):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Doc palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- **Shared data (all four charts):** 1,000 transactions, 50 fraud; bins 0–0.2/0.2–0.4/0.4–0.6/0.6–0.8/0.8–1.0; claimed averages 10/30/50/70/90%; counts 800/100/50/30/20; actual frauds 8/10/10/9/13; actual rates 1/10/20/30/65%.
- This page has no card links; in regenerated HTML any links would use `.html` extensions.
