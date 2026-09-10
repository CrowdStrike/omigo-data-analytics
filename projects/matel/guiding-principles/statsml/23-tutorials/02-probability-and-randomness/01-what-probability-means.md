# What Probability Means

**Page type:** detail page (tutorial card-sections: one h2 per section, two-column table layout — text left 50%, canvas right 50%)
**HTML title tag:** What Probability Means

**Subtitle:** "30% chance of rain" is a claim about many days like this one — and a measure of how strongly to expect rain tomorrow

## What a "30% Chance of Rain" Actually Claims

Tags: `core idea` (blue), `everyday example` (orange)

- **The forecast** — the weather app says "30% chance of rain tomorrow"
- **Not a promise** — it does not say "it will rain" or "it will not rain"
- **The real claim** — out of many days that look like tomorrow, about 3 in 10 end in rain
- **A number 0 to 1** — 0 means never, 1 means always, 0.3 means 30 times per 100 tries
- **Definition (after the example)** — probability is how often an outcome happens across repeats

*Example:* Collect 100 past days with the same clouds, pressure, and season — on about 30 of them, it rained.

**Key point:** A probability describes a crowd of similar situations, not a guarantee about any single one.

### Visualization (canvas `c1`, 720×300)

Grid diagram: 100 similar days as a 10×10 grid of squares, 30 rainy.

- **Title (bold 15px, `#1a5276`, top center):** "100 Days That Look Like Tomorrow".
- **Grid:** 10×10 cells, 21px squares with 3px gaps, offset left of center; rainy days filled blue `#2a78d6`, dry days light gray `#e5e9ef`. Rainy day numbers (1-based, shared by the whole page): `[3, 7, 10, 12, 18, 21, 24, 29, 33, 36, 40, 43, 47, 50, 55, 58, 61, 64, 67, 70, 74, 77, 80, 83, 86, 89, 92, 95, 97, 100]`.
- **Legend (right of the grid):** blue swatch "30 days end in rain"; light gray swatch "70 days stay dry".
- **Annotations (bold orange `#d95926` 13px, right side):** "30 / 100 = 0.30" / "that IS the \"30% chance\""; gray 12px note below: "(illustrative days)".
- **Caption (bold `#1a5276` 13px, bottom center):** "The forecast is a statement about this whole grid, not about one square".

## Scoring the Forecaster Over 100 Days

Tags: `worked example` (green), `by hand` (blue)

- **The check** — save every day the app said "30%", then count how many actually rained
- **After 10 days** — rain fell on days 3, 7, and 10: that is 3 out of 10, or 30%
- **After 50 days** — 14 rainy days so far: 14 / 50 = 28%, still bouncing around
- **After 100 days** — 30 rainy days total: 30 / 100 = 30%, right on the claim
- **The pattern** — the running fraction wobbles early, then settles near 0.30

*Example:* Day by day the fraction jumps from 0% to 33% and back, but by day 100 it sits at 30%.

**Key point:** You can test a probability by counting — if "30%" days rain about 30% of the time, the number is honest.

### Visualization (canvas `c2`, 720×300)

Line chart: the running fraction of rainy days converging to 30%.

- **Title (bold 15px, `#1a5276`, top center):** "Running Fraction of Rainy Days on \"30%\" Forecast Days".
- **Data:** running fraction = cumulative rainy count / day, computed from the same fixed rainy-day list as `c1` (30 rainy of 100), plotted for days 1–100.
- **Axes:** y 0–60% with labels every 15% (0%, 15%, 30%, 45%, 60%) and light gridlines `#e5e9ef`; x = days 1–100; padding top 50, bottom 55, left 65, right 40; gray `#999` axis lines. X-axis caption (gray 12px, centered): "days collected (each one had a \"30% chance of rain\" forecast)".
- **Reference line:** horizontal dashed orange (`#d95926`, dash 6/4, width 2) at 30%, labeled bold "claimed 30%" just below it at the left.
- **Series:** blue `#2a78d6` line, width 2.5.
- **Checkpoints (magenta `#d55181` 4px dots with bold 12px labels):** day 10 at 30% "day 10: 3/10 = 30%"; day 50 at 28% "day 50: 14/50 = 28%"; day 100 at 30% "day 100: 30/100 = 30%" (right-aligned).

## Frequency or Belief: Two Readings of the Same 30%

Tags: `two views` (blue), `same number` (green)

- **Frequency reading** — across many days like this, 30% end in rain (count over repeats)
- **Belief reading** — for tomorrow alone, 0.30 measures how strongly to expect rain
- **Same forecast** — one number carries both meanings at once
- **One-off events** — "30% this product launch slips" has no repeats; only the belief reading fits
- **They must agree** — if your 30% beliefs come true 60% of the time, your beliefs are off

*Example:* You cannot rerun tomorrow 100 times, yet 0.30 still tells you how much to hedge — pack a light umbrella.

**Key point:** Frequency is probability counted over repeats; belief is the same number applied to a single case — both are valid readings.

### Visualization (canvas `c3`, 720×300)

Two-panel diagram split by a dashed vertical divider at x=360.

- **Title (bold 15px, `#1a5276`, top center):** "One Number, Two Readings".
- **Left panel (header bold blue `#2a78d6` 13px):** "Frequency: many days like this" — 10 circles (17px radius) in a 5×2 arrangement; circles at indexes 2, 6, 9 filled blue with white "rain" labels, the rest light gray `#e5e9ef` with gray "dry" labels. Below: bold blue "3 rainy out of 10 repeats = 30%" and gray 12px "probability = counting over repeats".
- **Right panel (header bold violet `#4a3aa7` 13px):** "Belief: tomorrow, one single day" — a horizontal 0-to-1 belief bar (240×18, light gray `#e5e9ef` track, violet fill to 30%, `#1a5276` outline); endpoint labels gray 12px "0 = certain dry" (left) and "1 = certain rain" (right); a violet tick marker at 0.3 labeled bold "0.30" above. Below: bold violet "how strongly to expect rain" and gray 12px "no repeats needed — it guides one decision".
- **Caption (bold orange `#d95926` 13px, bottom center):** "Same forecast, same 0.30 — read as a count or as a strength of expectation".

## Why a Data Scientist Cares: Model Scores Are Forecasts

Tags: `where it's used` (blue), `common mistake` (red)

- **Model outputs** — "this customer has a 0.30 churn score" is exactly a rain forecast
- **Calibration** — group predictions by score; each group should rain (churn) at its stated rate
- **The mistake** — "it rained, so the 30% forecast was wrong" — one day cannot break a forecast
- **What can break it** — 30% days raining 60% of the time over many days is real evidence
- **Rule of thumb** — judge probabilities in batches, never one outcome at a time

*Example:* A churn model that says 30% for a group where 29% actually leave is doing its job — even when one member surprises you.

**Key point:** A single outcome can never prove a probability right or wrong — only the long-run count can.

### Visualization (canvas `c4`, 720×300)

Grouped bar calibration chart: forecast bucket vs observed rain rate.

- **Title (bold 15px, `#1a5276`, top center):** "Judging Forecasts in Batches: Said vs Happened".
- **Data:** said buckets `[10, 30, 50, 70, 90]`% vs observed `[11, 30, 48, 71, 88]`%.
- **Axes:** y 0–100% with labels every 25% and light gridlines `#e5e9ef`; x = five groups labeled with the quoted claim, e.g. `"10%"`, `"30%"` …; padding top 55, bottom 70, left 65, right 160; gray `#999` axis lines. X-axis caption (gray 12px): "what the forecast said (days grouped by claim, illustrative)".
- **Bars:** paired per group, 26px wide — "forecast said" in blue `#2a78d6`, "actually rained" in green `#008300`; observed value labeled above each green bar (e.g. "11%").
- **Legend (right margin):** blue swatch "forecast said", green swatch "actually rained"; below it bold orange `#d95926` two lines: "bars match =" / "well calibrated".
- **Caption (bold violet `#4a3aa7` 13px, bottom center):** "A good forecaster is judged over batches of days — never on one rainy afternoon".

## Regeneration instructions

- **Layout:** tutorial detail page. `<h1>` + `.subtitle`, then 4 `.card-section` blocks, each an `<h2>` (1.3rem, `#1a5276`, bottom border `2px solid #2980b9`) followed by `table.layout` with one `<tr>`: left `td.text-col` (50%) holding `.tags` pills + `<ul>` bullets + italic `.example` + `.key-point` callout; right `td.viz-col` (50%) holding one 720×300 canvas.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with bottom border `2px solid #2980b9`; `.subtitle` `#666` 0.95rem; canvases `width:100%`, border `1px solid #e0e0e0`, radius 4px; `.key-point` background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem; `.example` italic `#555` 0.9rem; `<li><b>` bold terms in `#1a5276`. No nav bar, no back/home links.
- **Tag pills:** `.tag` inline-block, 0.72rem, weight 600, padding 2px 10px, radius 10px — blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Overall doc palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- **Canvas:** all charts 720×300 logical; a shared `setup(id)` helper scales by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates). A page-level constant `RAINY` array (the 30 rainy day numbers listed under `c1`) drives both `c1` and `c2` — hardcoded, no `Math.random()`. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- No cross-page links; in regenerated HTML any card links would use `.html` extensions.
