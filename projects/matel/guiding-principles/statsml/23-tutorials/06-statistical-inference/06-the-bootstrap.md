# The Bootstrap

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table row, text left 50% / canvas right 50%)
**HTML title tag:** The Bootstrap

**Subtitle:** Reshuffle and resample your own data thousands of times to see how much your answer could vary — no formulas needed.

## One Bag of Receipts, a Thousand Pretend Days

Tags: `core idea` (blue), `resampling` (green), `simulation` (orange)

- **The data** — a coffee shop keeps 50 receipts from one day; average spend is $8.40
- **The worry** — a different day's 50 customers would give a different average
- **The trick** — pretend the 50 receipts ARE the whole world of customers
- **Resample** — draw 50 receipts from the bag WITH replacement; average them
- **Repeat** — do it 1,000 times; the 1,000 averages show how much luck moves you

*Example (italic):* Resample #1 averages $8.13, #2 gives $8.62, #3 gives $8.31 — the wobble is the point.

**Key point:** you cannot rerun the day, so you rerun the sample — with replacement, so each pretend day differs.

### Visualization (canvas `c1`, 720×300)

Flow diagram: bag of receipts → three resample boxes → mini histogram of 1,000 averages.

- **Title (bold 15px, `#1a5276`, top center):** "One Real Sample Becomes 1,000 Pretend Samples".
- **Bag (left):** rectangle at (40, 70), 150×140, fill `rgba(42,120,214,0.12)`, stroke `#2a78d6` width 2. Text inside: bold "50 receipts", "one real day", bold blue 14px "avg $8.40"; 12 small dots (radius 5, `rgba(26,82,118,0.45)`) in a 6×2 grid. Caption below bag (11px muted): "each draw: with replacement".
- **Arrows:** gray `#6b7280` arrows (line + filled triangle head) from the bag to each resample box, and from the boxes to the histogram.
- **Resample boxes (middle, x=280, 170×44 each):** fill `rgba(25,158,112,0.10)`, stroke `#199e70` width 1.5, at y = 62 / 128 / 194:
  - "resample #1 (50 draws)" with bold aqua "avg $8.13"
  - "resample #2 (50 draws)" with bold aqua "avg $8.62"
  - "resample #3 (50 draws)" with bold aqua "avg $8.31"
  - Below (bold muted 13px): "... 997 more".
- **Mini histogram (right, x=510, width 180, base y=235, height 145):** 12 bars, counts `[6, 19, 47, 94, 147, 185, 185, 147, 94, 47, 19, 6]`, scale max 200, fill `rgba(42,120,214,0.55)`; x labels "$7.20" / "$8.40" / "$9.60"; bold violet `#4a3aa7` label above: "1,000 pretend averages"; bold orange `#d95926` two-line annotation: "the wobble you" / "could not see before"; muted "(illustrative)" below.

## Five Receipts, Fully by Hand

Tags: `worked example` (green), `small numbers` (blue)

- **Tiny bag** — five receipts: $4, $6, $8, $10, $12; the real average is $8.00
- **Draw 1** — pull 5 with replacement: 6, 8, 8, 12, 4 → average $7.60
- **Draw 2** — 12, 10, 12, 8, 6 → average $9.60 (the $12 came up twice)
- **Draw 3** — 4, 4, 6, 10, 8 → average $6.40; Draw 4: 8, 6, 10, 10, 12 → $9.20
- **Draw 5** — 6, 4, 8, 8, 10 → $7.20; five pretend averages: 7.6, 9.6, 6.4, 9.2, 7.2

*Example (italic):* With replacement means a receipt can appear twice — or not at all — in one pretend day.

**Key point:** each resample is the same size as the original (5 of 5) — only the mix changes, and so does the average.

### Visualization (canvas `c2`, 720×300)

Left: five listed resamples with averages; right: those five averages as dots on a shared dollar axis.

- **Title (bold 15px, `#1a5276`, top center):** "Five Resamples of {$4, $6, $8, $10, $12} — Averages Wobble Around $8".
- **Left text rows** (starting y=62, 34px apart; row number bold in that row's color, values in `#2c3e50`, average bold in row color):
  - #1 (blue `#2a78d6`): 6, 8, 8, 12, 4 — avg $7.6
  - #2 (aqua `#199e70`): 12, 10, 12, 8, 6 — avg $9.6
  - #3 (violet `#4a3aa7`): 4, 4, 6, 10, 8 — avg $6.4
  - #4 (orange `#d95926`): 8, 6, 10, 10, 12 — avg $9.2
  - #5 (magenta `#d55181`): 6, 4, 8, 8, 10 — avg $7.2
  - Muted note below: "drawn with replacement:" / "#2 uses $12 twice, skips $4".
- **Right axis:** dollar scale $5.50–$10.50 mapped to x=340..680, axis line at y=210, ticks/labels at $6, $7, $8, $9, $10.
- **Original mean line:** dashed `#1a5276` vertical at $8 (width 2, dash 5/4) with bold label "original avg $8.00" above.
- **Dots:** each resample average as a radius-7 dot in its row color at staggered heights `[180, 100, 160, 120, 140]`, white bold number inside, thin dotted drop line to the axis.
- **Annotations:** muted "resample average" and bold orange line below axis: "five pretend averages: $6.40 to $9.60 — a $3.20 swing from luck alone".

## Error Bars for Anything — No Formula Required

Tags: `where it's used` (blue), `no formulas needed` (green)

- **Collect** — 1,000 resample averages pile into a bell around $8.40
- **Read off** — the middle 95% of them run $7.60 to $9.20: a ready-made interval
- **The payoff** — swap "average" for median, ratio, or a model score; same recipe
- **Why loved** — medians and ratios have ugly or unknown error formulas; resampling skips them
- **In practice** — error bars on dashboards and papers are often bootstrap percentiles

*Example (italic):* "Median spend $7.25, bootstrap 95% interval $6.80–$8.10" — no simple plug-in formula exists for that.

**Key point:** the spread of the 1,000 pretend answers estimates the spread of the one real answer.

### Visualization (canvas `c3`, 720×300)

Histogram of 1,000 bootstrap averages with the middle-95% band shaded.

- **Title (bold 15px, `#1a5276`, top center):** "1,000 Bootstrap Averages of the Coffee Receipts (illustrative)".
- **Bins:** width $0.20, centers `[7.3, 7.5, 7.7, 7.9, 8.1, 8.3, 8.5, 8.7, 8.9, 9.1, 9.3, 9.5]`, counts `[6, 19, 47, 94, 147, 185, 185, 147, 94, 47, 19, 6]`, count scale max 210, dollar scale $7.20–$9.60. Padding: top 56, bottom 56, left 60, right 30. Bars inside the band (7.6–9.2) filled `rgba(42,120,214,0.55)`; bars outside filled `rgba(107,114,128,0.35)`.
- **95% band:** background rectangle from $7.60 to $9.20 filled `rgba(0,131,0,0.10)`; dashed green `#008300` vertical edge lines (width 2, dash 5/4) at $7.60 and $9.20.
- **X ticks:** $7.20, $7.60, $8.00, $8.40, $8.80, $9.20, $9.60; axis label "bootstrap resample average"; L-shaped gray `#999` axes.
- **Annotations:** bold green top center: "middle 95%: $7.60 to $9.20 — the interval, read straight off the pile"; muted "2.5% below" (near $7.42) and "2.5% above" (near $9.38).

## What the Bootstrap Cannot Do

Tags: `common mistake` (red), `limits` (orange)

- **No new data** — every resampled value is one of your original 50 receipts
- **No bias repair** — if morning-only receipts skew low, all 1,000 resamples skew low too
- **No tail magic** — a $200 spender you never sampled can never appear in a resample
- **Small-n caution** — with 5 receipts the pretend worlds are crude copies of a crude sample
- **What it gives** — honest wobble around YOUR sample, not a window past it

*Example (italic):* 1,000 resamples of biased data give a beautifully precise interval around the wrong answer.

**Key point:** the bootstrap measures luck, not bias — it answers "how shaky?", never "how wrong?".

### Visualization (canvas `c4`, 720×300)

Three stacked dot-strip rows on a shared dollar axis showing that resamples never exceed the observed range.

- **Title (bold 15px, `#1a5276`, top center):** "Resamples Never Leave Your Sample (illustrative)".
- **Dollar axis:** $0–$40 mapped to x=70..650; axis line at y=262 with ticks/labels at $0, $10, $20, $30, $40.
- **Row 1 (y=90), label "all possible customers (unknown)":** dots at `[2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 16, 18, 21, 25, 30, 38]`; values > 18 colored `rgba(217,89,38,0.6)` (orange), the rest `rgba(107,114,128,0.45)` (gray); bold orange annotation: "big spenders exist out here".
- **Row 2 (y=160), label "your 50 receipts (observed: $2 to $18)":** dots at `[2, 4, 5, 6, 7, 8, 8, 9, 10, 11, 12, 13, 14, 16, 18]` in `rgba(42,120,214,0.65)`.
- **Row 3 (y=230), label "every bootstrap resample, forever":** same values as row 2 in `rgba(25,158,112,0.65)`.
- **Hard wall:** dashed red `#e74c3c` vertical line (width 2.5, dash 6/4) at $19 spanning rows 2–3 (y 120–255); bold red two-line label right of it: "hard wall: a value you never" / "sampled can never appear".
- All dots radius 5; row labels bold 12px `#2c3e50` above each row.

## Regeneration instructions

- **Layout:** tutorial detail page. h1 (2rem, `#1a5276`, 2px `#2980b9` bottom border) + `.subtitle` (`#666`, 0.95rem), then four `.card-section` blocks. Each section: `<h2>` (1.3rem, `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one `<tr>`: left `<td class="text-col">` (50%) holds `.tags` pill row, `<ul>` bullets (each starting with `<b>` term in `#1a5276`), one italic `.example` paragraph, one `.key-point` callout; right `<td class="viz-col">` (50%) holds one canvas.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6. Bullets 0.92rem; `.example` italic `#555` 0.9rem; `.key-point` background `#f8f9fa`, left border 3px solid `#e74c3c` (note: red on this page), padding 8px 12px, 0.9rem.
- **Tag pills:** `.tag` inline-block 0.72rem bold, padding 2px 10px, radius 10px. Colors — blue: bg `rgba(26,82,118,0.12)` / `#1a5276`; green: bg `rgba(39,174,96,0.15)` / `#27ae60`; red: bg `rgba(231,76,60,0.12)` / `#e74c3c`; orange: bg `rgba(230,126,34,0.15)` / `#e67e22`.
- **Canvas:** 720×300 intrinsic attributes (setup helper reads width/height attributes), CSS `width:100%`, 1px `#e0e0e0` border, radius 4px; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object `P`:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- No nav bar, no back/home links. In regenerated HTML any card links use `.html` extensions (this page has none).
