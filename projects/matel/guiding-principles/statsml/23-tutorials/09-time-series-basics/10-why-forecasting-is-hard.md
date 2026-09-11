# Why Forecasting Is Hard

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + `table.layout` two-column row, text left 50% / canvas right 50%)
**HTML title tag:** Why Forecasting Is Hard

**Subtitle:** Regime changes, one-off shocks, and compounding errors — why the future keeps surprising even good models

## The Office Building Opens: When the Rules Change

**Tags:** `core idea` (blue), `regime change` (red)

- **The shop** — a coffee shop sells a steady ~252 cups a day, week after week
- **The event** — in week 21 an office building opens across the street
- **New level** — sales jump to ~340 cups a day and stay there; the world changed
- **The model** — trained on weeks 1-20, it keeps forecasting ~252, off by ~90 cups
- **The name** — a lasting shift in the level or rules is called a regime change

*Example:* The forecast wasn't "wrong math" — the past it learned from simply contained no office building.

**Key point:** Models learn the world that produced the history — when the world itself changes, every model trained on the old world is wrong together.

### Visualization (canvas `c1`, 720×300)

Line chart of 30 weekly averages with a level shift at week 21 and a flat stale forecast.

- **Title (bold 15px, `#1a5276`, top center):** "Cups per day by week: the office building opens in week 21"
- **Data (`weekly`, comment: "Weekly average cups/day: 20 weeks at ~252, then office building, 10 weeks at ~340 (illustrative)"):** `[248,255,246,252,260,251,244,258,262,250,247,253,259,249,256,252,246,261,254,250,335,342,338,345,336,340,348,339,344,341]`
- **Axes:** y from 200 to 380, tick labels 220, 260, 300, 340 with light `#e5e9ef` gridlines; x labeled "wk 1", "wk 10", "wk 21", "wk 30"; L-shaped `#999` axes; padding top 46, bottom 46, left 58, right 20.
- **Regime marker:** dashed (5/4) red `#e74c3c` vertical line at week 21, width 1.5, labeled bold 12px red "building opens" at the top.
- **Actual series:** blue `#2a78d6` line, width 3.
- **Forecast:** dashed (7/5) orange `#d95926` width-3 horizontal segment at y=252 from week 21 to week 30, labeled bold 13px orange (right-aligned) "model trained on wk 1-20: forecasts 252".
- **Annotations:** bold 13px red centered "off by ~90 cups, every day"; bold 12px blue "actual: new level ~340" near the post-change line.

## The Festival and the Roadwork: One-Off Shocks

**Tags:** `shocks` (orange), `running example` (green)

- **Day 17** — a street festival outside the shop: 610 cups, 2.4x a normal day
- **Day 24** — roadwork blocks the entrance: 95 cups, the worst day on record
- **No pattern** — neither event appears anywhere in the earlier data to learn from
- **Both directions** — shocks spike up and crash down; history stays quiet either way
- **The fix** — flag known events (holidays, promos) as inputs; accept the rest as luck

*Example:* The festival was announced on a poster two blocks away — visible to any human, invisible to the sales history.

**Key point:** A one-off shock has no precedent in the training data, so no model that only reads the history can see it coming.

### Visualization (canvas `c2`, 720×300)

Line chart of 28 daily values with one extreme spike and one extreme crash.

- **Title (bold 15px, `#1a5276`, top center):** "28 days of cups: two shocks no history could predict (illustrative)"
- **Data (`daily`):** `[252,248,255,247,260,251,244,258,262,250,247,253,259,249,256,252,610,261,254,250,246,255,248,95,252,258,251,249]`
- **Axes:** y from 0 to 700, tick labels 0, 200, 400, 600 with light `#e5e9ef` gridlines; x labeled "day 1", "day 7", "day 14", "day 21", "day 28"; padding top 46, bottom 46, left 58, right 20.
- **Normal band:** rectangle from y=244 to y=262 across the plot, fill `rgba(42,120,214,0.10)`, labeled bold 12px blue "normal days: 244-262".
- **Series:** blue `#2a78d6` line, width 2.5.
- **Shock markers:** radius-5 magenta `#d55181` dot at day 17 (610); radius-5 red `#e74c3c` dot at day 24 (95).
- **Annotations:** bold 13px magenta "day 17: street festival, 610 cups"; bold 13px red "day 24: roadwork, 95 cups".

## One Day Out vs Sixteen: How Misses Pile Up

**Tags:** `worked example` (green), `error growth` (blue)

- **One step** — forecasting tomorrow, the shop's typical miss is about 12 cups
- **Each extra day** — adds another unpredictable ±12 swing on top of the last
- **The rule** — random swings add up like 12 × √days, not 12 × days
- **Check it** — 4 days out: 12 × √4 = 24; 9 days: 12 × 3 = 36; 16 days: 12 × 4 = 48
- **The lesson** — 16 days ahead the typical miss is 4x tomorrow's, from math alone

*Example:* The same model that misses tomorrow by 12 cups misses two weeks out by ~45 — nothing broke, errors just compounded.

**Key point:** Forecast error grows with the horizon even for a perfect model — every extra step stacks another round of unpredictable swings.

### Visualization (canvas `c3`, 720×300)

Curve of typical miss vs forecast horizon: the square-root rule against the feared linear one.

- **Title (bold 15px, `#1a5276`, top center):** "Typical miss vs days ahead: 12 × √days (random-walk rule)"
- **Axes:** x horizon 1 to 16 days, tick labels "1 day", "4 days", "9 days", "16 days"; y 0 to 55, tick labels 12, 24, 36, 48 with light `#e5e9ef` gridlines; padding top 46, bottom 48, left 58, right 24.
- **Linear reference:** dashed (4/4) light gray `#e5e9ef` width-2 line following 12 × days (drawn until it exceeds y=55), labeled 12px gray "12 × days (feared)".
- **Sqrt curve:** orange `#d95926` width-3 smooth curve of 12 × √days from 1 to 16.
- **Marker points:** radius-5 orange dots at (1, 12), (4, 24), (9, 36), (16, 48), each with a bold 12px `#2c3e50` label "12 cups", "24 cups", "36 cups", "48 cups" above (the 16-day label nudged left).
- **Annotation:** bold 13px orange centered "16 days out = 4x tomorrow's miss".

## "Just Add More Data" Can Make It Worse

**Tags:** `common mistake` (red), `why it matters` (orange)

- **The instinct** — forecast is bad after the office building opened? Feed it more history!
- **2 months** — training only on post-change data: average miss 14 cups a day
- **1 year** — mixing in the old 252-cup world: miss grows to 27 cups
- **3 years** — mostly old-world data: miss 41 cups; more data, worse forecast
- **The rule** — after a regime change, old data describes a world that no longer exists

*Example:* The team kept extending the training window all quarter — each extension dragged the forecast back toward the dead 252-cup level.

**Key point:** More data only helps if it comes from the same world you are forecasting — after a regime change, recency beats volume.

### Visualization (canvas `c4`, 720×300)

Horizontal bar chart: forecast miss by training-window length after the regime change.

- **Title (bold 15px, `#1a5276`, top center):** "Average daily miss after the regime change, by training window (illustrative)"
- **Data:** labels `['last 2 months (new world only)', 'last 1 year (mixed)', 'last 3 years (mostly old world)']`, MAE `[14, 27, 41]`, bar colors green `#008300`, yellow `#c98500`, red `#e74c3c`.
- **Layout:** x scale 0–50 cups; padding top 56, bottom 40, left 268 (row labels right-aligned, 13px `#2c3e50`), right 80; bars 32px tall; bold 13px value labels ("14 cups", "27 cups", "41 cups") in the bar's color to the right of each bar; vertical gray `#999` axis line at x=0.
- **Annotation:** bold 13px red `#e74c3c` "more history = dragged toward the dead 252-cup level".

## Regeneration instructions

- **Template/layout:** tutorials topic page. `<h1>` (no index number) with 2px `#2980b9` bottom border, `.subtitle`, then 4 `.card-section` blocks: `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one `<tr>`: `td.text-col` (50%) holding `.tags` pills, one-line bold-term bullets, italic `.example`, and a `.key-point` callout; `td.viz-col` (50%) holding one canvas.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; ul 0.92rem; `li b` `#1a5276`; `.example` italic `#555` 0.9rem; `.key-point` background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius.
- **Tag pills:** `.tag` inline-block 0.72rem weight 600, padding 2px 10px, radius 10px; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Chart palette (JS object `P`):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Project palette anchors: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- **Canvas:** all four canvases 720×300 intrinsic; scale by `window.devicePixelRatio` via a shared `setup(id)` helper; all data arrays hardcoded (the sqrt curve is computed deterministically from the 12 × √days formula).
- No nav bar, no back/home links, no cross-page links. In regenerated HTML any card links use `.html` extensions (this page has none).
