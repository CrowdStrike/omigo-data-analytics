# Naive Baselines

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + `table.layout` two-column row, text left 50% / canvas right 50%)
**HTML title tag:** Naive Baselines

**Subtitle:** Dead-simple forecasts like "same as yesterday" that any fancy model must beat before it earns its keep

## "Tomorrow = Today": the Forecast That Needs No Math

**Tags:** `core idea` (blue), `last value` (green)

- **The shop** — a coffee shop sells 205 to 330 cups a day and wants a daily forecast
- **The naive rule** — predict tomorrow's cups = today's cups; no model, no training
- **Why it works** — most days look like the day before, so the rule is often close
- **Where it breaks** — every Saturday it predicts Friday's ~250 and misses the ~320 rush
- **The name** — this is the "last value" or naive forecast, the oldest baseline there is

*Example:* On Saturday week 1 the rule said 250 cups (Friday's number) — the shop actually sold 320, a miss of 70.

**Key point:** The naive forecast just repeats the last observed value — free, instant, and surprisingly hard to beat on quiet days.

### Visualization (canvas `c1`, 720×300)

Line chart: two weeks of actual cup sales vs the shifted "same as yesterday" forecast.

- **Title (bold 15px, `#1a5276`, top center):** "Cups sold vs the "same as yesterday" forecast (2 weeks)"
- **Data (`cups`, comment: "Two weeks of daily cup sales, Mon..Sun twice (illustrative)"):** `[210, 220, 215, 225, 250, 320, 300, 205, 225, 220, 230, 255, 330, 310]`; x labeled with weekday names Mon–Sun repeating.
- **Axes:** y from 180 to 360, tick labels 200, 250, 300, 350 with light `#e5e9ef` gridlines; L-shaped `#999` axes; padding top 46, bottom 46, left 58, right 20.
- **Naive forecast series:** the same data shifted right one day (value at day i is `cups[i-1]`, drawn from day 2 on), dashed (6/4) orange `#d95926` line width 2.5.
- **Actual series:** blue `#2a78d6` line width 3 with radius-3.5 blue dots.
- **Saturday misses:** vertical red `#e74c3c` width-2 segments at indices 5 and 12 connecting actual to the naive value.
- **Annotations:** bold 13px red text "misses every Saturday by 70+" near the first Saturday; bold 12px blue label "actual cups" and bold 12px orange label "naive: yesterday's value" at the left.

## "Same as Last Saturday": Copying the Week by Hand

**Tags:** `worked example` (green), `seasonal naive` (blue)

- **The rule** — predict each day of week 2 with the same weekday of week 1
- **Saturday** — week 1 sold 320, so forecast 320; week 2 actually sold 330; error 10
- **Weekdays** — Mon 210 vs 205, Tue 220 vs 225, Wed 215 vs 220: each off by just 5
- **Add the misses** — errors 5+5+5+5+5+10+10 = 45 cups over the week
- **Average** — 45 / 7 ≈ 6.4 cups of error per day; this is the seasonal naive baseline

*Example:* Copying last week's Friday (250) as this Friday's forecast missed the true 255 by only 5 cups.

**Key point:** Seasonal naive copies the value from one full cycle ago — it handles the weekend rush that "same as yesterday" always misses.

### Visualization (canvas `c2`, 720×300)

Paired bar chart: week-1 copy forecast vs week-2 actual for each weekday.

- **Title (bold 15px, `#1a5276`, top center):** "Week 2 forecast = week 1 copy: errors 5+5+5+5+5+10+10 = 45, ÷7 ≈ 6.4 cups"
- **Data:** forecast `fc = [210, 220, 215, 225, 250, 320, 300]` (week 1); actual `act = [205, 225, 220, 230, 255, 330, 310]` (week 2); errors `err = [5, 5, 5, 5, 5, 10, 10]`; x labeled Mon–Sun.
- **Scale:** bar heights proportional to value / 380 of plot height; baseline gray `#999` line; padding top 52, bottom 46, left 58, right 20.
- **Bars:** 24px wide, paired per day — forecast bar fill `rgba(42,120,214,0.4)` with blue `#2a78d6` outline; actual bar fill `rgba(0,131,0,0.4)` with green `#008300` outline.
- **Error labels:** bold 12px red `#e74c3c` "±5"/"±10" above each pair.
- **Legend (top left, 12px):** blue swatch "forecast (week 1 value)", green swatch "actual (week 2)"; bold 13px red note below: "copying last week is off by 5-10 cups a day".

## The Bar Every Model Must Clear

**Tags:** `why it matters` (orange), `benchmark` (blue)

- **Three forecasters** — last value, seasonal naive, and a trained ML model, same week
- **Last value** — average miss of about 36 cups a day, wrecked by the weekend swings
- **Seasonal naive** — average miss of about 6.4 cups, from one line of copy-paste
- **The ML model** — average miss of 8.2 cups: sounds fine, but loses to the free rule
- **The habit** — always compute the naive baselines first; they set the bar to clear

*Example:* A week of tuning produced a model that was 1.8 cups a day worse than copying last week.

**Key point:** A forecast error means nothing on its own — it only means something next to what a zero-effort baseline scores on the same data.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart of average daily miss (MAE) for three forecasters.

- **Title (bold 15px, `#1a5276`, top center):** "Average daily miss, week 2 (cups) — lower is better (illustrative)"
- **Data:** labels `['last value ("yesterday")', 'seasonal naive ("last Sat")', 'ML model (tuned)']`, MAE values `[35.7, 6.4, 8.2]`, bar colors orange `#d95926`, green `#008300`, violet `#4a3aa7`.
- **Layout:** x scale 0–40 cups; padding top 56, bottom 40, left 230 (row labels right-aligned at 13px `#2c3e50`), right 90; bars 30px tall, one per row; bold 13px value labels ("35.7 cups", "6.4 cups", "8.2 cups") in the bar's color to the right of each bar; vertical gray `#999` axis line at x=0.
- **Baseline marker:** dashed (5/4) green width-2 vertical line at the 6.4-cup position spanning the plot height.
- **Annotation:** bold 13px red `#e74c3c` text "the free copy rule beats the ML model" near the ML model row.

## "92% Accurate" Can Still Be Worthless

**Tags:** `common mistake` (red), `skill score` (blue)

- **The trap** — an impressive-sounding error can be worse than what a copy rule scores
- **Skill score** — skill = 1 − (model error / baseline error); above 0 means real value
- **Model A** — error 8.2 vs baseline 6.4 gives skill 1 − 8.2/6.4 = −0.28: negative, no value
- **Model B** — error 5.1 vs baseline 6.4 gives skill 1 − 5.1/6.4 = +0.20: 20% better
- **The question** — never ask "how accurate?"; ask "how much better than naive?"

*Example:* "Only 8.2 cups off per day" sounded great in the demo — until someone computed the baseline's 6.4.

**Key point:** If a model cannot beat "same as last Saturday", ship the copy rule — it is cheaper, faster, and easier to explain.

### Visualization (canvas `c4`, 720×300)

Vertical bar chart of skill scores above/below the zero line.

- **Title (bold 15px, `#1a5276`, top center):** "Skill vs the 6.4-cup baseline: skill = 1 − model miss / baseline miss"
- **Data:** models `['Model A (miss 8.2)', 'Model B (miss 5.1)']`, skill `[-0.28, 0.20]`, colors red `#e74c3c` and green `#008300`.
- **Axes:** y from −0.5 to +0.5, light `#e5e9ef` gridlines at −0.4, −0.2, 0.2, 0.4 with labels; a heavier ink `#1a5276` width-2 zero line labeled (bold 12px ink) "skill 0 = exactly as good as naive"; padding top 56, bottom 44, left 58, right 24.
- **Bars:** 130px wide, centered at 28% and 68% of plot width, drawn from zero up (Model B) or down (Model A); bold 13px signed value labels "−0.28" and "+0.20" at the bar ends; 12px `#2c3e50` model labels below the axis.
- **Annotations:** bold 13px red bottom-left "below zero: worse than free — do not ship"; bold 13px green top-right "+0.20 = 20% better than the copy rule".

## Regeneration instructions

- **Template/layout:** tutorials topic page. `<h1>` (no index number) with 2px `#2980b9` bottom border, `.subtitle`, then 4 `.card-section` blocks: `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one `<tr>`: `td.text-col` (50%) holding `.tags` pills, one-line bold-term bullets, italic `.example`, and a `.key-point` callout; `td.viz-col` (50%) holding one canvas.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; ul 0.92rem; `li b` `#1a5276`; `.example` italic `#555` 0.9rem; `.key-point` background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius.
- **Tag pills:** `.tag` inline-block 0.72rem weight 600, padding 2px 10px, radius 10px; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Chart palette (JS object `P`):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Project palette anchors: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- **Canvas:** all four canvases 720×300 intrinsic; scale by `window.devicePixelRatio` via a shared `setup(id)` helper; all data arrays hardcoded.
- No nav bar, no back/home links, no cross-page links. In regenerated HTML any card links use `.html` extensions (this page has none).
