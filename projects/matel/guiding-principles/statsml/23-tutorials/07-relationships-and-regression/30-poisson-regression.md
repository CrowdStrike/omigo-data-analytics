# Poisson Regression

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Poisson Regression

**Subtitle:** A regression built for counts — it predicts the average number of events per time slot, and its log link means predictors multiply the count instead of adding to it

## Counting Iced Coffees by the Hour

**Tags:** `core idea` (blue), `count data` (green), `where it's used` (orange)

- **The shop** — a coffee shop logs iced coffees sold each hour, 8am–5pm: whole numbers, never negative
- **Cool day (18°C)** — hourly counts 3, 4, 2, 5, 4, 3, 5, 4, 3, 4; the average is 3.7 per hour
- **Warm day (31°C)** — counts 12, 15, 11, 18, 14, 16, 10, 17, 13, 15; the average jumps to 14.1
- **Spread grows too** — the cool day wobbles within 2–5, the warm day within 10–18
- **Poisson regression** — a model that predicts the average count per hour from things like temperature

*Example (italic):* The warm day is not just busier on average — its hour-to-hour swings are wider too, which is exactly what count data does.

**Key point:** Counts are non-negative whole numbers whose spread grows with their average. Poisson regression is the regression designed around both facts.

### Visualization (canvas `c1`, 720×300)

Dot plot of hourly iced-coffee counts for the two days on one shared time axis, with a dashed mean line per day.

- **Title (bold 15px, `#1a5276`, top center):** "Iced Coffees per Hour: a Cool Day vs a Warm Day".
- **Data:** hours "8a"–"5p" (10 slots); cool-day counts `[3, 4, 2, 5, 4, 3, 5, 4, 3, 4]`; warm-day counts `[12, 15, 11, 18, 14, 16, 10, 17, 13, 15]`.
- **Axes:** origin x=60, plot width 600, baseline y=250, chart height 195, y scale 0–20 with gridlines `#e5e9ef` and 12px `#444` labels at 0, 5, 10, 15, 20; hour labels 12px `#444` below baseline.
- **Cool day:** blue `#2a78d6` 5px dots at each hour; dashed blue mean line (dash 4/3) at y for 3.7 labeled "mean 3.7" bold 12px blue, right-aligned inside the plot just below the line's right end.
- **Warm day:** orange `#d95926` 5px dots; dashed orange mean line at y for 14.1 labeled "mean 14.1" bold 12px orange, right-aligned inside the plot just below the line's right end.
- **Annotation (bold 12px, `#d55181`, near y for 17):** "bigger average → bigger wobble (10–18 vs 2–5)".
- **Legend (top right, 12px):** blue dot "cool day 18°C", orange dot "warm day 31°C".
- **Caption (12px `#444`, bottom center):** "hourly counts, illustrative".

## The Log Link: Heat Multiplies Sales

**Tags:** `worked example` (blue), `log link` (green), `multiplicative change` (orange)

- **The model** — log(average cups/hour) = 0.17 + 0.081 × temp; the log makes effects multiply
- **Read the slope** — exp(0.081 × 5) = 1.5, so every +5°C multiplies the hourly average by 1.5
- **Check by hand** — 15°C → 4 cups, 20°C → 6, 25°C → 9, 30°C → 13.5, 35°C → 20.3 (each ×1.5)
- **Never negative** — exp of anything is positive, so the predicted count can never dip below zero
- **Observed hours** — actual counts 3, 7, 8, 14, 19 scatter around the curve, as counts always do

*Example (italic):* From a 25°C afternoon to a 35°C heatwave the model says ×1.5 twice: 9 → 13.5 → 20.3 cups per hour.

**Key point:** The log link turns "add 0.081 to the log" into "multiply the count by 1.5 per 5°C". Poisson coefficients are read as multipliers, not amounts.

### Visualization (canvas `c2`, 720×300)

Exponential model curve of average cups/hour vs temperature with observed counts as dots and ×1.5 step brackets between the five model points.

- **Title (bold 15px, `#1a5276`, top center):** "Model: Every +5°C Multiplies Iced-Coffee Sales by 1.5".
- **Data:** temps `[15, 20, 25, 30, 35]`; model means `[4, 6, 9, 13.5, 20.3]`; observed counts `[3, 7, 8, 14, 19]`; smooth curve from mean(t) = 4 × 1.5^((t−15)/5) sampled at every 1°C from 15 to 35.
- **Axes:** origin x=60, plot width 590, baseline y=250, chart height 195, x maps 15–35°C with labels "15°", "20°", "25°", "30°", "35°" (12px `#444`); y scale 0–22 with gridlines at 0, 5, 10, 15, 20.
- **Curve:** green `#008300` 3px line through the sampled points; 5px green dots at the five model means, each labeled bold 12px green above ("4", "6", "9", "13.5", "20.3").
- **Observed:** blue `#2a78d6` 5px dots at (temps, observed counts); 12px blue legend entry top left "observed hours".
- **Step brackets:** violet `#4a3aa7` bold 12px "×1.5" labels centered between consecutive model dots, each with a small violet elbow connector.
- **Annotation (bold 13px green, upper left):** "log link: slopes multiply, curve never goes below 0".
- **Caption (12px `#444`, bottom center):** "log(mean) = 0.17 + 0.081 × temp (illustrative fit)".

## Offsets: Comparing Days of Different Lengths

**Tags:** `worked example` (blue), `rates & offsets` (green), `common mistake` (red)

- **Raw counts mislead** — Monday sells 40 cups, Friday 55, Saturday 66; Saturday looks 65% busier
- **Different exposure** — Monday is open 8h, Friday 10h, Saturday 12h; the days are different lengths
- **Rates fix it** — per open hour it is 5.0, 5.5, and 5.5 cups; Saturday is only 10% busier
- **The offset** — adding log(hours open) with a fixed coefficient of 1 makes the model predict rates
- **Same trick** — accidents per mile driven, defects per 1,000 units, clicks per impression

*Example (italic):* Saturday's 66 cups beat Monday's 40 mostly because the shop was open 4 extra hours, not because customers changed.

**Key point:** When observation windows differ, model the rate, not the raw count. The offset log(exposure) does exactly that inside a Poisson regression.

### Visualization (canvas `c3`, 720×300)

Dual-panel bar chart: raw daily counts (left) vs per-hour rates (right) for the same three days, split by a vertical dashed divider at x=360.

- **Title (bold 15px, `#1a5276`, top center):** "Same Three Days: Raw Counts vs Cups per Open Hour".
- **Data:** days "Mon (8h)", "Fri (10h)", "Sat (12h)"; counts `[40, 55, 66]`; rates `[5.0, 5.5, 5.5]`.
- **Left panel (counts):** axis origin x=55, width 280, baseline y=240, chart height 175, y scale 0–70; three bars fill `rgba(42,120,214,0.45)` with bold 12px blue value labels "40", "55", "66" above; day labels 12px `#444` below; magenta `#d55181` bold 12px annotation above the Sat bar: "looks 65% busier"; caption 12px `#444` "cups per day".
- **Right panel (rates):** axis origin x=400, width 280, same baseline/height, y scale 0–7; bars fill `rgba(0,131,0,0.4)` with bold 12px green labels "5.0", "5.5", "5.5"; green bold 13px annotation "per hour: only +10%"; caption "cups per open hour = count ÷ hours".
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=360 from y=38 to h-12.

## Why Not Just a Straight Line?

**Tags:** `common mistake` (red), `rule of thumb` (green)

- **The straight fit** — a linear regression on the same data gives cups = 0.6 × temp − 4.2
- **Impossible orders** — at 5°C it predicts −1.2 cups and at 0°C it predicts −4.2 cups per hour
- **Equal-wobble lie** — a straight-line fit assumes the same spread at 3 cups/hour as at 20
- **Overdispersion** — real counts often wobble more than Poisson expects; check, then use a fix
- **Rule of thumb** — if the outcome is a count of events, start from Poisson, not ordinary regression

*Example (italic):* The intern's linear model forecast −4 iced coffees for a 0°C morning; the Poisson curve forecast a small but positive 1.2.

**Common mistake:** Running ordinary linear regression on counts. It happily predicts negative events and assumes constant spread — both wrong for counts by construction.

### Visualization (canvas `c4`, 720×300)

Linear fit vs Poisson curve over an extended temperature range, with the linear line dipping into a shaded impossible negative region.

- **Title (bold 15px, `#1a5276`, top center):** "Straight Line vs Poisson Curve on the Same Counts".
- **Data:** observed points at temps `[5, 10, 15, 20, 25, 30, 35]` with counts `[1, 2, 3, 7, 8, 14, 19]`; linear fit y = 0.6 × temp − 4.2 drawn from 0°C to 35°C; Poisson curve mean(t) = 4 × 1.5^((t−15)/5) sampled every 1°C from 0 to 35 (value 1.2 at 0°C).
- **Axes:** origin x=60, plot width 590, x maps 0–35°C with labels "0°", "5°", ... "35°" (12px `#444`); y scale −6 to 20, zero line y0 as a solid 2px `#999` horizontal line labeled "0 cups"; chart top y=45, bottom y=265.
- **Negative region:** below the zero line, fill `rgba(231,76,60,0.08)` with 11px `#e74c3c` label "impossible: negative counts" at bottom left.
- **Observed:** blue `#2a78d6` 5px dots at the seven points.
- **Linear fit:** magenta `#d55181` 3px line; bold 12px magenta annotation with elbow at its 0°C end: "predicts −4.2 cups at 0°C".
- **Poisson curve:** green `#008300` 3px line; bold 12px green annotation near its left end: "stays positive: 1.2 at 0°C".
- **Legend (top left, 12px):** magenta line "linear fit y = 0.6·temp − 4.2", green line "Poisson curve", blue dot "observed hours".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
