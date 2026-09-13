# Data-Centric vs Model-Centric

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Data-Centric vs Model-Centric

**Subtitle:** When a prediction is bad, you can improve the algorithm or improve the data it learns from — and fixing the data is usually the bigger, cheaper win

## Two Teams, One Bad Forecast

**Tags:** `core idea` (blue), `two strategies` (green), `fix the input` (orange)

- **The bakery** — a baker forecasts tomorrow's loaves from her sales log, and lately she sells out early
- **Team model** — spends a week swapping the simple average for a fancier forecasting model
- **Team data** — spends an hour reading the log and finds closed Sundays recorded as 0-sale days
- **Model-centric** — better algorithm, same log: the daily shortfall shrinks from 9 loaves to 6
- **Data-centric** — same plain average, fixed log: the shortfall drops from 9 loaves to about 1
- **The names** — model-centric improves the algorithm; data-centric improves what it learns from

*Example (italic):* The fancy model studied the bad zeros harder; the deleted zeros never got studied at all — and deleting them won.

**Key point:** A model can only be as good as the log it learns from — improving the data raises the ceiling, improving the model only climbs toward it.

### Visualization (canvas `c1`, 720×300)

Single-panel vertical bar chart: three bars showing loaves short per day under the three approaches, with the data-centric bar visibly winning.

- **Title (bold 15px, `#1a5276`, top center):** "Loaves Short per Day: Where Did the Effort Go?".
- **Axes:** origin x=70, baseline y=245, plot width 590, plot height 185; y axis = loaves short per day 0 to 10, 12px `#444` tick labels at 0, 2, 4, 6, 8, 10 with light `#e5e9ef` gridlines.
- **Bars (width 110, centered at x = 190, 385, 580):** values `[9, 6, 1]` — bar 1 "dirty log + simple average" fill `rgba(217,89,38,0.55)` with 2px `#d95926` border; bar 2 "dirty log + fancy model (1 week)" fill `rgba(42,120,214,0.45)` with 2px `#2a78d6` border; bar 3 "clean log + simple average (1 hour)" fill `rgba(0,131,0,0.45)` with 2px `#008300` border.
- **Bar labels:** bold 14px value on top of each bar ("9", "6", "1") in the bar's border color; 12px `#444` two-line category labels below the baseline.
- **Annotation (bold 13px green `#008300`, near x=470, y=90):** two lines: "one hour on the data" / "beat one week on the model".
- **Caption (12px `#444`, bottom right):** "illustrative — the worked numbers are in the next section".

## Ten Rows, Two Zeros

**Tags:** `worked example` (blue), `hand-checkable` (green)

- **The log** — ten days of loaf sales: 42, 38, 45, 0, 41, 44, 0, 39, 43, 40
- **Dirty average** — all ten rows sum to 332, so 332 ÷ 10 = 33.2: the baker bakes 33 loaves
- **The zeros** — the two 0s are closed Sundays, not days when nobody wanted bread
- **Clean average** — drop those two rows: 332 ÷ 8 = 41.5, so bake 42 loaves
- **The gap** — two wrong rows dragged the forecast down by 9 loaves, about a fifth of real demand
- **The cost** — at 33 loaves baked, roughly 9 would-be buyers walk away empty-handed every day

*Example (italic):* Same arithmetic, same shop — the only change was deleting two rows, and the forecast jumped from 33 to 42 loaves.

**Key point:** 332 ÷ 10 = 33.2 versus 332 ÷ 8 = 41.5 — the entire improvement lived in the data; the "model" (a plain average) never changed.

### Visualization (canvas `c2`, 720×300)

Single-panel bar chart of the ten daily sales rows with the two bad zeros highlighted in red, and two dashed horizontal lines comparing the dirty and clean averages.

- **Title (bold 15px, `#1a5276`, top center):** "Ten Rows of the Sales Log — Two of Them Are Lies".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 185; y axis = loaves sold 0 to 50, 12px `#444` tick labels at 0, 10, 20, 30, 40, 50 with light `#e5e9ef` gridlines; x axis = 12px `#444` labels "day 1" ... "day 10" under each bar.
- **Bars (width 40, evenly spaced across the plot):** values `[42, 38, 45, 0, 41, 44, 0, 39, 43, 40]`; normal bars fill `rgba(42,120,214,0.45)` with 2px `#2a78d6` border; the two zero rows (days 4 and 7) drawn as red `#e74c3c` 2px-border empty slots of height 8 at the baseline with bold 12px red label "closed!" above each.
- **Dirty-average line:** horizontal dashed orange `#d95926` (dash 6/4) line at y-value 33.2 across the plot; bold 12px orange label at its left end: "dirty average 33.2".
- **Clean-average line:** horizontal dashed green `#008300` (dash 6/4) line at y-value 41.5; bold 12px green label at its right end: "clean average 41.5".
- **Annotation (bold 13px `#1a5276`, near x=330, y=70):** "deleting 2 bad rows moved the bake from 33 to 42 loaves".

## Why Fixing Data Beats Tuning Models

**Tags:** `where it's used` (blue), `error ceiling` (green), `cheap wins` (orange)

- **The ceiling** — a model only learns what the rows say; wrong rows set an error floor tuning alone won't break
- **One direction** — the zeros always pull the forecast down, so no clever averaging can cancel them out
- **Cheap vs costly** — deleting two rows took minutes; each extra week of model tuning buys less than the last
- **Same story elsewhere** — mislabeled photos, stuck sensors, test results typed into the wrong column
- **The habit** — before reaching for a bigger model, spend an hour reading actual rows of your data

*Example (italic):* The fancy model tuned for six weeks on the dirty log still under-baked by 6 loaves; the intern who deleted two rows beat it in an afternoon.

**Key point:** Bad rows put a hard floor under your error — model effort flattens out against that floor, while a data fix removes it in one move.

### Visualization (canvas `c3`, 720×300)

Single-panel line chart: forecast error versus weeks of model tuning on the dirty log, flattening against a floor, with one green point showing where the plain average lands once the log is cleaned.

- **Title (bold 15px, `#1a5276`, top center):** "Model Tuning Flattens Out — the Data Fix Jumps Below the Floor".
- **Axes:** origin x=70, baseline y=245, plot width 590, plot height 185; x axis = weeks of model tuning 0 to 6, 12px `#444` tick labels "0" ... "6" ("weeks of tuning" 12px `#444` centered below); y axis = loaves short per day 0 to 10, 12px `#444` tick labels at 0, 2, 4, 6, 8, 10 with light `#e5e9ef` gridlines.
- **Dirty-log tuning curve:** blue `#2a78d6` 3px line through hardcoded points at weeks `[0, 1, 2, 3, 4, 5, 6]`, error = `[9.0, 7.6, 6.9, 6.5, 6.2, 6.1, 6.0]`; 6px blue dots at each point; 12px blue label "fancier models, dirty log" near week 3 above the curve.
- **Error floor:** horizontal dashed orange `#d95926` (dash 4/3) line at error 6.0 across the plot; 12px orange label at its right end: "floor set by the 2 bad rows".
- **Clean-data point:** green `#008300` 8px dot at week 0, error 1.0, with a green dashed (dash 4/3) vertical drop to the baseline; bold 13px green label beside it: "clean log, plain average: 1".
- **Annotation (bold 13px `#d95926`, near x=420, y=150):** two lines: "wrong rows set a floor" / "tuning alone won't break".
- **Caption (12px `#444`, bottom right):** "illustrative — curve shape, not measured runs".

## More Data Is Not Better Data

**Tags:** `common mistake` (red), `bias vs noise` (orange)

- **The trap** — "we just need more data" often means collecting more rows with the same flaw in them
- **Ten times more** — 100 days logged the same way still write closed Sundays as 0; the average stays near 33
- **Bias survives volume** — more rows shrink random wobble, but an error that points one way stays put
- **Fix the source** — one change in the register ("skip closed days") repairs every future row for free
- **The test** — if you can name what is wrong with the rows, fix that before collecting more of them

*Example (italic):* After 100 days the baker had 100 rows and the same wrong 33-loaf forecast — the Sundays in it were still zeros.

**Common mistake:** Treating data volume and data quality as the same lever. Piling up rows averages away random noise, but the closed-Sunday zeros always pull the same direction — only fixing the logging removes them.

### Visualization (canvas `c4`, 720×300)

Three-row horizontal dot chart on a shared loaves axis: forecasts from 10 dirty rows, 100 dirty rows, and 8 clean rows, against a dashed line at real demand, showing that more dirty rows go nowhere.

- **Title (bold 15px, `#1a5276`, top center):** "10x More Rows vs 2 Fixed Rows — Same Sundays, Same Miss".
- **Axis:** horizontal 2px `#999` line at y=250 from x=250 to x=680 (width 430), loaves forecast 25 to 45; 12px `#444` tick labels "25", "30", "35", "40", "45" every 5.
- **Real-demand marker:** vertical dashed `#6b7280` (dash 4/3) line at 42 from y=60 to the axis, bold 12px `#6b7280` label "real demand ≈ 42" at its top.
- **Rows (y = 105, 160, 215), each with a left-aligned 12px `#444` label at x=20:**
  - "10 dirty rows": orange `#d95926` 8px dot at 33.2, bold 12px orange label "33.2" above it
  - "100 dirty rows (10x the data)": orange `#d95926` 8px dot at 33.4, bold 12px orange label "33.4" above it
  - "8 clean rows (2 deleted)": green `#008300` 8px dot at 41.5, bold 12px green label "41.5" above it, thin green dashed connector to the 42 line
- **Gap brace:** thin `#e74c3c` bracket between 33.4 and 42 alongside the middle row, 12px `#e74c3c` label "the bias 10x more rows never closed".
- **Annotation (bold 13px magenta `#d55181`, centered near y=285):** "volume shrinks noise — only a data fix removes bias".
- **Caption (12px `#444`, bottom right):** "illustrative — 100-row figure invented to match the logging flaw".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all bar values, curve points, and dot positions are the hardcoded arrays above (no randomness); the section-2 sales rows and both averages (332 ÷ 10 = 33.2, 332 ÷ 8 = 41.5) must appear identically in the text and in canvas `c2`; invented figures (c1 bars, c3 curve, the 33.4 in c4) keep their "illustrative" captions.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
