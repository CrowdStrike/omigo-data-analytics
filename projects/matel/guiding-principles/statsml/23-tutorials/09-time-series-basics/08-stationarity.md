# Stationarity

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + `table.layout` two-column row, text left 45% / canvas right 55%; section 2 uses a three-column 38/31/31 variant with two canvases)
**HTML title tag:** Stationarity

**Subtitle:** A series is stationary when its typical level and spread stay put over time — most models quietly assume this, and growing data breaks it

## Growing Sales: the "Normal" Keeps Moving

**Tags:** `core idea` (blue), `running example` (green)

- **The series** — 36 months of a store's sales, climbing from about 100 to about 380
- **Ask "what's typical?"** — year 1 averages 145 per month; year 3 averages 335
- **No single answer** — the mean depends on when you look, so it isn't one number at all
- **The name** — a series whose mean (or spread) drifts like this is called non-stationary
- **Contrast** — a stationary series wobbles around one stable level with one stable spread

*Example:* Alerting on "sales below the historical average" flagged every month of year 1 and none of year 3.

**Key point:** Stationary means the statistical rules of the series — its mean and spread — do not change over time; growing sales break that.

### Visualization (canvas `c1`, 720×300)

Line chart of 36 months of growing sales with year-1 and year-3 mean segments.

- **Title (bold 15px, `#1a5276`, top center):** "36 months of sales: what counts as "average" keeps moving"
- **Data (`sales36`, comment: "36 months of growing sales (illustrative); year-1 mean 145, year-3 mean 335"):** `[100,128,107,139,113,132,163,148,186,159,168,196,184,230,207,209,232,217,267,254,255,273,250,300,296,301,320,288,332,332,343,368,332,367,366,379]`
- **Axes:** y from 50 to 420, tick labels 100, 200, 300, 400; x labeled "m1", "m12", "m24", "m36"; L-shaped `#999` axes; padding top 46, bottom 46, left 58, right 20.
- **Series:** blue `#2a78d6` line, width 2.
- **Mean segments:** orange `#d95926` solid lines width 3 — one at y=145 spanning months 1–12, one at y=335 spanning months 25–36; bold 13px orange labels "year-1 mean: 145" (left-aligned above the first segment) and "year-3 mean: 335" (right-aligned above the second).
- **Annotation:** bold 13px magenta `#d55181` text "the "typical value" moved by +190 — non-stationary" placed left in the upper plot area.
- **Caption (12px gray `#6b7280`, bottom center):** "monthly sales (illustrative)".

## Differencing by Hand: Look at the Changes Instead

**Tags:** `worked example` (green), `the standard fix` (blue)

- **Eight months** — sales: 100, 112, 108, 122, 130, 126, 138, 144
- **Subtract** — each month minus the one before: +12, −4, +14, +8, −4, +12, +6
- **The levels drift** — first four months average 110.5, last four average 134.5
- **The changes don't** — they hover around +6 the whole way through
- **The name** — this subtract-yesterday step is called differencing, the standard fix

*Example:* The question changed from "how big are sales?" (drifting) to "how much did they change?" (stable).

**Key point:** Differencing turns a drifting series into a stable one — model the changes, then add them back up if you need levels.

This row uses the three-column layout (`table.layout.three`, text 38%, two viz columns 31% each) with two canvases.

### Visualization (canvas `c2a`, 420×340)

Line-and-dot chart of the 8 monthly levels drifting upward.

- **Title (bold 15px, `#1a5276`, top center):** "The levels drift upward"
- **Data (`levels8`):** `[100, 112, 108, 122, 130, 126, 138, 144]`, x labeled m1–m8.
- **Axes:** y from 90 to 155, tick labels 100, 120, 140; L-shaped `#999` axes; padding top 50, bottom 50, left 52, right 16.
- **Half-mean lines:** dashed (6/4) orange `#d95926` width 2.5 segments — y=110.5 over points 1–4, y=134.5 over points 5–8; bold 12px orange labels "first-4 mean 110.5" (below its segment) and "last-4 mean 134.5" (above its segment).
- **Series:** blue `#2a78d6` line width 2.5 with radius-4.5 blue dots; each point's value printed above it (11px, `#2c3e50`).
- **Caption (12px gray, bottom center):** "monthly sales — the mean will not sit still".

### Visualization (canvas `c2b`, 400×340)

Bar chart of the 7 month-over-month differences.

- **Title (bold 15px, `#1a5276`, top center):** "The changes stay stable"
- **Data (`diffs7`):** `[12, -4, 14, 8, -4, 12, 6]`, x labeled m2–m8.
- **Axes:** y from −10 to 20, tick labels −10, 0, 10, 20; zero line in gray `#999`; padding top 50, bottom 50, left 52, right 16.
- **Bars:** width 32px with even gaps; positive bars `rgba(25,158,112,0.55)`, negative bars `rgba(213,81,129,0.55)`; signed value labels ("+12", "−4", …) 11px `#2c3e50` above/below each bar.
- **Mean-change line:** dashed (6/4) green `#008300` width 2.5 horizontal line at y=6.3.
- **Annotation:** bold 13px green centered near the top: "changes hover around +6 throughout".
- **Caption (12px gray, bottom center):** "month-over-month change — one stable level".

## Why Models Insist on It: Two Strangers, Correlation 0.98

**Tags:** `why it matters` (orange), `spurious correlation` (red)

- **Two series** — the store's monthly sales and an unrelated gym's memberships, both growing
- **Raw correlation** — 0.98, because any two climbing lines correlate, related or not
- **After differencing** — the month-to-month changes correlate at 0.04: nothing there
- **Models too** — regression and classic forecasting assume the level isn't drifting under them
- **Train vs test** — a model trained on year-1 levels meets year-3 levels it has never seen

*Example:* "Sales predict gym signups, r = 0.98!" — differencing showed the two had nothing to do with each other.

**Key point:** Trends manufacture correlation — check relationships and fit models on the stationary version, not the raw climb.

### Visualization (canvas `c3`, 720×300)

Two panels: raw levels of two unrelated growing series (left) and a scatter of their month-over-month changes (right).

- **Title (bold 15px, `#1a5276`, top center):** "Raw levels vs month-over-month changes: the correlation evaporates"
- **Divider:** dashed (4/3) vertical line `#bdc3c7` at x=370.
- **Data (comment: "two unrelated growing series (illustrative); level r = 0.98, diff r = 0.04"):**
  - `storeA`: `[100,118,99,130,131,122,156,143,149,177,157,179,193,176,209,206,200,234,218,229,254,233,259,268,253,287,280,279,311,293,308,329,309,338,343,331]`
  - `gymB`: `[50,54,57,63,65,72,72,80,79,89,87,98,94,106,102,115,109,123,117,132,124,140,132,149,139,157,147,165,155,173,162,182,170,190,178,198]`
- **Left panel (x0=45, width 290, top 70, height 155, y 0–400):** bold 13px title "raw levels: both climb"; storeA as blue `#2a78d6` line width 2.5 labeled "store sales" (bold 12px blue); gymB as aqua `#199e70` line width 2.5 labeled "gym members" (bold 12px aqua); bold 13px magenta caption below "r = 0.98 — but they are strangers"; gray "36 months".
- **Right panel (x0=420, width 260, top 70, height 155, both axes −40 to 40):** bold 13px title "the changes: a shapeless cloud"; scatter of (Δ storeA, Δ gymB) pairs computed as consecutive differences of the two arrays; dots radius 4, fill `rgba(74,58,167,0.65)`; light-gray `#e5e9ef` crosshair lines at zero; bold 13px violet `#4a3aa7` caption "r = 0.04 — nothing there"; gray "sales change vs member change".

## Stationary Does Not Mean Flat

**Tags:** `common mistake` (red), `rule of thumb` (blue)

- **Wobbly is fine** — a series bouncing noisily around 100 is stationary: the rules never change
- **Trend breaks it** — a drifting mean is the classic violation; differencing fixes it
- **Spread breaks it too** — a steady level whose swings keep growing is also non-stationary
- **Quick check** — split the series in half: do the halves share a similar mean and spread?
- **Fix by need** — difference for trend; a log transform when swings grow with the level

*Example:* The half-split check on the wild-looking series: both halves average close to 100 — stationary after all.

**Key point:** Stationarity is about stable behavior, not a flat line — noisy-but-stable passes; smooth-but-drifting fails.

### Visualization (canvas `c4`, 720×300)

Three small line panels: stable, drifting mean, growing spread.

- **Title (bold 15px, `#1a5276`, top center):** "Three shapes: only the mean AND spread staying put counts as stationary"
- **Data (comment: "three shapes (illustrative): stable, drifting mean, growing spread"):**
  - `statA`: `[102,97,93,103,90,89,99,90,109,99,97,104,91,104,103,88,98,90,87,92,103,96,95,104,106,90,87,93,106,100,114,113,108,87,98,91,102,112,108,108]`
  - `trendB`: `[65,70,67,59,67,65,77,70,82,87,77,88,85,96,92,94,103,90,103,98,98,104,101,118,115,114,113,121,114,126,127,122,123,132,135,142,133,138,148,141]`
  - `varC`: `[99,97,98,97,99,97,92,106,105,91,111,107,94,100,102,100,92,92,109,87,104,98,77,103,76,85,122,86,115,105,103,105,93,68,129,105,111,93,80,74]`
- **Panels:** each 200px wide at x0 = 30, 262, 494; plot top 66, height 150, y range 40–160; L-shaped `#999` axes; line width 2.
- **Panel 1:** statA in green `#008300`, bold 12px title "noisy around 100", bold 13px green verdict below "stationary ✓".
- **Panel 2:** trendB in orange `#d95926`, title "mean drifts upward", bold 13px red `#e74c3c` verdict "not stationary ✗".
- **Panel 3:** varC in magenta `#d55181`, title "spread keeps growing", bold 13px red `#e74c3c` verdict "not stationary ✗".
- **Caption (12px gray `#6b7280`, bottom center):** "half-split check on the left series: halves average 96 and 101 — wild-looking, but stable (illustrative)".

## Regeneration instructions

- **Template/layout:** tutorials topic page. `<h1>` (no index number) with 2px `#2980b9` bottom border, `.subtitle`, then 4 `.card-section` blocks: `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` — sections 1, 3, 4 use one `<tr>` with `td.text-col` (45%) and `td.viz-col` (55%, one canvas); section 2 uses `table.layout.three` with `td.text-col` 38% and two `td.viz-col` at 31% each (canvases c2a and c2b). Text cells hold `.tags` pills, one-line bold-term bullets, italic `.example`, and a `.key-point` callout.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; ul 0.92rem; `li b` `#1a5276`; `.example` italic `#555` 0.9rem; `.key-point` background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius.
- **Tag pills:** `.tag` inline-block 0.72rem weight 600, padding 2px 10px, radius 10px; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Chart palette (JS object `P`):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Project palette anchors: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- **Canvas:** intrinsic width/height attributes as given per chart (c1 720×300, c2a 420×340, c2b 400×340, c3 720×300, c4 720×300); scale by `window.devicePixelRatio` via a shared `setup(id)` helper; all data arrays hardcoded.
- No nav bar, no back/home links, no cross-page links. In regenerated HTML any card links use `.html` extensions (this page has none).
