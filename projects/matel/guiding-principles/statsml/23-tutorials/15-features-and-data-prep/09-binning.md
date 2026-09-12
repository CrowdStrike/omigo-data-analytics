# Binning

**Page type:** detail page (tutorial page: h1 + subtitle, 4 `.card-section` blocks each with an h2 and a two-column `table.layout` — text left 50%, canvas right 50%)
**HTML title tag:** Binning

**Subtitle:** Grouping a continuous value like age into buckets — what you gain in simplicity and lose in detail

## From 57 Different Ages to 4 Buckets

**Tags:** `core idea` (blue), `buckets` (orange), `discretization` (orange)

- **The table** — a customer table where the age column holds 57 distinct values from 18 to 74
- **The move** — replace each exact age with a bucket: 18–30, 31–45, 46–60, or 61+
- **Four values** — the column now takes only 4 values instead of 57; age 27 becomes "18–30"
- **New name, old idea** — data people say binning or discretization; a survey says "age group"
- **Deliberate blur** — everyone inside a bucket is treated as identical from now on

*Example:* A 22-year-old student and a 29-year-old engineer both become the same value: "18–30".

**Key point:** Binning trades detail for simplicity on purpose — the whole question is whether the detail you blurred away was carrying signal.

### Visualization (canvas `c1`, 720×300)

Age histogram with bars painted in four bucket colors, separated by dashed boundary lines.

- **Title (bold 16px, `#1a5276`, top center):** "Customer Ages (18–74) Painted into 4 Buckets  (illustrative)".
- **Axes:** L-shaped gray (`#6b7280`) axes; padding top 56, bottom 62, left 60, right 30; x maps ages 18–75; y scale max 45.
- **Histogram:** 5-year bars starting at ages [18, 23, 28, 33, 38, 43, 48, 53, 58, 63, 68] with counts [30, 42, 38, 30, 24, 18, 14, 10, 8, 5, 3], each filled at 0.8 alpha in its bucket's color — buckets 18–30 blue `#2a78d6`, 31–45 aqua `#199e70`, 46–60 violet `#4a3aa7`, 61+ orange `#d95926`.
- **Boundary lines:** dashed (`#2c3e50`, dash 5/4, width 1.5) vertical lines at ages 30.5, 45.5, 60.5 from plot top to baseline.
- **Bucket labels:** bold 13px, in bucket color, near plot top at ages 24, 38, 53, 68: "18–30", "31–45", "46–60", "61+".
- **X tick labels:** ages 20 to 75 step 10, 12px `#2c3e50`; axis label "age" centered below.
- **Caption (bold 13px violet `#4a3aa7`, bottom center):** "57 distinct ages in, 4 values out — every bar of a color becomes one value".

## Cutting 12 Customers Two Ways, by Hand

**Tags:** `worked example` (green), `quantiles` (blue)

- **The ages** — 19, 21, 22, 24, 25, 27, 29, 33, 38, 45, 58, 71 (already sorted)
- **Equal-width** — slice 19–71 into 4 equal 13-year spans: cuts at 32, 45, and 58
- **Lopsided counts** — the four bins hold 7, 2, 1, and 2 customers — one bin has almost everyone
- **Quantile bins** — instead cut after every 3rd customer: boundaries land at 23, 28, and 41.5
- **Even counts** — now each bin holds exactly 3 — but the last bin spans 45 to 71, a 26-year blur

*Example:* Same 12 customers, two cutting rules, two completely different stories about "the age groups".

**Key point:** Equal-width keeps bins the same size in years; quantile keeps them the same size in people — you can't have both, so pick per use.

### Visualization (canvas `c2`, 720×300)

Two horizontal dot rulers showing the same 12 ages cut by equal-width vs quantile rules.

- **Title (bold 16px, `#1a5276`, top center):** "Same 12 Ages, Two Cutting Rules".
- **Ages plotted as dots (radius 7):** 19, 21, 22, 24, 25, 27, 29, 33, 38, 45, 58, 71 on an axis mapping ages 15–75 from x=70 to width−50. Each dot colored by its resulting bin (left-closed bins: an age equal to a cut starts the next bin): bin colors in order blue `#2a78d6`, aqua `#199e70`, violet `#4a3aa7`, orange `#d95926`.
- **Top ruler (y=105):** bold 13px `#2c3e50` title above-left: "equal-width: four 13-year spans". Red dashed cut lines (`#e74c3c`, dash 4/3, width 2, spanning ±22px vertically) at ages 32, 45, 58 with bold red 12px labels "32", "45", "58" above. Right-aligned note below in bold 13px orange `#d95926`: "bin counts: 7, 2, 1, 2 — lopsided".
- **Bottom ruler (y=225):** title "quantile: cut after every 3rd customer"; red dashed cuts at 23, 28, 41.5 labeled "23", "28", "41.5". Note in bold 13px green `#008300`: "bin counts: 3, 3, 3, 3 — even, but last bin spans 45–71".
- **X tick labels:** ages 20 to 70 step 10 in muted 12px at y=265; "age" label centered at y=285.

## Why Bin at All: One Purchase Rate per Bucket

**Tags:** `where it's used` (blue), `rule of thumb` (green)

- **Readable tables** — "purchase rate by age group" fits in 4 rows a manager can act on
- **The numbers** — 18–30 buys at 4%, 31–45 at 9%, 46–60 at 14%, 61+ at 11% (illustrative)
- **Curved patterns** — a rise-then-dip like this is hard for a straight-line model; bins capture it
- **Outlier armor** — a typo age of 203 just lands in the 61+ bucket instead of wrecking a mean
- **Everywhere** — credit score bands, BMI classes, income brackets are all binning in disguise

*Example:* The pricing team never asked for a coefficient — they asked "which age group buys most?" and got row 3.

**Key point:** Inside each bucket the model's answer is flat — one number per bucket is exactly the simplicity you signed up for.

### Visualization (canvas `c3`, 720×300)

Step-bar chart of purchase rate per age bucket with the smooth underlying curve overlaid.

- **Title (bold 16px, `#1a5276`, top center):** "Purchase Rate by Age Bucket  (illustrative)".
- **Axes:** L-shaped gray axes; padding top 56, bottom 60, left 70, right 40; x maps ages 18–75; y from 0 to 16%, tick labels every 4% ("0%"…"16%") in muted 12px.
- **Bucket bars:** flat bars spanning their age ranges at 0.55 alpha in the bucket colors — [18,30] at 4% blue `#2a78d6`; [31,45] at 9% aqua `#199e70`; [46,60] at 14% violet `#4a3aa7`; [61,75] at 11% orange `#d95926`. Bold 13px `#2c3e50` rate labels above each bar ("4%", "9%", "14%", "11%") and bucket labels below the baseline ("18–30", "31–45", "46–60", "61+").
- **Underlying curve:** dashed (`#2c3e50`, dash 6/4, width 2.5) polyline through ages [18, 24, 30, 36, 42, 48, 54, 60, 66, 72] at rates [3.0, 4.5, 6.5, 8.8, 11.0, 13.2, 14.3, 13.6, 12.0, 10.4], labeled in bold 12px `#2c3e50`: "true smooth pattern".
- **Annotation (bold 13px magenta `#d55181`, near plot top at age 46):** "4 readable numbers — but flat inside each bucket".
- **Axis label:** muted 12px "age" centered below the x axis.

## The One Thing People Get Wrong: The Boundary Cliff

**Tags:** `common mistake` (red), `lost detail` (orange)

- **The cliff** — ages 30 and 31 land in different buckets and get treated as strangers
- **The blur** — ages 19 and 29 share a bucket and get treated as twins — a 10x wider gap
- **Too few bins** — 2 buckets flatten the rise-and-dip pattern into two crude steps
- **Too many bins** — 20 buckets of ~10 customers each turn real signal into noisy zigzag
- **Modern default** — strong models (trees, boosting) find their own cuts; don't pre-bin for them

*Example:* A discount rule keyed on "61+" gave a 61-year-old the deal and a 60-year-old nothing — one birthday apart.

**Key point:** Every bin boundary is a cliff you invented — look at the distribution first, and only bin when the audience or the model truly needs buckets.

### Visualization (canvas `c4`, 720×300)

Line chart contrasting the smooth true curve with the binned step function, highlighting the 30/31 cliff.

- **Title (bold 16px, `#1a5276`, top center):** "The Boundary Cliff at Age 30/31  (illustrative)".
- **Axes:** L-shaped gray axes; padding top 56, bottom 60, left 70, right 200 (legend space); x maps ages 18–50 with tick labels every 8 years; y 0–16%. Axis label "age" centered below.
- **True curve:** solid green (`#008300`, width 3) polyline through ages [18, 22, 26, 30, 34, 38, 42, 46, 50] at rates [3.0, 4.0, 5.4, 6.5, 8.1, 9.6, 11.2, 12.6, 13.6].
- **Binned model:** solid blue (`#2a78d6`, width 3) step function: flat at 4% from age 18 to 30.5, vertical jump to 9%, flat to 45, jump to 14%, flat to 50.
- **Cliff highlight:** orange (`#d95926`) 6px dots at (30, 4%) and (31, 9%), with bold 13px orange two-line annotation: "30 → 4%, 31 → 9%:" / "a cliff one birthday wide".
- **Sameness highlight:** magenta (`#d55181`) 5px dots at (19, 4%) and (29, 4%), with bold 12px label: "19 and 29: treated as twins".
- **Legend (right side, 12px):** green line swatch "true smooth pattern"; blue line swatch "binned model (steps)"; muted note "purchase rate, %".

## Regeneration instructions

- **Layout:** tutorial detail page. `<h1>` + `.subtitle` paragraph, then four `.card-section` divs, each an `<h2>` (bottom border `2px solid #2980b9`) followed by a `table.layout` with one `<tr>`: left `td.text-col` (50%) holding `.tags` pills, a `<ul>` of bold-term bullets, an italic `.example` line, and a `.key-point` callout; right `td.viz-col` (50%) holding one `<canvas width="720" height="300">`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6. h1 2rem `#1a5276` with `2px solid #2980b9` bottom border; `.subtitle` `#666` 0.95rem; h2 1.3rem `#1a5276`. `ul` 0.92rem; `li b` in `#1a5276`. `.example` italic `#555` 0.9rem. `.key-point` background `#f8f9fa`, left border `3px solid #1a5276`, padding 8px 12px, 0.9rem. Canvases `width:100%`, border `1px solid #e0e0e0`, radius 4px. No nav bar, no back/home links.
- **Tag pills:** `.tag` inline-block, 0.72rem bold, padding 2px 10px, radius 10px; `.tag.blue` background rgba(26,82,118,0.12) color `#1a5276`; `.tag.green` rgba(39,174,96,0.15) `#27ae60`; `.tag.red` rgba(231,76,60,0.12) `#e74c3c`; `.tag.orange` rgba(230,126,34,0.15) `#e67e22`.
- **Chart palette (JS object `P`):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Shared bucket arrays: `BINC = [blue, aqua, violet, orange]`, `BINL = ['18–30', '31–45', '46–60', '61+']`. Site palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- **Canvas:** all charts 720×300 logical, drawn via a shared `setup(id)` helper that sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- In regenerated HTML, any card links use `.html` extensions (this page has no outgoing links).
