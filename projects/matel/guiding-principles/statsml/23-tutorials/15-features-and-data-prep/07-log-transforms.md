# Log Transforms

**Page type:** detail page (tutorial layout: h2 card-sections, two-column table with text left 50% / canvas right 50%)
**HTML title tag:** Log Transforms

**Subtitle:** Taming skewed values like income or page views by working with their logarithms instead

## One Viral Post Hides Every Other Article

**Tags:** `core idea` (blue), `skew` (orange), `long tail` (orange)

- **The blog** — 400 articles; most get 30–300 daily views, one viral post got about 40,000
- **Raw histogram** — one giant bar near zero, then a huge empty stretch out to the viral post
- **The trick** — replace each count with its logarithm: how many zeros the number has
- **Log histogram** — the same 400 articles now form a readable hill centered near 100 views
- **Long tail** — views, income, city sizes, file sizes all share this lopsided shape

*Example:* On the raw chart, an editor cannot tell a 40-view article from a 400-view one — both hide in the first bar.

**Key point:** A log transform swaps the question "how big?" for "how many times bigger?" — and skewed data almost always answers the second question with a nice hill.

### Visualization (canvas `c1`, 720×300)

Side-by-side histograms (vertical dashed divider `#bdc3c7`, dash 4/3, at x=360): raw views spike-plus-tail vs log-views hill.

- **Title (bold 16px, `#1a5276`, top center):** "400 Articles: Raw Views vs log₁₀(Views)  (illustrative)".
- **Left histogram (x=55, width 280, baseline y=240, chart height 165, scale max 400):** 9 bins of 5,000 views spanning 0–45,000 with counts `[396, 2, 1, 0, 0, 0, 0, 0, 1]`; nonzero bars get a 3px minimum height. Bars filled `rgba(26,82,118,0.35)` except the last (viral) bin in orange `#d95926`. X labels "0" and "45,000"; axis caption "daily views (raw)".
- **Left annotations:** ink bold 12px "396 of 400 in the first bar" near the top; orange bold 12px "viral: ~40,000" near the last bar.
- **Right histogram (x=400, width 280, same baseline/height, scale max 160):** 8 bins 0.5 wide from log₁₀ 1.0 to 5.0 with counts `[40, 140, 150, 55, 10, 3, 1, 1]`, bars green `#008300` at 75% alpha. X tick labels 1–5; axis caption "log₁₀(daily views)".
- **Right annotation (green bold 13px):** "same 400 articles — now a readable hill".

## Taking log₁₀ of Four Articles by Hand

**Tags:** `worked example` (green), `multiplicative` (blue)

- **Four articles** — 20, 40, 80, and 40,000 daily views; log₁₀ gives 1.30, 1.60, 1.90, 4.60
- **Equal steps** — 20→40 and 40→80 are both "×2", so both are +0.30 on the log ruler
- **Raw average** — (20+40+80+40,000)/4 = 10,035 views — a number describing no article at all
- **Log average** — mean of the logs is 2.35; undo the log: 10²·³⁵ ≈ 224 views
- **Geometric mean** — that 224 is the "typical" article; the raw mean was 45x too big

*Example:* Averaging salaries in a room with one billionaire gives the same nonsense — the log average does not.

**Key point:** On the log ruler, multiplying becomes adding — so "typical" is computed by averaging logs, and one giant value can no longer drag the answer.

### Visualization (canvas `c2`, 720×300)

Two number-line rulers: the four view counts on a raw 0–40,000 ruler (with the distorted arithmetic mean) and on a log₁₀ ruler (with even ×2 steps and the geometric mean).

- **Title (bold 16px, `#1a5276`, top center):** "Views 20, 40, 80, 40,000 on the Raw and the Log Ruler".
- **Raw ruler (y=100, from x=70 to w−50, label "raw ruler (0 to 40,000)" bold 13px above left):** blue (`#2a78d6`) dots (radius 7) at raw values `[20, 40, 80, 40000]`. Red (`#e74c3c`) vertical 2px marker at 10,035 labeled bold 12px above: "raw mean 10,035 — near nothing". Blue bold 12px labels: "20, 40, 80 crushed together" below left; "40,000" below right.
- **Log ruler (y=215, label "log₁₀ ruler (1 to 5)"):** ticks at 1–5; green (`#008300`) dots at `[1.30, 1.60, 1.90, 4.60]`, each labeled bold 12px above with its value.
- **Equal-step brackets:** violet (`#4a3aa7`) 2px segments under 1.30→1.60 and 1.60→1.90, labeled bold 12px violet: "each \"×2\" = the same +0.30 step".
- **Geometric-mean marker:** dashed green vertical line (dash 5/4) at 2.35, labeled bold 13px green (offset right to clear the ruler label): "log mean 2.35 → 10^2.35 ≈ 224 views: the typical article".

## Why Models Need It: The Viral Post Owns the Fit

**Tags:** `what goes wrong` (red), `where it's used` (blue)

- **The task** — predict ad revenue from page views for 10 articles (one of them the viral post)
- **Raw fit** — 9 articles form a blob near zero; the line is drawn through the one outlier
- **Squared error** — a 40,000-view miss costs millions of error units; a 40-view miss costs nothing
- **Log-log fit** — take logs of both columns and the 10 points spread into a clean straight line
- **Also common** — log income in credit models, log counts in regressions, log degree in graphs

*Example:* Removing the viral post changed the raw model's slope by 10x; the log model barely moved.

**Key point:** Without the log, the model is fitted to one article and 9 spectators — the transform gives every row a comparable say.

### Visualization (canvas `c3`, 720×300)

Side-by-side scatter plots with fitted lines (vertical dashed divider at x=360): raw fit pinned to the outlier vs clean log-log fit.

- **Title (bold 16px, `#1a5276`, top center):** "Predicting Ad Revenue from Views: Raw Fit vs Log-Log Fit".
- **Data (10 articles):** views `[20, 35, 50, 80, 120, 180, 260, 400, 600, 40000]`; revenue `[0.25, 0.32, 0.55, 0.75, 1.3, 1.7, 2.4, 4.5, 5.5, 490]`.
- **Left scatter (x=60, width 275, baseline y=238, chart height 168; x scale 0–50,000, y scale 0–500):** red (`#e74c3c`) 2px fitted line from (0,0) to (40800,500) (slope ≈ 0.0123 $/view, pinned by the outlier); 9 blue (`#2a78d6`) dots radius 4 plus the viral point as an orange (`#d95926`) dot radius 6. Annotations: red bold 12px "line pinned to one point"; blue bold 12px "9 articles = one blob"; axis caption "views (raw)".
- **Right scatter (x=405, width 265; x scale log 1–5, y scale log −1 to 3.2):** log views `[1.30, 1.54, 1.70, 1.90, 2.08, 2.26, 2.41, 2.60, 2.78, 4.60]`; log revenue `[-0.60, -0.49, -0.26, -0.12, 0.11, 0.23, 0.38, 0.65, 0.74, 2.69]`. Green (`#008300`) 2px fitted line from (1.2, −0.73) to (4.8, 2.75); blue dots with viral point in orange. Annotations: green bold 13px "all 10 points get a vote"; orange bold 12px two lines "viral post: now just" / "the last point in line"; axis caption "log₁₀(views)".

## The One Thing People Get Wrong: log(0)

**Tags:** `common mistake` (red), `rule of thumb` (green)

- **Zeros happen** — a brand-new article has 0 views, and log(0) is undefined (minus infinity)
- **The fix** — use log(x+1), called log1p: it maps 0→0 and barely changes larger values
- **Negatives** — log cannot handle negative numbers at all; profit/loss columns need another tool
- **Back-transform** — undoing the log on an average prediction gives the typical value, not the total
- **Any base works** — log₁₀, log₂, natural log only differ by a constant; models don't care

*Example:* A nightly job crashed every time a zero-view article appeared — one character, log1p, fixed it.

**Key point:** Log is not a beauty filter for all data — it needs positive values, it won't fix every shape, and totals must be forecast on the raw scale.

### Visualization (canvas `c4`, 720×300)

Function plot: log₁₀(x) diving to −∞ at zero vs log₁₀(x+1) passing safely through the origin.

- **Title (bold 16px, `#1a5276`, top center):** "Zero Views Breaks log(x) — log(x+1) Handles It".
- **Axes:** x from 0 to 10 (muted 12px tick labels every 2, axis caption "views"), y from −1.2 to 1.2 in log₁₀ units with the y=0 line drawn as the horizontal axis; padding top 52, bottom 52, left 70, right 220.
- **Curves:** log₁₀(x) in red `#e74c3c`, 3px, plotted from x=0.07 to 10 (clipped below y=−1.2) — dives toward minus infinity; log₁₀(x+1) in green `#008300`, 3px, plotted from x=0 to 10, passing through (0,0).
- **Markers/annotations:** green dot radius 6 at (0,0) with bold 13px label "log(0+1) = 0 — safe"; red bold 13px near the diving curve "log(0) = −∞ — crash"; violet (`#4a3aa7`) bold 12px near the top "the two curves agree once x is large".
- **Legend (upper right, x=w−200):** red line swatch "log₁₀(x)"; green line swatch "log₁₀(x+1) = log1p".

## Regeneration instructions

- **Template:** tutorial topic page (tutorials/CLAUDE.md conventions). `<h1>` concept name, `.subtitle`, four `.card-section` blocks each `<h2>` + `table.layout` with one `<tr>`: `td.text-col` (50%) text, `td.viz-col` (50%) one 720×300 canvas.
- **Left column structure per section:** `.tags` pill row, `<ul>` of one-line bullets with `<b>` lead terms (colored `#1a5276`), italic `.example` line, `.key-point` callout (background `#f8f9fa`, left border `3px solid #1a5276` on this page, padding 8px 12px, 0.9rem) with bold "Key point:" lead-in.
- **Tag pill CSS:** inline-block, 0.72rem, weight 600, padding 2px 10px, radius 10px; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` bottom border; h2 1.3rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem; canvas `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300 (this page's `setup(id)` helper hardcodes 720×300), scaled by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates). Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object `P`:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- Card links in regenerated HTML (if referenced from grids) use `.html` extensions.
