# Confidence Intervals, Three Ways

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Confidence Intervals, Three Ways

**Subtitle:** The same 95% confidence interval can be built by a formula (analytic), by resampling your own data (bootstrap), or by scoring candidate values (profile likelihood) — same question, three routes

## One Question: Where Is the True Average Receipt?

**Tags:** `core idea` (blue), `running example` (green)

- **The shop** — a coffee shop pulls 40 random receipts to learn its true average order value
- **The sample** — the 40 receipts average $6.20 with a spread (sd) of about $2.00
- **The tail** — most receipts sit near $5–$7, but one $12.40 group order stretches the right side
- **The problem** — another 40 receipts would give a different mean; $6.20 alone hides that wobble
- **The interval** — a 95% CI is a range built so the method traps the true mean 95% of the time

*Example (italic):* "Is the true average $6? $6.50?" — the shop needs a range, not one number, before repricing its pastries.

**Key point:** One sample gives one mean; a confidence interval turns it into a range of plausible true values. Three different methods can build that range.

### Visualization (canvas `c1`, 720×300)

Histogram of the 40 receipt values with the sample mean marked and the skewed tail annotated.

- **Title (bold 15px, `#1a5276`, top center):** "40 Coffee-Shop Receipts (illustrative)".
- **Data:** ten $1-wide bins from $3 to $13 with counts `[3, 8, 10, 8, 5, 3, 1, 1, 0, 1]` (sum 40).
- **Layout:** axis origin x=60, plot width 600, baseline y=245, chart height 185, y scale 0–12.
- **Bars:** fill `rgba(42,120,214,0.45)`, 1px `#2a78d6` stroke; bin edge labels "$3"…"$13" 12px `#444` below baseline; count labels 12px `#2a78d6` above bars with count ≥ 3.
- **Mean line:** vertical dashed `#1a5276` (dash 5/4) at the $6.20 position, bold 13px ink label "sample mean $6.20" beside it.
- **Annotation:** orange `#d95926` bold 12px near the $12–$13 bar, two lines: "one $12.40 group order" / "stretches the tail".
- **Caption (12px `#444`, bottom right):** "n = 40, sd ≈ $2.00".

## Way 1: The Formula

**Tags:** `worked example` (blue), `analytic` (green)

- **The recipe** — take the sample mean and step 1.96 standard errors out on each side
- **Standard error** — SE = sd / √n = 2.00 / √40 = $0.32, the typical wobble of a 40-receipt mean
- **The margin** — 1.96 × $0.32 = $0.62, so the interval is $6.20 ± $0.62
- **The answer** — analytic 95% CI: [$5.58, $6.82], perfectly symmetric around the mean
- **The catch** — it assumes the mean's wobble is normal-shaped; skewed data strains that at small n

*Example (italic):* With n = 40 the central limit theorem does the heavy lifting — even skewed receipts give a roughly normal mean, so the formula lands close.

**Key point:** The analytic CI is one line of arithmetic — mean ± 1.96 × sd/√n — and is exactly symmetric by construction.

### Visualization (canvas `c2`, 720×300)

Normal sampling-distribution curve for the mean, centered at $6.20 with SE $0.32, middle 95% shaded and the interval bracketed underneath.

- **Title (bold 15px, `#1a5276`, top center):** "The Formula's Picture: a Normal Curve for the Mean".
- **Data:** normal density with center 6.20 and sd 0.32, drawn over x range 5.20–7.20 (compute the curve pointwise; no randomness).
- **Layout:** axis origin x=60, plot width 600, baseline y=240, curve peak reaching y=70; x tick labels "$5.20", "$5.60", "$6.00", "$6.40", "$6.80", "$7.20" 12px `#444`.
- **Curve:** blue `#2a78d6` 3px line; region between x=5.58 and x=6.82 filled `rgba(42,120,214,0.25)`.
- **Cut lines:** vertical dashed `#1a5276` (dash 5/4) at $5.58 and $6.82 from baseline to the curve; bold 12px ink labels "$5.58" and "$6.82" at their feet.
- **Bracket:** green `#008300` 3px bracket below the baseline spanning $5.58–$6.82, bold 13px green center label "95% CI: [$5.58, $6.82]".
- **Annotation:** blue bold 12px above the shaded region: "mean ± 1.96 × SE ($0.62 each side)".
- **Caption (12px `#444`, bottom right):** "SE = $2.00 / √40 = $0.32".

## Way 2: Resample Your Own Data

**Tags:** `worked example` (blue), `bootstrap` (orange)

- **The move** — redraw 40 receipts with replacement from your own 40, average them, repeat 1,000 times
- **What you get** — 1,000 pretend-sample means: a do-it-yourself picture of the mean's wobble
- **The cut** — sort the 1,000 means and read the 2.5th and 97.5th percentiles: [$5.65, $6.92]
- **Asymmetry** — resamples that catch the $12.40 receipt twice pull their mean up; the right arm is longer
- **No formula** — the bootstrap never touches SE or 1.96; the data supply their own distribution

*Example (italic):* The interval reaches $0.55 below the mean but $0.72 above it — the skew in the receipts survives into the CI.

**Key point:** The bootstrap simulates the repeat-sampling you cannot afford in real life; the percentiles of the resampled means ARE the interval.

### Visualization (canvas `c3`, 720×300)

Histogram of 1,000 bootstrap means (fixed illustrative counts) with the two percentile cutoffs marked and the longer right arm annotated.

- **Title (bold 15px, `#1a5276`, top center):** "1,000 Bootstrap Means of the Same 40 Receipts (fixed illustrative)".
- **Data:** twelve $0.15-wide bins from $5.40 to $7.20 with counts `[8, 30, 75, 140, 185, 180, 145, 100, 65, 40, 22, 10]` (sum 1,000) — hardcode this array; do NOT generate resamples at render time.
- **Layout:** axis origin x=60, plot width 600, baseline y=240, chart height 175, y scale 0–200; edge labels "$5.40", "$5.70", "$6.00", "$6.30", "$6.60", "$6.90", "$7.20" 12px `#444` (every other edge).
- **Bars:** fill `rgba(0,131,0,0.4)`, 1px `#008300` stroke.
- **Cut lines:** vertical dashed magenta `#d55181` (dash 5/4) at $5.65 and $6.92 from baseline to y=55; bold 13px magenta labels "2.5%: $5.65" and "97.5%: $6.92" at their tops.
- **Mean line:** thin dashed `#1a5276` at $6.20 with 12px ink label "mean $6.20".
- **Annotation:** orange `#d95926` bold 12px on the right slope, two lines: "longer right arm:" / "$0.72 up vs $0.55 down".
- **Caption (12px `#444`, bottom right):** "percentile CI: [$5.65, $6.92]".

## Way 3: Profile the Likelihood — and the Scoreboard

**Tags:** `worked example` (blue), `comparison` (green), `common mistake` (red)

- **The move** — score each candidate mean by how well a skewed gamma model explains the 40 receipts
- **The peak** — the log-likelihood score is highest at $6.20, the sample mean itself
- **The cutoff** — keep every candidate whose score drops less than 1.92 below the peak (the χ² rule)
- **The answer** — the curve crosses the cutoff at $5.63 and $6.86: profile 95% CI [$5.63, $6.86]
- **Scoreboard** — analytic [$5.58, $6.82], bootstrap [$5.65, $6.92], profile [$5.63, $6.86]: within a dime

*Example (italic):* All three intervals contain $6.20 and roughly agree at n = 40; on 10 receipts instead, the three would visibly split apart.

**Common mistake:** Reading "95%" as a 95% chance the truth sits inside this one interval — 95% is the method's long-run catch rate over repeated samples, not a probability for the interval you got.

### Visualization (canvas `c4`, 720×300)

Two-part chart: profile log-likelihood curve with the 1.92-drop cutoff (top), and a scoreboard strip of the three intervals as horizontal bars (bottom).

- **Title (bold 15px, `#1a5276`, top center):** "Profile Curve and the Three-Way Scoreboard".
- **Top panel (profile curve):** x maps candidate mean $5.20–$7.20 to x=60–660; y maps relative log-likelihood 0 (peak) at y=42 down to −6.5 at y=160; muted 12px note at top left: "profiled model: a skewed gamma".
- **Curve data:** candidate means `[5.2, 5.4, 5.6, 5.8, 6.0, 6.2, 6.4, 6.6, 6.8, 7.0, 7.2]` with relative log-likelihoods `[-6.1, -3.8, -2.05, -0.95, -0.22, 0, -0.18, -0.78, -1.55, -2.75, -4.3]`; violet `#4a3aa7` 3px line through the points, 4px dot at the peak (6.2, 0).
- **Cutoff:** horizontal dashed red `#e74c3c` (dash 5/4) line at the −1.92 level from x=60 to x=660, bold 12px red label "drop of 1.92" at its right end; 5px red dots where the curve crosses it, at $5.63 and $6.86, each with a bold 12px ink label below ("$5.63", "$6.86").
- **Scoreboard strip (bottom, y=200–285):** three horizontal 3px interval bars with 5px endpoint dots, mapped on the same $5.20–$7.20 x scale; left-side 12px bold labels at x=62: "analytic" (blue), "bootstrap" (green), "profile" (violet).
- **Bars:** analytic `#2a78d6` spanning $5.58–$6.82 at y=210; bootstrap `#008300` spanning $5.65–$6.92 at y=237; profile `#4a3aa7` spanning $5.63–$6.86 at y=264; endpoint dollar labels 11px `#444` beside each end.
- **Mean line:** vertical dashed `#1a5276` at $6.20 through all three bars (y=200 to y=275).
- **Takeaway (bold 13px `#d55181`, bottom center):** "three methods, one story — all within a dime at n = 40".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (here it hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all arrays above are hardcoded illustrative values — no `Math.random()`; the bootstrap histogram counts and profile curve points must be reproduced exactly as listed. The c2 normal curve is computed deterministically from center 6.20 / sd 0.32.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
