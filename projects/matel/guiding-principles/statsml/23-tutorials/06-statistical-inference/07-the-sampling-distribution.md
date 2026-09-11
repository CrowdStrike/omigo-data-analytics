# The Sampling Distribution

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** The Sampling Distribution

**Subtitle:** An average computed from a sample is itself a random number — the sampling distribution is the histogram it would trace out if you could redo the sample again and again

## One Coffee Shop, Eight Different Answers

**Tags:** `core idea` (blue), `estimate wobble` (orange)

- **The shop** — a manager pulls 25 receipts to estimate the average spend and gets $6.38
- **The rerun** — a second pull of 25 different receipts gives $6.10; a third gives $6.72
- **The wobble** — eight imaginary pulls give eight answers, ranging from $5.95 up to $6.81
- **The target** — every pull is aiming at the same true average, $6.30 (illustrative)
- **What moves** — the receipts in the till never change; the number computed from a sample does

*Example (italic):* One Monday's 25 receipts answered $6.38; the same question asked of 25 other receipts answered $6.10.

**Key point:** An estimate from a sample is itself a random number — redo the sample and it lands somewhere else. Everything else on this page follows from that.

### Visualization (canvas `c1`, 720×300)

Dollar number line with eight dots, one per imaginary sample mean, and a dashed vertical line at the true average.

- **Title (bold 15px, `#1a5276`, top center):** "Eight Pulls of 25 Receipts, Eight Different Averages (illustrative)".
- **Data:** sample means `[6.38, 6.10, 6.72, 5.95, 6.55, 6.28, 6.81, 6.05]` labeled "#1".."#8"; true average 6.30.
- **Axis:** horizontal 2px `#999` line at y=170 from x=70, width 580; range $5.80–$7.00; x = 70 + (v − 5.80) / 1.20 × 580; ticks every $0.20 with labels "$5.80".."$7.00" 12px `#444` below.
- **Dots:** 7px dots on the axis; sample #1 in orange `#d95926`, samples #2–#8 in blue `#2a78d6`; labels "#N  $X.XX" bold 12px in the dot's color, staggered above (y=140) for samples #1–#4 and below (y=200) for samples #5–#8 to avoid overlap.
- **Highlight:** orange bold 12px note near #1: "the one sample you actually took".
- **True line:** dashed `#1a5276` (dash 5/4) vertical at $6.30 from y=70 to y=220; bold 12px `#1a5276` label at top: "true average $6.30 — never seen in real life".
- **Takeaway (bold 13px magenta `#d55181`, centered at y=285):** "same shop, same question — eight different answers".

## Stacking 200 Reruns Into One Histogram

**Tags:** `worked example` (blue), `definition` (green)

- **The experiment** — imagine 200 managers, each averaging their own fresh pull of 25 receipts
- **The stack** — pile the 200 averages into one histogram; that picture has a formal name
- **The definition** — the sampling distribution is the histogram of an estimate across imaginary repeats
- **The center** — the 200 averages center exactly on $6.30, the true average they all aim at
- **The spread** — their standard deviation is about $0.44; this spread is called the standard error
- **The shape** — averages of 25 receipts stack into a bell even though single receipts do not

*Example (italic):* 148 of the 200 imaginary managers landed within $0.44 of the true $6.30 — one standard error.

**Key point:** The sampling distribution is not a histogram of data — it is the histogram of the estimate itself across imaginary repeats of the whole sampling process.

### Visualization (canvas `c2`, 720×300)

Histogram of the 200 imaginary sample means with the true average marked and the standard error bracketed.

- **Title (bold 15px, `#1a5276`, top center):** "200 Imaginary Repeats: the Histogram of the Sample Mean (illustrative)".
- **Data:** 13 bins of width $0.20 at centers `[5.10, 5.30, 5.50, 5.70, 5.90, 6.10, 6.30, 6.50, 6.70, 6.90, 7.10, 7.30, 7.50]` with counts `[1, 3, 7, 15, 25, 32, 34, 32, 25, 15, 7, 3, 1]` (sums to 200, symmetric around 6.30, sd ≈ $0.44 = receipt sd $2.18 ÷ √25).
- **Axes:** origin x=60, plot width 600, baseline y=240, chart height 180, y scale 0–40; bars fill `rgba(42,120,214,0.45)` with 1px `#2a78d6` stroke, 3px gap between bars.
- **X labels:** every other bin center labeled "$5.10", "$5.50", "$5.90", "$6.30", "$6.70", "$7.10", "$7.50" 12px `#444` below the baseline.
- **True line:** dashed `#1a5276` (dash 5/4) vertical through the $6.30 bin center from y=45 to baseline; bold 12px `#1a5276` label "true average $6.30".
- **SE bracket:** green `#008300` 3px horizontal bracket at y=62 spanning $5.86 to $6.74 (mean ± $0.44) with small end ticks; green bold 13px label above: "standard error ≈ $0.44".
- **Peak label:** bold 12px blue "34" above the tallest bar.
- **Caption (12px `#444`, bottom left):** "each bar counts managers; 25 receipts per manager".

## Why Bigger Samples Calm the Wobble

**Tags:** `where it's used` (blue), `rule of thumb` (green), `sample size` (orange)

- **Margin of error** — a poll's "±3 points" is just the spread of this histogram in disguise
- **The lever** — with 100 receipts per manager the spread halves, from $0.44 down to $0.22
- **The √n rule** — quadrupling the sample size halves the wobble; no size ever removes it
- **One real pull** — in real life you see one sample; theory tells you the histogram it came from
- **Downstream** — confidence intervals and p-values are both read directly off this histogram

*Example (italic):* Moving from 25 to 100 receipts per pull squeezed the 200 averages from a $5.10–$7.50 band into $5.70–$6.90.

**Key point:** You cannot shrink the wobble to zero, but the √n rule prices it exactly: four times the data buys half the spread.

### Visualization (canvas `c3`, 720×300)

Dual-panel histogram: the sampling distribution of the mean with n=25 per repeat (left) vs n=100 per repeat (right), same x and y scales, split by a dashed divider at x=360.

- **Title (bold 15px, `#1a5276`, top center):** "Same 200 Repeats, Bigger Samples: the Histogram Narrows (illustrative)".
- **Left panel (n=25):** bin centers `[5.10, 5.30, 5.50, 5.70, 5.90, 6.10, 6.30, 6.50, 6.70, 6.90, 7.10, 7.30, 7.50]` (bin width $0.20), counts `[1, 3, 7, 15, 25, 32, 34, 32, 25, 15, 7, 3, 1]`; axis origin x=50, width 290, baseline y=240, chart height 175, x range $5.00–$7.60, y scale 0–75; bars fill `rgba(42,120,214,0.45)`; blue bold 12px annotation "spread ≈ $0.44"; caption 12px `#444` "n = 25 per repeat"; x labels "$5.00", "$6.30", "$7.60".
- **Right panel (n=100):** bin centers `[5.70, 5.90, 6.10, 6.30, 6.50, 6.70, 6.90]` (bin width $0.20), counts `[2, 15, 48, 70, 48, 15, 2]` (sums to 200); axis origin x=395, width 290, same baseline/height, x range $5.60–$7.00 and y scale 0–75; bars fill `rgba(0,131,0,0.4)`; green bold 13px annotation, two lines: "4× the receipts →" / "half the spread ($0.22)"; caption "n = 100 per repeat"; x labels "$5.60", "$6.30", "$7.00".
- **X labels (both panels):** left edge, "$6.30" at center, right edge, 12px `#444` (per-panel edge values as above).
- **True lines:** dashed `#1a5276` (dash 5/4) vertical at $6.30 in both panels from y=50 to baseline.
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=360 from y=38 to h-12.

## The Receipts Histogram Is Not the Averages Histogram

**Tags:** `common mistake` (red), `rule of thumb` (green)

- **Two pictures** — 25 single receipts spread from $2 to $12; the 200 averages sit inside $5.10–$7.50
- **The mix-up** — people show the raw data's histogram when asked how uncertain the average is
- **Different spreads** — receipt-to-receipt spread stays put; average-to-average spread shrinks with n
- **Different shapes** — the receipts are right-skewed, yet their averages stack into a clean bell
- **Reading rule** — "how do spends vary?" uses the left picture; "how sure is $6.38?" uses the right

*Example (italic):* A report claimed the average spend was "uncertain by ±$2" by reading receipt spread, when the mean was good to about ±$0.44.

**Common mistake:** Quoting the spread of the raw data as the uncertainty of the average. The average wobbles far less than any single observation — that is the whole point of averaging.

### Visualization (canvas `c4`, 720×300)

Dual-panel histogram: the 25 individual receipts of sample #1 (left) vs the 200 sample means (right), split by a dashed divider at x=360.

- **Title (bold 15px, `#1a5276`, top center):** "Histogram of Receipts vs Histogram of Averages (illustrative)".
- **Left panel (raw receipts):** 10 bins of width $1 with edges $2–$12, centers `[2.5, 3.5, 4.5, 5.5, 6.5, 7.5, 8.5, 9.5, 10.5, 11.5]`, counts `[1, 2, 4, 5, 5, 2, 3, 1, 1, 1]` (sums to 25, bin-center mean $6.38); axis origin x=50, width 290, baseline y=240, chart height 175, y scale 0–6; bars fill `rgba(217,89,38,0.5)` with 1px `#d95926` stroke; edge labels "$2", "$4", "$6", "$8", "$10", "$12" 12px `#444`; dashed `#1a5276` vertical at $6.38 labeled bold 12px "mean $6.38"; magenta `#d55181` bold 12px annotation "single receipts: $2 to $12"; caption 12px `#444` "the 25 receipts of sample #1".
- **Right panel (sample means):** the c2 data — bin centers `[5.10, 5.30, 5.50, 5.70, 5.90, 6.10, 6.30, 6.50, 6.70, 6.90, 7.10, 7.30, 7.50]` (bin width $0.20), counts `[1, 3, 7, 15, 25, 32, 34, 32, 25, 15, 7, 3, 1]`; axis origin x=395, width 290, same baseline/height, x range $5.00–$7.60, y scale 0–40; bars fill `rgba(42,120,214,0.45)` with 1px `#2a78d6` stroke; x labels "$5.00", "$6.30", "$7.60" 12px `#444`; blue `#2a78d6` bold 13px annotation, two lines: "200 averages: all inside" / "$5.10–$7.50"; caption "one bar entry = one whole sample's mean".
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=360 from y=38 to h-12.

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (here it hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- All data is hardcoded literal arrays — no `Math.random()`; invented numbers carry an "(illustrative)" label in each chart title.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
