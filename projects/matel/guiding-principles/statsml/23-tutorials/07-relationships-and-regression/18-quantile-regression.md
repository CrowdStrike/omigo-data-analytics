# Quantile Regression

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Quantile Regression

**Subtitle:** Ordinary regression predicts the average outcome — quantile regression draws the line for the 90th percentile, the line you need when the promise is "on time 9 times out of 10"

## One Delivery App, Two Lines Through the Same Dots

**Tags:** `core idea` (blue), `90th percentile` (orange), `mean vs quantile` (green)

- **The promise** — a delivery app must quote a time that is right for 9 orders out of 10
- **The data** — 24 past orders: three delivery times in minutes at each distance 1–8 km
- **Mean line** — the average time is 12 + 3×km, so 27 min at 5 km; half the dots sit above it
- **q90 line** — quantile regression fits 12 + 4×km, aiming to keep 9 in 10 dots at or below
- **Definition (after the example)** — it fits a chosen percentile of y at each x, not the average
- **Two questions** — the mean answers "typical time"; the q90 line answers "safe promise"

*Example (italic):* At 5 km the mean line says 27 minutes — but quoting 27 would leave roughly half the orders arriving late.

**Key point:** The mean line predicts the center of the dots; quantile regression draws the line for whichever percentile your question actually needs.

### Visualization (canvas `c1`, 720×300)

Scatter of the 24 delivery times vs distance with two fitted lines: the mean line (blue) and the 90th-percentile line (orange) diverging above it.

- **Title (bold 15px, `#1a5276`, top center):** "24 Past Deliveries: Mean Line vs 90th-Percentile Line (illustrative)".
- **Data (hardcoded):** distances/times — 1 km `[14, 15, 16]`, 2 km `[16, 18, 20]`, 3 km `[18, 21, 24]`, 4 km `[21, 23, 28]`, 5 km `[23, 26, 32]`, 6 km `[25, 29, 36]`, 7 km `[27, 32, 40]`, 8 km `[29, 35, 44]`.
- **Axes:** origin x=60, baseline y=250, plot width 600, height 195; x maps 0–9 km with ticks "1 km" … "8 km" (12px `#444`); y maps 0–50 min with ticks 0, 10, 20, 30, 40, 50 and 11px labels; axis lines 2px `#1a5276`.
- **Dots:** 4.5px filled circles, `rgba(42,120,214,0.75)`.
- **Mean line:** blue `#2a78d6` 3px from d=0.5 to d=8.5 using t = 12 + 3d; blue bold 12px label at right end "mean = 12 + 3×km".
- **q90 line:** orange `#d95926` 3px, t = 12 + 4d over the same span; orange bold 13px label "q90 = 12 + 4×km".
- **Annotation (orange bold 13px, upper left):** "the slowest orders ride the q90 line".
- **Caption (12px `#444`, bottom center):** "same 24 dots — two lines answer two different questions".

## Finding the 32-Minute Promise by Hand

**Tags:** `worked example` (blue), `pinball loss` (green)

- **The slice** — all 12 five-km orders on record, sorted: 21, 23, 24, 25, 26, 26, 27, 28, 29, 30, 32, 33 min
- **Tilted penalty** — a promise pays 0.9 per minute late but only 0.1 per minute early
- **Try 27 (the mean)** — late minutes 17 × 0.9 plus early minutes 17 × 0.1 = 17.0 penalty
- **Try 32** — late 1 × 0.9 plus early 61 × 0.1 = 7.0, the lowest of any candidate promise
- **Read it off** — 32 is the 11th of the 12 sorted values, i.e. the sample 90th percentile
- **On time** — promising 32 gets 11 of 12 orders on time; promising 27 gets only 7 of 12

*Example (italic):* Under the tilted loss, candidate promises 27, 30, 32, 34 score 17.0, 8.6, 7.0, 8.4 — the dip lands exactly at 32.

**Key point:** Minimizing a 0.9/0.1 tilted ("pinball") loss lands exactly on the 90th percentile — the asymmetric penalty is the whole trick behind quantile regression.

### Visualization (canvas `c2`, 720×300)

Dual panel split by a vertical dashed divider at x=360: the tilted penalty function (left) and the total loss for four candidate promises (right).

- **Title (bold 15px, `#1a5276`, top center):** "A 0.9/0.1 Penalty Finds the 90th Percentile".
- **Left panel (tilted loss):** axis origin x=55, width 280, baseline y=240, chart height 175; x maps error −10 (early) to +10 (late) minutes with 0 centered at x=195; y maps penalty 0–9. Early arm: green `#008300` 3px line from (0, 0) to (−10, 1.0), labeled green bold 12px "early: 0.1 per min". Late arm: magenta `#d55181` 3px line from (0, 0) to (+10, 9.0), labeled magenta bold 12px "late: 0.9 per min". Ticks −10, 0, +10 (12px `#444`); caption 12px `#444` "the tilted (pinball) loss for q = 0.9".
- **Right panel (candidate losses):** bar chart, origin x=400, width 280, same baseline/height, y scale 0–18; candidates `[27, 30, 32, 34]` with losses `[17.0, 8.6, 7.0, 8.4]`; bar fill `rgba(42,120,214,0.45)` except the 32 bar in `rgba(0,131,0,0.5)`; loss value bold 12px above each bar; x labels "promise 27" … "promise 34" (12px `#444`); green bold 13px annotation "lowest at 32 = the 90th percentile"; caption "total tilted loss on the 12 deliveries at 5 km".
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=360 from y=38 to h-12.

## Why a Flat Buffer on the Mean Fails

**Tags:** `where it's used` (blue), `widening spread` (orange), `failure mode` (red)

- **The shortcut** — "fit the mean, add 5 minutes" gives one flat buffer for every distance
- **Widening spread** — times scatter about ±1 min at 1 km but about ±8 min at 8 km
- **Too loose near** — at 1 km the buffer quotes 20 min when 16 already covers 9 in 10
- **Too tight far** — at 6–8 km the slowest orders (36, 40, 44 min) blow past the mean+5 line
- **q90 adapts** — the quantile line's gap over the mean grows from 1 min to 8 min by itself
- **Same pattern** — delivery promises, hospital wait targets, server latency p99, flood levels

*Example (italic):* The same +5 buffer that wastes 4 minutes on a 1-km order still lets the 44-minute 8-km order arrive late.

**Key point:** When the spread of outcomes changes across x, no constant offset on the mean can track a percentile — quantile regression learns the widening funnel itself.

### Visualization (canvas `c3`, 720×300)

The same 24-dot scatter with three lines: the mean, the flat "mean + 5" buffer, and the q90 line — with the three deliveries that beat the flat buffer marked in red.

- **Title (bold 15px, `#1a5276`, top center):** "One Flat Buffer vs a Quantile Line".
- **Data:** same 24 points as `c1` — 1 km `[14, 15, 16]`, 2 km `[16, 18, 20]`, 3 km `[18, 21, 24]`, 4 km `[21, 23, 28]`, 5 km `[23, 26, 32]`, 6 km `[25, 29, 36]`, 7 km `[27, 32, 40]`, 8 km `[29, 35, 44]`.
- **Axes:** identical to `c1` — origin x=60, baseline y=250, width 600, height 195, x 0–9 km, y 0–50 min.
- **Dots:** 4.5px `rgba(42,120,214,0.75)`, except the three late-under-buffer points (6 km 36, 7 km 40, 8 km 44) drawn 5.5px solid red `#e74c3c`.
- **Mean line:** blue `#2a78d6` 2px, t = 12 + 3d, 12px blue label "mean".
- **Flat buffer:** gray `#6b7280` 2px dashed (dash 6/4), t = 17 + 3d, 12px gray label "mean + 5 min".
- **q90 line:** orange `#d95926` 3px, t = 12 + 4d, orange bold 12px label "q90".
- **Annotations:** red bold 12px near the red dots "late under the flat buffer"; gray bold 12px near 1 km "buffer wastes 4 min at 1 km".
- **Caption (12px `#444`, bottom center):** "a constant offset can't track a widening spread".

## A Percentile Is Not a Confidence Interval

**Tags:** `common mistake` (red), `CI vs quantile` (orange)

- **The mix-up** — a 90% confidence interval for the mean is not the 90th percentile of orders
- **CI shrinks** — with 10, 100, 1,000 orders the mean's 95% CI is ±2.1, ±0.7, ±0.2 minutes
- **q90 stays** — the 90th percentile sits near 32 min no matter how many orders you collect
- **Different targets** — the CI is uncertainty about the average; q90 is spread of real orders
- **The trap** — quoting "mean + CI" (about 27.2 min at n=1,000) still leaves 5 of 12 late

*Example (italic):* With a million orders the CI collapses to a hair around 27 minutes, yet one order in four still takes longer than 29.

**Common mistake:** Reading a tight confidence band as "orders won't vary much." More data sharpens your estimate of the mean; it does nothing to shrink how much deliveries actually vary.

### Visualization (canvas `c4`, 720×300)

Dual panel split by a vertical dashed divider at x=360: the mean's 95% CI narrowing as sample size grows (left) vs the 90th percentile staying put (right).

- **Title (bold 15px, `#1a5276`, top center):** "Confidence Interval of the Mean vs the 90th Percentile".
- **Shared y scale:** both panels map 24–34 minutes over chart height 175, baseline y=240; y ticks 24, 26, 28, 30, 32, 34 (11px `#444`, left panel only).
- **Left panel (CI of the mean):** origin x=55, width 280; three x positions labeled "n=10", "n=100", "n=1,000" (12px `#444`); blue `#2a78d6` 5px dots at mean 27 with 2px whiskers and caps spanning ±2.1, ±0.7, ±0.2; blue bold 12px annotation "CI shrinks with data"; caption 12px `#444` "95% CI of the mean time at 5 km".
- **Right panel (q90):** origin x=400, width 280; same three x positions; orange `#d95926` dashed (dash 6/4) horizontal reference line at 32 across the panel; orange 5px dots at 32 for all three n; orange bold 13px annotation, two lines: "q90 stays at 32 —" / "real spread doesn't shrink"; caption "90th percentile of delivery times at 5 km".
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=360 from y=38 to h-12.

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (here it hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
