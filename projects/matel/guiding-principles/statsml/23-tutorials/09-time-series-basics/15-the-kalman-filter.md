# The Kalman Filter

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** The Kalman Filter

**Subtitle:** When your prediction is noisy and your measurement is noisy, blend them — weighted by how much you trust each — and the blend beats both

## A Jumpy GPS on a Steady Van

**Tags:** `core idea` (blue), `noisy sensor` (orange), `prediction + measurement` (green)

- **The van** — a delivery van drives a straight road at a steady 10 m/s, so it gains 10 m each second
- **The GPS** — each fix jumps 4–7 m off the true spot: at second 2 the van is at 20 m, GPS says 26
- **The prediction** — "last estimate + 10 m" is a free second opinion that the van's physics gives us
- **The blend** — each second the filter nudges its own prediction partway toward the new GPS fix
- **The payoff** — raw GPS is off by 5.8 m on average; the blended track is off by about 1.2 m

*Example (italic):* At second 4 the filter predicts 39.7 m, GPS shouts 47 m, and the filter settles on 41.7 m — closer to the truth (40 m) than either one.

**Key point:** This loop — predict from a model, measure with a sensor, blend the two by trust — is the Kalman filter. The definition is just the recipe the van already used.

### Visualization (canvas `c1`, 720×300)

Single-panel line chart: true position (dashed), raw GPS fixes (dots), and the Kalman estimate (line) over ten seconds.

- **Title (bold 15px, `#1a5276`, top center):** "One Van, Three Tracks: Truth, GPS, Kalman (illustrative)".
- **Data:** seconds `[0,1,2,3,4,5,6,7,8,9]`; true position `[0,10,20,30,40,50,60,70,80,90]`; GPS fixes `[4,6,26,24,47,44,66,63,86,84]`; Kalman estimates `[4.0, 9.9, 22.1, 29.7, 41.7, 49.8, 61.3, 69.4, 80.9, 89.4]`.
- **Axes:** origin x=60, plot width 600, baseline y=245, chart height 190; y scale 0–95 m with gridlines (`#e5e9ef`) and 12px `#444` labels at 0, 25, 50, 75; x labels "0"–"9" (12px `#444`) below baseline, axis caption "seconds" centered.
- **True line:** mute `#6b7280` dashed (dash 5/4) 2px, small 12px label "true position" near its right end.
- **GPS fixes:** orange `#d95926` 5px dots, no connecting line; 12px orange label "raw GPS" near the t=2 dot.
- **Kalman line:** blue `#2a78d6` 3px line with 4px dots; 12px bold blue label "Kalman estimate" near its right end.
- **Annotation (bold 13px, `#008300`):** "GPS off 5.8 m avg → filter off 1.2 m avg" placed upper-left, clear of the lines.
- **Caption (12px `#444`, bottom right):** "van moves +10 m/s; GPS noise hardcoded, illustrative".

## One Second of Blending, by Hand

**Tags:** `worked example` (blue), `kalman gain` (green)

- **Two guesses** — at second 4 the model predicts 39.7 m (±4.8 m) and the GPS reads 47.0 m (±8.0 m)
- **Trust ratio** — the gain K = 23 / (23 + 64) ≈ 0.27, from the two variances 4.8² ≈ 23 and 8.0² = 64
- **The nudge** — estimate = 39.7 + 0.27 × (47.0 − 39.7) = 39.7 + 2.0 = 41.7 m
- **Sharper than both** — the blended spread shrinks to ±4.1 m, tighter than either input alone
- **Noisier sensor, smaller K** — a worse GPS lowers K, so the filter leans more on its prediction

*Example (italic):* The true position was 40.0 m — the GPS was 7.0 m off, the blend only 1.7 m off, and you can redo every step above with a pocket calculator.

**Key point:** The gain K = (prediction variance) / (prediction variance + measurement variance) is the whole trick: it says what fraction of the disagreement to concede to the sensor.

### Visualization (canvas `c2`, 720×300)

Three bell curves on one meters axis: the prediction, the GPS measurement, and the fused estimate sitting between them, taller and narrower.

- **Title (bold 15px, `#1a5276`, top center):** "Second 4: Blending Two Uncertain Guesses".
- **Data:** prediction curve mean 39.7, sigma 4.8; measurement curve mean 47.0, sigma 8.0; fused curve mean 41.7, sigma 4.1. Draw each as `y = peak × exp(−((x−mean)²)/(2σ²))` sampled at 1 px steps — deterministic, no randomness.
- **Axes:** x axis (meters) at y=250 from x=60 to x=680 mapping 22–70 m; ticks and 12px `#444` labels at 25, 30, 35, 40, 45, 50, 55, 60; curve peaks scale so the fused curve tops at y=70, and the other two peaks scale by 4.1/σ (prediction ≈ 0.85 of fused height, measurement ≈ 0.51).
- **Prediction curve:** blue `#2a78d6` 2.5px stroke, fill `rgba(42,120,214,0.12)`; bold 12px blue label "prediction 39.7 ±4.8" above its peak.
- **Measurement curve:** orange `#d95926` 2.5px stroke, fill `rgba(217,89,38,0.10)`; bold 12px orange label "GPS 47.0 ±8.0" above its peak.
- **Fused curve:** green `#008300` 3px stroke, fill `rgba(0,131,0,0.12)`; bold 13px green label "blend 41.7 ±4.1" above its peak.
- **Gain arrow:** magenta `#d55181` 2px horizontal arrow along y=95 from x(39.7) to x(41.7) with bold 12px magenta label "K = 0.27 of the gap" just above it.
- **Caption (12px `#444`, bottom right):** "the blend is tighter than either input".

## The Gain Is a Trust Dial That Settles

**Tags:** `rule of thumb` (green), `where it's used` (blue)

- **Start humble** — at second 0 the filter has no history, so it swallows the first GPS fix whole (K = 1.00)
- **Gains fall** — K runs 1.00, 0.52, 0.37, 0.30, 0.27, then eases down and settles near 0.22
- **Steady state** — once history has built up, each new GPS fix moves the estimate only about 22%
- **Self-tuning** — nobody picked 0.22; it falls out of the model noise and sensor noise levels
- **Everywhere** — GPS navigation, rocket guidance, robot vacuums, and asset trackers run this loop

*Example (italic):* Apollo-era engineers used exactly this recipe to steer to the Moon with sensors far noisier than a phone's GPS.

**Key point:** The Kalman gain is recomputed every step from the running uncertainties — that adaptive weighting, not any magic constant, is what makes the filter optimal for linear models with Gaussian noise.

### Visualization (canvas `c3`, 720×300)

Bar chart of the Kalman gain K at each second, decaying from 1.00 to a steady-state floor marked with a dashed line.

- **Title (bold 15px, `#1a5276`, top center):** "Kalman Gain per Second: From 'Trust the Sensor' to 'Mostly Trust Myself'".
- **Data:** seconds `[0,1,2,3,4,5,6,7,8,9]`; gains `[1.00, 0.52, 0.37, 0.30, 0.27, 0.25, 0.24, 0.23, 0.23, 0.22]`.
- **Axes:** origin x=60, plot width 600, baseline y=245, chart height 185; y scale 0–1.05 with gridlines (`#e5e9ef`) and 12px `#444` labels at 0, 0.25, 0.5, 0.75, 1.0; x labels "0"–"9" below bars, caption "seconds".
- **Bars:** fill `rgba(42,120,214,0.45)`, 1px blue `#2a78d6` stroke, ~44px wide, evenly spaced; each bar's K value as bold 12px blue label above it (two decimals).
- **Steady-state line:** green `#008300` dashed (dash 5/4) 2px horizontal line at K=0.22 across the plot; bold 12px green label "steady state K ≈ 0.22" above the line at the right.
- **Annotation (bold 13px, `#d95926`):** "second 0: no history, take the GPS whole" with a short arrow to the first bar.
- **Caption (12px `#444`, bottom right):** "gain recomputed each step from the uncertainties".

## The Model Can Lie: When the Van Parks

**Tags:** `common mistake` (red), `model mismatch` (orange)

- **The park** — at second 5 the van stops at 50 m, but the filter's model still adds 10 m every second
- **The overshoot** — estimates sail past: 58.9, 63.2, 68.6, 71.6 m while the van sits parked at 50 m
- **Why it drifts** — with K ≈ 0.22, each GPS fix cancels only a fifth of the model's phantom +10 m
- **Smooth ≠ true** — the filtered line stays clean and confident even while it is 21.6 m wrong
- **The fix** — real filters estimate velocity as part of the state, or raise K when GPS keeps disagreeing

*Example (italic):* For four straight seconds the GPS hovers near 50 m, yet the filter splits the difference with its stale model and drifts 21.6 m past the parked van.

**Common mistake:** Reading the smooth Kalman line as ground truth. The filter is only as good as its motion model — feed it "always +10 m/s" and it produces confidently wrong estimates the moment the van behaves differently.

### Visualization (canvas `c4`, 720×300)

Line chart replaying the van's trip, but the van parks at second 5: truth flattens, GPS hovers, and the fixed-model filter overshoots.

- **Title (bold 15px, `#1a5276`, top center):** "Van Parks at 50 m — the Fixed Motion Model Sails Past".
- **Data:** seconds `[0,1,2,3,4,5,6,7,8,9]`; true position `[0,10,20,30,40,50,50,50,50,50]`; GPS fixes `[4,6,26,24,47,44,56,44,53,47]`; Kalman estimates `[4.0, 9.9, 22.1, 29.7, 41.7, 49.8, 58.9, 63.2, 68.6, 71.6]`.
- **Axes:** origin x=60, plot width 600, baseline y=245, chart height 190; y scale 0–80 m with gridlines (`#e5e9ef`) and 12px `#444` labels at 0, 20, 40, 60, 80; x labels "0"–"9", caption "seconds".
- **Parked region:** shade x range for seconds 5–9 with `rgba(217,89,38,0.08)`; 12px `#d95926` label "van parked" at the top of the shaded band.
- **True line:** mute `#6b7280` dashed (dash 5/4) 2px, 12px label "true position" near its flat right end.
- **GPS fixes:** orange `#d95926` 5px dots, no connecting line, 12px orange label "raw GPS".
- **Kalman line:** magenta `#d55181` 3px line with 4px dots; bold 12px magenta label "filter (model: always +10 m/s)" near its right end.
- **Gap marker:** red `#e74c3c` vertical 2px bracket at second 9 from y(50) to y(71.6) with bold 13px red label "21.6 m wrong, still smooth".
- **Caption (12px `#444`, bottom right):** "same gain schedule as above; only the van changed".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data integrity:** all series, gains, and sigmas are the hardcoded arrays above — no `Math.random()`; the gaussian curves in `c2` are computed deterministically from the stated means and sigmas.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
