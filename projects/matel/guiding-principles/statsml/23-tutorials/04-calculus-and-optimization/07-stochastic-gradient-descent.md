# Stochastic Gradient Descent

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Stochastic Gradient Descent

**Subtitle:** You rarely need the exact downhill direction — a thousand cheap, noisy steps computed from single examples reach the bottom faster than one perfect step that reads all the data

## One Knob, Fifty Thousand Receipts

**Tags:** `core idea` (blue), `noisy steps` (orange), `worked example` (green)

- **The app** — a courier app predicts delivery time as w × distance in km; w is its only knob
- **The bowl** — average squared error over 50,000 past receipts forms a bowl, lowest at w = 4
- **Exact step** — the true slope of the bowl needs a pass over all 50,000 receipts per step
- **Cheap step** — SGD grabs ONE random receipt, gets a rough slope, and takes a small step
- **Zig-zag** — each step is slightly wrong but mostly downhill, so the walk drifts to the bottom

*Example (italic):* Starting at w = 7, ten one-receipt steps land near w = 4; the one exact step read 50,000 rows just to reach w = 5.2.

**Key point:** Stochastic gradient descent swaps the exact gradient for a cheap noisy estimate from a random example and takes many small steps — the noise averages out, the savings do not.

### Visualization (canvas `c1`, 720×300)

Single-panel loss bowl with the SGD zig-zag path descending it, plus the one full-batch step for contrast.

- **Title (bold 15px, `#1a5276`, top center):** "One Exact Step vs Ten Cheap Noisy Steps on the Loss Bowl".
- **Axes:** origin x=60, plot width 600, baseline y=250, chart height 195; x axis is w from 0 to 8 (ticks at 0,2,4,6,8, 12px `#444`, axis label "w (min per km)"); y axis is loss from 0 to 12 (ticks 0,4,8,12).
- **Loss curve:** L(w) = (w−4)² + 2 sampled at w = 0 to 8 step 0.1; blue `#2a78d6` 2px line.
- **Minimum marker:** green `#008300` 6px dot at (4, 2) with bold 12px green label "best w = 4" below.
- **SGD path:** w values `[7.0, 6.1, 6.5, 5.4, 5.8, 4.9, 4.4, 4.6, 4.1, 3.9, 4.05]` plotted on the curve (loss = (w−4)²+2), joined by magenta `#d55181` 2px segments with 5px dots; bold 12px magenta annotation near the path top: "10 steps, 1 receipt each".
- **Full-batch step:** orange `#d95926` 3px arrow from (7.0, 11.0) to (5.2, 3.44) with arrowhead; bold 12px orange annotation, two lines: "1 exact step" / "= 50,000 receipts".
- **Caption (12px `#444`, bottom right):** "loss = (w−4)² + 2, illustrative".

## Ten Noisy Steps by Hand

**Tags:** `worked example` (blue), `rule of thumb` (green)

- **Update rule** — new w = w − 0.3 × (slope reported by one random receipt); 0.3 is the step size
- **Step 1** — a receipt reports slope +3.0, so w = 7.0 − 0.3 × 3.0 = 6.1
- **Step 2** — an odd rush-hour receipt reports slope −1.3, so w bounces UP: 6.1 + 0.39 ≈ 6.5
- **Truth check** — the true slope at w = 7 is 2 × (7 − 4) = 6; single receipts scatter around it
- **Ten steps in** — w = 4.05, within 0.05 of the best value 4, after reading only 10 receipts

*Example (italic):* Step 2 goes the wrong way (6.1 → 6.5), yet by step 10 the walk sits at 4.05 — no step ever gets undone.

**Key point:** Individual steps can point uphill; SGD works because the steps are right on average, so wrong moves wash out instead of needing correction.

### Visualization (canvas `c2`, 720×300)

Line chart of the knob w over ten SGD steps, converging onto the dashed target line at w = 4.

- **Title (bold 15px, `#1a5276`, top center):** "The Knob w Over Ten One-Receipt Steps (start 7.0, step size 0.3)".
- **Axes:** origin x=60, plot width 600, baseline y=250, chart height 195; x axis is step 0–10 (integer ticks, 12px `#444`, label "step"); y axis is w from 3 to 7.5 (ticks 3,4,5,6,7).
- **Data:** w by step `[7.0, 6.1, 6.5, 5.4, 5.8, 4.9, 4.4, 4.6, 4.1, 3.9, 4.05]` (steps 0..10).
- **Path:** blue `#2a78d6` 3px line with 5px dots; 11px `#444` value labels above the dots at steps 0, 2, and 10 ("7.0", "6.5", "4.05").
- **Target:** dashed green `#008300` (dash 5/4) horizontal line at w = 4, bold 12px green label "best w = 4" at its right end.
- **Wrong-way callout:** magenta `#d55181` bold 12px annotation with a short arrow at step 2: "wrong-way step".
- **Finish callout:** green bold 13px annotation near step 10: "w = 4.05 after 10 receipts".

## Minibatches: 32 Receipts per Step

**Tags:** `core idea` (blue), `minibatch` (orange), `where it's used` (green)

- **Minibatch** — average the slopes from 32 random receipts instead of 1 before stepping
- **Noise math** — averaging 32 estimates shrinks the wobble by √32 ≈ 5.7×, so steps aim truer
- **Fair race** — compare methods by receipts READ, not by steps taken; reading is the real cost
- **Scoreboard** — after 1,000 receipts: batch-32 at loss 2.15, batch-1 near 2.4, full batch still 11.0
- **Hardware bonus** — vectorized math processes 32 receipts in nearly the time of 1, so 32 is almost free

*Example (italic):* The full-batch method has not moved after 1,000 receipts — its very first step needs all 50,000.

**Key point:** Minibatches (commonly 32–256) buy a √n cut in gradient noise at almost no wall-clock cost — the standard middle ground between one-receipt chaos and full-batch paralysis.

### Visualization (canvas `c3`, 720×300)

Loss versus receipts read for batch-1 and batch-32, with the untouched full-batch level and the best-possible floor.

- **Title (bold 15px, `#1a5276`, top center):** "Loss vs Receipts Read: Batch 1 vs Batch 32 vs Full Batch (illustrative)".
- **Axes:** origin x=60, plot width 600, baseline y=245, chart height 185; x axis is receipts read 0–1,000 (ticks 0, 250, 500, 750, 1000, 12px `#444`, label "receipts read"); y axis is loss 0–12 (ticks 0,4,8,12).
- **Checkpoints (x for both curves):** `[0, 100, 200, 300, 400, 500, 600, 700, 800, 900, 1000]`.
- **Batch-1 curve:** loss `[11.0, 5.2, 3.8, 4.6, 2.9, 3.4, 2.5, 2.9, 2.3, 2.6, 2.4]`; blue `#2a78d6` 2px line with 4px dots; bold 12px blue label "batch 1" near its right end.
- **Batch-32 curve:** loss `[11.0, 6.5, 4.4, 3.4, 2.9, 2.6, 2.4, 2.3, 2.25, 2.2, 2.15]`; green `#008300` 3px line with 4px dots; bold 12px green label "batch 32" near its right end.
- **Full batch:** dashed orange `#d95926` (dash 5/4) horizontal line at loss 11.0 across the panel; bold 12px orange annotation, two lines: "full batch: hasn't stepped yet" / "first step needs 50,000 receipts".
- **Floor:** dashed `#6b7280` (dash 3/3) horizontal line at loss 2.0, 11px `#6b7280` label "best possible loss = 2.0".

## Reading a Noisy Loss Curve

**Tags:** `common mistake` (red), `rule of thumb` (green)

- **Jumpy by design** — one-receipt loss readings bounce around; a single spike is not a failure
- **Watch the average** — judge the 10-step moving average, never the latest raw point
- **The buzz** — with a fixed step size the walk never fully settles; it hums around the best w
- **Classic fix** — shrink the step size over time so the hum fades and the walk parks at the bottom
- **Run compares** — comparing two runs by their latest raw loss picks the luckier run, not the better one

*Example (italic):* At step 8 the raw loss jumps while the moving average keeps falling — a manager who restarts here throws away a healthy run.

**Common mistake:** Reacting to individual noisy loss values — restarting a converging run after one spike, or picking the model whose last raw reading happened to be lucky.

### Visualization (canvas `c4`, 720×300)

Raw per-step loss (thin, jittery) overlaid with its 10-step moving average (bold) showing the true downward trend.

- **Title (bold 15px, `#1a5276`, top center):** "Raw Per-Step Loss vs 10-Step Moving Average (illustrative)".
- **Axes:** origin x=60, plot width 600, baseline y=245, chart height 185; x axis is step 0–60 (ticks 0, 15, 30, 45, 60, 12px `#444`, label "step"); y axis is loss 0–12 (ticks 0,4,8,12).
- **Raw data (deterministic, no randomness):** for i = 0..60, raw[i] = 2 + 9·exp(−i/12) + 1.1·sin(i·2.7)·exp(−i/30), plus a one-off spike raw[8] += 1.6; starts at 11.0, ends near 2.1.
- **Smoothed data:** smooth[i] = mean of raw[max(0, i−9) .. i] (trailing 10-step window).
- **Raw line:** `rgba(42,120,214,0.45)` 1.5px, no dots; 11px `#6b7280` label "raw (1 receipt per step)" near its early section.
- **Smoothed line:** green `#008300` 3px; bold 12px green label "10-step average" alongside it.
- **Spike callout:** magenta `#d55181` bold 12px annotation with a short arrow at the local raw bump near step 8: "a spike, not a failure".
- **Trend callout:** green bold 13px annotation at the right: "trend: down and settling near 2".
- **Caption (12px `#444`, bottom right):** "jitter is a fixed sine term plus one scripted spike — illustrative, not random".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
