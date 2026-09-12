# The Learning Rate

**Page type:** detail page (tutorial card-sections: one h2 + two-column table per section, text left 50%, canvas right 50%)
**HTML title tag:** The Learning Rate

**Subtitle:** The step size of each downhill move — too big and you overshoot, too small and you never arrive

## Same Valley, Three Hikers

**Tags:** `core idea` (blue), `step size` (green)

- **The setup** — a pizza shop tunes base time b in "b + 4×km"; the loss valley bottoms at b = 14
- **The rule** — each move is: new b = b − learning rate × slope; the rate scales every step
- **Timid hiker (0.05)** — tiny shuffles: b creeps 2 → 3.2 → 4.3 → 5.2, still far after 5 steps
- **Steady hiker (0.25)** — clean hops: 2 → 8 → 11 → 12.5 → 13.25, halving the gap each step
- **Reckless hiker (0.9)** — leaps clear over the bottom: 2 → 23.6 → 6.3 → 20.1, wall to wall
- **Same slope, same start** — only the step-size dial differs between the three walks

*Example (italic):* All three hikers feel the identical slope at b = 2; they differ only in how far they trust one reading.

**Key point:** **The learning rate** is the multiplier on every downhill step. It does not change the direction gradient descent picks — only how far it commits before looking again.

### Visualization (canvas `c1`, 720×300)

Loss valley (parabola) with three hikers' hop paths at learning rates 0.05, 0.25 and 0.9.

- **Title (bold 15px, `#1a5276`, top center):** "Three Step Sizes on the Same Loss Valley"
- **Curve:** loss(b) = (b − 14)² in ink `#1a5276`, width 3, over b in [0, 28], clipped at loss 200, 100 segments.
- **Axes:** x from 0 to 28 (ticks every 7), label "base time b"; no y ticks. Axis lines `#999`, tick labels muted `#6b7280` 12px. Padding: top 48, bottom 46, left 60, right 165.
- **Hiker paths:** each is a sequence of points at (b, loss(b)) drawn as 4.5px dots connected by quadratic-curve hop arcs (width 2) in the path's color:
  - rate 0.05, orange `#d95926`: b = `[2, 3.2, 4.28, 5.25, 6.13, 6.91]`
  - rate 0.25, green `#008300`: b = `[2, 8, 11, 12.5, 13.25]`
  - rate 0.9, magenta `#d55181`: b = `[2, 23.6, 6.32, 20.14, 9.09]`
- **Annotations (bold 12px, centered):** magenta near (14, 150): "0.9: leaps across the valley every step"; orange near the end of the timid path: "0.05: still up here after 5 steps"; green near (12.5, 0): "0.25: settles by step 4".
- **Legend (right margin, 12px swatches):** orange "rate 0.05: timid"; green "rate 0.25: steady"; magenta "rate 0.9: reckless".

## One Multiplication Predicts the Whole Walk

**Tags:** `worked example` (green), `convergence` (blue)

- **The shortcut** — for loss (b − 14)², each step multiplies the gap to 14 by (1 − 2×rate)
- **Rate 0.05** — gap ×0.9 per step: 12 → 10.8 → 9.7 → 8.7, shrinking but painfully slowly
- **Rate 0.25** — gap ×0.5: 12 → 6 → 3 → 1.5, the comfortable halving from before
- **Rate 0.9** — gap ×(−0.8): 12 → −9.6 → 7.7, overshooting the bottom every single step
- **Rate 1.1** — gap ×(−1.2): 12 → −14.4 → 17.3, each step lands FARTHER away — divergence
- **Loss follows** — 144 per-step becomes ×0.81, ×0.25, ×0.64, or ×1.44 respectively

*Example (italic):* Check rate 1.1 by hand: at b = 2 the slope is −24, so the step is +26.4, landing at b = 28.4 — a worse spot.

**Key point:** **The cliff is real:** past a critical rate (here 1.0) every step amplifies the error and the loss explodes to infinity — not "learns badly", literally blows up.

### Visualization (canvas `c2`, 720×300)

Multi-line chart of loss vs step number for four learning rates, with the divergent curve clamped at the top and marked with an arrow.

- **Title (bold 15px, `#1a5276`, top center):** "Loss per Step at Four Learning Rates (start: loss 144)"
- **Data:** each curve starts at loss 144 and multiplies per step (7 steps, 0–6): rate 0.05 → ×0.81 (orange `#d95926`); rate 0.25 → ×0.25 (green `#008300`); rate 0.9 → ×0.64 (aqua `#199e70`); rate 1.1 → ×1.44 (red `#e74c3c`, stops once it exceeds the axis max).
- **Axes:** x steps 0–6, label "step number"; y from 0 to 250 (ticks every 50, values clamped at 250). Axis lines `#999`, tick labels muted `#6b7280` 12px. Padding: top 50, bottom 50, left 65, right 165.
- **Series:** width-3 lines in each rate's color.
- **Divergence marker:** upward-pointing red `#e74c3c` filled triangle at about x=2.2 where the 1.44 curve leaves the top; bold 13px red label to its right: "rate 1.1: loss ×1.44 every step — explodes".
- **Annotation (bold 13px green, near x=1.6, y≈20):** "rate 0.25: ×0.25 per step".
- **Legend (right margin, 12px swatches):** orange "rate 0.05"; green "rate 0.25"; aqua "rate 0.9"; red "rate 1.1".

## Reading the Training Curve Like a Doctor

**Tags:** `tuning` (blue), `diagnosis` (orange)

- **First knob to check** — when training misbehaves, the learning rate is suspect number one
- **Barely falling** — a slow, steady glide that never gets low means the rate is too small
- **Smooth steep drop** — falls fast, then settles gently: the rate is in the healthy zone
- **Jagged sawtooth** — loss bounces up and down around a level: slightly too big, halve it
- **Explodes or NaN** — loss shoots upward or turns into NaN: way too big, cut it 10x
- **Try powers of 10** — practitioners scan 0.0001, 0.001, 0.01, 0.1 and keep the steepest smooth fall

*Example (italic):* A model "refusing to learn" is often nothing mysterious — the rate was 100x too small for the problem.

**Key point:** **Why it matters:** the learning rate is widely considered the single most impactful setting in training — the same model and data succeed or fail on this one dial.

### Visualization (canvas `c3`, 720×300)

Four mini diagnostic panels (2×2 grid) showing the canonical loss-curve shapes, each with a title and a fix note.

- **Title (bold 15px, `#1a5276`, top center):** "Four Curve Shapes, Four Diagnoses (illustrative)"
- **Panels:** each 310×108 with a light grid-color border `#e5e9ef`, a bold 13px colored title top-left, a muted 12px fix note top-right, and a width-2.5 line of 10 points (y values on a 0–100 scale, higher = higher loss):
  - Top-left (x=30, y=44), orange `#d95926`: "too small: barely falls" / "fix: raise 10x", ys `[90, 87, 84, 81, 79, 77, 75, 73, 71, 70]`
  - Top-right (x=380, y=44), green `#008300`: "healthy: steep then settles" / "keep it", ys `[90, 45, 24, 14, 9, 7, 6, 5.5, 5.2, 5]`
  - Bottom-left (x=30, y=172), yellow `#c98500`: "slightly big: sawtooth" / "fix: halve it", ys `[90, 40, 62, 30, 48, 24, 38, 20, 32, 18]`
  - Bottom-right (x=380, y=172), red `#e74c3c`: "too big: explodes / NaN" / "fix: cut 10x", ys `[90, 60, 75, 95, 55, 80, 100, 100, 100, 100]`
- **Extra marker:** bold 13px red "NaN" inside the explode panel, bottom-right area.
- **Footer (bold 12px ink `#1a5276`, bottom center):** "x: training steps, y: loss — the shape alone names the problem"

## The Confusion: Smaller Is Not Safer — Shrink It Over Time Instead

**Tags:** `common mistake` (red), `tuning` (green)

- **The instinct** — "overshooting is scary, so pick a tiny rate to be safe" wastes the whole budget
- **Tiny-rate trap** — the loss still falls, so it LOOKS fine, but converges long after you stop
- **False convergence** — a flat curve can mean "done" or "moving too slowly to tell" — ambiguous
- **The fix: decay** — start bold at 0.8 to cross the valley fast, then halve the rate as you settle
- **Decayed walk** — b: 2 → 21.2 → 9.7 → 13.1 → 13.8, parked near 14 by step 4
- **Fixed 0.8 walk** — overshoots every step: 2 → 21.2 → 9.7 → 16.6 → 12.4 → 14.9, settles slowly

*Example (italic):* Drive to a parking spot: fast on the highway, slow in the lot — nobody uses one speed for both.

**Key point:** **The confusion:** people treat the rate as one number to get right forever. Modern training schedules it — large early steps for speed, small late steps for precision.

### Visualization (canvas `c4`, 720×300)

Two-line chart of parameter b vs step number: fixed rate 0.8 (overshooting, settling slowly) vs a decaying schedule (settling fast), with a dashed target line at b = 14.

- **Title (bold 15px, `#1a5276`, top center):** "Fixed Rate 0.8 vs a Decaying Schedule (target b = 14)"
- **Data (10 steps, 0–9):**
  - fixed 0.8, magenta `#d55181`: `[2, 21.2, 9.68, 16.59, 12.44, 14.93, 13.44, 14.33, 13.80, 14.12]`
  - decayed, green `#008300`: `[2, 21.2, 9.68, 13.14, 13.83, 13.90, 13.94, 13.95, 13.96, 13.97]`
- **Axes:** x steps 0–9, label "step number"; y from 0 to 24 (ticks every 6). Axis lines `#999`, tick labels muted `#6b7280` 12px. Padding: top 50, bottom 50, left 60, right 175.
- **Target line:** dashed ink `#1a5276` (dash 6/4, width 1.5) horizontal line at b=14, bold 12px ink label "b = 14" at its left.
- **Series:** width-2.5 lines with 4px dots at every point in each series' color.
- **Annotations (bold 13px, centered near x=5.5):** green at y≈12.2: "decay: parked at 14 by step 4"; magenta at y≈19.5: "fixed 0.8: overshoots every step, settles slowly".
- **Legend (right margin, 12px swatches):** magenta "fixed rate 0.8"; green "0.8, halved as it settles"; below, bold 12px orange `#d95926` note: "start bold, finish careful".

## Regeneration instructions

- **Layout:** tutorial detail page. h1, `.subtitle` paragraph, then four `.card-section` blocks. Each section: `<h2>` with 2px `#2980b9` bottom border, then `table.layout` (full width, border-collapse) with one `<tr>`: left `<td class="text-col">` (50%) holding `.tags` pill row, `<ul>` bullets (each starting with `<b>` term in `#1a5276`), italic `.example` paragraph, and `.key-point` callout; right `<td class="viz-col">` (50%) holding one canvas.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6. h1 2rem `#1a5276` with 2px `#2980b9` bottom border; h2 1.3rem `#1a5276`; subtitle `#666` 0.95rem. Canvas `width:100%`, 1px `#e0e0e0` border, 4px radius. `.key-point`: background `#f8f9fa`, left border 3px solid `#e74c3c`, padding 8px 12px, 0.9rem. `.example`: italic `#555` 0.9rem. Bullets 0.92rem.
- **Tag pills:** `.tag` inline-block 0.72rem bold, padding 2px 10px, radius 10px. blue: bg rgba(26,82,118,0.12) / `#1a5276`; green: bg rgba(39,174,96,0.15) / `#27ae60`; red: bg rgba(231,76,60,0.12) / `#e74c3c`; orange: bg rgba(230,126,34,0.15) / `#e67e22`.
- **Chart palette (JS object P):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Canvas:** intrinsic 720×300 attributes; a shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Shared helper `loss(b) = (b − 14)²`. All data hardcoded and deterministic (no Math.random). Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- No nav bar, no back/home links. In regenerated HTML, any card links use `.html` extensions.
