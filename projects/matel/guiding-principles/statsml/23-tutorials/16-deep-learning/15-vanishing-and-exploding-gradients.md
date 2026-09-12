# Vanishing & Exploding Gradients

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Vanishing & Exploding Gradients

**Subtitle:** Backprop multiplies one factor per layer — twenty factors of 0.5 leave a millionth of the learning signal, twenty factors of 1.5 blow it up 3,325× — and dodging this arithmetic shaped every modern architecture

## A Message Through Twenty Relays

**Tags:** `core idea` (blue), `multiplication chain` (orange), `backprop` (green)

- **The relay** — a coach's correction reaches the newest player only after 20 assistants repeat it
- **Quiet chain** — each assistant repeats at half volume: after 20 relays only 0.00000095 survives
- **Loud chain** — each assistant repeats 1.5× louder: after 20 relays the message is 3,325× too loud
- **The gradient** — backprop is exactly this relay: the error signal multiplies one factor per layer
- **The names** — factors below 1 make gradients vanish; factors above 1 make them explode

*Example (italic):* The same correction, whispered at ×0.5 per relay, is one-millionth as loud by relay 20 — the player at the end of the chain hears nothing.

**Key point:** A deep network passes its learning signal through a chain of multiplications. Unless each factor sits near 1, the signal shrinks or grows exponentially with depth.

### Visualization (canvas `c1`, 720×300)

Log-axis line chart: signal strength after k relays for three per-relay factors, plotted over k = 0..20 on a log10 y-axis so each exponential becomes a straight line.

- **Title (bold 15px, `#1a5276`, top center):** "Signal Strength After k Relays: ×0.5 vs ×1.0 vs ×1.5 per Relay".
- **Data:** hardcoded factors `[0.5, 1.0, 1.5]`; y values are exact powers `Math.pow(f, k)` for k = 0..20 (no other data source). Key values: 0.5^10 = 0.00098, 0.5^20 = 0.00000095, 1.5^10 = 57.7, 1.5^20 = 3325.
- **Axes:** origin x=60, plot width 590, baseline y=245, plot height 190; x = relay count 0..20 evenly spaced, ticks labeled 0, 5, 10, 15, 20 (12px `#444`); y maps log10(value) over range −7 to +4; horizontal gridlines `#e5e9ef` at each even decade with 12px `#444` labels "10⁴", "10²", "1", "10⁻²", "10⁻⁴", "10⁻⁶".
- **Lines:** ×1.5 orange `#d95926` 3px rising straight line; ×1.0 green `#008300` 3px flat line at 1; ×0.5 blue `#2a78d6` 3px falling straight line; 4px dots at k = 0, 5, 10, 15, 20 on each line.
- **Annotations:** orange bold 13px near top right "×1.5 per relay → 3,325× at relay 20 (explodes)"; blue bold 13px near bottom right "×0.5 per relay → about one-millionth (vanishes)"; green bold 12px above the flat line "×1.0 keeps the signal alive".
- **Caption (12px `#444`, bottom center):** "log y-axis: a straight line means a constant per-relay factor".

## Multiplying It Out by Hand

**Tags:** `worked example` (blue), `chain rule` (green)

- **Chain rule** — the gradient k layers back is the product of k per-layer factors, nothing more
- **Shrink by hand** — 0.5 multiplied 10 times: 1 → 0.5 → 0.25 → 0.125 → ... → 0.00098
- **Grow by hand** — 1.5 multiplied 10 times: 1 → 1.5 → 2.25 → 3.375 → ... → 57.7
- **Update size** — weight updates scale with the gradient; a 0.00098× signal barely moves a weight
- **Depth = multiplier count** — 10 layers means 10 multiplications; 50 layers means 50

*Example (italic):* You can redo the whole chart on paper: keep multiplying 1 by 0.5 ten times and you land on 0.0009765625.

**Key point:** The effect is exponential in depth, not linear — going from 10 layers to 20 doesn't double the shrinkage, it squares it (0.00098 becomes 0.00000095).

### Visualization (canvas `c2`, 720×300)

Dual-panel bar chart on LINEAR axes so the pathology is visible as bars physically vanishing (left) or dwarfing everything (right); dashed divider at x=360.

- **Title (bold 15px, `#1a5276`, top center):** "Gradient Magnitude k Layers Back: ×0.5 Chain vs ×1.5 Chain (linear axes)".
- **Left panel (×0.5 chain):** bars for k = 0..10, values exactly `[1, 0.5, 0.25, 0.125, 0.0625, 0.03125, 0.015625, 0.0078125, 0.00390625, 0.001953125, 0.0009765625]`; axis origin x=55, width 280, baseline y=240, chart height 175, y scale 0–1.05; bars fill `rgba(42,120,214,0.45)`, 1px `#2a78d6` stroke; k labels 0..10 at 11px `#444` below bars; bold 12px value label "1" above the first bar and "0.00098" above bar k=10; magenta `#d55181` bold 12px annotation, two lines: "bars k=7..10 under one pixel —" / "no learning signal left"; caption 12px `#444` "each bar = previous × 0.5".
- **Right panel (×1.5 chain):** bars for k = 0..10, values exactly `[1, 1.5, 2.25, 3.375, 5.0625, 7.59375, 11.390625, 17.0859375, 25.62890625, 38.443359375, 57.6650390625]`; axis origin x=400, width 280, same baseline/height, y scale 0–60; bars fill `rgba(217,89,38,0.5)`, 1px `#d95926` stroke; bold 12px value label "57.7" above bar k=10; orange bold 12px annotation "the k=0 bar is invisible next to 57.7"; caption "each bar = previous × 1.5".
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=360 from y=38 to h−12.

## The Fixes That Built Modern Deep Learning

**Tags:** `where it's used` (blue), `architecture` (orange), `rule of thumb` (green)

- **Sigmoid's ceiling** — slope ≤ 0.25, so 8 layers with weights near 1 shrink by ≤ 0.25^8 = 0.0000153
- **ReLU** — slope is exactly 1 on active inputs, so the activation part of the product stays 1
- **Skip connections** — ResNets add an identity detour, guaranteeing a factor-1 path always survives
- **Gradient clipping** — cap the magnitude at 5: the ×1.5 chain stops growing at layer 4 (stylized; 1.5⁴ = 5.06)
- **Same fight everywhere** — LSTM gates, BatchNorm, careful init all push per-layer factors toward 1

*Example (italic):* ReLU replaced sigmoid as the default largely because 1^k = 1 while 0.25^k dies — a one-line change fixing a multiplication chain.

**Key point:** ReLU, ResNets, LSTMs, BatchNorm, and clipping look unrelated but solve one problem: keep the per-layer factor near 1 so the depth-k product neither vanishes nor explodes.

### Visualization (canvas `c3`, 720×300)

Log-axis line chart comparing the gradient surviving k layers back under three regimes: worst-case sigmoid chain, ReLU active path, and a clipped exploding chain.

- **Title (bold 15px, `#1a5276`, top center):** "What Survives k Layers Back: Sigmoid Chain vs ReLU vs Clipped ×1.5".
- **Data:** k = 0..12; sigmoid worst case exact powers `Math.pow(0.25, k)` (key values 0.25^8 = 0.0000153, 0.25^12 = 0.00000006); ReLU active path constant 1 for all k; clipped chain `Math.min(Math.pow(1.5, k), 5)` — exact values `[1, 1.5, 2.25, 3.375, 5, 5, 5, 5, 5, 5, 5, 5, 5]` (capped from k=4 since 1.5^4 = 5.06 > 5).
- **Axes:** origin x=60, plot width 590, baseline y=245, plot height 190; x ticks at k = 0, 4, 8, 12 (12px `#444`); y maps log10(value) over range −8 to +1; gridlines `#e5e9ef` at decades 1, 10⁻², 10⁻⁴, 10⁻⁶, 10⁻⁸ with 12px `#444` labels.
- **Lines:** sigmoid chain magenta `#d55181` 3px plunging straight line, 4px dots at k = 0, 4, 8, 12; ReLU green `#008300` 3px flat line at 1; clipped chain orange `#d95926` 3px line rising then flat at 5, 4px dot at the k=4 kink.
- **Annotations:** magenta bold 12px, two lines: "8 sigmoid layers:" / "≤ 0.25⁸ = 0.0000153 survives"; green bold 13px above flat line "ReLU active path: activation factor stays 1"; orange bold 12px at the kink "clip at 5 (stylized): explosion stops at layer 4".
- **Caption (12px `#444`, bottom center):** "every fix is a way to hold the per-layer factor near 1".

## Silence or NaN: Spotting Which One You Have

**Tags:** `common mistake` (red), `debugging` (orange)

- **Stuck loss** — vanishing looks like a plateau: early layers get ~0 gradient and simply never move
- **NaN loss** — exploding overshoots harder each step until numbers overflow and training dies
- **Not the learning rate** — retuning the rate rescales one number; depth multiplies twenty of them
- **Check per layer** — plot gradient norm by layer; a healthy net is roughly flat across depth
- **Deeper ≠ better** — before skip connections, a 56-layer plain net trained worse than a 20-layer one

*Example (italic):* A team spent a week sweeping learning rates on a stalled 30-layer net; swapping in skip connections fixed it in one run.

**Common mistake:** Blaming the learning rate or the data when loss plateaus or turns NaN. The two symptoms look unrelated but are the same multiplication chain failing in opposite directions.

### Visualization (canvas `c4`, 720×300)

Three training-loss curves over 12 epochs on one linear-axis panel: healthy descent, vanishing plateau, and exploding spikes ending in NaN (marked with a red X).

- **Title (bold 15px, `#1a5276`, top center):** "Three Training Runs, Three Fates (illustrative)".
- **Data (hardcoded, illustrative):** epochs 1..12; healthy `[2.3, 1.6, 1.2, 0.92, 0.74, 0.60, 0.50, 0.43, 0.38, 0.34, 0.31, 0.29]`; vanishing `[2.3, 2.24, 2.21, 2.19, 2.18, 2.17, 2.17, 2.16, 2.16, 2.16, 2.16, 2.16]`; exploding `[2.3, 1.9, 2.6, 1.8, 3.4, 2.4, 5.1, 8.7]` (stops at epoch 8).
- **Axes:** origin x=60, plot width 590, baseline y=245, plot height 190; x = epochs 1..12 evenly spaced, ticks labeled 1, 4, 8, 12 (12px `#444`); y linear 0–9 with gridlines `#e5e9ef` at 0, 3, 6, 9 and 12px `#444` labels.
- **Lines:** healthy green `#008300` 3px with 4px dots; vanishing blue `#2a78d6` 3px with 4px dots; exploding orange `#d95926` 3px with 4px dots, drawn only through epoch 8.
- **NaN marker:** bold red `#e74c3c` 16px "✕" at epoch 9 above the exploding line's last point, with red bold 12px label "NaN — training dead".
- **Annotations:** blue bold 12px near the plateau "vanishing: stuck at 2.16 — early layers never learn"; green bold 12px near the descending curve "healthy: factors near 1".
- **Caption (12px `#444`, bottom center):** "loss values illustrative; the shapes are the diagnostic".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (here it hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data rule:** never `Math.random()` — all gradient sequences are exact powers of the hardcoded factors (0.5^k, 1.5^k, 0.25^k, and min(1.5^k, 5)); the c4 loss curves are hardcoded literal arrays labeled illustrative.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
