# Backpropagation

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column `table.layout` — text left 50%, canvas right 50%)
**HTML title tag:** Backpropagation

**Subtitle:** When the network misses, blame flows backwards through the wires — the weights that contributed most to the miss get nudged most

## The Net Said 0.8, the Truth Was 1.0 — Who's Responsible?

**Tags:** `core idea` (blue), `running example` (green)

- **The setup** — the tiny customer net (2 inputs → 2 hidden → 1 output) predicted 0.8
- **The truth** — the customer did come back: the right answer was 1.0, a miss of 0.2
- **The question** — dozens of weights each did a small job; which ones caused the miss?
- **Blame flows backwards** — start at the error and walk the wires right to left
- **A chain of responsibility** — each wire passes back its share, in proportion to its role

*Example:* Like tracing a wrong invoice back through every desk that touched it, splitting blame at each step.

**Key point:** Backpropagation is bookkeeping for blame — one backwards sweep prices every weight's share of one miss.

### Visualization (canvas `c1`, 720×300)

Network diagram with blame flowing backwards over a faint forward pass.

- **Title (bold 15px, `#1a5276`, top center):** "Forward Gave 0.8 — Now Blame Walks the Wires Backwards".
- **Nodes (circles, 2.5px stroke, bold centered labels):** input x1 at (100,100), radius 19, stroke `#2a78d6`, white fill, label "1.0"; input x2 at (100,210), radius 19, stroke `#199e70`, white fill, label "2.0"; hidden h1 at (320,100), radius 23, stroke `#1a5276`, fill `rgba(26,82,118,0.08)`, two-line label "h1" / "1.0"; hidden h2 at (320,210), same style, label "h2" / "0.6"; output at (540,155), radius 23, stroke `#008300`, fill `rgba(0,131,0,0.10)`, label "out" / "0.8".
- **Forward wires:** six gray (`#6b7280`) solid arrows at 30% alpha connecting x1/x2 → h1/h2 → out (arrowheads 9px).
- **Error box:** rectangle at (610,110) 100×92, fill `rgba(231,76,60,0.08)`, stroke `#e74c3c` 2px; red bold centered text inside: "predicted 0.8", "truth 1.0", and larger "miss 0.2".
- **Backward blame arrows:** dashed orange (`#d95926`, dash 6/4) arrows running right-to-left: error box → out (width 2.5), out → h1 and out → h2 (width 2.5), h1 → x1 and h2 → x2 (width 2).
- **Blame labels (bold 12px orange, centered at x=432):** "blame to h1: 0.2 × 0.5 = 0.1" (y=92) and "blame to h2: 0.2 × 0.5 = 0.1" (y=232).
- **Legend (bottom, y=282):** gray 12px "grey wires: the forward pass" (left at x=70); bold orange "orange dashes: blame flowing back, shrinking at each split" (at x=300).

## Splitting the 0.2 Miss, Wire by Wire

**Tags:** `worked example` (green), `core idea` (blue)

- **Blame rule** — blame on a wire = error arriving at its end × signal the wire carried
- **Output wire A** — carried h1 = 1.0, so blame = 0.2 × 1.0 = 0.20
- **Output wire B** — carried h2 = 0.6, so blame = 0.2 × 0.6 = 0.12: less signal, less blame
- **Passing it back** — h1's share of the error = 0.2 × its weight 0.5 = 0.1; same for h2
- **Nudge = 0.1 × blame** — wire A moves +0.020 (0.5 → 0.52), wire B +0.012 (0.5 → 0.512)

*Example:* Deeper in: the wire from x2 into h1 carried 2.0, so its blame is 0.1 × 2.0 = 0.2 → nudge +0.02.

**Key point:** Prediction too low and the wire carried positive signal? Its weight goes up — by more when it carried more.

### Visualization (canvas `c2`, 720×300)

Horizontal bar chart: nudge size per wire, six rows.

- **Title (bold 15px `#1a5276`, top center):** "One Miss of 0.2, Priced Into Six Weight Nudges".
- **Rows (label / nudge / color / note):**
  - "out ← h1  (carried 1.0)" / 0.020 / magenta `#d55181` / "0.5 → 0.52"
  - "out ← h2  (carried 0.6)" / 0.012 / magenta `#d55181` / "0.5 → 0.512"
  - "h1 ← x2  (carried 2.0)" / 0.020 / blue `#2a78d6` / "0.3 → 0.32"
  - "h1 ← x1  (carried 1.0)" / 0.010 / blue `#2a78d6` / "0.5 → 0.51"
  - "h2 ← x2  (carried 2.0)" / 0.020 / aqua `#199e70` / "0.6 → 0.62"
  - "h2 ← x1  (carried 1.0)" / 0.010 / aqua `#199e70` / "−0.4 → −0.39"
- **Scale:** bar width proportional to nudge / 0.024 max; padding top 56, bottom 70, left 200, right 90; bars 20px tall at 0.7 alpha; vertical gray axis line at left edge of bars.
- **Labels:** row label right-aligned in gray `#444` 12px left of axis; bold colored value label right of each bar: "+0.020   0.5 → 0.52" etc. (nudge to 3 decimals plus note).
- **X-axis caption (centered, `#444` 12px):** "nudge size (step 0.1 × blame)".
- **Takeaway (bold 13px orange `#d95926`, bottom center):** "wires that carried a 2.0 get double the nudge of wires that carried a 1.0".

## Why This Loop Is All of Training

**Tags:** `where it's used` (blue), `best practice` (green)

- **One nudge helps** — rerun the forward pass with nudged weights: 0.8 becomes 0.88
- **Training = repeat** — predict, measure the miss, blame backwards, nudge; millions of times
- **Every big model** — image, speech, language models are all trained by this same loop
- **Step size matters** — the 0.1 multiplier is the learning rate; too big overshoots the truth
- **Sleeping neurons opt out** — a ReLU that output 0 passes zero blame to its weights

*Example:* A loss curve drifting down during training is this loop working — each point is one blame-and-nudge round.

**Key point:** "Training a network" is nothing more exotic than this miss-blame-nudge cycle, repeated until misses get small.

### Visualization (canvas `c3`, 720×300)

Line chart: prediction climbing toward the truth over training steps.

- **Title (bold 15px `#1a5276`, top center):** "Miss, Blame, Nudge, Repeat: the Prediction Climbs".
- **Data:** predictions by step 0–5: `[0.80, 0.88, 0.93, 0.96, 0.98, 0.99]`; y range 0.75–1.05; padding top 56, bottom 62, left 62, right 40; points centered per slot; gray `#999` L-shaped axes.
- **Truth line:** horizontal dashed green (`#008300`, dash 6/4, width 2) at y=1.0, labeled bold green "truth = 1.0" above it at the left.
- **Series:** blue `#2a78d6` line, width 3; dots at each point — orange `#d95926` radius 7 for the first two steps, blue radius 5 for the rest; bold value labels ("0.80"…"0.99") below each dot; gray "step 0"…"step 5" x labels.
- **Annotations:** bold orange 13px "one nudge: 0.80 → 0.88 (computed above)" near the first point; italic gray (`#6b7280`) 12px right-aligned "later steps illustrative" near the last point.
- **Y-axis label (rotated, `#444` 12px):** "prediction for this customer".
- **Takeaway (bold 13px blue `#2a78d6`, bottom center):** "each step is a full round: forward pass, miss, blame backwards, nudge".

## The Confusion: Big Weight Is Not Big Blame

**Tags:** `common mistake` (red), `rule of thumb` (blue)

- **Not a reverse replay** — data doesn't run backwards; only blame numbers travel that way
- **Inputs never change** — backprop adjusts weights, not the customer's data
- **Blame follows signal** — both output wires weigh 0.5, yet their nudges differ: +0.020 vs +0.012
- **Because contribution differs** — one carried 1.0, the other 0.6; the busier wire owns more of the miss
- **Blame is per-example** — the next customer produces a different miss and a different split

*Example:* A huge weight on a wire that carried 0 gets zero nudge — it did nothing this time, so it owes nothing.

**Key point:** Blame is earned by what a wire actually carried this example — not by how big its weight happens to be.

### Visualization (canvas `c4`, 720×300)

Three-bar column chart: same weight, different carried signal, different nudge.

- **Title (bold 15px `#1a5276`, top center):** "Two Output Wires, Both Weighing 0.5 — Unequal Blame".
- **Bars:** labels "wire from h1" / "wire from h2" / "imaginary wire"; carried "carried 1.0" / "carried 0.6" / "carried 0.0"; nudges 0.020 / 0.012 / 0.0; colors magenta `#d55181` / violet `#4a3aa7` / gray `#6b7280`; scale max 0.024; bar width 130px at 0.72 alpha; centers at 18%/50%/82% of chart width; padding top 60, bottom 84, left 62, right 30; gray L-shaped axes.
- **Value labels (bold 15px, bar color, above bar):** "+0.020", "+0.012", "zero nudge".
- **Below-bar captions (centered):** bold `#222` "wire from h1, weight 0.5" etc.; gray carried line; gray "blame 0.2 × 1.0" / "× 0.6" / "× 0.0".
- **Y-axis label (rotated, `#444` 12px):** "nudge this example".
- **Annotation (bold 13px orange `#d95926`, centered above bars):** "identical weights, different traffic — a wire that carried nothing owes nothing".

## Regeneration instructions

- **Layout:** tutorial detail page. `<h1>` (no index number), `.subtitle` paragraph, then four `.card-section` blocks; each has an `<h2>` (1.3rem `#1a5276`, 2px solid `#2980b9` bottom border) and a `table.layout` (full width, border-collapse) with one `<tr>`: left `td.text-col` (50%) and right `td.viz-col` (50%), both 12px padding, top-aligned.
- **Left column structure:** `.tags` row of colored pill spans (0.72rem, 600 weight, 2px 10px padding, 10px radius — blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`); then a `<ul>` (0.92rem) of bullets each opening with `<b>` term in `#1a5276`; one italic `.example` paragraph (`#555`, 0.9rem); one `.key-point` callout (background `#f8f9fa`, left border 3px solid `#e74c3c`, 8px 12px padding, 0.9rem) starting with `<strong>Key point:</strong>`.
- **Page style:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` bottom border; subtitle `#666` 0.95rem. No nav bar, no back/home links.
- **Canvas:** each canvas declared `width="720" height="300"`, CSS `width:100%`, 1px `#e0e0e0` border, 4px radius; a shared `setup(id)` helper scales by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates). Shared `arrow()` (arrowhead 9px, optional dash) and `node()` (circle with centered label) helpers. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** JS object `P = { blue:#2a78d6, green:#008300, magenta:#d55181, yellow:#c98500, aqua:#199e70, orange:#d95926, violet:#4a3aa7, ink:#1a5276, text:#2c3e50, mute:#6b7280, grid:#e5e9ef }`. Doc palette anchors: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`. All chart data is hardcoded literal arrays (no `Math.random()`).
- In regenerated HTML, any card links use `.html` extensions (this page has no outgoing links).
