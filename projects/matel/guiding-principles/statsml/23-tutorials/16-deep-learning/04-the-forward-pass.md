# The Forward Pass

**Page type:** detail page (tutorial layout: h1 + subtitle, 4 `.card-section` blocks, each h2 + two-column `table.layout` with text left 50% / canvas right 50%)
**HTML title tag:** The Forward Pass

**Subtitle:** A prediction is just arithmetic flowing left to right — multiply, add, bend, repeat — until one number falls out the far end

## Two Inputs, Two Hidden Neurons, One Answer

**Tags:** `core idea` (blue), `running example` (green)

- **The question** — will this customer come back next week? The network answers with one score
- **Two inputs** — x1 = 1.0 (visits this week), x2 = 2.0 (minutes per visit, in tens)
- **Two hidden neurons** — each takes a weighted sum of both inputs, then a ReLU bend
- **One output neuron** — takes a weighted sum of the two hidden results: 0.8
- **Left to right only** — numbers flow forward through the wires; nothing flows back

*Example (italic):* Every wire in the picture does one multiplication; every circle adds up what arrives.

**Key point:** A forward pass is the network answering one question — feed numbers in on the left, read the answer on the right.

### Visualization (canvas `c1`, 720×300)

Full 2-2-1 network diagram with every weight on its wire and every value in its circle.

- **Title (bold 15px, ink `#1a5276`, top center):** "The Whole Network: Weights on Every Wire, Values in Every Circle"
- **Nodes:** input x1 (white circle r=22, blue `#2a78d6` stroke, "1.0" inside) at (110,105); input x2 (aqua `#199e70` stroke, "2.0") at (110,215); hidden h1 (r=26, ink stroke, fill `rgba(26,82,118,0.08)`, "h1" over "1.0") at (360,105); hidden h2 ("h2" over "0.6") at (360,215); output (r=26, green `#008300` stroke, fill `rgba(0,131,0,0.10)`, "out" over "0.8") at (610,160).
- **Input→hidden wires** (width-2 arrows) with bold 13px weight labels: x1→h1 blue "× 0.5"; x1→h2 blue "× −0.4"; x2→h1 aqua "× 0.3"; x2→h2 aqua "× 0.6".
- **Hidden→output wires** in violet `#4a3aa7`, both labeled "× 0.5".
- **Node captions (12px gray):** "visits" above x1; "minutes ÷ 10" below x2; "bias −0.1, then ReLU" above h1; "bias −0.2, then ReLU" below h2; bold 12px green '"will return"' / "score" near the output.
- **Takeaway (bold 13px orange `#d95926`, centered at (360,288)):** "numbers only ever move left → right"

## Every Multiplication, Written Out

**Tags:** `worked example` (green), `core idea` (blue)

- **Hidden 1** — 0.5×1.0 + 0.3×2.0 − 0.1 = 0.5 + 0.6 − 0.1 = 1.0; ReLU keeps it: h1 = 1.0
- **Hidden 2** — −0.4×1.0 + 0.6×2.0 − 0.2 = −0.4 + 1.2 − 0.2 = 0.6; ReLU keeps it: h2 = 0.6
- **Output** — 0.5×h1 + 0.5×h2 = 0.5×1.0 + 0.5×0.6 = 0.5 + 0.3 = 0.8
- **Count the work** — 6 multiplications, 2 biases, 2 bends, 1 final sum: that's the whole prediction
- **Check any step** — every number above is one line on a pocket calculator

*Example (italic):* The 0.8 splits cleanly: 0.5 came through hidden neuron 1, 0.3 through hidden neuron 2.

**Key point:** There is no step in a forward pass beyond multiply, add, and bend — a million-neuron model just does it a million times.

### Visualization (canvas `c2`, 720×300)

Three-column "ledger" of horizontal diverging bars: each product/bias term drawn as a bar from a per-column zero line, with column totals beneath.

- **Title (bold 15px, ink, top center):** "The Whole Prediction as a Ledger of Products"
- **Columns** (headers bold 14px in column color; zero line `#ccc` at column center −30; bar scale 68 px per 1.0, bar height 22, fill alpha 0.65, positive bars in the column color, negative bars in orange `#d95926`; row labels 12px gray right of/left of the zero line, signed bold 12px values at bar ends):
  - **hidden 1** (blue `#2a78d6`, center x=130): rows "0.5 × 1.0" = +0.5, "0.3 × 2.0" = +0.6, "bias" = −0.1; total "sum 1.0 → ReLU" with bold 20px "1.0".
  - **hidden 2** (aqua `#199e70`, center x=365): rows "−0.4 × 1.0" = −0.4, "0.6 × 2.0" = +1.2, "bias" = −0.2; total "sum 0.6 → ReLU" with "0.6".
  - **output** (green `#008300`, center x=600): rows "0.5 × h1(1.0)" = +0.5, "0.5 × h2(0.6)" = +0.3; total "prediction" with "0.8".
- **Flow arrows:** gray arrows between columns at y=240.
- **Takeaway (bold 13px orange, bottom center):** "6 multiplications + 2 biases + 2 bends + 1 sum = one prediction: 0.8"

## Change One Input, Watch a Neuron Fall Asleep

**Tags:** `where it's used` (blue), `worked example` (green)

- **Shorter visits** — drop x2 from 2.0 to 1.0 and rerun the same arithmetic
- **Hidden 1 shrinks** — 0.5 + 0.3 − 0.1 = 0.7; still positive, still awake
- **Hidden 2 hits zero** — −0.4 + 0.6 − 0.2 = 0.0; ReLU outputs 0: the neuron went silent
- **Output drops** — 0.5×0.7 + 0.5×0 = 0.35: the score falls from 0.8 to 0.35
- **This is model.predict()** — serving, scoring, inference: all of it is exactly this pass

*Example (italic):* Tracing which neurons went silent for an odd prediction is a real debugging move, not a classroom trick.

**Key point:** Because the pass is plain arithmetic, you can follow any single prediction by hand and see which paths carried it.

### Visualization (canvas `c3`, 720×300)

Before/after pair of mini network diagrams split by a dashed divider at x=360.

- **Title (bold 15px, ink, top center):** "Same Network, Shorter Visits: x2 = 2.0 → 1.0"
- **Each mini net** (drawn by a `miniNet` helper): inputs x1 (blue circle r=17) and x2 (aqua) on the left, hidden nodes (r=19, ink stroke) in the middle, output node (r=21, green stroke) on the right; gray `#aaa` wires.
- **Left net (origin x=70):** title bold 13px green "before: score 0.8"; node values 1.0, 2.0, h1=1.0, h2=0.6, out=0.8.
- **Right net (origin x=420):** title bold 13px orange "after: score 0.35"; node values 1.0, 1.0, h1=0.7, h2=0.0, out=0.35. The h2 node is drawn "dead": gray `#aaa` stroke, `#f2f2f2` fill; its outgoing wire faded to `#ddd` while the live h1→out wire is highlighted blue (width 2.5). Bold 12px orange labels below h2: "ReLU(0.0) = 0" / "asleep".
- **Takeaway (bold 13px orange, centered at y=288):** "h2 = −0.4 + 0.6 − 0.2 = 0.0 → silent, so out = 0.5×0.7 + 0.5×0 = 0.35"

## The Confusion: Predicting Is Not Learning

**Tags:** `common mistake` (red), `rule of thumb` (blue)

- **Weights are frozen** — during a forward pass, no weight changes, ever
- **Same in, same out** — feed (1.0, 2.0) a thousand times, get 0.8 a thousand times
- **The model doesn't "remember"** — this network keeps no notes between predictions
- **Learning is separate** — training compares 0.8 to the truth and nudges weights afterwards
- **Only new weights change answers** — after one training nudge, the same input gives 0.88

*Example (italic):* A deployed model predicting all day is doing forward passes only — it learns nothing from the traffic.

**Key point:** The forward pass answers; it never learns. If the answer changed, someone changed the weights.

### Visualization (canvas `c4`, 720×300)

Dot-and-line chart of six identical prediction calls, flat until a training update.

- **Title (bold 15px, ink, top center):** "Same Input (1.0, 2.0), Six Calls to the Model"
- **Data:** predictions by call 1–6: `[0.8, 0.8, 0.8, 0.8, 0.88, 0.88]`.
- **Axes:** padding top 56 / bottom 62 / left 62 / right 40; y from 0 to 1.0; gray `#999` L-shaped axes; 12px x labels "call 1" … "call 6"; rotated 12px gray y-axis label "predicted score".
- **Update marker:** dashed magenta `#d55181` vertical line (width 2, dash 6/4) between call 4 and call 5, with bold 12px magenta two-line label above: "training nudges" / "the weights".
- **Points:** r=7 dots, calls 1–4 in blue `#2a78d6`, calls 5–6 in green `#008300`; bold 13px value labels ("0.80" ×4, "0.88" ×2) above each; thin `#bbb` connecting polyline.
- **Annotations:** bold 13px blue "frozen weights: 0.80 every time" near the first point; bold 13px green right-aligned "new weights, new answer" near the last point.
- **Takeaway (bold 13px orange, bottom center):** "the forward pass never changed anything — only the training step did"

## Regeneration instructions

- **Template:** tutorials topic-page layout (social-graph reference style). h1 + `.subtitle`, then 4 `.card-section` blocks; each has an `<h2>` (1.3rem, `#1a5276`, 2px solid `#2980b9` bottom border) and a `table.layout` (`width:100%`, border-collapse collapse) with one row: `td.text-col` (50%) and `td.viz-col` (50%), both `vertical-align: top`, padding 12px.
- **Text column structure:** `.tags` row of pill spans first (`.tag` — inline-block, 0.72rem, weight 600, padding 2px 10px, radius 10px; `.tag.blue` bg `rgba(26,82,118,0.12)` color `#1a5276`; `.tag.green` bg `rgba(39,174,96,0.15)` color `#27ae60`; `.tag.red` bg `rgba(231,76,60,0.12)` color `#e74c3c`; `.tag.orange` bg `rgba(230,126,34,0.15)` color `#e67e22`), then a `<ul>` (0.92rem) of one-line bullets each opening with `<b>` in `#1a5276`, one italic `.example` paragraph (`#555`, 0.9rem), and one `.key-point` callout (bg `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem) with a `<strong>Key point:</strong>` lead-in.
- **Page CSS:** global reset; body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem. Canvases styled `width:100%`, border `1px solid #e0e0e0`, radius 4px. No nav bar, no back/home links.
- **Canvas:** each canvas declares `width="720" height="300"`; a shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates; shared `arrow()` (line with filled triangular head) and `node()` (circle with one- or two-line centered label) helpers. All data is hardcoded literal arrays (no `Math.random()`). Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette (JS object `P`):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- In regenerated HTML, any card links use `.html` extensions (this page has none — it is a leaf tutorial page).
