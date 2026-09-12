# Layers & Depth

**Page type:** detail page (tutorial layout: h1 + subtitle, 4 `.card-section` blocks, each h2 + two-column `table.layout` with text left 50% / canvas right 50%)
**HTML title tag:** Layers & Depth

**Subtitle:** Each layer builds bigger parts out of the last layer's smaller parts — edges become loops, loops become an 8 — so "deep" means more reuse of parts, not just more machinery

## From Strokes to Loops to "That's an 8"

**Tags:** `core idea` (blue), `running example` (green)

- **The task** — look at a small picture of a handwritten digit and say which digit it is
- **Layer 1 sees strokes** — its neurons fire on tiny edges: short curves, lines, slants
- **Layer 2 sees parts** — its neurons combine edges into loops, corners, crossings
- **Layer 3 sees digits** — one neuron asks "two loops stacked?" — that is an 8
- **Each layer reads the last** — layer 2 never sees pixels, only layer 1's edge reports

*Example (italic):* No single neuron knows what an 8 is — one knows curves, one knows loops, one knows "loop on loop".

**Key point:** A layer's job is to build slightly bigger concepts out of the previous layer's smaller ones.

### Visualization (canvas `c1`, 720×300)

Three-panel diagram: what each of three layers "sees" in the same handwritten 8.

- **Title (bold 15px, ink `#1a5276`, top center):** "Three Layers Looking at the Same Handwritten 8"
- **Three outlined boxes** 160×150 centered at x=130, 360, 590 (vertical center y=150), stroke width 2, colors blue `#2a78d6`, aqua `#199e70`, magenta `#d55181`; bold 13px captions below each: "layer 1: edges", "layer 2: loops & corners", 'layer 3: "an 8"'.
- **Panel 1:** seven short blue stroke segments (round caps, width 3) scattered where an 8's edges would be — coordinates `[90,105→115,95]`, `[150,95→172,108]`, `[78,150→82,175]`, `[178,145→174,172]`, `[95,205→120,214]`, `[150,215→172,204]`, `[118,148→142,152]`.
- **Panel 2:** two aqua circles (r=26 at (330,118), r=30 at (395,180)) plus two short crossing strokes; 12px aqua labels "loops" and "corner".
- **Panel 3:** the digit 8 as two magenta circles (width 5): r=24 at (590,122) and r=30 at (590,176).
- **Arrows:** gray `#6b7280` arrows between the boxes at y=150, each with a 12px gray "combine" label above.
- **Takeaway (bold 13px orange `#d95926`, centered at y=288):** "each layer only reads the layer before it — never the raw pixels again"

## The Loop Detector, With Numbers You Can Check

**Tags:** `worked example` (green), `core idea` (blue)

- **Loop neuron** — reads 4 edge detectors (top, right, bottom, left curve), weight 0.3 each
- **Its bias is −1.0** — it fires only when the sum climbs above 0
- **All 4 curves present** — 0.3×4 − 1.0 = 0.2 > 0 → "I see a loop"
- **One curve missing** — 0.3×3 − 1.0 = −0.1 → silent, a C-shape is not a loop
- **The 8 neuron** — 0.6×(top loop) + 0.6×(bottom loop) − 0.9: both loops give 0.3 → fires

*Example (italic):* Feed it a "0": one loop only, so 0.6×1 + 0.6×0 − 0.9 = −0.3 — the 8 neuron stays quiet.

**Key point:** Every layer runs the same arithmetic as a single neuron — the magic is only in what its inputs already mean.

### Visualization (canvas `c2`, 720×300)

Two-stage network diagram: four curve inputs into a loop neuron, then two loop signals into the 8 neuron, with all weights and sums on the wires.

- **Title (bold 15px, ink, top center):** "The Loop Neuron and the 8 Neuron, Wired With Real Numbers"
- **Left stage:** four input nodes (white circles r=14, blue stroke, bold "1" inside) at x=118, y=66/122/178/234, right-aligned 12px gray labels "top curve", "right curve", "bottom curve", "left curve"; blue arrows into the loop neuron at (300,150), each wire labeled bold 12px blue "× 0.3".
- **Loop neuron:** circle r=30 at (300,150), fill `rgba(25,158,112,0.12)`, stroke aqua `#199e70`; labels inside "loop?" and "0.2"; 12px gray "bias −1.0" below; bold 12px aqua annotation above: "1.2 − 1.0 = 0.2 → fires".
- **Middle:** two gray arrows fan out from the loop neuron to two aqua nodes at (505,100) and (505,200) (white circles r=14, bold "1"), with 11px gray note "same detector," / "two positions" at (420,128–142); node labels 11px gray "top loop = 1" and "bottom loop = 1"; aqua arrows into the 8 neuron each labeled bold 12px aqua "× 0.6".
- **8 neuron:** circle r=30 at (640,150), fill `rgba(213,81,129,0.12)`, stroke magenta `#d55181`; labels "8?" and "0.3"; 12px gray "bias −0.9" below; bold 12px magenta annotation above: '1.2 − 0.9 = 0.3 → "an 8"'.
- **Takeaway (bold 13px orange, centered at (470,285)):** 'a "0" gives the 8 neuron 0.6 − 0.9 = −0.3 → silent'

## Why Depth Pays: One Loop Detector, Many Customers

**Tags:** `where it's used` (blue), `best practice` (green)

- **Reuse** — the same loop detector feeds the 8, 0, 6, and 9 neurons; built once, used four times
- **Shallow shares less** — a single layer shares one level of parts; depth builds parts from parts
- **Less data per concept** — every 0, 6, 8, 9 example helps train the one shared loop detector
- **The pattern is general** — pixels→edges→parts→objects; letters→words→phrases→topics
- **Transfer learning** — keep the early part-detecting layers, retrain only the top for a new task

*Example (italic):* To add a "9" detector you wire up the existing loop and tail detectors — no need to relearn curves.

**Key point:** Depth is a parts library — shared low-level parts make each new high-level concept cheap to add.

### Visualization (canvas `c3`, 720×300)

Fan-out reuse diagram: three shared part detectors feeding four digit neurons.

- **Title (bold 15px, ink, top center):** "One Shared Part, Four Digits Built From It"
- **Left column** (part detectors, white circles r=22 at x=170 with bold 11px labels inside): "loop" aqua `#199e70` at y=90, "tail stroke" blue `#2a78d6` at y=160, "open curve" violet `#4a3aa7` at y=230; bold 12px gray column caption "layer 2: part detectors" at y=275.
- **Right column** (digit neurons, circles r=20 at x=540, fill `rgba(213,81,129,0.10)`, stroke magenta, bold 15px digit inside): "0" at y=70, "6" at y=130, "8" at y=190, "9" at y=250; caption "layer 3: digit detectors" at y=285.
- **Wires:** the loop detector sends highlighted aqua arrows (width 2.2) to all four digits; thinner violet arrow from "open curve" to "6"; thinner blue arrow from "tail stroke" to "9".
- **Annotations:** bold 13px aqua centered at (372, 52/68): "the loop detector is trained once," / "used by 0, 6, 8 and 9"; bold 12px orange left-aligned at (590, 120/136): "every 0/6/8/9 example" / "improves the shared loop".

## The Confusion: Deep Is Not Just "More"

**Tags:** `common mistake` (red), `rule of thumb` (blue)

- **Wide vs deep** — 100 neurons in 1 layer and 100 across 4 layers are very different machines
- **Wide memorizes** — one flat layer tends to learn whole-shape templates, one per pattern
- **Deep composes** — stacked layers learn parts of parts, and parts get reused
- **Nobody assigns meanings** — training discovers "loop-ish" units; we name them afterwards
- **Deeper isn't free** — extra layers need data to feed them and are harder to train

*Example (italic):* A 50-layer net on 200 examples mostly learns noise — depth without data buys nothing.

**Key point:** Ask "what parts could layers share?" — if the task has no reusable parts, depth is just expense.

### Visualization (canvas `c4`, 720×300)

Two-panel comparison split by a dashed divider at x=392: wide/shallow template machine vs deep parts machine.

- **Title (bold 15px, ink, top center):** "Adding a New Digit: Template Machine vs Parts Machine"
- **Left panel** — header bold 13px violet `#4a3aa7`: "wide & shallow: whole-shape templates". Four 58×58 violet-outlined squares in a row (starting x=60, 72px spacing, y=76) each holding a bold 20px violet digit "0", "6", "8", "9" with 11px gray "own pixels" beneath. Bold 12px dark-red `#c0392b` two-line caption: "4 digits = 4 full templates, no parts library" / "new digit → learn everything again".
- **Right panel** — header bold 13px green `#008300`: "deep: shared parts, cheap additions". Three stacked outlined boxes 90×30 at x=420 (y=80, 42px spacing) labeled "edges" (blue), "loop" (aqua), "tail" (violet). Aqua arrows from the loop box to four green digit circles (r=16 at x=608, y≈86–206) holding "0", "6", "8", "9". Bold 12px green caption: "new digit → one new wire-up of old parts".
- **Takeaway (bold 13px orange, centered at y=290):** "same neuron count, different organization — depth buys reuse, not horsepower"

## Regeneration instructions

- **Template:** tutorials topic-page layout (social-graph reference style). h1 + `.subtitle`, then 4 `.card-section` blocks; each has an `<h2>` (1.3rem, `#1a5276`, 2px solid `#2980b9` bottom border) and a `table.layout` (`width:100%`, border-collapse collapse) with one row: `td.text-col` (50%) and `td.viz-col` (50%), both `vertical-align: top`, padding 12px.
- **Text column structure:** `.tags` row of pill spans first (`.tag` — inline-block, 0.72rem, weight 600, padding 2px 10px, radius 10px; `.tag.blue` bg `rgba(26,82,118,0.12)` color `#1a5276`; `.tag.green` bg `rgba(39,174,96,0.15)` color `#27ae60`; `.tag.red` bg `rgba(231,76,60,0.12)` color `#e74c3c`; `.tag.orange` bg `rgba(230,126,34,0.15)` color `#e67e22`), then a `<ul>` (0.92rem) of one-line bullets each opening with `<b>` in `#1a5276`, one italic `.example` paragraph (`#555`, 0.9rem), and one `.key-point` callout (bg `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem) with a `<strong>Key point:</strong>` lead-in.
- **Page CSS:** global reset; body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem. Canvases styled `width:100%`, border `1px solid #e0e0e0`, radius 4px. No nav bar, no back/home links.
- **Canvas:** each canvas declares `width="720" height="300"`; a shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates; a shared `arrow()` helper draws lines with filled triangular heads. All data is hardcoded literal arrays (no `Math.random()`). Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette (JS object `P`):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- In regenerated HTML, any card links use `.html` extensions (this page has none — it is a leaf tutorial page).
