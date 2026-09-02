# Synthetic Data — LLMs & Simulation

**Page type:** detail page (four titled sections, each a two-column row: text left 45%, canvas right 55%)
**HTML title tag:** Synthetic Data — LLMs & Simulation: Generating Data When the Real Thing Is Scarce

**Subtitle:** The newest acquisition channel skips collection entirely: prompt a model, render a scene, or interpolate what you already have, and a dataset appears — cheap, perfectly labeled, and bounded by what the generator already believes.

**Intro callout (blue-left-border box):** Every other channel in this survey finds data that exists; this one manufactures it. Large language models write training corpora on request, simulators render labeled scenes on demand, and classic augmentation stretches small datasets into large ones. The successes are real — some of the strongest small models ever trained ate mostly synthetic text — but so are the failure modes, and they all trace back to one fact: generated data contains no ground truth the generator did not already hold.

## 1. LLM-generated corpora — the teacher writes the textbook

Instead of hiring annotators, ask a strong model to write the training set: prompt a large LLM for examples, filter the output, and fine-tune a smaller model on it.

- **Alpaca (2023):** tens of thousands of instruction examples from a commercial LLM
- **Alpaca's cost:** a few hundred dollars of API calls
- **Alpaca's payoff:** a small open model that followed instructions well
- **Phi models:** Microsoft's small models trained on synthetic "textbook" text
- **Phi's lesson:** curated synthetic data can beat raw web crawl at small sizes
- **Distillation:** teacher generates, student trains — now standard practice
- **Curation:** prompting strategy, dedupe, and filtering carry the value

Key point: The dataset costs API calls instead of annotator hours — but every example is bounded by what the teacher already knows, so the student inherits the teacher's blind spots along with its skills.

### Visualization (canvas `c1`, 720×340)

Distillation flow: teacher-model box, generated-corpus card stack with a cost tag, student-model box, and a quality-gate note below.

- **Title (bold 14px `#1a5276`, centered, y=22):** "Distillation: a large model writes the training set for a small one"
- **Teacher box:** 190×86 at (30, 100), fill `rgba(26,82,118,0.12)`, 2px `#1a5276` border. Left-aligned at x+14: bold 12px `#1a5276` "LARGE TEACHER MODEL" (y+24); 11px `#555` "commercial LLM behind" / "an API" (y+46, y+62); 11px `#999` "prompted for examples" (y+80).
- **Corpus stack:** three 180×86 rectangles offset by 7px steps (back two at (279,86) and (272,93), white fill, 1.5px `#e67e22` border), front card at (265, 100), white fill, 1.5px `#e67e22` border. Left-aligned at x+14: bold 12px `#e67e22` "GENERATED EXAMPLES" (y+24); 11px `#666` "tens of thousands of" / "instruction–response pairs" (y+46, y+62). Below the stack, bold 11px `#e67e22` centered at x=360, y=222: "~a few hundred dollars of API calls (Alpaca, 2023)".
- **Student box:** 190×86 at (500, 100), fill `rgba(39,174,96,0.10)`, 2px `#27ae60` border. Left-aligned at x+14: bold 12px `#27ae60` "SMALL STUDENT MODEL" (y+24); 11px `#666` "fine-tuned on the" / "synthetic corpus" (y+46, y+62); 11px `#999` "cheap to run, easy to ship" (y+80).
- **Arrows:** 2px `#999` horizontal arrows with filled arrowheads at y=143, from x=222 to x=263 and from x=457 to x=498.
- **Quality gate:** 420×40 box centered at x=360, top y=250, white fill, 1.5px dashed `#8e44ad` border; bold 12px `#8e44ad` centered "QUALITY GATE" (y=266); 11px `#666` centered "dedupe and filter malformed examples — cheap generation, careful curation" (y=282). Thin 1px `#ccc` connector from the front corpus card's bottom edge (x=355, y=186) down to the gate's top edge.
- **Caption (12px `#999`, centered, y = h−14):** "Cost moves from annotator hours to API calls — capability stays bounded by the teacher"

## 2. Simulation — perfect labels from rendered worlds

When labels are expensive or dangerous to collect — a pedestrian stepping into traffic — render the scene instead: the simulator knows every pixel's identity for free.

- **CARLA:** open-source driving simulator rendering full urban scenes
- **Free ground truth:** depth, segmentation, and boxes with every frame
- **Game engines:** Unity and NVIDIA Omniverse render labeled imagery on demand
- **Rare events:** schedule the crash a real fleet waits years to see
- **Robotics:** a simulated arm practices millions of grasps overnight
- **Domain randomization:** vary textures and physics so policies survive reality
- **Sim-to-real gap:** models trained purely in simulation degrade on real sensors

Key point: Simulation inverts the usual economics: labels are free and rare events are schedulable, but realism becomes the scarce resource. Every simulated dataset carries a sim-to-real gap that must be measured on real data before deployment.

### Visualization (canvas `c2`, 720×360)

Two scene frames side by side — a simulated frame with green ground-truth label overlays and a real camera frame with unlabeled gray shapes — joined by a red sim-to-real gap arrow.

- **Title (bold 14px `#1a5276`, centered, y=22):** "Perfect labels in the simulator, an accuracy gap in the world"
- **Simulated frame (left):** header bold 12px `#1a5276` centered at x=185, y=48: "SIMULATED FRAME". Frame rect (40, 58) 290×190, fill `#f8f9fa`, 1.5px `#1a5276` border. Road: two 1.5px `#999` lines from the bottom corners converging toward (185, 90). Car: filled `rgba(26,82,118,0.35)` rect 64×32 at (100, 170). Pedestrian: filled `#555` rect 10×26 at (252, 158) with a 5px-radius `#555` circle head above it. Green ground-truth overlays: 1.5px dashed `#27ae60` boxes around the car (94, 162, 76×46) and pedestrian (244, 146, 26×44), each with a bold 10px `#27ae60` tag above: "car" and "pedestrian"; a third tag "lane" at (185, 232) beside a short dashed `#27ae60` line. Below the frame, 11px `#27ae60` centered at x=185, y=266: "depth, segmentation, boxes — free with every frame".
- **Real frame (right):** header bold 12px `#666` centered at x=535, y=48: "REAL CAMERA FRAME". Frame rect (390, 58) 290×190, white fill, 1.5px `#999` border. Same scene shapes in gray: road lines `#ccc`, car rect filled `#bbb` at (450, 170), pedestrian `#bbb` at (602, 158). Orange 14px bold `#e67e22` "?" marks above the car and pedestrian. Below the frame, 11px `#e67e22` centered at x=535, y=266: "labels cost human annotation per frame".
- **Gap arrow:** 2px `#e74c3c` double-headed horizontal arrow at y=300 from x=180 to x=540; bold 12px `#e74c3c` centered above (y=292): "SIM-TO-REAL GAP"; 11px `#666` centered below (y=318): "rendered textures, noise, and physics never match the sensor exactly".
- **Caption (12px `#999`, centered, y = h−14):** "The simulator knows every pixel's identity — the model still has to survive real sensors"

## 3. Augmentation & tabular synthesis — stretching what you have

Long before LLMs, practitioners stretched scarce datasets by transforming what they already had — the oldest synthetic-data tricks still pay for themselves first.

- **Image augmentation:** flips, crops, rotations, and color jitter multiply a set for free
- **Still standard:** augmentation ships in every modern vision training pipeline
- **SMOTE:** interpolates new minority-class rows between real neighbors
- **Why it helps:** the classifier sees more than a handful of positives
- **Tabular generators:** copula- and GAN-based models sample look-alike rows
- **Privacy stand-ins:** synthetic tables substitute for restricted originals
- **The caveat:** generated rows inherit the originals' biases and can leak outliers

Key point: Augmentation adds variation, not information: it teaches the model invariances you assert — a flipped cat is still a cat — rather than facts about the world it has never seen.

### Visualization (canvas `c3`, 720×380)

Two stacked panels: image augmentation multiplying one photo into four variants, and a SMOTE scatter showing interpolated minority points.

- **Title (bold 14px `#1a5276`, centered, y=22):** "Multiplying what you have: one image becomes many, two points become five"
- **Top panel label (11px `#666`, left-aligned at (40, 52)):** "Image augmentation — one labeled photo, many training examples"
- **Original image:** 70×70 rect at (50, 66), fill `rgba(26,82,118,0.12)`, 2px `#1a5276` border; inside, a simple glyph — filled `#1a5276` triangle (peak near the top-left third) and a 6px-radius `#1a5276` circle "sun" near the top-right; 10px `#666` centered label "original" below (y=152).
- **Variants:** four 70×70 rects at x=250, 360, 470, 580 (top y=66), white fill, 1.5px `#27ae60` border, each with the glyph transformed (mirrored triangle; enlarged/offset triangle for crop; triangle tilted for rotate; glyph drawn in `#e67e22` for color jitter); thin 1px `#bbb` connector lines from the original's right edge to each variant's left edge; 10px `#666` centered labels below each (y=152): "flip", "crop", "rotate", "color jitter".
- **Bottom panel label (11px `#666`, left-aligned at (40, 196)):** "SMOTE — interpolated minority points for an imbalanced table"
- **Scatter (region y=210 to 340):** about 14 filled `#ccc` majority dots (radius 4) scattered across x=70–420; four filled `#e74c3c` real-minority dots (radius 5) in x=460–650; dashed (4/4) 1.5px `#999` segments connecting three neighbor pairs among them; three filled `#27ae60` synthetic dots (radius 4) at the segment midpoints.
- **Legend (right-aligned block near (540, 214), one row per entry, 10px `#666` labels):** `#ccc` dot "majority class", `#e74c3c` dot "real minority", `#27ae60` dot "synthetic (interpolated)".
- **Caption (12px `#999`, centered, y = h−14):** "Free variation from asserted invariances — no new facts, just more views of the same ones"

## 4. Failure modes — collapse, amplification, and terms of service

Synthetic data has a physics of its own: a generator can only re-emit what it already believes, and feeding its output back to it makes that belief narrower every round.

- **Model collapse:** models trained recursively on model output degrade
- **Tails vanish first:** rare events disappear before the average moves
- **The evidence:** Shumailov et al., Nature 2024, measured the decay
- **No new ground truth:** a generator amplifies what it already believes
- **Error amplification:** one hallucinated fact, repeated at training-set scale
- **Terms of service:** providers often bar training competing models on outputs
- **Independent validation:** keep a real, separately collected holdout set

Key point: Treat synthetic data as leverage on real data, not a replacement for it: know what fraction of the training mix is synthetic, and validate on independently collected examples — the tails you lose are exactly the rare cases the data was supposed to cover.

### Visualization (canvas `c4`, 720×360)

Model-collapse series: four bell curves over one axis, narrowing and rising generation by generation, with the tail region flagged.

- **Title (bold 14px `#1a5276`, centered, y=22):** "Model collapse — each generation trained on the last one loses the tails"
- **Axis:** 1.5px `#999` horizontal line at y=280 from x=60 to x=660; 10px `#999` centered label "output distribution" at (360, 296).
- **Curves:** four Gaussian curves centered at x=360, baseline y=280, drawn as ~200-step polylines of `peak × exp(−((x−360)/σ)²)`: generation 1 σ=130 peak 130, 2px `#1a5276`; generation 2 σ=95 peak 150, 2px `rgba(231,76,60,0.45)`; generation 3 σ=65 peak 175, 2px `rgba(231,76,60,0.7)`; generation 4 σ=40 peak 200, 2.5px `#e74c3c`.
- **Legend (top-left block starting (70, 52), 18px line spacing):** 22px line swatch in each curve's color/width + 11px `#555` labels: "generation 1 — trained on real data", "generation 2 — trained on gen-1 output", "generation 3 — trained on gen-2 output", "generation 4 — trained on gen-3 output".
- **Tail annotation:** bold 11px `#e67e22` two-line note right-aligned near (655, 210): "rare cases live out here —" / "gone by generation 3", with a thin 1px `#e67e22` connector line from the note down to the gen-1 curve's right tail near (560, 268).
- **Caption (12px `#999`, centered, y = h−14):** "The distribution narrows every round — the rare cases disappear before anyone notices the average"

## Regeneration instructions

- **Template:** backlog multi-section detail page. After h1, `.subtitle`, and `.intro` callout, one `.lang-section` per numbered section: `h2` (1.3rem `#1a5276`, `border-bottom: 2px solid #2980b9`, 4px padding-bottom), then a `table.layout` with one `<tr>`: left `<td class="text-col">` (45%) with lead paragraph, bullets, `.key-point`; right `<td class="viz-col">` (55%) with the canvas. Section h2 headings carry the "1.–4." numbers shown above.
- **Bullet style:** each bullet is a bold label + short phrase that fits on one line at normal page width — no wrapping, no full-sentence bullets; split dense content into more bullets rather than longer ones. Bullet `<strong>` labels are colored `#1a5276` (via `li strong { color: #1a5276; }`).
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with `border-bottom: 2px solid #2980b9`; `.subtitle` `#666` 0.95rem; `.intro` — background `#f0f4f8`, `border-left: 3px solid #2980b9`, padding 8px 12px, 0.9rem, 32px margin-bottom; `.key-point` — background `#f8f9fa`, `border-left: 3px solid #e74c3c`, padding 8px 12px, 0.9rem; `ul` 0.92rem; `li` `white-space: nowrap` is NOT used — one-line fit comes from keeping phrases short; canvases `width: 100%`, `border: 1px solid #e0e0e0`, radius 4px; `.lang-section` 40px margin-bottom.
- **Canvases:** intrinsic width 720, heights 340/360/380/360 as specified; shared `setupCanvas(id, h)` helper scales by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates). Canvas fonts use `-apple-system, sans-serif`.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, purple `#8e44ad`; grays `#555`/`#666`/`#999`/`#bbb`/`#ccc`; translucent fills `rgba(26,82,118,0.12)`, `rgba(26,82,118,0.35)`, `rgba(39,174,96,0.10)`, `rgba(231,76,60,0.45)`, `rgba(231,76,60,0.7)`.
- No nav bar, no back/home links. In regenerated HTML, any card links use `.html` extensions.
