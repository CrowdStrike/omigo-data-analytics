# Real-Time Object Detection

**Page type:** detail page (one h2 per pitfall, each followed by a two-column obj-table row: text left ~40%, canvas right ~60%)
**HTML title tag:** Real-Time Object Detection - Domain-Specific Pitfalls

**Subtitle:** On-device, real-time visual inference must be fast, power-efficient, private, and correct — all at once, on hardware that fights back.

## 16ms Latency Budget

**60fps Means the ENTIRE Pipeline Fits in 16ms**

- **The budget:** 60fps allows just 16ms per frame for the entire capture-to-display pipeline.
- **Five stages:** Capture → preprocess → inference → post-process → render overlay all share that 16ms.
- **Inference dominates:** The model forward pass alone often consumes half of the 16ms budget.
- **Hard cutoff:** Any frame that runs over 16ms is dropped outright — being close earns nothing.
- **Visible lag:** Dropped frames read as lag, and on a head-mounted display that lag causes nausea.
- **No partial credit:** A 20ms model at 95% accuracy loses to a 12ms model at 88% accuracy.
- **Fast wins ties:** Real-time systems must be fast AND correct, and speed breaks the tie.

### Visualization (canvas `c1`, 720×300)

Two-part chart: a horizontal stacked pipeline bar against a 16ms budget, plus a mini bar chart of dropped frames vs per-frame latency.

- **Title (bold 17px `#1a5276`, top center):** "Frame Budget: Every Stage Must Fit in 16ms".
- **Stacked bar:** background track `rgba(41,128,185,0.3)` at (60, 40) size 600×32; segments proportional to time out of 16ms total, each with white 17px label "<name> <t>" inside (or just the number if the segment is narrower than 60px):
  - Capture 2ms `#27ae60`; Preproc 1.5ms `#27ae60`; Inference 8ms `#e74c3c`; Post 2ms `#f39c12`; Render 2.5ms `#2980b9`.
- **Limit marker:** vertical dashed (5/4) line `#1a5276` width 2 at the bar's right end, labeled "16ms limit" (17px, right-aligned below bar). Left-aligned red 17px note below bar: "Inference alone: half the budget".
- **Mini bar chart title (bold 17px `#333`, centered at y=125):** "Frames dropped per second vs per-frame latency".
- **Mini bars (width 70, spacing 95, starting x=80, baseline y=215, scale max 40 over 70px):** 12ms→0, 16ms→0, 20ms→12, 25ms→18, 33ms→27, 50ms→36. Color `#27ae60` if 0 dropped (drawn as a 4px stub), `#f39c12` if <20, `#e74c3c` otherwise. Value label above bar: "OK" in green for 0, "-N" in red otherwise; latency label below in `#333`.
- **Caption (bold 17px `#e74c3c`, centered at y=262):** "Over budget = dropped frames = visible lag = nausea on head-mounted."

## Power/Thermal Constraints On-Device

**2W Power Budget: Accuracy Traded for a Device That Stays Alive**

- **The ceiling:** Smart glasses run on a ~2W power budget, and compute gets only a fraction of it.
- **Shared draw:** Display, camera, and radios all draw from that same 2W before compute sees any.
- **Battery math:** Full-resolution YOLO running continuously drains the battery in ~20 minutes.
- **Dead device:** The "best" model is worthless once the battery it flattened stops responding.
- **The trade:** A smaller quantized model makes more detection errors, but it runs all day long.
- **Joint metric:** Accuracy must be evaluated jointly with watts drawn, never in isolation.
- **Thermal throttling:** Sustained load heats the device, the SoC throttles, and latency degrades.
- **Cold-start bias:** Benchmarks measured on a cold device are optimistic about a long session.

### Visualization (canvas `c2`, 720×300)

Table-style horizontal bar comparison: model accuracy vs battery life.

- **Title (bold 17px `#1a5276`, top center):** "2W Budget: Accuracy vs. Battery Life".
- **Column headers (bold 17px `#333` at y=52):** "Model" (x=40), "Accuracy" (x=260), "Battery (min)" (x=470).
- **Rows (starting y=68, 46px apart; accuracy bar width = acc×1.6 px, battery bar width = batt×0.35 px, both 20px tall, value labels in `#333` after each bar):**
  - YOLO full-res — accuracy 95% (bar `#27ae60`), battery 20 min (bar `#e74c3c`)
  - Medium quantized — accuracy 88% (bar `#f39c12`), battery 180 min (bar `#f39c12`)
  - Tiny on-device — accuracy 78% (bar `#e74c3c`), battery 480 min (bar `#27ae60`)
- **Bottom annotations (centered):** bold 17px `#e74c3c` at y=235: "The most accurate model kills the device in 20 minutes."; 17px `#555` at y=258: "Smaller model = more errors, but the device stays alive."; 17px `#555` at y=280: "Evaluate accuracy jointly with watts — never in isolation."

## Training Images ≠ Live Camera Feed

**Curated Datasets vs. What a Glasses-Cam Actually Sees**

- **Training side:** Benchmark datasets are curated — good lighting, centered objects, sharp focus.
- **Deliberate framing:** Someone chose every shot, so clean backgrounds and clear subjects dominate.
- **Live side:** A worn camera delivers motion blur, partial occlusion, extreme angles, and reflections.
- **Uncurated input:** Low light and whatever the wearer happened to face arrive with no filtering.
- **The gap:** The distance between ImageNet-style photos and a real glasses-cam feed is enormous.
- **mAP won't transfer:** Benchmark mAP therefore does not predict accuracy on the live device feed.
- **The fix costs:** Closing the gap needs in-domain data collection and augmentation pipelines.
- **Worth paying:** That collection is expensive, but the alternative is a model that works only in the lab.

### Visualization (canvas `c3`, 720×300)

Side-by-side comparison boxes with a gap arrow between them.

- **Title (bold 17px `#1a5276`, top center):** "Domain Gap: Curated Dataset vs. Live Glasses-Cam".
- **Left box (stroke `#27ae60` width 2, fill `rgba(39,174,96,0.08)`, at (40,40) size 300×170):** heading "Training (ImageNet-style)" in bold green centered; bullet lines (17px `#333`, left-aligned at x=70, y=95..195 step 25): "Good lighting", "Centered objects", "Sharp focus", "Deliberate framing", "Clean backgrounds".
- **Right box (stroke `#e74c3c` width 2, fill `rgba(231,76,60,0.08)`, at (380,40) size 300×170):** heading "Live camera feed" in bold red centered; lines at x=410: "Motion blur", "Partial occlusion", "Extreme angles", "Reflections", "Low light".
- **Gap arrow:** orange `#e67e22` arrow width 2.5 from (345,125) to (375,125) with arrowhead.
- **Bottom annotations (centered):** bold 17px `#e74c3c` at y=245: "Benchmark mAP 92% → live mAP 60s. The gap is the product."; 17px `#555` at y=270: "Collect in-domain data from the actual device or ship a lab-only model."

## Motion Blur at Head-Turn Speed

**User Turns Head → Whole Frame Is Smeared → Detector Goes Blind**

- **The physics:** A head turn during a ~50ms exposure smears the whole frame — nothing stays sharp.
- **Trained on sharp:** A detector trained on crisp images has never seen this input distribution.
- **Silent failure:** It fails silently rather than gracefully, reporting no signal that input went bad.
- **Option one:** Make the model blur-robust — harder training, bigger model, more latency and watts.
- **Option two:** Skip unstable frames instead, which means going blind roughly 30% of the time.
- **Worst timing:** Head turns happen exactly when the user is looking for something new.
- **Matters most:** So the blurriest moments are precisely the ones where detection matters most.

### Visualization (canvas `c4`, 720×300)

Frame timeline: seven camera frames across a head turn, with a detector status row underneath.

- **Title (bold 17px `#1a5276`, top center):** "Head Turn: Sharp → Smeared → Sharp".
- **Frames (80×60 outlined boxes, spacing 95, starting x=45, y=45; label below each in 17px `#333`):** stable (blur 0), stable (0), turn start (1), turning (2), turning (2), settling (1), stable (0).
- **Frame color by blur level:** 0 → `#27ae60`, 1 → `#f39c12`, 2 → `#e74c3c`. Inside each frame: a sharp filled circle (radius 8) when blur 0; a horizontal smear rectangle (length blur×22, height 10, alpha 0.6) when blurred.
- **Detector row:** bold label "Detector:" at (45, 165); under each frame at y=190: "OK" in `#27ae60` for sharp frames, "MISS" in `#e74c3c` for blurred ones.
- **Bottom annotations (centered):** bold 17px `#e74c3c` at y=230: "~30% of frames unusable during head motion."; 17px `#555` at y=255: "Choose: train blur-robust (bigger model) or skip unstable frames (blind 30% of time)."; 17px `#555` at y=277: "Head turns happen exactly when the user looks for something new."

## Privacy Constraints Limit Processing

**Must Detect Faces Before Processing — But Detection IS Processing**

- **The rule:** Raw video of bystanders cannot be stored or transmitted under any circumstance.
- **Blur first:** Faces must be detected and blurred BEFORE any downstream processing touches the frame.
- **The circle:** Face detection is itself processing of those same raw frames, so the rule eats itself.
- **Stated plainly:** You must process the video to learn what you are not allowed to process.
- **Practical escape:** The face-blur stage runs on-device, in memory, with nothing ever persisted.
- **Budget cost:** That escape spends part of the same 16ms latency and 2W power budgets.
- **Failure asymmetry:** A missed face is a privacy violation, not merely a metric that dipped.
- **Recall bar:** So the blur stage needs near-perfect recall even when the detector behind it doesn't.

### Visualization (canvas `c5`, 720×300)

Cycle diagram: three outlined boxes connected by arrows forming a circular dependency.

- **Title (bold 17px `#1a5276`, top center):** "The Circular Constraint".
- **Boxes (200×55, outlined width 2, two-line centered 17px text in box color):**
  - (50, 55) red `#e74c3c`: "Rule: no processing" / "of bystander faces"
  - (470, 55) blue `#2980b9`: "Fix: blur faces" / "before processing"
  - (260, 175) orange `#e67e22`: "But face detection" / "IS processing"
- **Arrows (gray `#555`, width 2, with arrowheads):** red box → blue box (horizontal at y=82); blue box → orange box (diagonal down-left); orange box → red box (diagonal up-left), completing the cycle.
- **Bottom annotations (centered):** bold 17px `#e74c3c` at y=258: "Must process the video to know what not to process."; 17px `#555` at y=282: "Escape: on-device, in-memory blur — spending latency and power budget to do it."

## Occlusion from Device Geometry

**The Device Blocks Its Own Camera — Misses in Predictable Places**

- **Fixed dead zones:** The glasses frame blocks ~10% of the visual field, and those zones never move.
- **Nose bridge:** A second permanent occlusion sits at center-bottom where the bridge meets the lens.
- **Systematic, not random:** Objects hidden by the device produce misses in the same regions every frame.
- **Metrics hide it:** Aggregate recall looks perfectly healthy because the misses average away.
- **Spatial error map:** Only a per-region error map reveals that misses cluster where hardware occludes.
- **Evaluate per-region:** Report recall by field-of-view region rather than as one pooled number.
- **Two mitigations:** Mask dead zones from confidence scores, or track objects temporally through them.

### Visualization (canvas `c6`, 720×300)

Field-of-view diagram with red occlusion zones and miss markers.

- **Title (bold 17px `#1a5276`, top center):** "Visual Field: Permanent Dead Zones from Device Geometry".
- **Field of view:** rect at (110, 40) size 500×180, fill `rgba(41,128,185,0.3)`, stroke `#2980b9` width 2; centered label "Camera field of view" (17px `#2980b9`).
- **Dead zones (fill `#e74c3c` at alpha 0.55):** 38px-wide strips along the left and right edges of the field; a triangle at center-bottom (base 110px wide, apex 70px up) for the nose bridge. Labels in 17px red: "frame" / "edge" (two lines) centered in each edge strip, "nose bridge" near the bottom center.
- **Miss markers:** three bold orange `#e67e22` "✕" marks placed inside the dead zones (left strip top, right strip lower, above nose triangle).
- **Bottom annotations (centered):** bold 17px `#e74c3c` at y=250: "Misses cluster in the SAME regions every frame — systematic, not random."; 17px `#555` at y=275: "Aggregate recall hides it; only a spatial error map reveals it. Track objects through dead zones."

## Regeneration instructions

- **Layout:** detail page in the domains-page style: h1 + `.subtitle`, then one `<h2>` per pitfall (1.4em `#1a5276`, bottom border `2px solid #2980b9`, padding-bottom 8px, margin 40px 0 15px), each followed by a full-width `.obj-table` with a single `<tr>`: left `<td>` (40%) holds `.obj-title` div + `<ul>` bullets, right `<td>` (60%, centered) holds the canvas. No thead, no nav, no badges, no cross-page links.
- **Page CSS:** body system sans-serif (-apple-system stack), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.8em `#1a5276`; `.subtitle` `#666` 1.05em; `ul` 0.9em `#333`, margin 8px 0 8px 20px; `strong` in `#1a5276`; `.obj-table td` border `1px solid #e0e0e0`, padding 20px 24px, vertical-align middle; even rows background `#fafcfe`; `.obj-title` 1.05em, weight 600, `#1a5276`; `.philosophy` callout style defined (background `#f0f4f8`, left border `4px solid #2980b9`) but unused on this page.
- **Palette:** primary blue `#1a5276`, accent blue `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, amber `#f39c12`, gray text `#666`/`#555`/`#333`, bar fill `rgba(41,128,185,0.3)`.
- **Canvas:** each canvas declares intrinsic `width`/`height` attributes (720×300); a shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart fonts are 17px -apple-system (bold for titles/emphasis). Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
