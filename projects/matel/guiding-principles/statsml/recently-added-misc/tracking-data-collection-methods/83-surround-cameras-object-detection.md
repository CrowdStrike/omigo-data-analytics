# Tracking Data: Surround Cameras & Object Detection

**Page type:** detail page (two-column obj-table layout: text left 45%, canvas right 55%, one row per section)
**HTML title tag:** Tracking Data: Surround Cameras & Object Detection

**Subtitle:** Cameras around the car's body feed every frame to a model that draws labeled boxes around what it thinks it sees — and the driving software steers around the boxes, not the picture.

## Section 1: What is it?

**Lede:** Cameras around the car that turn every frame into a list of labeled boxes.

- **Hardware:** cameras behind the windshield, on the sides, and at the rear, each sending many frames a second to an onboard computer
- **Boxes, not pixels:** the model draws a box around each thing it thinks it sees — car, person, cyclist, sign — and the driving software works with the boxes, merged from all cameras into one picture of the surroundings (a fused world model)

**Key point — Sure about the familiar only:** the model scores high on things it has seen many times. A person in a costume, an overturned truck, a couch on a highway score low precisely because they are rare (the long tail) — the model doubts them most when they matter most.

### Visualization (canvas `c1`, 720×320)

A schematic front-camera frame: simple street scene with detection boxes and their sureness scores drawn over it — one box straddling the cutoff.

- **Title (bold 14px `#1a5276`, top center, y=22):** "One frame, as the driving software receives it".
- **Corner tag (11px mute `#6b7280`, left-aligned at (60, 44)):** "front camera — schematic".
- **Horizon:** 1px `#e5e9ef` line at y=120 from x=60 to x=660.
- **Road:** filled trapezoid, vertices (310,120), (415,120), (600,268), (130,268), fill `#e5e9ef` at alpha 0.5; road edges 2px `#2c3e50` lines (310,120)→(130,268) and (415,120)→(600,268); dashed (6/6) 1.5px mute center line (362,120)→(365,268).
- **Car ahead (rear view, on road):** body rr(378,168,84,46,8) and roof rr(390,150,60,22,6), both fill `#e5e9ef`, 1.5px `#2c3e50` stroke; two wheel rects 14×8 `#2c3e50` at (388,212) and (438,212). Detection box: 2px solid blue `#2a78d6` rect (368,142,104,84); label above box, bold 11px blue, centered: "car 0.97".
- **Person on left sidewalk:** head circle r=7 at (185,168) and torso rr(177,176,16,32,6), both fill `#2c3e50`; two 2px leg lines from (181,208) to (177,228) and (189,208) to (193,228). Detection box: 2px solid green `#008300` rect (166,154,38,80); label above, bold 11px green: "person 0.91".
- **Sign on right shoulder:** 2px `#2c3e50` pole line (585,150)→(585,230); sign face circle r=13 at (585,136), fill `#c98500` at alpha 0.5, 1.5px `#c98500` stroke. Detection box: 2px solid violet `#4a3aa7` rect (568,119,34,34); label above, bold 11px violet: "sign 0.86".
- **Ambiguous lump near right road edge:** ellipse center (470,224), rx=16, ry=10, fill `#6b7280` at alpha 0.5. Detection box: 2px dashed (5/4) orange `#d95926` rect (446,204,48,34); label above, bold 11px orange: "person? 0.44"; below the box, 10px mute: "below cutoff — dropped".
- **Caption (italic 12px `#2c3e50`, bottom center, y=302):** "The software keeps only boxes above its cutoff; the 0.44 box never becomes an object. Illustrative frame."

## Section 2: What does it collect?

- **Per frame** — a list of boxes, each with a type label, a position in the frame, and a score for how sure the model is (a confidence score)
- **Across frames** — boxes linked over time into moving objects, each with a guessed speed and direction (tracks)
- **The footage itself** — the raw video behind the boxes; on some fleets, clips the system flags can be kept or uploaded

**Key point — Every box is an opinion, not a fact:** a cutoff on the sureness score decides which opinions become "objects". Set it low and a shadow becomes a phantom obstacle; set it high and a faint pedestrian disappears. There is no cutoff that removes both mistakes.

### Visualization (canvas `c2`, 720×320)

Bar chart: eight detections' sureness scores against two possible cutoffs — the loose one invents obstacles, the strict one erases a person.

- **Title (bold 14px `#1a5276`, top center, y=22):** "Eight detections, two possible cutoffs".
- **Legend (11px, left-aligned starting x=70, y=44):** swatch in blue tint (alpha 0.45) + mute label "really there"; swatch in yellow tint (alpha 0.45) + mute label "nothing there".
- **Plot:** baseline 1px `#e5e9ef` at y=252 from x=70 to x=680; eight equal slots, bar width 40 centered per slot; bar height = score × 192 px (score 1.0 → top y=60).
- **Bars (label, score, kind):** car 0.97 real; truck 0.93 real; person 0.91 real; sign 0.86 real; cyclist 0.74 real; shadow 0.52 nothing; faint person 0.46 real; plastic bag 0.41 nothing. Real bars: fill blue `#2a78d6` alpha 0.45, 1.5px blue stroke. Nothing bars: fill yellow `#c98500` alpha 0.45, 1.5px yellow stroke.
- **Value labels:** 10px mute score above each bar (e.g. "0.97"). X labels: 10px `#2c3e50` under baseline (y=266): "car", "truck", "person", "sign", "cyclist", "shadow", "faint person", "plastic bag".
- **Strict cutoff:** dashed (4/3) 1.5px violet `#4a3aa7` line at score 0.7 (y=117.6), right-aligned 11px violet label above it at x=680: "cutoff 0.7 — misses the faint person".
- **Loose cutoff:** dashed (4/3) 1.5px orange `#d95926` line at score 0.4 (y=175.2... note: y = 252 − 0.4×192 = 175.2), right-aligned 11px orange label above it at x=680: "cutoff 0.4 — keeps shadow and bag".
- **Annotations (bold 10px, 17px above the bar's value label):** violet "missed at 0.7" over the faint-person bar; orange "phantom at 0.4" over the shadow and plastic-bag bars.
- **Caption (italic 12px `#2c3e50`, bottom center, y=304):** "The shadow (0.52) outscores the faint person (0.46) — no cutoff drops one and keeps the other. Illustrative scores."

**Payload note (below canvas, italic, left-aligned):** Sample payload — illustrative structure, not real captured data.

**Payload block (monospace, left border `#1a5276`):**

```
// Perception-stack schemas are not published.
// Whole block is reconstruction; field names generic.
// ── inferred / plausible ──
{
  "frame_ts": "2026-08-22T17:03:41.120Z",
  "camera":   "front_main",

  // model output — opinions, not measurements
  "detections": [
    { "class": "car",        "score": 0.97,
      "box": [412, 188, 96, 54], "est_distance_m": 23.5 },
    { "class": "pedestrian", "score": 0.44,
      "box": [655, 230, 18, 40], "est_distance_m": 61.0 }
  ],

  // derived by linking boxes across frames
  "tracked_objects": [
    { "track_id": 3182, "class": "car",
      "speed_mps": 12.4, "frames_seen": 214 }
  ],

  "clip_upload_flag": false   // some fleets upload flagged clips
}
```

## Section 3: Why is it collected?

**Label (purpose pill):** Stated purpose

- **Knowing what is around the car** — the boxes and their tracks are the picture the driving software plans, brakes, and steers against
- **Improving the model** — clips the system found confusing can be flagged and sent back for training on some fleets

**Label (effect pill):** Additional consequence

- **Continuous filming of public space** — pedestrians, license plates, and storefronts pass through the cameras all day; flagged clips can leave the car for review on some fleets

**Key point — The distances are guesses:** a single camera does not measure how far away anything is — it guesses from how big the thing looks in the frame, and the guess gets worse with range. A camera-only system steers on those guesses.

### Visualization (canvas `c3`, 720×320)

Error cone: how far off a camera's distance guess can be at 10/25/50/100 m, against the near-flat band of a sensor that measures distance directly.

- **Title (bold 14px `#1a5276`, top center, y=22):** "How far away is it? One camera's guess vs a direct measurement".
- **Axis note (10px mute, left-aligned at (14, 64)):** "metres off, either way".
- **Plot:** x from 80 to 660 spans 0–110 m of true distance; midline (perfect guess, 0 m off) at y=160; vertical scale 4 px per metre, so ±20 m spans y=80 to y=240.
- **Gridlines:** 1px `#e5e9ef` horizontal lines at +20/+10/0/−10/−20 m (y = 80/120/160/200/240) with right-aligned 10px mute labels at x=72: "20", "10", "0", "10", "20".
- **X ticks:** 10px mute labels at y=256 under the four ranges: "10 m", "25 m", "50 m", "100 m".
- **Camera cone (blue `#2a78d6`):** error at (range → ± metres): 0→0, 10→1, 25→3, 50→8, 100→20. Filled shape through upper points then back through lower points, fill blue alpha 0.2; both edges stroked 1.5px blue. Small blue 10px labels just above the upper edge at each range: "±1", "±3", "±8", "±20 m".
- **Cone label (bold 11px blue, above the cone near x=560, y=88):** "guess from apparent size".
- **Direct-measurement band (green `#008300`):** 4px-tall band (±0.5 m) centered on the midline from x=80 to x=660, fill green alpha 0.5; bold 11px green label below the midline near the right end (right-aligned at x=660, y=178): "direct measurement (timing a reflected pulse) — usually near-flat".
- **Caption (italic 11px mute, bottom center, y=306):** "Illustrative — guessing distance from how big something looks degrades with range; timing an echo usually does not."

## Regeneration instructions

- **Layout:** tracking-page `.obj-table`: full-width `border-collapse: collapse` table, one `<tr>` per section; left `<td>` (45%) holds `.obj-title` + optional `.lede` + bullets + `.key-point` callouts + `.lbl` pills, right `<td>` (55%, `text-align: center`) holds the canvas — and in the "What does it collect?" row, the `.payload-note` and `.payload` `<pre>` below the canvas (both left-aligned).
- **Page style:** body system sans-serif (-apple-system stack), white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; table cell borders `1px solid #2980b9`, padding 16px; `.obj-title` bold 1.1em `#1a5276`; `li` 0.93em with `li b` in `#1a5276` weight 600. No nav bar, no back/home links.
- **Callouts:** `.key-point` — background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px 14px, 0.93em; leading `<strong>` in `#1a5276`.
- **Pills:** `.lbl` inline-block uppercase 0.7em bold, padding 2px 7px, radius 3px; `.lbl-purpose` background `#eaf2fb` color `#1a5276`; `.lbl-effect` background `#fdf0e6` color `#a8501c`.
- **Payload:** `.payload` — background `#f8f9fa`, left border `3px solid #1a5276`, ui-monospace 0.78em, `white-space: pre`; `.payload-note` italic 0.82em `#666` immediately above.
- **Canvas:** each declares intrinsic `width="720" height="320"`; a shared `setupCanvas(id)` reads those attributes, sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Shared helpers: `tint(hex, alpha)` for translucent fills and `rr()` rounded-rect path.
- **Palette (tracking pages):** categorical tokens blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`; ink `#1a5276` (headings/axes only), text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Red is reserved for genuine alarm states and not used here. (Site-wide palette reference: #1a5276 primary blue, #27ae60 green, #e74c3c red, #e67e22 orange.)
- In regenerated HTML, any card links use `.html` extensions.
