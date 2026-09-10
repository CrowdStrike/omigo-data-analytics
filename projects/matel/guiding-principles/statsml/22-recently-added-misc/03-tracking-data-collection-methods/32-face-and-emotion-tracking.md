# Tracking Data: Face & Emotion Tracking

**Page type:** detail page (two-column obj-table layout: text left 45%, canvas/payload right 55%, one row per section)
**HTML title tag:** Tracking Data: Face & Emotion Tracking

**Subtitle:** A camera measures face geometry. Age, gender and emotion are classifier outputs computed from it — predictions with confidence scores, not properties read off the face.

## Section 1: What is it?

Lede: A camera turns a face into numbers, then models turn the numbers into labels.

- **Embedding:** a vector encoding geometry — relative positions of eyes, nose and jaw, plus surrounding texture
- **Matching:** comparing two embeddings gives a similarity score — how a phone recognises its owner, or a store system links a face to one seen last week
- **Separate models** output an age bracket, a gender category, an expression category
- **Each carries a confidence score**, because each is a prediction over a fixed list of trained options

Key point callout: **Two different problems:** matching one face against one enrolled template under controlled conditions is a well-posed measurement problem. Assigning an emotion word to a face is not a measurement at all — it is a classification into categories chosen by whoever built the training set.

### Visualization (canvas `c1`, 720×320)

Schematic: a face outline with dashed measurement lines, next to a panel splitting "measured geometry" from "model output".

- **Face:** blue (`#2a78d6`) 2.5px ellipse outline at (220,120), radii 65×85; eye ellipses (14×8) at (198,105) and (242,105) with 4px blue pupils; triangular nose lines; neutral quadratic-curve mouth.
- **Measurement lines (dashed 4/3, 1.5px, with small end ticks):** magenta (`#d55181`) eye-distance line above the eyes labeled "44px"; magenta jaw-width line below the face labeled "110px"; orange (`#d95926`) vertical nose-to-chin line at the right of the face labeled "67px" (13px labels in the line color).
- **Attributes panel (right):** 300×180 rounded rect (radius 8) at (380,30), fill `rgba(42,120,214,0.35)`, blue 1.5px border.
  - Heading bold 16px `#2a78d6`: "MEASURED GEOMETRY"; subheading 13px `#6b7280`: "read from the image".
  - Rows (bold blue label at x=400, plain `#2c3e50` value at x=500, 15px): "Eye spacing:" 44 px; "Jaw width:" 110 px; "Nose–chin:" 67 px.
  - Dashed (3/3) blue divider line at y=160.
  - Heading bold 16px `#d95926`: "MODEL OUTPUT"; subheading 13px `#6b7280`: "predicted categories, not readings".
  - Rows (bold orange label, plain value): "Age bracket:" 28–34; "Expression:" "neutral".

## Section 2: What does it collect?

- **Face geometry**, stored as an embedding vector
- **Bounding box** and a per-session track identifier
- **Age bracket** — model output, with a confidence score
- **Gender category** — model output, with a confidence score
- **Expression label** — model output, with a confidence score
- **Match flag** — whether a prior embedding fell within a threshold
- **Head direction** relative to the display, and dwell time

Key point callout: **`emotion` is a model output:** the camera observes an arrangement of facial muscles; the label maps that arrangement onto a category from a fixed list. Expression and internal state are different variables, and the record holds no evidence about the second. Any downstream count of "happy shoppers" is a count of classifier decisions.

### Visualization (canvas `c2`, 720×320)

Inference-gap flow diagram: one measured expression → classifier → one label, fanning out to five internal states the label cannot separate.

- **Title (bold 16px `#2a78d6`, centered at y=20):** "What is measured, what is returned, and what is not in the frame".
- **Left (the measurement):** smiling-face circle at (92,108), radius 40, fill `rgba(42,120,214,0.12)`, blue 2px stroke; dot eyes and an upturned-arc mouth in `#2c3e50` — the muscle configuration, nothing more. Below: bold 13px blue "MEASURED", then 12px `#6b7280` "muscle configuration" / "(pixels, then geometry)".
- **Middle (the classifier):** 104×68 rounded rect (radius 6) centered at x=262, fill `rgba(217,89,38,0.18)`, orange 1.5px border; inside: bold 14px "classifier", 12px mute "fixed list of" / "categories". Below: bold 13px orange "RETURNED", then 12px mute "one label + a score" / 'emotion: "happy", 0.62'. A gray (`#e5e9ef`) arrow connects face → classifier.
- **Right (states the label cannot separate):** header bold 13px `#6b7280` centered: "NOT DISTINGUISHED BY THE LABEL". Five dashed-border (3/2) white rounded boxes (196×20, radius 4) at x=470, y=58 stepping 24, text 13px `#2c3e50`: "genuine amusement", "social politeness", "embarrassment", "a habitual expression", "posing for a camera". Faint dashed (2/3) `#e5e9ef` fan lines from the classifier box to each.
- **Bottom note (14px `#2a78d6`, centered, two lines):** "The same configuration is consistent with all of these. The label picks one;" / "the frame contains no evidence for the choice."

Below the canvas, payload note (italic gray): "Sample payload — illustrative structure, not real captured data."

Payload block:

```
// Reconstruction. Field names are generic — no single
// published schema covers these systems.
{
  // ── inferred / plausible ──
  "t":            "2026-08-22T13:07:55Z",
  "camera_id":    "entr-02",
  "track_id":     "tr_9f4c…",         // per-session, resets on loss of track
  "bbox":         { "x": 610, "y": 288, "w": 122, "h": 122 },
  "face_embedding": [0.041, -0.118, …],  // vector, not an image

  // model outputs — each is a prediction with a score
  "age_bracket":  "25-34",  "age_conf":     0.51,
  "gender_pred":  "f",      "gender_conf":  0.78,
  "emotion":      "happy",  "emotion_conf": 0.62,
  "gaze_at_display": true,
  "dwell_ms":     4300,
  "reid_match":   null       // no prior embedding within threshold
}
```

## Section 3: Why is it collected?

Label pill (Stated purpose):

- **Unlocking a phone** — one enrolled face, close range, cooperative pose, and no password to type
- **Counting** — faces past a camera give a footfall figure broken down by direction and dwell

Label pill (Additional consequence):

- The same pipeline **splits footfall by predicted age and gender**, and **recognises a return visit** with no account or card
- Those labels are a **by-product of running the detector** — the model emits them whether or not anyone asked

Key point callout: **The split describes who the camera saw:** enrolment is deliberate — the subject faces the sensor. A doorway camera gets whatever angle and light a path gives it, and detection usually works best on people who looked toward the lens. So an age or gender breakdown describes the subset the mounting favoured.

### Visualization (canvas `c3`, 720×320)

Grouped bar chart: detection rate falls off with head angle, so the labelled set over-represents people who looked toward the lens. 400 illustrative passers-by at one doorway camera bucketed by head angle.

- **Title (bold 13px `#1a5276`, centered at y=22):** "Who gets detected, by which way the head was turned"; subtitle (12px `#6b7280`, y=40): "400 people past one doorway camera".
- **Data (bucket / walked past / detected and labelled):** "at the lens" 60 / 57; "15° off" 90 / 79; "30° off" 110 / 77; "45° off" 80 / 36; "turned away" 60 / 12.
- **Layout:** baseline at y=236 (thin `#e5e9ef` line), max bar height 140 for value 110; group centers at x=124 stepping 118; outer "walked past" bar 64px wide, inner "detected" bar 38px wide overlaid on the same center.
- **Colors:** walked past — fill `rgba(42,120,214,0.18)`, stroke `#2a78d6`; detected and labelled — fill `rgba(0,131,0,0.45)`, stroke `#008300`. Legend swatches with those labels at the top.
- **Annotations:** bold 12px green percentage above each group (detected/past: 95%, 88%, 70%, 45%, 20%); bucket labels 12px `#2c3e50` under the baseline; axis note 12px `#6b7280`: "head angle away from the lens".
- **Captions (bottom center):** italic 12px `#2c3e50`: "Within 15° of the lens: 38% of the people who walked past, 52% of the labelled records."; italic 11px `#6b7280`: "Illustrative — the rates show the shape, not a tested camera."

## Regeneration instructions

- **Layout:** tracking-page `.obj-table`: full-width `border-collapse: collapse` table, one `<tr>` per section; left `<td>` (45%) holds `.obj-title` + `.lede` + bullets + `.key-point` callouts, right `<td>` (55%, text-align center) holds the canvas, and in the "What does it collect?" row also the `.payload-note` and `.payload` `<pre>` (both left-aligned).
- **Page style:** body system sans-serif, white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; table cell borders `1px solid #2980b9`, padding 16px; `.obj-title` bold 1.1em `#1a5276`; li 0.93em with `li b` in `#1a5276` weight 600.
- **Callouts:** `.key-point` — background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px 14px, 0.93em, leading `<strong>` in `#1a5276`.
- **Label pills:** `.lbl` inline-block uppercase 0.7em bold, padding 2px 7px, radius 3px; `.lbl-purpose` background `#eaf2fb` color `#1a5276`; `.lbl-effect` background `#fdf0e6` color `#a8501c`.
- **Payload:** `.payload` background `#f8f9fa`, left border `3px solid #1a5276`, ui-monospace 0.78em, `white-space: pre`; `.payload-note` 0.82em italic `#666` directly above.
- **Canvas:** 720×320 intrinsic attributes; a shared `setupCanvas(id)` reads the element's own width/height attributes and scales by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates). Charts use hardcoded literal data arrays (no Math.random). This page's `rr()` rounded-rect helper accepts either a single radius or a `[tl, tr, br, bl]` array (matching the roundRect signature it replaces).
- **Palette (tracking-set tokens, declared once as `P`):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Red is reserved for alarm states; navy `#1a5276` is ink only (headings, axes, callout borders). Project-wide palette anchors: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- No nav bar, no back/home links. In regenerated HTML, any card links elsewhere use `.html` extensions.
