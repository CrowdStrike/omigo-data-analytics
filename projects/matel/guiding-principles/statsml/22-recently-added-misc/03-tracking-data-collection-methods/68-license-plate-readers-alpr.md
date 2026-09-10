# Tracking Data: License Plate Readers (ALPR)

**Page type:** detail page (two-column obj-table layout: text left 45%, canvas right 55%, one row per section)
**HTML title tag:** Tracking Data: License Plate Readers (ALPR)

**Subtitle:** Pole-mounted cameras on residential streets that turn each passing vehicle into a searchable row — on many systems, without keeping the image.

## Section 1: What is it?

**Lede:** A camera that stores a searchable row instead of a picture.

- **Hardware:** small solar-powered cameras on poles in neighborhoods, lots and side streets
- **Buyers:** residents' associations, police departments, businesses
- **Not video:** a snapshot per vehicle, plate and details extracted; on many systems the image is then discarded
- **What remains** is pure searchable data

**Key point — "No video retained" cuts storage, not queryability:** video has to be watched, but a row — plate, time, place, description — can be searched, joined and filtered at scale.

**Key point — Discarding the image removes the audit:** nothing is left to check a read against, which is the one thing that would catch a wrong plate string.

### Visualization (canvas `c1`, 720×320)

Street-scene schematic: a small pole camera on a residential street reading passing cars.

- **Houses:** four house shapes (70×60 body rectangle plus triangular roof) in light `#e5e9ef` at x = 80, 250, 450, 600, y=90.
- **Road:** horizontal `#dfe6e9` band (40px tall) at y=160.
- **Camera pole:** dark `#2c3e50` 4×60 pole at x=355, topped by a 24×14 blue `#2a78d6` camera body and a 4px-radius magenta `#d55181` IR flash dot above it.
- **Cars** (body rectangle 50×22 plus 30×10 cabin): green `#008300` at x=150, orange `#d95926` at x=480.
- **Scan arcs:** four concentric downward arcs (radii 40, 80, 120, 160) centered on the camera head, 1px stroke in magenta tinted to alpha 0.25.
- **Caption (14px `#2c3e50`, bottom center):** "Small camera on a residential street — reads every plate that passes".

## Section 2: What does it collect?

- **License plate** number
- **Timestamp** — date and time to the second
- **GPS location** of the reader
- **Vehicle type** — sedan, SUV, truck, motorcycle
- **Vehicle color**
- **Make and model**, from shape recognition
- **Direction** of travel
- **Distinguishing features** — bumper stickers, roof racks, damage

**Key point — Two query modes, one record:** a plate query returns one vehicle; a descriptor query — grey, SUV, a time window — returns a candidate set whose size depends on how common the description is, not on how well the system works.

**Key point — OCR error is systematic, not noise:** a plate whose glyphs a model confuses is misread the same way at every reader, so the wrong string builds its own consistent sighting history while the real one goes missing. More sightings do not average out a bias that repeats. Matched against an alert list in real time, a misread can stop a driver within minutes — and the image that would settle it is already gone.

### Visualization (canvas `c2`, 720×320)

Extraction diagram: a vehicle silhouette with its plate on the left, an arrow, and the extracted attribute record on the right.

- **Vehicle:** blue `#2a78d6` filled sedan silhouette (polygon through (60,140), (80,100), (160,90), (200,100), (220,140)) with two dark `#2c3e50` 12px-radius wheels at (90,145) and (190,145); a white 40×14 plate patch at (120,128) carrying bold 13px monospace blue text "7XNK 291".
- **Arrow:** 2px magenta `#d55181` horizontal arrow from x=240 to x=310 at y=120, with an open arrowhead.
- **Extracted attributes** (seven rows at x=330, 28px pitch; bold 13px label in its own hue followed by 13px monospace value in `#2c3e50` at x=420):
  - "Plate:" (blue `#2a78d6`) — "7XNK 291"
  - "Type:" (green `#008300`) — "Sedan"
  - "Color:" (blue `#2a78d6`) — "Dark Blue"
  - "Make:" (orange `#d95926`) — "Honda Accord"
  - "Direction:" (text `#2c3e50`) — "Eastbound"
  - "Time:" (magenta `#d55181`) — "2024-03-15 08:42:17"
  - "Location:" (muted `#6b7280`) — "33.77°N, 84.39°W"

**Payload note (below canvas, italic, left-aligned):** Sample payload — illustrative structure, not real captured data.

**Payload block (monospace, left border `#1a5276`):**

```
// Vendor sighting schemas are not published.
// Whole block is reconstruction; field names generic.
// ── inferred / plausible ──
{
  "reader_id":   "RDR-0142",
  "reader_pos":  { "lat": 30.3812, "lon": -97.7219 },
  "ts":          "2026-08-22T21:47:12Z",

  // read from the image, then image discarded
  "plate_text":  "8XKJ402",
  "plate_conf":  0.88,
  "plate_state": null,        // region band unreadable at night

  // vehicle description vector — searchable without a plate
  "descriptor": {
    "body":   "suv",   "body_conf":  0.79,
    "color":  "grey",  "color_conf": 0.55,
    "make":   "toyota","make_conf":  0.61
  },
  "image_retained": false
}
```

## Section 3: Why is it collected?

**Label (purpose pill):** Stated purpose

- **One after-the-fact question** — which vehicles were on this street around the time of an incident
- For that, a **searchable row** beats a video file, which is why the image goes

**Label (effect pill):** Additional consequence

- Every pass is recorded, so a plate can be looked up **with no prior suspicion**
- **Searched as a network** — a query from one subscriber can span cameras other agencies and associations own, so the effective coverage is the network, not the pole the buyer paid for
- **Bought as a service**, the data sits under commercial terms — reuse is bounded by a contract, not a warrant

**Key point — The base rate breaks the obvious query:** ask which vehicles were near an address repeatedly and the answer is delivery vans, commuters cutting through, and neighbours — the reader watches a road, not a destination. A few real findings arrive inside a much larger pile of coincidences. Readers also sit wherever a subscriber bought coverage, so a gap in the history looks like a route not taken.

### Visualization (canvas `c3`, 720×320)

Horizontal bar chart: composition of a repeat-visit query's result set, sorted descending — the target category is the shortest bar.

- **Title (bold 14px `#1a5276`, top center):** "What a repeat-visit query returns".
- **Subtitle (12px muted, centered):** "vehicles seen near one address three or more times in a fortnight".
- **Bars** (30px tall, 12px gap, top y=66; plot from x=232 to width−96, lengths scaled to the max count 61; each bar hue-tinted fill at alpha 0.34 with 1.5px hue stroke; bold 12px hue-colored label right-aligned left of the bar; bold 13px count right of the bar end):
  - 61 — "commuters cutting through" — blue `#2a78d6`
  - 38 — "residents of nearby streets" — aqua `#199e70`
  - 24 — "delivery and service vans" — violet `#4a3aa7`
  - 9 — "buses, taxis, refuse rounds" — yellow `#c98500`
  - 2 — "actually of interest" — orange `#d95926`
- **Shortlist bracket:** dashed (4/3) orange 1.5px horizontal line above the bars, with right-aligned 12px `#2c3e50` label "134 plates on the shortlist" (134 = sum of counts); plus a solid orange 2px vertical tick bracketing the last ("actually of interest") row.
- **Caption 1 (italic 12px `#2c3e50`, centered):** "The routine is the answer. Whoever reads the list has to rule out the other 132."
- **Caption 2 (italic 11px muted, centered):** "Illustrative counts — the proportions show the shape, not a measured street."

## Regeneration instructions

- **Layout:** tracking-page `.obj-table`: full-width `border-collapse: collapse` table, one `<tr>` per section; left `<td>` (45%) holds `.obj-title` + optional `.lede` + bullets + `.key-point` callouts + `.lbl` pills, right `<td>` (55%, `text-align: center`) holds the canvas — and in the "What does it collect?" row, the `.payload-note` and `.payload` `<pre>` below the canvas (both left-aligned).
- **Page style:** body system sans-serif (-apple-system stack), white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; table cell borders `1px solid #2980b9`, padding 16px; `.obj-title` bold 1.1em `#1a5276`; `li` 0.93em with `li b` in `#1a5276` weight 600. No nav bar, no back/home links.
- **Callouts:** `.key-point` — background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px 14px, 0.93em; leading `<strong>` in `#1a5276`.
- **Pills:** `.lbl` inline-block uppercase 0.7em bold, padding 2px 7px, radius 3px; `.lbl-purpose` background `#eaf2fb` color `#1a5276`; `.lbl-effect` background `#fdf0e6` color `#a8501c`.
- **Payload:** `.payload` — background `#f8f9fa`, left border `3px solid #1a5276`, ui-monospace 0.78em, `white-space: pre`; `.payload-note` italic 0.82em `#666` immediately above.
- **Canvas:** each declares intrinsic `width="720" height="320"`; a shared `setupCanvas(id)` reads those attributes, sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Shared helpers: `tint(hex, alpha)` for translucent fills and `rr()` rounded-rect path.
- **Palette (tracking pages):** categorical tokens blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`; ink `#1a5276` (headings/axes only), text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Red is reserved for genuine alarm states and not used here. (Site-wide palette reference: #1a5276 primary blue, #27ae60 green, #e74c3c red, #e67e22 orange.)
- In regenerated HTML, any card links use `.html` extensions.
