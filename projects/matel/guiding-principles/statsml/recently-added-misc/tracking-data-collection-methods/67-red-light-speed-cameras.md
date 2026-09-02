# Tracking Data: Red Light & Speed Cameras

**Page type:** detail page (two-column obj-table layout: text left 45%, canvas right 55%, one row per section)
**HTML title tag:** Tracking Data: Red Light & Speed Cameras

**Subtitle:** They record every vehicle that passes, because the violation test is applied after the measurement — not before it.

## Section 1: What is it?

**Lede:** Measure first, apply the threshold second.

- **Siting:** mounted at intersections and along roads
- **Plate:** read with OCR from the image
- **Speed:** measured with radar or a pair of inductive loops
- **Trigger:** every vehicle, not only violators — the threshold test is applied after the measurement
- **Consequence:** a record exists whether or not a ticket follows

**Key point — The ticket is one row selected from the table:** the table is every pass, and it is the larger object — which is why retention policy on non-violating rows matters more analytically than the ticket rule.

### Visualization (canvas `c1`, 720×320)

Roadside schematic: a pole-mounted camera scanning four passing cars.

- **Road:** horizontal `#e5e9ef` band (60px tall) at vertical center, with a dashed (15/10, 2px) `#e5e9ef` center line.
- **Cars** (each: a 60×25 body rectangle plus a 40×12 cabin rectangle, with a small white plate patch labeled "PLT" in 11px `#333`): x=100 blue `#2a78d6`; x=250 green `#008300`; x=400 orange `#d95926`; x=550 magenta `#d55181`.
- **Camera:** dark pole (`#2c3e50`, 6×50) at x=340, topped by a 12px-radius blue `#2a78d6` circle with a 4px magenta `#d55181` center dot.
- **Scan lines:** thin 1px lines in magenta tinted to alpha 0.3 from the camera head to each car — every car, not only violators.
- **Caption (15px `#2c3e50`, bottom center):** "Every pass is measured; the threshold test comes after".

## Section 2: What does it collect?

- **Plate number**, from OCR on the image
- **Timestamp** — the exact second of the pass
- **Speed** at that moment
- **Lane position** and direction of travel
- **Photo** of the vehicle, front and/or rear
- **Colour, make, model**, from the image
- **Camera location** — GPS coordinates

**Key point — Nothing here measures speed directly:** two timestamps and a loop spacing are measured; the speed is arithmetic on top, inheriting the timing error of both detectors and the survey error of the distance. Hence the tolerance, and the issue threshold sitting above the posted limit.

**Key point — The plate has the same structure:** a string plus a confidence — an OCR read, not a reading off the vehicle.

**Key point — Dropping the uncertainty columns turns estimates into facts:** keeping `speed_mph` and `plate_text` while dropping `speed_tol_mph` and `plate_conf` leaves nothing to flag a low-confidence read with.

### Visualization (canvas `c2`, 720×320)

Extraction diagram: a plate image on the left, an arrow, and a stack of extracted data-field boxes on the right.

- **Plate image:** 140×60 box at (40, 30), light `#f4f4f4` fill with 2px blue `#2a78d6` border, containing bold 22px monospace blue text "ABC 1234".
- **Arrow:** 3px orange `#d95926` horizontal arrow from x=200 to x=280 at y=60, with an open arrowhead.
- **Field boxes** (six 200×22 boxes stacked at x=300, 28px pitch: `#f8f9fa` fill, 1px blue border, 14px monospace `#2c3e50` text):
  - "Plate: ABC 1234"
  - "Speed: 47 mph"
  - "Time: 14:32:07"
  - "Lane: 2 / East"
  - "Color: Blue Sedan"
  - "Loc: 34.05, -118.24"
- **Caption (14px muted `#6b7280`, bottom center):** "OCR extracts structured data from every frame".

**Payload note (below canvas, italic, left-aligned):** Sample payload — illustrative structure, not real captured data.

**Payload block (monospace, left border `#1a5276`):**

```
// No public API for an enforcement vendor's record.
// Reconstruction; field names kept generic.
// ── inferred / plausible ──
{
  "camera_id":   "RLC-2291",

  // measured at the roadside
  "loop_a_ts":   "2026-08-22T14:23:07.412Z",
  "loop_b_ts":   "2026-08-22T14:23:07.618Z",
  "loop_gap_m":  3.0,
  "signal_phase":"red",

  // derived from the rows above
  "speed_mph":   32.6,
  "speed_tol_mph": 1.5,
  "posted_mph":  25,
  "threshold_mph": 30,        // issue only above this

  // derived from the image
  "plate_text":  "7ABC123",
  "plate_conf":  0.91
}
```

## Section 3: Why is it collected?

**Label (purpose pill):** Stated purpose

- **Enforcement** of the signal phase and the posted limit
- **A record for every vehicle** — nothing can know which pass is a violation until it is measured

**Label (effect pill):** Additional consequence

- **Non-violating passes are the majority**, and carry a plate, a place and a time just as violating ones do — so the table is a sighting log
- Aggregated **speed distributions** are a saleable product, and retention policy decides for how long

**Key point — A plate is a vehicle, not a person:** per-plate statistics describe a vehicle's history, and a shared or fleet vehicle mixes several drivers into one set of rows. Cameras also sit where past violations and budget put them, so a citywide traffic model fitted on that footprint measures placement as much as traffic.

### Visualization (canvas `c3`, 720×320)

Histogram of measured speeds at one site, with the posted limit and issue threshold marked and the over-threshold tail highlighted.

- **Title (bold 14px `#1a5276`, top center):** "Measured speed of every pass at one site".
- **Subtitle (12px muted, centered):** "the threshold picks out the shaded tail; the rest of the passes are recorded too".
- **Bins** (2-mph bins, lower edge → count): 16→120, 18→340, 20→820, 22→1450, 24→1900, 26→1520, 28→840, 30→310, 32→120, 34→44, 36→16, 38→6. Total 7,488 passes; 496 at or over the threshold.
- **Bar styling:** bins below 30 mph in blue `#2a78d6` (fill tint alpha 0.30, 1px blue stroke); bins at 30 mph and above in orange `#d95926` (fill tint alpha 0.55, orange stroke).
- **Axes:** baseline at y=226, plot padding 56 left / 34 right, bar heights scaled to 148px max; x tick labels (11px `#2c3e50`) at 16, 20, 24, 28, 32, 36, 40; x-axis title "speed, mph" (12px muted, centered); rotated y-axis label "passes".
- **Reference lines** (vertical, 1.5px, from baseline up to y=58):
  - Posted limit at 25 mph: dashed (5/4) aqua `#199e70`, right-aligned bold 12px aqua label "posted 25".
  - Issue threshold at 30 mph: solid orange, left-aligned bold 12px orange label "issue threshold 30".
- **Percentage annotations (12px, at y=88):** to the right of the threshold in orange: "6.6% of passes"; to the left in blue: "93.4% of passes" (computed from the bins: over/total rounded to one decimal).
- **Caption 1 (italic 12px `#2c3e50`, centered):** "A table filtered to citations keeps the tail. The distribution it came from is the wider object."
- **Caption 2 (italic 11px muted, centered):** "Illustrative counts for one site — the shape, not a measured site."

## Regeneration instructions

- **Layout:** tracking-page `.obj-table`: full-width `border-collapse: collapse` table, one `<tr>` per section; left `<td>` (45%) holds `.obj-title` + optional `.lede` + bullets + `.key-point` callouts + `.lbl` pills, right `<td>` (55%, `text-align: center`) holds the canvas — and in the "What does it collect?" row, the `.payload-note` and `.payload` `<pre>` below the canvas (both left-aligned).
- **Page style:** body system sans-serif (-apple-system stack), white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; table cell borders `1px solid #2980b9`, padding 16px; `.obj-title` bold 1.1em `#1a5276`; `li` 0.93em with `li b` in `#1a5276` weight 600. No nav bar, no back/home links.
- **Callouts:** `.key-point` — background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px 14px, 0.93em; leading `<strong>` in `#1a5276`.
- **Pills:** `.lbl` inline-block uppercase 0.7em bold, padding 2px 7px, radius 3px; `.lbl-purpose` background `#eaf2fb` color `#1a5276`; `.lbl-effect` background `#fdf0e6` color `#a8501c`.
- **Payload:** `.payload` — background `#f8f9fa`, left border `3px solid #1a5276`, ui-monospace 0.78em, `white-space: pre`; `.payload-note` italic 0.82em `#666` immediately above.
- **Canvas:** each declares intrinsic `width="720" height="320"`; a shared `setupCanvas(id)` reads those attributes, sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Shared helpers: `tint(hex, alpha)` for translucent fills and `rr()` rounded-rect path.
- **Palette (tracking pages):** categorical tokens blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`; ink `#1a5276` (headings/axes only), text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Red is reserved for genuine alarm states and not used here. (Site-wide palette reference: #1a5276 primary blue, #27ae60 green, #e74c3c red, #e67e22 orange.)
- In regenerated HTML, any card links use `.html` extensions.
