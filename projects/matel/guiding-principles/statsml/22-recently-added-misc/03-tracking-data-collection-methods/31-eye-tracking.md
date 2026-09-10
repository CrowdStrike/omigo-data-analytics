# Tracking Data: Eye Tracking

**Page type:** detail page (two-column obj-table layout: text left 45%, canvas/payload right 55%, one row per section)
**HTML title tag:** Tracking Data: Eye Tracking

**Subtitle:** A camera estimates where the eyes are pointed. Gaze position is measured; interest, intent, and attention are inferred from it.

## Section 1: What is it?

Lede: A camera estimates a direction, then intersects it with a screen.

- **Mechanism:** infrared light reflects off the eye; the reflection's position relative to the pupil centre gives a gaze vector
- **Where it appears:** XR headsets, some monitors and laptops, research and retail layout studies
- **Headsets lean on it hardest:** on Apple Vision Pro gaze plus a finger pinch is the primary pointer, and eye-tracked Meta headsets use it for input and foveated rendering — so the tracker runs for the whole session. Camera-only smart glasses have no inward gaze sensor
- **Calibration required** — the person looks at known targets first
- **Accuracy is an angle**, and that angle limits everything downstream

Key point callout: **Gaze is measured, interest is not:** the device reports where the eyes pointed. Whether the person was reading, thinking of something else, or looking past the screen is an inference the sensor has no access to. Foveal direction and attention are correlated but distinct — a person can fixate a region while attending elsewhere, and attend to something peripheral without ever pointing their eyes at it.

### Visualization (canvas `c1`, 720×320)

Schematic diagram: a face with an IR eye-tracking camera, gaze rays projecting onto a screen mockup with gaze hotspots.

- **Face:** blue (`#2a78d6`) 2.5px ellipse outline at (120,120), radii 55×70; two white-filled eye ellipses (16×10) at (100,105) and (140,105); blue 5px pupils offset right (looking toward the screen).
- **IR camera:** magenta (`#d55181`) 30×18 rect below the face at (105,175) with a white/blue lens circle; label "IR camera" (13px `#6b7280`, centered at y=206). Dashed (3/3) 1px magenta rays from the camera up to each eye.
- **Gaze rays:** dashed (6/4) 2px orange (`#d95926`) lines from the eyes to the screen, landing at (420,60) and (420,90).
- **Screen:** 300×180 rect at (380,30), fill `#f0f4f8`, blue 2px border; fake webpage blocks in `#e5e9ef`: nav bar (395,45,270×20), image placeholder (395,75,130×60), two text blocks (535,75,130×30 and 535,112,130×23), banner (395,145,270×18), button area (395,170,130×28).
- **Hotspots:** translucent magenta circles where gaze lands — radius 14 at (425,60) alpha 0.5, radius 20 at (460,90) alpha 0.7, radius 12 at (580,120) alpha 0.3.
- **Labels:** bold 15px `#2a78d6` centered "Estimated gaze" at (460,78); caption 14px `#6b7280` bottom center: "Gaze vector computed from the pupil–reflection geometry; the circle is the error cone".

## Section 2: What does it collect?

- **Gaze coordinate**, estimated per sample
- **Calibration error**, as an angle
- **Pupil diameter** per eye, and a validity flag
- **Head pose change** since calibration
- **Derived: fixations and saccades**, via a velocity threshold
- **Derived: dwell time** per region of interest
- **Withheld from apps on consumer headsets:** the OS keeps the continuous gaze stream to itself — visionOS reveals where you looked only at the moment of a pinch

Key point callout: **Read the error before the coordinates:** gaze arrives in exact pixels, but its true resolution is an angular cone that widens with head movement. An error of a degree or two can cover several adjacent page elements, and the `aoi` assignment inherits that ambiguity.

Key point callout: **Fixations are a threshold decision:** `event` comes from a velocity cutoff. Move the cutoff and the same stream yields a different count.

### Visualization (canvas `c2`, 720×320)

Heatmap of gaze density over a retail shelf: 3 shelves × 9 products (27 cells labeled A–Z, AA), colored by hardcoded gaze-sample intensity.

- **Title (bold 16px `#2a78d6`, centered at y=18):** "Gaze density over a shelf — illustrative shape, not recorded data".
- **Shelf:** 600×170 rect at (60,35), fill `#f5f0e8`, border `#8B7355` 2px; two horizontal shelf-divider lines at y offsets +57 and +114.
- **Products:** 27 cells of 50×45 (last row of columns at x = 75,140,205,270,335,400,465,530,595; shelf rows at y = 40, 97, 154), thin `rgba(0,0,0,0.15)` borders.
- **Heat values (per cell, row-major):** top shelf `[0.1, 0.15, 0.2, 0.1, 0.05, 0.05, 0.0, 0.0, 0.0]`; middle (eye level) `[0.3, 0.6, 0.95, 0.85, 0.5, 0.3, 0.15, 0.1, 0.05]`; bottom `[0.2, 0.3, 0.4, 0.25, 0.15, 0.1, 0.05, 0.0, 0.0]` — eye-level shelf gets most attention with a center-left bias.
- **Color ramp:** heat < 0.2 → `rgba(255,255,255,0.8)`; < 0.4 → `rgba(26,82,118, heat*1.5)`; < 0.7 → `rgba(230,126,34, heat)`; else translucent magenta tint of `#d55181` at alpha = heat.
- **Legend (bottom left):** label "Samples per cell:" (14px `#6b7280`), a 230×12 horizontal gradient bar from white → `rgba(42,120,214,0.5)` → `rgba(217,89,38,0.7)` → magenta tint 0.95, with tick labels "No samples" / "Few" / "Many".
- **Annotation (right-aligned, 13px `#2a78d6`, two lines):** "Cell edges are the chosen region boundaries;" / "a different grid redistributes the same samples."

Below the canvas, payload note (italic gray): "Sample payload — illustrative structure, not real captured data."

Payload block:

```
// No single public schema covers consumer gaze trackers.
// Field names below are generic — the whole block is
// reconstruction from how these devices are described.
{
  // ── inferred / plausible ──
  "t":              "2026-08-22T10:41:02.480Z",
  "sample_hz":      60,
  "gaze":           { "x": 812, "y": 344 },   // screen px
  "gaze_valid":     true,
  "eye_left":       { "pupil_mm": 3.6, "valid": true },
  "eye_right":      { "pupil_mm": 3.5, "valid": false },
  "calib_error_deg": 1.4,        // angular offset at last calibration
  "head_pose_delta": 0.7,        // deg since calibration

  // derived from the stream, not measured
  "event":          "fixation",   // velocity below threshold
  "velocity_deg_s": 22,
  "dwell_ms":       310,
  "aoi":            "product_tile_3"
}
```

## Section 3: Why is it collected?

Label pill (Stated purpose):

- **Sharper picture, cheaper** — a headset renders full detail only where you are looking
- **Hands-free pointing**, including for people who cannot use a mouse

Label pill (Additional consequence):

- The same stream measures **attention without a click**
- Testing whether a layout is readable and measuring whether an ad was seen are **the same sum** — time spent looking at a region

Key point callout: **Rendering can be wrong and recover; a report cannot:** if the gaze estimate is off by a degree, the next frame corrects it and nobody notices. The same one-degree error assigned to "did they look at the ad" has nothing to correct it, and the answer changes with where someone drew the boundary.

### Visualization (canvas `c3`, 720×320)

Two-panel comparison: one gaze error, two consequences — foveated rendering absorbs it (left), an ad-viewability boundary turns it into a yes/no answer (right). Gaze error constant is a 34px circle standing in for the calibration cone.

- **Left panel ("Rendering the picture"):** 300×186 box at (24,46), fill `#fbfcfd`, `#e5e9ef` border, bold 13px `#1a5276` title above. Concentric detail-falloff rings centered in the panel: radii/alphas (86, 0.06), (58, 0.13), (30, 0.30) in translucent blue tint of `#2a78d6`, plus a solid blue 5px center dot. Labels: "sharp here" (12px `#1a5276`, above the dot) and "softer further out" (12px `#6b7280`, panel bottom).
- **Right panel ("Scoring "did they see the ad?""):** same box style at (396,46). An "AD" region occupying the right half: 126×106 rect at (546,86), fill orange tint 0.14, `#d95926` 1.5px border, bold 12px "AD" label. The gaze estimate sits exactly on the region's left boundary: violet (`#4a3aa7`) 5px dot with a dashed (4/3) 1.5px violet circle of radius 34 (the error cone) spanning both sides, labeled "best guess" below. Bold 12px verdict labels either side of the cone: "counted" in `#d95926` (inside the ad) and "not counted" in `#6b7280` (outside).
- **Captions (bottom center):** italic 11px `#6b7280`: "Schematic - the circle stands for calibration error, not a measured value."; italic 12px `#2c3e50`: "Left: a wrong guess is fixed by the next frame. Right: it becomes the answer."

## Regeneration instructions

- **Layout:** tracking-page `.obj-table`: full-width `border-collapse: collapse` table, one `<tr>` per section; left `<td>` (45%) holds `.obj-title` + `.lede` + bullets + `.key-point` callouts, right `<td>` (55%, text-align center) holds the canvas, and in the "What does it collect?" row also the `.payload-note` and `.payload` `<pre>` (both left-aligned).
- **Page style:** body system sans-serif, white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; table cell borders `1px solid #2980b9`, padding 16px; `.obj-title` bold 1.1em `#1a5276`; li 0.93em with `li b` in `#1a5276` weight 600.
- **Callouts:** `.key-point` — background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px 14px, 0.93em, leading `<strong>` in `#1a5276`.
- **Label pills:** `.lbl` inline-block uppercase 0.7em bold, padding 2px 7px, radius 3px; `.lbl-purpose` background `#eaf2fb` color `#1a5276`; `.lbl-effect` background `#fdf0e6` color `#a8501c`.
- **Payload:** `.payload` background `#f8f9fa`, left border `3px solid #1a5276`, ui-monospace 0.78em, `white-space: pre`; `.payload-note` 0.82em italic `#666` directly above.
- **Canvas:** 720×320 intrinsic attributes; a shared `setupCanvas(id)` reads the element's own width/height attributes and scales by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates). Charts use hardcoded literal data arrays (no Math.random), with a `tint(hex, alpha)` helper for translucent fills and an `rr()` rounded-rect helper.
- **Palette (tracking-set tokens, declared once as `P`):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Red is reserved for alarm states; navy `#1a5276` is ink only (headings, axes, callout borders). Project-wide palette anchors: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- No nav bar, no back/home links. In regenerated HTML, any card links elsewhere use `.html` extensions.
