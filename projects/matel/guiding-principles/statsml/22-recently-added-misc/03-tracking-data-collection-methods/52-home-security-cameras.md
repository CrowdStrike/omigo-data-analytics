# Tracking Data: Home Security Cameras

**Page type:** detail page (two-column obj-table layout: text left 45%, canvas right 55%, one row per section)
**HTML title tag:** Tracking Data: Home Security Cameras

**Subtitle:** Doorbell and outdoor cameras record on motion, store the clip with the vendor, and label what they saw with a classifier.

## What is it?

A doorway camera that records on motion and stores the clip off-device.

- **Trigger:** continuous recording, or motion-triggered
- **Storage:** footage sits with the vendor, not on the device
- **Neighbourhood feeds:** several vendors run one where owners post clips publicly
- **Footage requests:** some vendors have operated programs through which law enforcement can ask owners for clips

### Visualization (canvas `c1`, 720×320)

Overhead neighborhood schematic: two rows of houses along a street, with overlapping circular camera-coverage zones on the camera-equipped houses.

- **Background:** full-canvas fill `#f0f4f0`. Street: grid-gray `#e5e9ef` band from y=105, height 30, with a white dashed center line (20/15) at y=120.
- **Houses (top row, x/y, camera?):** (80,50, cam), (200,50, cam), (320,50, no), (440,50, cam), (560,50, no). **Bottom row:** (140,160, no), (260,160, cam), (380,160, no), (500,160, cam), (620,160, no).
- **House drawing:** 40×30 body rect plus triangular roof; blue `#2a78d6` if it has a camera, `#7f8c8d` otherwise. Camera houses additionally get a full circle of coverage radius 70 filled `rgba(230, 126, 34, 0.15)` and a 4px orange `#d95926` camera dot at the roofline — the circles of adjacent camera houses overlap.
- **Label (bold 15px blue, left at 10,230):** "Individually narrow views overlap into coverage of the block".
- **Legend:** 5px orange dot at (550,225) with 14px text `#2c3e50` "= Camera".

## What does it collect?

- **Video** of whatever falls inside the field of view
- **Audio** near the device, where the vendor records it
- **Motion triggers** with timestamps
- **Faces,** at whatever resolution the frame and lighting allow
- **Vehicles,** and plates when close and legible enough
- **A trigger time series,** from which a routine can be read

**"Person" is a model output:** a label with a confidence beside it, not an observation — a swaying branch and a passing cat both arrive labelled with something. Counting rows by label counts classifier decisions.

**Frame coordinates, not ground:** the bounding box says where in the picture something appeared, nothing about where it stood. And since the field of view extends past the owner's property, part of what the label counts happened on public ground.

### Visualization (canvas `c2`, 720×320)

Bar chart: motion triggers by hour of day, bars colored by time-of-day band, with band underlines and peak annotations.

- **Background:** `#fafafa`; header strip (full width × 30) in ink tint 7% alpha with bold 16px ink `#1a5276` centered title: "Motion triggers by hour — illustrative shape".
- **Axis:** timeline from x=60 to x=w−30, baseline y=232, 1.5px in text `#2c3e50`; hour ticks and 14px mute labels every 3 hours: "0:00" through "24:00".
- **Trigger counts per hour (24 values, illustrative, not measured):** `[2, 1, 0, 0, 0, 1, 5, 12, 8, 4, 3, 6, 9, 5, 4, 7, 11, 14, 10, 8, 6, 4, 3, 2]`; max scale 14, bar height up to 168px.
- **Time-of-day bands (one SERIES hue per band; bar fill = hue tint at 35% alpha, 1px hue stroke):** overnight 0–6 blue `#2a78d6`; morning 6–11 green `#008300`; midday 11–16 violet `#4a3aa7`; evening 16–24 orange `#d95926`.
- **Band markers:** 3px underline strip in the band hue below the axis (y+28) plus bold 12px centered band name in the hue (y+46).
- **Annotations (14px, y=46):** "morning peak" in green at x=left+165; "evening peak" in orange at x=left+420.
- **Y-axis label:** rotated vertical mute text at x=15: "Triggers logged".
- **Footer band:** ink tint 6% strip (bottom 26px) with centered 13px text: "A trigger is motion in view — not a visitor, and not a person."

Below the canvas (right column):

Sample payload — illustrative structure, not real captured data.

```
// Vendor event schemas are not published — the whole
// block is reconstruction from plausible fields.
// ── inferred / plausible ──
{
  "event":         "motion_event",
  "device_id":     "CAM-front-…",
  "ts_start":      "2026-08-22T18:02:14Z",
  "duration_ms":   6800,
  "trigger":       "pir+pixel",
  "zone":          "sidewalk",     // owner-drawn, not measured

  "label":         "person",       // model output
  "label_conf":    0.74,
  "bbox":          [0.41, 0.22, 0.18, 0.55],  // x,y,w,h, normalised
  "familiar_face": false,
  "audio_kept":    true,
  "clip_uri":      "s3://…/ev_88…mp4"
}
```

## Why is it collected?

**Stated purpose** (label pill, blue)

- **See who came to the door,** and keep a record if a package goes missing
- **Motion rather than continuous** recording, and off-device storage — local-only footage would be lost with the device

**Additional consequence** (label pill, orange)

- The field of view **reaches past the property**, so people who made no purchase and gave no consent appear in the record
- Several cameras on one street **overlap into coverage of the block**, and vendor terms **generally permit use of footage for product improvement**

**Emergent aggregation:** each owner decided about their own doorway; street-level coverage is a side effect of many such decisions and belongs to no one who could be asked about it. It is also not a sample of the street — a row exists only where someone bought a camera, aimed it where they chose, and set a sensitivity that motion crossed.

### Visualization (canvas `c3`, 720×320)

Stacked-overlay bar chart: triggers logged in one day at four sensitivity settings, with the street-movement portion overlaid inside each bar.

- **Title (bold 14px ink, centered at y=26):** "Triggers logged in one day, at four sensitivity settings". Subtitle (12px mute, centered at y=44): "same doorway, same street, same week — only the slider moved".
- **Data (setting, total triggers, of which street movement):** Low 9 / 2; Medium 21 / 9; High 46 / 27; Very high 88 / 61. Illustrative counts per day.
- **Geometry:** padL 92, baseline y=216, top y=68, y-scale max 96; bar width 58, evenly stepped.
- **Bars:** full bar (all rows logged) filled blue `#2a78d6` tint at 26% alpha with 1px blue stroke; the street-movement portion overlaid from the baseline in orange `#d95926` tint at 42% alpha with orange stroke. Bold 13px blue count label above each bar ("9", "21", "46", "88"); 12px text `#2c3e50` setting name below the baseline.
- **Legend (right side, 12px):** blue swatch "all triggers"; orange swatch "movement out" / "on the street".
- **X caption (12px mute, centered under the bars):** "sensitivity the owner set".
- **Captions (centered, italic):** 12px text "A count drawn from these rows describes the setting as much as the street." (h−26); 11px mute "Illustrative counts — not measured from any device." (h−9).

## Regeneration instructions

- **Layout:** tracking detail page `.obj-table` — full-width table, `border-collapse: collapse`, one `<tr>` per section; left `<td>` (45%) holds `.obj-title` + `.lede` + bullets + `.key-point` callouts, right `<td>` (55%, `text-align: center`) holds the canvas (and, in the "What does it collect?" row, the `.payload-note` caption plus `<pre class="payload">` block below the canvas, both left-aligned).
- **Page style:** body system sans-serif, white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; table cell borders `1px solid #2980b9`, padding 16px; `.obj-title` bold 1.1em `#1a5276`; li 0.93em with `li b` in `#1a5276` weight 600.
- **Callouts:** `.key-point` — background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px 14px, 0.93em, first `<strong>` in `#1a5276`.
- **Label pills:** `.lbl` inline-block uppercase 0.7em bold, padding 2px 7px, radius 3px; `.lbl-purpose` background `#eaf2fb` color `#1a5276`; `.lbl-effect` background `#fdf0e6` color `#a8501c`.
- **Payload block:** `.payload` — background `#f8f9fa`, left border `3px solid #1a5276`, monospace 0.78em, `white-space: pre`; `.payload-note` italic 0.82em `#666` immediately above.
- **Canvas:** declare intrinsic `width="720" height="320"`; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setupCanvas(id)` helper that reads the element's own width/height attributes. Shared helpers: `rr()` rounded-rect path and `tint(hex, alpha)` rgba derivation from palette hexes.
- **Chart palette (tracking pages):** categorical CVD-checked tokens — blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`; ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Red is reserved for genuine alarm states, not in the series rotation. Page/site palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- No nav bar, no back/home links. In regenerated HTML, any card links elsewhere use `.html` extensions.
