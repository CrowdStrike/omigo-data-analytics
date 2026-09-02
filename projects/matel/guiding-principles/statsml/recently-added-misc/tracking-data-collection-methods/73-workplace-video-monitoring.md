# Tracking Data: Workplace Video Monitoring

**Page type:** detail page (two-column obj-table layout: text left 45%, canvas right 55%, one row per section)
**HTML title tag:** Tracking Data: Workplace Video Monitoring

**Subtitle:** A workplace camera records pixels. Every number produced above it — a headcount, a dwell time, a zone occupancy, a compliance flag — is a model output, and the model's error rate is rarely measured once the system is live.

## Section 1: What is it?

**Lede:** The old camera, plus a software layer that turns pixels into rows.

- **Coverage** — entrance, corridors, open floor, loading dock, aisles, stockroom. That part is familiar
- **What changed:** the feed is processed continuously rather than reviewed after an incident
- **The pipeline:** software boxes each person it thinks it sees, gives the box an identifier, follows it frame to frame, decides its floor zone, writes a row
- **Built from those rows:** counts, dwell times, occupancy by zone, path traces, queue lengths, safety flags

**Key point — The camera measures light; everything after is inference.** A detection is a classifier decision, a track a matching decision, a dwell time an integral over those decisions, and a compliance flag a classifier on a classifier.

**Key point — Each layer's error belongs to the model and the placement**, not to the workplace. In production that rate is usually unknown, because nobody is in the aisle recording the truth to compare against. A number with no ground truth has no known accuracy, and a dashboard renders it to one decimal place regardless.

### Visualization (canvas `c1`, 720×320)

Pipeline diagram: one measured stage and four inference stages in a chain, with a widening uncertainty band beneath.

- **Title (bold 16px blue `#2a78d6`, centered at y=22):** "One measurement, then four layers of inference".
- **Stages (5 boxes 122×54 at y=48, 15px gaps, centered as a group; each box shows a bold 14px first line and a 13px muted `#6b7280` second line):**
  - "Pixels" / "recorded" — the measured stage: fill `rgba(0,131,0,0.28)`, stroke green `#008300`, first line in `#1e7e46`
  - "Person detection" / "classifier" — model stage: fill `rgba(42,120,214,0.35)`, stroke blue `#2a78d6`, first line in blue
  - "Track identity" / "matching" — model stage, same blue styling
  - "Zone + dwell" / "geometry + track" — model stage, same blue styling
  - "Occupancy, flag" / "the dashboard" — model stage, same blue styling
- **Arrows:** orange `#d95926` 2px connectors with filled arrowheads between consecutive boxes.
- **Uncertainty band:** beneath each stage at y=132, a 30px-tall centered rectangle in `rgba(217,89,38,0.30)` with orange 1px outline; widths grow left to right: 2, 14, 24, 34, 44.
- **Caption (14px text `#2c3e50`, centered at y=184):** "error accumulates left to right, and is reported at none of these steps".
- **Footnote (13px muted, centered at bottom):** "Schematic — band widths are illustrative, not estimated error".

## Section 2: What does it collect?

- **Person detections** per frame — a bounding box and a confidence score
- **Track identifiers** stitching detections across frames into one path
- **Zone entry and exit** against floor regions an installer drew
- **Dwell time** per zone, and occupancy per zone per interval
- **Line-crossing counts** at doors and aisle mouths, with direction
- **Queue length** and wait-time estimates at counters
- **Safety-compliance flags** — a required item present or absent, a restricted area entered
- **Re-identification links** proposing a person on one camera is the same on another

**Key point — Confidence is discarded on the way to the dashboard:** a low-confidence detection and a high-confidence one both increment the count by one.

**Key point — Thresholding throws away the interval:** collapsing a probability into a hard yes or no removes what would bound the total, so reported occupancy arrives with no uncertainty even though the system computed the ingredients for one.

### Visualization (canvas `c2`, 720×320)

Schematic floor plan: a wall-mounted camera's view cone across a floor, two fixed obstructions casting permanent shadow wedges, and three worker positions read differently.

- **Title (bold 16px blue `#2a78d6`, centered at y=22):** "Which positions are missed is fixed by geometry".
- **Floor:** rectangle at (70,40), 580×150, fill `#fbfcfd`, 1px muted `#6b7280` outline.
- **Camera:** blue `#2a78d6` dot radius 5 on the left wall at (70,115), labeled "camera" in 13px right-aligned beside it; view cone from the camera to the full right wall (corners (650,46) and (650,184)), fill `rgba(42,120,214,0.12)` with blue edge lines.
- **Obstructions:** two 16×16 dark-gray `#555` squares — a "pillar" at (240,70) and "shelving" at (340,140), 13px `#555` labels above each; each casts a projected shadow wedge to the right wall in `rgba(120,120,120,0.30)` computed from the camera position (the shadow does not move).
- **Worker positions (radius-5 dots with bold 13px labels to the right):** "A  observed" at (200,120) in green `#008300`; "B  in shadow" at (470,92) in orange `#d95926`; "C  edge of frame" at (600,178) in orange.
- **Caption (14px text `#2c3e50`, centered at y=212):** "B and C read the same as an empty floor — the shadow does not move, so time does not average it out".
- **Footnote (13px muted, centered at bottom):** "Schematic floor plan".

**Payload note (italic gray, below canvas):** Sample payload — illustrative structure, not real captured data.

**Payload block (monospace, `#f8f9fa` background, left border 3px solid `#1a5276`):**

```
// Workplace video-analytics platforms do not publish a common
// event schema. Field names below are reconstruction throughout.
{
  // ── inferred / plausible ──
  "camera_id":   "cam_b2_corr_04",
  "zone_id":     "zone_floor_east",
  "ts":          "2026-08-22T11:07:14.320Z",

  // ── inferred / plausible — model outputs, not measurements ──
  "detection":   { "class": "person", "confidence": 0.71,
                   "bbox": [412, 188, 96, 214] },
  "track_id":    "trk_9f2c",       // an assignment, may break or swap
  "track_age_s": 42.6,             // reset to 0 on any track break
  "dwell_s":     311.4,            // integral over the assignments above
  "reid_link":   { "from": "cam_b2_corr_03", "score": 0.63 },
  "compliance":  { "rule": "required_item_present", "result": false,
                   "confidence": 0.58 }
}
// confidence 0.71 and score 0.63 are the honest part of this record.
// Downstream, "occupancy = 7" and "compliance breach" carry neither.
```

## Section 3: Why is it collected?

**Label (STATED PURPOSE, blue pill):**

- **Safety and security** — the real reasons in most installations: reconstructing an accident, resolving theft, keeping people out of hazardous areas
- **Evacuation completeness**, and occupancy counting to size ventilation, exits and cleaning

**Label (ADDITIONAL CONSEQUENCE, orange pill):**

- **The analytics layer is generic** — once detections, tracks and zones exist, per-person summaries are a group-by away
- **Derivable** with no new hardware: minutes per zone, time away from a station, walking distance, arrival and departure times

**Key point — Camera placement is not a random sample:** cameras go where the assets and doors are, and where cabling was easy. Ranking areas by detected activity partly ranks them by how well they are watched — and dividing by floor area rather than observed area makes the well-covered zone look busy because it is well covered.

### Visualization (canvas `c3`, 720×320)

Two-panel bar chart: detected activity per zone raw, and the same data divided by observed area — the ranking flattens.

- **Title (bold 16px blue `#2a78d6`, centered at y=22):** "Detected activity, and the same data per observed area".
- **Data (5 zones A–E):** detected events `[92, 74, 51, 30, 12]`; camera coverage share of each zone `[0.90, 0.72, 0.50, 0.30, 0.12]`; right panel plots detected ÷ coverage (per-observed-area values ≈ `[102.2, 102.8, 102, 100, 100]`, i.e. nearly flat).
- **Panels (each 285px wide, baseline y=176, bar height scaled to the panel's own max over 96px):**
  - Left panel from x=60, title bold 14px blue centered at y=44: "detected events (raw)"; bars fill `rgba(42,120,214,0.35)` with blue `#2a78d6` outline.
  - Right panel from x=400, title: "events ÷ observed area"; bars fill `rgba(217,89,38,0.30)` with orange `#d95926` outline.
- **Bars:** 5 per panel, each 56% of its slot width, centered in the slot; 13px muted labels "zone A" … "zone E" below the baseline; 1px muted baseline per panel.
- **Caption (14px text `#2c3e50`, centered at y=216):** "the clean ranking on the left is mostly a ranking of camera coverage".
- **Footnote (13px muted, centered at bottom):** "Schematic — illustrative counts and coverage shares".

## Regeneration instructions

- **Layout:** tracking-page `.obj-table`: full-width `border-collapse: collapse` table, one `<tr>` per section; left `<td>` (45%) holds `.obj-title` heading + `.lede` + bullets + `.key-point` callouts, right `<td>` (55%, centered) holds the canvas (and, in the "What does it collect?" row, the `.payload-note` + `.payload` block below the canvas, left-aligned).
- **Page style:** body system sans-serif, white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; table cell borders `1px solid #2980b9`, padding 16px; `.obj-title` bold 1.1em `#1a5276`; li 0.93em with `li b` in `#1a5276` weight 600.
- **Callouts:** `.key-point` — background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px 14px, 0.93em, leading `<strong>` in `#1a5276`.
- **Label pills:** `.lbl` inline-block uppercase 0.7em weight 700, letter-spacing 0.05em, padding 2px 7px, radius 3px; `.lbl-purpose` background `#eaf2fb` color `#1a5276`; `.lbl-effect` background `#fdf0e6` color `#a8501c`.
- **Payload:** `.payload` background `#f8f9fa`, left border `3px solid #1a5276`, ui-monospace 0.78em, `white-space: pre`; `.payload-note` italic 0.82em `#666` immediately above it.
- **Canvas:** intrinsic 720×320 per chart; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setupCanvas(id)` helper that reads the element's own width/height attributes. A rounded-rect path helper `rr()` is declared alongside.
- **Palette:** charts use the validated categorical token palette — blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`; ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Red is deliberately not in the rotation (reserved for alarm states). Page chrome uses primary blue `#1a5276` (site palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange).
- No `Math.random()` in charts — all data arrays are hardcoded literals; invented numbers are labeled "illustrative" or "schematic".
- No nav bar, no back/home links. In regenerated HTML any card links use `.html` extensions.
