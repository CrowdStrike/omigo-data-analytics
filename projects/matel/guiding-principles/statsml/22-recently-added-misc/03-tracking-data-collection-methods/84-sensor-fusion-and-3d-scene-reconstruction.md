# Tracking Data: Sensor Fusion & 3D Scene Reconstruction

**Page type:** detail page (two-column obj-table layout: text left 45%, canvas right 55%, one row per section)
**HTML title tag:** Tracking Data: Sensor Fusion &amp; 3D Scene Reconstruction

**Subtitle:** The car merges what its cameras, radars, and laser scanner each report into one live 3-D picture of the street — the view its parking display draws is a simplified slice of that picture.

## Section 1: What is it?

**Lede:** A referee that combines three kinds of eyes into one picture the car can drive from.

- **Three different witnesses:** the camera can tell what a thing is, the radar can tell how fast it is closing in, and the spinning laser scanner (lidar) can tell exactly where it sits
- **One merged picture:** the car combines their reports into a single live 3-D model of everything around it (sensor fusion); the drawing on the parking screen is a simplified view of this model

**Key point — An estimate, not a photograph:** every object in the merged picture is a best guess built from whichever sensors voted for it, and it inherits their uncertainty. The car drives from this guess, revised many times per second.

### Visualization (canvas `c1`, 720×320)

Three horizontal sensor lanes showing the same street moment with different partial detections, merging down each column into one fused lane below.

- **Lanes:** four horizontal strips from x=115 to x=580 — camera y=30 h=34, radar y=80 h=34, laser y=130 h=34, fused y=210 h=42. Background tints: camera `tint(P.blue, 0.10)`, radar `tint(P.violet, 0.10)`, laser `tint(P.aqua, 0.10)`, fused `tint(P.green, 0.10)` with a 1.5px `#008300` border.
- **Lane names (bold 11px mute, right-aligned at x=105, lane vertical center):** "camera", "radar", "laser", "fused".
- **Lane annotations (10px mute, left-aligned at x=590, lane vertical center):** "sees WHAT it is", "measures CLOSING SPEED", "pins WHERE it is", "one merged list".
- **Objects (same street moment, hardcoded):** pedestrian at x=217, car at x=357, cyclist at x=487 (each x is the object's column across all four lanes).
- **Camera lane:** wide uncertainty band per object — rounded rect 64×14 (radius 7) centered on the column, `tint(P.blue, 0.30)`; 9px `#2a78d6` type label under the band inside the lane: "person", "car", "bike".
- **Radar lane:** medium band 30×14 `tint(P.violet, 0.30)`; 1.5px violet arrow (16px long, arrowhead) leaving the band along the lane axis — pedestrian and cyclist arrows point right, car arrow points left; 9px `#4a3aa7` speed label under the band: "1.4 m/s", "8.2 m/s", "4.0 m/s".
- **Laser lane:** tight marker — 8×14 filled rect `tint(P.aqua, 0.85)` centered on the column, plus 1px aqua vertical tick through the lane; no label.
- **Merge arrows:** for each column, a 1.5px `#6b7280` vertical arrow from y=170 to y=205 with a small arrowhead.
- **Fused lane:** tight marker 10×16 `tint(P.green, 0.75)` on each column; two stacked 9px `#008300` labels under the marker inside the lane: type on line one ("person", "car", "bike"), speed + sureness on line two ("1.4 m/s · 0.93", "8.2 m/s · 0.97", "4.0 m/s · 0.88").
- **Caption (italic 12px `#2c3e50`, bottom center, y=306):** "Three partial reports of the same street moment merge into one list. Illustrative scene."

## Section 2: What does it collect?

- **A list of tracked objects** — each nearby person, car, or bike gets an entry with its place, speed, kind, and a sureness score (confidence) that came from combining the sensors
- **Disagreement records** — moments when the sensors told different stories about the same spot, kept alongside which sensor said what

**Key point — The tie-break is a rule, not a measurement:** when the radar reports something ahead and the camera sees nothing, which one wins was written down by a person in advance. Trusting the camera risks missing a real obstacle; trusting the radar risks braking for a ghost — the rule quietly picks which mistake the car will make.

### Visualization (canvas `c2`, 720×320)

A disagreement case as a decision fork: two sensor verdicts converge on a pre-written rule, which branches into the two possible actions and the error each one risks.

- **Title (bold 14px `#1a5276`, top center, y=24):** "When the sensors disagree, a rule picks the mistake".
- **Verdict nodes (left):** violet `#4a3aa7` filled circle r=7 at (95,105) with 12px violet label "radar: something ahead" left-aligned at (110,109); blue `#2a78d6` filled circle r=7 at (95,195) with 12px blue label "camera: nothing there" at (110,199).
- **Converging lines:** 1.5px `#6b7280` lines from (270,105) and (270,195) to the fork at (330,150).
- **Fork marker:** ink `#1a5276` filled diamond (10px half-diagonal) at (330,150); 10px mute label "written rule decides" centered below at (330,175).
- **Branches:** 1.5px `#6b7280` lines from the fork to (460,95) and (460,205); 11px mute branch labels near each midpoint — "rule: trust the camera" above the top branch at (395,105), "rule: trust the radar" below the bottom branch at (395,195).
- **Top outcome:** red `#e74c3c` filled warning triangle (side ~16px) at (470,95) — genuine alarm state; 12px `#2c3e50` text "keep driving" at (490,92); bold 11px red text "risk: a real obstacle is missed" at (490,110).
- **Bottom outcome:** orange `#d95926` filled circle r=8 at (470,205); 12px `#2c3e50` text "brake hard" at (490,202); bold 11px orange text "risk: braking for a ghost" at (490,220).
- **Caption (italic 12px `#2c3e50`, bottom center, y=300):** "Neither choice is safe in both worlds — the rule, written in advance, picks which error the car will make. Illustrative case."

**Payload note (below canvas, italic, left-aligned):** Sample payload — illustrative structure, not real captured data.

**Payload block (monospace, left border `#1a5276`):**

```
// Perception-stack schemas are not published.
// Whole block is reconstruction; field names generic.
// ── inferred / plausible ──
{
  "frame_ts": "2026-08-22T17:03:41.120Z",
  "fused_objects": [
    { "id":         "obj-1041",
      "type":       "pedestrian",       // fused estimate
      "position_m": [12.4, -3.1, 0.0],  // fused estimate
      "speed_mps":  1.4,                // fused estimate
      "confidence": 0.93,
      "sources":    ["camera", "lidar"] }
  ],
  "disagreement_event": {
    "radar":  "object_ahead",
    "camera": "no_object",
    "lidar":  "weak_return",
    "resolution": "rule: trust_camera",
    "flagged_for_upload": true
  }
}
```

## Section 3: Why is it collected?

**Label (purpose pill):** Stated purpose

- **One coherent picture to drive from** — the car needs a single answer to "what is around me", not three partial ones
- **Filling each other's gaps** — usually the merge keeps the car seeing when one sensor is blinded by darkness, glare, or heavy rain

**Label (effect pill):** Additional consequence

- **A 3-D record of public space** — the merged scene is a continuously updated map of where every nearby pedestrian and vehicle was and how it moved, and on some fleets it can be kept long after the drive ends

**Key point — Disagreements are the most valuable data:** the moments when the sensors' pictures of the world part ways mark exactly where the system is weakest. On some fleets those moments are flagged, uploaded, and reviewed — so an unusual street scene is more likely to leave the car than an ordinary one.

### Visualization (canvas `c3`, 720×320)

Grouped bar chart: illustrative 0–10 scores for the three sensors across four situations, showing that no sensor wins every column.

- **Title (bold 14px `#1a5276`, top center, y=22):** "No sensor wins everywhere — why the reports get merged".
- **Axes:** plot from x=70 to x=680, baseline y=250 (score 0) to y=60 (score 10); 1px `#e5e9ef` gridlines at scores 0/5/10 with 11px mute y-labels ("0", "5", "10") right-aligned at x=62.
- **Groups (centered 11px mute labels at y=270):** "what is it?", "where exactly?", "closing speed?", "heavy rain?" — group width 152.5px starting at x=70.
- **Bars:** three per group (camera, radar, laser), width 30, gap 8, centered in the group; fills `tint(hue, 0.55)` with 1.5px solid hue stroke — camera blue `#2a78d6`, radar violet `#4a3aa7`, laser aqua `#199e70`; 10px mute score value above each bar.
- **Scores (hardcoded, illustrative):** camera [9, 4, 2, 3], radar [2, 5, 9, 8], laser [4, 9, 5, 4] across the four situations in order.
- **Legend (11px, top right at y=40):** swatch + label per sensor in its hue — "camera", "radar", "laser".
- **Caption (italic 11px mute, bottom center, y=300):** "Illustrative 0–10 scores — each column has a different winner, so the car needs all three."

## Regeneration instructions

- **Layout:** tracking-page `.obj-table`: full-width `border-collapse: collapse` table, one `<tr>` per section; left `<td>` (45%) holds `.obj-title` + optional `.lede` + bullets + `.key-point` callouts + `.lbl` pills, right `<td>` (55%, `text-align: center`) holds the canvas — and in the "What does it collect?" row, the `.payload-note` and `.payload` `<pre>` below the canvas (both left-aligned).
- **Page style:** body system sans-serif (-apple-system stack), white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; table cell borders `1px solid #2980b9`, padding 16px; `.obj-title` bold 1.1em `#1a5276`; `li` 0.93em with `li b` in `#1a5276` weight 600. No nav bar, no back/home links.
- **Callouts:** `.key-point` — background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px 14px, 0.93em; leading `<strong>` in `#1a5276`.
- **Pills:** `.lbl` inline-block uppercase 0.7em bold, padding 2px 7px, radius 3px; `.lbl-purpose` background `#eaf2fb` color `#1a5276`; `.lbl-effect` background `#fdf0e6` color `#a8501c`.
- **Payload:** `.payload` — background `#f8f9fa`, left border `3px solid #1a5276`, ui-monospace 0.78em, `white-space: pre`; `.payload-note` italic 0.82em `#666` immediately above.
- **Canvas:** each declares intrinsic `width="720" height="320"`; a shared `setupCanvas(id)` reads those attributes, sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Shared helpers: `tint(hex, alpha)` for translucent fills and `rr()` rounded-rect path.
- **Palette (tracking pages):** categorical tokens blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`; ink `#1a5276` (headings/axes only), text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Red `#e74c3c` appears once, on the missed-obstacle outcome in `c2` — a genuine alarm state.
- In regenerated HTML, any card links use `.html` extensions.
