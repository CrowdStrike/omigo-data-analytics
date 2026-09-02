# Tracking Data: Lane Departure & Lane Keeping

**Page type:** detail page (two-column obj-table layout: text left 45%, canvas right 55%, one row per section)
**HTML title tag:** Tracking Data: Lane Departure &amp; Lane Keeping

**Subtitle:** A camera behind the windshield looks for the painted lane lines, and either warns the driver when the car drifts over one or gently steers it back toward the middle.

## Section 1: What is it?

**Lede:** A camera that reads the paint on the road and turns it into a lane the car can follow.

- **Hardware:** one small camera behind the windshield, near the mirror, that looks for the painted lines ahead and draws a smooth curve through them
- **Two levels of help:** the basic version warns — a beep or a wheel vibration — when the car drifts over a line; the fuller version gently steers back toward the middle (lane keeping), with the driver's hands still on the wheel

**Key point — It follows a drawing, not the road:** the lane the system steers toward is a smooth curve it drew from whatever paint it could see (a fitted lane model). Where the paint is faded or doubled, that curve can part ways with the real lane.

### Visualization (canvas `c1`, 720×320)

Road schematic, top-down: the painted dashes actually on the road, the smooth curve the camera believes, and the car between them. Where the paint fades, the believed curve drifts away from the real line.

- **Road band:** fill `#f1f3f7` rect from x=60 to x=680, y=78 to y=242.
- **Near painted line (bottom):** 3px yellow `#c98500` dashes at y=218 — dash 26, gap 16 — from x=76 to x=664, full alpha.
- **Far painted line (top):** same dash style at y=102; full alpha from x=76 to x=396, then alpha 0.22 (faded paint) from x=404 to x=664.
- **Fitted curves (what the camera believes):** 2.5px blue `#2a78d6`. Bottom: straight line (70,218)→(668,218). Top: straight (70,102)→(396,102), then a quadratic curve with control (540,100) ending at (668,86) — the guess bends away where the paint faded.
- **Aim line (the middle it steers toward):** dotted (2/4) 1.5px blue alpha 0.6, halfway between the fitted curves: straight (70,160)→(396,160), then quadratic control (540,158) to (668,152).
- **Car:** blue `#2a78d6` rounded rect (rr, radius 8) 58×30 centered at (150,176) — sitting a little below the aim line; bold 11px white label "car" centered inside the rect.
- **Labels (11px):** mute "painted dashes — what is on the road" left-aligned at (100,94); mute italic "faded paint" centered at (530,118); blue bold "smooth curve — what the camera believes" left-aligned at (420,72); blue alpha-0.8 "the middle it aims for" left-aligned at (215,152).
- **Caption (14px `#2c3e50`, bottom center, y=300):** "Where the paint fades, the camera's curve and the real lane can part ways (schematic)".

## Section 2: What does it collect?

- **Where the car sits** — the distance from the middle of the lane, measured many times a second, and how fast the car is sliding toward a line
- **What happened and what it did** — each drift over a line, each warning, each gentle steering push it applied, and each time the driver's own steering took over
- **How sure it is** — a certainty score for each painted line ("did I actually find it?"), plus regular checks that hands are on the wheel

**Key point — A "drift" is measured against the guess:** the car is judged against the curve the camera drew, not the road itself — so faded or doubled paint can produce a drift record while the car holds a steady line on the real road.

### Visualization (canvas `c2`, 720×320)

Bar chart: how sure the camera is that it found the lines, under six road conditions, against the cutoff below which the assist bows out. The surprise bar is construction paint: certainty stays high — of the wrong lines.

- **Title (bold 14px `#1a5276`, top center):** "How sure the camera is that it found the lines".
- **Plot:** baseline y=252, full-scale (certainty 1.0) at y=64; bars 64 wide, six centers evenly spaced from x=125 to x=625 (step 100).
- **Bars (condition, certainty):** fresh paint 0.95; faded paint 0.55; rain glare 0.45; low sun 0.50; snow 0.15; construction — two line sets 0.85. All values invented.
- **Bar styling:** above-cutoff blue bars fill `#2a78d6` alpha 0.55 with 1.5px blue stroke; below-cutoff bars fill blue alpha 0.18 with 1.5px blue-alpha-0.5 stroke; the construction bar fills orange `#d95926` alpha 0.5 with 1.5px orange stroke.
- **Value labels:** 10px mute `#6b7280` value (e.g. "0.95") centered above each bar; the construction bar additionally gets bold 11px orange "sure — of the old lines" above its value, centered 30px left of the bar center so it stays clear of the right edge.
- **Cutoff line:** dashed (4/3) 1.5px mute horizontal line at certainty 0.70 (y=120.4), 11px mute label "assist available above this line" centered at x=375 just above it — the mid bars top out below this height, so the label sits in clear space.
- **X labels:** 11px mute centered at y=270; "construction" gets a second line "two line sets" at y=283.
- **Caption (italic 12px `#2c3e50`, bottom center, y=306):** "Illustrative. Weather lowers certainty until the assist bows out; construction paint can leave it sure — of the old lines."

**Payload note (below canvas, italic, left-aligned):** Sample payload — illustrative structure, not real captured data.

**Payload block (monospace, left border `#1a5276`):**

```
// Lane-assist vendor schemas are not published.
// Whole block is reconstruction; field names generic.
// ── inferred / plausible ──
{
  "vehicle_id": "veh-40317",
  "ts":         "2026-08-22T08:14:07Z",
  "speed_kmh":  104,

  // measured from the camera frame
  "lane_offset_m":         0.42,   // distance from lane middle
  "line_confidence_left":  0.91,
  "line_confidence_right": 0.34,   // faded paint on this side
  "lane_width_m":          3.6,

  // actions and checks
  "event":           "lane_departure_warning",
  "steering_nudge":  { "applied": true, "torque_nm": 1.8 },
  "hands_on_wheel":  true,
  "driver_override": false,
  "assist_state":    "active"
}
```

## Section 3: Why is it collected?

**Label (purpose pill):** Stated purpose

- **Drift warnings** — the beep or wheel shake when the car crosses a line without the turn signal on
- **Lane centering** — small steering pushes that keep the car near the middle on the highway; the driver stays responsible the whole time

**Label (effect pill):** Additional consequence

- **Events can travel** — on connected cars, drift-over events and hands-off-wheel warnings can be logged or uploaded with other driving data, where insurers and crash investigators can reach them

**Key point — Built for one setting:** it was designed for well-marked highways, and outside them its certainty drops quietly — the help just stops, on some systems without the driver noticing the exact moment it did.

### Visualization (canvas `c3`, 720×320)

Timeline: the car's position in the lane over twenty seconds — a slow drift toward the painted line, the warning, the gentle push back to the middle, then the driver steering across the same line on purpose with no warning at all.

- **Title (bold 14px `#1a5276`, top center):** "Drift, warning, gentle push — then the driver takes over".
- **Axes:** t=0..20 s maps to x=70..670; position in lane maps 0 m (lane middle) to y=250 and 0.9 m (painted line) to y=95 (172 px per metre). X ticks at 0/5/10/15/20 s, 11px mute labels "0 s".."20 s" at y=268.
- **Lane middle:** 1px `#e5e9ef` line at y=250, 11px mute label "middle of lane" left-aligned above it at x=72.
- **Painted line:** 3px yellow `#c98500` dashes (26/16) at y=95, 11px mute label "painted line" left-aligned above it at x=72.
- **Warning zone:** orange `#d95926` alpha 0.08 band between 0.75 m and 0.9 m (y=95 to y=121).
- **Trace, assist phase (blue `#2a78d6`, 2.5px):** points (t s, m): (0,0.05) (2,0.12) (4,0.32) (6,0.58) (8,0.80) (9,0.90) (10,0.78) (12,0.50) (14,0.28) (16,0.20) (17,0.30).
- **Trace, driver phase (violet `#4a3aa7`, 2.5px, continues from t=17):** (17,0.30) (18,0.62) (19,0.92) (20,1.15).
- **Warning marker:** orange filled dot radius 5 at (9 s, 0.90 m); bold 11px orange label "warning fires" right-aligned ending at X(9)-10, y just above the dot.
- **Nudge label:** 11px blue "gentle steering push back" left-aligned at (X(12)+10, y of 0.56 m) — to the right of the descending segment, clear of the trace.
- **Override labels:** bold 11px violet "driver turns the wheel" left-aligned at (X(16.9), y=228), below the start of the violet segment; 11px mute "turn signal on — no warning" right-aligned ending at (630, y=78), above the painted line near the second crossing.
- **Caption (italic 12px `#2c3e50`, bottom center, y=304):** "Illustrative trace. The same line-crossing fires a warning at 9 s and none at 19 s — the turn signal is the difference."

## Regeneration instructions

- **Layout:** tracking-page `.obj-table`: full-width `border-collapse: collapse` table, one `<tr>` per section; left `<td>` (45%) holds `.obj-title` + optional `.lede` + bullets + `.key-point` callouts + `.lbl` pills, right `<td>` (55%, `text-align: center`) holds the canvas — and in the "What does it collect?" row, the `.payload-note` and `.payload` `<pre>` below the canvas (both left-aligned).
- **Page style:** body system sans-serif (-apple-system stack), white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; table cell borders `1px solid #2980b9`, padding 16px; `.obj-title` bold 1.1em `#1a5276`; `li` 0.93em with `li b` in `#1a5276` weight 600. No nav bar, no back/home links.
- **Callouts:** `.key-point` — background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px 14px, 0.93em; leading `<strong>` in `#1a5276`.
- **Pills:** `.lbl` inline-block uppercase 0.7em bold, padding 2px 7px, radius 3px; `.lbl-purpose` background `#eaf2fb` color `#1a5276`; `.lbl-effect` background `#fdf0e6` color `#a8501c`.
- **Payload:** `.payload` — background `#f8f9fa`, left border `3px solid #1a5276`, ui-monospace 0.78em, `white-space: pre`; `.payload-note` italic 0.82em `#666` immediately above.
- **Canvas:** each declares intrinsic `width="720" height="320"`; a shared `setupCanvas(id)` reads those attributes, sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Shared helpers: `tint(hex, alpha)` for translucent fills and `rr()` rounded-rect path.
- **Palette (tracking pages):** categorical tokens blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`; ink `#1a5276` (headings/axes only), text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Red is reserved for genuine alarm states and not used here. (Site-wide palette reference: #1a5276 primary blue, #27ae60 green, #e74c3c red, #e67e22 orange.)
- In regenerated HTML, any card links use `.html` extensions.
