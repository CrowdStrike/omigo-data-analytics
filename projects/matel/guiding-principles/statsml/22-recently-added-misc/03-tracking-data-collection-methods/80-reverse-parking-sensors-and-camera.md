# Tracking Data: Reverse Parking Sensors & Camera

**Page type:** detail page (two-column obj-table layout: text left 45%, canvas right 55%, one row per section)
**HTML title tag:** Tracking Data: Reverse Parking Sensors & Camera

**Subtitle:** Small discs in the rear bumper send out sound pulses too high-pitched to hear and time the echo; the beeps carry the distance, and the rear camera supplies what the beeps cannot — what the object actually is.

## Section 1: What is it?

**Lede:** A ruler made of echoes, plus a camera that gives the echo a name.

- **Sound-pulse discs:** small discs in the bumper send out pulses too high-pitched to hear and time how long the echo takes to come back (ultrasonic sensors); the beeps speed up as the gap shrinks and go solid near contact
- **Only what bounces back counts:** thin poles, low curbs below the bumper line, and angled surfaces can deflect the pulse away and vanish from the beeps — coverage varies by car

**Key point — The guide lines are a guess, not a picture:** the colored lines on the camera screen are drawn by the car from the steering-wheel angle — a prediction of where the bumper will go, painted over the image, not something the camera sees.

### Visualization (canvas `c1`, 720×320)

Top-down schematic: the rear of a car with four bumper discs, their overlapping sound fans, distance rings, and one obstacle returning an echo.

- **Car body:** rounded rect (rr, radius 12) from x=180 to x=540, y=24 to y=84, fill `#e5e9ef`, 2px `#2c3e50` stroke; 11px mute `#6b7280` label centered inside at (360, 58): "rear of car (seen from above)".
- **Scale:** 120 px per meter, measured downward from the bumper edge y=84.
- **Distance rings:** dashed (4/3) 1px mute-alpha-0.35 horizontal lines at 0.5 m (y=144), 1.0 m (y=204), 1.5 m (y=264); 11px mute labels right-aligned at x=668: "0.5 m", "1.0 m", "1.5 m".
- **Sensor discs:** four filled blue `#2a78d6` circles radius 5 at y=84, x = 240, 320, 400, 480.
- **Sound fans:** per disc, a filled circle sector centered on the disc, radius 170 (≈1.4 m), spanning angles 90°±29° (pointing straight down); fill blue alpha 0.07, 1px blue alpha 0.25 outline — overlaps between neighboring fans stay visible.
- **Obstacle:** orange `#d95926` filled circle radius 9 at (455, 192) — inside the rightmost fan, ≈0.9 m out.
- **Echo line:** dashed (4/3) 1.5px orange line from the rightmost disc (480, 84) to the obstacle; 11px orange label left-aligned at (500, 145): "nearest echo ≈ 0.9 m".
- **Caption (italic 12px `#2c3e50`, centered, y=306):** "Four discs, overlapping sound fans; the nearest echo drives the beep. Illustrative geometry."

## Section 2: What does it collect?

- **Echo distances** — how far the nearest obstacle sits from each bumper disc, in centimeters, refreshed several times a second while in reverse
- **Warning stage and steering angle** — which beep stage is active, plus the steering-wheel angle the car uses to draw the guide lines on the picture
- **Self-braking moments** — on some connected cars, events where the car braked itself while reversing are recorded (reverse automatic emergency braking)

**Key point — Distance, but no identity:** a hedge, a wall, and a child at the same distance produce exactly the same beep — the camera exists because the echo cannot answer "what is it".

### Visualization (canvas `c2`, 720×320)

Two channels, same three objects: identical distance bars on the beep side, three visibly different shapes on the camera side.

- **Title (bold 14px `#1a5276`, top center, y=22):** "Three objects at the same distance".
- **Column headers (bold 11px mute):** "what the beeps report" centered at (195, 52); "what the camera shows" centered at (515, 52).
- **Divider:** 1px `#e5e9ef` vertical line at x=360 from y=40 to y=280.
- **Rows:** y = 100, 170, 240; 12px `#2c3e50` row labels left-aligned at x=30: "brick wall", "thin post", "child's ball".
- **Left bars (identical by design):** baseline vertical 1px `#e5e9ef` line at x=130 from y=80 to y=260 with 10px mute label "bumper" centered at (130, 274); one bar per row, x=130, width 135 (0.75 m at 180 px/m), height 18 centered on the row; fill blue alpha 0.45, 1.5px blue stroke; 11px mute value label "0.75 m" left-aligned 8px right of each bar end.
- **Right shapes (distinct by design), centered on x=515:** wall = rect 140×24 fill green `#008300` alpha 0.5 with 1.5px green stroke; post = rect 10×54 fill violet `#4a3aa7` alpha 0.5 with 1.5px violet stroke; ball = circle radius 20 fill yellow `#c98500` alpha 0.5 with 1.5px yellow stroke.
- **Caption (italic 12px `#2c3e50`, centered, y=306):** "Identical beeps, three different consequences — the picture, not the echo, says what is there. Illustrative."

**Payload note (below canvas, italic, left-aligned):** Sample payload — illustrative structure, not real captured data.

**Payload block (monospace, left border `#1a5276`):**

```
// Parking-sensor data formats are not published by vendors.
// Whole block is reconstruction; field names generic.
// ── inferred / plausible ──
{
  "vehicle_id": "veh-30157",
  "ts":         "2026-08-24T18:12:07Z",
  "gear":       "reverse",

  // measured by the bumper discs (cm; -1 = no echo)
  "sensor_readings_cm": [182, 96, 74, 88],
  "min_distance_cm":    74,

  // state of the warning, not a measurement
  "beep_stage":          "fast",     // off | slow | fast | solid
  "steering_angle_deg":  -14,        // drawn as guide lines
  "reverse_brake_event": false       // logged on some cars
}
```

## Section 3: Why is it collected?

**Label (purpose pill):** Stated purpose

- **Parking without contact** — one sensor, one bounded task: beeps for distance, picture for identity, and the driver stays responsible the whole time

**Label (effect pill):** Additional consequence

- **Near-contact records** — on some connected cars, reverse self-braking events and very close approaches are logged and uploaded with other vehicle data

**Key point — A last-moment instrument:** the pulses fade within a few meters, so the beeps say nothing until the gap is already small — it is a close-range check, not an early warning; the driver's eyes remain the long-range sensor.

### Visualization (canvas `c3`, 720×320)

Step chart: time between beeps against the shrinking gap — silent beyond range, stepping down to a solid tone near contact.

- **Title (bold 14px `#1a5276`, top center, y=22):** "How the beep changes as the gap closes".
- **Axes:** plot from x=80 to x=660; gap runs 2.0 m (left) to 0 m (right), X(m) = 80 + (2.0 − m)/2.0 × 580; interval runs 0 s (baseline y=252) to 1.2 s (y=70), Y(s) = 252 − s/1.2 × 182.
- **Gridlines:** 1px `#e5e9ef` horizontal at 0, 0.5, 1.0 s with 11px mute right-aligned labels at x=72 ("0 s", "0.5 s", "1.0 s"); x ticks at 2.0, 1.5, 1.0, 0.5, 0 m with 11px mute labels at y=270 ("2.0 m", "1.5 m", "1.0 m", "0.5 m", "0 m").
- **Out-of-range zone:** gap 2.0–1.5 m shaded mute alpha 0.08 from y=70 to y=252; 11px mute label centered at (X(1.75), 120): "silent —", second line at (X(1.75), 134): "no echo yet".
- **Step line (blue `#2a78d6`, 2.5px, hardcoded illustrative stages):** 1.5–1.0 m at 1.0 s; 1.0–0.6 m at 0.5 s; 0.6–0.3 m at 0.25 s; 0.3–0 m at 0 s; vertical connectors at each stage change.
- **Solid-tone zone (genuine alarm state — the one permitted red):** gap 0.3–0 m shaded `#e74c3c` alpha 0.10 from y=70 to y=252; bold 11px `#e74c3c` label centered at (X(0.15), 236): "solid tone".
- **Axis titles (11px mute):** "gap between bumper and obstacle" centered at (370, 290); "time between beeps" left-aligned at (80, 58).
- **Caption (italic 11px mute, centered, y=308):** "Illustrative stages — cutoffs vary by car; nothing sounds until the last couple of meters."

## Regeneration instructions

- **Layout:** tracking-page `.obj-table`: full-width `border-collapse: collapse` table, one `<tr>` per section; left `<td>` (45%) holds `.obj-title` + optional `.lede` + bullets + `.key-point` callouts + `.lbl` pills, right `<td>` (55%, `text-align: center`) holds the canvas — and in the "What does it collect?" row, the `.payload-note` and `.payload` `<pre>` below the canvas (both left-aligned).
- **Page style:** body system sans-serif (-apple-system stack), white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; table cell borders `1px solid #2980b9`, padding 16px; `.obj-title` bold 1.1em `#1a5276`; `li` 0.93em with `li b` in `#1a5276` weight 600. No nav bar, no back/home links.
- **Callouts:** `.key-point` — background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px 14px, 0.93em; leading `<strong>` in `#1a5276`.
- **Pills:** `.lbl` inline-block uppercase 0.7em bold, padding 2px 7px, radius 3px; `.lbl-purpose` background `#eaf2fb` color `#1a5276`; `.lbl-effect` background `#fdf0e6` color `#a8501c`.
- **Payload:** `.payload` — background `#f8f9fa`, left border `3px solid #1a5276`, ui-monospace 0.78em, `white-space: pre`; `.payload-note` italic 0.82em `#666` immediately above.
- **Canvas:** each declares intrinsic `width="720" height="320"`; a shared `setupCanvas(id)` reads those attributes, sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Shared helpers: `tint(hex, alpha)` for translucent fills and `rr()` rounded-rect path.
- **Palette (tracking pages):** categorical tokens blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`; ink `#1a5276` (headings/axes only), text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Red is reserved for genuine alarm states — used once here, for the solid-tone contact zone in `c3`. (Site-wide palette reference: #1a5276 primary blue, #27ae60 green, #e74c3c red, #e67e22 orange.)
- In regenerated HTML, any card links use `.html` extensions.
