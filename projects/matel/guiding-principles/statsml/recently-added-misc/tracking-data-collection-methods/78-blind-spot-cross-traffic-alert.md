# Tracking Data: Blind Spot & Cross-Traffic Alert

**Page type:** detail page (two-column obj-table layout: text left 45%, canvas right 55%, one row per section)
**HTML title tag:** Tracking Data: Blind Spot & Cross-Traffic Alert

**Subtitle:** Two small sensors in the rear corners of the bumper watch the stretch of road the mirrors cannot show, and answer one yes/no question with a light in the side mirror.

## Section 1: What is it?

**Lede:** A sensor whose entire output is one light: something is beside you, or it is not.

- **Hardware:** two small sensors hidden behind the rear bumper corners send out radio waves and listen for the bounce (radar), each covering the patch of the next lane the mirrors miss
- **When reversing:** the same corner sensors look sideways down the parking aisle and warn about cars crossing behind you (rear cross-traffic alert)

**Key point — A decision, not a picture:** the driver never sees what the sensor saw — no image, no outline, no size. The system makes the call on its own and hands over only the verdict: a light on, or a light off.

### Visualization (canvas `c1`, 720×320)

Top-down road schematic: own car in the middle lane, the two shaded corner zones the mirrors miss, and a car sitting fully inside one of them.

- **Road:** three horizontal lanes from x=60 to x=660, lane band y=58 to y=272; 1.5px `#2c3e50` solid edges at y=58 and y=272; dashed (10/8) 1.5px `#6b7280` lane dividers at y=129 and y=201. Traffic flows left to right.
- **Own car (blue `#2a78d6`):** rounded rect (rr, radius 10) 92×42 in the middle lane, x=380 to x=472, y=144 to y=186; small blue mirror stubs 8×5 at the front corners (x=452, y=139 and y=186).
- **Blind zones (orange tint):** two rects filled `#d95926` alpha 0.16 with dashed (5/4) 1px `#d95926` alpha 0.6 outlines — top lane x=250..392, y=64..124; bottom lane x=250..392, y=206..266. They sit beside and behind the rear corners.
- **Sensors:** 4px-radius magenta `#d55181` dots at the rear corners of the own car, (382,148) and (382,182), with two thin magenta alpha-0.35 arc strokes (radius 18 and 30) fanning from each dot back toward its zone.
- **Other car (dark `#2c3e50`):** rounded rect 78×36 at x=278..356, y=218..254 — fully inside the bottom zone.
- **Warning light:** 5px-radius orange `#d95926` dot just outside the bottom mirror stub at (466,196), with 11px orange label "light in the mirror" to its right at (476,200), left-aligned.
- **Zone label (11px mute `#6b7280`, centered over the top zone at (321,56)):** "zones the mirrors miss".
- **Caption (14px `#2c3e50`, bottom center, y=300):** "A whole car can sit inside the zone and appear in neither mirror — illustrative".

## Section 2: What does it collect?

- **Zone occupied, yes or no** — plus which side, left or right
- **Closing speed** — how fast something is gaining on the zone, so a fast car still short of the zone can trigger the light early
- **Context at warning time** — on some systems, whether the turn signal was on when the light fired

**Key point — Small things bounce back less:** the sensor hears an echo, and a bicycle returns a far weaker one than a truck. Motorcycles and bicycles are usually harder for these systems to catch, and detection distances vary by system.

### Visualization (canvas `c2`, 720×320)

Bar chart: strength of the returned echo for four objects in the same zone, against the alert threshold — the bicycle falls below the line.

- **Title (bold 14px `#1a5276`, top center):** "Same zone, four echoes — one below the line".
- **Axes:** baseline 1px `#2c3e50` at y=252 from x=90 to x=660; y gridlines `#e5e9ef` at 25/50/75/100 units (scale: y=252 minus units×1.8, so 100 units = y=72); 11px mute y-labels at left ("0", "50", "100"); rotated? No — only 0/50/100 labeled, right-aligned at x=82.
- **Bars (width 72, centered at x=170, 300, 430, 560):** hardcoded illustrative echo strengths — truck 92, car 70, motorcycle 38, bicycle 22 (units 0–100). Truck/car/motorcycle: fill blue `#2a78d6` alpha 0.35, 1.5px blue stroke. Bicycle: fill orange `#d95926` alpha 0.35, 1.5px orange stroke.
- **Bar labels:** 12px `#2c3e50` object name below the baseline at y=270 ("truck", "car", "motorcycle", "bicycle"); 11px mute value above each bar top.
- **Threshold:** dashed (5/4) 1.5px `#6b7280` horizontal line at 30 units (y=198) across x=90..660; right-aligned 11px mute label "alert threshold" above it at x=660.
- **Miss label:** bold 11px orange "no light" centered above the bicycle's value label.
- **Caption (italic 12px `#2c3e50`, bottom center, y=304):** "Smaller shapes return weaker echoes — illustrative units; detection distances vary by system."

**Payload note (below canvas, italic, left-aligned):** Sample payload — illustrative structure, not real captured data.

**Payload block (monospace, left border `#1a5276`):**

```
// Vendor schemas are not published.
// Whole block is reconstruction; field names generic.
// ── inferred / plausible ──
{
  "vehicle_id": "veh-30172",
  "ts":         "2026-08-24T09:12:07Z",
  "mode":       "driving",          // or "reversing"

  // measured from the radio echo
  "side":              "left",
  "zone_occupied":     true,
  "closing_speed_kmh": 18,
  "object_distance_m": 2.6,

  // derived, then paired with cabin state
  "warning_level":  2,              // 1 = light, 2 = flash + chime
  "turn_signal_on": true
}
```

## Section 3: Why is it collected?

**Label (purpose pill):** Stated purpose

- **Lane-change safety** — a second opinion on the one glance drivers most often skip
- **Reversing safety** — seeing down the aisle before the driver's window clears the neighboring cars

**Label (effect pill):** Additional consequence

- **Warnings can be remembered** — on some connected cars, each warning event, and whether the driver signaled, can be saved with the car's other driving records (telematics)

**Key point — A light that is usually right gets leaned on:** drivers who trust the light check the shoulder less, so the rare thing the sensor misses — often the bicycle — arrives with nobody looking. The better the light, the harder the miss lands.

### Visualization (canvas `c3`, 720×320)

Top-down parking schematic: reversing out of a spot between two larger vehicles, the sideways sensor cones catching a crossing car the driver cannot yet see.

- **Aisle:** horizontal band y=58 to y=138; 1.5px `#2c3e50` line at y=58; dashed (10/8) 1.5px `#6b7280` centerline at y=98.
- **Stall row:** 1.5px `#2c3e50` line at y=142 from x=140 to x=600; stall divider ticks (1px mute, vertical, y=142..300) at x=230, 320, 410, 500.
- **Neighbor vehicles (dark `#2c3e50`):** two tall rounded rects 74×130 filling the flanking stalls — left at x=238..312, right at x=418..492, both y=150..280.
- **Own car (blue `#2a78d6`):** rounded rect 66×112 in the middle stall, x=332..398, y=162..274, backing up — 2px blue arrow from (365,158) up to (365,132) with an arrowhead.
- **Sensor cones (aqua tint):** from the rear corners (334,164) and (396,164), two filled triangles `#199e70` alpha 0.14 with 1px aqua alpha-0.4 edges opening sideways along the aisle — left cone vertices (334,164), (90,72), (90,132); right cone vertices (396,164), (640,72), (640,132).
- **Crossing car (dark `#2c3e50`):** rounded rect 76×34 in the aisle at x=128..204, y=76..110, inside the left cone; 2px `#2c3e50` arrow from (208,93) to (244,93) with an arrowhead (moving right, toward the own car).
- **Blocked sight line:** dashed (4/4) 1.5px `#6b7280` line from the driver position (365,200) toward the crossing car (166,93), drawn only until it meets the left neighbor at about (312,172); 11px mute label "view blocked" near (300,196), center-aligned.
- **Cone label (11px `#199e70`, centered at (180,146)):** "sensors look down the aisle".
- **Caption (14px `#2c3e50`, bottom center, y=310):** "The sensors see the crossing car before the driver's window clears the neighbors — illustrative".

## Regeneration instructions

- **Layout:** tracking-page `.obj-table`: full-width `border-collapse: collapse` table, one `<tr>` per section; left `<td>` (45%) holds `.obj-title` + optional `.lede` + bullets + `.key-point` callouts + `.lbl` pills, right `<td>` (55%, `text-align: center`) holds the canvas — and in the "What does it collect?" row, the `.payload-note` and `.payload` `<pre>` below the canvas (both left-aligned).
- **Page style:** body system sans-serif (-apple-system stack), white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; table cell borders `1px solid #2980b9`, padding 16px; `.obj-title` bold 1.1em `#1a5276`; `li` 0.93em with `li b` in `#1a5276` weight 600. No nav bar, no back/home links.
- **Callouts:** `.key-point` — background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px 14px, 0.93em; leading `<strong>` in `#1a5276`.
- **Pills:** `.lbl` inline-block uppercase 0.7em bold, padding 2px 7px, radius 3px; `.lbl-purpose` background `#eaf2fb` color `#1a5276`; `.lbl-effect` background `#fdf0e6` color `#a8501c`.
- **Payload:** `.payload` — background `#f8f9fa`, left border `3px solid #1a5276`, ui-monospace 0.78em, `white-space: pre`; `.payload-note` italic 0.82em `#666` immediately above.
- **Canvas:** each declares intrinsic `width="720" height="320"`; a shared `setupCanvas(id)` reads those attributes, sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Shared helpers: `tint(hex, alpha)` for translucent fills and `rr()` rounded-rect path.
- **Palette (tracking pages):** categorical tokens blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`; ink `#1a5276` (headings/axes only), text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Red is reserved for genuine alarm states and not used here. (Site-wide palette reference: #1a5276 primary blue, #27ae60 green, #e74c3c red, #e67e22 orange.)
- In regenerated HTML, any card links use `.html` extensions.
