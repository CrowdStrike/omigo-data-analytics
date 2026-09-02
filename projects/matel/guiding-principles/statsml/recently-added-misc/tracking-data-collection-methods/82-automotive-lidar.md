# Tracking Data: Automotive Lidar

**Page type:** detail page (two-column obj-table layout: text left 45%, canvas right 55%, one row per section)
**HTML title tag:** Tracking Data: Automotive Lidar

**Subtitle:** A laser unit on a self-driving car fires invisible light pulses and times each echo — light's speed is known, so the timing itself is the distance — building a live 3-D dot picture of everything around the vehicle.

## Section 1: What is it?

**Lede:** The self-driving car's ruler: it does not photograph the street, it measures it.

- **Hardware:** a laser unit — spinning on the roof, or flat panels built into the body — fires invisible light pulses millions of times a second (lidar)
- **How it measures:** light travels at a known speed, so timing how long each pulse takes to bounce back is itself the distance to whatever it hit

**Key point — A measurement, not a guess:** a camera looks at a flat picture and estimates how far away things are; each dot here is a timed round trip — a genuine distance. This is the sensor the rest of the driving software usually trusts for "where things are."

### Visualization (canvas `c1`, 720×320)

Side view, schematic: the car's roof unit fans pulses across the scene; a nearby parked car catches many dots, a distant pedestrian catches a handful.

- **Ground line:** 2px `#2c3e50` horizontal line at y=250 from x=40 to x=690.
- **Ego car (left):** body rounded rect (rr, radius 10) 150×42 at (60,205) in blue `#2a78d6`; cabin rounded rect 75×30 at (95,180) blue; wheels — filled `#2c3e50` circles radius 11 at (95,248) and (175,248).
- **Laser unit:** rounded rect 22×12 at (125,166), radius 3, violet `#4a3aa7`, sitting on the cabin roof; 10px mute `#6b7280` label "spinning laser unit" centered above at (136,156).
- **Nearby parked car (middle):** rounded rect 110×47 at (330,205), radius 10, fill `#e5e9ef`, 1.5px `#6b7280` stroke.
- **Distant pedestrian (right):** drawn in 2px `#6b7280` strokes — head circle radius 7 centered (600,192), body line (600,199)→(600,228), legs (600,228)→(592,248) and (600,228)→(608,248).
- **Pulse lines:** 1px lines in blue at alpha 0.22 from the unit's right edge (147,172) to every dot listed below.
- **Dots on the nearby car (aqua `#199e70`, radius 2.5, filled):** left face x=330 at y = 210, 218, 226, 234, 242; top edge y=205 at x = 342, 356, 370, 384, 398, 412, 426.
- **Dots on the pedestrian (aqua, radius 2.5):** (600,190), (600,208), (600,226).
- **Labels (11px mute):** "nearby car — dozens of dots" centered at (385,188); "far pedestrian — a handful" centered at (600,168).
- **Caption (14px `#2c3e50`, bottom center, y=300):** "Each pulse's flight time is a distance — near things catch many pulses, far things catch few".

## Section 2: What does it collect?

- **Per dot** — the direction the pulse went out, the distance it measured, and how strongly the surface bounced it back
- **Per sweep** — hundreds of thousands of dots, refreshed many times a second: a live 3-D dot picture of the street (a point cloud)
- **Over a drive** — stacked sweeps trace the streets, parked cars, buildings, and passers-by the car moves past

**Key point — Thin exactly where it matters:** dots spread apart the farther they fly; a person far down the road may catch only a handful — too few for the software to say "person." Each dot is honest, but coverage is thinnest where warning time matters most.

### Visualization (canvas `c2`, 720×320)

Bar chart: how many dots land on one pedestrian-sized target at growing distance, with the zone where too few dots remain to recognize a person.

- **Title (bold 14px `#1a5276`, top center, y=22):** "Dots landing on a pedestrian, by distance".
- **Scale note (11px mute, centered, y=40):** "bar height on a square-root scale so the small counts stay visible".
- **Bars:** baseline y=262; five bars, width 56, centers at x = 140, 250, 360, 470, 580; heights = sqrt(count) × 9 px; fill blue `#2a78d6` alpha 0.4, 1.5px blue stroke.
- **Counts (illustrative, hardcoded):** 400 dots at 10 m, 65 at 25 m, 16 at 50 m, 4 at 100 m, 1 at 200 m.
- **Value labels (bold 11px `#2c3e50`, above each bar):** "400", "65", "16", "4", "1".
- **Distance labels (11px mute, below baseline, y=280):** "10 m", "25 m", "50 m", "100 m", "200 m".
- **Too-few zone:** band from the baseline up to the 10-dot level (sqrt scale, y≈233.5), fill yellow `#c98500` alpha 0.12, dashed (4/3) 1.5px yellow top edge; right-aligned 11px yellow label above the edge: "too few to say “person” (schematic)".
- **Caption (italic 12px `#2c3e50`, bottom center, y=304):** "Illustrative counts — real numbers vary by unit and beam pattern. The far bars are honest; there are just too few of them."

**Payload note (below canvas, italic, left-aligned):** Sample payload — illustrative structure, not real captured data.

**Payload block (monospace, left border `#1a5276`):**

```
// Lidar data formats vary by maker and are not
// published. Whole block is reconstruction; field
// names generic.
// ── inferred / plausible ──
{
  "frame_id": "sweep-004417",
  "ts":       "2026-08-22T17:03:41.120Z",
  "points_this_frame": 214856,
  "weather_flag":      "clear",

  // three of ~200k dots: direction, distance, echo strength
  "sample_points": [
    { "azimuth_deg": 12.4, "elev_deg": -1.2, "range_m":  8.31, "reflect": 0.62 },
    { "azimuth_deg": 12.6, "elev_deg": -1.2, "range_m":  8.29, "reflect": 0.60 },
    { "azimuth_deg": 87.1, "elev_deg":  0.4, "range_m": 42.77, "reflect": 0.08 }
  ],

  // decided downstream, not measured by the unit
  "retained_for_mapping": true
}
```

## Section 3: Why is it collected?

**Label (purpose pill):** Stated purpose

- **Driving** — measuring where everything is so the car can plan a path around it
- **Cross-checking the cameras** — the measured distances confirm or correct what cameras and radio-wave sensors report, merged into one picture of the world (sensor fusion)

**Label (effect pill):** Additional consequence

- **A 3-D scan of public space** — recorded drives capture streets, yards, and passers-by in measurable detail; on some systems saved scans are kept for map-building and for training the driving software

**Key point — Weather scatters the pulses:** rain, fog, snow, and even exhaust plumes bounce some pulses back early, planting false dots near the car; very dark surfaces return echoes too weak to register. How badly varies by unit and conditions.

### Visualization (canvas `c3`, 720×320)

Two panels, same short scene: a sensor facing a parked van. Clear weather returns a solid column of echoes; rain thins the column and adds false dots between sensor and van.

- **Title (bold 14px `#1a5276`, top center, y=22):** "Same scene, clear vs rain".
- **Panels:** left x=55–345, right x=375–665; 1px `#e5e9ef` vertical divider at x=360 from y=40 to y=260; ground — 2px `#2c3e50` line at y=252 across each panel.
- **Panel titles (bold 12px `#2c3e50`, centered, y=52):** "clear day" at x=200, "rain" at x=520.
- **Sensor:** violet `#4a3aa7` rounded rect 16×12, radius 2, at (60,196) left panel and (380,196) right panel.
- **Van (target):** rect 26×102 at (300,150) left and (620,150) right; fill `#e5e9ef`, 1.5px `#6b7280` stroke.
- **Pulse lines:** 1px blue alpha 0.18 from sensor right edge — (76,202) left, (396,202) right — to each echo dot on that panel's van face.
- **Clear-panel echoes (aqua `#199e70`, radius 2.5):** 12 dots on the van's left face x=300, y from 156 to 244 in steps of 8.
- **Rain-panel echoes (aqua, radius 2.5):** 5 dots on the van face x=620 at y = 160, 180, 200, 220, 240.
- **False early bounces (orange `#d95926`, radius 2.5, hardcoded):** (430,190), (450,210), (470,175), (468,235), (505,222), (520,195), (555,205), (575,182).
- **Rain streaks:** 1px blue alpha 0.15 slanted lines (each from (x,y) to (x−4,y+12)) at (415,80), (445,110), (475,70), (500,130), (530,90), (560,120), (590,75), (610,105), (640,85), (460,150), (540,60), (600,140).
- **Panel labels (11px):** mute "solid column of echoes" centered at (185,140) left panel; orange "false early bounces" centered at (495,158) right panel; mute "fewer real echoes" centered at (633,140) right panel.
- **Caption (italic 12px `#2c3e50`, bottom center, y=304):** "Raindrops bounce some pulses back early, planting dots where nothing is. Schematic — severity varies by unit and conditions."

## Regeneration instructions

- **Layout:** tracking-page `.obj-table`: full-width `border-collapse: collapse` table, one `<tr>` per section; left `<td>` (45%) holds `.obj-title` + optional `.lede` + bullets + `.key-point` callouts + `.lbl` pills, right `<td>` (55%, `text-align: center`) holds the canvas — and in the "What does it collect?" row, the `.payload-note` and `.payload` `<pre>` below the canvas (both left-aligned).
- **Page style:** body system sans-serif (-apple-system stack), white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; table cell borders `1px solid #2980b9`, padding 16px; `.obj-title` bold 1.1em `#1a5276`; `li` 0.93em with `li b` in `#1a5276` weight 600. No nav bar, no back/home links.
- **Callouts:** `.key-point` — background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px 14px, 0.93em; leading `<strong>` in `#1a5276`.
- **Pills:** `.lbl` inline-block uppercase 0.7em bold, padding 2px 7px, radius 3px; `.lbl-purpose` background `#eaf2fb` color `#1a5276`; `.lbl-effect` background `#fdf0e6` color `#a8501c`.
- **Payload:** `.payload` — background `#f8f9fa`, left border `3px solid #1a5276`, ui-monospace 0.78em, `white-space: pre`; `.payload-note` italic 0.82em `#666` immediately above.
- **Canvas:** each declares intrinsic `width="720" height="320"`; a shared `setupCanvas(id)` reads those attributes, sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Shared helpers: `tint(hex, alpha)` for translucent fills and `rr()` rounded-rect path.
- **Palette (tracking pages):** categorical tokens blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`; ink `#1a5276` (headings/axes only), text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Red is reserved for genuine alarm states and not used here. (Site-wide palette reference: #1a5276 primary blue, #27ae60 green, #e74c3c red, #e67e22 orange.)
- In regenerated HTML, any card links use `.html` extensions.
