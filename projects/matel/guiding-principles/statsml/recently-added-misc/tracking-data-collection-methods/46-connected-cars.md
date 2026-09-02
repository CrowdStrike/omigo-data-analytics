# Tracking Data: Connected Cars

**Page type:** detail page (two-column obj-table layout: text left 45%, canvas right 55%, three rows: What is it? / What does it collect? / Why is it collected?)
**HTML title tag:** Tracking Data: Connected Cars

**Subtitle:** A connected vehicle streams sensor readings already present on its internal bus. The record describes a vehicle; attributing it to a driver is an added assumption.

## What is it?

Lede: Signals the car already computes, forwarded off the vehicle.

- **Already internal:** engine, braking and steering controllers exchange readings over a bus so the car can function
- **Why they exist:** anti-lock braking needs wheel speed, stability control needs steering angle and yaw
- **The path out:** a cellular modem is the main uplink; some cars also upload over home Wi‑Fi when parked, and satellite supplies position only
- **Keyed to the vehicle:** a VIN identifies the car, and no person appears unless something else supplies one

**Two arrangements get conflated:** manufacturer telemetry over the built-in modem, and a separate insurance program where a driver opts into an app or plug-in device for a rate based on driving. Similar signals, different holders and different agreements — which changes what the data may be used for.

### Visualization (canvas `c1`, 720×360)

Recognizable side-view car (facing left) drawn X-ray style, with each of the six signals anchored to the physical part that measures it — a colored dot on the part, a dashed leader line, and a label in the same hue.

- **Header strip:** tinted ink band `rgba(26,82,118,0.07)` full width, 28px tall; bold 15px ink `#1a5276` centered title "Where each signal is measured on the car".
- **Ground line:** 1px `#e5e9ef` at y=276, x 40→680.
- **Car silhouette:** closed bezier path — front bumper (155,252) up to (152,216), hood curve to (232,192), windshield slope to (288,154) then roof corner (334,142), flat roof to (406,142), rear glass curve to (456,182), trunk (510,198), rear (515,224) rounded down to (502,252), rear wheel arch = arc center (462,252) r32, sill to (295,252), front wheel arch = arc center (263,252) r32, back to start. Fill 0.08-alpha ink tint, 2px ink stroke.
- **Wheels:** circles r24 at (263,252) and (462,252), fill 0.55-alpha ink tint, ink stroke; hub circle r8 fill `#e5e9ef`.
- **Glass (X-ray):** front window polygon (292,158)(334,150)(338,186)(300,190), rear window (348,150)(402,148)(446,182)(352,186), fill 0.12-alpha ink tint; door seam line (344,190)→(344,248) in 0.3-alpha ink.
- **Shark-fin antenna:** small triangle (416,142)(430,126)(433,142), ink fill.
- **Six markers** (5px dot in the hue, dashed 1.5px leader dash 3/3, 14px label in the same hue):
  - **Position** — blue `#2a78d6`: dot on the fin (427,131), vertical leader up to (427,74), label centered (427,66) "Position (roof antenna)".
  - **Seatbelt state** — violet `#4a3aa7`: belt drawn as violet 2.5px diagonal (354,162)→(374,200) across a schematic seat (back + cushion in 0.35-alpha ink), dot (364,178), vertical leader to (364,100), label centered (364,92).
  - **Steering angle** — green `#008300`: steering wheel circle r8 stroke green at (306,176), diagonal leader to (170,118), label right-aligned (162,116).
  - **Road speed** — orange `#d95926`: dot on the front wheel hub (263,252), diagonal leader to (140,300), label right-aligned (134,304).
  - **Brake / accelerator** — magenta `#d55181`: two pedal strokes (306,226)→(300,238) and (315,226)→(309,238), vertical leader from (308,240) to (308,296), label centered (308,308).
  - **Paired phone** — aqua `#199e70`: small rounded rect (322,196) 10×17 aqua fill, diagonal leader from (327,214) to (470,296), label centered (480,308).
- **Footer band:** tinted ink band `rgba(26,82,118,0.06)`, 34px tall at the bottom; 15px `#2c3e50` centered text "Signals the vehicle already computes to operate. Each is keyed to the car, not a driver."

### Visualization (canvas `c1b`, 720×300, below `c1` in the same cell)

Scene showing how readings leave the vehicle: the car at center-left, with four connection paths of different reach — each path in its own hue with its own line style and arrow direction.

- **Header strip:** tinted ink band, 28px; bold 15px ink centered title "How readings leave the car".
- **Ground line:** 1px `#e5e9ef` at y=240, x 30→710.
- **Main car:** the c1 silhouette path reused via a `drawCar(tx, ty, s)` helper at translate (114,102) scale 0.5 — spans x≈190–372, ground 240, fin dot at ≈(328,167).
- **Second car** (car-to-car peer): same helper at translate (347,130) scale 0.35, body filled at lower alpha — spans x≈400–527.
- **Satellite** — blue `#2a78d6`: icon at ≈(204,66) (body rect + two solar panels), label centered (204,48) 12px "satellite positioning — receive-only"; dashed blue arrow from (214,84) **one-way down** to the main car's fin (322,160).
- **Cellular** — orange `#d95926`: lattice mast at x=585 (apex y=110, feet (575,240)/(595,240), cross braces, arcs at the top); **two-way** solid orange curve from car roof (300,170) via control point (450,95) to (578,115), arrowheads both ends; label centered (460,95) 12px "cellular — primary uplink"; solid orange **one-way** line (595,145)→(628,165) into the server rack.
- **Backend rack** — ink: three stacked rects (630,150)(630,176)(630,202) each 60×22, ink stroke + 0.08-alpha ink fill, two indicator dots each; label centered (660,252) 12px mute "backend servers".
- **Home Wi‑Fi** — aqua `#199e70`: house at left (box (70,196) 52×44, roof (62,196)(96,168)(130,196)), three aqua arcs above the roof peak; dashed aqua **one-way** arrow from car front (192,210) to house (132,202); label centered (150,142) 12px, two lines "home Wi‑Fi" / "(when parked, some cars)".
- **Car-to-car** — violet `#4a3aa7`: short **two-way** violet arrow between the cars at y=205 (x 374→398); label centered (462,258) 12px "car-to-car — stays local".
- **Footer band:** tinted ink band, 34px; 14px `#2c3e50` centered text "Whichever path it takes, the record arrives keyed to the VIN. Car-to-car messages never reach a backend."

## What does it collect?

- **Position** and a positional accuracy estimate, sampled along a trip
- **Road speed**
- **Accelerator position** and brake state
- **Steering angle**
- **Seatbelt state** per seat
- **Trip start and end** times
- **Derived:** a trip summary with threshold-based event counts
- **Paired phone identifiers**, where a phone was connected
- **Driver-monitoring camera output** where fitted — usually a derived attention state rather than stored video

**Stream and summary are not interchangeable:** a per-second series answers questions the summary cannot, and the summary embeds thresholds — "hard brake" is a cutoff someone chose, so the same trip yields a different count under a different cutoff.

**`driver_id` is null and stays null:** the sensors observe a vehicle. Attributing the behaviour to a person requires an assumption the car never verified.

### Visualization (canvas `c2`, 720×320)

Route trail with three kinds of data points along it, each with an accuracy halo.

- **Header strip:** tinted ink band, 28px; bold 15px ink centered title "A trip record mixes three kinds of point".
- **Route:** smooth quadratic curve from (60,208) through control points (150,68)→(250,148), (350,228)→(450,128), (550,48)→(660,108); drawn twice — 5px `#e5e9ef` under-stroke, then 2px 0.45-alpha ink stroke. The route itself is the neutral field, not a series.
- **Point kinds** (hue + legend text): periodic speed sample = blue `#2a78d6` "periodic speed sample"; threshold event the car flagged = orange `#d95926` "threshold event the car flagged"; dwell / trip boundary = green `#008300` "dwell / trip boundary".
- **Points** (13px-radius 0.16-alpha halo = reported positional accuracy, 6px solid dot in the kind's hue, 14px `#2c3e50` label 18px above):
  - (100,168) "45 mph" — sample
  - (200,118) "brake event" — event
  - (310,183) "72 mph" — sample
  - (420,148) "stopped 4 min" — boundary
  - (550,78) "65 mph" — sample
  - (640,103) "trip end" — boundary
- **Legend** (12px dot-swatch row at y≈254, starting x=62) listing the three kinds.
- **Footer band:** tinted ink band, 34px; 14px `#2c3e50` centered text "Halo = reported positional accuracy. Near a junction the road match can be wrong."

### Payload (below canvas `c2`)

Caption (`.payload-note`, italic): "Sample payload — illustrative structure, not real captured data."

```
// Reconstruction. Signal names are generic; the VIN
// format is a published standard, the schema is not.
{
  // ── inferred / plausible ──
  "vin":         "1XX…",        // identifies the vehicle
  "trip_id":     "tr_4402",
  "sample_hz":   1,             // stream rate for the block below
  "samples": [
    { "t": "2026-08-22T08:14:00Z", "speed_kph": 51.2,
      "accel_pedal_pct": 18, "brake": false,
      "steering_deg": -2.1, "seatbelt_driver": true,
      "lat": 30.2711, "lon": -97.7437, "gps_acc_m": 8 },
    { "t": "2026-08-22T08:14:01Z", "speed_kph": 46.8,
      "accel_pedal_pct": 0,  "brake": true,
      "steering_deg": -1.4, "seatbelt_driver": true,
      "lat": 30.2710, "lon": -97.7434, "gps_acc_m": 12 }
  ],
  "trip_summary": { "hard_brake_events": 2, "max_speed_kph": 63,
                    "distance_km": 11.4, "driver_id": null }
}
```

## Why is it collected?

**Stated purpose** (label pill `.lbl-purpose`)

- **Vehicle function** — stability control that could not read steering angle would not work
- **Safety and service** — crash notification, remote diagnostics, breakdown prediction

**Additional consequence** (label pill `.lbl-effect`)

- **The same series is a behavioural record** — speed against position compares to the posted limit, and braking traces a driving-style score
- **Trip times reveal a routine**; these are risk-pricing inputs, and computing them needs no sensor the car lacks

**The unit-of-observation error in its clearest form:** the measurement unit is a car, the decision unit is a person, and nothing bridges them. A score computed per VIN and applied to one policyholder assumes an identity the vehicle never verified — seatbelt state indicates an occupied seat, not who occupies it.

### Visualization (canvas `c3`, 720×320)

Scatter of per-trip hard-brake rates for one household car, coloured by which of two people drove — a field the record does not contain — with the single per-VIN average line.

- **Title** (bold 14px ink, centered at x = w/2−60, y=26): "Hard-brake rate per trip, one household car". Subtitle (12px mute, y=44): "every point carries the same VIN".
- **Data (illustrative, 16 trips in order; v = events per 100 km, d = driver):** (0.9, A), (4.4, B), (0.6, A), (1.1, A), (5.2, B), (0.8, A), (4.0, B), (1.3, A), (4.8, B), (0.5, A), (3.9, B), (1.0, A), (5.5, B), (0.7, A), (4.6, B), (1.2, A). Driver A mean = 0.9; driver B mean = 4.6; overall per-VIN mean = 2.5 (computed from the arrays and printed to 1 decimal).
- **Axes:** y from 0 to 6 with gridlines `#e5e9ef` and labels 0/2/4/6 (11px); rotated y title "events per 100 km" (12px mute); x label "trips, in order"; plot area padding left 62, right 210 (key sits at right), top 66, baseline y=226.
- **Trip points:** 4.5px-radius dots, aqua `#199e70` for driver A, violet `#4a3aa7` for driver B, evenly spaced across the plot in trip order.
- **Average line:** solid orange `#d95926` 2px horizontal line at the overall mean.
- **Right-hand key** (x = plot right + 22): bold orange 13px "scored per VIN: 2.5" beside the line, then 12px `#2c3e50` "the only figure the data" / "supports"; bold violet "one driver: 4.6" near the top; bold aqua "the other: 0.9" near the baseline; italic mute 12px "the two colours are not" / "a field in the record".
- **Bottom captions (centered, italic):** `#2c3e50` 12px "The per-VIN figure sits between two people and describes neither of them."; mute 11px "Illustrative trips — the shape, not a measured vehicle."

## Regeneration instructions

- **Layout:** tracking-page `.obj-table`: full-width bordered table, one `<tr>` per section; left `<td>` (45%) holds `.obj-title` + `.lede` + bullets + `.key-point` callouts + `.lbl` pills, right `<td>` (55%, centered) holds the canvas; the "What does it collect?" row also carries the `.payload-note` caption and `.payload` pre block under its canvas (both left-aligned).
- **Page style:** body system sans-serif (-apple-system stack), white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; subtitle `#666` 1.05em; table cell borders `1px solid #2980b9`, padding 16px; `.obj-title` bold `#1a5276` 1.1em; `li b` `#1a5276` weight 600, li 0.93em. No nav bar, no back/home links.
- **Callouts:** `.key-point` — background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px 14px, 0.93em, leading `<strong>` in `#1a5276`.
- **Label pills:** `.lbl` inline-block uppercase 0.7em bold, padding 2px 7px, radius 3px; `.lbl-purpose` background `#eaf2fb` color `#1a5276`; `.lbl-effect` background `#fdf0e6` color `#a8501c`.
- **Payload:** `.payload` — background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px, ui-monospace 0.78em, `white-space: pre`, line-height 1.45; `.payload-note` 0.82em `#666` italic.
- **Canvas:** each declares its intrinsic size in `width`/`height` attributes (c1 720×360, c1b 720×300, c2 and c3 720×320); a shared `setupCanvas(id)` reads the element's attributes, sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Helpers: `tint(hex, a)` for translucent fills from palette tokens, `rr()` rounded-rect; c1/c1b share a `carPath(ctx)` silhouette and an `arrowHead()` helper. Charts hardcode literal data arrays (no Math.random); C3's three means are computed from the hardcoded trip array at draw time.
- **Palette (declared once as tokens):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`; ink `#1a5276` (headings/axes only), text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Red is reserved for genuine alarm states and is not used on this page. Site-wide accent palette: #1a5276 primary blue, #27ae60 green, #e74c3c red, #e67e22 orange.
- In regenerated HTML, any card links use `.html` extensions (this page has none).
