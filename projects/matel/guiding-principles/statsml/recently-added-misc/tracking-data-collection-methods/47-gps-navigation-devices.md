# Tracking Data: GPS Navigation Devices

**Page type:** detail page (two-column obj-table layout: text left 45%, canvas right 55%, three rows: What is it? / What does it collect? / Why is it collected?)
**HTML title tag:** Tracking Data: GPS Navigation Devices

**Subtitle:** A dashboard sat-nav computes its own position from satellite broadcasts it only receives. The movement record begins when the live-traffic connection reports that position back.

## What is it?

Lede: A navigator that works out where it is by listening, and a traffic service that asks it to report back.

- **The device:** a windshield or built-in sat-nav — TomTom and Garmin are the familiar kind
- **The satellite side is receive-only:** satellites broadcast time signals; the receiver compares arrival times and computes its own position
- **No satellite knows the car exists** — nothing is transmitted upward, so navigation alone leaves no record anywhere
- **The uplink:** a connected navigator (built-in SIM or a paired phone) reports position and speed to the traffic service

**Two different machines in one box:** the receiver that computes the position and the modem that reports it. Every question about tracking is a question about the second one.

### Visualization (canvas `c1`, 720×320)

Chain flow diagram for one probe report: four party boxes joined by labeled arrows, with a dashed "nothing travels up" annotation under the first hop.

- **Title** (bold 16px ink `#1a5276`, centered, y=22): "One-way down, then one hop back up".
- **Four boxes** (128×76, y=66, tinted fills at 0.16 alpha, 1.5px strokes, bold 15px title in the party hue, two 13px `#2c3e50` lines), one hue each in SERIES order:
  - x=26, blue `#2a78d6`: "GPS satellites" / "broadcast time signals" / "receive nothing"
  - x=194, green `#008300`: "Navigator" / "computes ITS OWN fix" / "lat, lon, speed"
  - x=362, violet `#4a3aa7`: "Uplink" / "SIM or paired phone" / "optional — the switch"
  - x=530, orange `#d95926`: "Traffic service" / "collects probe reports" / "builds the speed map"
- **Arrows** between boxes at mid-height, each drawn in the hue of the party it arrives at, 13px label above the shaft: "time signals" (green), "fix + speed" (violet), "probe report" (orange).
- **No-uplink annotation:** dashed mute `#6b7280` bracket (dash 4/4) running below the first two boxes from under the Navigator box back to under the satellites box, 13px mute caption "no signal travels up — the satellites never learn who is listening". Neutral ink: a structural fact, not an error.
- **Bottom caption** (mute 13px, centered, y=228): "Schematic. Listening creates no record; the report on the right-hand hop does."

## What does it collect?

- **Position, speed and heading** at a reporting interval, each with a timestamp — one probe report
- **Destination entries** — searches and the stored home address live on the device or its account
- **A session identifier** keys the reports, so consecutive fixes chain into a track
- **A standalone unit with no connection** collects nothing beyond its own screen — the map is on the device

**Pseudonymous is not anonymous here:** no report carries a name, but a chain of fixes is a route, and where the chain starts and ends each day is close to an address.

### Visualization (canvas `c2`, 720×320)

Schematic route made of individual probe fixes chaining into a journey, endpoints emphasized.

- **Title** (bold 16px ink `#1a5276`, centered, y=22): "Reports with one session id chain into a journey".
- **Route polyline:** ~14 hardcoded fix points from lower-left (≈70, 232) to upper-right (≈648, 118) with two bends; 1.5px blue `#2a78d6` connecting line, 4px-radius blue dots at each fix.
- **Fix annotation:** one mid-route dot labeled (13px mute, above): "one report every 30 s, same session id".
- **Endpoints:** first and last fixes drawn as 8px-radius orange `#d95926` rings (not filled), each with bold 13px orange two-line label: "first fix 08:02" / "a home street" (below-left of start), "last fix 08:41" / "an office car park" (above-right of end, clamped inside canvas).
- **Bottom captions (centered):** text `#2c3e50` italic 12px "No name appears in any report — the endpoints carry it anyway."; mute 11px italic "Illustrative route — the chaining is the mechanism, not a recorded journey."

### Payload (below canvas `c2`)

Caption (`.payload-note`, italic): "Sample payload — illustrative structure, not real captured data."

```
// One probe report as the traffic service receives it.
// The position fields follow public receiver-output specs
// (NMEA sentences); the upload wrapper is vendor-specific.
{
  // ── documented in public spec (receiver output) ──
  "utc":         "2026-08-22T08:14:31Z",
  "lat":         "<computed by the receiver>",
  "lon":         "<computed by the receiver>",
  "speed_kmh":   47.0,
  "heading_deg": 213,
  "hdop":        0.9,          // fix quality
  "satellites":  9,
  // ── inferred / plausible (upload wrapper) ──
  "session_id":  "prb_51ab…",  // chains reports into a track
  "segment":     "<matched road segment>",
  "interval_s":  30
}
```

## Why is it collected?

**Stated purpose** (label pill `.lbl-purpose`)

- **Turn-by-turn navigation** — the position is computed for the driver's own screen
- **Live traffic:** the service knows a road is slow because its users on that road are reporting their speed — every user is both consumer and sensor

**Additional consequence** (label pill `.lbl-effect`)

- **Aggregated speed data can be licensed onward** — TomTom's licensing of aggregate speed data, later used by Dutch police to site speed checks, was documented in 2011, and the company changed its terms afterwards
- **The record follows the device, not a driver** — whoever borrows the car drives inside the same session history

**Roads without users look clear:** the traffic layer measures its own fleet, so a jam on a road none of its users are driving does not appear in the data — usually the map falls back to historical averages there.

### Visualization (canvas `c3`, 720×320)

Two schematic road bars contrasting a measured jam with an unmeasured one.

- **Title** (bold 16px ink `#1a5276`, centered, y=24): "The traffic layer measures its own fleet". Subtitle (12px mute, y=42): "two congested roads at the same moment — only one has probe cars on it".
- **Road bars:** x from 60, width 600, height 34; bold 13px road name above-left of each bar, 12px count line right-aligned above each bar.
  - **Main road** (y=92): orange `#d95926` tinted fill (0.16) and 1.5px stroke; 14 blue `#2a78d6` probe dots (4px radius) spaced unevenly along the bar; count line (orange, bold): "14 probe cars — measured: slow (avg 12 km/h)".
  - **Side street** (y=186): grid `#e5e9ef` fill with mute `#6b7280` 1.5px dashed stroke (dash 4/4); no dots; count line (mute, bold): "0 probe cars — no measurement".
  - Under the side street bar, 12px mute line (centered under bar): "shown from historical averages, not the jam happening now".
- **Bottom captions (centered):** text `#2c3e50` italic 12px "A jam with no probes in it is invisible to the map."; mute 11px italic "Illustrative — coverage follows where the service's own users drive."

## Regeneration instructions

- **Layout:** tracking-page `.obj-table`: full-width bordered table, one `<tr>` per section; left `<td>` (45%) holds `.obj-title` + `.lede` + bullets + `.key-point` callouts + `.lbl` pills, right `<td>` (55%, centered) holds the canvas; the "What does it collect?" row also carries the `.payload-note` caption and `.payload` pre block under its canvas (both left-aligned). HTML-escape the angle-bracket placeholders in the payload (`&lt;…&gt;`).
- **Page style:** body system sans-serif (-apple-system stack), white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; subtitle `#666` 1.05em; table cell borders `1px solid #2980b9`, padding 16px; `.obj-title` bold `#1a5276` 1.1em; `li b` `#1a5276` weight 600, li 0.93em. No nav bar, no back/home links.
- **Callouts:** `.key-point` — background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px 14px, 0.93em, leading `<strong>` in `#1a5276`.
- **Label pills:** `.lbl` inline-block uppercase 0.7em bold, padding 2px 7px, radius 3px; `.lbl-purpose` background `#eaf2fb` color `#1a5276`; `.lbl-effect` background `#fdf0e6` color `#a8501c`.
- **Payload:** `.payload` — background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px, ui-monospace 0.78em, `white-space: pre`, line-height 1.45; `.payload-note` 0.82em `#666` italic.
- **Canvas:** each declares intrinsic `width="720" height="320"`; a shared `setupCanvas(id)` reads the element's attributes, sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Helpers: `tint(hex, a)` for translucent fills from palette tokens, `rr()` rounded-rect, `arrow()` for labeled horizontal arrows. Charts hardcode literal data arrays (no Math.random).
- **Palette (declared once as tokens):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`; ink `#1a5276` (headings/axes only), text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Red is reserved for genuine alarm states and is not used on this page. Site-wide accent palette: #1a5276 primary blue, #27ae60 green, #e74c3c red, #e67e22 orange.
- In regenerated HTML, any card links use `.html` extensions (this page has none).
