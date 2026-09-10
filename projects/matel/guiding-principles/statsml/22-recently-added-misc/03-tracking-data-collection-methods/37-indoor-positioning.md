# Tracking Data: Indoor Positioning

**Page type:** detail page (two-column obj-table layout: text left 45%, canvas right 55%, one row per section)
**HTML title tag:** Tracking Data: Indoor Positioning

**Subtitle:** Indoors, a phone works out where it is by asking a service rather than by computing the answer itself — so the positioning step becomes a network request, and leaves a record.

## Section 1: What is it?

Lede: Where satellites cannot reach, something else has to supply the coordinate.

- **Satellites need sky** — indoors, in a tunnel, in a parking structure, or between tall buildings a receiver may see too few to solve for a position
- **A phone reporting a position indoors** is using one of the methods below
- **Not interchangeable** — they measure different physical quantities and fail in different ways
- **The distinction that matters here:** whether producing the position requires talking to anything

Embedded method table (`.map-table`, in the left column below the bullets):

| Method | What it measures | Typical setting | Does positioning itself create a record elsewhere? |
|--------|------------------|-----------------|-----------------------------------------------------|
| WiFi positioning | Observed access point identifiers and signal strengths, sent to a lookup service | Indoors, urban | Yes — a lookup request |
| Cell tower / network-based | Serving cell and neighbour measurements; the network knows the serving cell inherently | Anywhere with coverage | Yes — inherent to the network, independent of any app |
| Bluetooth beacons | Proximity to fixed short-range transmitters | Retail, venues, airports | Depends on whether the app reports the sighting |
| Inertial dead reckoning | Accelerometer and gyroscope integrated from a known starting point | Tunnels, indoors, short gaps | No — computed on device |
| Barometric pressure | Pressure converted to a relative altitude, used for floor level | Multi-storey buildings | No, but needs a reference pressure |
| Ultra-wideband | Time-of-flight ranging to fixed anchors | Warehouses, industrial, some venues | Yes, at the anchor infrastructure |
| Magnetic field mapping | Local distortions from building steel matched to a survey | Indoors | Depends on where the matching runs |
| Visual / camera-based | Camera frames matched to a feature map of the surroundings | Indoors, AR | Yes if the matching is server-side |

Key point callout: **Outdoor and indoor invert the direction:** an outdoor fix is computed on the device — it solves for its own position, and no counterparty is involved or informed. Most indoor methods send what was observed to a service holding the reference data, and the position comes back as a reply. The privacy property people attribute to "location" is really a property of one method, and it is the method that stops working indoors, where people spend most of their time.

### Visualization (canvas `c1`, 720×360)

Three-row flow diagram: "listening vs. asking", then the same distinction applied to every method in the row's table. Hue scheme held constant: green = estimate stays on device, blue = the observation that goes out, violet = the reply that comes back, orange = where a record lands.

- **Title (bold 16px, `#1a5276` ink, centered at (360, 20)):** "Obtaining a coordinate: by listening, or by asking".
- **Row A (outdoor, receive-only):** green (`#008300`) filled circle at (52, 62) r=13 with two short green horizontal strokes either side (a signal glyph), gray (`#6b7280`) caption "outdoor signal" below at (52, 92). Solid green arrow from (84, 66) to (158, 76) labeled "signals in" in gray at (120, 60). Box at (162, 52) 128×50, green hue, 15% alpha fill and green stroke, bold line "Device" over "solves its own fix". Dashed gray arrow (296, 77)→(392, 77) with a bold gray "✕" at (344, 68). Right text: bold green 15px "nothing is sent" at (404, 68); gray 13px "no counterparty is involved, so no" / "record of the fix exists off the device" at (404, 84)/(404, 98).
- **Divider:** dashed `#d5dbdb` line at y=118 from x=20 to x=700.
- **Row B (lookup-based):** blue-hued box at (30, 145) 118×50 "Device" / "observes IDs only". Solid blue (`#2a78d6`) arrow (152, 158)→(300, 158) labeled above in blue "observed identifiers + signal strengths" (centered at (226, 149)). Solid violet (`#4a3aa7`) arrow (300, 186)→(152, 186) labeled below in violet "a coordinate comes back" (at (226, 202)). Orange-hued box at (304, 143) 128×54 "Lookup service" / "holds the reference". Solid orange (`#d95926`) arrow (438, 170)→(486, 170); right text: bold orange 15px "the request is itself" / "a record" at (494, 162)/(494, 178); gray 13px "held where the reference data is" at (494, 195).
- **Divider:** dashed `#d5dbdb` line at y=218 from x=20 to x=700.
- **Row C (sorted groups):** ink-colored bold 14px heading "Sorting the methods in the table by that same question" centered at (360, 240). Three columns (width 214, gap 14, starting x=24, header strip at y=252 height 20 in group hue with white bold 13px text; items as 214×19 outlined chips with 13% alpha fill at 22px vertical steps from y=278):
  - Orange `#d95926`, header "Record lands elsewhere": WiFi positioning, Cell tower / network, Ultra-wideband anchors
  - Violet `#4a3aa7`, header "Depends on the deployment": Bluetooth beacons, Magnetic field mapping, Visual / camera-based
  - Green `#008300`, header "Stays on the device": Inertial dead reckoning, Barometric pressure
- **Caption (gray 13px, centered at (360, 350)):** "Schematic — the direction of the arrows is the whole difference. The grouping is the table’s last column."

## Section 2: What does it collect?

- **Access point identifiers** with a signal strength each — the observation, not the position
- **Serving and neighbour cells**, which the operator has by virtue of the device being registered
- **Accelerometer and gyroscope** streams, integrated on device
- **Pressure**, plus whatever reference is available to turn it into an altitude
- **Ranging timestamps** to fixed anchors, magnetometer readings, or camera feature descriptors
- **Output side:** one coordinate, one accuracy estimate, and derived labels such as floor and indoor/outdoor

Key point callout: **One output over unequal inputs:** a remote lookup, a cell identifier, an integrated inertial track and a pressure reading with no reference are not comparable in quality. Read *secs_since_absolute_fix* first — it says how long the estimate has been coasting, and so how much of the coordinate is measurement and how much extrapolation.

Key point callout: **An assumption travels into the floor label:** pressure maps to altitude only relative to a reference pressure, and ambient pressure moves with the weather — over a day it can move by more than the difference between adjacent floors. An uncalibrated altitude drifts for reasons unrelated to the building, and the floor label inherits the drift. A relative altitude escapes this only if ambient pressure has not shifted since the reading it is relative to.

### Visualization (canvas `c2`, 720×320)

Fusion flow diagram: four unequal input boxes feeding a filter shape that emits one output box.

- **Title (bold 16px blue `#2a78d6`, centered at (360, 20)):** "One reported coordinate, several unequal inputs".
- **Input boxes** (190×38 at x=24, starting y=40, 10px gaps; fill `rgba(42,120,214,ALPHA)` with blue stroke; bold 14px blue name, 13px gray note):
  - "WiFi lookup" / "remote reference" — alpha 0.34
  - "Cell measurements" / "coarse, always there" — alpha 0.22
  - "Inertial track" / "error grows with time" — alpha 0.12
  - "Barometer" / "needs a reference" — alpha 0.08
- **Arrows:** solid blue from the right edge of each box (x=218) converging to (300, 118).
- **Filter block:** trapezoid path (306,78)→(392,96)→(392,140)→(306,158), fill `#f8f9fa`, blue stroke width 2; bold 15px blue "filter" at (349, 114), 13px "weights + state" at (349, 130).
- **Output:** blue arrow (396, 118)→(466, 118) into box at (472, 88) 216×62, fill `rgba(42,120,214,0.35)`, blue stroke; bold 15px blue "lat, lon  +  accuracy" at (580, 112); gray 13px "the accuracy value is the filter’s" / "own estimate, not an independent check" at (580, 128)/(580, 142).
- **Caption (gray 13px, centered at (360, 224)):** "Illustrative — shading suggests relative weight, not measured values."

Payload note (right column, under the canvas): *Sample payload — illustrative structure, not real captured data.*

Payload block (monospace `.payload`):

```
// Schemas vary by platform and none of this is a public schema,
// so the whole block is ── inferred / plausible ──
{
  "lat": 47.6205, "lon": -122.3493,   // ← the single output
  "accuracy_est": 38,                 // filter's own uncertainty
  "sources": ["wifi", "cell", "inertial", "baro"],

  // ── inputs, of very different quality ──
  "wifi":     { "aps_observed": 9, "rssi_span": [-92, -54],
                "lookup": "remote" },
  "cell":     { "serving_cell": "…4471", "neighbours": 3 },
  "inertial": { "integrated": true,
                "secs_since_absolute_fix": 214 },
  "baro":     { "relative_altitude": 11.4,
                "reference_pressure": null },

  // ── derived labels, not measured ──
  "floor_estimate": 4,
  "environment": "indoor",
  "confidence": 0.61
}
```

## Section 3: Why is it collected?

Label (`.lbl-purpose`): STATED PURPOSE

- **Continuity** — a map that blanks out on entering a building is not usable
- **Questions an outdoor fix cannot answer** — which floor, which aisle, whether the device is indoors at all

Label (`.lbl-effect`): ADDITIONAL CONSEQUENCE

- The lookup that produces the position **is a request, and a request is a record** — held by the service with the reference data, whether or not the app stores the answer
- For the network-based case **there is no app in the loop** — that is how a registered device is reached

Key point callout: **A gap is not reported as a gap:** a blank map is worse than a stale estimate, so the stack keeps emitting a coordinate while coasting on motion. Stored, those filled-in points look like observed ones, and dwell computed over them counts filter output as time spent.

### Visualization (canvas `c3`, 720×320)

Sawtooth chart (seconds since last absolute fix over one walk) plus a stacked dwell bar showing observed vs interpolated time.

- **Title (bold 14px ink `#1a5276`, centered):** "How long the coordinate has been coasting"; subtitle (12px gray, centered at y=42): "resets to zero whenever a lookup returns; rises whenever nothing new comes back".
- **Data:** fix times (seconds) `[0, 12, 26, 40, 52, 62, 136, 150, 164, 178]` over TMAX=180 s; the sawtooth value is seconds since the last fix, resetting to 0 at each fix time; y scale 0–80 s mapped to plot area top=62, base=178; x padding padL=54, padR=22.
- **Gap highlight:** orange tint `rgba(217,89,38,0.12)` rectangle spanning t=62 to t=136 from y=top−4 to base.
- **Sawtooth line:** blue `#2a78d6`, width 2; baseline in grid gray `#e5e9ef`; blue 3px-radius dots on the baseline at each fix time.
- **Y-axis label (rotated, gray 11px):** "seconds since a lookup".
- **Annotations:** bold orange 12px "74 s with no lookup" centered over the gap at y=top+6; gray 11px "a coordinate is still reported throughout" centered under the gap at base+15; gray 11px "lookups" under x(26) at base+15.
- **Dwell bar:** at bx=padL+130, y=216, width 386, height 26. Left segment 22/96 of the width in blue tint `rgba(42,120,214,0.34)` with blue stroke; right segment in orange tint `rgba(217,89,38,0.34)` with orange stroke. Left-of-bar right-aligned 12px labels "Dwell stored" / "for one zone"; bold 13px ink "96 s" to the right of the bar. Below the bar, centered 11px labels: blue "22 s observed" under the left segment, orange "74 s from integrated motion" under the right.
- **Captions (centered, bottom):** italic 12px `#2c3e50` "Live, the next lookup corrects it. Stored, the filled-in stretch counts as time spent." at h−24; italic 11px gray "Illustrative fix schedule — not measured at any venue." at h−8.

## Regeneration instructions

- **Layout:** tracking-page `.obj-table`: full-width table, one `<tr>` per section; left `<td>` (45%) holds `.obj-title`, optional `.lede`, bullets, optional `.map-table`, and `.key-point` callouts; right `<td>` (55%, centered) holds the canvas, and in the "What does it collect?" row also the `.payload-note` caption and `.payload` `<pre>` block (both left-aligned).
- **Page style:** body system sans-serif, white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; subtitle `#666` 1.05em; table cell borders `1px solid #2980b9`, padding 16px; `.obj-title` bold 1.1em `#1a5276`; `li b` `#1a5276` weight 600; list items 0.93em.
- **Callouts:** `.key-point` — background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px 14px, 0.93em, leading `<strong>` in `#1a5276`.
- **Labels:** `.lbl` uppercase pill 0.7em bold, padding 2px 7px, radius 3px; `.lbl-purpose` background `#eaf2fb` color `#1a5276`; `.lbl-effect` background `#fdf0e6` color `#a8501c`.
- **Payload:** `.payload` — background `#f8f9fa`, left border `3px solid #1a5276`, monospace 0.78em, `white-space: pre`; `.payload-note` 0.82em italic `#666` immediately above.
- **Method table:** `.map-table` — full width, 0.82em, `th` background `#f8f9fa` color `#1a5276`, cell borders `1px solid #d5dbdb`, first column 20% bold `#1a5276`.
- **Canvas:** intrinsic `width`/`height` attributes as given per chart; `setupCanvas(id)` reads the attributes, sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Shared helpers: `tint(hex, a)` translucent fill, `rr()` rounded-rect, `arrow()` with filled head (dashed option), `box()` labeled tinted box.
- **Palette (tracking-page chart tokens, declared once as `P`):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Red is deliberately not in the rotation — reserved for genuine alarm states. Project-level palette anchors: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- No nav bar, no back/home links. In regenerated HTML any card links would use `.html` extensions.
