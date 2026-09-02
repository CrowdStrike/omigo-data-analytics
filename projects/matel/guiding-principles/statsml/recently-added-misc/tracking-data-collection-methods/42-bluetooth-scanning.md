# Tracking Data: Bluetooth Beacon Scanning

**Page type:** detail page (two-column obj-table layout: text left 45%, canvas right 55%, three rows: What is it? / What does it collect? / Why is it collected?)
**HTML title tag:** Tracking Data: Bluetooth Beacon Scanning

**Subtitle:** Phones, earbuds and watches transmit BLE advertising packets, and passive receivers turn them into footfall and dwell estimates — via two chosen parameters.

## What is it?

Lede: Cheap receivers listening to packets devices already broadcast.

- **What broadcasts:** phones, earbuds, watches and fitness bands send short advertising packets at regular intervals so other devices can find them
- **Each packet** carries an address and, often, a declared transmit power
- **What venues do:** install inexpensive receivers that log the packets to estimate footfall, dwell and route
- **No install, no pairing** — the receiver only listens

### Visualization (canvas `c1`, 720×320)

Schematic floor plan of a store with BLE receivers detecting a phone's stitched path.

- **Floor plan:** light gray rectangle `#f8f9fa` from (40,20), 640×190, border `#e5e9ef` 2px. Internal walls in `#95a5a6` (2px): vertical (250,20)–(250,130); vertical (450,80)–(450,210); horizontal (250,130)–(350,130); horizontal (40,130)–(180,130).
- **Room labels** (`#95a5a6`, 14px, centered): "ENTRANCE" at (145,80), "SHOES" at (145,165), "CLOTHING" at (350,65), "ELECTRONICS" at (350,175), "CHECKOUT" at (570,115).
- **BLE receivers** (blue `#2a78d6`): six dots (radius 6) at (80,45), (250,55), (450,45), (620,55), (145,180), (450,180); each with a faint signal ring (radius 30, alpha 0.2) and a small antenna glyph above the dot.
- **Phone path:** dashed magenta line (`#d55181`, width 2.5, dash 6/4) through points (80,100), (145,95), (200,90), (300,80), (380,95), (420,120), (400,160), (350,170), (420,185), (500,150), (570,120). Magenta because the line is the stitched (inferred) route, distinct from the blue receivers that observed it; red is not used since an inferred path is not an alarm.
- **Timestamps** (magenta, 13px, centered, small 3px dots): "2:01", "2:03", "2:06", "2:11", "2:14" at path indices 0, 2, 5, 7, 10, each 14px below its point.
- **Phone icon:** small magenta rounded rectangle (14×22, radius 3) at (564,108), the end of the path.
- **Legend** (bold 13px, left-aligned, y≈225): blue dot + "BLE Receiver" at x=60; magenta text "--- Stitched path (inferred)" at x=170.

## What does it collect?

- **Advertising address** — rotates, so not a stable device ID
- **Received signal strength**, from which a range is estimated
- **First and last time** a radio was seen in each zone
- **An ordered sequence of zones**, stitched across observations
- **Dwell per zone**, bounded by the rotation window
- **Radios repeatedly co-present** — devices, not necessarily people

**Distance is not measured:** it is `tx_power_dbm` minus `rssi_dbm` inverted through an assumed path-loss exponent, and that assumption is the estimate. Indoors, bodies and shelving attenuate faster than free space, so the signal is placed farther away than the device is. TX power is declared by the device, not verified, so an error there shifts every distance from it one way.

**`track_id` is a stitching decision:** the identifier rotates, so too short a window makes one visitor several and too long makes several one. Position and visitor count both rest on parameters the operator chose.

### Visualization (canvas `c2`, 720×320)

Signal diagram: a phone in the center broadcasting BLE advertising fields as labeled outward arrows, plus a footer band separating the rotating field from the stable ones.

- **Phone:** dark rounded rectangle (`#2c3e50`, 60×100, radius 10) at (320,90) with a `#e5e9ef` screen inset; Bluetooth rune "ᛒ" in ink `#1a5276` (bold 22px) at (350,147). Ink, not a series hue — the handset and its rings are chrome; the four fields carry the colour.
- **Broadcasting rings:** five concentric circles centered (350,140), radii 75/95/115/135/155, ink `#1a5276`, 1.5px, alpha fading from 0.58 to 0.10.
- **Data-field arrows** radiating from radius 65 to 155, each a different broadcast field with its own hue in SERIES order, 2px line + arrowhead, bold 14px label just past the tip:
  - angle −0.8 rad: "Adv address: A3:F2:9B:…" in blue `#2a78d6`
  - angle −0.2 rad: "Signal: -67 dBm" in green `#008300`
  - angle 0.3 rad: "Frequency: 2.4 GHz" in violet `#4a3aa7`
  - angle 0.9 rad: "Type: BLE Advertisement" in orange `#d95926`
- **Center caption** (`#2c3e50`, 14px, centered, y=215): "Advertising interval is set by the device".
- **Footer band:** rounded rectangle (40,244) 640×62 radius 8, fill `rgba(42,120,214,0.06)`, ink border 1.5px. Two lines (13px, left-aligned at x=56):
  - Bold blue `#2a78d6` "The address rotates" followed in `#2c3e50` by "— so it is an identifier for a window, not a stable identity." (y=266)
  - Bold green `#008300` "Signal strength, frequency and packet type" followed in `#2c3e50` by "carry no identifier at all." (y=289)

### Payload (below canvas `c2`)

Caption (`.payload-note`, italic): "Sample payload — illustrative structure, not real captured data."

```
{
  // ── present in the advertising packet (BLE spec) ──
  "adv_address":   "6f:1d:a3:…",
  "address_type":  "random_resolvable",
  "adv_type":      "ADV_IND",
  // Note: masked for illustration; real BT SIG UUIDs follow
  // 0000xxxx-0000-1000-8000-00805f9b34fb format
  "service_uuid_masked":  "EXAMPLE_BT_SIG_UUID_redacted",
  "tx_power_dbm":  -4,          // as declared by the device
  "local_name":    null,
  "rssi_dbm":      -73,         // measured at the receiver
  "ts":            "2026-08-22T16:31:08.940Z",

  // ── inferred / plausible, added by the receiver ──
  "path_loss_exp": 2.0,         // free-space assumption
  "est_distance_m": 11.2,       // from tx_power − rssi
  "zone":          "concourse-B",
  "rotates":       true,
  "track_id":      "tr_44b8…"   // stitched, not observed
}
```

## Why is it collected?

**Stated purpose** (label pill `.lbl-purpose`)

- **How a space is used** — where people enter, which routes they take, where queues form
- **Cheap and install-free**, and none of it appears in till data

**Additional consequence** (label pill `.lbl-effect`)

- **Zone footfall becomes a saleable series**, read outside the venue as a proxy for retail activity
- **Loyalty falls out for free** — the logic separating a distinct visitor from a repeat observation also separates a first visit from a repeat one

**The unit is an advertising radio:** one visitor with a phone, earbuds and a watch contributes several. That inflates counts most for the device-rich, who are not evenly spread, so a level read off this series carries an inflation factor that moves with who walked in.

### Visualization (canvas `c3`, 720×320)

Two stacked bars comparing people who walked in vs radios the receiver heard, with a per-group legend and inflation factor.

- **Title** (bold 14px ink `#1a5276`, centered, y=24): "One hour of visitors, counted two ways". Subtitle (12px mute `#6b7280`, y=42): "the receiver counts radios; the venue reports the number as footfall".
- **Data (illustrative device mix):**
  - 1 radio, 120 people — "phone only"
  - 2 radios, 90 people — "phone + earbuds"
  - 3 radios, 60 people — "phone + earbuds + watch"
  - 4 radios, 30 people — "phone, earbuds, watch, band"
  - Totals: 300 people, 600 radios.
- **Stacked bars:** baseline y=210, max height 130px scaled to the radios total (600). Segment hues in order: blue `#2a78d6`, aqua `#199e70`, violet `#4a3aa7`, yellow `#c98500`; fills at 0.42 alpha of each hue, 1px solid strokes. Bars 88px wide.
  - Left bar centered x=122, segments = people per group (120/90/60/30), bold total "300" above, caption below "people who walked in".
  - Right bar centered x=292, segments = people × radios (120/180/180/120), bold total "600" above, caption below "radios the receiver heard".
  - Thin gray baseline `#e5e9ef` from x=58 to x=356.
- **Legend** (right side, starting x=392, rows 26px apart from y=74): tinted swatch + label (`#2c3e50`, 12px) per group, and right-aligned bold per-group inflation in the group's hue at x=690: "120 people → 120 radios", "90 people → 180 radios", "60 people → 180 radios", "30 people → 120 radios".
- **Inflation note:** bold orange `#d95926` 13px at (392,194): "Inflation this hour: ×2.00"; then `#2c3e50` 12px lines "An hour with a different mix of visitors gives a" / "different factor, and the series does not record it."
- **Bottom captions (centered, italic):** `#2c3e50` 12px "Comparing two hours in the same venue survives this. Reading a footfall level off it does not."; mute `#6b7280` 11px "Illustrative device mix — the shape, not a measured venue."

## Regeneration instructions

- **Layout:** tracking-page `.obj-table`: full-width bordered table, one `<tr>` per section; left `<td>` (45%) holds `.obj-title` + `.lede` + bullets + `.key-point` callouts + `.lbl` pills, right `<td>` (55%, centered) holds the canvas; the "What does it collect?" row also carries the `.payload-note` caption and `.payload` pre block under its canvas (both left-aligned).
- **Page style:** body system sans-serif (-apple-system stack), white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; subtitle `#666` 1.05em; table cell borders `1px solid #2980b9`, padding 16px; `.obj-title` bold `#1a5276` 1.1em; `li b` `#1a5276` weight 600, li 0.93em. No nav bar, no back/home links.
- **Callouts:** `.key-point` — background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px 14px, 0.93em, leading `<strong>` in `#1a5276`.
- **Label pills:** `.lbl` inline-block uppercase 0.7em bold, padding 2px 7px, radius 3px; `.lbl-purpose` background `#eaf2fb` color `#1a5276`; `.lbl-effect` background `#fdf0e6` color `#a8501c`.
- **Payload:** `.payload` — background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px, ui-monospace 0.78em, `white-space: pre`, line-height 1.45; `.payload-note` 0.82em `#666` italic.
- **Canvas:** each declares intrinsic `width="720" height="320"`; a shared `setupCanvas(id)` reads the element's attributes, sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Includes a rounded-rect helper `rr()`. Charts hardcode literal data arrays (no Math.random).
- **Palette (declared once as tokens):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`; ink `#1a5276` (headings/axes only), text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Red is reserved for genuine alarm states and is not used on this page. Site-wide accent palette: #1a5276 primary blue, #27ae60 green, #e74c3c red, #e67e22 orange.
- In regenerated HTML, any card links use `.html` extensions (this page has none).
