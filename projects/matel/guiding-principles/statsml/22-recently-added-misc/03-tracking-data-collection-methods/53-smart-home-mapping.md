# Tracking Data: Smart Home Mapping

**Page type:** detail page (two-column obj-table layout: text left 45%, canvas right 55%, one row per section)
**HTML title tag:** Tracking Data: Smart Home Mapping

**Subtitle:** Robot vacuums, thermostats, door sensors and smart bulbs each measure a small physical fact about a building. Joined on one account, those facts describe a dwelling and an occupancy schedule — and the dwelling outlives the account.

## What is it?

Each device measures one narrow physical fact about a building.

- **Vacuum LIDAR:** distance to walls
- **VR headset depth scan:** newer headsets sweep the room into a 3D mesh of walls and furniture — LIDAR-style geometry, captured so virtual content can be rendered against the real room
- **Thermostat:** temperature, and whether its motion sensor triggered
- **Contact sensor:** open or closed
- **Bulb:** its own on/off state
- **Most sync to vendor servers** — that is how the app and the automations work when the phone is off the home network

**Geometry, not knowledge:** a mapping pass yields a precise floor plan, but it records that a partition sits at a certain distance, not who lives behind it. Confusing the two is the recurring error on this page's data.

### Visualization (canvas `c1`, 720×320)

Schematic floor plan drawn by a mapping pass: rooms with dimensions, detected furniture, a dotted robot path, and side lists of what the map export does and does not carry.

- **Background:** full-canvas `#f8f9fa`. Header (bold 13px blue `#2a78d6`, left at 20,18): "WHAT A MAPPING PASS PRODUCES — SCHEMATIC FLOOR PLAN".
- **Walls (blue strokes):** outer rect (60,35,400×190) at 3px; internal walls 2px with door gaps — bedroom wall x=220 from y=35–140 and y=160–225; kitchen wall x=340 from y=35–100 and y=120–225.
- **Room labels (12px mute `#6b7280`, centered):** "BEDROOM" / "3.8m x 4.2m" at (140,90/104); "LIVING ROOM" / "2.4m x 3.5m" at (280,90/104); "KITCHEN" / "2.0m x 3.2m" at (400,70/84).
- **Furniture (detected obstacles):** rects filled `rgba(42,120,214,0.2)` — bed (80,45,50×30), couch (240,170,70×35), table (365,45,40×25).
- **Robot path:** dotted (3/3) 1.5px green `#008300` polyline snaking through the rooms; current robot position a 6px green dot at (260,210) with white bold 13px "R".
- **Right column, field lists:** bold 12px blue "FIELDS THE MAP CARRIES" at (500,55) over 11px mute bullets "• room polygons", "• floor area per room", "• fixed obstacle positions", "• no-go zones", "• map version"; then bold 12px blue "WHAT IT DOES NOT CARRY" at (500,158) over "• who is in the room", "• how many people live here".
- **Sync note:** 1.5px blue outlined box (490,200,200×26) with 11px blue text: "Map syncs to vendor servers so the" / "phone app can render it off-network."

## What does it collect?

- **Room polygons,** dimensions and fixed obstacle positions (vacuum LIDAR)
- **Thermostat** setpoint changes and motion-sensor triggers over the day
- **Contact sensors** — open/close transitions with timestamps
- **Bulb state changes,** and which automations fired
- **Cleaning schedule** — which rooms the vacuum was asked to cover
- **Setpoint preferences** by zone and time of day
- **Derived occupancy state** — a model output, not a sensor reading

**Unit of observation:** every measured field describes a structure — walls, dimensions, obstacles — and only `linked_account` describes a person.

**Different lifetimes:** the account can be closed, sold with the device, or handed to a new occupant; the polygons keep describing the same building. A floor plan captured under one household's agreement stays an accurate record of the next household's home, and they were never asked.

### Visualization (canvas `c2`, 720×320)

Multi-row device timeline for one account over one day — lights blocks, door events, a thermostat temperature line, motion blobs — plus a two-readings callout band.

- **Background:** `#f8f9fa`. Header (bold 13px blue, left at 20,18): "ONE ACCOUNT, ONE DAY — SCHEMATIC, ILLUSTRATIVE EVENT TIMES".
- **Axis:** time axis at y=200 from x=82 to x=680; hour markers every 3 hours with 11px mute labels "12am, 3am, 6am, 9am, 12pm, 3pm, 6pm, 9pm, 12am" and vertical grid `#e5e9ef` lines from y=35.
- **Rows (right-aligned bold 12px labels at x=74, each in its own SERIES hue, label doubles as legend):** Lights (y 55) blue `#2a78d6`; Door (y 95) green `#008300`; Thermostat (y 135) violet `#4a3aa7`; Motion (y 170) orange `#d95926`.
- **Lights row (filled blocks, 18px tall at y=45):** bedroom light 6:15–6:45am; kitchen 6:30–7:30am; living room 6pm–10:30pm; bedroom 10pm–10:45pm.
- **Door events (green dots radius 4 with short vertical strokes at y=95), hours:** `[7.5, 12.25, 12.75, 17.75, 19.0, 21.5]`.
- **Thermostat line (2px violet), points (hour, °C):** `(0,18), (6,18), (6.5,21), (7.5,21), (8,19), (17,19), (17.5,21.5), (22.5,21.5), (23,19), (24,19)`; y = 135 − (temp−17)×4. Labels 13px violet: "HOME" at hour 7 (y=122), "AWAY" at hour 10 (y=138), "HOME" at hour 18 (y=122).
- **Motion row (orange rects at 45% alpha, 14px tall at y=163):** morning 6:15–7:30am; brief midday 12:15–12:45pm; evening 5:45–10:30pm.
- **Callout band (below the axis):** rect (82,238,598×68) filled `rgba(42,120,214,0.07)` with 1.5px ink `#1a5276` stroke. Bold 12px ink: "Same four rows, two readings:"; 12px `#555`: "one occupant on a regular schedule — or two occupants with offset hours."; bold 12px ink: "The sensors cannot separate them. Occupant count is not a measured field."

Below the canvas (right column):

Sample payload — illustrative structure, not real captured data.

```
// Vacuum map exports are not publicly specified. The
// whole block is reconstruction from what a mapping
// pass has to produce in order to plan a route.
{
  // ── inferred / plausible ──
  "map_id":        "m-7c31…",
  "units":         "millimetres",
  "resolution_mm": 50,
  "rooms": [
    { "label": "kitchen", "area_m2": 11.4,
      "polygon": [[0,0],[3400,0],[3400,3350],[0,3350]] },
    { "label": "bedroom", "area_m2": 14.1, "polygon": ["…"] },
    { "label": "room_3",  "area_m2": 8.2,  "polygon": ["…"] }
  ],
  "obstacles":     [ { "kind": "furniture_leg", "at": [1200,880] } ],
  "no_go_zones":   [ { "label": "pet bowl", "polygon": ["…"] } ],
  "map_version":   4,            // remapped after furniture moved
  "linked_account": "u-52a…"
}
```

## Why is it collected?

**Stated purpose** (label pill, blue)

- **Route planning** needs persistent geometry — a vacuum cannot cover a floor, avoid a stair drop, or honour a no-go zone without it
- **Pre-conditioning** needs an occupancy estimate; heating an empty house is the waste the product removes

**Additional consequence** (label pill, orange)

- The same fields read as **dwelling attributes** — floor area, room count, occupancy regularity
- **Nothing extra is collected**, and floor area correlates with property value and so with income

**A proxy is not the thing:** floor area as a stand-in for income breaks in the cases that matter — a large inherited house with little income, a small expensive flat, a house share. A segment built on measured area is confidently wrong about those households, and a precise input lends the output an air of precision it did not earn.

### Visualization (canvas `c3`, 720×320)

Scatter plot: floor area vs income — a rising cloud with a fitted dashed trend line and three highlighted off-trend households.

- **Title (bold 14px ink `#1a5276`, centered at y=26):** "Floor area predicts income on average, and not household by household".
- **Axes:** grid-gray 1px L-axes; padL 62, padR 22, top 58, base 236; x range 30–170 m², y range 40–190 income index. Axis labels 12px mute: "measured floor area  →" (centered below), rotated "household income  →" (left).
- **Cloud (20 illustrative points, [area m², income index]):** `[48,62], [55,71], [58,66], [62,84], [66,79], [70,92], [74,88], [78,104], [82,97], [86,112], [90,108], [95,124], [99,118], [104,133], [110,129], [116,146], [122,141], [130,158], [138,152], [146,170]` — 4.5px dots in blue `#2a78d6` tint at 50% alpha.
- **Trend line:** dashed (6/4) 2px blue from (44,62) to (150,168); label bold 12px blue near (96,120): "the trend a segment is fitted to".
- **Off-trend households (7px circles, 2px orange `#d95926` stroke, orange tint 35% fill, two-line orange labels beside each point):** (152,58) "large inherited house," / "little income"; (44,168) "small flat," / "expensive city"; (128,74) "house share," / "income split four ways". Labels placed left or right of the point depending on x position.
- **Captions (centered, italic):** 12px text `#2c3e50` "A precise measurement on the x-axis does not make the y-axis estimate precise." (h−26); 11px mute "Illustrative points — the shape of the relationship, not survey data." (h−9).

## Regeneration instructions

- **Layout:** tracking detail page `.obj-table` — full-width table, `border-collapse: collapse`, one `<tr>` per section; left `<td>` (45%) holds `.obj-title` + `.lede` + bullets + `.key-point` callouts, right `<td>` (55%, `text-align: center`) holds the canvas (and, in the "What does it collect?" row, the `.payload-note` caption plus `<pre class="payload">` block below the canvas, both left-aligned).
- **Page style:** body system sans-serif, white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; table cell borders `1px solid #2980b9`, padding 16px; `.obj-title` bold 1.1em `#1a5276`; li 0.93em with `li b` in `#1a5276` weight 600.
- **Callouts:** `.key-point` — background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px 14px, 0.93em, first `<strong>` in `#1a5276`.
- **Label pills:** `.lbl` inline-block uppercase 0.7em bold, padding 2px 7px, radius 3px; `.lbl-purpose` background `#eaf2fb` color `#1a5276`; `.lbl-effect` background `#fdf0e6` color `#a8501c`.
- **Payload block:** `.payload` — background `#f8f9fa`, left border `3px solid #1a5276`, monospace 0.78em, `white-space: pre`; `.payload-note` italic 0.82em `#666` immediately above.
- **Canvas:** declare intrinsic `width="720" height="320"`; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setupCanvas(id)` helper that reads the element's own width/height attributes. Shared helpers: `rr()` rounded-rect path and `tint(hex, alpha)` rgba derivation from palette hexes.
- **Chart palette (tracking pages):** categorical CVD-checked tokens — blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`; ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Red is reserved for genuine alarm states, not in the series rotation. Page/site palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- No nav bar, no back/home links. In regenerated HTML, any card links elsewhere use `.html` extensions.
