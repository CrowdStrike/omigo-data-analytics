# Tracking Data: Store WiFi & Bluetooth Beacons

**Page type:** detail page (two-column obj-table layout: text left 45%, canvas right 55%, one row per section)
**HTML title tag:** Tracking Data: Store WiFi &amp; Bluetooth Beacons

**Subtitle:** In-store sensors that observe phone radio frames to estimate foot traffic, dwell time, and path.

## Section 1: What is it?

Lede: Sensors around a store listen for the radio frames phones already broadcast.

- **WiFi:** a phone with the radio on sends probe requests periodically to discover networks
- **Bluetooth:** advertises on a similar schedule
- **Position:** comparing signal strength across several sensors gives a rough location
- **Path:** repeating that over time gives a rough route

Key point callout: **The sensor observes a frame, not a shopper:** zone, dwell and path are all computed from signal strength by a model, and the model is where the error lives.

### Visualization (canvas `c1`, 420×320)

Top-down store floor plan with beacon dots and a dotted derived phone path.

- **Store outline:** blue `#2a78d6` rectangle, stroke width 2, at (30, 30) size (w−60)×(h−60).
- **Entrance:** green `#008300` bar 50×4 centered at bottom (w/2−25, h−32); green 12px label "ENTRANCE" centered at (w/2, h−18).
- **Shelf units:** nine 70×30 rectangles filled `rgba(42,120,214,0.12)` in a 3×3 arrangement at x ∈ {60, 180, 300}, y ∈ {60, 120, 180}.
- **Shelf labels (gray `#6b7280` 11px, centered):** Shoes (95, 79), Bags (95, 139), Jewelry (95, 199), Shirts (215, 79), Pants (215, 139), Jackets (215, 199), Hats (335, 79), Socks (335, 139), Belts (335, 199).
- **Beacon dots:** magenta `#d55181` filled circles r=6 with a 14px-radius signal ring stroked in magenta at 0.3 alpha, at eight positions: four corners (40, 40), (w−40, 40), (40, h−40), (w−40, h−40) and four mid-walls (w/2, 40), (40, h/2), (w−40, h/2), (w/2, h−40).
- **Phone trail:** dashed (4/4) orange `#d95926` polyline width 2.5 through `[w/2, h−45], [w/2, 230], [150, 230], [150, 160], [150, 100], [260, 100], [260, 160], [340, 160], [340, 100], [340, 60]`; orange phone icon (10×16 rect with white inner screen) at the final point.
- **Legend (12px, bottom left):** magenta "● Beacon sensor" at (50, h−5), orange "--- Derived path" at (170, h−5).

## Section 2: What does it collect?

- **MAC address** in the frame, which on current phones is usually randomised
- **Signal strength** in dBm, per sensor, per frame
- **Channel and frame type**
- **Timestamps**, from which dwell and revisit are derived
- **Derived zone and path** through the store

Key point callout: **No connection needed:** these frames are sent to discover networks, so any receiver in range observes them whether or not the phone ever joins one.

Key point callout: **RSSI is not a distance:** converting dBm to metres needs a path-loss assumption, and walls, shelving and the shopper's own body attenuate the signal — which the model reads as extra distance. The dBm value is measured; the metres, the zone and therefore the dwell time are model output on top of it.

### Visualization (canvas `c2`, 420×320)

Grid heatmap of derived dwell intensity across store zones, with a gradient legend.

- **Title (bold 13px blue `#2a78d6`, centered at (w/2, 16)):** "DERIVED DWELL HEAT MAP — ILLUSTRATIVE".
- **Grid:** 7 columns × 5 rows of cells starting at (20, 28), cell size ((w−40)/7)×((h−60)/5), each cell inset 2px with a faint `rgba(0,0,0,0.08)` border.
- **Dwell intensity data (0–1 scale, rows top to bottom):**
  - Row 1: `[0.2, 0.3, 0.5, 0.8, 0.9, 0.4, 0.1]`
  - Row 2: `[0.1, 0.2, 0.6, 0.7, 0.85, 0.3, 0.1]`
  - Row 3: `[0.3, 0.4, 0.3, 0.5, 0.6, 0.7, 0.2]`
  - Row 4: `[0.5, 0.6, 0.2, 0.3, 0.4, 0.8, 0.9]`
  - Row 5: `[0.1, 0.2, 0.1, 0.2, 0.3, 0.5, 0.6]`
- **Color scale:** value > 0.75 → magenta tint `rgba(213,81,129,0.7)`; > 0.5 → `rgba(217,89,38,0.6)`; > 0.3 → `rgba(241,196,15,0.45)`; else `rgba(42,120,214,0.2)`.
- **Zone labels (11px `#2c3e50`, centered in their cells):** "Entry" (col 1, row 0), "Shoes" (col 4, row 0), "Checkout" (col 5, row 3), "Aisle 2" (col 2, row 2), "Exit" (col 6, row 4).
- **Legend:** 180×10 horizontal gradient bar centered at bottom (y=h−22) running through the four scale colors (stops 0, 0.35, 0.65, 1), stroked in gray; 11px gray labels "Low dwell" (left-aligned at bar start) and "High dwell" (right-aligned at bar end) above it.

Payload note (right column, under the canvas): *Sample payload — illustrative structure, not real captured data.*

Payload block (monospace `.payload`):

```
{
  // ── present in the radio frame itself ──
  "sensor_id":   "AISLE-07-N",
  "client_mac":  "5A:3F:…",     // often randomised by the phone
  "rssi_dbm":    -71,           // signal strength, in dBm
  "channel":     6,
  "frame_type":  "probe_request",
  "ts":          "2026-08-22T16:22:09.310Z",

  // ── inferred / plausible, computed downstream ──
  "distance_m":   6.4,          // from rssi via a path-loss model
  "zone":        "footwear",
  "dwell_s":      212,
  "visitor_key": "h9c2…",       // hash of the observed MAC
  "repeat_visit": true
}
```

## Section 3: Why is it collected?

Label (`.lbl-purpose`): STATED PURPOSE

- **Layout and staffing** — sales data says what sold, not what was walked past
- **The denominator** — zone-level dwell is the cheapest measurement of what a door count cannot break down

Label (`.lbl-effect`): ADDITIONAL CONSEQUENCE

- **Aggregated foot traffic is an openly sold category**, bought by landlords for lease decisions and by investors as an early indicator of retail activity
- **Repeat-visit flags** allow a visit history per device, a different thing from a headcount

Key point callout: **The comparison it supports is within one store:** sensors go where that store wanted a zone measured, and the signal-to-distance assumption is fitted to that building. Both cancel out when this end is compared with that end, and neither cancels out between sites — so a level sold as foot traffic carries how densely each site was instrumented alongside how busy it was.

### Visualization (canvas `c3`, 420×320)

Bar chart: the same footfall reported at different levels at four sites with different sensor densities, against a dashed reference line for the true footfall.

- **Title (bold 12px ink `#1a5276`, centered at (w/2, 20)):** "Same footfall, different sensor density"; subtitle (11px gray, centered at (w/2, 36)): "four stores, zones equally busy".
- **Data (illustrative index, 100 = the site where the model was fitted):** 2 sensors → 44, 4 sensors → 71, 8 sensors → 100, 16 sensors → 137. Bar value labels "44", "71", "100", "137" in bold blue above each bar.
- **Bars:** width 40, fill blue tint `rgba(42,120,214,0.32)` with blue `#2a78d6` stroke; baseline at y=214 in grid gray `#e5e9ef`; y scale 0–160 mapped to top=62..base=214; x-axis labels "2 sensors", "4 sensors", "8 sensors", "16 sensors" in 11px `#2c3e50` below the baseline.
- **Reference line:** dashed (5/4) aqua `#199e70` horizontal line width 2 at the y of value 100, with bold 11px aqua left-aligned label above it: "actual footfall, the same at all four".
- **Y-axis label (rotated, gray 11px):** "reported level".
- **Captions (centered, italic):** 11px `#2c3e50` "Within one store the density is fixed, so it cancels." at base+42 and "Across stores it does not, and rides in the level." at base+58; 10px gray "Illustrative index, not measured at any site." at h−8.

## Regeneration instructions

- **Layout:** tracking-page `.obj-table`: full-width table, one `<tr>` per section; left `<td>` (45%) holds `.obj-title`, optional `.lede`, bullets, and `.key-point` callouts; right `<td>` (55%, centered) holds the canvas, and in the "What does it collect?" row also the `.payload-note` caption and `.payload` `<pre>` block (both left-aligned).
- **Page style:** body system sans-serif, white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; subtitle `#666` 1.05em; table cell borders `1px solid #2980b9`, padding 16px; `.obj-title` bold 1.1em `#1a5276`; `li b` `#1a5276` weight 600; list items 0.93em.
- **Callouts:** `.key-point` — background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px 14px, 0.93em, leading `<strong>` in `#1a5276`.
- **Labels:** `.lbl` uppercase pill 0.7em bold, padding 2px 7px, radius 3px; `.lbl-purpose` background `#eaf2fb` color `#1a5276`; `.lbl-effect` background `#fdf0e6` color `#a8501c`.
- **Payload:** `.payload` — background `#f8f9fa`, left border `3px solid #1a5276`, monospace 0.78em, `white-space: pre`; `.payload-note` 0.82em italic `#666` immediately above.
- **Canvas:** this page uses 420×320 intrinsic sizes; `setupCanvas(id)` reads the element's own `width`/`height` attributes, sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Shared helpers: `tint(hex, a)` translucent fill, `rr()` rounded-rect.
- **Palette (tracking-page chart tokens, declared once as `P`):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Red is deliberately not in the rotation — reserved for genuine alarm states. Project-level palette anchors: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- No nav bar, no back/home links. In regenerated HTML any card links would use `.html` extensions.
