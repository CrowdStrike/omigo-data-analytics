# Tracking Data: WiFi Location Databases

**Page type:** detail page (two-column obj-table layout: text left 45%, canvas right 55%, one row per section)
**HTML title tag:** Tracking Data: WiFi Location Databases

**Subtitle:** Surveyed WiFi access points act as position references where satellite signals do not reach — returning a radius, not a point.

## Section 1: What is it?

Lede: A lookup table from radio hardware to a surveyed coordinate.

- **How it was built:** survey vehicles, and later contributing handsets, recorded which access points were visible at satellite-fixed positions
- **The table** maps an access point's hardware identifier to an estimated coordinate
- **The request:** a phone sends the identifiers and signal strengths it can see
- **The response:** a position and an accuracy radius, with no satellite fix required — which is why it works indoors
- **A home router** appears as surveyed equipment, and contributes to fixes for any device that sees it

### Visualization (canvas `c1`, 720×320)

Building map with router triangulation resolving a person's position as a radius.

- **Background:** full-canvas `#f8f9fa` fill; building outline `#2c3e50` stroke width 2 at (100, 30) 520×180; internal walls in gray `#6b7280` width 1: vertical at x=280 and x=450 (y 30–210), horizontal at y=120 (x 100–280).
- **Room labels (gray 11px, centered):** "Store A" (190, 80), "Store B" (190, 170), "Corridor" (365, 120), "Store C" (540, 120).
- **Routers (blue `#2a78d6` filled triangles, 12px wide, with two concentric signal rings at radii 15 and 30 stroked `rgba(26, 82, 118, 0.3 − i×0.1)`, and a bold 11px blue label below):** R1 (140, 50), R2 (250, 180), R3 (420, 55), R4 (580, 170).
- **Person position (calculated) at (350, 130):** dashed (4/3) magenta-tint `rgba(213,81,129,0.4)` lines from each router to the point; magenta `#d55181` filled dot r=8, a magenta-tint ring r=14 (width 2), and a dashed (2/2) fainter accuracy circle r=22 in `rgba(213,81,129,0.2)`.
- **Labels:** blue 12px left-aligned "Fix returned as a radius" at (376, 134); bold 13px blue title centered at (w/2, 225): "Solved from surveyed access-point coordinates — schematic".

## Section 2: What does it collect?

- **Hardware identifier** (BSSID) and its surveyed coordinate
- **Signal strength** from the phone to each visible access point
- **The returned position**, plus an accuracy radius
- **Observation count** behind each entry, and when it was last seen
- **A sequence of fixes** over time, from which visits are inferred

Key point callout: **The row keys on equipment:** a BSSID says where a radio was last observed, not where anyone lives.

Key point callout: **Why that makes it fragile:** move the router to a new city and the stored coordinates stay put, so every phone that sees it is placed at the old address. `last_seen` and `observations` exist to manage this — a stale entry with many historical observations is harder to correct than a fresh one, so a well-surveyed router that moves gives a confidently wrong fix rather than a low-confidence one.

### Visualization (canvas `c2`, 720×320)

Vertical four-step data-layer stack with arrows, step numbers and right-side annotations.

- **Background:** full-canvas `#f8f9fa` fill.
- **Layer boxes (400×40 rounded rects, radius 6, at x=55, starting y=20, 12px gaps; bold 14px white label plus 12px monospace white-at-0.85-alpha detail):**
  1. "Router BSSID" / `AA:BB:CC:DD:EE:FF` — blue `#2a78d6`
  2. "Mapped GPS Location" / `37.7749° N, 122.4194° W` — blue `#2a78d6`
  3. "Phone Signal Strength" / `-45 dBm, -62 dBm, -71 dBm` — orange `#d95926`
  4. "Your Calculated Position" / `Floor 3, Room 302, Near window` — magenta `#d55181`
- **Connectors:** gray `#6b7280` down-arrows (width 2) between consecutive boxes, centered on the box midline.
- **Step numbers:** circles r=12 to the left of each box (x=startX−25), fill `rgba(26, 82, 118, 0.15)`, bold 14px blue numeral 1–4.
- **Right-side annotations (12px gray, one per box):** "Surveyed at a known position"; "Stored as an estimate per access point"; "Observed by the phone"; "Solved, and returned with a radius".

Payload note (right column, under the canvas): *Sample payload — illustrative structure, not real captured data.*

Payload block (monospace `.payload`):

```
// Request/response field names follow the public
// geolocation web APIs. Survey-side fields are inferred.
{
  // ── documented in public geolocation APIs ──
  "wifiAccessPoints": [
    { "macAddress": "a4:2b:8c:…", "signalStrength": -47,
      "age": 1200, "channel": 6 },
    { "macAddress": "f0:9f:c2:…", "signalStrength": -71,
      "age": 4300, "channel": 36 }
  ],
  "location": { "lat": 30.26719, "lng": -97.74305 },
  "accuracy": 24.0,          // metres, radius not a point

  // ── inferred / plausible: the survey-side row ──
  "bssid":        "a4:2b:8c:…",
  "est_lat":      30.26721,
  "est_lon":      -97.74298,
  "observations": 14,        // sightings backing this estimate
  "last_seen":    "2026-06-18",
  "ssid":         null       // name not stored
}
```

## Section 3: Why is it collected?

Label (`.lbl-purpose`): STATED PURPOSE

- **A fix where satellites are weak** — indoors and in dense streets GPS can be slow or fail
- **Speed and battery:** a table match returns a position in a fraction of the time

Label (`.lbl-effect`): ADDITIONAL CONSEQUENCE

- **Location products indoors** — footfall, visit attribution, dwell — though the coordinate carries no floor, so two stacked units look the same
- A household's own router **contributes to fixes for passers-by**, because the table keys on equipment, not accounts

Key point callout: **A coverage footprint, not a sample of places:** entries exist where survey vehicles and contributing handsets happened to travel. And `accuracy` is a radius — treating the returned point as the location turns an interval into a claim about one shop.

### Visualization (canvas `c3`, 720×320)

Bar chart: the reported accuracy radius shrinking as more surveyed access points are visible, against a dashed line for the width of one shop front.

- **Title (bold 13px ink `#1a5276`, centered at (w/2, 24)):** "Reported accuracy radius, by how many surveyed access points are visible"; subtitle (12px gray, centered at (w/2, 42)): "a visit claim needs the circle to fit inside one unit".
- **Data (illustrative metres):** "1 seen" → 180 m, "2 seen" → 95 m, "4 seen" → 55 m, "8 seen" → 32 m, "15 seen" → 20 m. Shop-front reference SHOP = 22 m.
- **Scale/geometry:** baseline y=232 (grid gray `#e5e9ef` from x=64 to w−30), max height 148px for max value 190; bars 62px wide at 116px steps from x=128.
- **Bars:** radius ≤ 22 m ("fits") → fill `rgba(0,131,0,0.40)` with green `#008300` stroke (only the 20 m bar); otherwise fill `rgba(42,120,214,0.28)` with blue `#2a78d6` stroke. Bold 12px value labels ("180 m" … "20 m") above each bar in the bar's hue; 12px `#2c3e50` category labels below the baseline.
- **Reference line:** dashed (6/4) violet `#4a3aa7` horizontal line width 1.5 at the y of 22 m, with bold 12px violet left-aligned label "width of one shop front, about 22 m" above it (from x=300).
- **X-axis caption (12px gray, centered at base+38):** "access points in range with a stored coordinate".
- **Captions (centered, italic):** 12px `#2c3e50` "Above the dashed line the circle spans several units, so \"which shop\" is not answered." at h−26; 11px gray "Illustrative — the trend is the point, not these metre values." at h−9.

## Regeneration instructions

- **Layout:** tracking-page `.obj-table`: full-width table, one `<tr>` per section; left `<td>` (45%) holds `.obj-title`, optional `.lede`, bullets, and `.key-point` callouts; right `<td>` (55%, centered) holds the canvas, and in the "What does it collect?" row also the `.payload-note` caption and `.payload` `<pre>` block (both left-aligned).
- **Page style:** body system sans-serif, white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; subtitle `#666` 1.05em; table cell borders `1px solid #2980b9`, padding 16px; `.obj-title` bold 1.1em `#1a5276`; `li b` `#1a5276` weight 600; list items 0.93em.
- **Callouts:** `.key-point` — background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px 14px, 0.93em, leading `<strong>` in `#1a5276`.
- **Labels:** `.lbl` uppercase pill 0.7em bold, padding 2px 7px, radius 3px; `.lbl-purpose` background `#eaf2fb` color `#1a5276`; `.lbl-effect` background `#fdf0e6` color `#a8501c`.
- **Payload:** `.payload` — background `#f8f9fa`, left border `3px solid #1a5276`, monospace 0.78em, `white-space: pre`; `.payload-note` 0.82em italic `#666` immediately above.
- **Canvas:** intrinsic size 720×320 per chart; `setupCanvas(id)` reads the element's own `width`/`height` attributes, sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Shared helpers: `tint(hex, a)` translucent fill, `rr()` rounded-rect.
- **Palette (tracking-page chart tokens, declared once as `P`):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Red is deliberately not in the rotation — reserved for genuine alarm states. Project-level palette anchors: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- No nav bar, no back/home links. In regenerated HTML any card links would use `.html` extensions.
