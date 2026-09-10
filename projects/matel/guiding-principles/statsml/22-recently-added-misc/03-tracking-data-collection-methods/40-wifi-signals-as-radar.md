# Tracking Data: WiFi Signals as Radar

**Page type:** detail page (two-column obj-table layout: text left 45%, canvas right 55%, one row per section)
**HTML title tag:** Tracking Data: WiFi Signals as Radar

**Subtitle:** Radio waves from a router are perturbed by anything that moves through them — and a classifier over those perturbations can report movement, an occupancy estimate, and sometimes a breathing rate.

## Section 1: What is it?

Lede: The existing radio link, read as a motion sensor.

- **Mechanism:** anything moving between transmitter and receiver changes how the signal arrives
- **The measurement** is per-subcarrier channel state the radios already compute to stay synchronised
- **Outputs:** movement, an occupancy estimate, and in favourable conditions a periodic component consistent with breathing
- **No camera, no microphone** — the input is the link itself

Key point callout: **Note the layering:** the measurement is a variance series. Motion, count, posture and respiration are all inference over it, and the difficulty rises steeply along that list.

### Visualization (canvas `c1`, 720×320)

Room cross-section: a router emitting waves, a person silhouette perturbing them, disrupted echoes returning.

- **Background:** full-canvas `#f8f9fa` fill; room drawn with `#2c3e50` strokes width 2 — floor (40, 200)–(680, 200), left wall (40, 30)–(40, 200), right wall (680, 30)–(680, 200), ceiling (40, 30)–(680, 30).
- **Router:** blue `#2a78d6` 30×20 rectangle at (55, 60) with white 11px centered label "WiFi" and two short blue antenna lines above.
- **Outgoing waves:** six concentric arcs centered at (70, 70), radii 55 to 330 in steps of 55, arc span −0.6..0.6 rad, stroke `rgba(26, 82, 118, 0.5 − i×0.07)`, width 1.5.
- **Person silhouette (magenta `#d55181`, centered at (400, 140)):** filled head circle r=14 at (400, 100); stick body, arms and legs stroked at width 4/2.
- **Disrupted/reflected waves:** four dashed (4/4) arcs centered on the person, radii 30–120 in steps of 30, arc span 2.5..3.8 rad, stroke magenta tint `rgba(213,81,129,0.6 − i×0.12)`, width 1.5.
- **Labels (bold 13px, centered):** blue "Signal sent" at (160, 220); magenta "Echo returned (disrupted)" at (500, 220).

## Section 2: What does it collect?

- **Per-subcarrier amplitude variation** — the actual measurement
- **Motion detected**, with a confidence value
- **An occupancy estimate**, not a headcount
- **Coarse posture class**, where the model supports it
- **Breathing rate**, only when a periodic component resolves
- **Which sensed zone** changed, and when

Key point callout: **No identity field to redact:** the mechanism never produces one. The unit of observation is a link between two radios, so occupancy from this sensor cannot be joined to a person-level table.

Key point callout: **Everything above the variance is derived:** motion, occupancy and any breathing rate are classifier outputs, each with its own error. That makes the failure modes physical — a ceiling fan, a curtain in a draught or a pet perturbs the channel much as a person does, so a false positive is about the room, not about anyone.

### Visualization (canvas `c2`, 720×320)

House floor grid (3×2 rooms) with per-room detection state, person icons and a legend.

- **Background:** full-canvas `#f8f9fa` fill.
- **Rooms (each 180×95, `#2c3e50` border width 2, gray `#6b7280` 12px label at top center):**
  - Living Room at (80, 20) — 2 people, moving
  - Kitchen at (270, 20) — 1 person, moving
  - Bedroom 1 at (460, 20) — 0 people
  - Bathroom at (80, 125) — 1 person, stationary
  - Bedroom 2 at (270, 125) — 1 person, stationary
  - Garage at (460, 125) — 0 people
- **Room background tint:** occupied+moving → magenta tint `rgba(213,81,129,0.1)`; occupied+stationary → `rgba(230, 126, 34, 0.1)`; empty → `rgba(39, 174, 96, 0.05)`.
- **Person icons:** stick figures (head circle r=7 plus stroked limbs, width 2), magenta `#d55181` when moving, orange `#d95926` when stationary, spaced 40px apart when two share a room; moving figures get a magenta 16px "→" arrow beside them.
- **Detection status dot:** r=5 circle at each room's top-right corner — magenta when occupied, green `#008300` when empty.
- **Legend (12px, bottom left, dot + label):** magenta "Moving" at (100, 235), orange "Stationary" at (180, 235), green "Empty" at (270, 235).

Payload note (right column, under the canvas): *Sample payload — illustrative structure, not real captured data.*

Payload block (monospace `.payload`):

```
// Consumer sensing products do not publish a record
// format. Channel state information is an 802.11
// concept; the packaging below is reconstruction.
// ── inferred / plausible ──
{
  "sensor_id":    "AP-KITCHEN-01",
  "window_start": "2026-08-22T02:14:00Z",
  "window_ms":    2000,
  "sample_hz":    50,

  // measured: per-subcarrier amplitude variation
  "csi_amp_var":  [0.041, 0.038, 0.129, 0.144, …],

  // derived from the variance series above
  "motion":       true,
  "motion_conf":  0.74,
  "occupancy_est": 1,
  "resp_rate_bpm": null,   // no periodic component resolved
  "zone_label":   "kitchen"
  // no device id and no subject id: the mechanism
  // has no identity field to populate
}
```

## Section 3: Why is it collected?

Label (`.lbl-purpose`): STATED PURPOSE

- **Presence detection** without a camera, using radios that are **already installed**
- **Automation** that follows an occupied room, and an alert when expected movement stops

Label (`.lbl-effect`): ADDITIONAL CONSEQUENCE

- Presence answers questions about **a household, not a room** — how many people are in a dwelling, or when it is empty
- The boundary is **retention and query access**, not the sensor

Key point callout: **A model output wearing measurement units:** the sensor measures how the channel between two radios changed, and there is no identity field, so it can never say who moved. A count of occupants is a classifier's guess over that variance — a fan, a curtain in a draught or a pet are plausible causes. The radios sit wherever connectivity was wanted, so room labels are attached to a coverage footprint afterwards.

### Visualization (canvas `c3`, 720×320)

Four-segment line chart: one channel-variance trace across four ten-minute stretches, each with a known cause; three of the four causes land in the same shaded band.

- **Title (bold 14px ink `#1a5276`, centered at (w/2, 26)):** "One variance trace, four causes"; subtitle (12px gray, centered at (w/2, 45)): "same sensor, same scale — only the first stretch is separable".
- **Segments (each 8 points, drawn left to right in equal-width quarters of the plot; padL=44, padR=20, top=62, plotH=118; y scale 0–0.65; dashed grid-gray dividers between segments):**
  - "nobody in the room" — mute gray `#6b7280`: `[0.05, 0.04, 0.06, 0.05, 0.04, 0.05, 0.06, 0.04]`
  - "ceiling fan on" — violet `#4a3aa7`: `[0.42, 0.51, 0.45, 0.49, 0.46, 0.52, 0.44, 0.48]`
  - "curtain in a draught" — aqua `#199e70`: `[0.31, 0.44, 0.28, 0.53, 0.36, 0.41, 0.29, 0.47]`
  - "one person walking" — orange `#d95926`: `[0.38, 0.55, 0.41, 0.47, 0.50, 0.39, 0.53, 0.44]`
- **Shared band:** orange tint `rgba(217,89,38,0.10)` rectangle spanning segments 2–4 vertically from variance 0.26 to 0.56 — the band the three moving causes share.
- **Axes:** baseline in grid gray `#e5e9ef` at top+plotH; 11px gray "variance" label above the left edge.
- **Segment labels:** bold 12px in each segment's hue, centered below the baseline, word-wrapped at ~14 characters per line.
- **Captions (centered, italic):** 12px `#2c3e50` "A count of occupants is a guess made inside the shaded band." at h−26; 11px gray "Illustrative traces — the overlap is the point, not the exact values." at h−9.

## Regeneration instructions

- **Layout:** tracking-page `.obj-table`: full-width table, one `<tr>` per section; left `<td>` (45%) holds `.obj-title`, optional `.lede`, bullets, and `.key-point` callouts; right `<td>` (55%, centered) holds the canvas, and in the "What does it collect?" row also the `.payload-note` caption and `.payload` `<pre>` block (both left-aligned).
- **Page style:** body system sans-serif, white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; subtitle `#666` 1.05em; table cell borders `1px solid #2980b9`, padding 16px; `.obj-title` bold 1.1em `#1a5276`; `li b` `#1a5276` weight 600; list items 0.93em.
- **Callouts:** `.key-point` — background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px 14px, 0.93em, leading `<strong>` in `#1a5276`.
- **Labels:** `.lbl` uppercase pill 0.7em bold, padding 2px 7px, radius 3px; `.lbl-purpose` background `#eaf2fb` color `#1a5276`; `.lbl-effect` background `#fdf0e6` color `#a8501c`.
- **Payload:** `.payload` — background `#f8f9fa`, left border `3px solid #1a5276`, monospace 0.78em, `white-space: pre`; `.payload-note` 0.82em italic `#666` immediately above.
- **Canvas:** intrinsic size 720×320 per chart; `setupCanvas(id)` reads the element's own `width`/`height` attributes, sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Shared helpers: `tint(hex, a)` translucent fill, `rr()` rounded-rect.
- **Palette (tracking-page chart tokens, declared once as `P`):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Red is deliberately not in the rotation — reserved for genuine alarm states. Project-level palette anchors: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- No nav bar, no back/home links. In regenerated HTML any card links would use `.html` extensions.
