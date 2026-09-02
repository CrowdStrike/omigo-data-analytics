# Tracking Data: In-Cabin Driver Monitoring

**Page type:** detail page (two-column obj-table layout: text left 45%, canvas right 55%, one row per section)
**HTML title tag:** Tracking Data: In-Cabin Driver Monitoring

**Subtitle:** An infrared camera near the rearview mirror watches the driver's face whenever the car is on, turning gaze and eyelid movement into attention states.

## Section 1: What is it?

**Lede:** A camera pointed at the driver whose output is a state, not a video.

- **Hardware:** a small infrared camera on the steering column or mirror mount, working at night and through many sunglasses
- **Already shipping:** standard in cars with hands-free highway assist; EU new-car rules require drowsiness warnings, with camera-based distraction detection following

**Key point — Monitoring, not recording:** a dashcam stores footage to watch later; this system runs a model on each frame and keeps the derived state. The frame is usually discarded — the "eyes off road" row is what remains.

### Visualization (canvas `c1`, 720×320)

Cabin schematic, side view: a camera at the mirror position with an infrared cone aimed at the driver's face.

- **Floor line:** 2px `#2c3e50` horizontal line at y=250 from x=60 to x=660.
- **Windshield:** 3px `#2c3e50` slanted line from (150,55) down to (70,200).
- **Dashboard:** rounded rect (rr, radius 8) from x=70 to x=190, y=200 to y=250, fill `#e5e9ef`.
- **Steering wheel:** 3px `#2c3e50` circle outline, radius 26, centered (255,185).
- **Seat:** dark `#2c3e50` rounded shape — vertical back rect 26×130 at (470,115) plus horizontal cushion rect 90×24 at (390,225), both radius 10.
- **Driver:** head circle radius 22 in blue `#2a78d6` centered (420,110); torso rounded rect 54×95 blue at (395,135).
- **Camera:** 18×12 blue `#2a78d6` box at mirror position (160,62), with a 4px-radius magenta `#d55181` IR dot on its right edge.
- **IR cone:** filled triangle from the camera's right edge (178,68) opening to the head — vertices (178,68), (400,85), (400,140) — magenta at alpha 0.10, plus 1px magenta alpha-0.3 edges.
- **Cone label (11px mute `#6b7280`, above the cone midpoint):** "infrared — works at night".
- **Caption (14px `#2c3e50`, bottom center, y=300):** "A camera aimed at the driver's face — running whenever the car is on".

## Section 2: What does it collect?

- **Gaze and head pose** — where the eyes point, sampled continuously while driving
- **Eyelid state** — blink rate, eye openness, long-closure episodes
- **Derived states** — a drowsiness level, distraction events, hands-on-wheel; phone-near-face on some systems

**Key point — "Distracted" is a threshold, not an observation:** the camera measures gaze-away seconds; the event is a cutoff applied to that number. A long mirror check and a glance at a phone can produce the same row.

### Visualization (canvas `c2`, 720×320)

One trace read as events: a minute of gaze as an on-road/off-road strip, then each off-road glance as a duration bar against the event cutoff.

- **Title (bold 14px `#1a5276`, top center):** "One minute of gaze, read as events".
- **Strip (top):** x=60 to x=680 spans 60 seconds (~10.33px/s), y=64, height 34. Base fill blue `#2a78d6` alpha 0.22 (eyes on road); off-road glances drawn over it in orange `#d95926` alpha 0.55. Row label (bold 11px mute, left-aligned above): "gaze".
- **Glances** (start s, duration s): (6, 0.8), (13, 1.2), (21, 0.6), (27, 2.8), (36, 0.9), (44, 3.4), (53, 1.1).
- **Legend (11px, right of strip label):** "on road" swatch in the blue tint, "off road" swatch in the orange tint.
- **Duration bars (bottom):** one vertical bar per glance, centered under its strip position, baseline y=272, scale 32px per second; fill orange alpha 0.4, 1.5px orange stroke; 10px mute duration value above each bar (e.g. "0.8s").
- **Cutoff line:** dashed (4/3) 1.5px `#6b7280` horizontal line at 2.0s height (y=208) across the bar zone, right-aligned 11px mute label "2.0 s cutoff".
- **Events:** the two bars crossing the cutoff (2.8s, 3.4s) get a bold 11px orange label "event" above their value.
- **Caption (italic 12px `#2c3e50`, bottom center):** "Seven glances away; the cutoff turns two of them into events. Illustrative trace."

**Payload note (below canvas, italic, left-aligned):** Sample payload — illustrative structure, not real captured data.

**Payload block (monospace, left border `#1a5276`):**

```
// DMS vendor schemas are not published.
// Whole block is reconstruction; field names generic.
// ── inferred / plausible ──
{
  "vehicle_id": "veh-88214",
  "ts":         "2026-08-22T17:03:41Z",
  "speed_kmh":  96,

  // measured from the camera frame
  "gaze":            "off_road",
  "gaze_off_road_s": 2.4,
  "eyes_closed_pct_1min": 4.1,
  "head_pose":       { "yaw": -21, "pitch": -8 },

  // derived by the model, not observed
  "drowsiness_level": 1,          // 0-4 scale
  "event":            "distraction_warning",
  "frame_retained":   false
}
```

## Section 3: Why is it collected?

**Label (purpose pill):** Stated purpose

- **Attention gating** — the camera is what lets hands-free steering be offered; look away too long and the assist disengages
- **Drowsiness and distraction warnings**, increasingly required by regulation on new cars

**Label (effect pill):** Additional consequence

- **The state can outlive the moment** — on some systems attention events are logged or uploaded with other telematics, where crash investigations and insurers can reach them

**Key point — Read only after something happens:** nobody looks at the log until a crash; then months of drowsiness scores, produced by thresholds the driver never saw, arrive as evidence about the driver.

### Visualization (canvas `c3`, 720×320)

Step chart: how long each kind of in-cabin data survives — a rolling video loop against an event log.

- **Title (bold 14px `#1a5276`, top center):** "What still exists, days after a drive".
- **Axes:** plot from x=80 to x=660, y=70 (100%) to y=252 (0%); 1px `#e5e9ef` gridlines at 0/50/100% with 11px mute y-labels ("0%", "50%", "100%"); x ticks at days 0, 3, 7, 14 with 11px mute labels ("day 0" … "day 14").
- **Line A (blue `#2a78d6`, 2.5px step line):** "dashcam footage (rolling loop)" — 100% from day 0 to day 3, vertical drop to 0% at day 3, 0% thereafter.
- **Line B (violet `#4a3aa7`, 2.5px line):** "derived attention states (event log)" — flat at 100% across the full range.
- **Legend:** bold 11px labels in each line's hue, placed near the lines (violet above its line at the right; blue below its segment before the drop).
- **Caption (italic 11px mute, bottom center):** "Illustrative — loop sizes and logging policies vary; some systems keep states only in the vehicle."

## Regeneration instructions

- **Layout:** tracking-page `.obj-table`: full-width `border-collapse: collapse` table, one `<tr>` per section; left `<td>` (45%) holds `.obj-title` + optional `.lede` + bullets + `.key-point` callouts + `.lbl` pills, right `<td>` (55%, `text-align: center`) holds the canvas — and in the "What does it collect?" row, the `.payload-note` and `.payload` `<pre>` below the canvas (both left-aligned).
- **Page style:** body system sans-serif (-apple-system stack), white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; table cell borders `1px solid #2980b9`, padding 16px; `.obj-title` bold 1.1em `#1a5276`; `li` 0.93em with `li b` in `#1a5276` weight 600. No nav bar, no back/home links.
- **Callouts:** `.key-point` — background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px 14px, 0.93em; leading `<strong>` in `#1a5276`.
- **Pills:** `.lbl` inline-block uppercase 0.7em bold, padding 2px 7px, radius 3px; `.lbl-purpose` background `#eaf2fb` color `#1a5276`; `.lbl-effect` background `#fdf0e6` color `#a8501c`.
- **Payload:** `.payload` — background `#f8f9fa`, left border `3px solid #1a5276`, ui-monospace 0.78em, `white-space: pre`; `.payload-note` italic 0.82em `#666` immediately above.
- **Canvas:** each declares intrinsic `width="720" height="320"`; a shared `setupCanvas(id)` reads those attributes, sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Shared helpers: `tint(hex, alpha)` for translucent fills and `rr()` rounded-rect path.
- **Palette (tracking pages):** categorical tokens blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`; ink `#1a5276` (headings/axes only), text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Red is reserved for genuine alarm states and not used here. (Site-wide palette reference: #1a5276 primary blue, #27ae60 green, #e74c3c red, #e67e22 orange.)
- In regenerated HTML, any card links use `.html` extensions.
