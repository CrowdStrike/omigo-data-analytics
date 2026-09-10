# Sport Wearables: Baseball Pitching

**Page type:** detail page (two-column obj-table layout: text left 45%, canvas right 55%, three rows: What is it? / What does it collect? / Why is it collected?)
**HTML title tag:** Sport Wearables: Baseball Pitching

**Subtitle:** An inertial sleeve near the elbow senses forearm motion. The elbow-torque number that sets pitch limits and rest days is a limb-model output, measured by nothing.

## What is it?

Lede: Motion sensing on the forearm, standing in for load on a ligament.

- **Worn:** a compression sleeve with an inertial unit seated near the elbow
- **Measures:** forearm acceleration and rotation through each throw
- **Derived:** arm slot, release speed, and elbow torque — from motion plus a model of the arm

**A model output guards the ligament:** no strain gauge touches the elbow. The torque number that sets pitch limits and rest days is computed from a limb model, not measured on this arm.

### Visualization (canvas `c1`, 720×360)

Side-view pitcher (facing left) in the cocked position on a low mound, the sleeve and its sensor drawn on the forearm, with hue-coded annotations separating what the sleeve measures from what the model assigns.

- **Header strip:** tinted ink band `rgba(26,82,118,0.07)` full width, 28px tall; bold 15px ink `#1a5276` centered title "Baseball — the sensor is on the forearm; the number is about the ligament".
- **Ground line:** 1px `#e5e9ef` at y=316, x 60→660; low mound bump — quadratic from (280,316) with control (360,284) to (440,316) (peak ≈ y=300), fill 0.08-alpha ink tint.
- **Pitcher** (ink `#1a5276` 2px strokes, side view, facing left, near x=360), cocked position: head circle (348,104) r13; torso (352,118)→(368,190); hips dot r4 filled at (368,192); back leg (368,192)→(392,252)→(398,314); front leg raised (368,192)→(330,230)→(318,262), foot off the ground; glove arm (354,128)→(310,150)→(286,146) ending in a glove circle r6 (0.25-alpha ink fill, ink stroke); throwing arm shoulder (366,126)→elbow (408,110)→hand (416,66) — the classic cocked L-shape; ball circle r5 at (418,60), 0.15-alpha ink fill, ink stroke.
- **Sleeve:** thick 9px line `rgba(74,58,167,0.35)` (violet tint) from (409,104) to (414,78) along the forearm; solid violet `#4a3aa7` sensor dot r6 at (410,98), seated near the elbow.
- **Whip arc:** dashed violet 2px (dash 6/4) centered near the shoulder (366,126), radius 64, sweeping anticlockwise from the current hand angle (≈ −0.88 rad) to the release direction pointing left (−π); violet arrowhead at the release end (302,126) pointing down along the tangent; 12px mute label right-aligned at (300,86): "the whip the model reconstructs".
- **Sensor label** — violet: dashed 1.5px leader (dash 3/3) from the sensor dot to (540,110); bold 13.5px violet left-aligned (546,106) "inertial sleeve"; 11.5px mute left-aligned (546,122) "seated near the elbow".
- **Measured annotation** — green `#008300`: bold 13px left-aligned (500,180) "measured: forearm"; 12px green (500,196) "acceleration + rotation".
- **Derived annotation** — orange `#d95926`: bold 13px left-aligned (86,200) "derived: elbow torque"; 12px orange (86,216) "motion + an arm model —"; 12px orange (86,230) "no force sensor anywhere".
- **Footer band:** tinted ink band `rgba(26,82,118,0.06)`, 34px tall at the bottom; 14px `#2c3e50` centered text "Rest days ride on a torque value no instrument measured — the sleeve senses motion, a model assigns the load."

## What does it collect?

- **Per-throw inertial burst** — acceleration and rotation samples around each detected throw
- **Throw detection and count** — itself a classifier deciding what counts as a throw
- **Derived per throw:** arm slot, arm speed, and an elbow-torque value
- **Season-long workload aggregates**, keyed to a named player account
- **Precision:** the limit is not the sensor but the strap — a sleeve seated a little differently reads a different torque from the same throw, and no error bar reports that

**Torque appears as a per-throw field:** it sits beside the measured burst in the record, with nothing in the schema marking it as a model estimate rather than a reading.

### Visualization (canvas `c2`, 720×320)

One outing's per-throw modeled-torque ticks (hardcoded illustrative array, 30 throws) drifting upward late in the outing, against a dashed horizontal workload guideline — every value labeled as model output.

- **Header strip:** tinted ink band, 28px; bold 15px ink centered title "One outing, throw by throw — every tick is a model output".
- **Disclaimer:** italic 11px mute right-aligned at (704,44): "illustrative values, not a measured outing".
- **Plot area:** padL=70, padR=36, top=60, baseline y=250; y scale 40→90 arbitrary units; gridlines `#e5e9ef` with 11px labels at 40/60/80; rotated 12px mute y-title "modeled torque (arb. units)"; 12px mute x-label "throws, in order" centered at y=268.
- **Data (30 throws, hardcoded, in order):** 56, 58, 54, 60, 57, 59, 55, 61, 58, 62, 60, 63, 59, 64, 62, 65, 61, 66, 64, 67, 66, 69, 68, 71, 70, 73, 72, 76, 75, 79 — a slow upward drift with the last few throws crossing the guideline.
- **Throw ticks:** violet `#4a3aa7` dots r3.5, evenly spaced across the plot in throw order.
- **Workload guideline:** dashed orange `#d95926` 2px horizontal line (dash 6/4) at 74; bold 12px orange left-aligned label just above the line at (padL+6): "workload guideline — a chosen cutoff, not a measured limit".
- **Footer band:** tinted ink band, 34px; 14px `#2c3e50` centered text "Warm-up and bullpen throws the detector misses never enter this total — the count is a classifier's count."

### Payload (below canvas `c2`)

Caption (`.payload-note`, italic): "Sample payload — illustrative structure, not real captured data."

```
// Reconstruction. Field names are generic;
// the vendor schema is not public.
{
  "player_id": "pl_2214",          // a named account
  "session":   "2026-08-22-game",

  // ── measured by the inertial unit ──
  "imu_burst": {
    "accel_g":   [ 3.1, 9.8, 24.6, "…" ],   // per-throw burst
    "gyro_dps":  [ 410, 1180, 2950, "…" ],
    "sample_hz": "vendor-set, not stated here"
  },

  // ── derived by the arm model ──
  "throw_detected":  true,         // itself a classifier call
  "arm_slot_deg":    47,
  "arm_speed":       612,          // arb. units
  "elbow_torque_nm": 38.2,         // limb-model output, no force sensor
  "workload_today":  41            // counted throws only
}
```

## Why is it collected?

**Stated purpose** (label pill `.lbl-purpose`)

- **Workload monitoring** — protecting the elbow ligament from overuse, throw by throw
- **Rehab pacing** — ramping a surgically repaired elbow back up on a schedule

**Additional consequence** (label pill `.lbl-effect`)

- **A longitudinal record of a named arm** — capability and stress across seasons, relevant to trades, contracts and insurance
- **Youth and amateur use** syncs to a vendor account under a consumer agreement, not a team's medical file

**The model was fitted on reference arms, not this arm:** a systematic offset for one pitcher's anatomy never shows up as an error bar — the torque just reads high or low, consistently, and the record cannot say which.

### Visualization (canvas `c3`, 720×320)

Two-panel picture: the same measured forearm rotation fed through two different assumed forearms (mass and length), yielding two different modeled torques — the assumption is the measurement.

- **Header strip:** tinted ink band, 28px; bold 15px ink centered title "Same motion in, two torques out — the assumed arm decides".
- **Divider:** vertical 1px 0.15-alpha ink dashed line (dash 3/3) at x=360, y 104→274.
- **Panel titles** (12px mute, centered): "assumed forearm: shorter, lighter" at (190,52); "assumed forearm: longer, heavier" at (530,52).
- **Panel A** (center x=190): faint 0.35-alpha ink upper arm (100,225)→elbow (150,190); ink 3.5px forearm (150,190)→hand (210,135); violet-tint 8px sleeve segment (165,176)→(183,160); solid violet sensor dot r5 at (170,171); hand dot r4 ink.
- **Panel B** (center x=530): faint upper arm (440,225)→elbow (490,190); ink 5.5px forearm — longer — (490,190)→hand (562,124); identical violet sleeve segment (505,176)→(523,160); identical sensor dot r5 at (510,171); hand dot r4 ink.
- **Measured-motion arcs** (identical in both panels — green `#008300` dashed 2px, dash 6/4): arc around each elbow, radius 34, from −1.4 to −0.1 rad, green arrowhead at the end.
- **Measured annotation** — green: bold 13px centered (360,86) "same measured rotation"; dashed 1.5px green leaders (dash 3/3) from (310,92) to (165,150) and from (410,92) to (485,150).
- **Torque readouts** — orange: bold 14px centered "modeled torque: 52" at (190,252) and "modeled torque: 63" at (530,252); 11px mute italic centered "arb. units — illustrative" under each at y=268.
- **Footer band:** tinted ink band, 34px; 14px `#2c3e50` centered text "Identical sensor data, two torque values — the assumed forearm is part of the measurement."

## Regeneration instructions

- **Layout:** tracking-page `.obj-table`: full-width bordered table, one `<tr>` per section; left `<td>` (45%) holds `.obj-title` + `.lede` + bullets + `.key-point` callouts + `.lbl` pills, right `<td>` (55%, centered) holds the canvas; the "What does it collect?" row also carries the `.payload-note` caption and `.payload` pre block under its canvas (both left-aligned).
- **Page style:** body system sans-serif (-apple-system stack), white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; subtitle `#666` 1.05em; table cell borders `1px solid #2980b9`, padding 16px; `.obj-title` bold `#1a5276` 1.1em; `li b` `#1a5276` weight 600, li 0.93em. No nav bar, no back/home links.
- **Callouts:** `.key-point` — background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px 14px, 0.93em, leading `<strong>` in `#1a5276`.
- **Label pills:** `.lbl` inline-block uppercase 0.7em bold, padding 2px 7px, radius 3px; `.lbl-purpose` background `#eaf2fb` color `#1a5276`; `.lbl-effect` background `#fdf0e6` color `#a8501c`.
- **Payload:** `.payload` — background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px, ui-monospace 0.78em, `white-space: pre`, line-height 1.45; `.payload-note` 0.82em `#666` italic.
- **Canvas:** each declares its intrinsic size in `width`/`height` attributes (c1 720×360, c2 and c3 720×320); a shared `setupCanvas(id)` reads the element's attributes, sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Helpers: `tint(hex, a)` for translucent fills from palette tokens, `rr()` rounded-rect, `arrowHead()` for arrow tips. Every canvas carries the tinted ink header band (28px, bold 15px ink centered title) and footer band (34px, 14px text centered). Charts hardcode literal data arrays (no Math.random); invented numbers are labeled illustrative on the canvas.
- **Palette (declared once as tokens):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`; ink `#1a5276` (headings/axes only), text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Sport hue for this page is violet `#4a3aa7`; measured annotations green `#008300`; derived annotations orange `#d95926`. Red is reserved for genuine alarm states and is not used on this page. Site-wide accent palette: #1a5276 primary blue, #27ae60 green, #e74c3c red, #e67e22 orange.
- In regenerated HTML, any card links use `.html` extensions (this page has none).
