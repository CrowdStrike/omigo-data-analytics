# Tracking Data: Fitness Wearables

**Page type:** detail page (two-column obj-table layout: text left 45%, canvas right 55%, one row per section)
**HTML title tag:** Tracking Data: Fitness Wearables

**Subtitle:** A wrist sensor package sampling continuously. Almost everything it reports — sleep stage, stress, calories — is a model output, not a measurement.

## What is it?

A wrist sensor package sampling continuously and syncing to the vendor.

- **Hardware:** optical heart-rate sensor, accelerometer and gyroscope, often GPS, and on some models blood oxygen and skin temperature
- **Optical sensor measures** light returning from the skin, from which a pulse rate is derived
- **Accelerometer measures** wrist acceleration
- **Not sensed:** sleep stages, stress, recovery and calorie burn are inferred from those two signals by a proprietary model
- **Sport-specific variants:** golf swing sensors on the glove or club, radio tags in American-football shoulder pads, GPS vests under a soccer shirt — the same worn-sensor pattern, tuned to one sport

Key-point callout: **Measurement vs model output:** pulse rate and wrist motion are measurements. Everything presented as a health insight is a model output. The interface displays both in the same typeface.

### Visualization (canvas `c1`, 720×320)

24-hour clock schematic showing continuous data collection.

- **Clock face:** circle of radius 90 centered in the canvas, stroke `#2a78d6` 3px.
- **Activity sectors** (filled pie wedges inside radius 87): Sleep from -90° to 30° in `rgba(26,82,118,0.15)`; Commute 30°–90° in `rgba(39,174,96,0.2)`; Work 90°–180° in `rgba(230,126,34,0.15)`; Exercise 180°–270° in magenta tint `rgba(213,81,129,0.15)`.
- **Hour marks:** 24 ticks between radius 80 and 87, stroke `#2c3e50`, heavier (2px) every 6th tick.
- **Pulse dots:** 48 small magenta `#d55181` dots (r=2) on a ring at radius 102 — data points around the clock.
- **Center text (bold 16px `#2a78d6`):** "24/7".
- **Bottom caption (13px `#6b7280`, centered):** "Collecting data every moment — awake or asleep".

## What does it collect?

- **Pulse rate**, at intervals set by the device and its battery mode — typically around once per second during a tracked workout and every few seconds to a minute in all-day mode; vendor APIs commonly expose per-second and per-minute series
- **Wrist acceleration**, from which steps and distance are counted
- **Blood oxygen estimate and skin temperature**, on models with those sensors
- **Glucose readings** relayed from a paired continuous glucose monitor (CGM) patch, on setups that link one
- **GPS track** during a recorded activity
- **Derived:** sleep stage per interval, a stress or readiness score, calorie estimates
- **Self-reported entries** — weight, cycle tracking, mood
- **Event flags** such as an irregular-rhythm notification or a detected fall
- **From sport variants:** swing motion or a radio ping is what the sensor produces; swing tempo, club speed and on-field position are computed from it

Key-point callouts:

- **Sampling interval is a field, not a constant:** it changes with battery mode, so resolution varies within one person's own history — and a comparison across that boundary compares two different instruments.
- **The nulls are structural:** blood oxygen is typically a spot check, not a continuous stream, so a sparse series is expected. Averaging it as though continuous weights whichever moments the device chose to sample.

### Visualization (canvas `c2`, 720×470)

Two stacked bands: measured signals vs derived outputs, joined by a "proprietary model" arrow.

- **Title (bold 17px `#2a78d6`, centered):** "What the sensors measure, and what the model produces".
- **MEASURED band:** rect 20,36 to width-40 × 168, fill `rgba(0,131,0,0.07)`, stroke green `#008300`; band label (bold 15px green): "MEASURED  —  a sensor produced this number". Contains three traces spanning x=150 to x=690:
  - "pulse rate" (right-aligned label, sub-label "optical sensor" in `#6b7280`): magenta `#d55181` line 1.8px over hardcoded 30-value array `[58,57,59,58,60,62,74,88,96,112,128,134,120,102,88,80,76,74,72,70,68,66,62,60,58,57,58,60,59,58]`, normalized into a 40px-high strip at y=62.
  - "wrist motion" (sub-label "accelerometer"): bar strip at baseline y=152, bars `rgba(42,120,214,0.55)` scaled to max 80 over 34px height, from array `[0,0,0,0,2,8,26,44,58,72,80,64,40,18,6,2,0,0,4,2,0,0,0,0,0,0,2,0,0,0]`.
  - "blood oxygen" (sub-label "spot checks only"): a thin `#e5e9ef` line at y=182 with blue `#2a78d6` dots (r=4) only at indices [1, 7, 14, 22, 28] of 29 slots; caption beneath (13px `#6b7280`, centered): "gaps are not zero values — nothing was sampled".
- **Divider arrow:** orange `#d95926` vertical arrow at mid-width from y=206 to 236 with the bold label "proprietary model" to its right.
- **DERIVED band:** rect 20,242 to width-40 × 168, fill `rgba(217,89,38,0.07)`, stroke orange; band label (bold 15px orange): "DERIVED  —  a model produced this, from the two signals above". Contains:
  - "sleep stage" row: a segmented horizontal bar (y=274, height 24) of stage blocks in order light(4), deep(6), rem(3), light(5), awake(2), deep(4), rem(6) — widths proportional to counts; colors: light `rgba(42,120,214,0.30)`, deep `rgba(42,120,214,0.65)`, rem `rgba(217,89,38,0.55)`, awake magenta tint `rgba(213,81,129,0.45)`. Caption (13px `#6b7280`, centered): "crisp boundaries, inferred from two continuous signals".
  - Three score bars (150×14 at y=336, gray track `#e5e9ef`, orange fill proportional to value, orange bold value at the right end): "sleep score" 82, "stress score" 41, "readiness" 60.
  - Caption (14px `#6b7280`, centered): "No error bar is shown on any of the three. The interface presents them like readings."
- **Bottom caption (14px `#6b7280`, centered):** "Schematic. Values are illustrative, not measured data."

### Sample payload (right column, under canvas `c2`)

Caption (italic): "Sample payload — illustrative structure, not real captured data."

```
// Field names are placeholders. No consumer wearable
// publishes its sync schema, so this whole block is
// reconstruction — but the measured/derived split is
// the part worth reading.
{
  "user_id": "u_4471…",
  "device":  { "model": "band-3", "fw": "9.2.1" },

  // ── measured by a sensor ──
  "samples": {
    "pulse_bpm":   [58, 57, 59, 61, 60],   // one per interval
    "accel_counts": [0, 0, 12, 4, 0],
    "spo2_pct":    [96, null, 95, null, null],  // sparse: spot checks
    "skin_temp_delta_c": [-0.2, -0.3, -0.1, 0.0, 0.1]
  },
  "sample_interval_s": 60,   // varies with battery mode

  // ── inferred / plausible — model output, not measured ──
  "sleep_stages":   ["light","deep","deep","rem","awake"],
  "sleep_score":     82,
  "stress_score":    41,
  "calories_active": 512,
  "readiness":       "moderate"
}
```

## Why is it collected?

Label pill: STATED PURPOSE

- **Activity feedback** and sleep and fitness trends for one wearer
- **Notifications** for irregular rhythm or a detected fall

Label pill: ADDITIONAL CONSEQUENCE

- The record is **continuous, timestamped and tied to one person**, which suits **wellness, insurance and employer schemes**
- Whether it leaves the vendor is a **policy decision**, not a technical one

Key-point callout: **Built for a trend, read as a level:** the sensor and the scoring model only had to be useful on one person's own history, where a steady bias cancels and the direction of change is what matters. Comparing an absolute score between two wrists was never a design target. And a stress score has an error rate the wearer cannot see, so if it feeds a premium there is no channel to contest it.

### Visualization (canvas `c3`, 720×320)

Line chart: the same score read two ways — within one wrist the trend survives a steady offset; across two wrists the offset is the whole comparison.

- **Titles (centered):** bold 14px `#1a5276` "Two wearers, identical week, two devices"; sub-line 12px `#6b7280` "the shapes agree; the levels are 16 points apart".
- **Data (illustrative readiness scores, Mon–Sun):** wearer A `[71, 68, 63, 66, 74, 79, 77]`; wearer B `[55, 52, 47, 50, 58, 63, 61]` — same shape, 16-point offset. Cut-off constant 60.
- **Axes:** x labeled Mon…Sun; y ticks at 40, 60, 80; y maps value range 35–90 between baseline y=224 and top y=62; padding left 52, right 132; axis/grid color `#e5e9ef`, labels `#6b7280` 11px.
- **Cut-off line:** dashed violet `#4a3aa7` (dash 6/4) horizontal at 60, labeled to the right in bold 12px violet: "a scheme’s cut-off".
- **Series:** wearer A in blue `#2a78d6`, wearer B in orange `#d95926`; lines 2.5px with 3.5px dots; series name at the right end of each line in bold 12px.
- **Offset marker:** vertical aqua `#199e70` 2px line at Wednesday between the two series, labeled in bold 12px aqua: "16 points of device offset".
- **Captions (centered):** italic 12px `#2c3e50` "Both wearers dipped Wednesday and recovered. Only one clears the line."; italic 11px `#6b7280` "Illustrative scores — the offset is schematic, not a measured device bias."

## Regeneration instructions

- **Layout:** tracking detail page — `<table class="obj-table">`, one `<tr>` per section; left `<td>` (45%) holds `.obj-title` heading, optional `.lede`, bullets with bold lead terms (`li b` in `#1a5276`), `.key-point` callouts, `.lbl` label pills; right `<td>` (55%, text-align center) holds the canvas, and for the "What does it collect?" row also the `.payload-note` caption plus `<pre class="payload">` block (left-aligned).
- **Page CSS:** body system sans-serif (-apple-system stack), white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; `.obj-title` bold 1.1em `#1a5276`; td borders `1px solid #2980b9`, padding 16px; li 0.93em; `.key-point` background `#f8f9fa`, left border `3px solid #1a5276`, 0.93em, `strong:first-child` in `#1a5276`; `.lede` 0.95em; `.lbl` pills 0.7em bold uppercase radius 3px — `.lbl-purpose` `#eaf2fb`/`#1a5276`, `.lbl-effect` `#fdf0e6`/`#a8501c`; `.payload` background `#f8f9fa`, left border `3px solid #1a5276`, ui-monospace 0.78em, white-space pre; `.payload-note` 0.82em italic `#666`. No nav bar, no back/home links.
- **Canvas:** each canvas declares intrinsic `width`/`height` attributes; a shared `setupCanvas(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Helpers: `tint(hex, a)` for translucent fills from palette hexes, and rounded-rect helper `rr()`.
- **Palette:** charts use the validated categorical palette `P = { blue: #2a78d6, green: #008300, magenta: #d55181, yellow: #c98500, aqua: #199e70, orange: #d95926, violet: #4a3aa7, ink: #1a5276, text: #2c3e50, mute: #6b7280, grid: #e5e9ef }`; red is deliberately not in the rotation (reserved for genuine alarm states). Site palette anchors: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- In regenerated HTML, any card links elsewhere use `.html` extensions.
