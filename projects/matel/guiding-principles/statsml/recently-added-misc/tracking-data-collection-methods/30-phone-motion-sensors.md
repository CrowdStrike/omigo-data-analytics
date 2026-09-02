# Tracking Data: Phone Motion Sensors

**Page type:** detail page (two-column obj-table layout: text left 45%, canvas/payload right 55%, one row per section)
**HTML title tag:** Tracking Data: Phone Motion Sensors

**Subtitle:** The accelerometer and gyroscope report acceleration and rotation rate many times a second. Activity labels are classifier output computed from those samples, not readings.

## Section 1: What is it?

Lede: Six numbers, sampled many times a second.

- **Accelerometer:** acceleration along three axes
- **Gyroscope:** rate of rotation about three axes
- **Sampling rate:** tens to hundreds of times a second, depending on what the requesting code asks for
- **Everything else is computed** — orientation, step count and activity label come from integrating or differentiating the series and comparing against a model or threshold

Key point callout: **Access gates:** web pages reach these sensors through a documented browser event. Access was originally ungated, and browsers later moved motion and orientation behind a permission request; native apps read the same sensors under whatever the platform requires. For analysis, the gate controls who subscribes, not what the sensor resolves.

### Visualization (canvas `c1`, 720×320)

Schematic diagram: a phone body with three labeled measurement axes and rotation arrows, plus text annotations.

- **Phone:** rounded rect (radius 8) at center-left of canvas center, 60×100, fill `#34495e`; light screen inset fill `#e5e9ef`; small home-button circle stroked `#7f8c8d`.
- **X axis:** horizontal magenta (`#d55181`) 2.5px line through the phone's vertical center, extending 60px beyond each side, filled arrowhead on the right, bold 16px label "X".
- **Y axis:** vertical green (`#008300`) 2.5px line through the phone, arrowhead at top, bold label "Y".
- **Z axis:** diagonal blue (`#2a78d6`) 2.5px line (depth direction, lower-right to upper-left), arrowhead at upper-left, bold label "Z".
- **Rotation arrows:** dashed (4/3) 1.5px arcs — magenta arc around the X axis to the right of the phone, green arc around the Y axis above the phone.
- **Right-side text (14px `#555`, at x=420):** "Accelerometer: detects movement" (y=40), "Gyroscope: detects rotation" (y=60), "3 axes each = 6 data streams" (y=80).
- **Left-side labels (14px `#555`, right-aligned at x=130):** "Tilt" (y=80), "Shake" (y=100), "Rotate" (y=120), "Move" (y=140).
- **Bottom-right title (bold 15px `#2a78d6`, x=420):** "Six measured series —" (y=200) / "everything else is computed" (y=218).

## Section 2: What does it collect?

- **Acceleration** on three axes, with and without gravity
- **Rotation rate** about three axes
- **Sampling interval** the subscriber asked for
- **Derived: orientation**, from integrating rotation rate
- **Derived: step count and cadence**, from peak spacing
- **Derived: activity label** — walking, driving, still
- **Derived: carry mode** — held, or resting on a surface

Key point callout: **Interval sets the ceiling:** at a coarse interval only gross activity survives; at a fine one the same stream carries the small oscillations gait and keystroke inference depend on. Everything below the split is a window-level classification, not a reading — and the null shows the classifier declining to commit.

Key point callout: **Derived fields are noisier than their source:** differencing to get a rate amplifies noise, since adjacent samples differ little while their independent errors add — and each further derivative is worse. Integration fails the other way, accumulating small bias into unbounded drift. So `step_cadence` and `gait_vector` are not "the same data, summarised"; they carry error the accelerometer output does not, which is why these pipelines low-pass filter before differentiating and why a fine interval alone does not buy precision.

### Visualization (canvas `c2`, 720×320)

Four-segment timeline of schematic sensor traces, one activity per quarter-width segment, each with a distinct waveform shape.

- **Segments (each 180px wide, tinted background from y=30 to h-30, dashed `#e5e9ef` dividers):**
  - "Walking" — background `rgba(42,120,214,0.08)`; trace in `#2a78d6` (2.5px): rhythmic compound sine `sin(x*0.12)*40 + sin(x*0.24)*15` around the vertical center baseline (y=160)
  - "Driving" — background `rgba(0,131,0,0.08)`; trace in `#008300`: smooth low wave `sin(x*0.02)*8` plus two localized bumps (x 60-80 amplitude 25; x 130-145 amplitude 18)
  - "Typing" — background `rgba(217,89,38,0.08)`; trace in `#d95926`: small rapid oscillation `sin(x*0.5)*6` plus fixed tap spikes (every 7th sample, ±8)
  - "Idle" — background tint of `#d55181` at 0.08; trace in `#d55181`: near-flat `sin(x*0.8)*1.5`
- **Baseline:** thin `#e5e9ef` horizontal line at mid-height from x=20 to x=700.
- **Top labels (bold 14px, centered per segment, in each trace color):** "Walking", "Driving", "Typing", "Idle".
- **Bottom descriptions (13px `#555`, centered per segment):** "Periodic", "Smooth + shocks", "Low amplitude — hard case", "Near flat".
- **Caption (italic 12px `#6b7280`, top center at y=12):** "Schematic shapes, not recorded traces — coarse classes separate because the shapes differ".

Below the canvas, payload note (italic gray): "Sample payload — illustrative structure, not real captured data."

Payload block:

```
{
  // ── documented in public API (DeviceMotionEvent) ──
  "interval": 16,                 // ms between samples
  "acceleration":               { "x": 0.11, "y": -0.04, "z": 0.27 },
  "accelerationIncludingGravity":{ "x": 0.09, "y": -9.73, "z": 0.31 },
  "rotationRate":               { "alpha": 1.8, "beta": -0.6, "gamma": 0.2 },
  // units: acceleration m/s², rotationRate deg/s

  // ── inferred / plausible, added downstream ──
  "activity":      "walking",     // classifier over a window of samples
  "step_cadence":  1.9,           // steps/sec, from peak spacing
  "carry_mode":    "in_hand",
  "gait_vector":   [0.31, -0.08, …],
  "surface":       null           // stairs vs flat: undetermined
}
```

## Section 3: Why is it collected?

Label pill (Stated purpose):

- **The interface needs them** — rotating the screen, tilt in games, step counting, steadying the camera

Label pill (Additional consequence):

- A stream fine enough to rotate the screen is **fine enough to tell walking from driving**, and to count paces
- None of that needs a new sensor or a permission — **just different arithmetic on the same numbers**

Key point callout: **It measures a phone, not a person:** a way of walking is not a name, and the signal changes more when the phone moves from pocket to bag than it does between two people.

### Visualization (canvas `c3`, 720×320)

One hardcoded accelerometer trace read two ways: the slow average (screen rotation) and the peak spacing (pace/activity).

- **Title (bold 13px `#1a5276`, centered at y=22):** "One accelerometer stream".
- **Trace data (28 samples, plotted left to right, `#2a78d6` 2px line):** `[0.10, 0.62, 0.28, -0.18, 0.05, 0.70, 0.34, -0.12, 0.02, 0.66, 0.30, -0.16, 0.08, 0.72, 0.36, -0.10, 0.04, 0.64, 0.26, -0.14, 0.06, 0.68, 0.32, -0.15, 0.09, 0.71, 0.33, -0.11]`.
- **Plot geometry:** left pad 34, right pad 18, trace band top y=40 height 96; zero line in `#e5e9ef`; y scaled so value 1.0 spans traceH/2.4.
- **Reading 1 (aqua `#199e70`):** dashed (6/4) 2px horizontal line at the mean of the trace; bold 12px left-aligned label below it: "which way is up  →  rotate the screen".
- **Reading 2 (orange `#d95926`):** 4px-radius dots at stride-peak indices `[1, 5, 9, 13, 17, 21, 25]`; a bracket between peaks 9 and 13 below the trace labeled "one stride" (centered, 12px); bold 12px label to the right of the bracket: "spacing of the peaks  →  pace, walking or driving".
- **Captions (bottom center):** italic 12px `#2c3e50`: "The second reading needs no extra sensor and no permission — the peaks were always there."; italic 11px `#6b7280`: "Illustrative trace — shape only, not measured data."

## Regeneration instructions

- **Layout:** tracking-page `.obj-table`: full-width `border-collapse: collapse` table, one `<tr>` per section; left `<td>` (45%) holds `.obj-title` + `.lede` + bullets + `.key-point` callouts, right `<td>` (55%, text-align center) holds the canvas, and in the "What does it collect?" row also the `.payload-note` and `.payload` `<pre>` (both left-aligned).
- **Page style:** body system sans-serif, white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; table cell borders `1px solid #2980b9`, padding 16px; `.obj-title` bold 1.1em `#1a5276`; li 0.93em with `li b` in `#1a5276` weight 600.
- **Callouts:** `.key-point` — background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px 14px, 0.93em, leading `<strong>` in `#1a5276`.
- **Label pills:** `.lbl` inline-block uppercase 0.7em bold, padding 2px 7px, radius 3px; `.lbl-purpose` background `#eaf2fb` color `#1a5276`; `.lbl-effect` background `#fdf0e6` color `#a8501c`.
- **Payload:** `.payload` background `#f8f9fa`, left border `3px solid #1a5276`, ui-monospace 0.78em, `white-space: pre`; `.payload-note` 0.82em italic `#666` directly above.
- **Canvas:** 720×320 intrinsic attributes; a shared `setupCanvas(id)` reads the element's own width/height attributes and scales by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates). Charts use hardcoded literal data arrays (no Math.random), with a `tint(hex, alpha)` helper for translucent fills and an `rr()` rounded-rect helper.
- **Palette (tracking-set tokens, declared once as `P`):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Red is reserved for alarm states; navy `#1a5276` is ink only (headings, axes, callout borders). Project-wide palette anchors: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- No nav bar, no back/home links. In regenerated HTML, any card links elsewhere use `.html` extensions.
