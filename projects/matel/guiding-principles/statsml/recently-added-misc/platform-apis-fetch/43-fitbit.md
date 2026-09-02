# Fitbit Web API

**Page type:** detail page (two-column obj-table layout: text left 45%, payload/canvas right 55%, one row for the Overview section)
**HTML title tag:** Fitbit Web API — Platform APIs

**Subtitle:** Lets your server read a user's daily activity, sleep and heart data from Fitbit's cloud after their tracker syncs.

**Verified badge:** Last verified: August 2026

## Overview

### What you can get

- Daily totals for steps, distance, calories and active minutes
- Nightly sleep logs with minutes of deep, light and REM sleep
- Resting heart rate, heart-rate zones and nightly heart-rate variability
- Weight, body fat and temperature readings
- Device status: battery level and when it last synced

### Key point (callout)

**Fine-grained "intraday" data needs special approval — and without it the failure is silent.** By default you get one processed summary per day, never raw sensor data. Your own developer account can see its own intraday data, so code tested on yourself works — then quietly returns only coarse daily numbers for every other user, with no error raised.

### Watch out for

- Data exists only after the device syncs — today is always incomplete, and yesterday can still change days later
- Sleep logs come in two formats (with or without stages), and the same user can switch between them night to night
- Composite numbers like Sleep Score are Fitbit's own formulas and can shift when the software updates — prefer the underlying components

### Payload example (right column)

Payload note: **One night of sleep, as the API returns it** — minutes per stage, already computed by Fitbit

```json
{
  "sleep": [{
    "dateOfSleep": "2026-08-19",
    "minutesAsleep": 401,
    "efficiency": 92,
    "type": "stages",
    "levels": {
      "summary": {
        "deep":  { "minutes": 71 },
        "light": { "minutes": 233 },
        "rem":   { "minutes": 97 },
        "wake":  { "minutes": 56 }
      }
    }
  }]
}
```

Second payload note (chart caption): **One hour of heart rate at the three granularities the API offers.** Without intraday approval, only the flat daily value exists.

### Visualization (canvas `hrGranularity`, responsive width × 380)

Line chart: one deterministic 60-minute (3600 s) heart-rate signal shown at three API granularities — 1-second series, 1-minute binned means, and a flat daily resting-HR scalar.

- **Data generation (deterministic, exact formulas):** base signal per second t (0–3600): `hr = 66 + 3·sin(t/540)` (slow drift); sustained bout 1200 ≤ t < 1800: add `74·min(1, u/0.35)·(1 − 0.10u)` where `u = (t−1200)/600`; recovery tail 1800 ≤ t < 2200: add `74·exp(−(t−1800)/130)`; transient A (stairs, ~05:00, 45 s) 300 ≤ t < 345: add `46·sin(π(t−300)/45)`; transient B (startle/effort spike, ~41:00, 25 s) 2460 ≤ t < 2485: add `58·sin(π(t−2460)/25)`; transient C (brief effort, ~52:00, 30 s) 3120 ≤ t < 3150: add `40·sin(π(t−3120)/30)`. Deterministic noise ±~3 bpm: `noise(t) = (frac(sin(t·12.9898)·43758.5453) − 0.5)·6`. 1-minute series = mean of each 60 s window. Daily resting HR scalar = 58.
- **Series styling:** 1-second series thin dense line `#1a5276` width 0.6; 1-minute series line `#e67e22` width 2 plotted at bin centres; daily resting HR flat dashed line `#e74c3c` width 2, dash 7/5, at y=58.
- **Axes:** y from 50 to 160 bpm, gridlines `#e8e8e8` every 10, labels every 20 in `#555`; x ticks/gridlines every 10 minutes (0–60) in `#555`; axes `#2c3e50`. Axis titles: "Minutes into window" (bottom center) and rotated "Heart rate (bpm)" (left), both `#2c3e50` 11px. Padding: left 54, right 16, top 48, bottom 78.
- **Title (bold 13px, `#1a5276`, left-aligned):** "One heart-rate signal, three API granularities". Subtitle (11px, `#555`): "Same 60 minutes. Peaks visible at 1 s are attenuated at 1 min and absent from the daily scalar."
- **Annotation:** dashed gray `#888` leader line from the transient-B peak (t=2472 s) to two lines of right-aligned 10.5px `#555` text: "25 s transient: ~N bpm at 1 s," / "~M bpm once binned to 1 min" where N = the rounded 1-s value at t=2472 and M = the rounded minute-41 bin mean (computed from the data).
- **Legend (bottom left):** line samples with labels in `#555`: `#1a5276` solid "1 s intraday (approval-gated)"; `#e67e22` solid "1 min intraday (approval-gated)"; `#e74c3c` dashed "daily resting HR (default access)".
- **Footnote (italic 10.5px, red `#e74c3c`, bottom left):** "Under default scopes only the dashed scalar exists. Any analysis of short-lived events is unidentifiable from it."
- **Behavior:** redraws on window resize; width follows container.

## Official API References

- [Fitbit Web API Reference](https://dev.fitbit.com/build/reference/web-api/) — full endpoint reference for activity, sleep, heart rate, HRV, SpO2, body and device resources
- [Intraday](https://dev.fitbit.com/build/reference/web-api/intraday/) — second- and minute-level series and the access-approval requirements that gate them

## Regeneration instructions

- **Layout:** single-page detail doc: h1, `.subtitle` paragraph, `.verified` badge, then `## Overview` (h2 with 2px `#2980b9` bottom border) containing one `.obj-table` (full width, one `<tr>`: left `<td>` 45% with `.section-label` headings + bullet lists + `.key-point` callout; right `<td>` 55% with `.payload-note` paragraphs, `<pre>` JSON payload, and the canvas), then `## Official API References` as a plain `<ul>` of external links.
- **Page CSS:** body system sans-serif (-apple-system stack), line-height 1.6, text `#2c3e50`, white background, padding 30px 40px; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; `.verified` badge inline-block, background `#eaf2f8`, border `1px solid #2980b9`, text `#1a5276`, padding 2px 10px, radius 4px, 0.8em; h2 1.3em `#1a5276` with `border-bottom: 2px solid #2980b9`; `.section-label` bold `#1a5276` block; `.key-point` background `#f8f9fa`, left border `3px solid #e74c3c`, padding 10px 14px, 0.93em; `.payload-note` 0.85em `#555`; `pre` background `#f4f4f4`, padding 14px, radius 6px, 0.82em; `code` background `#f4f4f4`; links `#1a5276`; li/p 0.93em. No nav bar, no back/home links.
- **Canvas:** `width: 100%` via CSS, height attribute 380; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates), redraw on resize.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, bar fill `rgba(26,82,118,0.35)`; grid `#e8e8e8`, axis `#2c3e50`, muted text `#555`/`#888`.
- In regenerated HTML, any card links use `.html` extensions (this page has only external links).
