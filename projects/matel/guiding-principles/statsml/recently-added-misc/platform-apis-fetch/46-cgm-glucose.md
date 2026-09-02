# CGM / Continuous Glucose Monitors

**Page type:** detail page (two-column obj-table layout: text left 45%, payload + canvas right 55%, single Overview row, followed by an API-references list)
**HTML title tag:** CGM / Continuous Glucose Monitors — Platform APIs

**Subtitle:** Lets you pull glucose readings, taken every few minutes, from a user's Dexcom or Abbott sensor after they consent.

**Verified badge:** Last verified: August 2026

## Overview

**What you can get** (section label, left column)

- A glucose estimate roughly every 5 minutes (Dexcom), with a trend arrow per reading
- User-logged meals, insulin doses and exercise events
- Sensor and device details, plus the date range of data that actually exists for each user
- Abbott/Libre: mainly clinic reports and CSV exports — there is no open developer API like Dexcom's

**Key-point callout (red left border):**

**A CGM does not measure blood glucose.** The sensor sits in fluid under the skin, so its readings lag blood glucose by several minutes — and lag most exactly when glucose is changing fastest, like after a meal or during exercise. A CGM curve that peaks later and lower than the true blood peak is the instrument working as designed, not an error.

**Watch out for** (section label, left column)

- Gaps are built in: sensor warm-up, replacement every couple of weeks, and lost phone connections — and they cluster at exactly the times that matter
- Extreme highs and lows are capped at display limits, so the worst readings are understated
- Each reading carries two clocks — device time and the user's local time — and mixing them corrupts any time-of-day analysis
- Data arrives only after upload: this is a look-back tool, not something to build alerts or treatment decisions on

**Payload note (right column):** **Two Dexcom readings** — a normal value, and a null with a status explaining why. Nulls are a state, not a mistake.

Code block (`pre`), verbatim:

```
{
  "records": [
    { "systemTime":  "2026-08-21T06:45:12",
      "displayTime": "2026-08-21T08:45:12",
      "value": 96, "unit": "mg/dL",
      "trend": "flat",
      "status": null },
    { "systemTime":  "2026-08-21T07:05:12",
      "displayTime": "2026-08-21T09:05:12",
      "value": null,
      "trend": "notComputable",
      "status": "sensorWarmup" }
  ]
}
```

**Chart caption (payload-note above canvas):** **Blood glucose (dashed) vs what the CGM reports over 24 hours** — later and lower peaks, and a data gap at sensor change.

### Visualization (canvas `cgmLagCanvas`, responsive width × 380)

Line chart: 24-hour "true" blood glucose curve (dashed red) vs a simulated CGM interstitial estimate (blue 5-minute sample points joined), with a target band, a sensor-change gap, and a lag annotation at the breakfast peak.

- **Data model (deterministic, computed):**
  - True blood glucose: baseline `92 + 5*sin((t-3)/24 * 2π)` plus three asymmetric meal excursions (fast rise, slower exponential decay) at `{t: 8.0, amp: 170, rise: 0.20, fall: 0.80}` (breakfast), `{t: 13.0, amp: 158, rise: 0.22, fall: 0.90}` (lunch), `{t: 19.0, amp: 185, rise: 0.19, fall: 0.95}` (dinner); each bump `amp * (1 - exp(-d/rise)) * exp(-d/fall)` for `d = t - meal.t > 0`.
  - CGM series: interstitial compartment modeled as a first-order lag on the blood curve with time constant TAU = 8/60 hours (8 minutes), integrated at dt = 1/600 h — one mechanism produces both the delay and the peak attenuation.
  - CGM sample grid: every 5 minutes (5/60 h) from 0 to 24, with a structural gap from t=15.0 to t=17.0 (sensor change).
  - Breakfast peaks located by search near t=8.4 on both curves for the lag annotation.
- **Layout:** height 380; padding left 54, right 22, top 52, bottom 68. x maps t∈[0,24] hours across plot width; y maps glucose 40–300 mg/dL.
- **Target band:** 70–180 mg/dL filled `rgba(39,174,96,0.10)` with boundary lines `rgba(39,174,96,0.55)`; right-aligned green (`#27ae60`) 10px label just above the 70 line: "target band 70–180 mg/dL".
- **Gridlines:** `#e8e8e8`, horizontal every 40 mg/dL from 40 to 300, vertical every 3 hours.
- **Sensor gap:** region 15:00–17:00 shaded `rgba(231,76,60,0.07)` with dashed (`[3,3]`) red `#e74c3c` vertical boundary lines; centered red 10px two-line label near the top: "sensor change + warm-up:" / "no data".
- **Blood glucose trace:** dashed (`[6,4]`) red `#e74c3c`, width 1.9, sampled every 0.5 min.
- **CGM trace:** solid blue `#1a5276`, width 1.8, joining the 5-minute points but broken across the gap (line restarts when spacing > 0.2 h); 1.7px-radius blue dots at every sample.
- **Lag annotation (orange `#e67e22`):** horizontal line 30px above the higher of the two breakfast peaks connecting blood-peak x to CGM-peak x, with filled triangular arrowheads at both ends pointing inward; dashed (`[2,3]`) vertical droppers from the annotation line down to each peak; 10.5px left-aligned label to the right of the CGM peak: "interstitial lag: the CGM peak arrives later and lower".
- **Axes:** `#2c3e50` L-shaped y/x axes; 11px `#555` ticks — y labels 40..300 step 40; x labels "00:00" through "24:00" every 3 hours (zero-padded HH:00).
- **Axis titles (`#2c3e50`):** x "Time of day (local display clock)" centered below; y "Glucose (mg/dL)" rotated −90° at left.
- **Title (top left):** bold 13px `#1a5276` "Blood glucose vs CGM interstitial estimate over 24 hours"; 11px `#888` sub-line "Illustrative: lag, peak attenuation, and a structural sensor gap".
- **Legend (bottom row, 10.5px, `#555` text):** dashed red line swatch "blood glucose (reference)"; blue line-with-dot swatch "CGM EGV, 5-min samples"; `rgba(39,174,96,0.35)` filled rect swatch "target range".
- Redraws on window resize.

## Official API References

- [Dexcom Developer Portal](https://developer.dexcom.com/) — API documentation, endpoint reference (egvs, events, calibrations, devices, dataRange), sandbox and app registration
- [Apple HealthKit](https://developer.apple.com/documentation/healthkit) — the on-device store where Libre and other CGM reader apps write glucose samples on iOS

## Regeneration instructions

- **Layout:** platform-APIs detail page. h1, `.subtitle` paragraph, `.verified` badge span, then `h2` "Overview" with a single-row `.obj-table` (left `<td>` 45%: section labels + bullet lists + one `.key-point` callout; right `<td>` 55%: payload note + `<pre>` JSON + chart note + `<canvas>`), then `h2` "Official API References" with a link list.
- **Page CSS:** body system sans-serif (-apple-system stack), line-height 1.6, text `#2c3e50`, white background, padding 30px 40px. h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; `.verified` inline badge — background `#eaf2f8`, border `1px solid #2980b9`, text `#1a5276`, padding 2px 10px, radius 4px, 0.8em. h2 1.3em `#1a5276` with `border-bottom: 2px solid #2980b9`. `pre` background `#f4f4f4`, padding 14px, radius 6px, 0.82em. `.key-point` background `#f8f9fa`, `border-left: 3px solid #e74c3c`, padding 10px 14px, 0.93em. `.section-label` bold `#1a5276` block. `.payload-note` 0.85em `#555`. `li`/`p` 0.93em; links `#1a5276`; `code` background `#f4f4f4`. No nav bar, no back/home links.
- **Canvas:** `<canvas id="cgmLagCanvas" height="380">`, CSS `display:block; width:100%`; drawing code reads `getBoundingClientRect().width`, sets backing store to `rect.width * dpr` / `380 * dpr` using `window.devicePixelRatio`, fixes CSS height to 380px, `ctx.scale` back to logical coordinates, and re-renders on `resize`.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`; grid `#e8e8e8`; axis/tick text `#555`/`#2c3e50`; muted `#888`.
- In regenerated HTML, any card/page links use `.html` extensions.
