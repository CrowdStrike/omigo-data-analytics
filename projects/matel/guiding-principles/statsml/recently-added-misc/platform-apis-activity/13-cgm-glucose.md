# CGM / Glucose Monitor APIs

**Page type:** detail page (two-column obj-table layout: text left 45%, payload/canvas right 55%, one row per section)
**HTML title tag:** CGM / Glucose Monitor APIs

**Subtitle:** Continuous interstitial glucose from arm-worn sensors — over-the-counter since 2024, no prescription required

## What the API Provides

- **Estimated glucose values (EGVs):** one reading roughly every 5 minutes, in mg/dL or mmol/L
- **Trend:** direction arrow and rate of change per reading (rising, falling, flat)
- **Devices:** sensor model, transmitter generation, display device
- **Events:** user-logged meals, insulin, exercise entries
- **Calibrations:** user fingerstick calibration entries, where the device supports them
- **Alerts:** high/low threshold crossings and urgent-low events
- **Data range:** earliest and latest available timestamps per record type
- **OTC shift:** the FDA cleared over-the-counter CGMs in 2024 (Dexcom Stelo in March, Abbott Lingo and Libre Rio in June) — sensors are now sold to wellness users with no prescription and no clinician in the loop

### Payload block (right column, `.payload` div)

```
// ── illustrative payload; field names follow the documented
//    Dexcom API v3 EGV response shape, values are not real ──
{
  "recordType": "egv",
  "recordVersion": "3.0",
  "userId": "EXAMPLE_USER_redacted",  // Note: masked for illustration
  "records": [
    {
      "systemTime":  "2026-08-22T07:45:00Z",
      "displayTime": "2026-08-22T09:45:00",
      "value": 112,                // mg/dL — an estimate, not a blood draw
      "trend": "flat",
      "trendRate": 0.3,            // mg/dL per minute
      "unit": "mg/dL",
      "rateUnit": "mg/dL/min",
      "transmitterGeneration": "g7",
      "displayDevice": "receiver"
    }
  ]
}
```

### Visualization (canvas `glucoseCanvas`, width 100% × 380)

One illustrative day of 5-minute EGVs (288 samples, midnight to midnight), drawn as a single line against a shaded target band.

- **Title (bold 13px, `#1a5276`, top center):** "One Day of 5-Minute EGVs (illustrative)".
- **Y-axis:** mg/dL, 40–220, labels at 70, 100, 140, 180 (`#2c3e50` 11px, right-aligned left of plot).
- **Target band:** 70–180 mg/dL shaded `rgba(39,174,96,0.12)` with `#27ae60` dashed edges labeled "70" and "180".
- **Curve (`#1a5276`, 2px):** deterministic piecewise segments — overnight ~95 flat; a dip to ~62 around 3am; back to ~90 by 5am; breakfast spike to ~165 peaking ~8:45am, settling ~110 by 10:30am; lunch spike to ~150 peaking ~1:30pm; dinner spike to ~175 peaking ~7:45pm; easing to ~100 by midnight. Values linearly interpolated between control points — no randomness.
- **Annotations (11px):** "compression low — pressure on the sensor, not hypoglycemia" with an arrow to the 3am dip (`#e67e22`); "meal rises" above the breakfast peak (`#666`).
- **X-axis:** labels 12am, 4am, 8am, 12pm, 4pm, 8pm, 12am (`#2c3e50` 10px, centered) with small `#999` tick marks.
- **Bottom caption (10px `#6b7280`, centered):** "Illustrative curve — values are constructed, not measured data."
- **Margins:** left 50, right 20, top 40, bottom 60.

## Access & Authentication

- OAuth 2.0 authorization code flow (Dexcom API); the user grants access to their own data
- Sandbox environment with synthetic users for development — no real accounts needed
- **API base:** https://api.dexcom.com/v3/users/self/
- **Endpoints:** egvs, devices, events, calibrations, alerts, dataRange
- Abbott's ecosystem routes through LibreView/LibreLinkUp apps; there is no comparable open developer API — third-party integrations commonly rely on unofficial LibreLinkUp endpoints
- Consumer apps also write glucose into Apple HealthKit and Android Health Connect, which is often the practical integration path

### Right column: key-point heading "**Request shape:**" followed by a `.payload` block

```
GET /v3/users/self/egvs?startDate=2026-08-21T00:00:00&endDate=2026-08-22T00:00:00
Host: api.dexcom.com
Authorization: Bearer (your-access-token)

// OAuth 2.0 authorization code flow; user consents
// to sharing their own glucose records.
// Sandbox: sandbox-api.dexcom.com with synthetic users.
// Abbott/Libre: no equivalent open API — data reaches
// third parties via LibreLinkUp (unofficial) or via
// HealthKit / Health Connect on the phone.
```

## Granularity & Limitations

- **Interval:** ~5 minutes per EGV (288/day); gaps appear during sensor warm-up, signal loss, and between sessions
- **Delayed, not real-time:** the public API serves recent history with a built-in delay — live values stay in the vendor's own app ecosystem
- **Interstitial, not blood:** the sensor reads interstitial fluid, which lags blood glucose by several minutes — fastest-moving values are the least trustworthy
- **EGV means estimated:** an algorithm converts sensor current to mg/dL; accuracy is characterized by a relative error (MARD) of several percent, not by an error bar per reading
- **Sensor sessions are finite:** 10–15 days per sensor depending on model, so a long series is a chain of sensors, each with its own bias and warm-up
- **Artifacts are structural:** pressure on the sensor during sleep produces false lows ("compression lows"); first-day readings run noisier than the rest of the session

### Right column: key-point heading "**Resolution summary:**" followed by a `.payload` block

```
// Data granularity and caveats:
//
// EGV interval:    ~5 min (288 per day)
// Sensor life:     10-15 days per sensor (model-dependent)
// Warm-up:         no data for the first hours of a session
// Latency:         delayed history via API; live values
//                  remain in the vendor app
// Value type:      estimated (interstitial fluid + algorithm),
//                  lags blood glucose by several minutes
// Known artifacts: compression lows at night,
//                  noisier first-day readings
```

## Business Scenarios & Notes

- Metabolic wellness apps, nutrition-response tracking, and research studies on non-diabetic populations — the OTC clearances created this segment
- Diabetes management remains the regulated core: alerts, follower/share features, and clinician reports live in the vendor apps, not the public API
- **A wellness reading is not a diagnosis:** post-meal rises into the 140–180 mg/dL range are normal physiology; wellness dashboards that grade every spike invite over-interpretation of healthy variation
- **Population shift:** OTC sensors put CGM data on people it was never validated against as a diagnostic — comparing their curves to diabetic reference ranges is a base-rate error
- **Integration partners:** Apple HealthKit and Android Health Connect both carry blood-glucose record types written by CGM apps

### Right column: key-point heading "**The 2024 OTC shift:**" followed by a `.payload` block

```
// Prescription CGM (pre-2024)      OTC CGM (2024-)
// -------------------------        -------------------------
// Prescribed for diabetes          Sold to anyone 18+
// Clinician sees the data          User alone interprets it
// Alerts tuned for insulin risk    Wellness framing, softer alerts
// Validated on diabetic users      Worn by healthy users
//
// Cleared in 2024: Dexcom Stelo (March),
// Abbott Lingo and Libre Rio (June).
// Same sensor technology - different population,
// different decisions made from the same curve.
```

## Official API References

- [Dexcom Developer API](https://developer.dexcom.com/) — official reference for OAuth, sandbox, and the egvs/devices/events/calibrations/alerts/dataRange endpoints

## Regeneration instructions

- **Layout:** detail page with `.obj-table`: full-width `border-collapse: collapse` table, one `<tr>` per section; left `<td>` (45%) holds `.obj-title` + bullets, right `<td>` (55%, `text-align: center`) holds — row 1: `.payload` pre + the canvas; rows 2–4: a short `.key-point` heading (bold label) followed by a `.payload` block (no canvases). After the table, an `<h2>Official API References</h2>` with a plain `<ul>` of links. The subtitle is a `<div class="subtitle">`.
- **Page style:** body system sans-serif, white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; table cell borders `1px solid #2980b9`, padding 16px; `.obj-title` bold `#1a5276` 1.1em; `li` 0.93em; links `#1a5276`. No nav bar, no back/home links.
- **Callout style:** `.key-point` — background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px 14px, 0.93em. `.payload` — same background/border, monospace (ui-monospace/Menlo) 0.78em, `white-space: pre`, left-aligned.
- **Canvas:** `display: block; width: 100%`; intrinsic height 380; sized from `getBoundingClientRect().width`, backing store sized to rendered width × `window.devicePixelRatio` (display capped via `style.maxWidth`), `ctx.scale` back to logical coordinates. Curve built from hardcoded control points with linear interpolation — never `Math.random()`.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, light-blue fill `rgba(26,82,118,0.35)`, gray text `#666`/`#2c3e50`.
- In regenerated HTML, any card/nav links use `.html` extensions (this page has none; external doc links stay as-is).
