# Oura Ring API

**Page type:** detail page (two-column obj-table layout: text left 45%, payload/canvas right 55%, one row per section)
**HTML title tag:** Oura Ring API

**Subtitle:** Wearable sleep, readiness, and biometric data via REST API

## What the API Provides

- **Sleep:** total duration, efficiency, latency, time in bed, sleep stages (awake, light, deep, REM)
- **Readiness score:** composite 0-100 based on HRV, resting HR, body temperature, sleep quality
- **Activity:** steps, active calories, total calories, movement intensity (met_minutes)
- **Heart rate:** 5-minute intervals throughout the day, resting heart rate
- **HRV (heart rate variability):** nightly average, 5-minute intervals during sleep
- **Body temperature deviation:** nightly deviation from personal baseline (in Celsius)
- **SpO2 (blood oxygen):** nightly average percentage

### Payload block (right column, `.payload` div)

```
// ── illustrative payload; field names from Oura API v2 docs, values are not real ──
{
  "data": [
    {
      "id": "EXAMPLE_UUID_redacted",  // Note: masked for illustration
      "day": "2026-08-21",
      "bedtime_start": "2026-08-21T23:14:00+00:00",
      "bedtime_end": "2026-08-22T06:52:00+00:00",
      "duration": 27480,
      "total_sleep_duration": 25140,
      "awake_time": 2340,
      "light_sleep_duration": 12420,
      "deep_sleep_duration": 6180,
      "rem_sleep_duration": 6540,
      "restless_periods": 4,
      "efficiency": 91,
      "latency": 480,
      "average_heart_rate": 54,
      "lowest_heart_rate": 48,
      "average_hrv": 62,
      "temperature_deviation": -0.12,
      "readiness": {
        "score": 84
      }
    }
  ],
  "next_token": "NEXT_PAGE_TOKEN"
}
```

### Visualization (canvas `sleepCanvas`, width 100% × 380)

Hypnogram: sleep-stage timeline across 96 five-minute epochs (8 hours, 11pm–7am), one horizontal band per stage.

- **Title (bold 13px, `#1a5276`, top center):** "Sleep Stages Timeline (illustrative night)".
- **Bands (top to bottom, each 1/4 of plot height, labels right-aligned `#2c3e50` 11px on the left):** Awake, REM, Light, Deep; horizontal grid lines `#e0e0e0` 0.5px between bands.
- **Stage colors:** awake `#e74c3c`, rem `#e67e22`, light `rgba(26,82,118,0.35)`, deep `#1a5276`. Each epoch is drawn as a filled rect in its stage's band; awake epochs use a half-height band, vertically centered.
- **Epoch sequence (0-indexed epochs → stage):** 0–5 light (falling asleep); 6–15 deep (first deep cycle); 16–18 light; 19–24 REM (first REM, short); 25 awake (brief); 26–30 light; 31–40 deep (second deep cycle); 41–45 light; 46–55 REM (second REM, longer); 56 awake; 57–60 light; 61–67 deep (third, shorter); 68–72 light; 73–85 REM (third REM, longest); 86 awake; 87–90 light; 91–95 REM (final).
- **X-axis:** time labels 11pm, 12am, 1am, 2am, 3am, 4am, 5am, 6am, 7am (`#2c3e50` 10px, centered, evenly spaced eighths) with small `#999` tick marks.
- **Legend (bottom left, 10px swatches + labels):** Deep `#1a5276`, Light `rgba(26,82,118,0.35)`, REM `#e67e22`, Awake `#e74c3c`.
- **Margins:** left 50, right 20, top 40, bottom 60.

## Access & Authentication

- OAuth 2.0 authorization code flow
- Personal access tokens available for individual developers (simpler for own data)
- **Scopes:** daily, heartrate, session, tag, workout, personal, spo2
- **API base:** https://cloud.ouraring.com/v2/usercollection/
- Free tier API access for all Oura ring owners (no separate API fee), but without an Oura membership the returned data/features are reduced
- **Rate limit:** 5000 requests per 5-minute period

### Right column: key-point heading "**Authentication header:**" followed by a `.payload` block

```
GET /v2/usercollection/sleep?start_date=2026-08-21
Host: cloud.ouraring.com
Authorization: Bearer (your-access-token)

// Scopes requested during OAuth:
// daily, heartrate, session, tag, workout, personal, spo2

// Rate limit: 5000 requests per 5-minute period
// No separate API fee, but data is reduced without membership
```

## Granularity & Limitations

- **Sleep stages:** 5-minute epoch resolution (each epoch classified as awake/light/deep/REM)
- **Heart rate:** 5-minute intervals (not beat-to-beat)
- **HRV:** 5-minute intervals during sleep only
- **Body temperature:** single nightly value (deviation from baseline)
- Data available after next sync (typically morning after sleep)
- **Historical data:** available from first day of ring use, no time limit
- Readiness/sleep scores calculated on-device, then synced to cloud
- No raw data streaming — poll the REST API, or register webhooks for new-data event notifications

### Right column: key-point heading "**Resolution summary:**" followed by a `.payload` block

```
// Data granularity per metric:
//
// Sleep stages:     5-min epochs (96 per 8h night)
// Heart rate:       5-min intervals (288 per day)
// HRV:             5-min intervals (sleep only)
// Body temperature: 1 value per night (deviation °C)
// SpO2:            1 value per night (average %)
// Activity:        daily summary + intraday 5-min
// Readiness:       1 score per day (composite 0-100)
//
// Latency: data arrives after morning sync
// No streaming/websocket — poll or use webhooks
// Historical: no expiration, all data since first use
```

## Business Scenarios & Deprecation Notes

- Personal health dashboards, sleep optimization tracking, research studies
- Oura API v1 deprecated around 2022, fully shut down January 2024; v2 is current
- Subscription model introduced with Gen 3 ring (October 2021) — $5.99/mo for full features
- API access is free of any separate fee, but without a membership the returned data/features are reduced
- Used in academic research (e.g., COVID-19 early detection via temperature deviations)
- **Integration partners:** Google Health Connect, Apple Health (export only, not API)
- **Webhook support:** can register webhooks for new data availability notifications

### Right column: key-point heading "**Webhook registration:**" followed by a `.payload` block

```
POST /v2/webhook/subscription
Host: cloud.ouraring.com
x-client-id: (your-client-id)
x-client-secret: (your-client-secret)
Content-Type: application/json

{
  "callback_url": "https://your-app.com/oura-webhook",
  "verification_token_masked": "EXAMPLE_SECRET_redacted",  // Note: masked for illustration; replace with your actual webhook secret
  "event_type": "create",
  "data_type": "daily_sleep"
}

// Supported data_type values (collection names):
// daily_activity, daily_readiness, daily_sleep, daily_spo2,
// sleep, session, workout, enhanced_tag
// (no heart_rate webhook type; "tag" is deprecated — use enhanced_tag)

// v1 → v2 migration: v1 deprecated ~2022, shut down Jan 2024;
// breaking changes in endpoint paths, response structure, pagination
```

## Official API References

- [Oura API v2 Documentation](https://cloud.ouraring.com/docs) — official reference for endpoints, OAuth scopes, webhooks, and personal access tokens

## Regeneration instructions

- **Layout:** detail page with `.obj-table`: full-width `border-collapse: collapse` table, one `<tr>` per section; left `<td>` (45%) holds `.obj-title` + bullets, right `<td>` (55%, `text-align: center`) holds — row 1: `.payload` pre + the canvas; rows 2-4: a short `.key-point` heading (bold label) followed by a `.payload` block (no canvases). After the table, an `<h2>Official API References</h2>` with a plain `<ul>` of links. The subtitle here is a `<div class="subtitle">` rather than `<p>`.
- **Page style:** body system sans-serif, white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; table cell borders `1px solid #2980b9`, padding 16px; `.obj-title` bold `#1a5276` 1.1em; `li` 0.93em; links `#1a5276`. No nav bar, no back/home links.
- **Callout style:** `.key-point` — background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px 14px, 0.93em. `.payload` — same background/border, monospace (ui-monospace/Menlo) 0.78em, `white-space: pre`, left-aligned.
- **Canvas:** `display: block; width: 100%`; intrinsic height 380; sized from `getBoundingClientRect().width`, backing store sized to rendered width × `window.devicePixelRatio` (display capped via `style.maxWidth`), `ctx.scale` back to logical coordinates.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, light-blue fill `rgba(26,82,118,0.35)`, gray text `#666`/`#2c3e50`.
- In regenerated HTML, any card/nav links use `.html` extensions (this page has none; external doc links stay as-is).
- Page footnote (italic 12px, muted `#6b7280`, after the last card-section): "Note: realistic credential strings on this page were converted to generic placeholders — for illustration only, and to avoid false positives from secret scanners."
