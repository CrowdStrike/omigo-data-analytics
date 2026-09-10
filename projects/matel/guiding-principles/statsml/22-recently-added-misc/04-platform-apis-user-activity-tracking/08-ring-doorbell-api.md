# Ring Doorbell API

**Page type:** detail page (two-column obj-table layout: descriptive text left 45%, payload + canvas right 55%, one row)
**HTML title tag:** Ring Doorbell API

**Subtitle:** Motion events, doorbell presses, and video history — no official public API exists; access relies on reverse-engineered endpoints.

## What It Provides

Motion detection events (with timestamp, device ID, zone), ding events (doorbell press), video recording URLs (temporary signed links), device health (battery, wifi signal), snapshot images.

## Authentication

Unofficial — uses Ring's internal OAuth flow. Requires email/password + 2FA token. No official developer program, no API keys, no documented rate limits.

## Granularity

Near real-time — motion events trigger within seconds via push notification infrastructure. Video clip length is configurable, up to ~120 seconds on wired devices. Events are timestamped to the second.

## No Official API

**Key-point callout:** Ring (Amazon) has never published a public API. All third-party access (python-ring-doorbell library, Home Assistant integration) uses reverse-engineered endpoints. Ring has changed auth flows multiple times to break unofficial access.

## Business Scenarios

Home automation triggers, security monitoring dashboards, package delivery verification, neighborhood activity patterns.

## Restrictions

No official support — can break at any time. Ring has added hardware-based tokens, forced 2FA, changed API endpoints without notice. No guaranteed rate limits (community reports suggest ~20 requests/min before throttling). Video access requires Ring Protect subscription.

## Deprecation History

**Key-point callout:** Ring moved from basic auth to OAuth (~2019-2020), and made 2FA mandatory in February 2020 after the late-2019 account-takeover incidents. Each change broke all unofficial integrations for days/weeks.

## Payload (Inferred / Reverse-Engineered)

Monospace `.payload` block (right column), verbatim:

```
// ── illustrative payload; structure inferred from reverse-engineered endpoints, values are not real ──
// Based on python-ring-doorbell library observations
{
  "id": 73829461052,
  "created_at": "2026-08-22T14:32:07.000Z",
  "answered": false,
  "events": [],
  "kind": "motion",
  "favorite": false,
  "snapshot_url": "https://ring-transcoded-videos.s3.amazonaws.com/...",
  "duration": 30,
  "device_id": "aabbccdd1234",
  "device_name": "Front Door",
  "motion_zone": "zone-1",
  "cv_properties": {
    "person_detected": true,
    "detection_type": "human"
  }
}
```

## Illustrative: 24-hour event timeline

### Visualization (canvas `eventTimeline`, 720×300)

Scatter/event timeline: motion and ding events plotted across a 24-hour x-axis, points jittered vertically around the plot's vertical midline for visibility.

- **Layout:** padding left 50, right 30, top 50, bottom 40. L-shaped axes (left and bottom) stroked in `#2c3e50`, width 1.
- **X-axis:** hours 0–24, tick + label every 4 hours formatted "00:00", "04:00", … "24:00", 12px system font, `#2c3e50`, 5px tick marks below the axis.
- **Y-axis label:** rotated -90° at x=14, centered vertically: "Events" in `#2c3e50`.
- **Motion events (orange filled circles, `#e67e22`, radius 5):** hours (decimal) `[0.5, 1.2, 3.8, 7.1, 7.3, 7.5, 7.8, 8.0, 8.2, 8.5, 8.9, 9.3, 10.1, 10.8, 11.5, 12.2, 13.0, 13.7, 14.5, 15.3, 16.0, 17.1, 17.4, 17.8, 18.0, 18.3, 18.6, 19.0, 19.4, 19.8, 20.5, 21.2, 22.0]` — clustered in morning and evening. Vertical jitter: yOffset = `((i % 5) - 2) * 18` around plot mid-height.
- **Ding events (blue diamonds — 10×10 squares rotated 45°, `#1a5276`):** hours `[11.1, 14.2, 17.0, 18.8, 20.1]` — delivery times and a couple evening. Vertical jitter: yOffset = `((i % 3) - 1) * 22` around plot mid-height.
- **Legend (top right, 12px font):** orange circle at (w-180, 20) labeled "Motion" in `#2c3e50`; blue diamond at (w-90, 20) labeled "Ding" in `#2c3e50`.

## Official API References

- [Ring Support (official site)](https://ring.com/support) — Ring/Amazon publishes no public developer API; official data access is limited to the Ring app and account video export via Ring Protect

## Regeneration instructions

- **Layout:** platform-API detail page. h1 + `.subtitle`, then a single `.obj-table` (full width, border-collapse) with one `<tr>`: left `<td>` (45%) holds `.obj-title` headings + paragraphs and `.key-point` callouts; right `<td>` (55%) holds the `.payload` code block and the canvas. After the table, an `<h2>Official API References</h2>` with a `<ul>` of links.
- **Page style:** body system sans-serif (-apple-system stack), white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; `* { box-sizing: border-box; }`; h1 1.8rem `#1a5276`, margin-bottom 4px; `.subtitle` `#666` 1.05em, margin-bottom 24px. No nav bar, no back/home links.
- **Table style:** `.obj-table td` vertical-align top, padding 16px, border `1px solid #2980b9`; `.obj-title` bold `#1a5276` 1.1em, margin-bottom 8px.
- **Payload block:** `.payload` — background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px, ui-monospace/Menlo 0.78em, `white-space: pre`, `overflow-x: auto`, line-height 1.45, margin 12px 0.
- **Callout:** `.key-point` — background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px 14px, margin 12px 0, 0.93em.
- **Links:** `a { color: #1a5276; }`. In regenerated HTML, any card/page links use `.html` extensions.
- **Canvas:** `display: block; margin: 0 auto`; intrinsic 720×300; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setupCanvas(id, w, h)` helper. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, bar fill `rgba(26,82,118,0.35)`.
