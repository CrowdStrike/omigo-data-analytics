# Okta

**Page type:** detail page (single-row two-column obj-table: text left 45%, payload + canvas right 55%; followed by a references list)
**HTML title tag:** Okta — Platform APIs

**Subtitle:** Pull sign-in records, user accounts, and access changes from Okta — the service many companies use for workplace logins.

**Verified badge:** Last verified: August 2026

## What you can get

- Every login attempt: who, from where, on what device, success or failure
- Admin and configuration changes — accounts created, access granted or removed
- Users, groups, and app assignments as they stand right now
- Which extra-verification (MFA) methods each person has set up

**Key point (callout box):** An event that was not captured when it happened is **gone for good — an audit log cannot be backfilled**. Every retention and export decision is a bet on which questions will still be answerable in six months, made before you know what the questions are.

## Watch out for

- The log keeps roughly 90 days; longer history exists only if you were continuously exporting it all along
- User, group, and app data is a snapshot of now — yesterday's memberships needed yesterday's snapshot
- Records land slightly late, so counting "the last few minutes" always undercounts
- Deleted users vanish from the directory but still appear in old events, breaking joins

## Payload (right column)

**Payload note (italic):** Sample log event (abbreviated) — metadata only, never content: it says a Salesforce session began, not what was done inside Salesforce.

```json
{
  "published": "2026-08-18T14:22:31.812Z",
  "eventType": "user.session.start",
  "actor": {
    "alternateId": "a.jaiswal@acme.com",
    "displayName": "Amit Jaiswal"
  },
  "client": {
    "device": "Computer",
    "ipAddress": "203.0.113.44",
    "geographicalContext": { "city": "Bengaluru",
                             "country": "India" }
  },
  "outcome": { "result": "SUCCESS" },
  "target": [ { "type": "AppInstance",
                "displayName": "Salesforce" } ]
}
```

### Visualization (canvas `c1`, responsive width × 360)

Horizontal bar chart of retrievable history in days per Okta surface; each row has a main label plus an italic sub-label, a rounded bar (radius 3), and a note to the right of the bar.

- **Title (bold 14px `#1a5276`, top center):** "How far back each Okta surface can answer".
- **Subtitle (italic 11px `#666`):** "days of history retrievable today".
- **Rows (scale max 120 days; bar height 26px, gap 30px, first row at y=58; label column min(190, 34% width), right-aligned; main label 12px `#2c3e50`, sub-label italic 10px `#888`):**

| Label | Sub-label | Value (days) | Color | Note |
|---|---|---|---|---|
| System Log API | /api/v1/logs, rolling window | 90 | `#1a5276` | ~90 days (plan-dependent) |
| Log Streaming export | EventBridge / Splunk / SIEM | 120 (drawn with ragged right edge = unbounded) | `#27ae60` | bounded only by your sink |
| Users / Groups / Apps | current state only | 0 | `#e74c3c` | no history — snapshot only |
| Sessions | fetch by id; no list endpoint | 0 | `#e74c3c` | no history — infer from log |
| Enrolled MFA factors | /users/{id}/factors | 0 | `#e74c3c` | no history — snapshot only |

- **Bar style:** fill at alpha 0.85, minimum drawn width 4px; the Log Streaming bar's right edge is cut into a white zigzag (ragged edge) to signal "unbounded". Notes 11px, colored `#e74c3c` for zero-value rows, else `#2c3e50`, placed 8px right of the bar.
- **Gridlines:** `#eee` vertical lines at 0, 30, 60, 90 days with `#aaa` 10px labels "0d", "30d", "60d", "90d" below the chart.
- **Caption (italic 11px `#666`, bottom center):** "Red rows are not gaps you can fill later — they are snapshots that must be taken as they happen."

## Official API References

- [Okta API Reference](https://developer.okta.com/docs/reference/) — management API reference hub
- [System Log API](https://developer.okta.com/docs/reference/api/system-log/) — event types, filter expressions and cursor pagination

## Regeneration instructions

- **Layout:** single-row `.obj-table` (full width, border-collapse): left `<td>` 45% with "What you can get" (`.obj-title` + `<ul>`), a `.key-point` callout, then "Watch out for" (`.obj-title` with `margin-top:18px` + `<ul>`); right `<td>` 55% (text-align center) with `.payload-note` (italic), a `<pre class="payload">` JSON block, and the canvas. After the table, an `h2` "Official API References" with a plain `<ul>` of external links. Verified badge is a `<span class="verified">`.
- **Page style:** body system sans-serif, white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; `.verified` badge — 0.8em `#888`, 1px solid `#ddd` border, inline-block, padding 2px 10px, radius 4px; h2 1.3em `#1a5276` with 2px solid `#2980b9` bottom border; `.obj-title` bold 1.1em `#1a5276`; table cell borders `1px solid #2980b9`, padding 16px; li 0.93em; links `#1a5276`.
- **Payload / key-point style:** `.payload` — background `#f8f9fa`, left border `3px solid #1a5276`, monospace 0.78em, white-space pre, left-aligned; `.payload-note` — 0.82em italic `#666`, left-aligned; `.key-point` — background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px 14px, 0.93em.
- **Canvas:** `<canvas id="c1" height="360">` with CSS `display:block; margin:16px auto 0; width:100%`; width taken from `canvas.offsetWidth` at draw time, backing store scaled by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates), redrawn on window resize.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`. No nav bar, no back/home links.
