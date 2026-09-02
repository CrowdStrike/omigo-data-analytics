# Apple Screen Time

**Page type:** detail page (two-column obj-table layout: text left 45%, payload/canvas/callouts right 55%, one row per section)
**HTML title tag:** Apple Screen Time

**Subtitle:** Per-app usage duration, pickups, and notification counts. No public API — data stays on-device unless Family Sharing or MDM is configured.

## What It Provides

- Per-app usage duration (time spent in foreground)
- Pickup count (how many times device was unlocked/woken)
- Notification count per app
- Category-level aggregation (Social, Entertainment, Productivity, etc.)
- Most used apps ranked by duration
- Weekly summary comparisons (this week vs last week)

### Payload block (right column, `.payload` div; its own `.obj-title` heading: "Payload (No Public API — Inferred from UI)")

```
// ── illustrative payload; no public API exists, structure inferred from
//    Settings UI and DeviceActivity framework docs ──

{
  "summary_type": "daily",
  "date": "2026-08-21",
  "total_screen_time_minutes": 247,
  "pickups": 62,
  "first_pickup": "06:42",
  "notifications_received": 184,
  "categories": [
    { "name": "Social", "minutes": 78, "apps": ["Instagram", "Messages", "LinkedIn"] },
    { "name": "Entertainment", "minutes": 52, "apps": ["YouTube", "Netflix", "Spotify"] },
    { "name": "Productivity", "minutes": 45, "apps": ["Mail", "Slack", "Notes"] },
    { "name": "Information & Reading", "minutes": 38, "apps": ["Safari", "News", "Kindle"] },
    { "name": "Other", "minutes": 34, "apps": ["Maps", "Camera", "Settings"] }
  ],
  "hourly_minutes": [
    0, 0, 0, 0, 0, 0, 4, 18, 12, 8,
    22, 15, 32, 10, 8, 14, 20, 28, 24, 18,
    12, 8, 4, 0
  ]
}
```

## Access & Data Availability

- NO public REST API — Apple does not expose Screen Time data via a web service
- DeviceActivity framework (iOS 15+, Swift): for parental control / digital wellness apps only
- Family Sharing: parents can view children's Screen Time reports
- MDM (Mobile Device Management): enterprise can query device usage via managed profiles
- On-device only: data stays on device unless Family Sharing or MDM is configured
- Third-party apps CANNOT read Screen Time data — there is no API to read even their own app's Screen Time record; apps can only self-instrument their own foreground time

### Visualization (canvas `chartStacked`, width 100% × 420; right-column `.obj-title` above it: "Screen Time by Category and Hour (illustrative)")

Stacked bar chart of screen-time minutes per clock hour, split into five categories.

- **Title (bold 13px, `#1a5276`, top center):** "Screen Time by Category and Hour (illustrative)".
- **Data (24 hourly values per category; stack totals per hour equal `[0,0,0,0,0,0,4,18,12,8,22,15,32,10,8,14,20,28,24,18,12,8,4,0]`):**
  - Social (`#1a5276`): `[0,0,0,0,0,0,1,6,4,2,8,5,10,3,2,4,7,10,8,6,4,3,1,0]`
  - Entertainment (`#e74c3c`): `[0,0,0,0,0,0,0,2,2,1,4,3,8,2,2,3,4,7,6,5,3,2,1,0]`
  - Productivity (`#27ae60`): `[0,0,0,0,0,0,2,6,4,3,6,4,6,3,2,4,5,4,3,2,1,1,0,0]`
  - Information (`#e67e22`): `[0,0,0,0,0,0,1,3,1,1,2,2,5,1,1,2,2,4,4,3,2,1,1,0]`
  - Other (`#95a5a6`): `[0,0,0,0,0,0,0,1,1,1,2,1,3,1,1,1,2,3,3,2,2,1,1,0]`
- **Stacking order (bottom to top):** Social, Entertainment, Productivity, Information, Other.
- **Axes:** y from 0 to 35 minutes, labels every 5 (`#666` 11px, right-aligned) with light `#eee` grid lines; y-axis title "Minutes" rotated vertical (`#2c3e50` 11px); L-shaped axes in `#2c3e50`; x labels at 12am, 3am, 6am, 9am, 12pm, 3pm, 6pm, 9pm (`#2c3e50` 10px). Padding: left 50, right 20, top 40, bottom 70.
- **Legend (bottom, centered row of color swatches + labels, 11px):** Social `#1a5276`, Entertainment `#e74c3c`, Productivity `#27ae60`, Information `#e67e22`, Other `#95a5a6`.

## Granularity & Limitations

- On-device display: daily breakdown with hourly bars, weekly summaries
- DeviceActivity framework: reports events (app opened, threshold crossed) not raw duration data
- No historical export — only current week + last week visible in Settings
- Screen Time data lives on-device but syncs across a user's devices via iCloud when "Share Across Devices" is enabled — still not exportable or API-accessible
- MDM can request installed app list and usage restrictions, not detailed minute-by-minute usage
- "App usage" counts foreground time only — background audio/navigation not counted as screen time
- Category classification is Apple-determined, not configurable

**Key point (callout, right column):** The DeviceActivity framework does not give raw duration numbers. It fires callbacks when a usage threshold is crossed — for example, "social apps exceeded 30 minutes today." The app cannot ask "how many minutes of Instagram so far?" It can only set a budget and be told when that budget is hit. One nuance since iOS 16: the DeviceActivityReport extension CAN render raw per-app durations — but only inside a sandboxed SwiftUI extension with no network access and no way to pass the data back to the host app, so the privacy conclusion stands.

**Key point (callout, right column):** Screen Time's hourly bars in Settings show total usage per clock hour. They do not attribute time to a specific app within that hour unless the user taps into the detail view. The payload above represents what the UI can display — not what any API returns.

## Business Scenarios & Constraints

- Digital wellness apps (e.g., Opal, one sec) use DeviceActivity to set shields/limits
- Parental control apps rely on Family Sharing or MDM profiles
- Academic research on phone addiction must rely on self-report or custom logging apps
- Apple deliberately restricts access — privacy as a product differentiator
- Screen Time was introduced in iOS 12 (2018); the DeviceActivity, FamilyControls, and ManagedSettings frameworks shipped in iOS 15 (2021, WWDC 2021), with the DeviceActivityReport extension added in iOS 16 (2022)
- Android is more open here: `UsageStatsManager` (with the user-granted Usage Access special permission) lets third-party apps read per-app usage — iOS has no equivalent

**Key point (callout, right column):** The practical consequence: any study claiming "average screen time is X hours" based on app-collected data is measuring what a self-selected population of wellness-app users permits a third-party to infer from threshold callbacks — not actual duration from the OS. Apple's own weekly notification ("Your screen time was up 12% last week") is the only source with full access, and it is not exportable.

## Official API References

- [DeviceActivity Framework](https://developer.apple.com/documentation/deviceactivity) — Apple's official framework for monitoring device usage via threshold events (iOS 15+)
- [FamilyControls Framework](https://developer.apple.com/documentation/familycontrols) — Apple's authorization framework for parental control and Screen Time-related apps

## Regeneration instructions

- **Layout:** detail page with `.obj-table`: full-width `border-collapse: collapse` table, one `<tr>` per section; left `<td>` (45%) holds `.obj-title` + bullets, right `<td>` (55%, `text-align: center`) holds — row 1: the `.payload` block with its own `.obj-title`; row 2: an `.obj-title` + the single canvas; rows 3 and 4: `.key-point` callouts only (no canvases). After the table, an `<h2>Official API References</h2>` with a plain `<ul>` of links. Uses `* { box-sizing: border-box; }`.
- **Page style:** body system sans-serif, white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; table cell borders `1px solid #2980b9`, padding 16px; `.obj-title` bold `#1a5276` 1.1em; `li` 0.93em; links `#1a5276`. No nav bar, no back/home links.
- **Callout style:** `.key-point` — background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px 14px, 0.93em. `.payload` — same background/border, monospace (ui-monospace/Menlo) 0.78em, `white-space: pre`, left-aligned.
- **Canvas:** `display: block; width: 100%`; intrinsic height 420; sized from `getBoundingClientRect().width` (fallback 720), backing store sized to rendered width × `window.devicePixelRatio` (display capped via `style.maxWidth`), CSS height fixed, `ctx.scale` back to logical coordinates.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, gray `#95a5a6`, gray text `#666`/`#2c3e50`.
- In regenerated HTML, any card/nav links use `.html` extensions (this page has none; external doc links stay as-is).
