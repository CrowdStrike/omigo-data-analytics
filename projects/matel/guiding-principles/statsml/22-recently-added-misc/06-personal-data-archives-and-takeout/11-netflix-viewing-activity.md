# Netflix Viewing Activity

**Page type:** detail page (single-row two-column obj-table: text left 45%, payload samples + canvas right 55%)
**HTML title tag:** Netflix Viewing Activity

**Subtitle:** Personal data export via account settings and viewing activity page

**Verified line:** Last verified: August 2026

## How to Request

**Key-point callout 1:** **Quick CSV Export:** Account → Profile & Parental Controls → Viewing Activity → Download All. Delivers immediately as a CSV file of watch history.

**Key-point callout 2:** **Full Data Download:** Account Settings → Security & Privacy → "Download your personal information." Delivery takes approximately **30 days**. Arrives as a ZIP with multiple CSV/JSON files.

## What's Included

- **Viewing history** — title, date watched, duration per session
- **Search history** — queries with timestamps
- **Ratings** — thumbs up/down and legacy star ratings
- **Device logins** — IP, device type, browser/app, location
- **Billing history** — plan changes, charges, payment method type
- **Profile preferences** — maturity settings, language, playback prefs
- **Taste community/cluster assignments** — internal grouping labels Netflix uses for recommendation segmentation
- **Interaction events** — pause, rewind, skip intro clicks, fast-forward, episode abandon points

## What's Missing

**Missing callout (red-bordered):**

- Content ranking algorithm weights or scores
- A/B test group assignments (UI variants, feature rollouts)
- Thumbnail selection model — which artwork you were shown per title
- Real-time session quality metrics (bitrate, buffering events)

## Right column: payload samples

**Payload note (italic):** Quick CSV Export — ViewingActivity.csv

**Payload block (monospace, verbatim):**

```
Title,Date
"Stranger Things: Season 4: Chapter 1",2024-07-15
"The Bear: Season 3: Fork",2024-07-15
"Dark: Season 1: Secrets",2024-07-14
"Wednesday: Season 1: Episode 3",2024-07-12
"Our Planet: Frozen Worlds",2024-07-10
```

**Payload note (italic):** Full Download — viewing record with interaction events and taste cluster (JSON)

**Payload block (monospace, verbatim):**

```
{
  "profileName": "User1",
  "title": "Stranger Things: S4: Chapter 1",
  "videoId": 81002370,
  "date": "2024-07-15T21:04:18Z",
  "duration_seconds": 4620,       // documented
  "deviceType": "PS5",            // documented
  "country": "US",                // documented
  "bookmark": 4580,               // resume position
  "interactions": [               // documented
    {"type": "skip_intro", "ts": 42},
    {"type": "pause", "ts": 1830},
    {"type": "rewind", "ts": 2904, "delta": -15}
  ],
  "tasteCluster": "US_Drama_SciFi_Thriller_27",  // documented
  "tasteCommunity": "Dark Sci-Fi Enthusiasts",   // inferred label
  "matchScore": null              // NOT exported — algorithm internal
}
```

### Visualization (canvas `viewingChart`, 100% width × 380px CSS height)

Bar chart: weekly viewing hours over 26 weeks with binge weeks highlighted and a dashed average line.

- **Title (bold 13px, `#1a5276`, top center):** "Weekly Viewing Hours — 26 Weeks (Binge Patterns Highlighted)".
- **Data (weekly hours, weeks 1–26):** `[8, 6, 7, 11, 9, 28, 32, 7, 5, 10, 6, 8, 12, 9, 7, 6, 25, 30, 8, 7, 11, 9, 6, 35, 10, 8]`.
- **Bars:** one per week, 3px padding each side; color `#1a5276` (blue) when value ≤ 20, `#e67e22` (orange) when value > 20 (binge weeks: W6=28, W7=32, W17=25, W18=30, W24=35).
- **Axes:** y from 0 to 40 with ticks every 10 labeled "0h"–"40h" (`#666` 11px) and `#eee` gridlines; x tick labels every 5 bars ("W1", "W6", "W11", "W16", "W21", "W26") in `#666` 10px; x-axis title "Week" centered at bottom (`#666` 11px); padding left 50, right 20, top 40, bottom 50.
- **Average line:** horizontal dashed green line (`#27ae60`, width 2, dash 6/4) at the data mean (sum/26 ≈ 12.5h), labeled to its right in green 11px: "avg: 12.5h" (computed as `avg.toFixed(1)`).
- **Legend (bottom left):** blue `#1a5276` swatch + "Normal (<20h)"; orange `#e67e22` swatch + "Binge (>20h)".

## Regeneration instructions

- **Layout:** single `.obj-table` (full width, collapsed borders) with one `<tr>`: left `<td>` (45%) holds `.obj-title` headings, two `.key-point` callouts, bullet lists, and a `.missing` callout; right `<td>` (55%, text-align center) holds two `.payload-note` + `.payload` blocks and the canvas. Subtitle and verified lines are `<div>`s on this page.
- **Page style:** body system sans-serif, white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; `.verified` 0.85em `#888`; `.obj-title` bold `#1a5276` 1.1em; table cell borders `1px solid #2980b9`, padding 16px; li 0.93em.
- **Callouts:** `.key-point` — background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px 14px. `.missing` — background `#fdf2f2`, left border `3px solid #e74c3c`.
- **Payload block:** `.payload` — background `#f8f9fa`, left border `3px solid #1a5276`, monospace (ui-monospace/Menlo) 0.78em, white-space pre, left-aligned; `.payload-note` 0.82em `#666` italic left-aligned.
- **Canvas:** styled `width:100%; height:380px`; sized from `getBoundingClientRect()` and scaled by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`. No nav bar, no back/home links; in regenerated HTML any card links use `.html` extensions.
