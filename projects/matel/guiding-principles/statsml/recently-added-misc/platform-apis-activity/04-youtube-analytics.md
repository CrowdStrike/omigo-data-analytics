# YouTube Analytics API

**Page type:** detail page (two-column obj-table layout: text left 45%, payload/canvas right 55%, one row per section)
**HTML title tag:** YouTube Analytics API

**Subtitle:** Channel and video performance data — views, watch time, retention curves, traffic sources, demographics, and revenue — accessible via OAuth 2.0 for the channel owner.

## What the API Provides

- **Channel-level:** views, watch time (minutes), subscribers gained/lost, revenue (if monetized)
- **Video-level:** views, average view duration, audience retention curve, traffic sources
- **Audience demographics:** age group, gender, geography (country/region)
- **Real-time:** concurrent viewers (Data API v3 `videos.list` → `liveStreamingDetails.concurrentViewers`), ~2 minute delay
- **Traffic sources:** YouTube search, suggested videos, external, browse features, playlists
- **Engagement:** likes, dislikes (since Dec 2021 removed from public UI and public Data API — visible only to the channel owner via Studio or owner-authorized Analytics API), comments, shares

**Key point (callout):** The retention curve is the most analytically useful metric YouTube exposes. It shows exactly where viewers leave, revealing structural problems (slow intros, content mismatches) that aggregate view counts cannot.

### Payload block (right column, monospace `.payload` pre)

```
{
  // ── illustrative payload; field names from YouTube Analytics API v2 docs, values are not real ──
  "kind": "youtubeAnalytics#resultTable",
  "columnHeaders": [
    { "name": "day", "dataType": "STRING" },
    { "name": "views", "dataType": "INTEGER" },
    { "name": "estimatedMinutesWatched", "dataType": "INTEGER" },
    { "name": "averageViewDuration", "dataType": "INTEGER" },
    { "name": "subscribersGained", "dataType": "INTEGER" }
  ],
  "rows": [
    ["2026-08-18", 12450, 34200, 164, 89],
    ["2026-08-19", 14102, 38900, 165, 112],
    ["2026-08-20", 11890, 31400, 158, 74]
  ]
}
```

### Visualization (canvas `c1`, width 100% × 360)

Area/line chart of a typical audience retention curve.

- **Title (bold 13px, `#1a5276`, top center):** "Audience Retention Curve (typical drop-off pattern)".
- **Data (20 points, retention % over video progress):** `[100, 72, 65, 60, 57, 55, 52, 50, 48, 46, 44, 43, 41, 40, 38, 37, 36, 35, 37, 34]`.
- **Axes:** L-shaped axes in `#ccc`; y from 0% to 100% with labels at 0/25/50/75/100% (`#666` 11px, right-aligned) and light `#eee` grid lines; x labels 0%, 25%, 50%, 75%, 100% (`#666`); padding top 40 / right 30 / bottom 40 / left 50.
- **Axis titles (gray `#888` 10px):** "Video Progress" bottom center; "Retention" rotated vertical on the left.
- **Series:** area under the curve filled `rgba(26,82,118,0.35)`, curve line `#1a5276` 2.5px.
- **Reference line:** horizontal dashed red (`#e74c3c`, dash 6/4, width 1.5) at y=50%, labeled "Average retention" in `#e74c3c` 11px above-left of the plot's right edge.

## Access & Authentication

- OAuth 2.0 — channel owner must authorize; no way to query another channel's analytics
- Scopes: `yt-analytics.readonly`, `yt-analytics-monetary.readonly` (for revenue)
- YouTube Data API v3 (public video stats) uses API key — no OAuth needed for public metrics
- Data API v3 quota: 10,000 units/day default; request costs range from 1 unit (most list calls) to 100 (search) to 1,600 (videos.insert)
- Analytics API has its own separate quota, independent of the Data API's unit pool; its queries do not consume Data API units
- Can request quota increase but approval is not guaranteed

**Key point (callout):** The quota system is the primary rate limiter. A single search request burns 100 units — meaning a naive implementation polling search every minute exhausts the daily quota in under two hours.

### Visualization (canvas `c2`, width 100% × 300)

Horizontal bar chart of quota cost per request type.

- **Title (bold 13px, `#1a5276`, top center):** "API Quota Cost per Request Type (units)".
- **Bars (28px tall, 14px gap, labels right-aligned `#2c3e50` 12px left of bars, bars at 0.7 alpha, scale max 100; cost label bold 12px `#2c3e50` right of each bar as "N unit(s)"):**
  - Analytics query — 1 — `#27ae60`
  - Data API list — 1 — `#27ae60`
  - Data API videos — 1 — `#27ae60`
  - Data API channels — 1 — `#27ae60`
  - Data API search — 100 — `#e74c3c`
- **Footer note (bottom center, gray `#888` 11px):** "Data API daily quota: 10,000 units — one search/min exhausts it in 100 minutes".

## Granularity & Limitations

- Historical data: daily granularity, available from channel creation date
- Revenue data: delayed 2-3 days, reconciled monthly (final numbers change retroactively)
- Audience retention: relative retention curve at ~100 data points per video (percentage intervals)
- Real-time concurrent viewers: only for live streams, ~2 minute delay
- "Views" metric has a processing delay of 24-48 hours for final counts
- YouTube Studio shows "real-time" views (last 48h) but the API does not expose this directly
- Demographics suppressed if audience is too small (privacy threshold)

**Key point (callout):** Revenue reconciliation means any dashboard showing daily revenue is provisional. Individual days can shift during monthly finalization, making day-over-day revenue comparisons unreliable until the month closes.

### Visualization (canvas `c3`, width 100% × 300)

Dot-on-timeline chart of data availability delays per metric.

- **Title (bold 13px, `#1a5276`, top center):** "Data Availability Delays".
- **Layout:** metric labels right-aligned 12px `#2c3e50` in a left column; a horizontal axis line `#ccc` below the rows with endpoint labels "Real-time" (left) and "30 days" (right) in gray `#888` 10px; each row has a faded (0.4 alpha) colored line from the timeline's left edge to a 6px dot at the delay position, with the delay text in bold 11px of the same color right of the dot. Rows 44px apart.
- **Rows (label — delay text — position fraction — color):**
  - Live concurrent viewers — "~2 min" — 0.02 — `#27ae60`
  - View counts (provisional) — "24-48 hrs" — 0.25 — `#e67e22`
  - Revenue (provisional) — "2-3 days" — 0.35 — `#e67e22`
  - Demographics — "2-3 days" — 0.35 — `#1a5276`
  - Revenue (final) — "~30 days" — 1.0 — `#e74c3c`

## Business Scenarios & Deprecation Notes

- Creator dashboards, MCN (Multi-Channel Network) reporting, ad revenue optimization
- Analytics API v1→v2 transition in 2018 (v1 sunset 2019); the 2014 deprecation was the old Data API v2 (GData)
- YouTube Reporting API (bulk reports) runs on a different schedule — daily dumps, not real-time
- CMS (Content Management System) API for large rights-holders — separate access tier
- Shorts metrics added 2022-2023 but retention curves differ from long-form
- Public subscriber counts in the Data API are abbreviated/rounded (since 2019) — exact counts only for the channel owner

**Key point (callout):** Shorts retention curves are structurally different from long-form: the looping mechanic means a viewer can "watch" 300% of a Short without any active decision to continue. Comparing retention percentages across formats is misleading.

### Visualization (canvas `c4`, width 100% × 300)

Four side-by-side rounded boxes describing the YouTube API landscape, with status badges.

- **Title (bold 13px, `#1a5276`, top center):** "YouTube API Landscape".
- **Boxes (4 across, ~130px tall, fill `#f8f9fa`, 2px border in status color, 6px radius; each with a small tinted status badge (0.15 alpha of status color, bold 10px status text), a bold 11px `#1a5276` title, and two 10px `#666` sub-lines):**
  - "Analytics API v2" — "Channel owner metrics" / "OAuth required" — badge "Active", `#27ae60`
  - "Data API v3" — "Public video stats" / "API key only" — badge "Active", `#27ae60`
  - "Reporting API" — "Bulk daily dumps" / "Async jobs" — badge "Active", `#27ae60`
  - "CMS API" — "Rights holders" / "Separate tier" — badge "Restricted", `#e67e22`
- **Footer notes (bottom center):** in `#e74c3c` 11px: "Data API v2 (GData) deprecated 2014 · Analytics API v1 → v2 in 2018 (v1 sunset 2019)"; below it in `#888` 10px: "Shorts metrics (2022-2023) added to v2 but retention curves differ from long-form".

## Official API References

- [YouTube Analytics API](https://developers.google.com/youtube/analytics) — targeted queries for channel and video metrics (views, watch time, retention, revenue)
- [YouTube Reporting API](https://developers.google.com/youtube/reporting) — bulk daily report downloads for large-scale analytics pipelines

## Regeneration instructions

- **Layout:** detail page with `.obj-table`: full-width `border-collapse: collapse` table, one `<tr>` per section; left `<td>` (45%) holds `.obj-title` + bullets + `.key-point` callout, right `<td>` (55%, `text-align: center`) holds the `.payload` pre (row 1 only) and one canvas per row. After the table, an `<h2>Official API References</h2>` with a plain `<ul>` of links.
- **Page style:** body system sans-serif, white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; table cell borders `1px solid #2980b9`, padding 16px; `.obj-title` bold `#1a5276` 1.1em; `li` 0.93em; links `#1a5276`. No nav bar, no back/home links.
- **Callout style:** `.key-point` — background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px 14px, 0.93em. `.payload` — same background/border, monospace (ui-monospace/Menlo) 0.78em, `white-space: pre`, left-aligned.
- **Canvas:** `display: block; width: 100%`; intrinsic `height` attribute per chart (360/300/300/300); a shared `setupCanvas(id)` helper reads `getBoundingClientRect().width`, sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`), fixes CSS height, and calls `ctx.scale` so drawing stays in logical coordinates.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, area fill `rgba(26,82,118,0.35)`, gray text `#666`/`#888`/`#2c3e50`.
- In regenerated HTML, any card/nav links use `.html` extensions (this page has none; external doc links stay as-is).
