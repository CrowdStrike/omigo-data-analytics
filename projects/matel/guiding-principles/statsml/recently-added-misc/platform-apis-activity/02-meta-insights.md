# Meta Insights API (Facebook / Instagram)

**Page type:** detail page (two-column obj-table layout: text left 45%, payload/key-points/canvas right 55% centered, one row per section; footer references list)
**HTML title tag:** Meta Insights API (Facebook / Instagram)

**Subtitle:** Page and post analytics via the Graph API — reach, impressions, engagement, audience demographics

## Section 1: What the API Provides

- **Page-level:** reach, impressions, post engagements, page views, follower count changes
- **Post-level:** impressions, reach, engagement (reactions, comments, shares)
- **Instagram:** reach, views (replacing deprecated `impressions`), profile views, follower demographics (age/gender/city)
- **Stories/Reels:** replies, reach, `navigation` (consolidated metric that replaced taps forward/back and exits)
- **Audience breakdown:** age ranges, gender, top cities, top countries
- Available via `GET /{page-id}/insights` and `GET /{ig-media-id}/insights`

**Payload block (monospace, left-aligned, `.payload`), verbatim:**

```
// ── illustrative payload; field names from Meta Graph API docs, values are not real ──
{
  "data": [
    {
      "name": "page_impressions",
      "period": "day",
      "values": [
        { "value": 14892, "end_time": "2026-08-20T07:00:00+0000" },
        { "value": 16340, "end_time": "2026-08-21T07:00:00+0000" }
      ],
      "title": "Daily Total Impressions",
      "id": "17841400123456/insights/page_impressions/day"
    },
    {
      "name": "page_post_engagements",
      "period": "day",
      "values": [
        { "value": 892, "end_time": "2026-08-20T07:00:00+0000" },
        { "value": 1041, "end_time": "2026-08-21T07:00:00+0000" }
      ]
    }
  ],
  "paging": {
    "previous": "https://graph.facebook.com/v23.0/...",
    "next": "https://graph.facebook.com/v23.0/..."
  }
}
```

### Visualization (canvas `chartReachImpressions`, responsive width × 380)

Layered area chart: Reach vs Impressions over a 14-day window.

- **Title (bold 13px, `#1a5276`, left-aligned at plot left, y=20):** "Page Reach vs Impressions (14-day window)"
- **X labels (Aug 7 – Aug 20):** `['Aug 7','Aug 8','Aug 9','Aug 10','Aug 11','Aug 12','Aug 13','Aug 14','Aug 15','Aug 16','Aug 17','Aug 18','Aug 19','Aug 20']`; only every other label drawn (10px `#666`, centered)
- **Reach data:** `[8200, 9100, 8800, 9400, 10200, 11500, 10800, 9600, 8900, 9800, 11200, 12100, 11400, 10600]`
- **Impressions data:** `[14200, 15400, 14800, 15900, 16800, 17600, 17200, 15800, 14500, 16100, 17800, 18200, 17500, 16900]`
- **Y axis:** 0 to 20,000 in 5,000 steps, labels as "0K"–"20K" (11px `#666`, right-aligned), horizontal gridlines `#e0e0e0` 1px
- **Padding:** left 55, right 20, top 45, bottom 50
- **Impressions series (drawn first):** closed area fill `rgba(39,174,96,0.3)` down to y=0, line `#27ae60` 2px
- **Reach series (drawn on top):** closed area fill `rgba(26,82,118,0.35)` down to y=0, line `#1a5276` 2px
- **Legend (top-right inside plot):** two 14×14 swatches — reach fill `rgba(26,82,118,0.35)` with 1.5px `#1a5276` border labeled "Reach"; impressions fill `rgba(39,174,96,0.3)` with 1.5px `#27ae60` border labeled "Impressions" (11px `#2c3e50`)

## Section 2: Access & Authentication

- OAuth 2.0 with Facebook Login
- Requires a Facebook Business account linked to a Page
- App must pass Facebook's App Review for `pages_read_engagement`, `read_insights` permissions
- Instagram insights require `instagram_basic` + `instagram_manage_insights` permissions
- Page access tokens (long-lived: 60 days, then refresh)
- Rate limits: 200 calls/user/hour for page insights; Instagram uses Business Use Case limits computed by formula (4800 × usage factor per rolling 24h), not a flat daily cap

**Key point 1:** **Token lifecycle:** Short-lived token (1-2 hrs) → exchange for long-lived (60 days) → refresh before expiry. System user tokens (for server-to-server) do not expire but require Business Manager setup.

**Key point 2:** **Rate limit structure:** Graph API uses a sliding-window model. Exceeding limits returns HTTP 400 with an error code (4, 17, 32, or 80001-80008 for Business Use Case limits) and an `x-business-use-case-usage` header showing percentage consumed per business object.

### Visualization (canvas `chartRateLimit`, responsive width × 300)

Line chart of API calls per rolling hour breaching the rate cap.

- **Title (bold 13px `#1a5276`, left at padLeft, y=20):** "Sliding-window rate limit: calls per rolling hour vs the 200-call cap".
- **Data (18 points):** `[24, 38, 52, 70, 95, 124, 152, 176, 194, 213, 208, 168, 128, 96, 74, 58, 44, 36]`; y scale 0-240, gridlines every 60 with numeric labels.
- **Cap line:** dashed (6/4) orange `#e67e22` width 2 at 200, bold 11px orange label "cap: 200 calls / rolling hour" above-left; faint red zone `rgba(231,76,60,0.08)` fills the area above the cap.
- **Calls line:** width 2 with 2.5px dots; blue `#1a5276` below the cap, red `#e74c3c` on segments/dots above it (points 213 and 208 breach).
- **Annotation (bold 11px red, centered above the breach):** "HTTP 400 (code 4/17/32) until the window slides back under".
- **Caption (italic 11px `#666`, bottom center):** "Illustrative traffic — the cap is per user per page, evaluated on a rolling window, not a daily reset."


## Section 3: Granularity & Limitations

- Page metrics: available in `day`, `week`, `days_28` periods
- Post metrics: lifetime aggregates only (no time-series per post)
- Data available after 24-48 hours (not real-time)
- Instagram insights require a Business/Creator account; audience/demographic insights additionally require 100+ followers
- Historical data: up to 2 years for page-level, 2 years for post-level
- Metrics can be "gated" — some require specific permission scopes
- Demographic data only shown if audience is 100+ people in a bucket (privacy threshold)

**Key point 1:** **Key constraint:** Post-level metrics are lifetime-only. You cannot ask "how did this post perform on day 3 vs day 7." You get a single cumulative number. To build a time-series, you must poll repeatedly and store deltas yourself.

**Key point 2:** **Privacy thresholds:** If fewer than 100 people in a demographic bucket (e.g., women 18-24 in Portland), Meta suppresses that row entirely. Small-audience pages get very sparse demographic data.

### Visualization (canvas `chartLifetimeOnly`, responsive width × 300)

Cumulative post-impressions curve reconstructed from daily polling, ending at the single number the API actually returns.

- **Title (bold 13px `#1a5276`, left at padLeft, y=20):** "Post metrics are lifetime-only: the curve exists only if you polled for it".
- **Data (14 daily polls, cumulative):** `[1200, 2600, 3900, 4700, 5200, 5600, 5850, 6000, 6100, 6180, 6240, 6280, 6300, 6310]`; y scale 0-7K, gridlines every 2K ("0K".."6K"); x labels "d1", "d3", ... every other day.
- **Reconstructed curve:** blue `#1a5276` width-2 line with 3px dots at every poll.
- **API endpoint marker:** 7px green `#27ae60` dot on the final point, bold 11px green right-aligned label "what the API returns: 6,310 (one lifetime number)".
- **Annotation (bold 11px orange `#e67e22`, two lines near day 2):** "day-2 spike, day-7 plateau —" / "visible only in your own stored deltas".
- **Caption (italic 11px `#666`, bottom center):** "Illustrative cumulative impressions — blue dots are your daily polls; the API alone gives only the endpoint."


## Section 4: Business Scenarios & Deprecation Notes

- Content performance dashboards, audience growth analysis, cross-platform comparison
- Graph API version deprecation cycle: each version supported ~2 years, then sunset
- Versions are released several times a year (e.g., v23.0) — older versions return errors after deprecation
- Many Page Insights metrics (including `page_engaged_users`) were removed in Meta's metrics purge; Instagram `impressions` is deprecated in favor of `views`
- Facebook Analytics (the standalone product) was deprecated June 2021
- CrowdTangle (social listening) shut down August 14, 2024, replaced by Meta Content Library
- Organic reach metrics have declined over time — comparing across years is misleading

**Key point 1:** **Deprecation trap:** If you hardcode a Graph API version (e.g., `/v16.0/`), your integration silently breaks ~2 years later. Use versionless calls cautiously — they default to the oldest supported version, which itself eventually sunsets.

**Key point 2:** **Organic reach decline:** A page that had 15% organic reach in 2014 may see 2-3% in 2026 for the same content quality. Year-over-year reach comparisons without controlling for algorithm changes are meaningless.

### Visualization (canvas `chartReachDecline`, responsive width × 300)

Declining organic-reach benchmark line, 2014-2026.

- **Title (bold 13px `#1a5276`, left at padLeft, y=20):** "Organic reach benchmark drift: the same content, a shrinking denominator".
- **Data (x labels 2014, 2016, ... 2026):** reach % `[15, 10.5, 7, 5.2, 3.8, 2.9, 2.4]`; y scale 0-16% with gridlines every 4%.
- **Line:** red `#e74c3c` width 2 with 3px dots; area under the curve filled `rgba(231,76,60,0.12)`.
- **Endpoint labels (bold 11px `#1a5276`):** "15%" at the 2014 point, "2.4%" at the 2026 point.
- **Annotation (bold 11px red, centered mid-chart):** "~6× decline from algorithm changes alone — raw YoY reach comparisons are meaningless".
- **Caption (italic 11px `#666`, bottom center):** "Illustrative industry-benchmark trajectory, not measured data."


## Official API References

- [Meta Graph API docs](https://developers.facebook.com/docs/graph-api/) — core Graph API reference used for all Insights endpoints
- [Pages API docs](https://developers.facebook.com/docs/pages-api/) — Facebook Pages API, including Page Insights metrics

## Regeneration instructions

- **Layout:** h1 + `.subtitle` (a `<div class="subtitle">` on this page), then one `.obj-table` with 4 rows; each row: left `<td>` (45%) holds `.obj-title` + `<ul>` bullets, right `<td>` (55%, text-align center) holds a `.payload` block + canvas (row 1) or two `.key-point` boxes + a canvas (rows 2-4). After the table: h2 "Official API References" + link list. No nav bar, no back/home links.
- **Table style:** `.obj-table` full width, `border-collapse: collapse`; td vertical-align top, padding 16px, border `1px solid #2980b9`.
- **Key-point / payload style:** `.key-point` — background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px 14px, 0.93em. `.payload` — same background/border, monospace (ui-monospace, Menlo), 0.78em, `white-space: pre`, `overflow-x: auto`, left-aligned.
- **Page CSS:** body system sans-serif, white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; `.obj-title` bold `#1a5276` 1.1em; links `#1a5276`; li 0.93em; canvas `display: block; margin: 12px auto 0; width: 100%`.
- **Palette:** `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange; area fills `rgba(26,82,118,0.35)` and `rgba(39,174,96,0.3)`; secondary text `#666`, body text `#2c3e50`.
- **Canvas:** fixed height 380 with width from bounding rect; backing store sized to rendered width × `window.devicePixelRatio` (display capped via `style.maxWidth`) and `ctx.scale` back to logical coordinates applied.
- In regenerated HTML, any card/page links use `.html` extensions.
