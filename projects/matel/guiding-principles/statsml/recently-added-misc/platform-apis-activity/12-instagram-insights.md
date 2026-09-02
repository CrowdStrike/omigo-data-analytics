# Instagram Insights API

**Page type:** detail page (two-column obj-table layout: descriptive text left 45%, payload + canvas right 55%, one row)
**HTML title tag:** Instagram Insights API

**Subtitle:** Reach, views, engagement, story interactions, and follower demographics — available only for Creator and Business accounts via the Meta Graph API.

## What It Provides

Account-level metrics: reach, views, profile views, follower count, follower demographics by age/gender/city/country. Media-level metrics: reach, views, engagement, saves, shares, comments, likes. Story metrics: exits, replies, taps forward/back. Reel metrics: views (formerly plays), reach, shares. Graph API v22.0+ removed `impressions` for media and account insights; Reel `plays` was also consolidated into `views`.

## Authentication

Two routes: the Instagram API with Instagram Login (since late 2024 — Business or Creator account, no Facebook Page required), or the Instagram API with Facebook Login (requires a Facebook App and an Instagram Business/Creator account linked to a Facebook Page). Permissions (Facebook Login route): `instagram_basic`, `instagram_manage_insights`, `pages_show_list`, `pages_read_engagement`.

## Granularity

Account metrics available in daily, weekly, or 28-day periods. Media metrics are lifetime (cumulative since posting). Story metrics are available only during the story's 24-hour lifetime. The since/until window is capped at 30 days per request, and certain metrics (e.g., `follower_count`) only cover the last 30 days — longer time series require repeated windowed requests.

## Rate Limit

**Key-point callout:** The 200-calls formula is the Graph API platform rate limit: an app-level pool of 200 calls per hour × number of app users — not a per-user-token quota. Instagram additionally applies Business Use Case rate limiting per Instagram account.

## Business Scenarios

Influencer analytics platforms, brand partnership valuation, content strategy optimization, audience overlap analysis, competitive benchmarking (public metrics only), ROI tracking for sponsored posts.

## Restrictions

**Key-point callout:** Personal accounts have zero API access to insights. Follower demographic breakdowns require minimum 100 followers. Story insights are available only during the story's 24-hour lifetime. Reach and views can differ dramatically (reach = unique accounts, views = total views including repeats). Cannot access other users' insights — only your own account.

## Deprecation Notes

The legacy Instagram Platform API (api.instagram.com) shut down in 2020. The Basic Display API was sunset on December 4, 2024 — its replacement is primarily the Instagram API with Instagram Login. All access now goes through the Meta Graph API. Graph API v22.0+ removed `impressions` in favor of `views`.

## Payload Example

Monospace `.payload` block (right column), verbatim:

```
// ── illustrative payload; field names from Meta Graph API docs, values are not real ──
// GET /{ig-user-id}/insights?metric=reach,views,
//     profile_views,follower_count&period=day
{
  "data": [
    {
      "name": "reach",
      "period": "day",
      "values": [
        { "value": 4821, "end_time": "2026-08-22T07:00:00+0000" },
        { "value": 5104, "end_time": "2026-08-21T07:00:00+0000" }
      ],
      "title": "Reach",
      "description": "Total number of unique accounts that have seen any of your posts"
    },
    {
      "name": "views",
      "period": "day",
      "values": [
        { "value": 12847, "end_time": "2026-08-22T07:00:00+0000" },
        { "value": 13201, "end_time": "2026-08-21T07:00:00+0000" }
      ],
      "title": "Views",
      "description": "Total number of times your content was viewed"
    }
  ]
}
```

## Reach vs Views — 7-Day Comparison

### Visualization (canvas `chartReachImpressions`, 720×380)

Grouped bar chart: daily views vs reach across a 7-day week.

- **Layout:** padding left 60, right 30, top 40, bottom 50; y scale 0–15,000 with grid lines and labels every 3,000 (`#e0e0e0` grid width 0.5, labels 11px `#666`, right-aligned, formatted with thousands separators).
- **Data:** days `['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']`; views `[11200, 9800, 13500, 12100, 14800, 8900, 10200]` (blue `#1a5276` bars); reach `[4200, 3800, 5100, 4600, 5500, 3400, 3900]` (green `#27ae60` bars). Per day: two bars each 30% of the group width, 4px gap, group starts at 15% of group width; day label centered below (12px `#2c3e50`).
- **Annotation line:** horizontal dashed orange line (`#e67e22`, dash 5/4, width 1.5) drawn 20px above the average-reach height, with bold 11px orange label "ratio ~2.7x" near the right end (average views ≈ 11,500 vs average reach ≈ 4,357).
- **Legend (top left, above the plot):** blue 12×12 swatch labeled "Views", green 12×12 swatch labeled "Reach" (11px `#2c3e50`).

**Caption (`.canvas-label`, centered, 0.82em, `#666`):** Illustrative: Daily views vs reach — 7-day window

## Official API References

- [Instagram API (Meta for Developers)](https://developers.facebook.com/docs/instagram-api) — official Graph API documentation including insights endpoints for Business and Creator accounts

## Regeneration instructions

- **Layout:** platform-API detail page. h1 + `.subtitle`, then a single `.obj-table` with one `<tr>`: left `<td>` (45%) holds `<p class="obj-title">` headings ("What It Provides", "Authentication", "Granularity", "Rate Limit", "Business Scenarios", "Restrictions", "Deprecation Notes") with inline `style="margin-top: 18px;"` after the first, paragraphs and `.key-point` callouts; right `<td>` (55%) holds "Payload Example" title, `.payload` block, "Reach vs Views — 7-Day Comparison" title, canvas, and `.canvas-label` caption. After the table, `<h2>Official API References</h2>` with a `<ul>` of links.
- **Page style:** `* { box-sizing: border-box; margin: 0; padding: 0; }`; body system sans-serif (-apple-system stack), white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`, margin-bottom 4px; `.subtitle` `#666` 1.05em, margin-bottom 24px. No nav bar, no back/home links.
- **Table style:** `.obj-table` full width, border-collapse, margin-bottom 24px; td vertical-align top, padding 16px, border `1px solid #2980b9`; `.obj-title` bold `#1a5276` 1.1em, margin-bottom 8px.
- **Payload block:** `.payload` — background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px, ui-monospace/Menlo 0.78em, `white-space: pre`, `overflow-x: auto`, line-height 1.45, margin 12px 0.
- **Callout:** `.key-point` — background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px 14px, margin 12px 0, 0.93em.
- **Caption:** `.canvas-label` — centered, 0.82em, `#666`, margin-top 6px.
- **Links:** `a { color: #1a5276; }`. In regenerated HTML, any card/page links use `.html` extensions.
- **Canvas:** `display: block; margin: 0 auto`; intrinsic 720×380; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setupCanvas(id, w, h)` helper. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, bar fill `rgba(26,82,118,0.35)`.
