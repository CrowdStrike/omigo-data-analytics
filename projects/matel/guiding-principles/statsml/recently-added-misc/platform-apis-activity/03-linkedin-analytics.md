# LinkedIn Analytics API

**Page type:** detail page (two-column obj-table layout: text left 45%, payload/canvas right 55%, one row per section)
**HTML title tag:** LinkedIn Analytics API

**Subtitle:** Organization analytics, share statistics, and campaign metrics — with partner-level access gates and deliberate restrictions on personal profile data.

## What the API Provides

- **Organization (company page) analytics:** follower count, follower demographics, page views
- **Share (post) statistics:** impressions, clicks, engagement, comments, likes, shares
- **Profile-level (personal):** not available — profile views and search appearances are UI-only features, never exposed via API; Basic OAuth (OpenID Connect) returns only name, email, and photo
- **Marketing API:** campaign performance, ad impressions, conversions, spend
- **Visitor demographics on company pages:** industry, company size, function, seniority

**Key point (callout):** The asymmetry is intentional: organizations paying for ads or managing employer brands get rich data. Individual professionals get almost nothing programmatically — LinkedIn wants them checking the app.

### Payload block (right column, monospace `.payload` pre)

```
// ── illustrative payload; field names from LinkedIn Marketing API docs, values are not real ──
{
  "elements": [
    {
      "totalShareStatistics": {
        "shareCount": 12,
        "clickCount": 287,
        "engagement": 0.0342,
        "likeCount": 94,
        "impressionCount": 8401,
        "commentCount": 18,
        "uniqueImpressionsCount": 7230
      },
      "share": "urn:li:share:7091234567890123456",
      "timeRange": {
        "start": 1724112000000,
        "end": 1724198400000
      }
    }
  ],
  "paging": {
    "start": 0,
    "count": 10,
    "total": 47
  }
}
```

### Visualization (canvas `c1`, width 100% × 380)

Horizontal bar/timeline chart of data availability windows per metric.

- **Title (bold 14px, `#1a5276`, top center):** "Data Availability Windows".
- **Rows (5, row height 58, bar height 28, bars start at x=170, rounded corners 4px, scale max 24 months):**
  - Post Stats — bar label "Lifetime only", 24 months long, fill `rgba(26,82,118,0.35)`
  - Org Followers — "Daily, up to 12 months", 12 months, fill `#27ae60`
  - Page Views — "Daily, up to 12 months", 12 months, fill `#27ae60`
  - Follower Demographics — "Lifetime snapshot only", 24 months, fill `rgba(26,82,118,0.35)`
  - Campaign Metrics — "Daily, up to 2 years", 24 months, fill `#27ae60`
- **Row labels:** right-aligned dark text `#2c3e50` 12px left of the bar area; window label in bold white 11px inside each bar (left-aligned, 8px inset).
- **Timeline axis:** vertical tick lines `#ddd` at 0, 3 mo, 6 mo, 12 mo, 18 mo, 24 mo with gray `#888` 11px labels below the bars.
- **Legend (bottom center, 11px):** "■ Daily time-series (long window)" in `#27ae60` and "■ Lifetime totals / snapshot only" in `#1a5276`.

## Access & Authentication

- OAuth 2.0 three-legged flow
- **Personal profile:** Sign In with LinkedIn via OpenID Connect — name, email, photo only; no personal analytics (r_liteprofile deprecated, replaced by openid/profile scopes)
- **Organization analytics:** requires Marketing Developer Platform access — must apply as a partner
- **Marketing API:** requires ads_read or r_organization_social scope — partner-level approval only
- Partner approval can take weeks/months, requires demonstrating a product use case
- **Rate limits:** per-endpoint and per-app/per-member — generous for approved partner applications

**Key point (callout):** The access hierarchy is steep. A solo developer cannot get organization analytics without going through a formal partnership application. This is unlike most social APIs where a developer token unlocks read access immediately.

### Visualization (canvas `c2`, width 100% × 340)

Stacked tier diagram of access levels (three rounded boxes in a vertical column).

- **Title (bold 14px, `#1a5276`, top center):** "Access Tiers — What Each Level Unlocks".
- **Tiers (rounded rect boxes, full width minus 20px padding each side, 65px tall, 80px row pitch, 2px colored border, 6px radius):**
  - "Basic OAuth" — "Name, email, photo only (OpenID Connect)"; fill `rgba(26,82,118,0.15)`, border `#1a5276`
  - "Marketing Developer" — "Org analytics, follower demographics, page stats"; fill `rgba(39,174,96,0.15)`, border `#27ae60`
  - "Ads Partner" — "Campaign metrics, conversions, spend data"; fill `rgba(230,126,34,0.15)`, border `#e67e22`
- **Box text:** tier name bold 13px in the border color, access description 12px `#2c3e50` below it.
- **Arrows between tiers (gray `#888` 11px, right-aligned at box bottom-right):** "apply as partner ↓" after tier 1, "ads approval ↓" after tier 2.
- **Footer note (bottom center, gray `#888` 11px):** "Rate limits are per-endpoint and per-app/member — generous for approved apps".

## Granularity & Limitations

- **Organization follower stats:** daily granularity, available via timeIntervals parameter
- **Share statistics:** lifetime totals only per post (no daily breakdown per post)
- **Page statistics:** page views and visitor demographics for organization pages, daily via timeIntervals
- **Follower demographics:** lifetime snapshot breakdowns (industry, function, seniority) — no history
- **Data latency:** 24-48 hours for organization analytics
- No real-time metrics for any endpoint
- Personal analytics are deliberately restricted — LinkedIn wants users on-platform

**Key point (callout):** The combination of lifetime-only post stats and no real-time data means you cannot build a time-series of post performance. You get where a post ended up, not how it got there.

### Visualization (canvas `c3`, width 100% × 380)

Canvas-drawn comparison table of temporal granularity by metric type.

- **Title (bold 14px, `#1a5276`, top center):** "Temporal Granularity by Metric Type".
- **Columns (header bold 12px `#1a5276` with a 1px underline in `#1a5276`, at x = 30 / 180 / 320 / 420):** Metric, Granularity, Latency, Time-series?
- **Rows (12px text, 52px row height, alternating background `rgba(26,82,118,0.05)` on even rows):**

| Metric | Granularity (bold, colored) | Latency (`#666`) | Time-series? (bold, colored) |
|---|---|---|---|
| Org Followers | Daily (`#27ae60`) | 24-48h | Yes (`#27ae60`) |
| Campaign Metrics | Daily (`#27ae60`) | 24-48h | Yes (`#27ae60`) |
| Page Views | Daily (`#27ae60`) | 24-48h | Yes (`#27ae60`) |
| Follower Demographics | Lifetime total (`#e74c3c`) | N/A | No (`#e74c3c`) |
| Post Stats | Lifetime total (`#e74c3c`) | N/A | No (`#e74c3c`) |

- **Footer note (bottom center, gray `#888` 11px):** "No real-time metrics available for any endpoint".

## Business Scenarios & Deprecation Notes

- Employer branding dashboards, recruitment marketing ROI, thought leadership measurement
- LinkedIn's v1 API was fully deprecated in 2019 — massive breaking change for third-party tools
- Sign In with LinkedIn migrated to OpenID Connect (2023) — r_liteprofile/r_emailaddress scopes retired
- Versioned API uses YYYYMM dates (launched 2022) — versions move monthly/quarterly, each with a planned retirement
- Community Management API (newer) expanding some access for approved tools
- No bulk export — must paginate through all posts individually

**Key point (callout):** LinkedIn has a pattern of opening access, letting an ecosystem form around it, then restricting or deprecating. Third-party tools built on v1 had to rebuild entirely. Plan for the rug pull.

### Visualization (canvas `c4`, width 100% × 340)

Horizontal timeline with alternating above/below event labels.

- **Title (bold 14px, `#1a5276`, top center):** "LinkedIn API Deprecation History".
- **Timeline:** horizontal line in `#1a5276` (2px) at y=140 from x=60 to width−60; year span 2014–2026; small ticks with gray `#888` 10px year labels every 2 years from 2015 to 2025.
- **Events (5px dot on the line, 1.5px vertical connector in event color, label bold 11px in event color with the year in gray 10px below; alternating above/below the line):**
  - 2015 — "v1 API at peak adoption" — `#27ae60`
  - 2019 — "v1 fully deprecated" — `#e74c3c`
  - 2022 — "Versioned API launched (YYYYMM)" — `#1a5276`
  - 2023 — "Sign In moves to OpenID Connect" — `#e67e22`
  - 2025 — "Community Mgmt API expansion" — `#27ae60`
- **Footer note (bottom center, gray `#888` 11px):** "Each versioned release has a planned retirement — no bulk export means rebuilding integrations".

## Official API References

- [LinkedIn Marketing API docs](https://learn.microsoft.com/en-us/linkedin/marketing/) — organization analytics, share statistics, and campaign reporting live here
- [LinkedIn API documentation](https://learn.microsoft.com/en-us/linkedin/) — top-level portal covering all LinkedIn developer programs and access tiers

## Regeneration instructions

- **Layout:** detail page with `.obj-table`: full-width `border-collapse: collapse` table, one `<tr>` per section; left `<td>` (45%) holds `.obj-title` + bullets + `.key-point` callout, right `<td>` (55%, `text-align: center`) holds the `.payload` pre (row 1 only) and one canvas per row. After the table, an `<h2>Official API References</h2>` with a plain `<ul>` of links.
- **Page style:** body system sans-serif, white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; table cell borders `1px solid #2980b9`, padding 16px; `.obj-title` bold `#1a5276` 1.1em; `li` 0.93em; links `#1a5276`. No nav bar, no back/home links.
- **Callout style:** `.key-point` — background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px 14px, 0.93em. `.payload` — same background/border, monospace (ui-monospace/Menlo) 0.78em, `white-space: pre`, left-aligned.
- **Canvas:** `display: block; width: 100%`; intrinsic `height` attribute per chart (380/340/380/340); a shared `setupCanvas(id)` helper reads `getBoundingClientRect().width`, sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`), fixes CSS height, and calls `ctx.scale` so drawing stays in logical coordinates.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, bar fill `rgba(26,82,118,0.35)`, gray text `#666`/`#888`/`#2c3e50`.
- In regenerated HTML, any card/nav links use `.html` extensions (this page has none; external doc links stay as-is).
