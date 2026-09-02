# Twitter/X Analytics API

**Page type:** detail page (two-column obj-table layout: text left 45%, payload/canvas right 55% centered, one row per section; footer references list)
**HTML title tag:** Twitter/X Analytics API

**Subtitle:** Tweet-level and account-level engagement metrics via the X API v2. Point-in-time snapshots of cumulative counters, not time-series data.

## Section 1: What the API Provides

- Tweet-level metrics: impressions, engagements, retweets, replies, likes, profile clicks, URL clicks
- Account-level: snapshot `public_metrics` via users lookup (followers_count, tweet_count) — no growth time-series
- Available via `GET /2/tweets/:id` with `tweet.fields=public_metrics,non_public_metrics,organic_metrics`
- Non-public metrics (impressions, profile clicks) only available to tweet author
- Data returned is point-in-time snapshot, not time-series

**Key point:** The API gives you a cumulative counter at the moment you ask. If you want a time-series, you must poll repeatedly and store the differences yourself. Many third-party dashboards do exactly this, introducing their own sampling artifacts.

**Payload block (monospace, left-aligned, `.payload`), verbatim:**

```
{
  // ── illustrative payload; field names from X API v2 docs, values are not real ──
  "data": {
    "id": "1780123456789012480",
    "text": "Launching our new feature today...",
    "public_metrics": {
      "retweet_count": 142,
      "reply_count": 38,
      "like_count": 891,
      "quote_count": 23,
      "bookmark_count": 67,
      "impression_count": 48210
    },
    "non_public_metrics": {
      "impression_count": 48210,
      "url_link_clicks": 312,
      "user_profile_clicks": 89
    },
    "organic_metrics": {
      "impression_count": 41003,
      "retweet_count": 128,
      "reply_count": 34,
      "like_count": 802
    }
  }
}
```

### Visualization (canvas `c1`, responsive width × 380)

Vertical bar chart with log-scale-proportioned heights.

- **Title (bold 14px, `#1a5276`, centered, y=24):** "Monthly Tweet Read Limits by Tier"
- **Bars (4):** Free ($0/mo, "~100"), Basic ($200/mo, "10K"), Pro ($5,000/mo, "1M"), Enterprise ($42K+/mo, "10M+")
- **Bar heights (fraction of chart height, log-scale visual mapping):** 0.08, 0.25, 0.60, 0.95
- **Bar colors:** `#e74c3c`, `#e67e22`, `#27ae60`, `#1a5276` — fill at 75% alpha with 2px solid border in same color
- **Chart area:** top 50, bottom h−70, left 60, right w−30; bar gap 20px, bar width evenly divided
- **Axes:** L-shaped axis lines in `#ccc`; rotated y-axis label "Tweets/month (log scale)" in `#666` 11px; dashed (`4,4`) gridlines in `#eee` at 25%, 50%, 75%, 100% of chart height
- **Labels:** tier name bold 12px `#2c3e50` below bar; price 11px `#666` below tier name; value label bold 12px in bar color above each bar
- **Bottom note (11px, `#888`, centered):** "Bar heights use log-scale proportions for readability"

## Section 2: Access & Authentication

- OAuth 2.0 with PKCE or OAuth 1.0a (user context)
- Since the 2023 restructuring: Free tier (essentially write-only, ~100 read posts/mo), Basic ($200/mo, 10K read tweets/mo), Pro ($5000/mo, 1M read tweets/mo), Enterprise (custom)
- App-level authentication for public metrics only; user-context required for non_public_metrics
- Rate limits: Basic = 10,000 tweets/month read, Pro = 1,000,000 tweets/month read
- Per-request rate limits are per-endpoint and tier-dependent, enforced in 15-minute windows

**Key point:** The Free tier is essentially write-only (500 posts/mo at the user level) with only a token read allowance of ~100 posts/month — not enough to track your own tweet metrics. This monetization gate came with the 2023 API paywall restructuring, following the October 2022 acquisition.

### Visualization (canvas `c2`, responsive width × 400)

Four-column tier comparison table rendered on canvas.

- **Title (bold 14px, `#1a5276`, centered, y=24):** "API Access Tiers — What You Get"
- **Columns (name, color, bullet features):**
  - Free, `#e74c3c`: "Mostly write-only", "~100 reads/mo", "No analytics", "500 posts/mo (user)"
  - Basic ($200), `#e67e22`: "10K reads/mo", "Public metrics", "3K tweets/mo post", "2 app IDs"
  - Pro ($5,000), `#27ae60`: "1M reads/mo", "Full metrics", "300K tweets/mo post", "Full archive search"
  - Enterprise, `#1a5276`: "10M+ reads/mo", "Firehose access", "Real-time streams", "Custom limits"
- **Structure:** each column has a filled colored header box (32px tall) with white bold 12px tier name; below, feature bullets as 4px-radius filled dots in the tier color with 11px `#2c3e50` text, 28px line spacing, starting 60px below header
- **Lower section (starting header y + 190):** centered heading "Per-Request Rate Limits" (bold 13px `#1a5276`) with two lines of 12px `#2c3e50` text: "Limits are per-endpoint and tier-dependent," / "enforced in 15-minute windows (app and user context differ)"

## Section 3: Granularity & Limitations

- Metrics are cumulative totals, not time-bucketed — no native "impressions per hour" endpoint
- Historical data: up to 7 days for recent search, full archive search on Pro/Enterprise only
- Filtered stream delivers matching tweets in real time; the 15-minute window applies to rate limiting, not to data aggregation
- `non_public_metrics` and `organic_metrics` are only available for tweets created within the last 30 days; public `impression_count` keeps updating
- No audience demographic breakdown via standard API (only via ads API)

**Key point:** The 30-day availability window means non-public and organic metrics for older tweets are simply gone — if you did not capture them in time, you cannot recover them. Public metrics remain retrievable, but without the click and organic breakdowns.

### Visualization (canvas `c3`, responsive width × 340)

Horizontal bar timeline of data availability windows on a 0–365-day axis.

- **Title (bold 14px, `#1a5276`, centered, y=24):** "Data Availability Windows"
- **Bars (label, days out of 365, color, right-side note):**
  - "Recent Search", 7 days, `#e67e22`, note "7 days"
  - "Non-public/Organic Metrics", 30 days, `#e74c3c`, note "Tweets from last 30 days only"
  - "Streaming (filtered)", 0.01 days (min 8px sliver), `#27ae60`, note "Real-time delivery"
  - "Full Archive (Pro+)", 365 days (full width), `#1a5276`, note "All historical tweets"
- **Layout:** bars start at x=140 (labels right-aligned at x−10 in 12px `#2c3e50`), extend toward w−40; bar height 30px, 18px gaps, starting y=70; fill at 60% alpha with 1.5px solid border; note text in bar color, 11px, left of bar end +8px
- **Axis (below bars):** horizontal line in `#999` with tick marks and labels "0", "7d", "30d", "90d", "365d" at proportional positions (0, 7/365, 30/365, 90/365, 1.0) in `#666` 10px

## Section 4: Business Scenarios & Deprecation Notes

- Social listening dashboards, competitor benchmarking, campaign ROI measurement
- Free v1.1 API access was cut off during the 2023 paywall restructuring — many third-party tools broke
- "Engagement rate" calculation differs between platform UI and API (denominator varies)
- Academic Research tier ($0, 10M tweets/mo) was eliminated in 2023
- Enterprise tier pricing is negotiated, reportedly $42K+/mo for full firehose equivalent

**Key point:** The engagement rate discrepancy is a common source of confusion. The platform UI divides engagements by impressions; some API consumers divide by followers. These produce very different numbers from the same underlying data.

### Visualization (canvas `c4`, responsive width × 340)

Side-by-side comparison boxes showing two engagement-rate formulas from the same data.

- **Title (bold 14px, `#1a5276`, centered, y=24):** "Engagement Rate: Same Data, Different Answers"
- **Scenario data:** engagements = 891, impressions = 48,210, followers = 25,000; computed rates: by impressions 1.85%, by followers 3.56%
- **Left box (green):** fill `rgba(39,174,96,0.12)`, 2px border `#27ae60`, at x=20, y=55, height 130; heading "Platform UI Method" (bold 12px `#27ae60`); formula lines "engagements / impressions" and "891 / 48,210" (11px `#2c3e50`); large value "1.85%" (bold 22px `#27ae60`)
- **Right box (red):** fill `rgba(231,76,60,0.12)`, 2px border `#e74c3c`, mirrored right half; heading "Common API Method" (bold 12px `#e74c3c`); formula lines "engagements / followers" and "891 / 25,000"; large value "3.56%" (bold 22px `#e74c3c`)
- **Difference callout (centered box below, `#f8f9fa` fill, 1.5px `#1a5276` border, w/2 wide × 40 tall):** "~1.9x difference from same underlying data" (bold 13px `#1a5276`)
- **Bottom note (11px, `#888`, centered):** "Neither is wrong — but comparing across tools that use different denominators is meaningless"

## Official API References

- [docs.x.com](https://docs.x.com/) — official X API documentation portal
- [X API v2 docs](https://developer.x.com/en/docs/x-api) — developer platform reference for the X API v2, including tweet metrics fields

## Regeneration instructions

- **Layout:** h1 + `.subtitle`, then one `.obj-table` with 4 rows; each row: left `<td>` (45%) holds `.obj-title` + `<ul>` bullets + `.key-point` box, right `<td>` (55%, text-align center) holds an optional `.payload` `<pre>` block and a canvas. After the table: h2 "Official API References" + link list. No nav bar, no back/home links.
- **Table style:** `.obj-table` full width, `border-collapse: collapse`; td vertical-align top, padding 16px, border `1px solid #2980b9`.
- **Key-point / payload style:** `.key-point` — background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px 14px, 0.93em. `.payload` — same background/border, monospace (ui-monospace, Menlo), 0.78em, `white-space: pre`, `overflow-x: auto`, left-aligned.
- **Page CSS:** body system sans-serif, white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; `.obj-title` bold `#1a5276` 1.1em; links `#1a5276`; li 0.93em; canvas `display: block; margin: 12px auto 0; width: 100%`.
- **Palette:** `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange; secondary text `#666`/`#888`, body text `#2c3e50`.
- **Canvas:** each canvas declares only a `height` attribute; a shared `setupCanvas(id)` helper reads bounding-rect width (fallback 720), sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`), and calls `ctx.scale` so drawing stays in logical coordinates.
- In regenerated HTML, any card/page links use `.html` extensions.
