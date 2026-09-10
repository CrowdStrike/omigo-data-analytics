# YouTube Content API (Data API v3)

**Page type:** detail page (two-column obj-table layout: text left 45%, payload + canvas right 55%, one row)
**HTML title tag:** YouTube Content API (Data API v3) — Platform APIs

**Subtitle:** Lets you search YouTube, read video details and stats, and upload and manage videos — all metered by a small daily quota.

**Verified badge:** Last verified: August 2026

## What you can get

- Video details: title, duration, view / like / comment counts
- Search across videos and channels
- Upload videos and manage playlists and captions
- Public channel information

### Key-point callout

**Everything is metered by a quota of 10,000 units per day** — and operations cost wildly different amounts. Reading one video's details costs 1 unit, a search costs 100, a single upload costs 1,600 (about 6 uploads a day). Design around quota cost, not request count.

## Watch out for

- Analytics (watch time, audience) live in a separate YouTube Analytics API, not here
- There is no community-posts endpoint — channel posts are not retrievable through this API
- Quota resets at midnight Pacific Time
- Private and unlisted videos are visible only to their owner

## What a video record looks like — `videos.list`

Code block (pre.payload, monospace, left border #1a5276):

```
{
  "id": "dQw4w9WgXcQ",
  "snippet": {
    "publishedAt": "2026-08-15T14:00:00Z",
    "title": "Q3 Product Update",
    "categoryId": "28"
  },
  "contentDetails": { "duration": "PT12M34S" },
  "statistics": {
    "viewCount": "14523",
    "likeCount": "892",
    "commentCount": "67"
  }
}
```

## Quota Cost per Operation

### Visualization (canvas `quotaChart`, 100% width × 400px CSS height)

Horizontal bar chart of API quota cost per operation, with per-day capacity notes on the right.

- **Title (bold 13px, `#1a5276`, top center):** "YouTube API Quota Cost per Operation".
- **Data (top to bottom):** label / cost (units) / bar color / italic note at right edge:
  - videos.insert (upload) — 1600 — `#e74c3c` — "~6 uploads/day"
  - captions.insert — 400 — `#e67e22` — "25/day"
  - search.list — 100 — `#e67e22` — "100/day"
  - videos.update — 50 — `#1a5276` — "200/day"
  - videos.rate — 50 — `#1a5276` — "200/day"
  - playlists.insert — 50 — `#1a5276` — "200/day"
  - videos.list — 1 — `#27ae60` — "10,000/day"
- **Scale:** bar width proportional to cost with maxCost = 1800; padding left 130, right 100, top 40, bottom 30; bar height min(30, 65% of row); vertical baseline axis at padLeft in `#ccc`.
- **Labels:** operation names right-aligned left of the axis (11px `#2c3e50`); cost values bold 11px `#333` right of each bar formatted as "1,600 units", "400 units", … "1 unit(s)" via `toLocaleString() + ' units'`; per-day notes italic 10px `#666` right-aligned at the canvas right edge.
- **Quota line:** vertical dashed red line (`#e74c3c`, dash 5/4, width 1.5) at x = 10000/1800 of chart width with centered bold red label "Daily quota: 10,000" below the chart — note: since 10,000 > maxCost this x falls beyond the plot area, so the guard `if (quotaX < padLeft + chartW)` means the line does not actually render.

## Official API References

- [YouTube Data API v3](https://developers.google.com/youtube/v3) — main reference for videos, playlists, search, captions
- [Quota Cost Calculator](https://developers.google.com/youtube/v3/determine_quota_cost) — official per-operation quota unit table

## Regeneration instructions

- **Layout:** single detail page: h1, `.subtitle` paragraph, `.verified` badge span, one `.obj-table` (full-width, border-collapse, one `<tr>`): left `<td>` 45% with `.section-header` headings ("What you can get", "Watch out for"), bullet lists and a `.key-point` callout; right `<td>` 55% with a `.section-header`, a `<pre class="payload">` JSON record and the canvas. Below the table: an `h2` "Official API References" with a two-link list. Links in HTML are external URLs as given.
- **Page CSS:** body system sans-serif (-apple-system stack), line-height 1.6, text `#2c3e50`, white background, padding 40px; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; `.verified` badge — background `#f0f8ff`, border `1px solid #2980b9`, color `#1a5276`, 2px 8px padding, 4px radius, 0.8em; table cells `1px solid #ddd` border, 16px padding, top-aligned; `.section-header` bold `#1a5276` 0.95em; li 0.92em; links `#1a5276`.
- **Pre style:** `pre.payload` — background `#f8f9fa`, left border `3px solid #1a5276`, ui-monospace 0.78em, 12px padding, 4px radius.
- **Key-point style:** background `#f8f9fa`, left border `3px solid #e74c3c`, padding 10px 14px, 0.92em.
- **Canvas:** `display: block; width: 100%; height: 400px; margin: 16px auto 0`; sized from `getBoundingClientRect()` and scaled by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates), redrawn on window resize.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`; grays `#666`/`#333`/`#ccc`.
- No nav bar, no back/home links. In regenerated HTML, any card links use `.html` extensions.
