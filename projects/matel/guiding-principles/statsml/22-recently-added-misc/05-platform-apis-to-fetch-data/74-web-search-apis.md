# Web Search APIs

**Page type:** detail page (h1 + subtitle + verified badge, then h2 sections; one two-column obj-table row: text left 45%, code sample + canvas right 55%)
**HTML title tag:** Web Search APIs — Platform APIs

**Subtitle:** APIs that let a program run web searches and get results back — plus Google's private report of how your own site performs in search.

**Verified badge:** Last verified: August 2026

## Overview

### Left column

**What you can get**

- Ranked search results (titles, links, snippets) from Google Programmable Search, Brave and similar services
- Your own site's performance in Google Search — queries, clicks, impressions, positions — from Search Console
- Bulk copies of crawled web pages from Common Crawl: a downloadable archive, not a query service
- Not any more: Bing's standalone search API was retired; its replacement answers questions rather than returning ranked lists

**Key point callout:** **Search numbers are slippery.** Result counts are rough estimates, rankings change from minute to minute, and Search Console hides rare queries for privacy — so per-query numbers never add up to the site totals. That gap is deliberate and cannot be closed by fetching more data.

**Watch out for**

- Switching search providers mid-study puts a jump in your data that looks like a real-world change but is not
- Deleted, blocked and paywalled pages are invisible to every index, so "how common is X on the web" is always understated
- Search Console keeps only about 16 months of data — export it continuously or lose it
- Providers' terms usually forbid storing results long term or building your own index from them

### Right column

**Search Console query report — abridged** (code block, `pre`):

```
POST .../searchAnalytics/query   { "dimensions": ["query"] }

{ "rows": [
    { "keys": ["statistical power calculator"],
      "clicks": 1841, "impressions": 24310 },
    { "keys": ["how to check normality"],
      "clicks": 962, "impressions": 18744 }
] }

// sum over query rows < site totals for the same period.
// Rare queries are withheld for privacy — the gap is
// by design, not a pagination bug.
```

**The 16-month window and the withheld-query gap**

### Visualization (canvas `scWindowChart`, responsive width × 380)

Stacked monthly bar chart over 24 months showing the ~16-month retention window, the withheld-query share, and the recent reporting-lag zone.

- **Data (procedural, illustrative):** `MONTHS = 24`, `RETAINED = 16`. For each month index i (0–23):
  - total impressions: `base = 40 + i * 3.2 + 8 * Math.sin(i / 2.1)`
  - coverage (share attributable to returned query rows): `Math.min(0.78, 0.36 + base / 190)`
  - attributable = total × coverage; withheld = total − attributable
  - x labels (9px `#666`, one per slot): `S O N D J F M A M J J A S O N D J F M A M J J A`
- **Title (bold 13px `#1a5276`, top left):** "Search Console: rolling ~16-month window, split by query-row coverage"
- **Mandatory caption (italic 10px `#e74c3c`):** "Stacked magnitudes are ILLUSTRATIVE, not measured. The withheld portion is why query rows do not sum to totals."
- **Axes/scale:** y max 130, plot padding top 58 / right 16 / bottom 74 / left 46; L-shaped axes `#999`; bar width max(4, 66% of slot). Rotated y-axis label "impressions (illustrative)" 10px `#666`; x-axis label "month (oldest left)" 10px `#666` centered below tick labels.
- **Retention boundary:** the oldest 8 slots (i < MONTHS − RETAINED) are shaded `rgba(231,76,60,0.10)` with a vertical dashed `#e74c3c` line (width 1.5, dash 5/4) at the cut. Inside the shaded region, centered: bold 10px `#e74c3c` "dropped: older than ~16 months" over italic "no backfill possible". To the right of the cut line, above the plot: 10px `#e74c3c` "retention boundary".
- **Bars:**
  - Months outside the window: ghost outlines only — dashed (3/3) `rgba(231,76,60,0.45)` rect of the full total height, no fill.
  - Months in the window: bottom segment (attributable) fill `rgba(26,82,118,0.35)` with `#1a5276` stroke 0.8; top segment (withheld) fill `#8e44ad` at alpha 0.75 with `#8e44ad` stroke.
- **Reporting-lag marker:** the last slot shaded `rgba(230,126,34,0.16)` with vertical dashed `#e67e22` line (width 1.2, dash 4/3) and rotated bold 9px `#e67e22` label "reporting lag: incomplete, revises up".
- **Legend (bottom left, 10px `#666`, swatches with strokes):** `rgba(26,82,118,0.35)` / stroke `#1a5276` "attributable to returned query rows"; `#8e44ad` "withheld: anonymized rare queries"; `rgba(231,76,60,0.10)` / stroke `#e74c3c` "aged out of the window".
- **Footnote (italic 10px `#8e44ad`, bottom left):** "withheld share is larger where traffic is thinner — so the long tail is the part you can least see"

## Official API References

- [Google Search Console API](https://developers.google.com/webmaster-tools) — Search Analytics, URL Inspection and Sitemaps documentation
- [Google Custom Search JSON API](https://developers.google.com/custom-search/v1/overview) — Programmable Search Engine JSON API overview, pricing and daily query ceiling

## Regeneration instructions

- **Layout:** platform-APIs detail page. h1, `.subtitle` paragraph, `.verified` badge span, h2 "Overview" with a single-row two-column `table.obj-table` (left td 45% text, right td 55% code sample + canvas), then h2 "Official API References" with a link list.
- **Section heads inside cells:** `.section-head` — `#1a5276`, bold, 0.95em, 16px top margin (0 for first).
- **Page CSS:** body system sans-serif, `#2c3e50` text, white background, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; h2 1.3em `#1a5276` with 2px `#2980b9` bottom border; `.verified` badge — background `#eaf2f8`, border `1px solid #e0e0e0`, color `#1a5276`, padding 2px 10px, radius 4px, 0.8em; obj-table cells `1px solid #e0e0e0`, padding 16px; `pre` — background `#f8f9fa`, left border `3px solid #1a5276`, monospace 0.78em, padding 16px, radius 4px; `.key-point` — background `#f8f9fa`, left border `3px solid #e74c3c`, padding 10px 14px, 0.93em; links `#1a5276`. No nav bar, no back/home links.
- **Canvas:** `width: 100%` CSS, height attribute 380; redraw on window resize using `getBoundingClientRect().width`; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, purple `#8e44ad`, bar fill `rgba(26,82,118,0.35)`.
- In regenerated HTML, any card/page links use `.html` extensions.
