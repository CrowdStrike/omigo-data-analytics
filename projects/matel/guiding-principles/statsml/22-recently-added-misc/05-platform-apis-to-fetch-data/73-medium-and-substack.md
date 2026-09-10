# Medium Stories API

**Page type:** detail page (h1 + subtitle + verified badge, then h2 sections; one two-column obj-table row: text left 45%, code sample + canvas right 55%)
**HTML title tag:** Medium Stories API — Platform APIs

**Subtitle:** Medium's API only ever let you publish posts in — and it is now closed to new users. Substack has no official API at all.

**Verified badge:** Last verified: August 2026

## API Overview

### Left column

**What you can get** (only with credentials issued years ago)

- Publish a post or draft to a Medium account — write only
- Basic details about the signed-in user and their publications
- Nothing to read back: no articles, stats, search, comments or followers
- Substack: nothing — there is no official public API

**Key point callout:** **The door is closed.** Medium removed self-serve integration tokens around 2023 and stopped accepting new OAuth app registrations, so new users cannot get API access at all — only credentials issued earlier still work. Treat this API as unavailable when planning anything new.

**Watch out for**

- Posts published through the API cannot be edited or deleted through it
- The documentation has been frozen since 2017 and could disappear without notice
- Anything sold as "Medium or Substack data" comes from scraping RSS feeds or web pages, not from an API

### Right column

**Create Post Request** — the one thing the API did (code block, `pre`):

```
POST /v1/users/{userId}/posts
{
  "title": "Building Data Pipelines at Scale",
  "contentFormat": "markdown",
  "content": "# Introduction\n\nData pipelines are...",
  "tags": ["data-engineering", "python", "etl"],
  "publishStatus": "draft"
}
```

**Read vs Write Capabilities Across Platforms**

### Visualization (canvas `capChart`, responsive width × 380)

Grouped vertical bar chart: read vs write endpoint counts for six platforms, with Medium highlighted in red.

- **Data:**
  - Platforms (x categories): `['GitHub', 'Spotify', 'Notion', 'Discord', 'Medium', 'WhatsApp']`
  - Read endpoint counts: `[15, 12, 8, 10, 2, 3]`
  - Write endpoint counts: `[15, 5, 8, 12, 3, 8]`
- **Axes:** y from 0 to max 18, gridlines and labels every 3 (`#eee` gridlines, `#666` 11px labels); L-shaped axis lines `#ccc`. Rotated y-axis label "Endpoint Count" in bold 11px `#1a5276`. Padding: left 50, right 20, top 40, bottom 60.
- **Bars:** two bars per group (read left, write right), each 30% of group width, 4px gap. Colors: read `#27ae60`, write `#1a5276` — except Medium: read `#e74c3c`, write `#c0392b`. Value labels in `#333` 10px above each bar.
- **Platform labels** below axis: 11px `#333`; Medium bold 11px `#e74c3c`.
- **Legend (top right):** green swatch `#27ae60` "Read endpoints", blue swatch `#1a5276` "Write endpoints".
- **Annotation:** italic 10px `#e74c3c` centered under the Medium group: "(red = Medium)".

## Official API References

- [Medium API Documentation](https://github.com/Medium/medium-api-docs) — the only official Medium API reference; the repository is frozen and access is no longer granted to new users. Substack has no official public API or developer documentation

## Regeneration instructions

- **Layout:** platform-APIs detail page. h1, `.subtitle` paragraph, `.verified` badge span, h2 "API Overview" with a single-row two-column `table.obj-table` (left td 45% text with plain `<strong>` paragraph headings, right td 55% code sample + canvas), then h2 "Official API References" with a link list.
- **Page CSS:** body system sans-serif, `#2c3e50` text, white background, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; h2 1.3em `#1a5276` with 2px `#2980b9` bottom border; `.verified` badge — background `#eaf2f8`, border `1px solid #e0e0e0`, color `#1a5276`, padding 2px 10px, radius 4px, 0.8em; obj-table cells padding 16px, no borders; `pre` — background `#f4f4f4`, padding 14px, radius 6px, 0.82em; `.key-point` — background `#f8f9fa`, left border `3px solid #e74c3c`, padding 10px 14px, 0.93em; links `#1a5276`. No nav bar, no back/home links.
- **Canvas:** `width: 100%` CSS, height attribute 380; resize handler re-reads `getBoundingClientRect()` and redraws; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c` (dark-red variant `#c0392b`), orange `#e67e22`, bar fill `rgba(26,82,118,0.35)`.
- In regenerated HTML, any card/page links use `.html` extensions.
