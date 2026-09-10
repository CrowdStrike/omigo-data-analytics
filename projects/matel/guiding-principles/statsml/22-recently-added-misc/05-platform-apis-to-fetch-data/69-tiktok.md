# TikTok

**Page type:** detail page (two-column obj-table layout under an "Overview" h2: text left 45%, payload + canvas right 55%, one row)
**HTML title tag:** TikTok — Platform APIs

**Subtitle:** Lets an app read a consenting user's own TikTok videos and stats, publish to their account, and — for vetted researchers only — search public videos and comments.

**Verified badge:** Last verified: August 2026

## Overview

### What you can get

- A signed-in user's own profile and the view/like counts on their own videos (with their consent)
- The ability to publish videos to a consenting user's account
- Public videos, comments and user info at scale — but only through the separate Research programme
- Ad campaign performance for ad accounts you manage

### Key-point callout

**Who you are decides what you can see.** Broad access to public TikTok content exists in exactly one place — the Research API — and it is granted only to vetted researchers: academics, plus vetted non-profit and civil-society researchers in Europe under the Digital Services Act. There is no commercial route to it.

### Watch out for

- Deleted or private videos simply vanish from results — re-running the same query later returns a different, smaller population
- Counts are point-in-time snapshots; there is no history, so trends must be built by polling and storing yourself
- If a vendor sells "broad TikTok data" that did not come from the Research programme, it was scraped — and the buyer inherits that risk

### Research API — search public videos by hashtag (abridged)

Code block (pre, monospace, left border #1a5276):

```
POST /v2/research/video/query/
{
  "query": { "and": [
    { "operation": "EQ", "field_name": "hashtag_name",
      "field_values": ["climate"] } ] },
  "start_date": "20260801", "end_date": "20260807"
}

// abridged response
{ "videos": [ { "id": 7401234567890,
                "region_code": "US",
                "view_count": 41200,
                "like_count": 3810 } ],
  "has_more": true }
```

### Which data classes each programme can reach

### Visualization (canvas `tiktokAccessMatrix`, 100% width × 380px CSS height)

Access matrix (heatmap grid): 8 data-class rows × 5 programme columns, each cell a colored pill marked "yes" / "part" / "no".

- **Title (bold 13px, `#1a5276`, top left):** "Access breadth by programme, and the gate on each".
- **Subtitle (italic 10px, `#666`):** "qualitative availability map, not measured magnitudes".
- **Columns (two-line bold header, `#2c3e50`, plus italic purple `#8e44ad` gate label below):**
  1. "Display API" — gate "app review"
  2. "Content Posting" — gate "review + audit"
  3. "Research API" — gate "academic vetting"
  4. "Commercial Content" — gate "gated application"
  5. "Business / Marketing" — gate "advertiser only"
- **Rows and values** (0 = not available red `#e74c3c` "no", 1 = partial/conditional orange `#e67e22` "part", 2 = available green `#27ae60` "yes"), ordered by column 1–5:
  - Own content (read): [2, 1, 0, 0, 2]
  - Own metrics: [2, 0, 0, 0, 2]
  - 3rd-party public videos: [0, 0, 2, 1, 0]
  - 3rd-party public comments: [0, 0, 2, 0, 0]
  - 3rd-party public user info: [0, 0, 2, 0, 0]
  - Follower / following graph: [1, 0, 1, 0, 0]
  - Ad performance (own spend): [0, 0, 0, 0, 2]
  - Ad / commercial transparency: [0, 0, 0, 2, 0]
- **Rendering:** padding top 76, right 12, bottom 54, left min(168, 32% of width); cell pills max 84×20px at 0.85 alpha with `rgba(0,0,0,0.12)` border and bold 9.5px white mark text; zebra bands `rgba(26,82,118,0.04)` on odd rows; row labels 10.5px right-aligned, colored `#1a5276` when starting with "3rd-party", else `#2c3e50`; grid frame and column dividers `#ddd`.
- **Highlight:** dashed purple rectangle (`#8e44ad`, width 2, dash 4/3) around the entire Research API column including its header, marking the single broad-access route.
- **Legend (bottom left, 10px):** color swatches — green "available", orange "partial / conditional", red "not provided".
- **Conclusion (italic 10px, `#8e44ad`, bottom left):** "third-party content is reachable in one column only, and that column is academically vetted".

## Official API References

- [TikTok for Developers](https://developers.tiktok.com/) — developer portal root for Login Kit, Display API and Content Posting API documentation
- [Research API](https://developers.tiktok.com/products/research-api/) — the vetted-researcher programme for public videos, comments and user info

## Regeneration instructions

- **Layout:** single detail page: h1, `.subtitle` paragraph, `.verified` badge span, `h2` "Overview", one `.obj-table` (full-width, border-collapse, one `<tr>`): left `<td>` 45% with `.section-head` headings ("What you can get", "Watch out for"), bullet lists and a `.key-point` callout; right `<td>` 55% with a `.section-head`, a `<pre>` request/response payload and the canvas (`height="380"` attribute). Then an `h2` "Official API References" with a two-link list. Links in HTML are external URLs as given.
- **Page CSS:** body system sans-serif (-apple-system stack), line-height 1.6, text `#2c3e50`, white background, padding 30px 40px; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; `.verified` badge — background `#eaf2f8`, border `1px solid #2980b9`, color `#1a5276`, 2px 10px padding, 4px radius, 0.8em; h2 1.3em `#1a5276` with `2px solid #2980b9` bottom border; table cells `1px solid #ddd` border, 16px padding, top-aligned; `.section-head` bold `#1a5276` 0.95em; li 0.93em; links `#1a5276`.
- **Pre style:** background `#f8f9fa`, left border `3px solid #1a5276`, ui-monospace 0.78em, 12px padding, 4px radius.
- **Key-point style:** background `#f8f9fa`, left border `3px solid #e74c3c`, padding 10px 14px, 0.93em.
- **Canvas:** `display: block; width: 100%; margin: 16px auto 0`; fixed 380px CSS height set in JS; sized from `getBoundingClientRect()` and scaled by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates), redrawn on window resize.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, purple accent `#8e44ad`; zebra `rgba(26,82,118,0.04)`; grays `#666`/`#ddd`.
- No nav bar, no back/home links. In regenerated HTML, any card links use `.html` extensions.
