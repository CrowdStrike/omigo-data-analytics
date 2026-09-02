# Twitter/X Tweets API (v2)

**Page type:** detail page (single two-column obj-table row: text left 45%, JSON payload + canvas right 55%; followed by a references section)
**HTML title tag:** Twitter/X Tweets API (v2) — Platform APIs

**Subtitle:** Lets you read and search public posts on X (Twitter) — but meaningful read access now starts at a paid tier.

**Verified badge:** Last verified: August 2026

## Left column

### What you can get

- Individual posts with likes, reposts, replies and view counts
- Search over roughly the last 7 days of posts; full history only on top tiers
- A live stream of posts matching your filters (paid tiers only)
- Posting and replying from your own account

**Key-point callout:** **The Free tier is essentially write-only** — roughly 1,500 posts per month and no meaningful read access. Reading costs real money: Basic is $200/month, Pro $5,000/month, and full-archive search and the filtered stream require Pro or Enterprise. Budget for the API before designing anything around it.

### Watch out for

- The Academic Research tier was discontinued in 2023 — older research pipelines built on it are not reproducible today
- Deleted posts and historical DMs are not retrievable
- Prices and tier limits have changed repeatedly; re-check before committing

## Right column

### What a post looks like

JSON payload block (`.payload`, monospace):

```
{
  "data": {
    "id": "1346889436626259968",
    "text": "Learn how to use the user Tweet timeline...",
    "author_id": "2244994945",
    "created_at": "2021-01-06T18:40:40.000Z",
    "public_metrics": {
      "retweet_count": 11,
      "reply_count": 2,
      "like_count": 38,
      "impression_count": 5421
    }
  }
}
```

### Tweet Object Structure

### Visualization (canvas `treeChart`, responsive width × 400)

Top-down tree diagram of the Tweet object structure: rounded-rectangle nodes (20px tall, radius 4, white text, width fitted to label + 8px padding each side) connected by curved gray bezier lines (`#bbb`, width 1.2).

- **Node colors by level:** root `#1a5276`, level 1 `#2980b9`, level 2 `#5dade2`, metrics leaves `#27ae60`.
- **Root (bold 12px, top center, y=30):** "Tweet Object".
- **Level 1 (y=80, spread evenly across the width, 11px):** "data", "includes", "errors" (errors has no children).
- **Level 2 under "data" (y=150, spread over the left two-thirds, 10px):** "id", "text", "author_id", "created_at", "public_metrics", "referenced_tweets", "attachments", "context_annotations".
- **Level 2 under "includes" (y=150, spread over the right third, 10px):** "users", "media", "polls", "places", "tweets".
- **Level 3 under "public_metrics" (y=240, 180px spread centered on the public_metrics node, 9px, green):** "retweet_count", "reply_count", "like_count", "quote_count", "impression_count".
- **Legend (bottom left, y = H−30, 12×12 rounded swatches, labels `#2c3e50` 10px):** "Root" (`#1a5276`), "Level 1" (`#2980b9`), "Level 2" (`#5dade2`), "Metrics" (`#27ae60`).
- Redraws on window resize; width taken from `getBoundingClientRect()`.

## Official API References

- [X Developer Platform](https://developer.x.com/) — top-level developer portal, access tiers and sign-up
- [X API Documentation](https://docs.x.com/) — current documentation site for the X API

## Regeneration instructions

- **Layout:** platform-APIs detail page. h1, `.subtitle` paragraph, `.verified` badge span, then a single-row `.obj-table` (full width, border-collapse): left `<td>` 45% with `.section-head` headings + bullet lists + one `.key-point` callout; right `<td>` 55% with a `.section-head` ("What a post looks like", margin-top 0), a `.payload` JSON block, another `.section-head`, and the canvas. After the table, an `h2` "Official API References" with a link list.
- **Page CSS:** universal `* { margin:0; padding:0; box-sizing:border-box; }` reset; body system sans-serif (-apple-system stack), line-height 1.6, text `#2c3e50`, white background, padding 40px; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em, margin-bottom 18px; `.verified` inline-block, background `#f0f8ff`, border `1px solid #2980b9`, color `#1a5276`, padding 2px 8px, radius 4px, 0.8em; table cells `1px solid #ddd`, padding 16px; `.section-head` bold `#1a5276`, block; `.payload` background `#f8f9fa`, left border `3px solid #1a5276`, monospace 0.78em, padding 14px, radius 4px, pre whitespace; `.key-point` background `#f8f9fa`, left border `3px solid #e74c3c`, padding 10px 14px, 0.93em; `li` 0.93em; links `#1a5276`; canvas `display:block`, `width:100%`, margin `16px auto 0`.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`; tree levels use blue shades `#2980b9` and `#5dade2`.
- **Canvas:** declared with `height="400"` attribute and `width:100%` CSS; scaled by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates), redrawn on resize.
- No nav bar, no back/home links. In regenerated HTML, any card links use `.html` extensions.
