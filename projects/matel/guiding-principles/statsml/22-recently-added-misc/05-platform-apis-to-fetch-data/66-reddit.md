# Reddit Posts & Comments API

**Page type:** detail page (two-column obj-table layout: text left 45%, payload + canvas right 55%, one row)
**HTML title tag:** Reddit Posts & Comments API — Platform APIs

**Subtitle:** Lets you read Reddit posts, comment threads and subreddit listings — free for small-scale use, paid for commercial volume since 2023.

**Verified badge:** Last verified: August 2026

## What you can get

- Posts and full comment threads from any public subreddit
- Listings sorted by hot, new, top, rising
- Public user profiles and their post history
- Posting, commenting and voting from your own account

### Key-point callout

**Since 2023, commercial and high-volume access is a paid API** (announced at $0.24 per 1,000 calls) — the pricing change that shut down many third-party apps. The free tier allows 100 requests per minute per client with OAuth (10/min without), which is fine for small projects but not for bulk collection.

## Watch out for

- No bulk history download — deep comment threads come back truncated with "load more" stubs that cost extra calls
- Deleted and removed content is not retrievable; Pushshift, the old historical archive, had its access revoked by Reddit in May 2023 and returned as a moderator-only tool
- API terms prohibit selling the data or training ML models on it without an agreement

## What a listing looks like — `GET /r/datascience/hot`

Code block (pre, monospace, left border #1a5276):

```
{
  "kind": "Listing",
  "data": {
    "after": "t3_abc123",
    "children": [{
      "kind": "t3",
      "data": {
        "title": "What statistical test should I use...",
        "author": "stats_curious_42",
        "score": 287,
        "upvote_ratio": 0.94,
        "num_comments": 83
      }
    }]
  }
}
```

## Reddit Comment Tree Structure

### Visualization (canvas `treeChart`, 100% width × 400px CSS height)

Tree diagram of a Reddit comment thread, drawn top-down with rectangular nodes connected by lines, plus a dashed truncation node.

- **Title (bold 13px, `#1a5276`, top center):** "Comment Tree Depth & Branching".
- **Layout:** padTop 30, padLeft/padRight 20; level gap 70px; standard node 100×28px; nodes centered on x, text label centered inside (10px white text on colored fill; node border `rgba(0,0,0,0.1)`); connector lines `#aaa`, width 1.2.
- **Level 0 (root):** one node at horizontal center, y = padTop+10, 160×32px, fill `#1a5276`, label "Original Post (t3)".
- **Level 1:** 4 nodes evenly spaced across usable width, fill `#2980b9`, labels "Comment L1.1" … "Comment L1.4", each connected by a line from the root.
- **Level 2:** children per L1 node = [3, 1, 2, 0]; nodes 85×~25px (0.85× width, 0.9× height), fill `#3498db`, labels "Reply L2.<parent>.<child>" (e.g. "Reply L2.1.1"); spread around parent x with max 80px spacing, connected from parents.
- **Level 3:** 2 nodes 75×~24px, fill `#5dade2`, both labeled "Reply L3" — one offset −20px from the first L2 node, one offset +10px from the third L2 node.
- **Truncation node:** at horizontal center, y ≈ L3 + 52px, 150×28px, dashed gray border (`#999`, dash 5/3, width 1.5) with `#f8f9fa` fill and italic gray (`#666`) label "[more comments]". Dashed connector lines (`#bbb`, dash 4/3) from both L3 nodes down to it.
- **Annotation (red `#e74c3c`, italic 11px):** two lines "Requires additional" / "API call" to the right of the truncation node, with a red 1.5px arrow (filled triangular arrowhead) pointing at the truncation node's right edge.
- **Annotation (orange `#e67e22`, bold 10px):** "max 100 per listing" to the right of the root node.
- **Depth legend (bottom left, 10px `#666`):** label "Depth:" followed by four 12×12 color swatches with labels — `#1a5276` "Root", `#2980b9` "L1", `#3498db` "L2", `#5dade2` "L3", spaced 50px apart.

## Official API References

- [Reddit API Documentation](https://www.reddit.com/dev/api/) — full endpoint reference for the Data API
- [Reddit Data API Terms](https://www.redditinc.com/policies/data-api-terms) — commercial/data-use terms introduced with the 2023 API changes

## Regeneration instructions

- **Layout:** single detail page: h1, `.subtitle` paragraph, `.verified` badge span, one `.obj-table` (full-width, border-collapse, one `<tr>`): left `<td>` 45% with `.section-head` headings ("What you can get", "Watch out for"), bullet lists and a `.key-point` callout; right `<td>` 55% with a `.section-head`, a `<pre>` JSON payload and the canvas. Below the table: an `h2` "Official API References" with a two-link list. Links in HTML are external URLs as given.
- **Page CSS:** body system sans-serif (-apple-system stack), line-height 1.6, text `#2c3e50`, white background, padding 40px; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; `.verified` badge — background `#f0f8ff`, border `1px solid #2980b9`, color `#1a5276`, 2px 8px padding, 4px radius, 0.8em; table cells `1px solid #ddd` border, 16px padding, top-aligned; `.section-head` bold `#1a5276` 0.95em; li 0.93em; links `#1a5276`.
- **Pre style:** background `#f8f9fa`, left border `3px solid #1a5276`, ui-monospace 0.78em, 12px padding, 4px radius.
- **Key-point style:** background `#f8f9fa`, left border `3px solid #e74c3c`, padding 10px 14px, 0.93em.
- **Canvas:** `display: block; width: 100%; height: 400px; margin-top: 16px`; sized from `getBoundingClientRect()` and scaled by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates), redrawn on window resize.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`; blue depth ramp `#1a5276` / `#2980b9` / `#3498db` / `#5dade2`; grays `#666`/`#999`/`#aaa`/`#bbb`.
- No nav bar, no back/home links. In regenerated HTML, any card links use `.html` extensions.
