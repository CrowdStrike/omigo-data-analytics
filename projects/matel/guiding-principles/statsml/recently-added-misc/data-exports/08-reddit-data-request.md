# Reddit Data Request

**Page type:** detail page (single-row two-column obj-table: text left 45%, payload samples + canvas right 55%)
**HTML title tag:** Reddit Data Request

**Subtitle:** Full account data export including vote history, IP logs, and ad interactions

**Verified line:** Last verified: August 2026

## How to Request

- **Path:** Settings → Safety & Privacy → Data Request
- **Alternative:** reddithelp.com/GDPR (for EU/UK residents)
- **Delivery:** Up to 30 days (typically 3-7 days for smaller accounts)
- **Format:** ZIP archive with CSV files per category

## What's Included

- Posts and comments (full text, timestamps, subreddit)
- **Votes (up/down) — your full voting history on all content** (highlighted in orange `#e67e22`)
- Private messages and chat history
- IP address logs (with timestamps)
- Ad click history and ad interactions
- Community interactions (joins, leaves, mutes)
- Moderation actions taken (if moderator)

**Key-point callout:** **Most surprising inclusion:** Reddit exports your complete voting history — every upvote and downvote you have ever cast, with the target post/comment ID and timestamp. This is a detailed behavioral signal most users forget they are generating.

## What's Missing

**Missing callout (red-bordered):**

- **Content scoring signals** — how Reddit's algorithm ranked your posts
- **Spam/bot classification** — whether your account was ever flagged
- **Shadow-ban status** — no indication if content was silently suppressed

## Right column: payload samples

**Payload note (italic):** Vote record (from votes.csv export):

**Payload block (monospace, verbatim):**

```
{
  "direction": "up",
  "target_fullname": "t3_abc123",
  "target_type": "post",
  "target_subreddit": "datascience",
  "target_author": "researcher42",
  "timestamp": "2024-03-15T09:22:41Z"
}
```

**Payload note (italic):** Comment record (from comments.csv export):

**Payload block (monospace, verbatim):**

```
{
  "id": "t1_kx7m2p",
  "subreddit": "statistics",
  "body": "The sample size assumption here...",
  "score": 47,
  "created_utc": "2024-06-01T14:08:33Z",
  "parent_id": "t3_def456",
  "permalink": "/r/statistics/comments/def456/..."
}
```

### Visualization (canvas `voteChart`, 100% width × 380px CSS height)

Diverging monthly bar chart: upvotes above a zero axis, downvotes below it.

- **Data (12 months Jan–Dec):**
  - Upvotes: `[142, 167, 195, 123, 210, 178, 154, 231, 189, 204, 162, 198]`
  - Downvotes: `[31, 42, 28, 35, 47, 39, 22, 51, 44, 38, 29, 41]`
- **Layout:** padding left 50, right 20, top 30, bottom 40; zero axis positioned proportionally at `chartH × maxUp/(maxUp+maxDown)` from the top; bar group width = chartW/12, bar width 60% of group.
- **Bars:** upvote bars above axis in `#1a5276`; downvote bars below axis in `#e67e22`.
- **Grid:** light `#eee` horizontal lines — 4 divisions above the axis, 2 below; zero axis line `#2c3e50`, width 1.5.
- **X-axis labels:** month abbreviations Jan–Dec, centered under each bar, `#2c3e50` 11px.
- **Y-axis labels (right-aligned, `#666` 10px):** "210" (maxUp) at top, "105" (maxUp/2) at midpoint above axis, "0" at the axis, "-51" (−maxDown) at bottom.
- **Legend (top left):** blue `#1a5276` square + "Upvotes"; orange `#e67e22` square + "Downvotes"; label text `#2c3e50` 11px.
- **Caption (below canvas, 0.8em `#888`):** "Monthly vote distribution — upvotes (blue) vs downvotes (orange, below axis)"

## Regeneration instructions

- **Layout:** single `.obj-table` (full width, collapsed borders) with one `<tr>`: left `<td>` (45%) holds `.obj-title` headings, bullet lists, `.key-point` and `.missing` callouts; right `<td>` (55%, text-align center) holds two `.payload-note` + `.payload` blocks, the canvas, and its caption paragraph.
- **Page style:** body system sans-serif, white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; `.verified` 0.85em `#888`; `.obj-title` bold `#1a5276` 1.1em; table cell borders `1px solid #2980b9`, padding 16px; li 0.93em; the votes bullet's strong element carries inline `color: #e67e22`.
- **Callouts:** `.key-point` — background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px 14px. `.missing` — background `#fdf2f2`, left border `3px solid #e74c3c`.
- **Payload block:** `.payload` — background `#f8f9fa`, left border `3px solid #1a5276`, monospace (ui-monospace/Menlo) 0.78em, white-space pre, left-aligned; `.payload-note` 0.82em `#666` italic left-aligned.
- **Canvas:** styled `width: 100%; height: 380px`; sized from `getBoundingClientRect()` and scaled by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`. No nav bar, no back/home links; in regenerated HTML any card links use `.html` extensions.
