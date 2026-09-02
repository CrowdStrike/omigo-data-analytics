# Notion API

**Page type:** detail page (two-column obj-table layout: text left 45%, code sample + canvas right 55%, one row)
**HTML title tag:** Notion API — Platform APIs

**Subtitle:** Read and write the pages and databases in a Notion workspace from a program.

**Verified badge:** Last verified: August 2026

## Overview

### Left column

**What You Can Get**

- Page content as a nested tree of typed blocks — paragraphs, headings, lists, tables
- Database rows and their properties
- Create and update pages, rows, and comments
- Webhook notifications when shared content changes
- Search across everything shared with the integration

**Key-point callout:** **An integration only sees what is shared with it.** There is no global workspace access — every page or database must be explicitly shared with the integration by a user. A connector that "works" may still be reading a fraction of the workspace without any error telling you so.

**Watch Out For**

- Reading a deep page takes many calls — children come back one level at a time, 100 blocks per request
- The rate limit is low (roughly 3 requests per second), so large exports are slow by design
- Newer API versions split a database into one or more "data sources" — code that assumes one table per database breaks

### Right column

**Heading (bold):** Response — GET /v1/blocks/{block_id}/children

Code block (`pre`):

```
{
  "object": "list",
  "results": [
    { "object": "block", "type": "heading_2",
      "heading_2": { "rich_text": [
        { "text": { "content": "Project Overview" } } ] } },
    { "object": "block", "type": "paragraph",
      "paragraph": { "rich_text": [
        { "text": { "content": "This document outlines..." } } ] },
      "has_children": false },
    { "object": "block", "type": "bulleted_list_item",
      "has_children": true }
  ],
  "has_more": false
}
```

### Visualization (canvas `blockTreeCanvas`, responsive width × 400)

Node-link tree diagram of a Notion page's block hierarchy, three depth levels, rounded-rectangle nodes connected by gray edges.

- **Title (bold 13px, #1a5276, centered at y=15):** "Block Tree Structure — Page Hierarchy".
- **Nodes** (rounded rects, radius 8, height 32, width = max(text+20, 70); fill = node color + "22" alpha suffix, stroke = node color at width 2; 12px label centered in node color):
  - Depth 0 (y=40): "Page" at 50% width — #1a5276
  - Depth 1 (y=140): "heading_2" at 10% — #1a5276; "paragraph" at 30% — #27ae60; "bulleted_list" at 50% — #e67e22; "toggle" at 70% — #e67e22; "code" at 90% — #e74c3c
  - Depth 2 (y=260): under bulleted_list — "item_1" at 38%, "item_2" at 50%, "item_3" at 62% (all #e67e22); under toggle — "paragraph" at 65% (#27ae60), "image" at 78% (#27ae60)
- **Edges:** straight #bbb lines, width 1.5, from Page to each depth-1 node; from bulleted_list to item_1/2/3; from toggle to its paragraph and image.
- **Depth labels (11px, #999, left edge):** "Depth 0" at y=40, "Depth 1" at y=140, "Depth 2" at y=260.
- **Annotations (italic 10px, #e67e22, centered at y=175):** "has_children: true" under bulleted_list (50% width) and under toggle (70% width).
- **Legend (bottom-left, 12px color swatches + 11px #2c3e50 labels, 120px apart):** "Heading" #1a5276, "Paragraph" #27ae60, "List / Toggle" #e67e22, "Code" #e74c3c.

## Official API References

- [Notion developer documentation](https://developers.notion.com/) — hub for guides, reference, and integration setup
- [Notion API reference](https://developers.notion.com/reference/intro) — endpoints, objects, versioning, and errors

## Regeneration instructions

- **Layout:** single-page detail doc: h1, `.subtitle` paragraph, `.verified` badge span, `h2` "Overview" with one `.obj-table` (one `<tr>`: left `<td>` 45% with `<strong>` headings + bullet lists + one `.key-point` callout; right `<td>` 55% with a `<strong>` heading, a `pre` code block, and the canvas), then `h2` "Official API References" with a link list. No nav bar, no back/home links.
- **Page CSS:** body system sans-serif (-apple-system stack), line-height 1.6, text #2c3e50, padding 30px 40px, white background. h1 1.8rem #1a5276; `.subtitle` #666 1.05em; `.verified` badge — background #eaf2f8, border 1px solid #2980b9, color #1a5276, padding 2px 10px, radius 4px, 0.8em; h2 1.3em #1a5276 with 2px solid #2980b9 bottom border; `pre` background #f4f4f4, padding 14px, radius 6px, 0.82em; `.key-point` background #f8f9fa, left border 3px solid #e74c3c, padding 10px 14px, 0.93em; li 0.93em; links #1a5276.
- **Canvas:** `<canvas id="blockTreeCanvas" height="400">`, CSS `width: 100%`; backing store scaled by `window.devicePixelRatio` (width = rect.width × dpr, height = 400 × dpr, `ctx.setTransform(dpr,0,0,dpr,0,0)`), redraw on window resize; node x positions are fractions of the current canvas width.
- **Palette:** #1a5276 primary blue, #27ae60 green, #e74c3c red, #e67e22 orange, grays #bbb/#999/#666.
