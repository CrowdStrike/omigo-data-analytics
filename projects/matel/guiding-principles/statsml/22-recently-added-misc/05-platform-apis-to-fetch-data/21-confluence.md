# Confluence

**Page type:** detail page (two-column obj-table layout: text left 45%, code sample + canvas right 55%, one row)
**HTML title tag:** Confluence API — Platform APIs

**Subtitle:** Read and update the pages, comments, and attachments in a Confluence wiki — including every past version of a page.

**Verified badge:** Last verified: August 2026

## Overview

### Left column

**What You Can Get**

- Pages and blog posts, with their full content
- Every prior version of a page — author, timestamp, and complete body
- Comments, labels, and attachments
- Search across the whole wiki with a query language
- Create and update pages from a program

**Key-point callout:** **Page content comes in several formats, and none is complete.** The editable format stores macros as placeholders without the content they display; the rendered format shows what readers see but cannot be written back. Extracting text and editing pages need different formats — and converting one into the other destroys content.

**Watch Out For**

- Editing a secret out of a page does not remove it — the old version stays fully readable, and deleted pages sit in a trash that can still be fetched
- Content your account cannot see is silently left out of results — an incomplete view looks exactly like a complete one
- Search lags behind reality — a page created seconds ago may not be findable yet
- Page-view analytics is available only on the Premium/Enterprise plans

### Right column

**Section head:** Version history remembers what the page forgot

Code block (`pre`, monospace):

```
GET /wiki/api/v2/pages/{id}/versions
{
  "results": [
    { "number": 27, "createdAt": "2026-08-18T16:09:44Z",
      "message": "Rotated the on-call escalation path" },
    { "number": 26, "createdAt": "2026-07-30T11:41:02Z" },
    { "number": 25, "createdAt": "2026-06-12T09:03:55Z",
      "message": "Added temporary DB password for migration" }
  ]
}

// Version 25's full body is still retrievable, including
// the credential that was removed in version 26.
```

**Section head:** What Each Body Format Preserves

### Visualization (canvas `formatChart`, responsive width × 380)

Dot-matrix comparison grid: 6 feature rows × 4 format columns, each cell a circle indicating whether that body format preserves the feature (filled = present, half-filled = lossy, hollow with red X = absent).

- **Title (top-left, bold 13px, #1a5276):** "No single body format is complete".
- **Legend line (italic 10px, #888, under title):** "Filled = present  ·  half = lossy  ·  hollow = absent".
- **Feature rows (right-aligned labels, 10.5px #2c3e50):** "Plain prose text", "Headings & lists", "Macro declaration", "Macro rendered output", "User / page links", "Writable back via PUT".
- **Format columns (bold 10.5px headers in the column's color, centered above each column):**
  - "storage (XHTML)" — #1a5276, values [2, 2, 2, 0, 2, 2]
  - "atlas_doc_format" — #8e44ad, values [2, 2, 2, 0, 2, 2]
  - "view (rendered)" — #e67e22, values [2, 2, 0, 2, 1, 0]
  - "export_view" — #27ae60, values [2, 2, 0, 2, 1, 0]
  - (value coding: 2 = fully preserved, 1 = partial/lossy, 0 = absent)
- **Cell marks:** circle radius min(11, rowHeight×0.3), stroke width 1.8 in the format color (gray #ccc when absent). Value 2: circle filled with the format color at ~55% alpha (blue uses rgba(26,82,118,0.35) underlay). Value 1: right half-disc filled at 55% alpha. Value 0: red (#e74c3c) X drawn inside the hollow circle, line width 1.4.
- **Layout:** left padding min(210, 30% of width) for row labels, top padding 66, bottom padding 60; alternating row background #fafafa on even rows; plot area framed with #e5e5e5 1px border.
- **Footer captions (italic 10px, left-aligned at bottom):** red #e74c3c: "Text extraction needs view; editing needs storage. Doing both means two fetches per page." then gray #888: "Converting view back into storage is not supported and destroys every macro on the page."

## Official API References

- [Confluence Cloud developer documentation](https://developer.atlassian.com/cloud/confluence/) — hub for REST APIs, auth, and app development
- [Confluence REST API v2](https://developer.atlassian.com/cloud/confluence/rest/v2/intro/) — the current pages, spaces, and versions surface

## Regeneration instructions

- **Layout:** single-page detail doc: h1, `.subtitle` paragraph, `.verified` badge span, `h2` "Overview" section containing one `.obj-table` (full width, one `<tr>`: left `<td>` 45% with `.section-head` headings + bullet lists + one `.key-point` callout; right `<td>` 55% with `.section-head` headings, a `pre` code block, and the canvas), then `h2` "Official API References" with a link list. No nav bar, no back/home links.
- **Page CSS:** body system sans-serif (-apple-system stack), line-height 1.6, text #2c3e50, padding 30px 40px, white background; global `* { margin:0; padding:0; box-sizing:border-box }` reset. h1 1.8rem #1a5276; `.subtitle` #666 1.05em; `.verified` badge — background #eaf2f8, border 1px solid #2980b9, color #1a5276, padding 2px 10px, radius 4px, 0.8em; h2 1.3em #1a5276 with 2px solid #2980b9 bottom border; `.section-head` bold #1a5276 0.95em; `pre` background #f4f4f4, padding 14px, radius 6px, 0.8em, ui-monospace/Menlo; `.key-point` background #f8f9fa, left border 3px solid #e74c3c, padding 10px 14px, 0.93em; li 0.93em; links #1a5276.
- **Canvas:** `<canvas id="formatChart" height="380">`, CSS `width: 100%`; on resize, backing store scaled by `window.devicePixelRatio` (width = rect.width × dpr, height = 380 × dpr, `ctx.setTransform(dpr,0,0,dpr,0,0)`), redraw on window resize.
- **Palette:** #1a5276 primary blue, #27ae60 green, #e74c3c red, #e67e22 orange, #8e44ad purple, rgba(26,82,118,0.35) bar fill, grays #888/#ccc/#e5e5e5/#fafafa.
