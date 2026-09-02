# SharePoint Lists via Microsoft Graph

**Page type:** detail page (two-column obj-table layout: text left 45%, code sample + canvas right 55%, one row)
**HTML title tag:** SharePoint Lists (Microsoft Graph) — Platform APIs

**Subtitle:** Read and write SharePoint lists — the tables behind many internal trackers — including each row's edit history.

**Verified badge:** Last verified: August 2026

## Overview

### Left column

**What You Can Get**

- A list's schema — its columns, types, and choice values
- Rows with all their field values
- Every prior version of a row — who changed which field, and when
- An incremental change feed for keeping a copy in sync
- Add and update rows from a program

**Key-point callout:** **Big lists hit a hidden ceiling.** Filtering or sorting on a column without an index starts failing once a list grows past roughly 5,000 items — the limit is on how many rows the engine may scan, not on how many you get back. The fix is adding an index to that column; retrying will never work.

**Watch Out For**

- Columns are addressed by a frozen internal name, not the label you see — a column created as "Due Date" is keyed `Due_x0020_Date` forever, even after renaming
- Overwritten field values stay readable in version history for anyone who can read the item
- Classic item attachments and per-row permissions are simply not exposed here
- Rows a user cannot see are silently omitted — indistinguishable from not existing

### Right column

**Section head:** A cleared field, still readable in version history

Code block (`pre`, monospace):

```
GET .../items/2/versions
{
  "value": [
    { "id": "4.0",
      "fields": { "Status": "In progress",
                  "ApprovalNotes": null } },
    { "id": "3.0",
      "fields": { "Status": "New",
                  "ApprovalNotes": "Temp console cred in ticket OPS-8812" } }
  ]
}

// ApprovalNotes was cleared in version 4. Version 3 still
// returns the original text to anyone who can read the item.
```

**Section head:** List View Threshold — Indexed vs Unindexed Filter

### Visualization (canvas `thresholdChart`, responsive width × 380)

Line chart: percentage of `$filter` queries succeeding vs list size, showing an unindexed-column cliff at the 5,000-item view threshold.

- **Title (bold 13px, #1a5276, top-left):** "$filter behaviour as a list grows past the view threshold".
- **Subtitle (italic 10.5px, #888):** "The cap applies to rows the engine must scan, not to rows returned."
- **X axis:** list sizes sampled at `[500, 1000, 2000, 3000, 4000, 5000, 6000, 8000, 12000, 20000, 50000]`, plotted at equal spacing; labels shown for every other point plus 5,000, formatted "1k"/"2k"/… for ≥1000; axis title "items in the list" (#666, 10px, centered below).
- **Y axis:** 0–100% success, gridlines at 0/25/50/75/100% (#eee, baseline #999), right-aligned "%" labels in #888. Padding: left 58, right 20, top 62, bottom 74.
- **Threshold marker:** vertical dashed red (#e74c3c, dash 5/4, width 1.5) line at 5,000 items; region to the right shaded rgba(231,76,60,0.06); bold 10px red label at the top: "list view threshold (5,000 items)".
- **Series 1 — indexed:** flat 100% across all sizes; solid green (#27ae60) line, width 2.4, 3px dots.
- **Series 2 — unindexed:** 100% up to and including 5,000, then 0% at all larger sizes (step cliff); dashed blue (#1a5276, dash 6/3) line, width 2.4, 3px dots; area under this curve filled rgba(26,82,118,0.35).
- **Cliff annotation:** orange (#e67e22) pointer line from just right of the threshold, with two lines of bold 10px orange text: "HTTP 400 / throttled —" / "adding an index is the fix, not retrying".
- **Legend (bottom row, 10px):** green solid line sample + "$filter on an indexed column" (#2c3e50); blue dashed line sample + "$filter on an unindexed column" (#2c3e50); italic gray (#888) note at right: "Threshold is a documented service limit and may change."

## Official API References

- [SharePoint sites and lists in Microsoft Graph](https://learn.microsoft.com/en-us/graph/api/resources/sharepoint?view=graph-rest-1.0) — site, list, listItem, and column resources
- [listItem resource type](https://learn.microsoft.com/en-us/graph/api/resources/listitem?view=graph-rest-1.0) — the fields bag, versions, and delta support

## Regeneration instructions

- **Layout:** single-page detail doc: h1, `.subtitle` paragraph, `.verified` badge span, `h2` "Overview" with one `.obj-table` (one `<tr>`: left `<td>` 45% with `.section-head` headings + bullet lists + one `.key-point` callout; right `<td>` 55% with `.section-head` headings, a `pre` code block, and the canvas), then `h2` "Official API References" with a link list. No nav bar, no back/home links.
- **Page CSS:** body system sans-serif (-apple-system stack), line-height 1.6, text #2c3e50, padding 30px 40px, white background; global `* { margin:0; padding:0; box-sizing:border-box }` reset. h1 1.8rem #1a5276; `.subtitle` #666 1.05em; `.verified` badge — background #eaf2f8, border 1px solid #2980b9, color #1a5276, padding 2px 10px, radius 4px, 0.8em; h2 1.3em #1a5276 with 2px solid #2980b9 bottom border; `.section-head` bold #1a5276 0.95em; `pre` background #f4f4f4, padding 14px, radius 6px, 0.8em, ui-monospace/Menlo; `code` ui-monospace 0.92em; `.key-point` background #f8f9fa, left border 3px solid #e74c3c, padding 10px 14px, 0.93em; li 0.93em; links #1a5276.
- **Canvas:** `<canvas id="thresholdChart" height="380">`, CSS `width: 100%`; backing store scaled by `window.devicePixelRatio` (width = rect.width × dpr, height = 380 × dpr, `ctx.setTransform(dpr,0,0,dpr,0,0)`), redraw on window resize.
- **Palette:** #1a5276 primary blue, #27ae60 green, #e74c3c red, #e67e22 orange, rgba(26,82,118,0.35) area fill, grays #888/#999/#eee.
