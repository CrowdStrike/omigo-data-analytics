# Jira Cloud REST API

**Page type:** detail page (two-column obj-table layout with bordered cells: text left 45%, payload + canvas right 55%, one row)
**HTML title tag:** Jira Cloud REST API — Platform APIs

**Subtitle:** Pull issues out of Jira — along with the full history of every status change, the time people logged, and sprint dates.

**Verified badge:** Last verified: August 2026

## Overview

### Left column

**What You Can Get**

- Issues with all their fields, comments, and attachments
- The changelog — every status, assignee, and field change, with who did it and when
- Work logs — self-reported time spent on each issue
- Boards and sprints, with start, end, and completion dates
- Search across issues with a query language

**Key-point callout:** **The issue only shows where things stand now.** How long work sat in each state — cycle time, waiting time, rework loops — exists nowhere on the issue itself. It has to be reconstructed by replaying the change history, entry by entry.

**Watch Out For**

- The change history embedded in search results is silently truncated — fetch it separately whenever completeness matters
- Issues the account cannot see are silently missing — a permissions change looks exactly like a drop in workload
- Deleted issues vanish without a trace, so a historical extract can shrink between runs
- Webhooks expire and can drop events — never make one your only data source

### Right column

**Payload note (italic, #666):** A changelog entry — one status change, with who and when. Note total: 9 against maxResults: 2 — the rest must be paged.

Payload block (`pre.payload`, monospace, blue left border):

```
{
  "total": 9,
  "maxResults": 2,
  "values": [
    {
      "author": { "displayName": "Amit Jaiswal" },
      "created": "2026-08-14T09:12:41+0530",
      "items": [
        { "field": "status",
          "fromString": "To Do",
          "toString": "In Progress" }
      ]
    }
  ]
}
```

### Visualization (canvas `c1`, responsive width × 380)

Two-band timeline comparing the same issue lifecycle as reconstructed from the changelog (top band, colored status segments) vs the snapshot fields on the issue document (bottom band, three dots on a line).

- **Title (bold 14px, #1a5276, centered):** "One issue, two views of the same lifecycle".
- **Subtitle (italic 11px, #666, centered):** "illustrative shape — days since creation".
- **X scale:** days 0 to 10.4, mapped over full width with 24px side margins.
- **Band 1 (y=78, height 40) — label (bold 11px, #1a5276, left):** "Replayed from changelog  (field = \"status\")". Contiguous status segments, filled at 85% alpha with 1.5px white separators:
  - "To Do" 0–2.1d — #e67e22
  - "In Progress" 2.1–3.4d — #1a5276
  - "Blocked" 3.4–7.2d — #e74c3c
  - "In Progress" 7.2–8.6d — #1a5276
  - "In Review" 8.6–9.5d — #e67e22
  - "Done" 9.5–10.4d — #27ae60
  - Wide segments (>58px) get a bold white label centered inside plus a duration label below (e.g. "3.8d", 9px #2c3e50); narrow segments get a colored label above with a connector tick line.
- **Rework arrow:** dashed red (#e74c3c, dash 4/3, width 1.5) horizontal line under band 1 from day 7.2 back to day 3.4, with italic 10px red caption: "backwards transition — visible only in history".
- **Band 2 (y=236) — label (bold 11px, #1a5276, left):** "From the issue document alone  (fields on GET /issue)". A thin #ccc horizontal line with three 5px-radius #1a5276 dots and 10px #2c3e50 labels:
  - day 0: "created"
  - day 9.5: "resolutiondate"
  - day 10.4: "updated"
- **Gap bar:** full-span 18px bar filled rgba(26,82,118,0.35) below the markers, with italic 10px #1a5276 centered text: "everything between the markers is unrecoverable from the snapshot".
- **Caption (italic 11px, #666, centered at bottom):** "Same issue. Time-in-status, waiting time and rework exist only in the upper band."

## Official API References

- [Jira Cloud platform REST API (v3)](https://developer.atlassian.com/cloud/jira/platform/rest/v3/intro/) — issues, changelogs, worklogs, fields, and search
- [Jira Software (Agile) REST API](https://developer.atlassian.com/cloud/jira/software/rest/intro/) — boards, sprints, backlog, epics, and rank

## Regeneration instructions

- **Layout:** single-page detail doc: h1, `.subtitle` paragraph, `.verified` badge span, `h2` "Overview" with one `.obj-table` (one `<tr>`: left `<td>` 45% with `.obj-title` headings + bullet lists + one `.key-point` callout; right `<td>` 55%, text-align center, with a `.payload-note`, a `pre.payload` block, and the canvas), then `h2` "Official API References" with a link list. No nav bar, no back/home links.
- **Page CSS:** body system sans-serif (-apple-system stack), line-height 1.6, text #2c3e50, padding 30px 40px, white background. h1 1.8rem #1a5276; `.subtitle` #666 1.05em; `.verified` badge — plain variant: color #888, border 1px solid #e0e0e0, padding 2px 10px, radius 4px, 0.8em; h2 1.3em #1a5276 with 2px solid #2980b9 bottom border; `.obj-title` bold #1a5276 1.1em; `.obj-table td` border 1px solid #e0e0e0, padding 16px, vertical-align top; `.payload` background #f8f9fa, left border 3px solid #1a5276, ui-monospace 0.78em, left-aligned; `.payload-note` 0.82em #666 italic left-aligned; `.key-point` background #f8f9fa, left border 3px solid #1a5276, padding 10px 14px, 0.93em; li 0.93em; links #1a5276.
- **Canvas:** `<canvas id="c1" height="380">`, CSS `width: 100%`; draw() sizes backing store by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates), redraw on window resize.
- **Palette:** #1a5276 primary blue, #27ae60 green, #e74c3c red, #e67e22 orange, rgba(26,82,118,0.35) bar fill, grays #666/#ccc.
