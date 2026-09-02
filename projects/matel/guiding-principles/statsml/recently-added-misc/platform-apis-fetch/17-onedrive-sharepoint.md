# OneDrive / SharePoint

**Page type:** detail page (two-column obj-table layout: text left 45%, code sample + canvas right 55%, single row under an Overview h2)
**HTML title tag:** OneDrive / SharePoint API — Platform APIs

**Subtitle:** Read the files and folders stored in OneDrive and SharePoint — plus who changed them, who can see them, and how often they are viewed.

**Verified badge:** Last verified: August 2026

## Overview

## What You Can Get

- Files and folders, with their contents and metadata
- Version history — who changed a file, and when
- Aggregate usage stats — how often a file was viewed, and by how many people
- Sharing links and permissions on each file
- A change feed for keeping an external copy in sync

**Key-point callout:** **The service decides how fast you may go.** There is no fixed request quota — Microsoft slows callers down dynamically based on load. A client that identifies itself and backs off when asked gets far more total work done than one that hammers away and keeps getting blocked.

## Watch Out For

- Deleted and edited content lives on — old versions and the recycle bin keep it readable long after it disappears from view
- Usage stats are aggregate counts only — you cannot see which person viewed a file at what time
- Sync tokens expire — every sync design needs a "start over from scratch" path
- Change notifications say *something* changed, not what — you still have to go look

## The service saying "slow down"

Code block (`pre`, HTTP response):

```
HTTP/1.1 429 Too Many Requests
Retry-After: 47
RateLimit-Remaining: 0

{
  "error": {
    "code": "activityLimitReached",
    "message": "The request has been throttled"
  }
}
```

## Throttling Behaviour — Decorated vs Undecorated Client

### Visualization (canvas `throttleChart`, width 100% responsive × 380)

Multi-line time-series chart (60 time steps) contrasting effective throughput of a well-behaved vs naive client under dynamic throttling.

- **Title (bold 13px, `#1a5276`, at 12,10):** "Effective throughput under dynamic throttling"
- **Subtitle (italic 10px, `#888`, at 12,28):** "Illustrative shape of two documented behaviours — axes are relative, not a published quota"
- **Data (generated, N=60 steps):**
  - Naive (undecorated) series: value 100 when not blocked; drops to 0 for a block of `3 + floor(t/15)` steps triggered whenever `t > 4 && t % 7 === 0`. Burst-to-100 then stall-at-0 sawtooth with lengthening stalls.
  - Decorated (good) series: `62 + 8*sin(t/5)` — a gentle sine around 62%.
  - Service ceiling (dashed orange `#e67e22`, dash 5/4, 1.5px): `78 + 10*sin(i/7 + 1)`.
- **Axes:** y 0–110 scale with gridlines every 25% labeled "0%"…"100%" (10px `#888`, gridlines `#eee`, baseline `#999`); x label "time →" centered below plot. Padding: left 52, right 22, top 50, bottom 96.
- **Series rendering:** naive line `#e74c3c` 2px with fill `rgba(231,76,60,0.14)`; good line `#1a5276` 2px with fill `rgba(26,82,118,0.35)`.
- **Mean lines:** dashed (dash 2/3) horizontal lines at each series mean — good mean in green `#27ae60` 2px labeled (bold 10px, right-aligned, above line) "self-throttled mean — higher than the burst-and-stall mean"; naive mean in red `#e74c3c` 1.4px labeled (below line) "burst-and-stall mean".
- **Stall markers:** bold 9px red "429" labels centered above the baseline at each step where the naive series first drops to 0.
- **Legend (bottom left, 11px, 11×11 swatches, four stacked rows 15px apart):**
  - `#1a5276` — Decorated User-Agent + RateLimit-Remaining aware
  - `#e74c3c` — Undecorated client, fixed rate, ignores Retry-After
  - `#e67e22` — Service ceiling (moves with tenant load)
  - `#27ae60` — Mean sustained throughput of the well-behaved client

## Official API References

- [OneDrive developer documentation](https://learn.microsoft.com/en-us/onedrive/developer/rest-api/) — file storage concepts, delta sync, sharing
- [Microsoft Graph throttling guidance](https://learn.microsoft.com/en-us/graph/throttling) — 429 handling and Retry-After

## Regeneration instructions

- **Layout:** platform-apis-fetch detail page. h1, `.subtitle` paragraph, `.verified` badge span, h2 "Overview", then one `.obj-table` (full width) with a single `<tr>`: left `<td>` (45%) holds `.section-head` headings ("What You Can Get", "Watch Out For") + bullets + one `.key-point` callout; right `<td>` (55%) holds a `.section-head` + `<pre>` HTTP sample, another `.section-head` + the canvas. After the table, h2 "Official API References" with a link list.
- **Page CSS:** body system sans-serif (-apple-system stack), line-height 1.6, text `#2c3e50`, padding 30px 40px, white background; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; `.verified` badge — background `#eaf2f8`, border `1px solid #2980b9`, color `#1a5276`, padding 2px 10px, radius 4px, 0.8em; h2 1.3em `#1a5276` with 2px `#2980b9` bottom border; `.section-head` bold 0.95em `#1a5276`; `.key-point` — background `#f8f9fa`, left border `3px solid #e74c3c`, padding 10px 14px, 0.93em; `pre` — background `#f4f4f4`, padding 14px, radius 6px, 0.8em, ui-monospace; `li`/`p` 0.93em; links `#1a5276`. No nav bar, no back/home links.
- **Canvas:** `<canvas id="throttleChart" height="380">`, CSS `display:block; width:100%`; JS resizes on window resize, sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`), fixes CSS height to 380px, and applies `ctx.setTransform(dpr,0,0,dpr,0,0)` before drawing.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, bar/area fill `rgba(26,82,118,0.35)`, gray text `#888`/`#666`/`#2c3e50`.
- In regenerated HTML, any card/page links use `.html` extensions.
