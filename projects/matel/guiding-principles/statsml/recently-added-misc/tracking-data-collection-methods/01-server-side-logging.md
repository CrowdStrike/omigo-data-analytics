# Tracking Data: Server-Side Logging

**Page type:** detail page (two-column obj-table layout: text left 45%, canvas right 55%, one row per section, three rows)
**HTML title tag:** Tracking Data: Server-Side Logging

**Subtitle:** Every request a web server answers is written to a log — source address, path, timestamp, and client details. This is a byproduct of how HTTP works.

## Section 1: What is it?

A byproduct of answering a request, not an added instrument. *(lede)*

- **Mechanism:** the server must receive the request and know where to reply
- **Default on:** every common web server writes that exchange to a file
- **Nothing added:** no tag, no script, no change to the page

**Key point callout:** **Not blockable from the client:** ad blockers and private browsing act in the browser. This record is written at the other end, after the request has arrived.

### Visualization (canvas `c1`, 720×320)

Data-flow diagram: User -> Server -> Log File, one palette hue per actor (visitor `#2a78d6` blue, server `#4a3aa7` violet, record `#199e70` aqua).

- **Visitor icon:** filled blue circle (radius 22) at (80, cy−20) where cy = h/2, white bold 18px text "You" inside; blue 15px "(visitor)" below at (80, cy+15).
- **Arrow 1 (blue, 2px, triangular head):** from (115, cy−10) to (210, cy−10), labeled "request page" in 14px blue above at (160, cy−20).
- **Server box:** rect at (220, cy−45) size 140×70, fill = violet tinted to alpha 0.30 (rgba from hex), violet stroke; bold 16px violet "Web Server" centered at (290, cy−10) and 14px "(writes one line per request)" at (290, cy+8).
- **Arrow 2 (violet, 2px, triangular head):** from (360, cy−10) to (450, cy−10), labeled "writes log" in 14px violet at (400, cy−20).
- **Log file:** rounded rect (radius 6) at (460, cy−50) size 180×80 filled solid aqua; white bold 17px "Log File" at (550, cy−28); white 14px lines "203.0.113.42 - GET /shoes" (550, cy−10), "2026-08-22 14:23:07" (550, cy+5), "Chrome/Mac - from google.com" (550, cy+20).
- **Bottom note (15px muted gray `#6b7280`, centered):** "Created on the server side, so no client-side setting changes it."

## Section 2: What does it collect?

- **Source IP address**, which resolves to a rough location — usually city level at best, often just the ISP's region
- **Timestamp**, typically to the second or finer
- **Path requested** and the response code
- **User agent string** — browser, version, operating system
- **Referrer** — the page that linked here

**Key point callout:** **Derived fields are reconstructions:** the log holds no city and no session. Each is added downstream, and each can be wrong.

**Key point callout:** **Unit-of-observation error:** sessionising by IP merges everyone behind one office router into a single visitor.

### Visualization (canvas `c2`, 720×320)

Horizontal schematic bar chart: which standard log fields are always present vs client-supplied. Two hues only (a genuine binary distinction): blue `#2a78d6` = written by the server itself; violet `#4a3aa7` = supplied by the client.

- **Title (bold 16px `#1a5276`, centered):** "Standard log fields — always present vs. client-supplied"
- **Data** (label, fractional bar length of 260px max, note text):
  | Label | frac | Note |
  |---|---|---|
  | IP Address | 1.0 | always present |
  | Timestamp | 1.0 | always present |
  | Path + status | 1.0 | always present |
  | User agent | 0.8 | if the client sends one |
  | Referrer | 0.6 | if the client sends one |
- **Bars:** height 28, gap 12, starting y 45; labels right-aligned at x=120 in 16px `#2c3e50`; bar fill = hue tinted to alpha 0.32 with 1px solid hue stroke (blue when frac = 1, violet otherwise); note text in the hue color, 15px, left of bar end + 22px.
- **Caption (14px `#6b7280`, bottom center):** "Bar lengths are schematic. A missing referrer is not a missing visit."

**Payload note (italic, `#666`):** Sample payload — illustrative structure, not real captured data.

**Payload block** (monospace `.payload`, below the canvas in the right column):

```
// Combined Log Format — one line per request.
// All fields below are part of the standard and
// are written by default.
203.0.113.42 - - [22/Aug/2026:14:23:07 +0000]
  "GET /shoes/running?size=10 HTTP/2.0" 200 18342
  "https://www.google.com/"
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)
   AppleWebKit/537.36 Chrome/128.0 Safari/537.36"

// As parsed into a warehouse table:
{
  // ── present in the raw log ──
  "client_ip":    "203.0.113.42",
  "ts":           "2026-08-22T14:23:07Z",
  "method":       "GET",
  "path":         "/shoes/running",
  "query":        { "size": "10" },
  "status":       200,
  "bytes":        18342,
  "referrer":     "https://www.google.com/",
  "user_agent":   "Mozilla/5.0 … Chrome/128.0 …",

  // ── inferred / plausible, added downstream ──
  "geo_city":     "Austin",     // IP lookup
  "geo_country":  "US",
  "device_type":  "desktop",    // UA parsing
  "bot_score":    0.02,
  "session_id":   "a41f…"       // IP + UA + time window
}
```

## Section 3: Why is it collected?

**Label pill (Stated purpose, blue `.lbl-purpose`):**

- **Operations** — diagnosing errors, measuring latency, capacity planning
- **Abuse detection** — logs are genuinely required for this

**Label pill (Additional consequence, orange `.lbl-effect`):**

- The same records support **traffic analysis**, and can be **joined to other datasets**
- Sharing or segmentation is a **policy decision**, not a technical necessity

**Key point callout:** **Retention decides the numbers:** the window was chosen for how long an incident stays interesting. But a repeat visit is only visible while the earlier line is still on disk, so the same traffic reports a different split of new and returning depending on a choice nobody made for that purpose.

### Visualization (canvas `c3`, 720×320)

Bar chart: how many of 200 repeat visits are recognised as repeat, as a function of log retention window. Hardcoded illustrative counts.

- **Title (bold 13px `#1a5276`, centered):** "Repeat visits recognised as repeat, by how long the logs are kept"
- **Subtitle (12px `#6b7280`, centered):** "200 return visits; a repeat is only visible while the earlier line still exists"
- **Data:** return-visit gap distribution (cumulative source): same day 34, 2-7 days 46, 8-30 days 52, 1-3 months 38, over 3 mths 30; TOTAL = 200. Retention windows and how many gap buckets each keeps: 24 hours keeps 1 bucket (seen 34), 7 days keeps 2 (80), 30 days keeps 3 (132), 90 days keeps 4 (170), 2 years keeps 5 (200). Seen value = sum of first `keeps` bucket counts.
- **Geometry/scale:** baseline y 226, max bar height 148 mapped to 200 visits; first bar center x 130, step 118, bar width 66; baseline line `#e5e9ef`.
- **Reference line:** dashed (6/4) violet `#4a3aa7` 1.5px horizontal at the 200 level, labeled bold 12px violet "all 200 return visits" at left.
- **Bars (one per window):** faint gray block `rgba(107,114,128,0.14)` from the 200-line down to the bar top (the unseen part); recognised portion fill `rgba(42,120,214,0.30)` with `#2a78d6` 1px stroke. Above each bar: bold 12px blue seen-count; at the 200-line: bold 12px orange `#d95926` "(200−seen) counted new". Window label below baseline in 12px `#2c3e50`.
- **X-axis note (12px `#6b7280`, centered):** "how long lines are kept — a choice made for incident response"
- **Captions (bottom center):** italic 12px `#2c3e50` "The same traffic reports a different mix of new and returning at each window."; italic 11px `#6b7280` "Illustrative counts — the mechanism, not measured traffic."

## Regeneration instructions

- **Layout:** tracking-methods detail page. h1 + `.subtitle`, then a three-row `.obj-table` (left td 45%: `.obj-title` heading + `.lede`/bullets/`.lbl` pills/`.key-point` callouts; right td 55%, centered: canvas — row 2 also holds `.payload-note` + `.payload` pre below its canvas).
- **Page CSS:** body system sans-serif, `#2c3e50` text, white background, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; obj-table cells `1px solid #2980b9`, padding 16px; `.obj-title` bold 1.1em `#1a5276`; `.key-point` — background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px 14px, 0.93em, leading `strong` in `#1a5276`; `.lede` 0.95em; `.lbl` pills — 0.7em bold uppercase, letter-spacing 0.05em, padding 2px 7px, radius 3px; `.lbl-purpose` background `#eaf2fb` color `#1a5276`; `.lbl-effect` background `#fdf0e6` color `#a8501c`; `li b` in `#1a5276` weight 600; `.payload` — background `#f8f9fa`, left border `3px solid #1a5276`, monospace 0.78em, padding 10px, line-height 1.45; `.payload-note` 0.82em italic `#666`. No nav bar, no back/home links.
- **Canvases:** three fixed 720×320 canvases (`c1`, `c2`, `c3`); a shared `setupCanvas(id)` helper reads each element's own width/height attributes, sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates.
- **Chart palette (CVD-validated tokens, declared once — red deliberately excluded from the series rotation, reserved for genuine error/alarm states):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`; ink `#1a5276` (headings/axes only), text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Helpers: `tint(hex, alpha)` for translucent fills and a rounded-rect path function. Project-wide accents remain `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- All chart numbers are hardcoded literal arrays (no `Math.random()`); invented numbers are labeled "illustrative"/"schematic" on-canvas.
- In regenerated HTML, any card/page links use `.html` extensions.
