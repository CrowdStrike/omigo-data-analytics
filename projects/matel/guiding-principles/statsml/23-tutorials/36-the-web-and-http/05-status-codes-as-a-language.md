# Status Codes as a Language

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Status Codes as a Language

**Subtitle:** Every HTTP response opens with a three-digit code — the first digit says whose fault it was, and about a dozen codes cover almost everything a server will ever say

## One Tuesday in a Coffee Shop's Ordering API

**Tags:** `core idea` (blue), `HTTP` (green), `running example` (orange)

- **The shop** — a coffee shop's online ordering API answers 10,000 requests on one Tuesday
- **Every reply** — each response begins with a three-digit code, sent before any data arrives
- **The tally** — 9,210 replies start with 2, 310 start with 3, 430 start with 4, 50 start with 5
- **First digit** — 2 means "done", 3 means "look elsewhere", 4 means "your mistake", 5 means "ours"
- **The definition** — a status code is the reply's one-word verdict, standardized so any client gets it

*Example (italic):* A customer's latte order gets 201, a typo in the menu URL gets 404, and the shop's crashed pricing service gets 500 — no body needs reading yet.

**Key point:** Status codes are a shared vocabulary: before parsing a single byte of the body, the three digits already tell both sides who succeeded, who failed, and whose fault it was.

### Visualization (canvas `c1`, 720×300)

Vertical bar chart of the Tuesday's 10,000 responses grouped by first digit — four bars, one per class.

- **Title (bold 15px, `#1a5276`, top center):** "One Tuesday, 10,000 Responses: Four First Digits".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; y = responses 0 to 10,000, gridlines `#e5e9ef` at 2,500/5,000/7,500 with 12px `#6b7280` tick labels.
- **Bars (width 90, centered at x = 135, 285, 435, 585), heights scaled to counts `[9210, 310, 430, 50]`:**
  - "2xx done" — green `#008300`, fill `rgba(0,131,0,0.30)` with 2px `#008300` top edge
  - "3xx elsewhere" — blue `#2a78d6`, fill `rgba(42,120,214,0.30)` with 2px `#2a78d6` top edge
  - "4xx your fault" — yellow `#c98500`, fill `rgba(201,133,0,0.25)` with 2px `#c98500` top edge
  - "5xx our fault" — orange `#d95926`, fill `rgba(217,89,38,0.30)` with 2px `#d95926` top edge
- **Labels:** class name 12px `#2c3e50` under each bar; count bold 12px in the bar's color above each bar top ("9,210", "310", "430", "50").
- **Annotation (bold 13px ink `#1a5276`, near x=340, y=90):** "92% plain success — the other 790 replies tell the story".
- **Caption (12px `#6b7280`, bottom right):** "counts illustrative".

## A Baker's Dozen Worth Memorizing

**Tags:** `worked example` (blue), `the dozen codes` (green), `classes` (orange)

- **2xx done** — 200 (8,650 menu reads), 201 (480 orders created), 204 (80 cancels, empty body)
- **3xx elsewhere** — 301 (12 moved forever), 302 (18 moved just today), 304 (280 cache still valid)
- **4xx your fault** — 400 (90 bad request), 401 (60 no login), 403 (25 forbidden), 404 (205 not found)
- **429 slow down** — one bot hammered the menu and collected all 50 rate-limit replies itself
- **5xx our fault** — 500 (35, a bug in order pricing), 503 (15, down for a restart — retry later)
- **Hand-check** — 8,650+480+80 = 9,210 twos; 90+60+25+205+50 = 430 fours; the classes add up

*Example (italic):* The busiest code after 200 and 201 is 304 — 280 phones asked "has the menu changed?" and were told "no, keep using your copy."

**Key point:** Thirteen codes — a baker's dozen — account for all 10,000 responses; learn these and you can read almost any API's day at a glance.

### Visualization (canvas `c2`, 720×300)

Horizontal bar chart, one row per code (13 rows), bar length by count, colored by class.

- **Title (bold 15px, `#1a5276`, top center):** "The Baker's Dozen: Every Code From the Tuesday Log".
- **Layout:** rows top to bottom at y = 62, 79, 96, 113, 130, 147, 164, 181, 198, 215, 232, 249, 266; code + name 12px `#2c3e50` right-aligned at x=150 ("200 OK", "201 Created", "204 No Content", "301 Moved", "302 Found", "304 Not Modified", "400 Bad Request", "401 Unauthorized", "403 Forbidden", "404 Not Found", "429 Too Many", "500 Server Error", "503 Unavailable"); bars start at x=160, height 11.
- **Bar widths (hardcoded pixels, square-root feel so small counts stay visible):** `[380, 150, 70, 30, 36, 120, 75, 62, 42, 105, 58, 50, 35]` for counts `[8650, 480, 80, 12, 18, 280, 90, 60, 25, 205, 50, 35, 15]`.
- **Colors by class:** 2xx rows green `#008300` fill `rgba(0,131,0,0.30)`; 3xx rows blue `#2a78d6` fill `rgba(42,120,214,0.30)`; 4xx rows yellow `#c98500` fill `rgba(201,133,0,0.25)`; 5xx rows orange `#d95926` fill `rgba(217,89,38,0.30)`; each with a 1px solid edge in the class color.
- **Count labels:** 11px `#6b7280` at each bar's right end ("8,650", "480", "80", "12", "18", "280", "90", "60", "25", "205", "50", "35", "15").
- **Annotation (bold 12px magenta `#d55181`, near x=420, y=140):** "304 outnumbers every 4xx — caching is the log's second-loudest voice".
- **Caption (12px `#6b7280`, bottom right):** "pixel widths schematic, counts illustrative".

## Reading the Day's Logs by First Digit

**Tags:** `where it's used` (blue), `monitoring` (green), `retry logic` (orange)

- **Group by digit** — dashboards sum 5xx per hour first; the class matters before the exact code
- **The spike** — 21 of the day's 50 server errors land in the 7pm hour, right after a deploy
- **Alerting** — a threshold of 5 per hour stays silent all day and fires exactly once, at 7pm
- **Retry rules** — retry 503 and 429 (the server said "later"); never retry 400 or 404 (it won't change)
- **4xx forensics** — a 404 spike after a release usually means the release broke old links

*Example (italic):* The on-call engineer rolls back at 7:40pm; the 8pm hour falls back to 4 server errors and the alert clears itself.

**Key point:** Codes make logs computable — one GROUP BY on the first digit turns 10,000 raw lines into an incident timeline, an alert, and a retry policy.

### Visualization (canvas `c3`, 720×300)

Hourly bar chart of the Tuesday's 5xx responses with an alert-threshold line and a single deploy spike.

- **Title (bold 15px, `#1a5276`, top center):** "The Day's 50 Server Errors, Hour by Hour".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = hours 0 to 23 with 12px `#6b7280` tick labels "0h", "4h", "8h", "12h", "16h", "20h"; y = 5xx count 0 to 25, gridlines `#e5e9ef` at 5/10/15/20.
- **Bars:** 24 bars ~18px wide, orange `#d95926`, fill `rgba(217,89,38,0.35)` with 1px `#d95926` edge; hourly counts (hours 0–23, sum 50): `[0, 0, 0, 0, 0, 0, 1, 1, 2, 2, 1, 2, 2, 1, 1, 1, 2, 2, 3, 21, 4, 2, 2, 0]`.
- **Threshold line:** dashed `#6b7280` (dash 4/3) horizontal line at y for count 5, 12px `#6b7280` label "alert threshold: 5/hr" above its left end.
- **Spike:** the hour-19 bar reaches 21; bold 12px orange `#d95926` value label "21" above it.
- **Annotation (bold 13px orange `#d95926`, near x=380, y=80):** "7pm deploy: 21 errors in one hour — rolled back at 7:40".
- **Caption (12px `#6b7280`, bottom right):** "counts illustrative".

## The Lying 200 (and 401 vs 403)

**Tags:** `common mistake` (red), `200-but-failed` (orange), `401 vs 403` (blue)

- **The lying 200** — the payment service replies 200 with body `{"error": "card declined"}`
- **Invisible failures** — 74 declined orders one Wednesday show as green on every dashboard
- **The fix** — failures must fail in the status line; bodies explain, codes classify
- **401 vs 403** — 401 means "I don't know who you are"; 403 means "I know you, and no"
- **The tell** — 401 deserves a login prompt; answering 403 with one just loops the user forever

*Example (italic):* A barista's expired token gets 401 (log in again and retry); the same barista opening the payroll page gets 403 (no login will ever help).

**Common mistake:** Tunneling errors through 200 breaks every tool that speaks the language — monitors, retries, caches, and log queries all trust the code, not the body.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: the same 74 failed payments reported through a lying 200 vs an honest 4xx, and what each dashboard sees.

- **Title (bold 15px, `#1a5276`, top center):** "The Lying 200: When the Code and the Body Disagree".
- **Row 1 (y=95), label 12px `#6b7280` at x=20:** "code says 200"; blue `#2a78d6` rounded box at x=170 labeled "200 + {\"error\": \"declined\"}" (12px), 3px arrow to a yellow `#c98500` box at x=430 labeled "dashboard: all green" with bold 12px yellow `#c98500` "74 failures invisible".
- **Row 2 (y=205), label:** "code says 4xx"; blue box at x=170 labeled "4xx + the same body", 3px arrow to a green `#008300` box at x=430 labeled "dashboard: 74 client errors" with bold 12px green `#008300` "visible, alertable".
- **Box style:** 170–190px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(201,133,0,0.15)` / `rgba(0,131,0,0.12)`, 1px border in the box color, 12px `#2c3e50` text.
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=270):** "bodies explain, status codes classify — tooling only reads the code".
- **Caption (12px `#6b7280`, bottom right):** "order counts illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); the Tuesday traffic is invented and labeled illustrative — 10,000 requests splitting `[9210, 310, 430, 50]` by class, per-code counts `[8650, 480, 80, 12, 18, 280, 90, 60, 25, 205, 50, 35, 15]` (each class sums to its bar in c1), the hourly 5xx array summing to 50 with the spike of 21 at hour 19, and the 74 declined orders in c4; the code meanings themselves (200 OK through 503 Service Unavailable) are the real standardized semantics.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
