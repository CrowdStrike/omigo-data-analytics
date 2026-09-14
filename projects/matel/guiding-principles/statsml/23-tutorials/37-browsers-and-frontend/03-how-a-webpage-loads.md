# How a Webpage Loads

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** How a Webpage Loads

**Subtitle:** From typing a URL to seeing pixels is one timeline — find the server, open a connection, fetch the HTML, fetch what the HTML asks for, paint

## Typing brewandbean.example and Hitting Enter

**Tags:** `core idea` (blue), `one timeline` (green), `URL to pixels` (orange)

- **The URL** — you type a coffee shop's address, brewandbean.example, and hit enter
- **The lookup** — DNS turns the name into a server address, like looking up a phone number
- **The handshake** — the browser opens a TCP connection, then a TLS handshake makes it private
- **The HTML** — the browser asks for the page; the server sends back one HTML file
- **The discoveries** — reading the HTML, the browser finds a stylesheet, a script, and a hero photo to fetch
- **The paint** — with HTML parsed and the stylesheet in, the browser draws the first pixels

*Example (italic):* You press enter at 0 ms; the shop's menu page is fully on screen 450 ms later, and every one of those milliseconds belongs to exactly one step.

**Key point:** A page load is a fixed sequence — DNS, connect, fetch HTML, fetch discovered assets, render — and in this simplified model nothing later can start before the step it depends on finishes.

### Visualization (canvas `c1`, 720×300)

Single horizontal phase strip: one bar from 0 to 450 ms split into five colored segments, one per step, with the step name above each segment and its duration below.

- **Title (bold 15px, `#1a5276`, top center):** "One Load, Five Steps: brewandbean.example in 450 ms".
- **Axis:** time 0–450 ms mapped to x = 60–660 (scale 600/450 px per ms); 2px `#999` baseline at y=200 with 12px `#444` tick labels at 0 / 100 / 200 / 300 / 400 ms.
- **Strip (y=140, 40px tall), segments left to right with hardcoded pixel spans:**
  - "DNS" blue `#2a78d6`: 0–40 ms → x 60, width 53
  - "TCP" aqua `#199e70`: 40–70 ms → x 113, width 40
  - "TLS" violet `#4a3aa7`: 70–130 ms → x 153, width 80
  - "HTML" green `#008300`: 130–250 ms → x 233, width 160
  - "assets + render" magenta `#d55181`: 250–450 ms → x 393, width 267
- **Segment labels:** step name bold 12px in the segment color above the strip (y=125, rotated none, staggered to y=110 where segments are narrow); duration ("40 ms", "30 ms", "60 ms", "120 ms", "200 ms") 11px `#6b7280` below at y=220.
- **Annotation (bold 13px ink `#1a5276`, centered near y=70):** "each step waits for the one before it".
- **Caption (12px `#444`, bottom right):** "millisecond durations illustrative".

## Brew & Bean's 450-Millisecond Waterfall

**Tags:** `worked example` (blue), `waterfall` (green)

- **The setup** — DNS 40 ms, TCP 30 ms, TLS 60 ms, HTML request and download 120 ms
- **Hand-check** — 40 + 30 + 60 + 120 = 250 ms before the browser has any HTML to read
- **The fan-out** — at 250 ms the parser finds three assets and requests all of them at once
- **The assets** — style.css takes 80 ms (done 330), menu.js takes 120 ms (done 370), hero.jpg takes 200 ms (done 450)
- **First paint** — HTML plus stylesheet is enough to draw, so pixels appear at 350 ms
- **The finish** — the load ends when the slowest asset lands: 250 + 200 = 450 ms

*Example (italic):* The three asset bars all start at the 250 ms mark because none of them could be requested before the HTML that names them arrived — real browsers stream-parse and can request assets earlier; this model keeps the steps whole.

**Key point:** The serial steps add (250 ms to the HTML); the parallel assets don't — the page finishes when the longest one does, so the total is 250 + max(80, 120, 200) = 450 ms.

### Visualization (canvas `c2`, 720×300)

Waterfall chart: seven horizontal bars, one row per step, each starting at its offset — the classic devtools network waterfall drawn by hand.

- **Title (bold 15px, `#1a5276`, top center):** "The Waterfall: Serial Steps Stack, Parallel Assets Overlap".
- **Axis:** time 0–500 ms mapped to x = 130–650 (scale 520/500 px per ms); 2px `#999` baseline at y=245; 12px `#444` tick labels at 0 / 100 / 200 / 300 / 400 / 500 ms; vertical gridlines `#e5e9ef` at each tick.
- **Rows (14px-tall bars at y = 70, 95, 120, 145, 170, 195, 220), left-aligned 12px `#444` row labels at x=20, hardcoded start/width in ms → px:**
  - "DNS" 0–40 ms: x 130, width 42, blue `#2a78d6`
  - "TCP" 40–70 ms: x 172, width 31, aqua `#199e70`
  - "TLS" 70–130 ms: x 203, width 62, violet `#4a3aa7`
  - "HTML" 130–250 ms: x 265, width 125, green `#008300`
  - "style.css" 250–330 ms: x 390, width 83, yellow `#c98500`
  - "menu.js" 250–370 ms: x 390, width 125, orange `#d95926`
  - "hero.jpg" 250–450 ms: x 390, width 208, magenta `#d55181`
- **Duration labels:** 11px `#6b7280` at each bar's right end ("40", "30", "60", "120", "80", "120", "200" ms).
- **First-paint marker:** vertical dashed `#6b7280` (dash 4/3) line at 350 ms (x=494) from y=55 to y=245, 12px `#6b7280` label "first paint 350 ms" at its top.
- **Annotation (bold 13px green `#008300`, near x=200, y=55):** "250 ms spent before one asset could even be asked for".
- **Caption (12px `#444`, bottom right):** "timings illustrative; layout matches browser devtools".

## Where Slow Pages Hide Their Time

**Tags:** `where it's used` (blue), `page speed` (green), `debugging` (orange)

- **The metrics** — first paint (350 ms here) and total load (450 ms) are the numbers speed tools report
- **The diagnosis** — a waterfall shows which single step a slow page is actually spending its time in
- **A bad day** — the same shop with a slow DNS resolver: the 40 ms lookup becomes 300 ms
- **The shift** — every later step slides right by 260 ms, so the total goes 450 → 710 ms
- **The lesson** — nothing downstream got slower, yet the whole page did; fix the step, not the site
- **The habit** — before optimizing images or code, look at the waterfall to see where the time really is

*Example (italic):* A team spends a week shrinking hero.jpg by 50 ms while the waterfall shows a 260 ms DNS problem sitting in plain sight at the far left.

**Key point:** Because the front of the timeline is serial, one slow early step delays everything — a waterfall makes the guilty step visible in one glance.

### Visualization (canvas `c3`, 720×300)

Two stacked horizontal bars comparing the normal day and the slow-DNS day, same five segments, so the DNS segment visibly balloons and drags the total from 450 to 710 ms.

- **Title (bold 15px, `#1a5276`, top center):** "Same Site, Slow DNS: 450 ms Becomes 710 ms".
- **Axis:** time 0–750 ms mapped to x = 130–650 (scale 520/750 px per ms); 2px `#999` baseline at y=245; 12px `#444` tick labels at 0 / 250 / 500 / 750 ms.
- **Row 1 (y=100, 26px tall), 12px `#444` label "normal day — 450 ms" at x=20; segments (ms → px width):** DNS 40 → blue `#2a78d6` width 28, TCP 30 → aqua `#199e70` width 21, TLS 60 → violet `#4a3aa7` width 42, HTML 120 → green `#008300` width 83, assets 200 → magenta `#d55181` width 139; bar starts at x=130.
- **Row 2 (y=180, 26px tall), label "slow DNS day — 710 ms":** DNS 300 → blue width 208, then the same TCP 21 / TLS 42 / HTML 83 / assets 139 widths; bar starts at x=130.
- **End labels:** bold 12px ink `#1a5276` "450 ms" and "710 ms" just right of each bar's end.
- **Bracket:** thin `#6b7280` bracket under row 2's DNS segment with 11px `#6b7280` label "+260 ms, all in one step".
- **Annotation (bold 13px orange `#d95926`, near x=420, y=60):** "downstream steps unchanged — they just start later".
- **Caption (12px `#444`, bottom right):** "durations illustrative; the +260 ms shift is exact arithmetic".

## HTML Arriving Is Not the Page Loading

**Tags:** `common mistake` (red), `first paint` (orange)

- **The confusion** — people picture the server "sending the page" as one delivery, done at 250 ms
- **The reality** — at 250 ms the user still sees a blank screen; the HTML is instructions, not pixels
- **The wait** — the browser holds first paint until the stylesheet lands at 330 ms, then draws at 350 ms
- **The straggler** — hero.jpg pops in at 450 ms, a full 200 ms after the HTML was "delivered"
- **The mistake** — measuring only server response time (250 ms) and declaring the page fast

*Example (italic):* The server team celebrates a 250 ms response while customers stare at a blank screen for another 100 ms and a photo-less menu for 200 ms more.

**Common mistake:** Treating HTML arrival as the finish line. The user's experience is first paint and last asset — a page can have a fast server and still feel slow, because rendering is a second act the server never sees.

### Visualization (canvas `c4`, 720×300)

Two parallel lanes on one time axis: "what the network sees" (HTML delivered at 250 ms, all bytes done at 450 ms) vs "what the user sees" (blank until 350 ms, photo missing until 450 ms).

- **Title (bold 15px, `#1a5276`, top center):** "Two Finish Lines: the Server's and the User's".
- **Axis:** time 0–500 ms mapped to x = 60–660 (scale 600/500 px per ms); 2px `#999` baseline at y=245; 12px `#444` tick labels at 0 / 100 / 200 / 300 / 400 / 500 ms.
- **Lane 1 (y=110, 30px tall), 12px `#444` label "network" at x=60 above the lane (y=95):** segment "fetching HTML" 0–250 ms (x 60, width 300) fill `rgba(42,120,214,0.25)` with 2px `#2a78d6` border; segment "fetching assets" 250–450 ms (x 360, width 240) fill `rgba(42,120,214,0.15)`; 12px `#2c3e50` labels inside.
- **Lane 2 (y=185, 30px tall), label "user's screen" at y=170:** segment "blank screen" 0–350 ms (x 60, width 420) fill `rgba(231,76,60,0.12)` with 2px `#e74c3c` border and 12px red label; segment "menu visible, photo missing" 350–450 ms (x 480, width 120) fill `rgba(201,133,0,0.18)` with 2px `#c98500` border; green `#008300` 30px-tall block "complete" at 450–500 ms (x 600, width 60).
- **Markers:** vertical dashed `#6b7280` (dash 4/3) lines at 250 ms (x=360, label "HTML delivered") and 350 ms (x=480, label "first paint"), 11px `#6b7280` labels at top.
- **Annotation (bold 13px red `#e74c3c`, near x=170, y=70):** "server done at 250 ms — user sees nothing until 350 ms".
- **Caption (12px `#444`, bottom right):** "timings illustrative, same numbers as the waterfall above".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all timings are the hardcoded milliseconds above (no randomness) — serial steps DNS 40 / TCP 30 / TLS 60 / HTML 120 (cumulative 250), parallel assets style.css 80 / menu.js 120 / hero.jpg 200, first paint 350, total 450; the slow-DNS variant swaps DNS to 300 for a 710 total; all durations invented and labeled illustrative, the sums are exact arithmetic on those durations.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
