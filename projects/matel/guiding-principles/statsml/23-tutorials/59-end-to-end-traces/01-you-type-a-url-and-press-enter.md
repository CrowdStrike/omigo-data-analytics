# You Type a URL and Press Enter

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** You Type a URL and Press Enter

**Subtitle:** Between the Enter key and the first pixel, a dozen subsystems each do one job in strict order — keyboard interrupt, OS, browser, DNS, TCP, TLS, HTTP, server, and the render pipeline

## One Keypress, a Dozen Subsystems

**Tags:** `core idea` (blue), `layers` (green), `systems` (orange)

- **The URL** — you type `https://news.example.com/today` and press Enter at 9:00am
- **The interrupt** — the keyboard raises a hardware interrupt; the OS wakes the browser process
- **The parse** — the browser splits the URL into scheme `https`, host `news.example.com`, path `/today`
- **The lookup** — DNS turns the host name into an IP address before any connection can open
- **The handshakes** — TCP agrees on a connection, TLS agrees on encryption, only then HTTP speaks
- **The pixels** — the server's HTML becomes DOM, CSSOM, layout, paint, and finally lit pixels

*Example (italic):* One Enter press for `news.example.com/today` triggers ten handoffs — interrupt, OS, parse, DNS, TCP, TLS, HTTP, server, render, pixels — each unaware of the others.

**Key point:** No single component "loads the page" — a chain of specialized subsystems each does one narrow job and hands the result to the next layer in a fixed order.

### Visualization (canvas `c1`, 720×300)

Two-row flow diagram: ten stage boxes connected by arrows, tracing one URL from the Enter key (top left) to pixels on screen (bottom right).

- **Title (bold 15px, `#1a5276`, top center):** "news.example.com/today: Enter Key to Pixels in Ten Handoffs".
- **Row 1 (box centers at y=95), five boxes left to right at x = 85, 220, 355, 490, 625:** "keyboard interrupt", "OS wakes browser", "browser parses URL", "DNS: name → IP", "TCP handshake" — blue `#2a78d6` rounded boxes, 3px `#2a78d6` arrows between them.
- **Row wrap arrow:** 3px `#6b7280` arrow from the right end of row 1 curving down to the left start of row 2.
- **Row 2 (box centers at y=205), five boxes left to right at x = 85, 220, 355, 490, 625:** "TLS handshake", "HTTP GET /today", "server responds", "DOM + CSSOM + layout", "paint → pixels" — first three blue, last two green `#008300` boxes (the render side), arrows as in row 1.
- **Box style:** 120px wide, 44px tall, 8px radius, fill `rgba(42,120,214,0.15)` for blue and `rgba(0,131,0,0.12)` for green, 12px `#2c3e50` centered text (two lines allowed).
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=270):** "each box knows nothing about the others — it just does its one job".
- **Caption (12px `#444`, bottom right):** "stages ordered, durations not to scale".

## 310 Milliseconds, Stage by Stage

**Tags:** `worked example` (blue), `timing waterfall` (green)

- **Instant part** — interrupt, OS handoff, and URL parse together take under 1ms; they round to 0 here
- **DNS 40ms** — cache miss: browser asks the resolver, which walks root → `.com` TLD → authoritative
- **TCP 30ms** — SYN, SYN-ACK, ACK: one full round trip to the server before any data moves
- **TLS 40ms** — certificate exchange and key agreement: roughly another round trip, plus crypto
- **Server 120ms** — the HTTP GET lands, the server builds the page, the HTML travels back
- **Render 80ms** — parse HTML, fetch subresources, build DOM/CSSOM, layout, paint

*Example (italic):* 40 + 30 + 40 + 120 + 80 = 310ms exactly — and 110ms of it (DNS + TCP + TLS) is spent before the request even reaches the server.

**Key point:** The waterfall is strictly sequential — DNS must finish before TCP can start, TCP before TLS, TLS before HTTP — so the stage times add, and every early millisecond delays everything after it.

### Visualization (canvas `c2`, 720×300)

Waterfall (Gantt) chart: five stage bars stacked top to bottom, each starting where the previous ends, spanning 0–310ms on a shared time axis.

- **Title (bold 15px, `#1a5276`, top center):** "First Visit Waterfall: 40 + 30 + 40 + 120 + 80 = 310ms".
- **Axes:** time axis baseline at y=250, plot from x=60 to x=660 (600px = 310ms); 12px `#444` tick labels at 0 / 100 / 200 / 310ms (x = 60, 254, 447, 660), vertical gridlines `#e5e9ef` at each tick.
- **Bars (18px tall, at y = 60, 98, 136, 174, 212, left edge = 60 + start px, hardcoded start/width in px):**
  - "DNS 40ms": start 0, width 77, blue `#2a78d6`
  - "TCP 30ms": start 77, width 58, aqua `#199e70`
  - "TLS 40ms": start 135, width 78, violet `#4a3aa7`
  - "Server 120ms": start 213, width 232, orange `#d95926`
  - "Render 80ms": start 445, width 155, green `#008300`
- **Row labels:** 12px `#444` stage name left of each bar at x=8; 11px duration label at each bar's right end.
- **Annotation (bold 13px `#1a5276`, above the server bar near x=330, y=50):** "110ms gone before the request even lands".
- **Caption (12px `#444`, bottom right):** "timings illustrative, arithmetic exact".

## Caches at Every Layer: Why Visit Two Is Fast

**Tags:** `where it's used` (blue), `caching` (green)

- **DNS cache** — the OS and browser remember `news.example.com`'s IP: 40ms drops to 0
- **Connection reuse** — HTTP keep-alive reuses the open TCP+TLS session: 30 + 40ms drop to 0
- **CDN / server cache** — the edge already holds `/today` and skips the origin: 120ms drops to 30
- **Browser cache** — CSS, JS, and images come from disk, but layout and paint still run: render stays 80
- **The sum** — second visit: 0 + 0 + 0 + 30 + 80 = 110ms, versus 310ms cold

*Example (italic):* The second visit to `news.example.com/today` takes 110ms instead of 310ms — not because any code got faster, but because four caches each deleted a stage.

**Key point:** Every layer of the trace has its own cache — DNS, connection, CDN, browser — and the speedup from 310ms to 110ms comes from skipping stages entirely, not from doing them faster.

### Visualization (canvas `c3`, 720×300)

Two stacked horizontal bars comparing first visit (310ms, five segments) with second visit (110ms, two segments) on the same millisecond scale.

- **Title (bold 15px, `#1a5276`, top center):** "First Visit 310ms vs Second Visit 110ms: Caches Delete Stages".
- **Scale:** bars start at x=110, 600px = 310ms; 12px `#444` tick labels at 0 / 100 / 200 / 310ms (x = 110, 304, 497, 710) along a 2px `#999` baseline at y=240, gridlines `#e5e9ef`.
- **Bar 1 (y=90, 34px tall), label 12px `#444` "first visit" at x=8; segments left to right (hardcoded px widths):** DNS blue `#2a78d6` 77, TCP aqua `#199e70` 58, TLS violet `#4a3aa7` 78, server orange `#d95926` 232, render green `#008300` 155 — 11px white segment labels "40" / "30" / "40" / "120" / "80" inside each segment.
- **Bar 2 (y=170, 34px tall), label "second visit":** server orange 58 ("30"), render green 155 ("80") — total width 213; the missing DNS/TCP/TLS region shown as a dashed `#6b7280` outline box (width 213 offset is not drawn; instead a 12px `#6b7280` note "DNS, TCP, TLS: all cached → 0ms" to the right of the bar at x≈340).
- **Legend (11px, below title at y=52):** five color swatches with stage names.
- **Annotation (bold 13px green `#008300`, near x=520, y=180):** "110ms vs 310ms — nothing ran faster; stages were skipped".
- **Caption (12px `#444`, bottom right):** "timings illustrative, sums exact".

## Blaming the Server for the Network's Time

**Tags:** `common mistake` (red), `latency attribution` (orange)

- **The complaint** — "the page took 310ms, the server must be slow" — but the server used 120ms
- **The hidden part** — DNS + TCP + TLS burn 110ms of round trips before the request exists server-side
- **The server's view** — its own logs show 120ms and look fine; the other 190ms is invisible to it
- **The wrong fix** — buying faster servers cannot touch the 110ms of pre-request network setup
- **The right fix** — cut round trips: cached DNS, kept-alive connections, closer edges, fewer handshakes

*Example (italic):* Of the 310ms total, only 120ms (39%) is the server — 110ms (35%) is network setup before the request lands and 80ms (26%) is the browser rendering.

**Common mistake:** Attributing all page latency to "the server". Server logs only start when the request arrives — the DNS, TCP, and TLS round trips before that never appear in them, so the biggest costs can hide entirely off the server's books.

### Visualization (canvas `c4`, 720×300)

Two horizontal attribution bars on the same scale: what the complaint assumes (all 310ms on the server) versus where the 310ms actually went.

- **Title (bold 15px, `#1a5276`, top center):** "Where Did 310ms Go? The Server Only Saw 120ms of It".
- **Scale:** bars start at x=110, 600px = 310ms; 2px `#999` baseline at y=240 with 12px `#444` tick labels at 0 / 100 / 200 / 310ms (x = 110, 304, 497, 710).
- **Bar 1 (y=90, 34px tall), label 12px `#444` "assumed" at x=8:** one red `#e74c3c` segment, width 600, fill `rgba(231,76,60,0.25)` with 3px `#e74c3c` border, centered bold 12px red label "server: 310ms (wrong)".
- **Bar 2 (y=170, 34px tall), label "actual":** three segments (hardcoded px widths): network setup violet `#4a3aa7` 213 ("110ms — 35%"), server orange `#d95926` 232 ("120ms — 39%"), render green `#008300` 155 ("80ms — 26%") — 11px white labels inside each segment.
- **Bracket:** thin `#6b7280` bracket over the violet segment with 12px `#6b7280` note above at y=150: "round trips before the request lands".
- **Annotation (bold 13px red `#e74c3c`, near x=430, y=270):** "server logs never see the first 110ms".
- **Caption (12px `#444`, bottom right):** "timings illustrative, percentages exact (110/120/80 of 310)".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all stage durations are the hardcoded values above (no randomness); the 40/30/40/120/80ms timings and the second-visit 0/0/0/30/80ms are invented and labeled illustrative; the sums (310, 110) and percentages (35% / 39% / 26%) are exact arithmetic on those values; pixel widths use the fixed scale 600px = 310ms (77/58/78/232/155 and 213/232/155, each summing to 600; 58+155 = 213 for the second visit).
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
