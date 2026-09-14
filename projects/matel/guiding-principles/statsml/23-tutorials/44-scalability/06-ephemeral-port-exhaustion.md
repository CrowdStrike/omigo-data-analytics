# Ephemeral Port Exhaustion

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Ephemeral Port Exhaustion

**Subtitle:** Every outbound connection borrows a local port number, and a closed one stays reserved for a minute — open a new connection per request and you run out of numbers around 470 requests per second

## A New Phone Line for Every Payment

**Tags:** `core idea` (blue), `TIME_WAIT` (orange), `outbound connections` (green)

- **The service** — a checkout service calls a payment API over TCP for every order it processes
- **The habit** — the code opens a fresh connection per request: connect, send, read reply, close
- **The port** — each outbound connection borrows one local ephemeral port from a fixed range
- **The range** — Linux defaults to 32768–60999, which is 28,232 usable port numbers in total
- **The lingering** — a closed connection sits in TIME_WAIT for ~60s, still holding its port

*Example (italic):* One payment call does its work in 100ms, then its port stays unusable for 60 more seconds — busy for 0.1s, reserved for 60.1s.

**Key point:** An ephemeral port is a slot the kernel lends to each outbound connection; TIME_WAIT means the slot is returned a full minute after the connection closes, not when it closes.

### Visualization (canvas `c1`, 720×300)

Horizontal timeline bar showing the life of one connect-per-request payment call: a sliver of useful work followed by a long TIME_WAIT tail occupying the same port.

- **Title (bold 15px, `#1a5276`, top center):** "One Request's Port: Busy 0.1s, Reserved 60.1s".
- **Axis:** horizontal 2px `#999` baseline at y=200, from x=60 to x=660 representing 0s to 60s; 12px `#444` tick labels at 0s / 15s / 30s / 45s / 60s.
- **Bar (y=140, 44px tall):** green `#008300` solid segment from x=60 width 8 (the 100ms of real work, exaggerated to stay visible), then orange fill `rgba(217,89,38,0.30)` with 2px `#d95926` border from x=68 to x=660 (the 60s TIME_WAIT).
- **Segment labels:** bold 12px green "connect + pay: 0.1s" above the green sliver at (x=64, y=120) anchored left; bold 13px orange `#d95926` "TIME_WAIT: port still held for 60s" centered inside the orange band.
- **Port tag:** 12px `#2c3e50` label "local port 45182" with a small `rgba(42,120,214,0.15)` rounded box at (x=60, y=90), pointing at the bar with a 1px `#6b7280` connector line.
- **Annotation (bold 13px violet `#4a3aa7`, below the baseline at y=240, centered):** "the port does 0.1s of work and then sits idle 600× longer".
- **Caption (12px `#444`, bottom right):** "durations illustrative; 60s is the classic TIME_WAIT default".

## Hand-Math: 500 Orders/s Against 28,232 Ports

**Tags:** `worked example` (blue), `rule of thumb` (green)

- **Demand** — at 500 orders/s, each minute the service opens 500 × 60 = 30,000 connections
- **Supply** — every one of those holds a port ~60s, so ~30,000 ports must be held at once
- **The shelf** — the range only has 28,232 ports, so demand exceeds supply by ~1,800 slots
- **Time to empty** — 28,232 ports ÷ 500 new/s ≈ 56 seconds from cold start to a full range
- **The ceiling** — the sustainable rate is 28,232 ÷ 60s ≈ 470 new connections/s, no matter the CPU

*Example (italic):* The checkout service boots at 500 orders/s; TIME_WAIT sockets climb by 500 every second and hit the 28,232-port wall about 56 seconds later.

**Key point:** Connection-per-request has a hard arithmetic ceiling — ports ÷ TIME_WAIT seconds, about 28,232 ÷ 60 ≈ 470 new connections per second on default Linux settings.

### Visualization (canvas `c2`, 720×300)

Line chart of TIME_WAIT socket count during the first 90 seconds at 500 orders/s, ramping linearly into a hard ceiling at 28,232.

- **Title (bold 15px, `#1a5276`, top center):** "500 Orders/s Fills All 28,232 Ports in ~56 Seconds".
- **Axes:** origin x=70, baseline y=245, plot width 580, plot height 180; x = seconds since start 0 to 90, 12px `#444` tick labels every 15s; y = sockets in TIME_WAIT 0 to 30,000, gridlines `#e5e9ef` at 10,000 / 20,000, labels "10k" / "20k" / "30k".
- **Ceiling line:** horizontal dashed red `#e74c3c` (dash 6/4) 2px line at y for 28,232, 12px bold red label "port range exhausted: 28,232" above it at the left end.
- **TIME_WAIT line:** blue `#2a78d6` 3px line through seconds `[0, 10, 20, 30, 40, 50, 56, 70, 90]`, sockets `[0, 5000, 10000, 15000, 20000, 25000, 28232, 28232, 28232]` — linear +500/s climb, then flat pinned at the ceiling.
- **Impact marker:** vertical dashed `#6b7280` (dash 4/3) line at 56s from baseline to the ceiling, 12px `#6b7280` label "56s" at its foot.
- **Annotation (bold 13px red `#e74c3c`, near x=70s, y=70):** "every new connect now fails".
- **Caption (12px `#444`, bottom right):** "illustrative; assumes 60s TIME_WAIT and one port per request".

## The Error That Only Shows Up at Peak

**Tags:** `where it's used` (blue), `symptoms` (red)

- **The message** — connect() starts failing with "cannot assign requested address" (EADDRNOTAVAIL)
- **The misdirection** — the payment API is healthy; the failure is local, before a packet is sent
- **Load-dependent** — 300 orders/s all day is fine; the flash sale crossing ~470/s falls over
- **Self-healing** — traffic dips, TIME_WAIT entries expire, errors vanish — until the next peak
- **Where it bites** — API clients, DB connections, proxies, health checkers: anything dialing out fast

*Example (italic):* During a flash sale the checkout service climbs past 470 orders/s at 12:02; payment calls start failing at 12:03 and recover on their own once traffic falls back at 12:08.

**Key point:** Intermittent "cannot assign requested address" errors that track traffic peaks and clear themselves are the signature of ephemeral port exhaustion — the remote service is innocent.

### Visualization (canvas `c3`, 720×300)

Dual-line timeline of a flash sale: order rate rising past the 470/s ceiling, with failed payment calls appearing only while the rate is above the line.

- **Title (bold 15px, `#1a5276`, top center):** "Flash Sale: Errors Appear Only Above ~470 Connections/s".
- **Axes:** origin x=70, baseline y=245, plot width 580, plot height 180; x = clock time "12:00" to "12:10", 12px `#444` tick labels every 2 minutes; y = per-second rate 0 to 800, gridlines `#e5e9ef` at 200 / 400 / 600.
- **Ceiling line:** horizontal dashed `#6b7280` (dash 6/4) line at 470, 12px `#6b7280` label "port ceiling ≈ 470/s" at its right end.
- **Order-rate line:** blue `#2a78d6` 3px line through minutes `[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]`, rate `[300, 380, 480, 620, 700, 680, 640, 540, 430, 350, 310]`.
- **Failure line:** red `#e74c3c` 3px line, same minute grid, failures/s `[0, 0, 10, 150, 230, 210, 170, 70, 0, 0, 0]` — nonzero only where orders exceed 470.
- **Shading:** light red fill `rgba(231,76,60,0.08)` over the x-band from minute 2 to minute 8 (the over-ceiling window), full plot height.
- **Legend (12px, top left inside plot):** blue swatch "orders/s", red swatch "failed connects/s".
- **Annotation (bold 13px red `#e74c3c`, near minute 4.5, y=75):** "'cannot assign requested address'".
- **Caption (12px `#444`, bottom right):** "rates illustrative".

## Tuning the Kernel Only Moves the Wall

**Tags:** `common mistake` (red), `connection pooling` (green)

- **The band-aid** — widening the range to 1024–65535 gives 64,512 ports: a ~1,075/s ceiling, not none
- **More knobs** — tcp_tw_reuse and shorter FIN timeouts shave TIME_WAIT but keep the same arithmetic
- **The real fix** — a keep-alive pool reuses connections, so requests stop consuming fresh ports
- **The math flip** — 50 pooled connections at ~10 requests/s each carry 500 orders/s on 50 ports
- **The mistake** — treating a resource-leak bug as a kernel-tuning problem and re-hitting it at 2× load

*Example (italic):* After widening the port range, the checkout service survives 500 orders/s — then next quarter's 1,100/s peak hits the new 1,075/s wall; a 50-connection pool would have used 50 ports at either load.

**Common mistake:** Fixing port exhaustion with sysctls. Tuning raises the connections-per-second ceiling from ~470 to ~1,075; pooling and keep-alive remove the per-request port cost entirely.

### Visualization (canvas `c4`, 720×300)

Horizontal bar chart comparing ports held at 500 orders/s under three strategies, against the default and widened range limits.

- **Title (bold 15px, `#1a5276`, top center):** "Ports Held at 500 Orders/s: Pooling Beats Tuning".
- **Layout:** left-aligned 12px `#444` row labels at x=20, bars start at x=230, max bar width 420 mapping linearly to 32,000 ports; bars 26px tall.
- **Rows (top to bottom at y = 80, 140, 200):**
  - "new conn/request, default range": red `#e74c3c` solid bar width 394 (30,000 ports needed), 11px red end label "30,000 needed > 28,232 — fails"
  - "new conn/request, widened range": orange `#d95926` solid bar width 394 (30,000 ports), 11px orange end label "30,000 of 64,512 — survives, for now"
  - "keep-alive pool of 50": green `#008300` solid bar width 6 (50 ports, minimum visible width), bold 12px green end label "50 ports — no ceiling from TIME_WAIT"
- **Limit markers:** vertical dashed red `#e74c3c` (dash 4/3) line at x=601 (28,232 on the 32,000-port scale) with 11px red label "default limit 28,232" at its top; the widened 64,512 limit lies off-scale, so note it with an 11px `#6b7280` label "widened limit 64,512 →" at the right plot edge (x=650, y=140).
- **Annotation (bold 13px green `#008300`, centered near y=260):** "reuse connections and the port math disappears".
- **Caption (12px `#444`, bottom right):** "pool sizing illustrative: 50 connections × ~10 req/s each".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); order rates, failure counts, and pool sizing are invented and labeled illustrative; 32768–60999 (= 28,232 ports), 1024–65535 (= 64,512 ports), 60s TIME_WAIT, 28,232÷60 ≈ 470/s, and 64,512÷60 ≈ 1,075/s are real Linux defaults and their exact arithmetic.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
