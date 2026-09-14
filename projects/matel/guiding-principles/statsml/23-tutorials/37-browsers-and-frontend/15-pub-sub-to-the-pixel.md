# Pub/Sub to the Pixel

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Pub/Sub to the Pixel

**Subtitle:** How one row inserted into a database becomes a moving number on a wall screen — every hop from the write to the changed pixel

## One Latte, One INSERT, One Tick of the Counter

**Tags:** `core idea` (blue), `event flow` (green), `full stack` (orange)

- **The write** — a barista rings up a latte; one row lands in the coffee shop's orders table
- **The change event** — a change-capture reader picks up the new row the moment the commit finishes
- **The broker** — a message broker receives the event and fans it out to every subscriber
- **The push** — an open websocket carries the event down to the dashboard in the browser
- **The re-render** — the page updates its in-memory count and repaints just that number
- **The pixel** — the wall screen's "orders today" ticks from 1,283 to 1,284, untouched by hands

*Example (italic):* Nobody refreshes anything — the INSERT itself sets off a chain that ends with different pixels on the screen.

**Key point:** Pub/sub to the pixel is one unbroken chain: the database publishes a change, layers subscribe and forward it, and the last subscriber is the pixel itself.

### Visualization (canvas `c1`, 720×300)

Left-to-right pipeline diagram: seven rounded boxes connected by arrows, tracing the single INSERT from the register to the changed pixel.

- **Title (bold 15px, `#1a5276`, top center):** "One INSERT, Seven Hops, One Changed Pixel".
- **Layout:** seven rounded boxes (86px wide, 46px tall, 8px radius) in a row snaking over two lines: top row at y=95 holds "INSERT row", "commit", "change event", "broker"; bottom row at y=205 holds "websocket", "state update", "re-render + paint"; 3px `#6b7280` arrows connect them in order, with a curved arrow from "broker" down to "websocket".
- **Box fills/borders (12px `#2c3e50` text):** database-side boxes 1–3 `rgba(42,120,214,0.15)` with 2px `#2a78d6` border; "broker" `rgba(74,58,167,0.12)` with 2px `#4a3aa7` border; browser-side boxes 5–7 `rgba(0,131,0,0.12)` with 2px `#008300` border.
- **Counter callout:** at far right (x≈610, y=205) a bold 16px `#008300` "1,283 → 1,284" beside the last box.
- **Zone labels (11px `#6b7280`):** "database" under the blue boxes, "middle tier" under the violet box, "browser" under the green boxes.
- **Annotation (bold 13px `#2a78d6`, centered near y=265):** "no refresh button anywhere in this chain".
- **Caption (12px `#444`, bottom right):** "hop layout schematic; counter values illustrative".

## Adding Up the 110 Milliseconds

**Tags:** `worked example` (blue), `latency budget` (green)

- **Commit** — the INSERT commits and becomes durable: 5 ms
- **Change capture** — the change-event reader notices the new row: 30 ms
- **Broker publish** — the broker accepts the event and routes it to subscribers: 10 ms
- **Websocket delivery** — the event crosses the network to the open browser tab: 40 ms
- **Browser work** — state update 3 ms, re-render 15 ms, paint 7 ms: 25 ms in total
- **Hand-sum** — 5 + 30 + 10 + 40 + 3 + 15 + 7 = 110 ms from write to pixel

*Example (italic):* The latte's row commits at 09:14:02.000; the wall screen shows 1,284 at 09:14:02.110 — about a tenth of a second later.

**Key point:** End-to-end latency is just the hops added up — and the slow hops are the network and the change capture, not the database or the paint.

### Visualization (canvas `c2`, 720×300)

Horizontal waterfall bar: seven segments laid end to end along a millisecond axis, each hop a colored block whose width is its latency.

- **Title (bold 15px, `#1a5276`, top center):** "The 110 ms Journey, Hop by Hop".
- **Axes:** origin x=60, single bar row centered at y=140 (bar 34px tall), plot width 600; x = 0 to 110 ms mapped linearly, 12px `#444` tick labels at 0/25/50/75/110, vertical gridlines `#e5e9ef`.
- **Segments (hardcoded ms, left to right):** commit 5 (`#2a78d6`), change capture 30 (`#c98500`), broker 10 (`#4a3aa7`), websocket 40 (`#d95926`), state update 3 (`#199e70`), re-render 15 (`#008300`), paint 7 (`#d55181`); segment array `[5, 30, 10, 40, 3, 15, 7]`.
- **Segment labels:** 11px `#2c3e50` hop names alternating above (y=110) and below (y=185) the bar with short 1px `#6b7280` leader lines; ms values 11px inside or beside each block.
- **Total marker:** bold 13px `#1a5276` "total 110 ms" at the right end of the bar (x≈660, y=140).
- **Annotation (bold 13px `#d95926`, near x=270, y=235):** "network + change capture = 70 of the 110 ms".
- **Caption (12px `#444`, bottom right):** "per-hop latencies illustrative".

## Why Live Dashboards Beat the Refresh Button

**Tags:** `where it's used` (blue), `freshness` (green)

- **Ops consoles** — an on-call screen showing error counts must move the moment errors happen
- **Live dashboards** — the shop's wall screen, delivery maps, stock tickers all ride this chain
- **The old way** — polling re-runs the query every 5 s; the number is up to 5,000 ms stale
- **The push way** — the same number is at most ~110 ms stale, and idle when nothing changes
- **The full-stack lens** — one story touches SQL, queues, sockets, and rendering in a single trace
- **Debug value** — when the number stops moving, you can ask which of the seven hops broke

*Example (italic):* A 5-second poller shows the 09:14:02 latte at 09:14:05 at worst; the push chain shows it at 09:14:02.110.

**Key point:** Push turns freshness from "how often do we ask?" into "how fast does one event travel?" — a 45× improvement here (5,000 ms worst case down to 110 ms).

### Visualization (canvas `c3`, 720×300)

Timeline chart: the displayed counter value over 15 seconds after three orders arrive, polling (staircase, lagging) vs push (steps almost instantly), on a shared time axis.

- **Title (bold 15px, `#1a5276`, top center):** "Three Orders in 15 Seconds: Polling Lags, Push Moves Now".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = 0 to 15 s, 12px `#444` tick labels every 5 s; y = counter value 1,283 to 1,287, gridlines `#e5e9ef` at each integer value.
- **Order markers:** vertical dashed `#6b7280` (dash 4/3) lines at t = `[2, 6, 13]` s (the three INSERTs), 11px `#6b7280` "order" labels at top.
- **Push line:** green `#008300` 3px step line — value 1283 until t=2.11, then 1284; 1285 at t=6.11; 1286 at t=13.11 (steps at order time + 0.11 s).
- **Polling line:** orange `#d95926` 3px step line, polls at t = `[0, 5, 10, 15]` — value 1283 until t=5, 1284 at t=5, 1285 at t=10, 1286 at t=15.
- **Labels:** bold 12px green "push (110 ms behind)" near (x≈8 s, upper area); bold 12px orange "poll every 5 s" near (x≈8 s, lower area).
- **Annotation (bold 13px `#008300`, near t=13, y=70):** "the 09:14:13 order shows 110 ms later, not 2 s later".
- **Caption (12px `#444`, bottom right):** "order times and latencies illustrative".

## A Frozen Number Still Looks Live

**Tags:** `common mistake` (red), `silent failure` (orange)

- **The trap** — a push dashboard shows no spinner and no error when its websocket silently dies
- **The freeze** — the counter stops at 1,284 and simply looks like a quiet morning
- **Polling fails loud** — a broken poller throws a visible query error; a broken socket shows nothing
- **The tell** — a "last event received" timestamp exposes staleness that the number itself hides
- **The fix** — heartbeats every few seconds, auto-reconnect, and a resync query after reconnect
- **The re-check** — after reconnecting, refetch the true count; missed events never replay themselves

*Example (italic):* The socket drops at 09:20; forty real orders later the wall screen still says 1,284, and nobody notices until the till is counted.

**Common mistake:** Trusting a live dashboard because it is push-based. A dead subscription looks exactly like no news — build heartbeats and a resync, or the pixel lies.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: a healthy chain updating the pixel vs a chain with a dead websocket leaving a frozen, live-looking number.

- **Title (bold 15px, `#1a5276`, top center):** "Dead Socket, Live-Looking Number".
- **Row 1 (y=95), label 12px `#444` at x=20:** "healthy"; three rounded boxes — blue `#2a78d6` "DB: 40 new orders" at x=150, violet `#4a3aa7` "broker" at x=340, green `#008300` "screen: 1,324" at x=520 — joined by 3px `#6b7280` arrows, bold 12px green "✓ pixel tracks the table" at the right.
- **Row 2 (y=205), label:** "socket dropped"; same blue "DB: 40 new orders" and violet "broker" boxes, then the arrow to the screen box broken mid-way with a bold 16px red `#e74c3c` "✗" at the gap (x≈450); screen box border red `#e74c3c`, text "screen: 1,284 (frozen)", bold 12px red "40 orders invisible".
- **Box style:** 150–170px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(74,58,167,0.12)` / `rgba(0,131,0,0.12)` / `rgba(231,76,60,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px `#c98500`, centered near y=270):** "a heartbeat + resync query is the only proof the number is real".
- **Caption (12px `#444`, bottom right):** "order counts illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); hop latencies `[5, 30, 10, 40, 3, 15, 7]` ms summing to 110, counter values 1,283→1,286 (c3) and 1,284/1,324 (c4), order times `[2, 6, 13]` s, and poll times `[0, 5, 10, 15]` s are all invented and labeled illustrative.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
