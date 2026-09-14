# WebSockets

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** WebSockets

**Subtitle:** One connection that stays open so either side can speak at any time — a phone call instead of a stack of letters

## A Phone Call Instead of Letters

**Tags:** `core idea` (blue), `push vs pull` (green), `handshake` (orange)

- **The board** — a café's browser screen shows every order's status: received, brewing, ready
- **Letters** — plain HTTP asks "any news on order 42?" again and again; each ask is a fresh envelope
- **The call** — a WebSocket dials once and keeps the line open; either side may talk whenever it wants
- **The handshake** — it begins as an HTTP request with `Upgrade: websocket`; the server answers `101 Switching Protocols`
- **The push** — when order 42 is ready, the server says so on the open line — nobody had to ask

*Example (italic):* At 3:02pm the barista marks order 42 ready; the message travels down the already-open line and the café board flips to "ready" within a blink.

**Key point:** A WebSocket is one long-lived, two-way connection that starts life as an ordinary HTTP request and then stays open — the server can push news the moment it happens instead of waiting to be asked.

### Visualization (canvas `c1`, 720×300)

Two-row flow diagram on a shared 60-second time axis: polling as six ask/answer letter pairs (five wasted) vs one WebSocket call that stays open and carries a single pushed message.

- **Title (bold 15px, `#1a5276`, top center):** "Asking 6 Times vs One Call That Stays Open".
- **Time axis:** 2px `#999` line at y=150 from x=140 to x=680 representing 0–60s; 12px `#444` tick labels "0s", "30s", "60s" at x=140/410/680; row labels 12px `#444` at x=20: "polling (letters)" at y=85 and "WebSocket (call)" at y=225.
- **Row 1 (polling, around y=85):** six ask/answer arrow pairs centered at x = `[160, 250, 340, 430, 520, 610]` (one per 10s poll); each pair is a 2px blue `#2a78d6` down-arrow (ask) beside a 2px mute `#6b7280` up-arrow (answer); 11px `#6b7280` label "no" over the first five answers, bold 11px green `#008300` "ready!" over the sixth.
- **Row 2 (WebSocket, around y=225):** blue `#2a78d6` rounded box (150×36, fill `rgba(42,120,214,0.15)`) at x=140 labeled "Upgrade → 101" (12px); then a 3px green `#008300` horizontal line from x=300 to x=670 labeled 12px green "line stays open" beneath it; one 3px magenta `#d55181` down-arrow at x=610 labeled bold 11px magenta "order 42 ready (pushed)".
- **Annotation (bold 12px green `#008300`, near x=340, y=270):** "one message, sent exactly when news exists".
- **Caption (12px `#444`, bottom right):** "timing illustrative; the Upgrade → 101 handshake is the documented protocol".

## Counting the Envelopes: 3,600 Polls vs One Open Socket

**Tags:** `worked example` (blue), `header overhead` (green), `hand-check` (orange)

- **Per envelope** — one poll costs ~500 B of request headers plus ~300 B of response headers = 800 B
- **Every second** — 3,600 polls/hr × 800 B = 2,880,000 B of headers, almost all of it answering "no news"
- **Every 10 s** — 360 polls/hr × 800 B = 288,000 B, and each update now shows up to 10 s late
- **The call** — one handshake (~800 B) up front, then ~6 B of frame header per pushed message
- **Hand-check** — 20 updates in the hour: 800 + 20 × 6 = 920 B, roughly 3,100× less than 2,880,000 B

*Example (italic):* The café board receives 20 real updates in an hour; per-second polling pays 2,880,000 bytes of headers to learn them, the open socket pays 920.

**Key point:** Polling cost scales with how often you ask; WebSocket cost scales with how much actually happens — for rare updates the difference is thousands-fold.

### Visualization (canvas `c2`, 720×300)

Horizontal bar chart: one hour of header/frame overhead for the same 20 order updates under three approaches; log-feel achieved by hardcoded pixel widths, not a real log axis.

- **Title (bold 15px, `#1a5276`, top center):** "Overhead for the Same 20 Updates in One Hour".
- **Axis:** vertical 2px `#999` baseline at x=250, bars extend right, max width 420; left-aligned 12px `#444` row labels at x=20.
- **Rows (bar centers at y = 90, 155, 220; bars 22px tall):**
  - "poll every 1 s — 3,600 × 800 B": orange `#d95926` bar width 420, 12px `#444` end label "2,880,000 B"
  - "poll every 10 s — 360 × 800 B": blue `#2a78d6` bar width 260, end label "288,000 B"
  - "WebSocket — 800 + 20 × 6 B": green `#008300` bar width 14, bold 12px green end label "920 B"
- **Bar style:** fills solid at 85% opacity, 1px darker stroke of the same hue.
- **Annotation (bold 13px magenta `#d55181`, right side near y=250):** "≈3,100× less overhead for the same news".
- **Caption (12px `#444`, bottom right):** "pixel widths schematic; byte counts illustrative but hand-checkable".

## Where Live Data Can't Wait

**Tags:** `where it's used` (blue), `latency` (green), `real time` (orange)

- **Chat** — a message must appear when it is sent, not at the next poll; sockets even carry "typing…"
- **Dashboards** — a live ops dashboard pushes each metric tick instead of re-asking every panel every second
- **Tickers** — a price feed changes many times a second; polling that fast hammers the server with asks
- **Staleness math** — with polling, an update waits on average half the interval: 10 s polls mean 5 s stale
- **Server side** — 10,000 pollers at 1/s is 10,000 requests/s of mostly "no"; 10,000 idle sockets are mostly silence

*Example (italic):* On a 10-second poll the price you see is on average 5 seconds old; on the open socket it is about 0.05 seconds old — the network trip itself.

**Key point:** Reach for WebSockets when updates are frequent or unpredictable and staleness hurts — chat, live dashboards, tickers, multiplayer state.

### Visualization (canvas `c3`, 720×300)

Vertical bar chart of average staleness — how old a just-displayed update is on average — for three poll intervals vs a WebSocket push; log-feel achieved by hardcoded pixel heights.

- **Title (bold 15px, `#1a5276`, top center):** "How Stale Is the Screen? Average Age of a Displayed Update".
- **Axes:** origin x=70, baseline y=245, plot width 590, plot height 180; no numeric y gridlines (heights schematic), 2px `#999` baseline.
- **Bars (55px wide, centered at x = `[150, 300, 450, 600]`, heights `[170, 110, 60, 10]` px):**
  - "poll 60 s" — blue `#2a78d6`, bold 12px `#2c3e50` value label "30 s" on top
  - "poll 10 s" — blue `#2a78d6`, value label "5 s"
  - "poll 1 s" — blue `#2a78d6`, value label "0.5 s"
  - "WebSocket" — green `#008300`, bold 12px green value label "~0.05 s"
- **X labels:** 12px `#444` under each bar at y=265.
- **Annotation (bold 13px aqua `#199e70`, arrow toward the WebSocket bar, near x=490, y=100):** "chat, tickers, dashboards live down here".
- **Caption (12px `#444`, bottom right):** "poll staleness = half the interval (exact); 0.05 s socket delay illustrative; bar heights schematic".

## An Open Line Can Still Drop

**Tags:** `common mistake` (red), `reconnection` (orange), `heartbeat` (green)

- **The drop** — Wi-Fi blips and proxies that time out idle lines can kill a socket without a goodbye
- **The silence trap** — a dead socket and a quiet socket look identical from either end: no messages
- **Ping/pong** — the protocol includes ping/pong frames; a ping unanswered for ~5 s means redial
- **Resync** — after reconnecting, fetch the current state once; missed messages do not replay themselves
- **The mistake** — shipping only the happy path: the board freezes at 3:12pm and shows stale orders forever

*Example (italic):* A router reboot at 3:12pm kills the café's socket; without a heartbeat the board keeps showing 3:11pm orders all afternoon and nobody notices.

**Common mistake:** Treating the socket as permanent. Real deployments need heartbeats, reconnect-with-backoff, and a one-time state resync after redial — otherwise a network blip becomes a silently frozen screen.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: a socket drop with no heartbeat (screen silently freezes) vs with ping/pong plus redial (brief gap, then recovery), shown as boxes flowing left to right.

- **Title (bold 15px, `#1a5276`, top center):** "A Dead Line Sounds Exactly Like a Quiet Line".
- **Row 1 (boxes centered on y=95), label 12px `#444` at x=20:** "no heartbeat"; blue `#2a78d6` rounded box at x=160 labeled "socket open, board live" (12px), 3px arrow to a red `#e74c3c` box at x=400 labeled "Wi-Fi blip 3:12pm — socket dead", 3px arrow to a red box at x=610 labeled "board frozen, looks fine" with bold 12px red "✗ stale forever".
- **Row 2 (boxes centered on y=205), label:** "ping/pong + redial"; blue box "socket open, board live", 3px arrow to a yellow `#c98500` box at x=380 labeled "ping unanswered 5 s", 3px arrow to a green `#008300` box at x=590 labeled "reconnect + resync orders" with bold 12px green "✓ live again".
- **Box style:** 150–180px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(231,76,60,0.12)` / `rgba(201,133,0,0.15)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px orange `#d95926`, centered near y=270):** "heartbeats are how you tell a dead line from a quiet one".
- **Caption (12px `#444`, bottom right):** "timings illustrative; ping/pong frames are part of the WebSocket protocol".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); the 800 B per poll (500 request + 300 response), 20 updates/hr, and 0.05 s push delay are invented and labeled illustrative; the arithmetic on them is exact (3,600 × 800 = 2,880,000 B; 360 × 800 = 288,000 B; 800 + 20 × 6 = 920 B; average poll staleness = half the interval: 30 / 5 / 0.5 s); the `Upgrade: websocket` → `101 Switching Protocols` handshake and ping/pong frames are documented protocol facts.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
