# Polling, SSE, WebSockets

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Polling, SSE, WebSockets

**Subtitle:** Four ways for a browser to learn something new from a server — from asking over and over to keeping an open two-way line — trading server load against how fresh the news is

## Is My Latte Ready? Four Ways to Find Out

**Tags:** `core idea` (blue), `realtime spectrum` (green), `client vs server` (orange)

- **The wait** — a coffee shop app shows "preparing..." while your latte is made; the phone must learn when it's done
- **Polling** — the app asks "ready yet?" every 10 seconds; the counter answers "no" until finally "yes"
- **Long-polling** — the app asks once and the counter holds the question open, answering the moment there's news
- **SSE** — the app opens a one-way announcement channel; the counter pushes "ready!" whenever it happens
- **WebSocket** — the app and counter open a phone line; either side can speak at any time ("ready!" / "make it oat milk")

*Example (italic):* Your latte finishes at 2:04:07pm — the 10-second poller learns at 2:04:10, but SSE and WebSocket learn at 2:04:07.

**Key point:** All four solve the same problem — the server knows something before the client asks — and differ only in who initiates each message and how long the connection stays open.

### Visualization (canvas `c1`, 720×300)

Four-row message-flow diagram: client column on the left, server column on the right, one row per technique showing who sends arrows and when.

- **Title (bold 15px, `#1a5276`, top center):** "Who Speaks, and When: the Same 'Latte Ready' News Four Ways".
- **Layout:** vertical 2px `#6b7280` client line at x=150 and server line at x=590, 12px `#444` labels "client" and "server" at y=48 above each; four rows at y = 80, 135, 190, 245, each with a left-aligned 12px `#444` row label at x=20: "polling", "long-poll", "SSE", "WebSocket".
- **Polling row (y=80):** five 2px blue `#2a78d6` right arrows client→server at x-midpoints spread evenly, each with a short gray `#6b7280` return arrow labeled 11px "no" beneath the first four; the fifth return arrow green `#008300` labeled bold 11px "ready!".
- **Long-poll row (y=135):** one 2px blue right arrow at the row start, then a dashed `#6b7280` (dash 4/3) horizontal hold line labeled 11px "held open...", ending in one green return arrow labeled bold 11px "ready!".
- **SSE row (y=190):** one 2px blue right arrow labeled 11px "subscribe", then two green left arrows server→client labeled 11px "brewing" and bold 11px "ready!".
- **WebSocket row (y=245):** alternating 2px arrows both directions — blue right "oat milk pls", green left "ok", green left bold "ready!" — 11px labels above each.
- **Annotation (bold 13px violet `#4a3aa7`, right side near y=60):** "below the polling row, the news is pushed — the server speaks without being asked".
- **Caption (12px `#444`, bottom right):** "message timing illustrative".

## 600 Waiting Customers: Counting the Requests

**Tags:** `worked example` (blue), `requests per second` (green), `staleness` (orange)

- **The setup** — 600 customers wait at once; each latte takes 4 minutes; each app polls every 10 seconds
- **Polls per order** — 4 minutes / 10s = 24 polls, and 23 of them come back "not ready" — wasted trips
- **Server load** — 600 customers / 10s interval = 60 requests per second, around the clock
- **Staleness** — news lands anywhere in the 10s gap, so the average answer is 5 seconds old
- **Long-poll** — with a 60s hold limit, a 4-minute order costs 4 timeouts + 1 answer = 5 requests, news instant
- **Push** — SSE and WebSocket send exactly 1 "ready" message per order, with ~0s staleness

*Example (italic):* Cutting the poll interval from 10s to 2s drops average staleness from 5s to 1s but raises load from 60 to 300 requests per second — freshness is bought with requests.

**Key point:** Polling cost = clients / interval, and average staleness = interval / 2 — you can check both by hand, and no interval makes both small at once; push makes both small by sending only real news.

### Visualization (canvas `c2`, 720×300)

Horizontal bar chart: requests needed to deliver one order's "ready" news under each technique, wasted requests shaded separately, staleness noted at bar ends.

- **Title (bold 15px, `#1a5276`, top center):** "One 4-Minute Latte: Requests per Order and How Stale the News Is".
- **Axis:** vertical 2px `#999` baseline at x=230, bars extend right, scale 18px per request, max width 440; rows at y = 70, 120, 170, 220, each with a left-aligned 12px `#444` label at x=20.
- **Rows (top to bottom):**
  - "polling 10s — 24 req": orange `#d95926` segment width 414 (23 wasted "no" polls) + green `#008300` segment width 18 (the 1 useful poll), 11px `#444` end label "avg 5s stale"
  - "long-poll 60s — 5 req": blue `#2a78d6` bar width 90, end label "~0s stale"
  - "SSE — 1 msg": green bar width 18, end label "~0s stale"
  - "WebSocket — 1 msg": green bar width 18, end label "~0s stale"
- **Bar style:** 16px tall, wasted segment fill `rgba(217,89,38,0.35)` with 1px `#d95926` border, useful/push bars solid; 11px request counts just past each bar end before the staleness label.
- **Annotation (bold 13px magenta `#d55181`, near x=340, y=95):** "23 of 24 polls come back 'not ready'".
- **Caption (12px `#444`, bottom right):** "600 customers, 4-min orders, counts exact for the stated setup; setup illustrative".

## Dashboards, Notifications, Chat: One Spectrum, Three Tools

**Tags:** `where it's used` (blue), `choosing the tool` (green)

- **The question** — pick by asking two things: how fresh must the news be, and does the client talk back?
- **Dashboards** — a metrics page refreshed every minute is fine with plain polling; simple and cache-friendly
- **Notifications** — order status, live scores, price tickers flow one way: SSE fits, and it auto-reconnects
- **Chat** — messages go both directions with no warning; that is the WebSocket case
- **One-way limit** — SSE only flows server→client; the moment the client must push mid-stream, you've left SSE territory
- **Plumbing cost** — moving right on the spectrum adds held-open connections, reconnect logic, and stateful servers

*Example (italic):* The coffee shop's "ready" board is SSE; the barista chat where you change your order mid-brew is a WebSocket; the manager's daily sales page just polls.

**Key point:** Match the tool to the freshness and direction the feature actually needs — the three techniques are points on one spectrum, not ranks from worst to best.

### Visualization (canvas `c3`, 720×300)

Spectrum diagram: a left-to-right freshness axis with three colored zones, each zone naming its technique and typical uses.

- **Title (bold 15px, `#1a5276`, top center):** "The Realtime Spectrum: Freshness Needed Picks the Tool".
- **Axis:** 2px `#6b7280` horizontal arrow from x=60 to x=660 at y=70, 12px `#444` labels "minutes-old is fine" at the left end and "instant, two-way" at the right end, bold 12px `#6b7280` "fresher →" centered above.
- **Zones (rounded rects 12px radius, y=95 to y=200, 11px gaps):**
  - x=60 to x=250, fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border, bold 13px `#2a78d6` "POLLING" centered near the top, 12px `#2c3e50` lines beneath: "daily dashboard", "settings sync"
  - x=261 to x=451, fill `rgba(25,158,112,0.15)`, 2px `#199e70` border, bold 13px `#199e70` "SSE (one-way push)", lines: "order status", "live scores", "price ticker"
  - x=462 to x=660, fill `rgba(74,58,167,0.12)`, 2px `#4a3aa7` border, bold 13px `#4a3aa7` "WEBSOCKET", lines: "chat", "multiplayer game", "collaborative editing"
- **Long-poll marker:** dashed `#6b7280` (dash 4/3) vertical tick at x=255 between the first two zones, 11px `#6b7280` label "long-poll: the bridge" below at y=225.
- **Annotation (bold 13px orange `#d95926`, centered near y=255):** "each step right adds held connections and reconnect plumbing".
- **Caption (12px `#444`, bottom right):** "zone placement schematic".

## The Upgrade Trap: WebSockets Everywhere

**Tags:** `common mistake` (red), `over-engineering` (orange)

- **The reflex** — "WebSockets are the newest, so use them for everything" treats the spectrum as a ranking
- **Idle sockets** — a dashboard that changes once a minute over WebSockets holds 10,000 open, silent connections
- **The poll math** — 10,000 viewers polling every 60s is 10,000 / 60 ≈ 167 requests/sec of stateless, cacheable traffic
- **Hidden work** — held sockets need heartbeats, reconnect-with-backoff, and load balancers that pin clients to servers
- **Same staleness** — the data itself refreshes every 60s, so the socket's instant delivery buys nothing here
- **The reverse trap** — polling a chat app every second is the same mistake mirrored: wrong point on the spectrum

*Example (italic):* The 1-minute dashboard gets zero freshness benefit from WebSockets — the 60s data refresh, not the transport, sets how stale the numbers are.

**Common mistake:** Choosing the transport by novelty instead of by the feature's freshness and direction needs — an idle WebSocket is pure cost, and a hot polling loop is pure waste.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: the same 1-minute dashboard served two ways — 10,000 held-open WebSockets vs plain 60s polling — with the cost of each in a box.

- **Title (bold 15px, `#1a5276`, top center):** "A Dashboard That Updates Once a Minute, Served Two Ways".
- **Row 1 (y=95), label 12px `#444` at x=20:** "WebSockets"; blue `#2a78d6` rounded box at x=170 labeled "10,000 viewers" (12px), 3px arrow to an orange `#d95926` box at x=400 labeled "10,000 sockets held open, ~1 msg/min each" with bold 12px orange "heartbeats + sticky routing" beneath.
- **Row 2 (y=205), label:** "polling 60s"; blue box at x=170 "10,000 viewers", 3px arrow to a green `#008300` box at x=400 labeled "≈167 req/s, stateless, cacheable" with bold 12px green "same 60s freshness" beneath.
- **Box style:** 150–230px wide, 44px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(217,89,38,0.15)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px magenta `#d55181`, centered near y=270):** "the data refresh, not the transport, sets the staleness here".
- **Caption (12px `#444`, bottom right):** "viewer count illustrative; 167 req/s = 10,000 / 60, exact for that count".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded values above (no randomness); the coffee-shop setup (600 customers, 4-minute orders, 10s poll interval) is invented and labeled illustrative, but the derived counts are exact for it: 24 polls per order (23 wasted), 60 req/s = 600/10, avg staleness 5s = 10/2, long-poll 5 requests (4 sixty-second holds + 1 answer), SSE/WebSocket 1 message; the dashboard case uses 10,000 viewers (illustrative) with 167 req/s = 10,000/60 exact for that count.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
