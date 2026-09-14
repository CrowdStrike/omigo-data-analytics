# HTTP Headers

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** HTTP Headers

**Subtitle:** Every web message opens with short key: value notes about itself — headers are that metadata conversation

## What Rides Along With a Menu Request

**Tags:** `core idea` (blue), `metadata` (green), `request/response` (orange)

- **The app** — a coffee shop's phone app asks `api.cafe-orders.example` for today's menu
- **The body** — the menu JSON is the actual message; everything about the exchange rides in headers
- **Request headers** — `Host`, `Accept: application/json`, `User-Agent`, `Authorization` go out with the ask
- **Response headers** — `Content-Type`, `Content-Length: 2148`, `Cache-Control: max-age=300` come back
- **The definition** — headers are key: value lines sent before the body, read by machines, unseen by users

*Example (italic):* The 2,148-byte menu is the payload; the seven header lines around it tell both sides how to send, parse, cache, and trust it.

**Key point:** Headers are the metadata conversation — machine-to-machine notes about the message, exchanged before the message itself.

### Visualization (canvas `c1`, 720×300)

Flow diagram of one request/response exchange: app box on the left, server box on the right, a request arrow on top carrying a request-header list, a response arrow below carrying a response-header list plus the body.

- **Title (bold 15px, `#1a5276`, top center):** "One Menu Request: the Headers Around a 2,148-Byte Body".
- **Endpoints:** blue `#2a78d6` rounded box at x=30, y=130 (110×46, 8px radius, fill `rgba(42,120,214,0.15)`) labeled "CafeApp 2.1" (12px `#2c3e50`); matching box at x=580, y=130 labeled "api.cafe-orders .example" (two 12px lines).
- **Request arrow (top):** 3px `#2a78d6` arrow left-to-right at y=118 from x=150 to x=570; above it a header card at x=190, y=34 (300×76, fill `rgba(42,120,214,0.08)`, 1px `#e5e9ef` border) listing 11px monospace `#2c3e50` lines: "GET /menu", "Host: api.cafe-orders.example", "Accept: application/json", "User-Agent: CafeApp/2.1 (Android)", "Authorization: Bearer <token>".
- **Response arrow (bottom):** 3px `#008300` arrow right-to-left at y=200 from x=570 to x=150; below it a header card at x=190, y=210 (300×62, fill `rgba(0,131,0,0.08)`) listing "200 OK", "Content-Type: application/json", "Content-Length: 2148", "Cache-Control: max-age=300"; a small aqua `#199e70` box at x=510, y=210 (90×30) labeled "body: menu JSON" (11px).
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=290):** "7 header lines around 1 body — the metadata outnumbers the message".
- **Caption (12px `#444`, bottom right):** "byte count illustrative".

## Same URL, Four Different Conversations

**Tags:** `worked example` (blue), `content negotiation` (green), `Content-Type` (orange)

- **Accept** — the app sends `Accept: application/json`; a browser sends `Accept: text/html` to the same URL
- **The server picks** — one `/menu` endpoint answers JSON (2,148 B), HTML (5,120 B), or CSV (890 B) to match
- **Content-Type** — the response header names what actually came back, so the client parses it correctly
- **User-Agent** — `CafeApp/2.1 (Android)` tells the server which client software is asking
- **Authorization** — the `Bearer` token header proves who is asking before order history is returned

*Example (italic):* Three clients hit the same `/menu` URL; Accept headers of json, html, and csv get bodies of 2,148, 5,120, and 890 bytes respectively.

**Key point:** Content negotiation is headers doing the talking — the URL names the resource, and the Accept/Content-Type pair agrees on its format.

### Visualization (canvas `c2`, 720×300)

Vertical bar chart: three requests to the same `/menu` URL, one bar per Accept header, bar height = response size in bytes, each bar labeled with the matching response Content-Type.

- **Title (bold 15px, `#1a5276`, top center):** "One /menu URL, Three Accept Headers, Three Response Sizes".
- **Axes:** origin x=70, baseline y=245, plot width 590, plot height 185; y = bytes 0 to 6,000, gridlines `#e5e9ef` at 2,000 and 4,000 with 12px `#444` labels "2,000 B" / "4,000 B".
- **Bars (110px wide, centered at x = 180, 380, 580):** heights from data `[2148, 5120, 890]` bytes; fills blue `rgba(42,120,214,0.35)` with 2px `#2a78d6` edge, orange `rgba(217,89,38,0.30)` with 2px `#d95926` edge, aqua `rgba(25,158,112,0.30)` with 2px `#199e70` edge.
- **Labels:** under each bar, 12px `#444` Accept value ("application/json", "text/html", "text/csv"); above each bar, bold 12px matching-color byte count ("2,148 B", "5,120 B", "890 B"); inside the baseline, 11px `#6b7280` response line "Content-Type: <same value>".
- **Annotation (bold 13px violet `#4a3aa7`, near x=300, y=60):** "the URL never changed — only the Accept header did".
- **Caption (12px `#444`, bottom right):** "byte sizes illustrative".

## Reading the Traffic Without Opening a Single Body

**Tags:** `where it's used` (blue), `APIs` (green), `debugging` (orange)

- **APIs** — every call sets `Authorization` and `Content-Type`; a wrong header means a 401 or 415 error
- **Debugging** — a 401 vs 415 vs 406 status almost always traces back to one missing or wrong header
- **Analytics** — `User-Agent` alone splits a day's 10,000 menu requests: 6,100 mobile, 3,200 desktop, 700 bots
- **Caching** — `Cache-Control: max-age=300` lets five minutes of repeat requests skip the server entirely
- **Tracing** — a request-id header lets one slow order be followed across every service it touched

*Example (italic):* The cafe's team spots a scraper without reading any body: 700 requests share a script User-Agent and send no Accept-Language at all.

**Key point:** Whole categories of API work — auth failures, format errors, caching, traffic analytics — happen in headers before anyone reads a body.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart: one day of 10,000 requests to `/menu` classified purely from the User-Agent header.

- **Title (bold 15px, `#1a5276`, top center):** "10,000 Menu Requests Classified by User-Agent Alone".
- **Axis:** vertical 2px `#999` baseline at x=200, bars extend right, max width 460 mapped to 6,100 requests; light `#e5e9ef` gridlines at 2,000 / 4,000 / 6,000 with 11px `#6b7280` labels along the bottom.
- **Rows (bar centers at y = 90, 155, 220), each with a right-aligned 12px `#444` label at x=190:**
  - "mobile app": blue bar width 460 from data 6,100, fill `rgba(42,120,214,0.35)` with 2px `#2a78d6` edge, bold 12px `#2a78d6` value "6,100" at bar end
  - "desktop browser": aqua bar width 241 from data 3,200, fill `rgba(25,158,112,0.30)` with 2px `#199e70` edge, value "3,200"
  - "bot / script": orange bar width 53 from data 700, fill `rgba(217,89,38,0.30)` with 2px `#d95926` edge, value "700"
- **Bar style:** 30px tall, values placed 8px right of each bar end.
- **Annotation (bold 13px magenta `#d55181`, near x=380, y=255):** "one header line, a full traffic breakdown — no bodies opened".
- **Caption (12px `#444`, bottom right):** "request counts illustrative".

## A Header Is Not a Secret

**Tags:** `common mistake` (red), `security` (orange), `HTTPS` (blue)

- **The feeling** — users never see headers, so developers quietly assume nobody else does either
- **The reality** — over plain HTTP, every header line crosses the network as readable text
- **The token** — an `Authorization: Bearer` header sent over HTTP is a password on a postcard
- **The fix** — HTTPS encrypts headers and body together; only then are header contents private in transit
- **Still local** — even with HTTPS, the sending client can read every header in its own dev tools

*Example (italic):* The same Bearer token that unlocks the cafe's order history rides across coffee-shop Wi-Fi readable by anyone until the app switches to HTTPS.

**Common mistake:** Confusing "invisible to the user" with "encrypted." Headers are metadata, not secrets — HTTPS, not the header mechanism itself, is what hides them in transit.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: the same Authorization header crossing public Wi-Fi over HTTP (readable, red) vs over HTTPS (encrypted, green), with an eavesdropper marker between client and server.

- **Title (bold 15px, `#1a5276`, top center):** "The Same Token on the Wire: HTTP vs HTTPS".
- **Row 1 (boxes centered at y=100), label 12px `#444` at x=20:** "HTTP"; blue `#2a78d6` rounded box at x=90 (130×40, fill `rgba(42,120,214,0.15)`) labeled "app sends token" (12px), 3px arrow to a red `#e74c3c` box at x=290 (200×40, fill `rgba(231,76,60,0.12)`) labeled "Authorization: Bearer <token>" (11px monospace), 3px arrow to a blue box at x=560 (120×40) labeled "server"; below the red box, bold 12px red "✗ readable by anyone on the Wi-Fi".
- **Row 2 (boxes centered at y=210), label:** "HTTPS"; identical app box at x=90, 3px arrow to a green `#008300` box at x=290 (200×40, fill `rgba(0,131,0,0.12)`) labeled "◼◼◼ encrypted ◼◼◼" (11px), arrow to the server box at x=560; below the green box, bold 12px green "✓ headers and body sealed together".
- **Eavesdropper marker:** 12px `#6b7280` label "public Wi-Fi" with a vertical dashed `#6b7280` (dash 4/3) line at x=390 spanning y=60 to y=250.
- **Annotation (bold 13px orange `#d95926`, centered near y=280):** "headers hide nothing by themselves — encryption is a separate layer".
- **Box style:** 8px radius, 12px `#2c3e50` text unless colored above.

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded values above (no randomness); response body sizes `[2148, 5120, 890]` bytes, the 2,148-byte menu body, the `max-age=300` cache window, and the 10,000-request User-Agent split `[6100, 3200, 700]` are invented and labeled illustrative; the domain `api.cafe-orders.example` and the `<token>` stand-in are placeholders.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
- Page footnote (italic 12px, muted `#6b7280`, after the last card-section): "Note: realistic credential strings on this page were converted to generic placeholders — for illustration only, and to avoid false positives from secret scanners."
