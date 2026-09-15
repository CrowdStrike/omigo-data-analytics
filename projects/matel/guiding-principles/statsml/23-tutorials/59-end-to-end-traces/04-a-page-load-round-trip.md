# A Page Load, Round Trip

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** A Page Load, Round Trip

**Subtitle:** One page visit split into four legs — the client prepares and addresses the request, the network secures and carries it, the server side answers from edge or application, and the browser turns the reply into pixels

## Client Side: Everything Before the First Byte Leaves

**Tags:** `client side` (blue), `dns` (green), `cookies` (orange)

- **The open** — launching the browser spins up several processes: UI, network, and one sandboxed per tab
- **The typing** — Alice types `shop.example.com/deals`; the address bar decides it is a URL, not a search
- **The parse** — scheme `https`, host `shop.example.com`, path `/deals` — the scheme demands encryption
- **The lookup** — DNS: browser cache → OS cache → resolver → root → `.com` → authoritative → `203.0.113.7`
- **The cookie jar** — saved cookies for this site are found, including the session cookie from yesterday's login
- **The request** — a GET for `/deals` is written with headers and those cookies — before any network use

*Example (italic):* Before one byte leaves the laptop, the browser has parsed the URL, resolved the name to 203.0.113.7, and chosen which cookies ride along.

**Key point:** The whole first leg happens on Alice's machine — the request is fully addressed and carries her identity (cookies) before the network is ever touched.

### Visualization (canvas `c1`, 720×300)

Top row of four client-side stage boxes, with a dashed zoom panel below expanding the DNS step into its six-hop lookup chain.

- **Title (bold 15px, `#1a5276`, top center):** "Before Any Byte Leaves the Laptop".
- **Row 1 (box centers at y=70), four boxes left to right at x = 105, 275, 445, 615:** "browser opens / UI + net + tab", "URL parsed / https · host · path", "DNS: / name → IP", "GET written / cookies attached" — blue `#2a78d6` rounded boxes 150×44, fill `rgba(42,120,214,0.15)`, 3px `#2a78d6` arrows between them, 12px `#2c3e50` two-line centered text.
- **Zoom connector:** dashed 2px `#6b7280` line from the bottom of the "DNS" box (445, 92) down to the top edge of the panel.
- **Zoom panel:** dashed 2px `#6b7280` rounded rect at x=60, y=118, w=600, h=112; 12px `#6b7280` label "inside the DNS step" at top-left (x=72, y=136).
- **DNS chain (box centers at y=190), six boxes at x = 110, 205, 300, 395, 490, 585:** "browser cache ✗", "OS cache ✗", "resolver", "root", ".com TLD", "authoritative" — 85×34 rounded boxes, fill `rgba(74,58,167,0.10)`, border violet `#4a3aa7`, 11px `#2c3e50` text (two lines allowed), 2px `#4a3aa7` arrows between.
- **Annotation (bold 13px green `#008300`, centered at y=258):** "answer: 203.0.113.7 — cached so the next visit skips all six hops".
- **Caption (12px `#444`, bottom right y=292):** "illustrative flow; both caches shown missing".

## Transit: Two Handshakes, Then a Locked Envelope

**Tags:** `transit` (blue), `https` (green), `handshakes` (orange)

- **TCP first** — SYN, SYN-ACK, ACK: three messages confirm both ends are listening; a reliable pipe exists
- **TLS second** — the server presents a certificate proving it is `shop.example.com`; both agree secret keys
- **HTTPS = HTTP + TLS** — the padlock means the request now travels inside that encrypted channel
- **Plain HTTP leaks** — without TLS, every router on the path can read the URL, headers, and cookies
- **The send** — the GET, its headers, and the session cookie cross ~12 router hops as unreadable ciphertext

*Example (italic):* A wiretap on any hop sees that Alice's laptop talked to the shop's address — but the path `/deals` and her session cookie are sealed inside.

**Key point:** HTTPS hides what the request says, not that it happened — the destination stays visible, the contents and cookies do not; that is the entire difference between http:// and https://.

### Visualization (canvas `c2`, 720×300)

Sequence ladder: two vertical lifelines (browser left, edge server right) exchanging arrows top to bottom — three TCP messages, two TLS messages, a dashed "encrypted from here down" divider, then the sealed GET and reply.

- **Title (bold 15px, `#1a5276`, top center):** "Two Handshakes, Then the Locked Envelope".
- **Lifelines:** header labels bold 13px `#2c3e50` at y=52 — "Alice's browser" centered at x=170, "shop.example.com edge" centered at x=550; vertical 2px `#6b7280` lines at x=170 and x=550 from y=62 to y=272.
- **Stage labels (bold 12px, left margin x=12, textAlign left):** "TCP" `#2a78d6` at y=119, "TLS" `#4a3aa7` at y=179, "HTTP" `#008300` at y=249.
- **Arrows (3px, label 12px above the midpoint of each):**
  - y=95 → "SYN", blue `#2a78d6`
  - y=115 ← "SYN-ACK", blue
  - y=135 → "ACK", blue
  - y=165 → "hello, let's encrypt this", violet `#4a3aa7`
  - y=185 ← "certificate + key agreement", violet
  - y=235 → "GET /deals + headers + cookies (sealed)", green `#008300`
  - y=260 ← "HTML response (sealed)", green
- **Divider:** dashed 2px green `#008300` horizontal line from x=110 to x=610 at y=207; bold 12px green label "everything below is encrypted" centered at y=202.
- **Caption (12px `#444`, bottom right y=292):** "message order exact, timing not to scale".

## Server Side: One IP, a Whole Chain Behind It

**Tags:** `server side` (blue), `cdn` (green), `application` (orange)

- **Not the shop's machine** — `203.0.113.7` is a CDN edge near Alice, one of thousands placed worldwide
- **Hit or miss** — static files answer straight from edge cache; `/deals` is personalized, so it goes to origin
- **Load balancer** — the origin's front door picks one healthy application server out of the pool
- **The application** — code reads the session cookie, loads Alice's session, queries the database for deals
- **The response** — HTML built for Alice, plus `Set-Cookie` (refresh session) and `Cache-Control` (cache rules)

*Example (italic):* No password traveled — the session cookie alone turned an anonymous GET into "Alice's deals page" on the application server.

**Key point:** "The server" is really a chain — edge, balancer, application, database — and the cookie is the only link between this request and Alice's login yesterday.

### Visualization (canvas `c3`, 720×300)

Left-to-right flow: the sealed GET arrives at a CDN edge, branches up to a green cache-hit exit or continues right through load balancer, application server, and database, with the response returning along a dashed bottom path.

- **Title (bold 15px, `#1a5276`, top center):** "Behind One IP: Edge, Balancer, Application, Database".
- **Annotation (bold 13px violet `#4a3aa7`, centered at y=48):** "the browser sees one answer — never the chain that built it".
- **Main row (box centers at y=215):** "CDN edge" at x=110, "load balancer" at x=280, "app server / reads cookie" at x=450, "database" at x=620 — rounded boxes 120×50, blue `#2a78d6` border, fill `rgba(42,120,214,0.15)`, 12px `#2c3e50` centered text (two lines allowed), 3px blue arrows between; the edge→balancer arrow labeled "MISS / personalized" (11px `#6b7280`, above the arrow).
- **Incoming arrow:** 3px green `#008300` arrow from x=15 to the left edge of the CDN edge box at y=215, 11px green label "GET /deals (sealed)" above it.
- **Hit branch:** 3px green arrow from the top of the CDN edge box (110, 190) up to a green box "cache HIT: / answered at the edge" centered at (300, 105), 170×44, border `#008300`, fill `rgba(0,131,0,0.12)`; 11px `#6b7280` note "static files end here" just below the box (y=140).
- **Return path:** dashed 3px green arrow from below the app server box (450, 245) running along y=272 back to x=20; bold 12px green label "HTML + Set-Cookie + Cache-Control" centered above it at (300, 265).
- **Caption (12px `#444`, bottom right y=292):** "one of many origin layouts".

## Back to the Client: From HTML to Pixels

**Tags:** `back to client` (blue), `rendering` (green), `caching` (orange)

- **Parse** — the HTML streams in and becomes the DOM, a tree of every element on the page
- **Discover** — the parser finds CSS, JS, and images; each starts its own fetch, mostly from CDN edges
- **Style and layout** — CSS becomes the CSSOM; DOM + CSSOM together fix every box's size and position
- **Paint** — boxes become pixels and are composited to the screen; Alice finally sees the deals
- **Remember** — new cookies and cacheable files are stored, so the next visit skips lookups and downloads

*Example (italic):* One typed URL quietly became ~30 requests — the HTML plus every stylesheet, script, and image it named, each doing its own small round trip.

**Key point:** The round trip repeats in miniature for every subresource — a "page load" is really dozens of client → transit → server → client loops finishing at different times.

### Visualization (canvas `c4`, 720×300)

Render pipeline flow on top (HTML and CSS merging into layout, paint, pixels), with a five-bar subresource fetch waterfall below showing the fan-out after the HTML arrives.

- **Title (bold 15px, `#1a5276`, top center):** "One Reply Fans Out, Then Becomes Pixels".
- **Pipeline (top):** "HTML → DOM" box centered at (120, 62) and "CSS → CSSOM" box at (120, 112) — 130×36 rounded, blue `#2a78d6`; both feed 2px blue arrows into "layout" at (300, 87), then 3px arrows to "paint" at (440, 87), then "pixels on screen" at (600, 87) — layout/paint 100×40 blue, pixels 130×40 green `#008300` fill `rgba(0,131,0,0.12)`.
- **Waterfall (bottom, scale x = 110 + ms × 5.5, i.e. 0–100ms spans x=110–660):** bars 16px tall, 11px stage name 12px `#444` at x=8 left of each bar, hardcoded starts/durations:
  - "/deals HTML": y=155, start 0ms, 40ms, blue `#2a78d6` (width 220)
  - "style.css": y=176, start 45ms, 25ms, aqua `#199e70` (width 138)
  - "app.js": y=197, start 45ms, 35ms, violet `#4a3aa7` (width 193)
  - "hero.jpg": y=218, start 50ms, 45ms, orange `#d95926` (width 248)
  - "logo.png": y=239, start 50ms, 25ms, magenta `#d55181` (width 138)
- **Axis:** 2px `#999` baseline at y=262 from x=110 to x=660; 12px `#444` tick labels "0" / "50" / "100ms" at x = 110, 385, 660 (y=278), gridlines `#e5e9ef` from y=150 to y=262.
- **Annotation (bold 12px green `#008300`, left-aligned at x=340, y=147):** "the HTML names them — each does its own mini round trip".
- **Caption (12px `#444`, bottom right y=292):** "timings illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms). Shared `roundedRect` and `arrowHead` helpers as in the other pages of this folder.
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all numbers are hardcoded literals (no randomness); the IP `203.0.113.7` is from the documentation range; hop count (~12), request count (~30), and the c4 waterfall timings (40/25/35/45/25ms with starts 0/45/45/50/50) are invented and labeled illustrative; c4 pixel widths use the fixed scale 5.5px = 1ms (220/138/193/248/138).
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
