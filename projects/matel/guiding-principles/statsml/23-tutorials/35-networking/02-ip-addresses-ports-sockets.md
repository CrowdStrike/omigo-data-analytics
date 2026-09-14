# IP Addresses, Ports, Sockets

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** IP Addresses, Ports, Sockets

**Subtitle:** An IP address is a building's street address, a port is the apartment number, and a socket is the open door where one conversation actually happens

## One Street Address, Many Apartment Doors

**Tags:** `core idea` (blue), `IP = building` (green), `port = apartment` (orange)

- **The building** — a server is an apartment building sitting at one street address on the network
- **The IP address** — the street address, `203.0.113.5`; it gets a message to the right machine
- **The port** — the apartment number; apt 25 is mail, apt 443 is the web, apt 5432 is the database
- **The socket** — the open door: one live conversation between a visitor and one apartment
- **The pair** — every delivery needs both; the address alone finds the building, never the resident

*Example (italic):* A courier from `198.51.100.7` walks to building `203.0.113.5` and knocks on apartment 443 — the web service opens its door, and that open door is a socket.

**Key point:** An IP address names a machine, a port names one program on that machine, and a socket is one live connection between two address:port pairs.

### Visualization (canvas `c1`, 720×300)

Diagram of one apartment building (the server) with three numbered apartment doors (ports), and a courier (the client) whose arrow to apartment 443 is the socket.

- **Title (bold 15px, `#1a5276`, top center):** "IP Finds the Building, the Port Finds the Door".
- **Building:** rounded rect x=430, y=55, width 250, height 210, fill `rgba(42,120,214,0.10)`, 2px `#2a78d6` border; bold 13px `#1a5276` label centered inside at y=75: "203.0.113.5 — street address (IP)".
- **Apartment boxes (inside building):** three white rounded boxes x=455, width 200, height 42, 1.5px `#6b7280` border, 12px `#2c3e50` centered labels, at y = 92 "Apt 25 — mail", y = 145 "Apt 443 — web", y = 198 "Apt 5432 — database"; the Apt 443 box gets a 2.5px green `#008300` border instead (the open door).
- **Courier box:** rounded rect x=40, y=145, width 195, height 42, fill `rgba(201,133,0,0.15)`, 1.5px `#c98500` border, 12px `#2c3e50` label "courier from 198.51.100.7".
- **Socket arrow:** 3px aqua `#199e70` line from (235, 166) to (455, 166) with a filled arrowhead at the right end; bold 12px green `#008300` label "socket = this open door" centered above it at y=150.
- **Annotation (bold 13px green `#008300`, centered near y=282):** "IP → building, port → door, socket → the conversation".
- **Caption (12px `#444`, bottom right):** "addresses illustrative (documentation ranges)".

## Three Visitors Knock on Apartment 443

**Tags:** `worked example` (blue), `4-tuple` (green)

- **The server** — one machine, `203.0.113.5`, with the web service listening on port 443
- **Visitor 1** — a laptop at `198.51.100.7` calls out from its own side door, port 51000
- **Visitor 2** — the same laptop opens a second tab; it uses a fresh side door, port 51001
- **Visitor 3** — a different laptop, `192.0.2.9`, connects out from its port 52344
- **Hand-check** — all 3 sockets share server port 443, yet each 4-tuple of two IPs + two ports differs
- **The range** — ports are 16-bit numbers, 0 to 65535, so every building has 65536 possible doors

*Example (italic):* The three sockets are (198.51.100.7:51000 ↔ 203.0.113.5:443), (198.51.100.7:51001 ↔ 203.0.113.5:443), and (192.0.2.9:52344 ↔ 203.0.113.5:443) — no two identical.

**Key point:** A socket is identified by four numbers — client IP, client port, server IP, server port — so one listening port can hold thousands of simultaneous conversations.

### Visualization (canvas `c2`, 720×300)

Flow diagram: three client boxes on the left, one server box on the right, three arrows converging on the same server port — each arrow one socket.

- **Title (bold 15px, `#1a5276`, top center):** "Three Sockets, One Server Port: the 4-Tuple Tells Them Apart".
- **Client boxes (left, x=40, width 205, height 40, 8px radius, 12px `#2c3e50` centered labels):** y=70 "198.51.100.7 : 51000" fill `rgba(42,120,214,0.15)` border 1.5px `#2a78d6`; y=140 "198.51.100.7 : 51001" fill `rgba(25,158,112,0.15)` border 1.5px `#199e70`; y=210 "192.0.2.9 : 52344" fill `rgba(74,58,167,0.12)` border 1.5px `#4a3aa7`.
- **Server box (right):** x=520, y=115, width 175, height 90, 8px radius, fill `rgba(0,131,0,0.12)`, 2px `#008300` border, bold 13px `#008300` centered label "203.0.113.5 : 443" with 12px `#2c3e50` sub-label "one listening port".
- **Arrows:** 3px lines with arrowheads from each client box's right edge to the server box's left edge (converging at y = 135, 160, 185): colors `#2a78d6`, `#199e70`, `#4a3aa7` top to bottom; 11px matching-color labels "socket #1", "socket #2", "socket #3" above each arrow's midpoint.
- **Annotation (bold 13px magenta `#d55181`, centered near y=272):** "same port 443 three times — the client side makes each socket unique".
- **Caption (12px `#444`, bottom right):** "client ports illustrative".

## Every host:port a Data Scientist Types

**Tags:** `where it's used` (blue), `debugging` (green), `refused vs timeout` (orange)

- **The database** — `psql -h 203.0.113.5 -p 5432` literally says "building 203.0.113.5, apartment 5432"
- **The notebook** — Jupyter's `localhost:8888` means "my own building, apartment 8888"
- **Connection refused** — the building answered but apartment 8888 has nobody home: the service is down
- **Timeout** — nobody answered the street address at all: wrong IP, or a firewall ate the knock
- **The firewall** — a doorman who only forwards visitors to an approved list of apartment numbers

*Example (italic):* A "connection refused" that returns in a blink means the machine is up but the service is not; a 30-second timeout means the knock never reached the building.

**Key point:** Reading an error as "building unreachable" versus "apartment empty" — timeout versus refused — cuts most connectivity debugging in half.

### Visualization (canvas `c3`, 720×300)

Port number line 0–65535 with the three official ranges as colored bands, and the five ports a data scientist types most often marked with staggered labeled ticks.

- **Title (bold 15px, `#1a5276`, top center):** "The 65,536 Doors: Where Everyday Ports Live".
- **Number line:** horizontal band 26px tall centered on y=150, from x=60 to x=660; three segments (pixel widths schematic, not to scale): well-known 0–1023 at x=60..210 fill `rgba(42,120,214,0.25)`; registered 1024–49151 at x=210..500 fill `rgba(25,158,112,0.20)`; ephemeral 49152–65535 at x=500..660 fill `rgba(201,133,0,0.20)`; 12px `#444` range labels centered under each segment at y=195: "well-known 0–1023", "registered 1024–49151", "ephemeral 49152–65535".
- **Port markers (2px vertical lines from the band top up to a staggered label height, bold 12px label in the same color):** "22 ssh" `#6b7280` at x=70, label y=62; "443 https" `#008300` at x=120, label y=88; "5432 postgres" `#2a78d6` at x=250, label y=62; "6379 redis" `#d55181` at x=300, label y=114; "8888 jupyter" `#d95926` at x=350, label y=88.
- **Annotation (bold 13px violet `#4a3aa7`, near x=505, y=105):** "your laptop dials out from the ephemeral band".
- **Caption (12px `#444`, bottom right):** "positions schematic, port numbers exact".

## One Port Is Not One Connection

**Tags:** `common mistake` (red), `sockets vs ports` (orange)

- **The myth** — "a visitor is using port 443, so the next client must be sent to port 444"
- **The reality** — port 443 is the apartment, and its door can be open to many visitors at once
- **What's unique** — the 4-tuple, not the port: each socket differs by client IP or client port
- **The real limit** — one process listens per port; a second server on 443 gets "address in use"
- **The mix-up** — "address already in use" means two listeners fighting, never too many clients

*Example (italic):* A web server holds 10,000 open sockets on port 443 at once, yet a second copy of that server cannot start because the listening door is already claimed.

**Common mistake:** Confusing the one listening socket (one per port) with connection sockets (thousands per port) — clients never use up the server's port number.

### Visualization (canvas `c4`, 720×300)

Two-row diagram: the wrong mental model (a new server port per client, crossed out) vs the real one (one listening port fanning out to three sockets).

- **Title (bold 15px, `#1a5276`, top center):** "One Door, Many Guests: Ports Don't Run Out".
- **Row 1 (y=95), label 12px `#444` at x=20:** "the myth"; three rounded boxes width 130, height 40 at x=180, x=360, x=540 labeled "client 1 → 443", "client 2 → 444?", "client 3 → 445?" (12px `#2c3e50`); first box fill `rgba(42,120,214,0.15)` border `#2a78d6`, second and third fill `rgba(231,76,60,0.12)` border `#e74c3c` with bold 14px red `#e74c3c` "✗" at their top-right corners; 12px red label under row at y=145: "servers never hand out new ports".
- **Row 2 (y=205), label:** "the reality"; one green rounded box x=180, width 170, height 44, fill `rgba(0,131,0,0.12)`, 2px `#008300` border, bold 12px `#008300` label "port 443 — one listener"; three 2.5px arrows fanning in from the right edge at x=660 (start y = 180, 205, 230) into the box, colored `#2a78d6`, `#199e70`, `#4a3aa7`, each with an 11px matching-color label "from :51000", "from :51001", "from :52344"; bold 12px green "✓ 3 sockets, 1 port" under the box at y=255.
- **Annotation (bold 13px orange `#d95926`, centered near y=282):** "the 4-tuple makes each connection unique — the port never runs out".
- **Caption (12px `#444`, bottom right):** "client ports illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all boxes, arrows, and markers use the hardcoded coordinates and labels above (no randomness); IPs `203.0.113.5`, `198.51.100.7`, `192.0.2.9` are documentation-range addresses and client ports 51000/51001/52344 are invented, both labeled illustrative; the port facts are exact — 16-bit range 0–65535 (65536 values), well-known 0–1023, registered 1024–49151, ephemeral 49152–65535, and services ssh 22, https 443, postgres 5432, redis 6379, jupyter 8888.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
