# Well-Known Ports

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Well-Known Ports

**Subtitle:** One machine has one address but many numbered doors — ports 22, 25, 80, and 443 are the door numbers the whole internet agreed on in advance

## One Address, Four Numbered Doors

**Tags:** `core idea` (blue), `ports` (green), `one server, many services` (orange)

- **The server** — a small bakery chain runs its website, email, and admin login on one rented machine
- **One address** — every packet arrives at the same IP address, like mail to one street address
- **The doors** — each service listens on a numbered port: SSH on 22, mail on 25, web on 80 and 443
- **The convention** — ports 0–1023 are "well-known": IANA reserved each number for one famous service
- **The payoff** — a browser finds any website's front door because everyone agrees HTTPS lives at 443

*Example (italic):* When a customer types the bakery's address, her browser silently dials port 443 — nobody had to tell her the number.

**Key point:** A port is the apartment number on a shared street address — well-known ports are the numbers the whole internet memorized so nobody has to ask.

### Visualization (canvas `c1`, 720×300)

Diagram: one server box on the right with four numbered door boxes on its edge; four clients on the left, each connected by an arrow to the door it uses.

- **Title (bold 15px, `#1a5276`, top center):** "One Address, Four Numbered Doors".
- **Annotation (bold 13px ink `#1a5276`, centered at y=42, under the title):** "same machine, same IP — the port number picks the service".
- **Server:** rounded rect x=470, y=55, 200×210, fill `rgba(42,120,214,0.10)`, 2px `#2a78d6` border, bold 13px `#1a5276` label "bakery server — one IP" centered inside near the top (y=72).
- **Doors:** four 60×30 boxes straddling the server's left edge at x=440, y = `[90, 135, 180, 225]`, bold 13px port numbers `["22", "25", "80", "443"]`; borders/fills per door: 22 green `#008300`/`rgba(0,131,0,0.15)`, 25 magenta `#d55181`/`rgba(213,81,129,0.15)`, 80 yellow `#c98500`/`rgba(201,133,0,0.15)`, 443 blue `#2a78d6`/`rgba(42,120,214,0.15)`; 12px `#6b7280` service names just inside the server: `["SSH", "SMTP", "HTTP", "HTTPS"]`.
- **Clients:** four rounded 170×30 boxes at x=40 on the same y rows, fill `rgba(229,233,239,0.6)`, 1px `#e5e9ef` border, 12px `#2c3e50` labels `["admin laptop", "another mail server", "old plain-HTTP browser", "customer's browser"]`.
- **Arrows:** 2px arrow from each client box to its matching door, drawn in that door's color (green, magenta, yellow, blue).
- **Caption (12px `#6b7280`, bottom right):** "layout schematic; port assignments are real IANA well-known ports".

## One Day of Traffic, Sorted by Door

**Tags:** `worked example` (blue), `firewall log` (green)

- **The log** — one Tuesday, the bakery's server receives exactly 10,000 connection attempts
- **The split** — 8,200 hit port 443 (HTTPS), 1,400 hit 80 (HTTP), 300 hit 25 (mail), 60 hit 22 (SSH)
- **The leftover** — the last 40 probe port 3389, a remote-desktop door the server never opened
- **Hand-check** — 8,200 + 1,400 + 300 + 60 + 40 = 10,000, and 9,600 of them (96%) are web
- **The read** — the port column alone says what each connection wanted, before reading a single byte

*Example (italic):* The 60 SSH attempts arrive in business hours from the office; the 40 hits on 3389 arrive at 3am from addresses the bakery has never seen.

**Key point:** Grouping connections by destination port turns a raw log into a story — which is why firewall dashboards, and firewall rules, are organized by port first.

### Visualization (canvas `c2`, 720×300)

Horizontal bar chart: the day's 10,000 connections bucketed by destination port, one bar per port.

- **Title (bold 15px, `#1a5276`, top center):** "10,000 Connections in One Day, Sorted by Destination Port".
- **Rows (top to bottom at y = 80, 125, 170, 215, 255), each with a left-aligned 12px `#444` label at x=20:** `["443 HTTPS", "80 HTTP", "25 SMTP", "22 SSH", "3389 probe"]`.
- **Bars:** start at x=140, 18px tall, pixel widths proportional to counts with max 470 for the largest: counts `[8200, 1400, 300, 60, 40]` → widths `[470, 80, 17, 4, 3]` (tiny bars floored at 3px for visibility); colors `[#2a78d6, #199e70, #d55181, #008300, #d95926]`; 12px `#444` count labels at bar ends: `["8,200", "1,400", "300", "60", "40"]`.
- **Annotation (bold 13px blue `#2a78d6`, near x=320, y=115):** "96% of the day lands on just two well-known web ports".
- **Caption (12px `#6b7280`, bottom right):** "counts illustrative; port numbers are real assignments".

## Why Firewalls Think in Port Numbers

**Tags:** `where it's used` (blue), `firewall rules` (green), `0–1023 privileged` (orange)

- **The rule** — a firewall rule is mostly a port plus a verdict: "allow 443", "allow 80", "deny the rest"
- **The speed** — the port sits at a fixed spot in every packet header, so the check costs one comparison
- **The bakery's rules** — allow 443, 80, 25 to all, allow 22 from the office IP only, deny the rest
- **The result** — 9,960 connections pass (8,200 + 1,400 + 300 + 60); all 40 probes on 3389 bounce
- **The privilege** — on Unix, only root may open a port below 1024, which once made low ports a trust signal

*Example (italic):* The bakery never wrote a rule about remote desktop — "deny everything else" had door 3389 shut before the first 3am probe arrived.

**Key point:** Firewalls think in ports because the port is the cheapest signal in a packet: one number, fixed position, readable without opening the envelope.

### Visualization (canvas `c3`, 720×300)

Flow diagram: five labeled traffic streams hit a firewall wall; four pass through to the right, one bounces back.

- **Title (bold 15px, `#1a5276`, top center):** "Five Rules Sort All 10,000 Connections".
- **Firewall:** vertical rounded rect x=330, y=55, 60×215, fill `rgba(26,82,118,0.12)`, 2px `#1a5276` border, bold 13px `#1a5276` label "firewall" drawn vertically at its center.
- **Incoming streams:** 12px `#444` labels at x=20 on rows y = `[80, 120, 160, 200, 240]`: `["443 web — 8,200", "80 web — 1,400", "25 mail — 300", "22 admin — 60", "3389 probe — 40"]`; each with a 3px arrow from x=180 to x=330 in the stream color: 443 blue `#2a78d6`, 80 aqua `#199e70`, 25 magenta `#d55181`, 22 green `#008300`, 3389 orange `#d95926`.
- **Verdicts:** rows 1–4 continue with 3px green `#008300` arrows from x=390 to x=560, 12px green labels at the arrow ends: `["allow", "allow", "allow", "allow (office IP only)"]`; row 5 stops at the wall and a 3px orange `#d95926` arrow curves back toward x=220 with bold 12px orange label "deny — bounced".
- **Annotation (bold 13px green `#008300`, near x=430, y=262):** "9,960 pass, 40 bounce — one number per rule".
- **Caption (12px `#6b7280`, bottom right):** "counts illustrative; rule style matches real firewall configs".

## A Label, Not a Lock

**Tags:** `common mistake` (red), `port vs protocol` (orange)

- **The mistake** — reading "port 443" as "safe web traffic": anything can be sent over any port
- **The tunnel** — malware routinely phones home over 443 precisely because every firewall allows it
- **The false hide** — moving SSH from 22 to 2222 stops nobody; scanners sweep all 65,535 ports in minutes
- **The registry** — IANA assigns the numbers (53 DNS, 5432 Postgres), but nothing enforces them on the wire
- **The fix** — serious firewalls add inspection: check the protocol actually spoken, not just the door number

*Example (italic):* The bakery's firewall waves an infected laptop's beacon through on 443 while proudly blocking 3389 — the label matched, the contents didn't.

**Common mistake:** Treating the port number as proof of what is inside. It is a shared naming convention — perfect for sorting and triage, worthless as a guarantee.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: trusting the 443 label lets a beacon through (top); hiding SSH on 2222 is undone by a full port sweep (bottom).

- **Title (bold 15px, `#1a5276`, top center):** "The Port Is a Label, Not a Lock".
- **Row 1 (y=90), label 12px `#444` at x=20:** "'443 must be web'"; blue rounded box at x=140, 170×40, labeled "malware beacon sent over 443" (12px), 3px arrow to a green box at x=360, 110×40, "rule: allow 443", 3px arrow to an orange box at x=520, 170×40, "walks straight through" with bold 12px orange `#d95926` text.
- **Row 2 (y=200), label:** "'2222 hides SSH'"; blue box at x=140 "SSH moved to port 2222", 3px arrow to a magenta box at x=360, 150×40, "scanner sweeps all 65,535 ports", 3px arrow to an orange box at x=560 "found in minutes".
- **Box style:** 8px radius, 40px tall, fills `rgba(42,120,214,0.15)` / `rgba(0,131,0,0.12)` / `rgba(213,81,129,0.12)` / `rgba(217,89,38,0.12)`, 12px `#2c3e50` text, 2px borders in the box color (blue `#2a78d6`, green `#008300`, magenta `#d55181`, orange `#d95926`).
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=272):** "sort by port, verify by protocol".
- **Caption (12px `#6b7280`, bottom right):** "scenarios illustrative; 65,535 is the real maximum port number".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); the daily connection counts `[8200, 1400, 300, 60, 40]` (sum 10,000, 9,960 allowed / 40 denied) are invented and labeled illustrative; the port assignments (22 SSH, 25 SMTP, 53 DNS, 80 HTTP, 443 HTTPS, 5432 Postgres), the 0–1023 privileged range, and the 65,535 port maximum are real documented facts.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
