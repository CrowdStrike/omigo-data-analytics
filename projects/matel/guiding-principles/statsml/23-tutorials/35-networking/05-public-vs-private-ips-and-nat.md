# Public vs Private IPs & NAT

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Public vs Private IPs &amp; NAT

**Subtitle:** Every home reuses the same private addresses inside — NAT is the front desk that swaps them for the one public address the internet can actually see

## Every Home Has a Room 192.168.1.7

**Tags:** `core idea` (blue), `private = inside` (green), `public = internet` (orange)

- **Two kinds** — private addresses work only inside one building; public addresses work across the internet
- **The private ranges** — 10.x.x.x, 172.16–31.x.x, and 192.168.x.x are reserved for inside use only
- **The reuse** — millions of homes all have a 192.168.1.7; it's a room number, not a street address
- **The public IP** — the router's outside address, unique on the whole internet, rented from the ISP
- **The rule** — routers on the public internet refuse to forward packets addressed to private ranges

*Example (italic):* Alice's laptop and Bob's printer next door are both 192.168.1.7 — no conflict, because each address only means something inside its own home.

**Key point:** Private addresses are reusable room numbers valid inside one network; the public IP is the single street address the rest of the internet sees.

### Visualization (canvas `c1`, 720×300)

Two houses side by side, each containing devices on the same private addresses, each fronted by a router with a unique public address, connected through an internet box in the middle.

- **Title (bold 15px, `#1a5276`, top center):** "Same Room Numbers Inside, Unique Street Address Outside".
- **Alice's home (left):** rounded rect x=30, y=50, width 210, height 218, fill `rgba(42,120,214,0.08)`, 2px `#2a78d6` border; bold 13px `#1a5276` centered label "Alice's home" at y=70.
- **Alice's devices:** two white rounded boxes x=45, width 180, height 34, 12px `#2c3e50` centered labels: y=82 "laptop — 192.168.1.7" with 2px orange `#d95926` border (the duplicated address), y=124 "TV — 192.168.1.12" with 1.5px `#6b7280` border.
- **Alice's router:** rounded box x=45, y=170, width 180, height 44, fill `rgba(0,131,0,0.12)`, 2px `#008300` border; two centered lines: 12px `#2c3e50` "router" at y=188, bold 12px `#008300` "public 198.51.100.2" at y=204.
- **Bob's home (right):** mirrored rounded rect x=480, y=50, width 210, height 218, same style; label "Bob's home"; devices x=495: y=82 "printer — 192.168.1.7" with 2px orange border, y=124 "phone — 192.168.1.20" with mute border; router y=170 labeled "router" / "public 203.0.113.9".
- **The internet (middle):** rounded rect x=285, y=145, width 150, height 60, fill `rgba(74,58,167,0.10)`, 2px `#4a3aa7` border; bold 12px `#4a3aa7` "the internet" at y=170 and 11px `#4a3aa7` "public addresses only" at y=188, both centered.
- **Arrows:** 2.5px aqua `#199e70` from Alice's router right edge (240, 192) to internet left edge (285, 175), and from Bob's router left edge (480, 192) to internet right edge (435, 175).
- **Annotation (bold 13px magenta `#d55181`, centered near y=288):** "two homes, same 192.168.1.7 — private addresses never leave the building".
- **Caption (12px `#444`, bottom right):** "public addresses illustrative (documentation ranges)".

## Following One Packet Through the Front Desk

**Tags:** `worked example` (blue), `address rewrite` (green), `NAT table` (orange)

- **The setup** — Alice's laptop 192.168.1.7 asks web server 203.0.113.5, port 443, for a page
- **Outbound** — the packet leaves the laptop stamped "from 192.168.1.7:51000"
- **The rewrite** — the router swaps the source to its own public 198.51.100.2:61001 before forwarding
- **The table** — the router notes one row: inside 192.168.1.7:51000 ↔ outside 61001
- **The reply** — arrives addressed to 198.51.100.2:61001; the table row maps it back to the laptop
- **Hand-check** — the server only ever saw 198.51.100.2; the private address never crossed the router

*Example (italic):* The server's access log records a visitor from 198.51.100.2 — the router — and every device in Alice's home shows up in that log as the same address.

**Key point:** NAT rewrites the source address and port on the way out, remembers the mapping in a table, and reverses it on the way back — that table is the whole trick.

### Visualization (canvas `c2`, 720×300)

Three boxes left to right (laptop, router, server) with an outbound arrow pair on top showing the rewrite and a return arrow pair below, plus the NAT table drawn underneath the router.

- **Title (bold 15px, `#1a5276`, top center):** "The Router Rewrites the Return Address — and Remembers".
- **Laptop box:** rounded rect x=30, y=62, width 160, height 44, fill `rgba(42,120,214,0.15)`, 1.5px `#2a78d6` border, 12px `#2c3e50` centered label "laptop 192.168.1.7".
- **Router box:** rounded rect x=280, y=62, width 160, height 44, fill `rgba(0,131,0,0.12)`, 2px `#008300` border, 12px `#2c3e50` centered label "router 198.51.100.2".
- **Server box:** rounded rect x=530, y=62, width 160, height 44, fill `rgba(74,58,167,0.12)`, 1.5px `#4a3aa7` border, 12px `#2c3e50` centered label "server 203.0.113.5:443".
- **Outbound arrows (above the boxes, y=76):** 2.5px blue `#2a78d6` from (190, 76) to (280, 76) with bold 11px blue label "from 192.168.1.7:51000" centered above at y=56 — actually place labels at y=52 over each arrow; 2.5px green `#008300` from (440, 76) to (530, 76) with bold 11px green label "from 198.51.100.2:61001" above it. (Draw the labels at y=52, arrows at the box vertical midline minus 8.)
- **Return arrows (below, y=96):** 2.5px violet `#4a3aa7` from (530, 96) to (440, 96) labeled "to 198.51.100.2:61001" (11px violet, below at y=124), and 2.5px aqua `#199e70` from (280, 96) to (190, 96) labeled "to 192.168.1.7:51000" (11px aqua, below at y=124).
- **NAT table:** rounded rect x=200, y=170, width 320, height 66, white fill, 1.5px `#c98500` border; bold 12px `#c98500` centered header "NAT table" at y=190; 1px `#e5e9ef` divider line at y=200 from x=212 to x=508; 12px `#2c3e50` centered row "inside 192.168.1.7:51000  ↔  outside :61001" at y=220. Dashed 1.5px `#c98500` connector line from the router box bottom (360, 106) to the table top (360, 170).
- **Annotation (bold 13px orange `#d95926`, centered near y=270):** "no table row, no way back in — replies ride the remembered mapping".
- **Caption (12px `#444`, bottom right):** "ports illustrative".

## Why the Internet Runs on This Trick

**Tags:** `where it's used` (blue), `IPv4 shortage` (orange), `data science angle` (green)

- **The shortage** — IPv4 addresses are 32-bit: about 4.3 billion total, far fewer than the world's devices
- **The sharing** — one public IP serves a whole household or office; NAT stretched IPv4 for decades
- **CGNAT** — mobile carriers add a second NAT layer, so thousands of customers share one public IP
- **Counting users by IP** — shared IPs undercount users, and address churn overcounts them
- **Blocking by IP** — banning one abusive address can silence a whole dorm, office, or cell tower

*Example (italic):* A dashboard shows one IP with 400 sessions in an hour — a university's NAT, not a bot — while one logged-in user appears from three IPs in a day as their phone hops towers.

**Key point:** One public IP is neither one device nor one user — a lesson every IP-based metric, rate limiter, and fraud rule eventually learns the hard way.

### Visualization (canvas `c3`, 720×300)

Funnel diagram: three private networks on the left, each with a device count, converging through their routers into a carrier NAT box, emerging as a single public address on the right.

- **Title (bold 15px, `#1a5276`, top center):** "352 Devices, One Address the Internet Sees".
- **Network boxes (left, x=35, width 190, height 46, 8px radius, two centered text lines — 12px `#2c3e50` name at +19, 11px `#6b7280` private range at +36):** y=55 "home — 12 devices" / "192.168.1.x" fill `rgba(42,120,214,0.15)` border 1.5px `#2a78d6`; y=125 "café — 40 devices" / "192.168.0.x" fill `rgba(25,158,112,0.15)` border 1.5px `#199e70`; y=195 "office — 300 devices" / "10.0.x.x" fill `rgba(201,133,0,0.15)` border 1.5px `#c98500`.
- **Carrier NAT box (middle):** rounded rect x=330, y=118, width 160, height 60, fill `rgba(0,131,0,0.12)`, 2px `#008300` border; bold 12px `#008300` centered "carrier NAT" at y=143, 11px `#2c3e50` "(CGNAT)" at y=161.
- **Converging arrows:** 2.5px lines matching each network's border color, from each box's right edge (x=225, y = box center) to the carrier box's left edge (330, y = 133/148/163 top to bottom), with arrowheads.
- **Public IP box (right):** rounded rect x=560, y=124, width 135, height 48, fill `rgba(74,58,167,0.12)`, 2px `#4a3aa7` border; bold 12px `#4a3aa7` centered "one public IP" at y=145 and "203.0.113.77" at y=162. Arrow 3px `#008300` from (490, 148) to (560, 148).
- **Annotation (bold 13px magenta `#d55181`, centered near y=285):** "1 IP ≠ 1 user — distinct-IP counts and IP bans both mislead".
- **Caption (12px `#444`, bottom right):** "device counts illustrative".

## But What IS My IP Address?

**Tags:** `common mistake` (red), `two right answers` (orange)

- **The trap** — `ifconfig` says 192.168.1.7 but a "what is my IP" site says 198.51.100.2; both are right
- **Inside view** — the OS only knows the private address the local router handed it via DHCP
- **Outside view** — websites see the address on the packet after NAT: the router's public IP
- **Allowlists** — cloud firewalls and API allowlists must use the public one; the private one never arrives
- **Inbound needs help** — a server behind NAT needs port forwarding; unsolicited packets have no table row
- **Not a firewall** — NAT drops unknown inbound as a side effect, but it is not a security policy

*Example (italic):* A tester adds the address from `ifconfig` (10.0.3.12) to a cloud database allowlist and nothing connects — the database only ever saw the NAT'd public address.

**Common mistake:** Asking "what's my IP" has two correct answers — the private one your machine knows and the public one the internet sees — and allowlists, firewalls, and server logs all use the public one.

### Visualization (canvas `c4`, 720×300)

Split view: an inside-view panel and an outside-view panel showing the two different answers, joined by a small NAT box in the middle.

- **Title (bold 15px, `#1a5276`, top center):** "Two Right Answers to 'What Is My IP?'".
- **Inside panel (left):** rounded rect x=40, y=55, width 270, height 185, fill `rgba(42,120,214,0.08)`, 2px `#2a78d6` border; bold 13px `#2a78d6` centered header "inside view — what ifconfig says" at y=82; bold 22px `#1a5276` centered "192.168.1.7" at y=140; 12px `#6b7280` centered "private — handed out by the home router" at y=170; 11px `#6b7280` centered "only meaningful inside this network" at y=190.
- **Outside panel (right):** rounded rect x=410, y=55, width 270, height 185, fill `rgba(74,58,167,0.08)`, 2px `#4a3aa7` border; bold 13px `#4a3aa7` centered header "outside view — what websites see" at y=82; bold 22px `#1a5276` centered "198.51.100.2" at y=140; 12px `#6b7280` centered "public — the router's shared address" at y=170; 11px `#6b7280` centered "what logs, bans, and allowlists record" at y=190.
- **NAT box (middle):** rounded rect x=325, y=125, width 70, height 40, fill `rgba(0,131,0,0.12)`, 2px `#008300` border, bold 12px `#008300` centered "NAT" at y=150; 2px aqua `#199e70` arrows from (310, 145) to (325, 145) and from (395, 145) to (410, 145).
- **Annotation (bold 13px red `#e74c3c`, centered near y=280):** "allowlist the public one — the private address never reaches the other side".
- **Caption (12px `#444`, bottom right):** "addresses illustrative (documentation ranges)".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms). Shared `roundRect` and `arrow` helpers as in the sibling pages.
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all boxes, arrows, and labels use the hardcoded coordinates above (no randomness). The private ranges are exact RFC 1918 facts — 10.0.0.0/8, 172.16.0.0/12 (172.16–172.31), 192.168.0.0/16 — and IPv4's 32-bit space (~4.3 billion addresses) is exact; public IPs `198.51.100.2`, `203.0.113.5`, `203.0.113.9`, `203.0.113.77` are documentation-range addresses, and ports 51000/61001 plus all device counts are invented, labeled illustrative in the captions.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
