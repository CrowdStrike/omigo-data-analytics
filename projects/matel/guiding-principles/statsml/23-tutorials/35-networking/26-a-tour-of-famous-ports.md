# A Tour of Famous Ports

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** A Tour of Famous Ports

**Subtitle:** SSH, SMTP, Telnet, NTP — the door numbers worth memorizing, plus ping, which has no door number at all

## Doors Worth Memorizing

**Tags:** `core idea` (blue), `port numbers` (green), `TCP vs UDP` (orange)

- **The machine** — one rented server runs a clinic's reports, its mail, and its clock sync
- **SSH on 22** — the encrypted remote terminal; a staffer logs in and types as if sitting there
- **SMTP on 25** — server-to-server mail handoff; the door mail servers use to reach each other
- **Telnet on 23** — the same remote terminal as SSH, but in plain text; kept only as a museum piece
- **NTP on 123** — the only one of the four on UDP; a short question about what time it really is
- **Ping on nothing** — ICMP rides beside TCP and UDP, so it has no port field to fill in

*Example (italic):* A staffer's morning touches every door here except 23 — that one has been welded shut since SSH arrived in 1995.

**Key point:** A door number tells you which service, but the transport (TCP or UDP) tells you what kind of conversation — and ping proves not every network tool goes through a door at all.

### Visualization (canvas `c1`, 720×300)

Diagram: one server box on the right with four numbered door boxes on its left edge, plus a fifth arrow that bypasses the doors entirely and strikes the server's outer wall (ping).

- **Title (bold 15px, `#1a5276`, top center):** "Numbered Doors, and a Knock on the Wall".
- **Server:** rounded rect x=430, y=48, 250×230, fill `rgba(42,120,214,0.10)`, 2px `#2a78d6` border, bold 13px `#1a5276` label "clinic server — one IP address" centered at (555, 66).
- **Doors:** four 56×28 boxes straddling the server's left edge at x=402, y = `[86, 128, 170, 212]`; bold 13px port numbers `["22", "25", "23", "123"]`; border/fill per door: 22 green `#008300`/`rgba(0,131,0,0.15)`, 25 magenta `#d55181`/`rgba(213,81,129,0.15)`, 23 orange `#d95926`/`rgba(217,89,38,0.15)`, 123 violet `#4a3aa7`/`rgba(74,58,167,0.15)`.
- **Inside-server labels (12px `#6b7280`, left-aligned at x=468):** `["SSH — TCP, encrypted", "SMTP — TCP, mail relay", "Telnet — TCP, plain text", "NTP — UDP, clock sync"]`.
- **Callers:** four rounded 150×28 boxes at x=30 on the same y rows, fill `rgba(229,233,239,0.6)`, 1px `#e5e9ef` border, 12px `#2c3e50` labels `["staff laptop", "another mail server", "(nobody, since 1995)", "public time server"]`.
- **Arrows:** 2px arrow from each caller to its door in that door's color; the Telnet arrow (row 3) is drawn dashed `[5,4]` to mark it disused.
- **Ping row (y=254):** 12px `#2c3e50` label "ping" at x=30; a 2px aqua `#199e70` arrow from x=190 to x=430 that lands on the server wall *below* the lowest door, with bold 12px aqua text "ICMP — no port to knock on" placed above it at (300, 248).
- **Caption (12px `#6b7280`, bottom right):** "layout schematic; all four port assignments are real IANA numbers".

## Telnet vs SSH on the Wire

**Tags:** `worked example` (blue), `packet arithmetic` (green)

- **The login** — a staffer types a 12-character password into a remote terminal on port 23
- **One key, one packet** — interactive Telnet sends each keystroke immediately: 12 tiny packets
- **The envelope** — every packet carries a 20-byte IP header plus a 20-byte TCP header = 40 bytes
- **Hand-check** — 12 × (40 + 1) = 492 bytes sent to move 12 useful bytes, so 12/492 = 2.4% payload
- **The eavesdropper** — reading those 12 packets recovers all 12 characters, in order, in plain text
- **Same keys over SSH** — still 12 packets, but each payload is ciphertext: 0 of 12 characters readable

*Example (italic):* The 492 bytes ignore the 18 bytes of Ethernet framing per packet, which would push the real total past 700 — the payload share only gets worse.

**Key point:** Telnet and SSH send the same keystrokes through nearly identical envelopes; the only difference is whether anyone on the path can read what is inside.

### Visualization (canvas `c2`, 720×300)

Top: one stacked horizontal bar splitting 492 bytes into headers and payload. Bottom: two short bars for how many of the 12 password characters an eavesdropper recovers.

- **Title (bold 15px, `#1a5276`, top center):** "Bytes on the Wire to Carry One Password".
- **Stacked bar (y=78, height 30, starts x=150, total width 470 = 492 bytes, so 0.955 px/byte):** first segment 480 bytes → 459px, fill `#6b7280`; second segment 12 bytes → 11px, fill `#008300`. 12px `#444` row label "Telnet, 12 keystrokes" left-aligned at x=20, y=97.
- **Segment labels:** 12px white bold "480 bytes of IP + TCP headers" centered inside the grey segment; 12px green `#008300` "12 bytes" at x=634, y=97 with a short 1px `#008300` leader line from the green segment up-right to it.
- **Annotation (bold 13px violet `#4a3aa7`, left-aligned at x=150, y=128):** "only 2.4% of the traffic is the actual password".
- **Sub-heading (bold 13px `#1a5276`, left at x=20, y=172):** "characters an eavesdropper on the path can read".
- **Two bars (start x=150, height 24, scale 12 chars → 320px):** row y=190 label "Telnet (port 23)" → width 320, fill `#d95926`, 12px `#d95926` bold label "12 of 12" at bar end; row y=232 label "SSH (port 22)" → width 0 with a 3px `#008300` vertical tick at x=150, bold 12px `#008300` label "0 of 12" at x=162, y=249.
- **Caption (12px `#6b7280`, bottom right):** "header sizes are real (IPv4 20 B, TCP 20 B); the 12-character password is illustrative".

## One Morning, Several Doors

**Tags:** `where it's used` (blue), `daily reality` (green)

- **08:59:58** — ping checks the gateway is alive; an ICMP echo comes back in 3 ms
- **09:00:01** — NTP asks a time server on UDP 123 and finds the local clock 240 ms fast
- **09:00:12** — SSH opens an encrypted terminal on TCP 22; the nightly report job gets started
- **09:04:30** — SMTP hands the finished report to the next mail server on TCP 25
- **Never** — Telnet on 23 stays shut, because it would put that same login on the wire in clear text
- **The pattern** — diagnose first, agree on the time, do the work, then deliver the result

*Example (italic):* Skip the 09:00:01 clock nudge and the report's own timestamps drift 240 ms away from every other machine's log lines.

**Key point:** These are not exotic protocols — a single ordinary morning walks through reachability, time, remote work, and delivery, one well-known port each.

### Visualization (canvas `c3`, 720×300)

Horizontal timeline of one morning with four labeled events above it and the unused Telnet door marked below it.

- **Title (bold 15px, `#1a5276`, top center):** "One Staffer's Morning, One Port at a Time".
- **Axis:** 2px `#1a5276` horizontal line from x=70 to x=670 at y=170; 12px `#6b7280` tick labels below at y=190: "08:59:58" (x=110), "09:00:01" (x=250), "09:00:12" (x=390), "09:04:30" (x=600); 1px `#e5e9ef` tick marks from y=163 to y=177 at each x.
- **Event markers:** filled 7px-radius dots on the axis at each tick x, colors `[#199e70, #4a3aa7, #008300, #d55181]`.
- **Event cards (rounded 150×54 boxes, 8px radius, above the axis at y=76, centered on their tick x but clamped to the canvas, 2px border and 0.12-alpha fill in the marker color):** bold 12px port line then 12px `#2c3e50` detail line — `["ping / ICMP", "reply in 3 ms"]`, `["NTP / UDP 123", "clock was 240 ms fast"]`, `["SSH / TCP 22", "start report job"]`, `["SMTP / TCP 25", "hand off the report"]`; 1px leader line in the marker color from each card's bottom center down to its dot.
- **Unused door (below axis):** dashed 1.5px `#d95926` rounded rect x=250, y=222, 220×40, fill none; bold 12px `#d95926` centered text "Telnet / TCP 23 — never opened" at (360, 246).
- **Annotation (bold 13px `#1a5276`, right-aligned at x=670, y=222):** "reachability → time → work → delivery".
- **Caption (12px `#6b7280`, bottom right):** "timings illustrative; port numbers and transports are real".

## Ping Has No Port

**Tags:** `common mistake` (red), `ICMP` (orange)

- **The mistake** — asking "what port does ping use?" and picking one, usually 7 or 0
- **The layer** — ICMP sits directly on IP as a sibling of TCP and UDP; its header has no port field
- **What it carries instead** — a type and code: echo request is type 8, echo reply is type 0
- **The consequence** — a firewall rule listing ports cannot allow or deny ping; ICMP needs its own rule
- **The false death** — many hosts drop ICMP by policy, so an unanswered ping proves nothing about uptime
- **The false life** — a reply only proves the kernel is up; the service on 22 or 25 can still be dead

*Example (italic):* The clinic's server answers ping all night while its mail service on port 25 has been down since 2am — one says the machine is on, not that the work is getting done.

**Common mistake:** Treating ping as a port check. It tests whether IP packets reach the machine at all; only a connection to a specific port tests whether a service is listening.

### Visualization (canvas `c4`, 720×300)

Layered stack diagram: one IP layer with three sibling boxes above it — TCP and UDP each showing a port field, ICMP showing a type/code field where the ports would be.

- **Title (bold 15px, `#1a5276`, top center):** "TCP and UDP Have Ports. ICMP Has Types.".
- **IP layer:** rounded rect x=60, y=210, 600×46, 8px radius, fill `rgba(26,82,118,0.10)`, 2px `#1a5276` border, bold 13px `#1a5276` centered text "IP — carries a packet to the machine's address" at (360, 238).
- **Three sibling boxes (y=96, height 82, 8px radius, 2px border, 0.12-alpha fill in the box color):** TCP at x=60 width 180 (blue `#2a78d6`); UDP at x=270 width 180 (aqua `#199e70`); ICMP at x=480 width 180 (orange `#d95926`).
- **Box contents:** bold 14px box-colored protocol name centered at y=120; then a 12px `#2c3e50` centered field line at y=142 — TCP "source port + dest port", UDP "source port + dest port", ICMP "type + code, no port"; then a 12px centered example line at y=163 — TCP `#6b7280` "22, 25, 23", UDP `#6b7280` "123", ICMP bold `#d95926` "type 8 = echo request".
- **Connectors:** 2px vertical line in each box's color from the box bottom (y=178) to the IP layer top (y=210).
- **Annotation (bold 13px red `#e74c3c`, centered at (360, 282)):** "a port-based firewall rule can never match a ping".
- **Caption (12px `#6b7280`, bottom right):** "ICMP echo types 8 and 0 and the header field lists are real; layout is schematic".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`), matching `networking/16-well-known-ports.html`. Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms). Shared `roundRect` and `arrow` helpers as in `16-well-known-ports.html`.
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all values hardcoded, no randomness. Real documented facts: SSH TCP 22, SMTP TCP 25, Telnet TCP 23, NTP UDP 123, the 0–1023 well-known range, IPv4 header 20 bytes, TCP header 20 bytes, Ethernet framing 18 bytes, ICMP as an IP-layer sibling of TCP/UDP with type/code and no ports, echo request type 8 / echo reply type 0, SSH's 1995 origin. Illustrative and labeled as such: the 12-character password, the 492-byte total derived from it (12 × 41 = 492; 12/492 = 2.4%), the morning's timestamps, the 3 ms ping reply, and the 240 ms clock offset.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
