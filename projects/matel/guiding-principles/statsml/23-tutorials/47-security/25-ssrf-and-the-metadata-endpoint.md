# SSRF & the Metadata Endpoint

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** SSRF &amp; the Metadata Endpoint

**Subtitle:** A "fetch this URL for me" feature turns your server into the attacker's proxy — the outsider cannot reach the internal network, but your server can, and the outsider picks the destination

## Paste a Link, and the Server Goes There

**Tags:** `core idea` (blue), `server as proxy` (orange), `defensive` (green)

- **The feature** — a web app lets Alice paste a link to an image; the server downloads it and makes a thumbnail
- **Genuinely useful** — nothing here is exotic; avatar imports, link previews, and webhooks all work this way
- **Who chooses** — Alice supplies the address, but the fetch is performed by the SERVER, not by her browser
- **The borrowed position** — Bob, an outsider, cannot route to the internal network; the server sits inside it
- **The definition** — server-side request forgery (SSRF): an outsider dictates a destination the server will visit
- **The precise flaw** — not that the server makes requests, but that an untrusted party names where they go

*Example (italic):* Bob pastes not an image link but an internal address; the thumbnailer dutifully fetches it and returns whatever came back.

**Key point:** SSRF is a boundary failure of DESTINATION, the sibling of injection's boundary failure of CODE — user input crosses from data into the address the server will visit.

### Visualization (canvas `c1`, 720×300)

Network diagram: Bob outside the perimeter, the app server inside it, and the borrowed reach drawn as arrows from the server to internal services and to the metadata address.

- **Title (bold 15px, `#1a5276`, top center):** "Bob Cannot Reach Inside — The Server Can, and Bob Picks the Address".
- **Perimeter:** dashed 2px `#1a5276` (dash 6/5) rounded rectangle from x=225 to x=700, y=48 to y=272, 10px radius; 12px `#1a5276` label "network perimeter" at (233, 66), text-align left.
- **Bob box (outside):** x=20, y=125, 150×50, 8px radius, fill `rgba(217,89,38,0.12)`, 2px `#d95926` border, two centered 12px `#2c3e50` lines "Bob (outsider)" / "internet only".
- **Server box (inside):** x=255, y=125, 160×50, 8px radius, fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border, two centered 12px lines "app server" / "thumbnailer".
- **Request arrow:** 3px `#d95926` arrow from (170, 150) to (251, 150), 12px `#d95926` label "pasted link" centered at (210, 140) above it.
- **Internal targets (three boxes at x=505, width 175, 40px tall, 8px radius, fill `rgba(25,158,112,0.13)`, 2px `#199e70` border, centered 12px text):** y=70 "internal admin panel", y=140 "database / health port", y=210 "link-local metadata address" (this last one fill `rgba(213,81,129,0.14)`, border 2px `#d55181`).
- **Reach arrows:** 2.5px arrows from the server box right edge (415, 150) to each target's left edge (501, y+20) — first two in `#199e70`, the third in `#d55181`.
- **Blocked path:** a 2px dashed `#6b7280` (dash 5/4) line from (95, 200) to (222, 200), a red `#e74c3c` X (two 2.5px crossed strokes, 9px arms) centred at (232, 200), and a 12px `#6b7280` left-aligned label "no route" at (100, 220).
- **Annotation (bold 13px violet `#4a3aa7`, centred at (455, 116)):** "60× the reach, borrowed".
- **Caption (12px `#444`, bottom right):** "topology illustrative; placeholder destinations".

## Counting What Each Party Can Reach

**Tags:** `worked example` (blue), `reachability` (orange), `illustrative` (green)

- **Setup** — the app's public interface exposes 2 ports to the internet: one for HTTP, one for HTTPS
- **Inside** — the server's network segment holds 40 hosts, each running 3 listening services
- **Server's reach** — 40 × 3 = 120 internal endpoints, none of them designed to face the internet
- **Amplification** — Bob's direct reach is 2 endpoints; through the server it is 120, and 120 / 2 = 60×
- **One more door** — the metadata address makes 121 destinations, so 2 / 121 = 1.7% is Bob's own share
- **Assumption stated** — this counts reachability only; each internal service may still check authorization
- **The catch** — many internal services never learned to, treating "reachable" as "authorized" by default

*Example (italic):* 2 endpoints from the internet versus 120 from inside is a 60× jump in attack surface, reached by pasting one link (counts illustrative).

**Key point:** The severity is arithmetic, not cleverness — one feature converts a 2-endpoint surface into a 120-endpoint one, plus the metadata address as the 121st.

### Visualization (canvas `c2`, 720×300)

Two-bar comparison of reachable endpoints: Bob directly (2) versus Bob via the server (121 = 120 internal + 1 metadata).

- **Title (bold 15px, `#1a5276`, top center):** "Reachable Endpoints: 2 Directly vs 121 Through the Server".
- **Axes:** origin x=95, baseline y=245, plot width 560, plot height 180; y scale 0 to 121 endpoints; gridlines `#e5e9ef` at 30, 60, 90, 120 (pixel y = 245 − v/121×180 → 200.4, 155.7, 111.1, 66.4) with 12px `#444` right-aligned tick labels at x=89; x-axis 2px `#999` from x=95 to x=655.
- **Bars (width 110):** bar A centred at x=215, value 2, height 2/121×180 = 3.0px, fill `rgba(217,89,38,0.35)`, 2px `#d95926` border; bar B centred at x=505, value 121, height 180px, fill `rgba(42,120,214,0.35)`, 2px `#2a78d6` border.
- **Metadata cap on bar B:** a 3px `#d55181` horizontal stroke across bar B's top edge (x=450 to x=560 at y=65), with a bold 12px `#d55181` label "+1 metadata address" right of it, drawn at (452, 56) text-align left.
- **Value labels:** bold 13px `#d95926` "2" centred above bar A at (215, 232); bold 13px `#2a78d6` "121" centred at (505, 40).
- **Category labels (12px `#444`, centred at y=263):** "Bob, direct" under bar A; "Bob, via the server" under bar B; 12px `#444` axis caption "endpoints reachable" centred at (375, 285).
- **Annotation (bold 13px violet `#4a3aa7`, at (300, 120), text-align left):** "120 / 2 = 60×".
- **Caption (12px `#444`, bottom right):** "40 hosts × 3 services illustrative".

## Naming Allowed Destinations Instead of Forbidden Ones

**Tags:** `defensive` (green), `allowlist` (blue), `arithmetic` (orange)

- **The naive fix** — block the one literal address people warn about, and call the thumbnailer safe
- **Count the spellings** — the same destination can be written 6 ways in our list: literal, decimal integer, shortened octets, a DNS name that resolves to it, a redirect from an allowed host, an alternate address family
- **Coverage** — the blocklist stops 1 of those 6, so 1 / 6 = 16.7% blocked and 5 / 6 = 83.3% pass
- **Worse than it looks** — 6 is only what we enumerated; the set of ways to name a destination is open-ended
- **Allowlist instead** — name the 3 image hosts the feature needs; every other destination is denied by default
- **Resolve first** — validate the address AFTER name resolution, since a friendly name can resolve inward
- **No blind redirects** — a permitted host may redirect onward, so re-check every hop, not just the first
- **Cheap extras** — hardened metadata access needing a session token, a minimal instance role, and a separate segment with no internal route

*Example (italic):* Blocking one literal spelling leaves 5 of the 6 enumerated forms working — 83.3% — while an allowlist of 3 hosts denies all six without naming any of them.

**Key point:** A blocklist must enumerate an open-ended set while an allowlist enumerates a closed one; that asymmetry, not diligence, is why allowlisting is the correct default.

### Visualization (canvas `c3`, 720×300)

Two-panel comparison: the blocklist panel lists 6 enumerated spellings (1 stopped, 5 through, plus an open-ended row); the allowlist panel shows 3 approved hosts and a catch-all deny.

- **Title (bold 15px, `#1a5276`, top center):** "Blocklist: An Open-Ended Set. Allowlist: A Closed One".
- **Divider:** 1px `#e5e9ef` vertical line at x=372 from y=45 to y=272.
- **Left header (bold 13px `#d95926` at (20, 62), left-aligned):** "blocklist — 1 of 6 stopped (16.7%)".
- **Left rows (six rows, 12px `#2c3e50` text at x=44, row y = 90, 116, 142, 168, 194, 220):** "the literal address", "same address, decimal form", "shortened-octet spelling", "a name that resolves inward", "redirect from an allowed host", "alternate address family". Row 1 gets a 10px filled square at x=24 in `#008300` and a bold 11px `#008300` tag "stopped" right-aligned at x=364; rows 2–6 get a 10px filled square in `#e74c3c` and a bold 11px `#e74c3c` tag "through" right-aligned at x=364.
- **Left open-ended row (12px italic `#6b7280` at (44, 246)):** "… and more forms not listed".
- **Right header (bold 13px `#008300` at (394, 62), left-aligned):** "allowlist — 3 named hosts".
- **Right rows (three boxes at x=396, width 290, 36px tall, 8px radius, fill `rgba(0,131,0,0.12)`, 2px `#008300` border, 12px `#2c3e50` centred text):** y=78 "approved image host A", y=124 "approved image host B", y=170 "approved image host C".
- **Right catch-all box:** x=396, y=216, 290×36, 8px radius, fill `rgba(107,114,128,0.14)`, 2px `#6b7280` border, bold 12px `#6b7280` centred text "everything else — denied".
- **Annotation (bold 12px violet `#4a3aa7`, centred at (541, 272)):** "validate after resolving the name".
- **Caption (12px `#444`, bottom right):** "6 enumerated forms illustrative".

## Why It Gets Filed as Low Severity

**Tags:** `common mistake` (red), `cloud credentials` (orange)

- **The triage error** — the report reads "it fetches a URL and shows an error", so the ticket sinks to the backlog
- **What it really is** — on a cloud instance the response can carry the instance's own role credentials
- **Escalation, not leakage** — that turns "read a URL" into "act as the server's cloud identity"
- **The blind variant** — even when nothing is echoed back, response timing and side effects still leak answers
- **Wrong control** — checking the URL's FORMAT proves nothing; the question is where it resolves and what that host can reach
- **Right question in review** — "who chose this destination, and what can this component route to?"

*Example (italic):* A blind thumbnailer returns only "invalid image", yet a slow reply versus an instant refusal distinguishes a live internal port from a dead one.

**Common mistake:** Rating SSRF by what the response shows. Rate it by the network position being borrowed — a silent fetch from a cloud host is still a path to the instance's credentials.

### Visualization (canvas `c4`, 720×300)

Rising four-step escalation ladder from "fetch a URL" to "act with the server's role", with a side note that the blind variant skips the visible response yet keeps the last steps.

- **Title (bold 15px, `#1a5276`, top center):** "From 'It Just Fetches a URL' to the Server's Cloud Identity".
- **Steps (four boxes, width 150, 46px tall, 8px radius, 12px `#2c3e50` centred text, each higher than the last):** x=25 y=210 "fetch a URL" (fill `rgba(42,120,214,0.15)`, border `#2a78d6`); x=195 y=170 "reach an internal service" (fill `rgba(25,158,112,0.14)`, border `#199e70`); x=365 y=130 "read the metadata response" (fill `rgba(201,133,0,0.16)`, border `#c98500`); x=535 y=90 "act with the server's role" (fill `rgba(213,81,129,0.16)`, border `#d55181`).
- **Connectors:** 3px `#6b7280` arrows from each box's right edge midpoint to the next box's left edge midpoint (175→195 at y=233→193, 345→365 at y=193→153, 515→535 at y=153→113).
- **Severity labels (bold 12px, above each box, centred):** "looks harmless" in `#2a78d6` at (100, 202); "information" in `#199e70` at (270, 162); "credentials" in `#c98500` at (440, 122); "privilege escalation" in `#d55181` at (610, 82).
- **Blind-variant note:** a dashed 2px `#6b7280` (dash 5/4) bracket under steps 1–2 from (25, 272) to (345, 272) with short 8px upticks at both ends, and a 12px `#6b7280` left-aligned label "blind SSRF: nothing echoed back — timing still answers" at (25, 290).
- **Annotation (bold 13px `#1a5276`, at (25, 60), left-aligned):** "severity comes from the position borrowed, not the body returned".
- **Caption (12px `#444`, bottom right):** "escalation path schematic".

## Footnote (rendered as a small note under the last section)

12px `#6b7280` italic note in the page body: "All addresses, hosts, and credentials on this page are placeholders — no real metadata address, token, or payload is shown, so security scanners have nothing to flag."

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label). A `.footnote` paragraph closes the page.
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Red `#e74c3c` only for genuine alarm states (the "no route" X, the five bypassed blocklist rows).
- **Data (all hardcoded, no randomness):** 2 public ports; 40 hosts × 3 services = 120 internal endpoints; 120 + 1 metadata = 121 destinations; 120 / 2 = 60× amplification; 2 / 121 = 1.7% share; 6 enumerated address spellings with 1 blocked (1/6 = 16.7%) and 5 through (5/6 = 83.3%); 3 allowlisted hosts. Every figure is invented and labeled illustrative; text numbers must match chart numbers to the digit.
- **Framing:** strictly defensive/educational. No real metadata IP address, no exploit payload, no bypass string, no step-by-step procedure — mechanisms are described conceptually with placeholder destinations ("the link-local metadata address", "approved image host A"). No real company, product, or vendor names; people are Alice and Bob.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
