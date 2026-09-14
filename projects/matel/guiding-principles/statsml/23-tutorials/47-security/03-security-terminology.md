# Security Terminology

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Security Terminology

**Subtitle:** The terms a data scientist meets in incident reports and security news, each anchored to one running breach story at a generic retail company

## One Breach, Term by Term

**Tags:** `core idea` (blue), `attack lifecycle` (orange), `the breach story` (green)

- **Phishing** — the fake supplier email that tricks a retail clerk into opening an attachment
- **Payload** — the malicious code inside the attachment; opening the file quietly runs it
- **C2 / command-and-control** — the attacker's server the payload phones home to for orders
- **Lateral movement** — hopping from the clerk's laptop to other machines, like the finance server
- **Privilege escalation** — upgrading the clerk's ordinary account into admin rights
- **Exfiltration** — the customer table quietly leaves the network, disguised as normal traffic

*Example (italic):* In nine days the retail company went from one clicked invoice email to its entire customer table sitting on an attacker's server.

**Key point:** Each term names one step of the same lifecycle — get in, run code, phone home, spread, gain power, steal — so an incident report is a story told in order.

### Visualization (canvas `c1`, 720×300)

Left-to-right kill-chain flow: six labeled boxes, one per term, with a plain-English gloss under each and arrows between them.

- **Title (bold 15px, `#1a5276`, top center):** "One Breach, Six Terms: the Attack Lifecycle".
- **Boxes:** six rounded rects (radius 8) 100×44, tops at y=100, centered at x = `[72, 188, 304, 420, 536, 652]`; fills `rgba(42,120,214,0.12)` / `rgba(25,158,112,0.12)` / `rgba(74,58,167,0.12)` / `rgba(201,133,0,0.12)` / `rgba(217,89,38,0.12)` / `rgba(231,76,60,0.12)` with 2px strokes `#2a78d6` / `#199e70` / `#4a3aa7` / `#c98500` / `#d95926` / `#e74c3c`.
- **Box labels (bold 12px `#2c3e50`, centered; one line at y=127 or two lines at y=119/134):** "phishing"; "payload"; "C2 server"; "lateral" / "movement"; "privilege" / "escalation"; "exfiltration".
- **Step numbers:** bold 12px in each box's stroke color, centered above each box at y=92: "1".."6".
- **Arrows:** 3px `#6b7280` horizontal arrows with solid arrowheads between neighboring boxes at y=122 (from box edge +2 to next box edge −8).
- **Glosses (11px `#444`, two centered lines at y=166 and y=180 under each box):** "the fake" / "supplier email"; "the attachment" / "that runs"; "the server it" / "phones home to"; "clerk's laptop →" / "finance server"; "clerk account →" / "admin rights"; "customer table" / "leaves the network".
- **Annotation (bold 13px red `#e74c3c`, centered at y=235):** "one clicked email ends with the customer table on an attacker's server".
- **Caption (12px `#444`, bottom right):** "attack lifecycle of the retail-company breach — illustrative".

## Vulnerability, Exploit, Breach — and the Numbers Around Them

**Tags:** `vocabulary` (blue), `severity scores` (orange), `patch or breach` (green)

- **Vulnerability vs exploit** — the unlocked window vs the technique of climbing through it
- **Breach** — the burglar actually inside; a hole only becomes a breach once someone uses it
- **Zero-day** — a hole the vendor has known about for zero days: no patch exists yet
- **CVE** — the public catalog number a hole gets, like CVE-2026-1234, so everyone means the same bug
- **CVSS** — its 0–10 severity score; a 9.8 is severe on paper — urgency depends on your exposure
- **Patch** — the vendor's fix; it only protects the systems that actually install it

*Example (italic):* The hole in the retail company's VPN box became CVE-2026-1234, scored 9.8 — the patch shipped in March, and the stores that skipped it were breached in April.

**Key point:** A vulnerability is a possibility, an exploit is a method, and a breach is an event — the three words describe escalating certainty, not the same thing.

### Visualization (canvas `c2`, 720×300)

Pipeline diagram: the normal life of a hole across the top (discovered → CVE → CVSS → patch → patched: safe), a red "unpatched systems: breached" box below, and a dashed zero-day lane that skips from discovery straight to breached.

- **Title (bold 15px, `#1a5276`, top center):** "The Life of a Security Hole — and the Zero-Day Shortcut".
- **Top-row boxes:** five rounded rects (radius 8) 118×44, tops at y=73, centered at x = `[85, 225, 365, 505, 645]`; fills `rgba(42,120,214,0.12)` / `rgba(74,58,167,0.12)` / `rgba(201,133,0,0.12)` / `rgba(25,158,112,0.12)` / `rgba(0,131,0,0.12)` with 2px strokes `#2a78d6` / `#4a3aa7` / `#c98500` / `#199e70` / `#008300`.
- **Box labels (bold 12px, two lines at y=92/107, `#2c3e50` except the last box in `#008300`):** "hole" / "discovered"; "CVE assigned" / "CVE-2026-1234"; "CVSS scored" / "9.8 / 10"; "patch" / "shipped"; "patched:" / "safe".
- **Glosses (11px `#6b7280`, centered at y=133 under the first four boxes):** "the unlocked window"; "public catalog number"; "0–10 severity score"; "the fix".
- **Arrows:** 3px `#6b7280` horizontal arrows between neighboring top-row boxes at y=95.
- **Breached box:** rounded rect 150×44 at top y=195 centered at x=505, fill `rgba(231,76,60,0.12)`, 2px `#e74c3c` stroke, bold 12px `#e74c3c` two-line label "unpatched systems:" / "breached".
- **Unpatched drop:** 2px dashed (dash 5/4) orange `#d95926` vertical arrow from the "patch shipped" box bottom (x=505, y=119) down to the breached box top; bold 12px orange label left-aligned at (520, 165): "patch never installed".
- **Zero-day lane:** 2px dashed (dash 5/4) red `#e74c3c` elbow path from the "hole discovered" box bottom (x=85, y=119) down to y=217, then right to the breached box's left edge, ending in a red arrowhead; bold 12px red label centered at (262, 205): "zero-day: attackers find it first — no patch exists, the pipeline is skipped".
- **Annotation (bold 13px green `#008300`, left-aligned at (20, 282)):** "the same hole, two endings — installing the patch decides which".
- **Caption (12px `#444`, bottom right):** "illustrative timeline".

## The Defender's Side: SOC, SIEM, EDR

**Tags:** `where it's used` (blue), `defense in depth` (green), `the watchers` (orange)

- **SOC** — the security operations center: the room, or team, watching alerts around the clock
- **SIEM** — the big database every log flows into, so one search sweeps the whole company
- **EDR** — the watchdog agent on every laptop and server, recording what programs actually do
- **IOC** — an indicator of compromise: a fingerprint to hunt, like a bad file hash or a beacon address
- **Red / blue / purple team** — paid pretend attackers, the defenders, and both working together
- **Honeypot** — a fake server left out as bait; nobody legitimate touches it, so any touch is an alarm

*Example (italic):* When the breach made the news, the retail company's SOC took the published IOCs, searched the SIEM, and found the beacon address on exactly one laptop.

**Key point:** The defender stack is one funnel: EDR watches the machines, logs pour into the SIEM, the SOC watches the SIEM, and IOCs are the search terms.

### Visualization (canvas `c3`, 720×300)

Defense-in-depth map: laptops with EDR chips and a honeypot inside a dashed network boundary, logs flowing into a SIEM, alerts flowing out to the SOC, and an IOC-hunt arrow from the SOC back into the SIEM.

- **Title (bold 15px, `#1a5276`, top center):** "Where Each Defender Term Lives".
- **Network boundary:** dashed (dash 6/4) 1.5px `#1a5276` rounded rect (radius 10) from (25, 55) sized 390×210; 11px `#6b7280` label inside top-left at (36, 71): "the retail company network".
- **Laptops:** three rounded rects 95×30 at x=45, tops y = 85, 130, 175, fill `rgba(42,120,214,0.12)`, 1.5px `#2a78d6` stroke, 11px `#2c3e50` label "laptop" at left; each carries a green EDR chip — rounded rect 34×16 (radius 8) at x=100, y = box top +7, fill `rgba(0,131,0,0.15)`, 1px `#008300` stroke, bold 10px `#008300` centered text "EDR".
- **EDR gloss (11px `#444`, left-aligned at (45, 222) and (45, 235)):** "EDR: watchdog agent" / "on every machine".
- **Honeypot:** rounded rect 120×32 at (175, 218), fill `rgba(213,81,129,0.12)`, 1.5px `#d55181` stroke; bold 11px `#d55181` centered "honeypot (bait)" at y=231 and 10px `#444` "fake server, no real users" at y=244.
- **SIEM:** rounded rect 115×56 at (280, 105), fill `rgba(74,58,167,0.12)`, 2px `#4a3aa7` stroke; bold 13px `#4a3aa7` "SIEM" and 11px `#444` "all logs flow in".
- **Log arrows:** 2px `#199e70` arrows from each laptop's right edge (x=142, box middle) to the SIEM's left edge midpoint, with arrowheads.
- **Honeypot alarm:** 2px dashed (dash 4/4) `#d55181` arrow from (280, 234) up to the SIEM's bottom edge near (330, 169); bold 11px `#d55181` label at (305, 210): "any touch = alarm".
- **SOC:** rounded rect 150×66 at (545, 95) — outside the boundary — fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` stroke; bold 13px `#1a5276` "SOC", then 11px `#444` "analysts watching" / "alerts around the clock".
- **Alert arrow:** 3px `#2a78d6` arrow from the SIEM's right edge to the SOC's left edge, 11px `#6b7280` label "alerts" above its midpoint.
- **IOC hunt:** 2px dashed (dash 5/4) orange `#d95926` arrow from the SOC's bottom edge (x=575) back to just below the SIEM's bottom-right corner; bold 12px orange two-line label at (470, 210)/(470, 226): "hunt IOCs: a bad file hash," / "a beacon address".
- **Annotation (bold 13px violet `#4a3aa7`, left-aligned at (25, 288)):** "every machine reports in — one SIEM search sweeps the whole company".
- **Caption (12px `#444`, right-aligned at y=46):** "defense-in-depth schematic — illustrative".

## The Confusions Everyone Makes

**Tags:** `common mistake` (red), `say vs mean` (orange)

- **Hacker ≠ criminal** — the word means skilled tinkerer; the criminal is an attacker or threat actor
- **Vulnerability ≠ breach** — a hole is a possibility; most holes are never exploited by anyone
- **Zero-day ≠ any bad bug** — it specifically means no patch existed when the attacks began
- **Anti-virus ≠ EDR** — AV matches known bad files; EDR watches live behavior for new tricks
- **Headline test** — "may have accessed data" often means a hole was found, not that data moved

*Example (italic):* The first headline said the retail company "was hit by a zero-day" — the post-mortem showed a patched-for-months hole and one clicked phishing email.

**Common mistake:** Reading "vulnerability disclosed" as "data was stolen." A hole, a technique, and an actual break-in are three different words for a reason.

### Visualization (canvas `c4`, 720×300)

Two-column comparison card drawn on canvas: four "people say" phrases in red cells on the left, each pointing to its "it actually means" correction in a green cell on the right.

- **Title (bold 15px, `#1a5276`, top center):** "People Say vs It Actually Means".
- **Column headers (bold 13px, centered at y=56):** "people say" in `#e74c3c` at x=195; "it actually means" in `#008300` at x=530.
- **Cells:** four rows with tops at y = `[68, 116, 164, 212]`, height 42; left cells rounded rects (radius 6) from x=40 width 310, fill `rgba(231,76,60,0.08)`, 1.5px `#e74c3c` stroke; right cells from x=370 width 320, fill `rgba(0,131,0,0.08)`, 1.5px `#008300` stroke; bold 13px `#6b7280` "→" centered at x=360 in each row.
- **Cell text (12px `#2c3e50`, left-aligned at x=52 / x=382; one line at row top +26, two lines at +18/+33):**
  - Row 1 left: `"a hacker stole the data"`; right: "hacker = skilled tinkerer — the criminal" / "is an attacker or threat actor".
  - Row 2 left: `"a vulnerability was found,` / `so we've been breached"`; right: "a hole is not a break-in; most holes" / "are never exploited".
  - Row 3 left: `"this nasty bug is a zero-day"`; right: "zero-day = no patch existed when the" / "attacks began — not just any bad bug".
  - Row 4 left: `"anti-virus already covers that"`; right: "AV matches known bad files; EDR watches" / "live behavior for brand-new tricks".
- **Annotation (bold 13px orange `#d95926`, centered at y=282):** "same words in the headline, very different facts on the ground".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates; shared `roundRect(ctx,x,y,w,h,r)` and `arrowHead(ctx,x,y,angle,color)` helpers. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`. Red is reserved for genuine alarm states (exfiltration, breach, zero-day, the "people say" column).
- **Data:** all diagram nodes, coordinates, labels, and glosses are the hardcoded values above (no randomness); CVE-2026-1234, the 9.8 CVSS score, and the nine-day timeline are invented and labeled illustrative; the running story stays at a generic retail company — no real company names; CVE-YYYY-NNNN and the 0–10 CVSS range are the standard public formats.
- Framing is strictly defensive education: the page decodes the vocabulary of incident reports; it never explains how to perform an attack.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
