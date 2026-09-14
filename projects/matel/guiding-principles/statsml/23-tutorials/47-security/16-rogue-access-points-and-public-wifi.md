# Rogue Access Points & Public WiFi

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Rogue Access Points &amp; Public WiFi

**Subtitle:** A network name is not an identity — any laptop can call itself the coffee shop, and a phone that remembers the name rejoins on its own

## Alice's Phone Rejoins a Network She Never Chose

**Tags:** `core idea` (blue), `evil twin` (red), `familiar SSID` (orange)

- **The memory** — Alice joined the coffee shop's open WiFi last week, so her phone stored that network name
- **The announcement** — a laptop at a corner table now broadcasts a network using that exact same name
- **The match** — her phone compares the announced name against its stored list as plain text and finds a hit
- **The rejoin** — the network is open, so it connects with no password prompt and no tap from Alice at all
- **No break-in** — nothing was cracked or forged; the attacker simply answered to a name the phone trusted
- **The label** — a network name (SSID) is a freely chosen string, never a credential, and nobody verifies it

*Example (italic):* Alice takes her phone out of her pocket already connected — the status bar shows the shop's network name, and the network it names is the laptop in the corner.

**Key point:** An **evil twin** is a rogue access point announcing a network name that a device already remembers; because the name is unverified text, "a network I have used before" is decided by a string comparison, and automatic rejoin turns that match into a connection.

### Visualization (canvas `c1`, 720×300)

Vertical flow of the phone's join decision, ending in an automatic connection with no prompt.

- **Title (bold 15px, `#1a5276`, top center):** "The Join Decision, Step by Step".
- **Boxes:** five rounded boxes (8px radius), each x=140, width 360, height 32, at y = `[60, 100, 140, 180, 220]`; centered 12px `#2c3e50` text; 2px borders and matching light fills:
  1. y=60 — "stored list of remembered network names", `rgba(42,120,214,0.15)` / `#2a78d6`
  2. y=100 — "hears an announcement matching a stored name", `rgba(42,120,214,0.15)` / `#2a78d6`
  3. y=140 — "several candidates: prefer the strongest signal", `rgba(74,58,167,0.12)` / `#4a3aa7`
  4. y=180 — "name is open — no password required", `rgba(201,133,0,0.15)` / `#c98500`
  5. y=220 — "auto-joined, no prompt", `rgba(231,76,60,0.14)` / `#e74c3c`
- **Step numbers (bold 13px `#1a5276`, right-aligned at x=128):** "1".."5" at each box's vertical centre.
- **Arrows:** 3px `#6b7280` vertical arrows down the centre (x=320) in each 8px gap, from box bottom to next box top, with a small filled arrowhead.
- **Right-side notes (left-aligned at x=512):** 12px `#6b7280` "text match only" beside box 2; bold 12px `#4a3aa7` "closer wins" beside box 3; bold 12px `#e74c3c` "no tap from Alice" beside box 5.
- **Caption (12px `#444`, bottom right):** "defensive illustration".

## Being Closer Is the Whole Trick

**Tags:** `worked example` (blue), `signal strength` (aqua), `rule of thumb` (orange)

- **The setup** — the shop's real access point is 20 m from Alice's table; the rogue laptop is 3 m away
- **The physics** — received radio power falls with the square of the distance travelled in open space
- **The ratio** — (20 / 3)² = 400 / 9 = 44.4, so the rogue's signal arrives about 44.4× stronger at the phone
- **In decibels** — 10 × log₁₀(44.4) = 16.5 dB stronger, a wide margin by WiFi standards
- **The tiebreak** — among remembered names the phone favours the strongest signal, so proximity beats provenance
- **Back it off** — at 6 m the edge is (20/6)² = 11.1× = 10.5 dB; at 10 m it is 4.0× = 6.0 dB, still winning
- **Model caveat** — free-space only; walls, antenna patterns, and interference move real figures substantially

*Example (italic):* No special hardware is involved — sitting three metres from Alice instead of twenty is what wins the tiebreak (distances illustrative).

**Key point:** The "strongest signal" rule is decided by geometry, so an attacker in the room out-shouts the real access point across the shop by roughly 16.5 dB with ordinary equipment.

### Visualization (canvas `c2`, 720×300)

Floor-plan schematic: the far real access point and the near rogue laptop, with the computed power ratio.

- **Title (bold 15px, `#1a5276`, top center):** "20 m Away vs 3 m Away: Who Sounds Louder".
- **Floor line:** 2px `#e5e9ef` horizontal line at y=176 from x=60 to x=690 (the shop floor).
- **Scale:** 20 px per metre; Alice at x=620, rogue at x=560 (3 m), real access point at x=220 (20 m).
- **Real access point:** blue rounded box (8px radius) centred at x=215, y=95..140, width 130, fill `rgba(42,120,214,0.15)`, 2px `#2a78d6`, centered 12px text "shop access point".
- **Rogue laptop:** red rounded box centred at x=545, y=95..140, width 130, fill `rgba(231,76,60,0.12)`, 2px `#e74c3c`, centered 12px text "rogue laptop".
- **Alice:** aqua filled circle radius 9 at (630, 200), 2px `#199e70` border, 12px `#2c3e50` label "Alice's phone" right-aligned ending at x=612, y=204.
- **Distance lines:** 2px `#2a78d6` line from (630, 200) to (240, 140) with a 12px `#2a78d6` label "20 m" at its midpoint offset 12px above; 2px `#e74c3c` line from (630, 200) to (560, 140) with a 12px `#e74c3c` label "3 m" placed at (585, 158).
- **Main annotation (bold 13px `#e74c3c`, left-aligned at x=60, y=62):** "(20 / 3)² = 44.4× more received power = 16.5 dB stronger".
- **Ratio ladder (12px, left-aligned at x=60, y = 220 / 240 / 260):** "rogue at 3 m: 44.4× (16.5 dB)" in `#e74c3c`, "rogue at 6 m: 11.1× (10.5 dB)" in `#d95926`, "rogue at 10 m: 4.0× (6.0 dB)" in `#c98500`.
- **Caption (12px `#444`, bottom right):** "free-space model; distances illustrative".

## What a Hostile Network Can Actually Do

**Tags:** `where it's used` (blue), `captive portals` (orange), `defensive` (green)

- **Still encrypted** — HTTPS keeps page and form contents unreadable even when the whole network is hostile
- **The steering** — the rogue network answers name lookups (DNS), so it chooses which server the phone reaches
- **The portal** — the "accept terms / enter your email" page is written by the attacker and looks entirely normal
- **What you type** — anything entered into that portal page goes to whoever served it, not to the coffee shop
- **Downgrade tries** — plain-HTTP first requests can be rewritten or redirected before HTTPS ever engages
- **Metadata** — which services the phone contacts, and how much it sends, stay visible even under HTTPS

*Example (italic):* Alice sees a familiar-looking "sign in to continue" portal, types her email address, and it lands in the corner laptop's log while the page politely says "connected".

**Key point:** On a rogue network the danger is not decryption — it is being steered somewhere else and typing into a page the attacker wrote.

### Visualization (canvas `c3`, 720×300)

Branching flow: the phone's traffic reaches the rogue access point, which controls the portal branch but not the HTTPS branch.

- **Title (bold 15px, `#1a5276`, top center):** "The Network Picks the Destination — Not the Contents".
- **Phone box:** blue rounded box x=30, y=126, 120×50, fill `rgba(42,120,214,0.15)`, 2px `#2a78d6`, centered 12px "Alice's phone".
- **Rogue box:** red rounded box x=210, y=126, 150×50, fill `rgba(231,76,60,0.12)`, 2px `#e74c3c`, centered 12px two-line "rogue access" / "point".
- **DNS note (12px `#6b7280`, centered at x=285, y=116):** "answers name lookups (DNS)".
- **Arrow:** 3px `#6b7280` from (150, 151) to (204, 151) with arrowhead.
- **Top branch:** orange rounded box x=440, y=52, 240×48, fill `rgba(230,126,34,0.15)`, 2px `#e67e22`, centered 12px two-line "captive portal it wrote:" / "\"enter your email to continue\""; 3px `#e67e22` arrow from (360, 145) to (436, 80) with arrowhead; bold 12px `#e74c3c` centered at (560, 118): "what she types goes to the attacker".
- **Bottom branch:** green rounded box x=440, y=200, 240×48, fill `rgba(0,131,0,0.10)`, 2px `#008300`, centered 12px two-line "HTTPS site she opens" / "(certificate checked)"; 3px `#008300` arrow from (360, 160) to (436, 224) with arrowhead; bold 12px `#008300` centered at (560, 266): "contents stay unreadable".
- **Caption (12px `#444`, bottom right):** "defensive schematic".

## "Anyone Can Read My Banking" — and Why That Framing Backfires

**Tags:** `common mistake` (red), `rule of thumb` (orange), `defensive` (green)

- **The old claim** — "public WiFi means anyone can read my banking" was far closer to true before HTTPS was default
- **Why overstating hurts** — a threat that sounds absurd gets dismissed, and the genuine risks get dismissed with it
- **The accurate version** — contents are protected; what is exposed is where you are steered and what you type
- **VPN reality** — a VPN moves the trust from the coffee shop's network to the VPN provider; it does not remove it
- **Forget networks** — deleting remembered open networks you no longer visit removes the automatic-rejoin path
- **Disable auto-join** — turning off auto-join for open networks makes every connection a deliberate choice
- **Use cellular** — tethering to your own phone's mobile data skips the join decision entirely when it matters

*Example (italic):* Alice's banking session was never readable; the email address she typed into the portal page, and the fact that she contacted her bank at all, were.

**Common mistake:** Believing HTTPS makes a hostile network harmless, or that a hostile network makes HTTPS useless. Both are wrong — content is protected, destination selection and attacker-served pages are not.

### Visualization (canvas `c4`, 720×300)

Two-panel comparison: what HTTPS still protects versus what the network controls or observes.

- **Title (bold 15px, `#1a5276`, top center):** "Protected by HTTPS vs Controlled by the Network".
- **Left panel:** rounded rect x=40, y=60, 300×180, fill `rgba(0,131,0,0.06)`, 2px `#008300`; header bold 13px `#008300` centered at (190, 84): "protected by HTTPS".
- **Left rows (bold 14px `#008300` "✓" at x=58; 12px `#2c3e50` text at x=78), y = 118 / 158 / 198:** "page and form contents", "passwords sent to real sites", "server identity (certificate)".
- **Right panel:** rounded rect x=380, y=60, 300×180, fill `rgba(217,89,38,0.07)`, 2px `#d95926`; header bold 13px `#d95926` centered at (530, 84): "controlled or seen by the network".
- **Right rows (bold 14px `#d95926` "•" at x=398; 12px `#2c3e50` text at x=416), y = 112 / 148 / 184 / 220:** "which services you contact", "name lookups and redirects", "the captive portal page itself", "plain-HTTP first requests".
- **Annotation (bold 13px `#1a5276`, centered at x=360, y=268):** "defenses: forget old networks, disable auto-join, tether to cellular".
- **Caption (12px `#444`, bottom right):** "defensive summary".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`, aqua `rgba(25,158,112,0.15)`/`#199e70`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** no randomness anywhere; all coordinates and figures are hardcoded. The distances (20 m, 3 m, 6 m, 10 m) are illustrative, and every derived figure is the exact free-space inverse-square result: (20/3)² = 44.4 and 10·log₁₀(44.4) = 16.5 dB; (20/6)² = 11.1 and 10.5 dB; (20/10)² = 4.0 and 6.0 dB. Text figures must match chart figures to the digit, and the free-space simplification must stay stated.
- **Accuracy guardrails:** the page must not overstate the threat — HTTPS genuinely protects content, and the residual risks are DNS/redirect steering, downgrade attempts on plain-HTTP requests, captive-portal harvesting, and metadata. A VPN is described as relocating trust, not removing it.
- **Framing:** defensive/educational only — the mechanism is explained so readers recognise it and apply the defenses. No setup or operational guidance for running a rogue access point, and no real business names or credential-looking strings.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
