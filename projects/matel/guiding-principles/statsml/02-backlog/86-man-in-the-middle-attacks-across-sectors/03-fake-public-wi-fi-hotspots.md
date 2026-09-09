# Fake Public Wi-Fi Hotspots

**Page type:** detail page (backlog kusto-style two-column layout: one `.lang-section` per numbered h2, text left ~45%, canvas right ~55%)
**HTML title tag:** Fake Public Wi-Fi Hotspots

**Subtitle:** A network name is just a label anyone can broadcast — your phone picks the strongest signal wearing a familiar name, and every page you visit takes a detour.

**Intro callout (blue-left-border box):** The "evil twin" needs no hacking of your device at all; it wins by being nearby, loud, and named like the network you expect.

## 1. The setup: phones join names, not places

A wireless network announces itself by name alone.

- **Fact:** a network name (SSID) is a label anyone can broadcast.
- **Fact:** open networks never prove who is behind the name.
- **Mechanism:** phones remember names of networks joined before.
- **Mechanism:** a remembered name triggers a silent auto-rejoin.
- **Mechanism:** with duplicate names, the strongest signal wins.
- **Scene:** a copycat can simply sit nearer to you than the router.

**Key point (red-left-border box):** **Risk:** nothing in the join step checks which box wears the name — nearby and loud is enough.

### Visualization (canvas `c1`, 720×300)

Decision schematic: phone between two access points broadcasting the same name; the near, strong one wins.

- **Title (bold 13px `#1a5276`, top center):** "Two boxes, one name — the phone picks the stronger signal".
- **Access-point boxes** 170×60 at y=50 (fill = box color at 0.12 alpha, stroke = box color width 2; title bold 12px in box color centered at y=74; sub-line 10px `#666` centered at y=94):
  | Title | sub-line | color | x |
  |---|---|---|---|
  | Real hotspot (far) | broadcasts "Cafe Wi-Fi" | #27ae60 | 50 |
  | Fake hotspot (near) | broadcasts "Cafe Wi-Fi" | #e74c3c | 500 |
- **Signal arcs** (downward arcs from 0.25π to 0.75π, lineWidth 1.5): real — one arc radius 12 centered (135, 112) in `#27ae60`; fake — three arcs radii 12/20/28 centered (585, 112) in `#e74c3c`.
- **Strength labels** (11px `#666`, centered): "weak here" at (135, 158); "strong here" at (585, 158).
- **Phone box** 120×60 at x=300, y=185, color `#2980b9`: title "Phone" bold 12px centered at (360, 209); sub-line 10px `#666` "remembers the name" at (360, 227).
- **Links:** dashed `#999` width-1.5 line from (302, 190) to (150, 150), label "ignored" 11px `#999` at (215, 178); solid `#e74c3c` width-2 arrow with filled head from (418, 190) to (562, 150), label "joins" bold 11px `#e74c3c` at (500, 178).
- **Bottom line (bold 12px `#e67e22`, centered, y=265):** "Same name, no identity check — near and loud beats real and far."
- **Caption (bottom center, 11px `#999`, y=285):** "The phone matched a remembered name; nothing verified which box owns it."

## 2. The trick: the detour you never notice

Once joined, every page you visit takes one extra hop.

- **Mechanism:** all traffic passes through the fake box first.
- **Mechanism:** it forwards everything on, so pages still load.
- **Fact:** from the screen, nothing looks or feels different.
- **Risk:** a fake "log in for free Wi-Fi" page can ask anything.
- **Seen:** which sites you contact, and when, is visible.
- **Seen:** anything sent unencrypted is readable in full.

**Key point:** **Risk:** the join page itself is the harvest — whatever it asks for is handed straight over.

### Visualization (canvas `c2`, 720×300)

Two-path diagram: expected route above in green, actual route below in red with a copy point at the hotspot.

- **Title (bold 13px `#1a5276`, top center):** "Expected route vs actual route — both end at the internet".
- **Row labels** (bold 12px, left-aligned at x=40): "Expected route" in `#27ae60` at y=48; "Actual route" in `#e74c3c` at y=148.
- **Boxes** 130×50 (fill = box color at 0.12 alpha, stroke = box color width 2; title bold 12px in box color centered at box y+21; sub-line 10px `#666` centered at box y+38):
  | Row | Title | sub-line | color | x | y |
  |---|---|---|---|---|---|
  | top | Phone | sends a request | #1a5276 | 60 | 58 |
  | top | Cafe router | passes it along | #27ae60 | 290 | 58 |
  | top | Internet | site responds | #2980b9 | 520 | 58 |
  | bottom | Phone | same request | #1a5276 | 60 | 158 |
  | bottom | Fake hotspot | reads, then forwards | #e74c3c | 290 | 158 |
  | bottom | Internet | site responds | #2980b9 | 520 | 158 |
- **Arrows:** width-1.5 horizontal arrows with filled triangular heads; top row in `#27ae60` from (190,83) to (288,83) and from (420,83) to (518,83); bottom row in `#e74c3c` from (190,183) to (288,183) and from (420,183) to (518,183).
- **Copy point:** filled `#e74c3c` circle radius 4 at (355, 216); centered 11px `#e74c3c` label at (355, 234): "copy point — traffic is readable here".
- **Bottom line (bold 12px `#e67e22`, centered, y=262):** "The page loads either way — the detour adds a reader, not a failure."
- **Caption (bottom center, 11px `#999`, y=285):** "From the screen, the two routes are indistinguishable."

## 3. What stops it: the sealed envelope

Sealing protects the letter, not the address on it.

- **Defense:** most sites seal contents end to end (HTTPS).
- **Fact:** the middleman reads the envelope, not the letter.
- **Seen:** the site name and the timing stay visible either way.
- **Defense:** tampering triggers a loud alert (certificate warning).
- **Defense:** the defense is not clicking past that alert.
- **Risk:** join pages asking for card numbers deserve suspicion.
- **Defense:** forgetting open networks stops the silent rejoin.

**Key point:** **Win:** on sealed sites the middleman gets the address and timing — and nothing more.

### Visualization (canvas `c3`, 720×300)

Envelope diagram: one row per item, readable items marked in orange with an envelope icon, sealed items in green with a padlock icon.

- **Title (bold 13px `#1a5276`, top center):** "The sealed envelope: what a middleman can and cannot read".
- **Rows** at y = 58, 103, 148, 193 (item text 12px `#2c3e50` left-aligned at x=100, baseline row y+22; status pill = rect at x=500, width 140, height 26 at row y+2, fill = pill color at 0.12 alpha, stroke = pill color width 2, pill text bold 11px in pill color centered):
  | Item | pill text | color | icon |
  |---|---|---|---|
  | site name on the envelope (the address) | readable | #e67e22 | envelope |
  | when and how often you connect | readable | #e67e22 | envelope |
  | page contents you read | sealed | #27ae60 | padlock |
  | passwords and card numbers you type | sealed | #27ae60 | padlock |
- **Envelope icon:** stroked `#e67e22` rect 24×16 at (58, row y+6), width 1.5, plus flap lines from the two top corners to the rect's center point.
- **Padlock icon:** filled `#27ae60` body rect 16×12 at (62, row y+11), plus stroked `#27ae60` shackle arc radius 5 centered (70, row y+11) from π to 2π, width 2.
- **Bottom line (bold 12px `#1a5276`, centered, y=255):** "The middleman reads the outside of the envelope, never the letter inside."
- **Caption (bottom center, 11px `#999`, y=285):** "A tampered seal triggers a loud browser warning — the split above holds only if you do not click through."

## Regeneration instructions

- **Layout:** backlog detail-page style — h1 with `border-bottom: 2px solid #2980b9`, `.subtitle`, one `.intro` callout, then one `.lang-section` per numbered h2 (also blue-underlined). Each section holds a `table.layout` (border-collapse, full width) with one `<tr>`: left `td.text-col` (45%) for paragraphs/bullets/`.key-point`, right `td.viz-col` (55%) for the canvas.
- **Bullet style:** every bullet is one non-wrapping line — a bold colored label plus a phrase of roughly 55 characters or fewer; in HTML the label is `<span class="pt-label" style="color:COLOR">Label:</span>` with `.pt-label { font-weight: bold; }`. Long ideas are split into more labeled bullets, never wrapped. Lead paragraphs are at most one short sentence.
- **Label colors by meaning:** `#1a5276` blue = mechanism/fact (Fact, Mechanism); `#27ae60` green = defense/win (Defense, Win); `#e74c3c` red = risk/loss (Risk, Seen); `#e67e22` orange = scene/context (Scene). Key-point boxes open with the same colored bold lead word (Risk, Win) followed by one short sentence.
- **Page CSS:** body system-ui sans-serif, background `#fff`, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; h2 1.3rem `#1a5276`; `.subtitle` `#666` 0.95rem; `.intro` background `#f0f4f8` with `border-left: 3px solid #2980b9`; `.key-point` background `#f8f9fa` with `border-left: 3px solid #e74c3c`, 0.9rem; `.pt-label` bold; ul 0.92rem; canvases `width: 100%` with `1px solid #e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic sizes `c1` 720×300, `c2` 720×300, `c3` 720×300; a shared `setupCanvas(id, w, h)` helper sizes the backing store from the displayed width times `window.devicePixelRatio` and scales the context accordingly. All chart data is literal hardcoded coordinates — no randomness, no dates.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, plus `#2980b9`, `#8e44ad`, gray text `#666`/`#999`.
- **Content rule:** tone stays mechanical and neutral — describe the mechanism, no drama words, no attributed intent, no absolutes ("most sites", not "all sites"). Each technical term (SSID, HTTPS, certificate warning) appears at most once, in parentheses. No realistic credentials or network secrets anywhere on the page.
- In regenerated HTML, any card/page links use `.html` extensions (this page has none).
