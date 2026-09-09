# Man-in-the-Browser Banking Trojans

**Page type:** detail page (backlog kusto-style two-column layout: one `.lang-section` per numbered h2, text left ~45%, canvas right ~55%)
**HTML title tag:** Man-in-the-Browser Banking Trojans

**Subtitle:** The middleman is not on the network — it lives inside your own browser, editing the transfer after you approve it and the screen after the bank replies.

**Intro callout (blue-left-border box):** Encryption protects the pipe between the browser and the bank; this attack sits before the pipe starts, where everything is already decrypted (a man-in-the-browser).

## 1. The setup: the screen is your only window

Everything you know about your account arrives through one window.

- **Scene:** everything about your account arrives via one screen.
- **Fact:** the browser draws every page you ever read.
- **Fact:** the browser is ordinary software on the machine.
- **Risk:** on a compromised machine, other software edits it.
- **Fact:** the padlock certifies the pipe, not either room.
- **Mechanism:** pages are built and read before encryption starts.

**Key point (red-left-border box):** **Risk:** the malware sits inside the trust boundary — everything it touches is already plain text.

### Visualization (canvas `c1`, 720×300)

Flow diagram: user → browser → encrypted pipe (padlock) → bank, with a red resident-malware box attached to the browser inside a dashed trust boundary, before the padlock.

- **Title (bold 15px `#1a5276`, top center):** "The malware sits inside the trust boundary, before the padlock".
- **Trust boundary:** dashed `#999` rect (dash [5,4], width 1) at x=25, y=42, w=340, h=165; label 12px `#999` left-aligned at (35, 56): "Alice's machine".
- **Malware box:** 145×44 at (205, 62), color `#e74c3c` (fill at 0.12 alpha, stroke width 2); title bold 14px `#e74c3c` centered at (277, 80): "resident malware"; sub 12px `#666` at (277, 96): "attached to the browser".
- **Vertical arrow:** `#bbb` width-1.5 line from (277, 106) to (277, 128), filled triangular head from y=128 to tip (277, 135), pointing down into the browser box.
- **Boxes** height 62 at y=135 (fill = color at 0.12 alpha, stroke = color width 2; title bold 15px in box color centered at y=160; sub 12px `#666` centered at y=178):
  | Title | sub-line | color | x | width |
  |---|---|---|---|---|
  | Alice | reads the screen | #1a5276 | 45 | 105 |
  | Browser | builds every page | #2980b9 | 205 | 145 |
  | Bank | executes requests | #27ae60 | 585 | 110 |
- **Pipe:** rect x=375, y=150, w=195, h=32, stroke `#2980b9` width 2, fill `#2980b9` at 0.08 alpha; label bold 12px `#2980b9` centered at (472, 170): "encrypted pipe".
- **Padlock (above pipe, `#27ae60`):** shackle = arc center (472, 127) radius 6, from π to 2π, stroke width 2; body = filled rect (462, 127, 20, 15).
- **Horizontal arrows** (`#bbb` width 1.5, filled heads): from (152, 166) to (203, 166); from (352, 166) to (373, 166); from (572, 166) to (583, 166).
- **Bottom line (bold 14px `#e74c3c`, centered, y=245):** "Everything the malware touches is already plain text."
- **Caption (bottom center, 13px `#999`, y=285):** "The padlock certifies the pipe — not the room where the page is built and read."

## 2. The trick: edit the order, then edit the receipt

The attack edits both directions of the conversation.

- **Scene:** the malware waits quietly for a banking session.
- **Edit:** payee and amount are rewritten after the click.
- **Fact:** the bank correctly executes what it received.
- **Edit:** reply pages are rewritten before Alice sees them.
- **Edit:** balance and history are forged to match her intent.
- **History:** documented behavior of past banking trojans.

**Key point (red-left-border box):** **Risk:** the screen agrees with Alice, the ledger agrees with the malware — only the bank's side is real.

### Visualization (canvas `c2`, 720×300)

Two-timeline diagram: a green "what Alice sees" lane telling a consistent story, a red "what the bank received" lane diverging at the click and re-converging on a forged screen.

- **Title (bold 15px `#1a5276`, top center):** "Two stories: what Alice sees vs what the bank received".
- **Lanes:** green lane — label bold 14px `#27ae60` left-aligned at (30, 88): "what Alice sees"; line `#27ae60` width 2 from (30, 110) to (690, 110). Red lane — label bold 14px `#e74c3c` left-aligned at (30, 178): "what the bank received"; line `#e74c3c` width 2 from (30, 200) to (690, 200).
- **Nodes:** filled circles radius 5 in the lane color on both lanes at x = 150, 310, 470, 630.
- **Green node captions** (12px `#2c3e50`, centered, y=98): x150 "fills in transfer", x310 "clicks send", x470 "sees success page", x630 "balance looks right".
- **Red node captions** (12px `#2c3e50`, centered, y=222): x150 "same request so far", x310 "payee + amount swapped", x470 "executes the swapped order", x630 "true ledger differs".
- **Divergence markers:** dashed `#e74c3c` vertical lines (dash [4,3], width 1) from y=118 to y=192 at x=310 and x=630; beside each, two left-aligned 12px `#e74c3c` lines at x+8: at x=310 — "edit #1" (y=148), "the order" (y=161); at x=630 — "edit #2" (y=148), "the receipt" (y=161).
- **Bottom line (bold 14px `#1a5276`, centered, y=258):** "The stories split at the click — the forged screen hides the split."
- **Caption (bottom center, 13px `#999`, y=285):** "Every check Alice can run goes through the same edited window."

## 3. What stops it: a second window

A second, independent window breaks the single-screen problem.

- **Defense:** confirm every transfer on a separate device.
- **Fact:** the phone shows what the bank actually received.
- **Defense:** reading that message is the actual defense.
- **Risk:** people often tap approve without reading.
- **Defense:** banks may also flag scripted-looking sessions.
- **Fact:** keeping devices clean is the root fix.
- **Fact:** good design assumes some machines are infected.

**Key point (red-left-border box):** **Win:** two devices must lie consistently to fool Alice — far harder than editing one screen.

### Visualization (canvas `c3`, 720×300)

Second-window diagram: the compromised browser channel in red, an independent phone confirmation channel in green showing the bank's real payee and amount schematically.

- **Title (bold 15px `#1a5276`, top center):** "A second window: the phone shows the bank's version".
- **Bank box:** 125×70 at (40, 110), color `#1a5276` (fill 0.12 alpha, stroke width 2); title bold 15px centered at (102, 140): "Bank"; sub 12px `#666` at (102, 158): "sends two channels".
- **Browser box:** 230×62 at (430, 48), color `#e74c3c`; title bold 15px `#e74c3c` centered at (545, 72): "Alice's browser"; sub 12px `#666` at (545, 92): "forged screen — looks fine".
- **Phone box:** 230×80 at (430, 168), color `#27ae60`; title bold 15px `#27ae60` centered at (545, 190): "Alice's phone"; message lines 13px `#2c3e50` centered: "pay: (real payee)" at (545, 212), "amount: (real amount)" at (545, 230).
- **Channel arrows** (width 1.5, filled angled heads in the channel color): red `#e74c3c` from (165, 132) to (430, 80); green `#27ae60` from (165, 158) to (430, 208).
- **Channel labels** (12px, centered): `#e74c3c` at (295, 92): "web session — editable"; `#27ae60` at (295, 200): "confirmation — independent".
- **Bottom line (bold 14px `#27ae60`, centered, y=268):** "Two devices must lie consistently — much harder than one screen."
- **Caption (bottom center, 13px `#999`, y=290):** "Reading the message before approving is the actual defense."

## Regeneration instructions

- **Layout:** backlog detail-page style — h1 with `border-bottom: 2px solid #2980b9`, `.subtitle`, one `.intro` callout, then one `.lang-section` per numbered h2 (also blue-underlined). Each section holds a `table.layout` (border-collapse, full width) with one `<tr>`: left `td.text-col` (45%) for paragraphs/bullets/`.key-point`, right `td.viz-col` (55%) for the canvas.
- **Bullet style:** every bullet is one non-wrapping line — a bold colored label plus a phrase of roughly 55 characters or fewer; in HTML the label is `<span class="pt-label" style="color:COLOR">Label:</span>` with `.pt-label { font-weight: bold; }`. Long ideas are split into more labeled bullets, never wrapped. Lead paragraphs are at most one short sentence.
- **Label colors by meaning:** `#1a5276` blue = mechanism/fact (Fact, Mechanism); `#27ae60` green = defense/win (Defense, Win); `#e74c3c` red = risk/loss (Risk, Edit); `#e67e22` orange = scene/context/history (Scene, History). Key-point boxes open with the same colored bold lead word (Risk, Win) followed by one short sentence.
- **Page CSS:** body system-ui sans-serif, background `#fff`, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; h2 1.3rem `#1a5276`; `.subtitle` `#666` 0.95rem; `.intro` background `#f0f4f8` with `border-left: 3px solid #2980b9`; `.key-point` background `#f8f9fa` with `border-left: 3px solid #e74c3c`, 0.9rem; `.pt-label` bold; ul 0.92rem; canvases `width: 100%` with `1px solid #e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic sizes `c1` 720×300, `c2` 720×300, `c3` 720×300; a shared `setupCanvas(id, w, h)` helper sizes the backing store from the displayed width times `window.devicePixelRatio` and scales the context accordingly; each chart is a named draw function (`drawC1`/`drawC2`/`drawC3`) called once at load and again on window `resize` so the charts stay sharp. All data hardcoded; no randomness or dates.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, plus `#2980b9`, `#8e44ad`, gray text `#666`/`#999`, arrows `#bbb`.
- **Content rule:** mechanical, neutral tone; no malware family names, no bank names; people are Alice/Bob; payee and amount are schematic placeholders only — never realistic account numbers or money amounts; hedges ("often", "may", "on a compromised machine") stay in place for unsourced behavioral claims.
- In regenerated HTML, any card/page links use `.html` extensions (this page has none).
