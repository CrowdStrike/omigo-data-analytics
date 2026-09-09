# Phishing & Password Theft

**Page type:** detail page (backlog kusto-style two-column layout: one `.lang-section` per numbered h2, text left ~45%, canvas right ~55%)
**HTML title tag:** Phishing & Password Theft

**Subtitle:** A convincing copy of a familiar sign-in page collects whatever is typed into it — the thief signs in minutes later, from somewhere else.

**Intro callout (blue-left-border box):** Nothing is broken into at first — the page simply asks, looks right, and waits; the real break-in comes later, with the answer.

## 1. The lure: an urgent message, a lookalike address

The message creates hurry; the link supplies the trap.

- **Scene:** an urgent note claims the account is locked.
- **Mechanism:** the note links to a copied sign-in page.
- **Fact:** the address differs by a single character.
- **Fact:** the page itself is a pixel-perfect copy.
- **Mechanism:** urgency pushes clicking before checking.
- **Risk:** small phone screens hide most of the address.

**Key point (red-left-border box):** **Risk:** every pixel on the page can be right — the address is the one part a copy cannot match.

### Visualization (canvas `c1`, 720×300)

Side-by-side address comparison: two browser-style bars, real above in green, lookalike below in red, with the swapped character circled.

- **Title (bold 16px `#4527a0`, top center, y=20):** "Two address bars — one character apart".
- **Real row:** label "Real sign-in page" bold 14px `#00695c` left-aligned at (60, 48); bar rect at (60, 56) size 600×44, fill `#00695c` at 0.08 alpha, stroke `#00695c` width 2; padlock icon in `#666` (body rect 10×8 at (76, 73); shackle arc radius 4 centered (81, 73) from π to 2π, width 1.5); address text 16px monospace `#2c3e50` left-aligned at (105, 84): "www.retailer.example/sign-in".
- **Mid label (13px `#999`, centered at (360, 122)):** "the two pages render identically — only the bar differs".
- **Lookalike row:** label "Lookalike page" bold 14px `#c62828` left-aligned at (60, 138); bar rect at (60, 146) size 600×44, fill `#c62828` at 0.08 alpha, stroke `#c62828` width 2; padlock icon in `#666` (body rect 10×8 at (76, 163); shackle arc radius 4 centered (81, 163) from π to 2π, width 1.5); address text 16px monospace left-aligned at y=174 in three segments: "www.retai" in `#2c3e50` at x=105, "1" in bold `#c62828` at x=192, "er.example/sign-in" in `#2c3e50` at x=202.
- **Swap marker:** stroked `#c62828` circle radius 11 centered (197, 169), width 1.5; annotation "letter l swapped for the digit 1" 13px `#c62828` centered at (360, 215).
- **Bottom line (bold 14px `#ef6c00`, centered, y=252):** "The padlock appears on both — it proves a sealed line, not the right owner."
- **Caption (bottom center, 13px `#999`, y=285):** "A pixel-perfect copy takes minutes to make; the address is the only part that cannot be duplicated."

## 2. The harvest: typed once, taken instantly

Whatever is typed into the copy goes straight to the thief.

- **Mechanism:** the fake form saves whatever is typed.
- **Mechanism:** it then forwards Alice to the real site.
- **Fact:** the visit ends on the real page, feeling normal.
- **Risk:** stolen entries are often tried within minutes.
- **Risk:** unused entries are sold on in bulk lists.
- **Risk:** a reused password opens other accounts too.

**Key point:** **Risk:** nothing on the screen marks the theft — the first sign is a sign-in Alice never made.

### Visualization (canvas `c2`, 720×300)

Flow diagram: four boxes left to right, victim to real account, with a dashed forwarding elbow underneath showing the cover story.

- **Title (bold 16px `#4527a0`, top center, y=20):** "One typed password, two sign-ins".
- **Boxes** 130×54 at y=88 (fill = box color at 0.12 alpha, stroke = box color width 2; title bold 14px in box color centered at y=110; sub-line 12px `#666` centered at y=128):
  | Title | sub-line | color | x |
  |---|---|---|---|
  | Alice | types into the form | #4527a0 | 30 |
  | Fake page | records the entry | #c62828 | 210 |
  | Thief | signs in minutes later | #00838f | 390 |
  | Real account | opens normally | #6a1b9a | 570 |
- **Arrows:** width-1.5 horizontal arrows with filled triangular heads at y=115: from (160,115) to (208,115) in `#c62828`; from (340,115) to (388,115) in `#c62828`; from (520,115) to (568,115) in `#00838f`. Arrow labels 12px `#999` centered at y=105: "typed" at (184, 105); "copied" at (364, 105); "replayed" at (544, 105).
- **Forwarding elbow:** dashed `#999` width-1.5 polyline (275,142) → (275,200) → (635,200) → (635,152), then filled `#999` triangle head pointing up with points (631,154), (639,154), (635,146); label "Alice is forwarded to the real page — the screen looks normal" 13px `#999` centered at (455, 218).
- **Bottom line (bold 14px `#ef6c00`, centered, y=262):** "The forwarding is the cover — the visit ends where it was supposed to."
- **Caption (bottom center, 13px `#999`, y=285):** "Entries not used at once are sold on in bulk lists."

## 3. What limits the damage: checks the copy cannot pass

The strongest defenses check the address instead of the look.

- **Defense:** a password manager fills only the exact address.
- **Fact:** its silence on a lookalike is itself a warning.
- **Defense:** a second step blocks a bare password (two-factor).
- **Risk:** a one-time code typed into the fake can be relayed.
- **Defense:** sign-in bound to the real site defeats copies (passkey).
- **Defense:** an early report lets the account be locked first.
- **Defense:** a changed password makes the stolen copy worthless.

**Key point:** **Win:** layers that check the address, not the look, turn a perfect copy into a dead end.

### Visualization (canvas `c3`, 720×300)

Layered-defense rows: one row per setup, item text and sub-line on the left, outcome pill on the right in red (passes) or green (blocked / no harvest).

- **Title (bold 16px `#4527a0`, top center, y=20):** "Layered checks: how far the stolen entry gets".
- **Rows** at y = 52, 100, 148, 196 (item text bold 14px `#2c3e50` left-aligned at (50, y+10); sub-line 12px `#666` left-aligned at (50, y+27); outcome pill = rect at x=530, width 130, height 26 at row y, fill = pill color at 0.12 alpha, stroke = pill color width 2, pill text bold 13px in pill color centered at (595, y+17)):
  | Item | sub-line | pill text | color |
  |---|---|---|---|
  | Password typed by hand, no second step | the thief's copy signs in | passes | #c62828 |
  | Password manager fills the form | it stays silent on the wrong address | no harvest | #00695c |
  | Second sign-in step on the account | a bare password is not enough | blocked | #00695c |
  | Sign-in bound to the site itself | the copy has nothing usable to take | blocked | #00695c |
- **Bottom line (bold 14px `#4527a0`, centered, y=252):** "Layers that check the address cannot be fooled by a perfect-looking page."
- **Caption (bottom center, 13px `#999`, y=285):** "An early report shortens the window between the harvest and the first stolen sign-in."

## Regeneration instructions

- **Layout:** backlog detail-page style — h1 with `border-bottom: 2px solid #6a1b9a`, `.subtitle`, one `.intro` callout, then one `.lang-section` per numbered h2 (also underlined in `#6a1b9a`). Each section holds a `table.layout` (border-collapse, full width) with one `<tr>`: left `td.text-col` (45%) for paragraphs/bullets/`.key-point`, right `td.viz-col` (55%) for the canvas.
- **Bullet style:** every bullet is one non-wrapping line — a bold colored label plus a phrase of roughly 55 characters or fewer; in HTML the label is `<span class="pt-label" style="color:COLOR">Label:</span>` with `.pt-label { font-weight: bold; }`. Long ideas are split into more labeled bullets, never wrapped. Lead paragraphs are at most one short sentence.
- **Label colors by meaning:** `#4527a0` primary = mechanism/fact (Fact, Mechanism); `#00695c` green = defense/win (Defense, Win); `#c62828` red = risk/loss (Risk, Seen); `#ef6c00` orange = scene/context (Scene). Key-point boxes open with the same colored bold lead word (Risk, Win) followed by one short sentence.
- **Page CSS:** body system-ui sans-serif, background `#fff`, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#4527a0`; h2 1.3rem `#4527a0`; `.subtitle` `#666` 0.95rem; `.intro` background `#f0f4f8` with `border-left: 3px solid #6a1b9a`; `.key-point` background `#f8f9fa` with `border-left: 3px solid #c62828`, 0.9rem; `.pt-label` bold; ul 0.92rem; canvases `width: 100%` with `1px solid #e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic sizes `c1` 720×300, `c2` 720×300, `c3` 720×300; a shared `setupCanvas(id, w, h)` helper sizes the backing store from the displayed width times `window.devicePixelRatio` and scales the context accordingly. Each chart is a named `drawC1()`/`drawC2()`/`drawC3()` function; a `renderAll()` call runs them once and again on window resize (debounced 150ms) so canvases stay sharp after resize. All chart data is literal hardcoded coordinates — no randomness, no dates.
- **Palette:** primary `#4527a0`, green `#00695c`, red `#c62828`, orange `#ef6c00`, plus `#6a1b9a`, `#00838f`, gray text `#666`/`#999`.
- **Content rule:** tone stays mechanical and neutral — describe the mechanism, no drama words, no attributed intent, no absolutes ("most sites", not "all sites"). Each technical term (two-factor, passkey) appears at most once, in parentheses. Fictional naming only (Alice; generic example addresses); no realistic credential strings anywhere on the page.
- In regenerated HTML, any card/page links use `.html` extensions (this page has none).
