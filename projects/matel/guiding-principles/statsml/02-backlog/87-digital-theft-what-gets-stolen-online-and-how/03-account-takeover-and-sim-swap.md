# Account Takeover & SIM Swap

**Page type:** detail page (backlog kusto-style two-column layout: one `.lang-section` per numbered h2, text left ~45%, canvas right ~55%)
**HTML title tag:** Account Takeover &amp; SIM Swap

**Subtitle:** Control the phone number or the recovery inbox and every "forgot my password" door opens — accounts fall through their recovery paths, not their front doors.

**Intro callout (blue-left-border box):** The takeover never guesses a password; it borrows the place where the reset codes land.

## 1. The recovery path is also a door

Every account keeps a side entrance for the day you forget.

- **Fact:** most sites offer a "forgot my password" flow.
- **Mechanism:** the flow sends a code to a phone or an email.
- **Mechanism:** typing the code back counts as proof of identity.
- **Fact:** the proof is really ownership of the number or inbox.
- **Risk:** whoever holds those two holds a master key.
- **Risk:** the weakest reset path sets the real strength.

**Key point (red-left-border box):** **Risk:** a strong password guards the front door while the reset flow stays a second, easier door.

### Visualization (canvas `c1`, 720×300)

Hub diagram: one phone-number box on the left, reset arrows fanning out to bank, email, and social boxes on the right.

- **Title (bold 16px `#283593`, top center):** "One phone number, many doors — reset codes fan out from a hub".
- **Hub box** 160×60 at x=50, y=115, color `#283593` (fill = box color at 0.12 alpha, stroke = box color width 2): title "Phone number" bold 14px in box color centered at (130, 139); sub-line 12px `#666` "reset codes land here" centered at (130, 157).
- **Target boxes** 160×50 at x=500 (same fill/stroke pattern; title bold 14px in box color centered at box y+21; sub-line 12px `#666` centered at box y+38):
  | Title | sub-line | color | y |
  |---|---|---|---|
  | Bank account | "forgot password" reset | #3949ab | 45 |
  | Email inbox | "forgot password" reset | #6d4c41 | 125 |
  | Social profile | "forgot password" reset | #c2185b | 205 |
- **Arrows:** solid `#d84315` width-2 arrows with filled heads from (212, 140) to (498, 70), from (212, 145) to (498, 150), and from (212, 150) to (498, 230).
- **Arrow labels** (bold 13px `#d84315`, centered): "reset code" at (355, 95), (355, 138), and (355, 202).
- **Bottom line (bold 14px `#c2185b`, centered, y=268):** "Hold the hub and every spoke opens — the number is the master key."
- **Caption (bottom center, 13px `#999`, y=288):** "An account's real lock is whatever guards the place its reset codes land."

## 2. Taking the number

The number moves to whoever tells the carrier the best story.

- **Scene:** a caller poses as Alice at her phone carrier.
- **Mechanism:** the carrier moves her number to a new chip (SIM swap).
- **Seen:** Alice's own phone quietly drops to no signal.
- **Mechanism:** texted reset codes now land on the new device.
- **Risk:** one texted code opens the recovery inbox too.
- **Risk:** with the inbox, resets cascade account by account.

**Key point:** **Risk:** no device of Alice's was touched — one carrier conversation moved the number.

### Visualization (canvas `c2`, 720×300)

Sequence flow: top row carrier desk → number moved → new device; bottom row shows Alice's silent phone and a bank code detouring up to the new device.

- **Title (bold 16px `#283593`, top center):** "Moving the number — the texted code takes a detour".
- **Top-row boxes** 170×50 at y=55 (fill = box color at 0.12 alpha, stroke = box color width 2; title bold 14px in box color centered at box y+21; sub-line 12px `#666` centered at box y+38):
  | Title | sub-line | color | x |
  |---|---|---|---|
  | Carrier desk | a convincing story | #c2185b | 30 |
  | Number moved | onto a new chip | #d84315 | 275 |
  | New device | texts arrive here | #d84315 | 520 |
- **Top-row arrows:** horizontal `#d84315` width-1.5 arrows with filled heads from (200, 80) to (273, 80) and from (445, 80) to (518, 80).
- **Bottom-row boxes** 170×50 at y=175 (same pattern):
  | Title | sub-line | color | x |
  |---|---|---|---|
  | Alice's phone | screen shows no signal | #999 | 30 |
  | Bank sends code | reset by text message | #3949ab | 290 |
- **Detour arrow:** solid `#d84315` width-2 arrow with filled head from (460, 173) to (595, 107); label "detours" bold 13px `#d84315` centered at (555, 155).
- **Silent link:** dashed `#999` width-1.5 line from (288, 200) to (202, 200); label "never arrives" 13px `#999` centered at (245, 192).
- **Bottom line (bold 14px `#c2185b`, centered, y=262):** "The code still goes to the number — the number just lives somewhere new."
- **Caption (bottom center, 13px `#999`, y=285):** "Nothing on Alice's phone was touched; the detour happened at the carrier."

## 3. What limits the damage

Each defense removes one link the takeover chain needs.

- **Risk:** codes sent only by text travel with the number.
- **Defense:** a code set with the carrier blocks the transfer (PIN).
- **Defense:** app-made codes never travel over text (authenticator).
- **Fact:** app codes stay put even when the number moves.
- **Defense:** guard the recovery inbox hardest of all accounts.
- **Defense:** a long stretch of no signal deserves a carrier call.

**Key point:** **Win:** move the master key off the phone number and the cascade never starts.

### Visualization (canvas `c3`, 720×300)

Defense rows: one row per setup, each with a blocked/passes status pill; a cross icon marks the row the takeover passes, check icons mark the rows it is blocked.

- **Title (bold 16px `#283593`, top center):** "Where the takeover chain gets cut".
- **Rows** at y = 58, 103, 148, 193 (item text 14px `#2c3e50` left-aligned at x=95, baseline row y+22; status pill = rect at x=510, width 150, height 26 at row y+2, fill = pill color at 0.12 alpha, stroke = pill color width 2, pill text bold 13px in pill color centered at (585, row y+19)):
  | Item | pill text | color | icon |
  |---|---|---|---|
  | reset codes sent only by text | passes | #d84315 | cross |
  | a carrier code required before any transfer | blocked | #00796b | check |
  | sign-in codes made by an app on the device | blocked | #00796b | check |
  | recovery inbox guarded by app codes | blocked | #00796b | check |
- **Check icon:** stroked `#00796b` width-2.5 polyline from (58, row y+16) to (64, row y+22) to (76, row y+8).
- **Cross icon:** stroked `#d84315` width-2.5 lines from (60, row y+8) to (74, row y+22) and from (74, row y+8) to (60, row y+22).
- **Bottom line (bold 14px `#283593`, centered, y=255):** "Texted codes travel with the number; app codes stay with the device."
- **Caption (bottom center, 13px `#999`, y=285):** "One carrier code and one hardened inbox remove most of the cascade."

## Regeneration instructions

- **Layout:** backlog detail-page style — h1 with `border-bottom: 2px solid #3949ab`, `.subtitle`, one `.intro` callout, then one `.lang-section` per numbered h2 (also blue-underlined). Each section holds a `table.layout` (border-collapse, full width) with one `<tr>`: left `td.text-col` (45%) for paragraphs/bullets/`.key-point`, right `td.viz-col` (55%) for the canvas.
- **Bullet style:** every bullet is one non-wrapping line — a bold colored label plus a phrase of roughly 55 characters or fewer; in HTML the label is `<span class="pt-label" style="color:COLOR">Label:</span>` with `.pt-label { font-weight: bold; }`. Long ideas are split into more labeled bullets, never wrapped. Lead paragraphs are at most one short sentence.
- **Label colors by meaning:** `#283593` blue = mechanism/fact (Fact, Mechanism); `#00796b` green = defense/win (Defense, Win); `#d84315` red = risk/loss (Risk, Seen); `#c2185b` pink = scene/context (Scene). Key-point boxes open with the same colored bold lead word (Risk, Win) followed by one short sentence.
- **Page CSS:** body system-ui sans-serif, background `#fff`, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#283593`; h2 1.3rem `#283593`; `.subtitle` `#666` 0.95rem; `.intro` background `#f0f4f8` with `border-left: 3px solid #3949ab`; `.key-point` background `#f8f9fa` with `border-left: 3px solid #d84315`, 0.9rem; `.pt-label` bold; ul 0.92rem; canvases `width: 100%` with `1px solid #e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic sizes `c1` 720×300, `c2` 720×300, `c3` 720×300; a shared `setupCanvas(id, w, h)` helper sizes the backing store from the displayed width times `window.devicePixelRatio` and scales the context accordingly. Each chart is drawn by a named function (`drawC1`, `drawC2`, `drawC3`); a `renderAll()` runs once on load and again on window resize (debounced 150 ms) so canvases re-render sharp at any width. All chart data is literal hardcoded coordinates — no randomness, no dates.
- **Palette:** primary indigo `#283593`, teal `#00796b`, red `#d84315`, pink `#c2185b`, plus `#3949ab`, `#6d4c41`, gray text `#666`/`#999`.
- **Content rule:** tone stays mechanical and neutral — describe the mechanism, no drama words, no attributed intent, no absolutes ("most sites", not "all sites"). Each technical term (SIM swap, PIN, authenticator) appears at most once, in parentheses. Fictional naming only (Alice); no real company names. No realistic credentials, codes, or account secrets anywhere on the page.
- In regenerated HTML, any card/page links use `.html` extensions (this page has none).
