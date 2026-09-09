# Stolen One-Time Codes

**Page type:** detail page (backlog kusto-style two-column layout: one `.lang-section` per numbered h2, text left ~45%, canvas right ~55%)
**HTML title tag:** Stolen One-Time Codes

**Subtitle:** The thief's login attempt triggers a real code to your phone — and a convincing caller talks you into reading it back.

**Intro callout (blue-left-border box):** The code is genuine, the text message is genuine, the sender is genuine — the only fake thing on the line is the person asking for it.

## 1. The call: urgency from someone who already knows you

The caller's credibility is assembled from data that is already stolen.

- **Scene:** a caller claims to be the fraud desk stopping a transfer.
- **Fact:** the caller already knows Alice's name and last digits.
- **Mechanism:** breach lists supply those personal details in bulk.
- **Mechanism:** caller ID can show the bank's real number (spoofing).
- **Mechanism:** urgency discourages hanging up and calling back.
- **Risk:** Alice is primed to expect a security code.

**Key point (red-left-border box):** **Risk:** everything checkable in the moment — name, number, last digits — checks out; none of it proves the caller.

### Visualization (canvas `c1`, 720×300)

Phone-call scene: caller box on the left in danger color listing what the thief already knows, Alice box on the right, an arrow carrying the pretext, and a spoofing annotation underneath.

- **Title (bold 16px `#0277bd`, top center, y=20):** "The call that checks out — except for the caller".
- **Caller box:** rect at (50, 60) size 220×110, fill `#ad1457` at 0.12 alpha, stroke `#ad1457` width 2; title "Caller (thief)" bold 14px `#ad1457` centered at (160, 84); sub-lines 12px `#666` centered at x=160: "knows Alice's name" y=106, "knows the card's last digits" y=124, "displays the bank's number" y=142.
- **Alice box:** rect at (450, 60) size 220×110, fill `#0277bd` at 0.12 alpha, stroke `#0277bd` width 2; title "Alice" bold 14px `#0277bd` centered at (560, 84); sub-lines 12px `#666` centered at x=560: "sees the bank's number" y=106, "hears an urgent transfer story" y=124, "waits for a security code" y=142.
- **Arrow:** width-1.5 horizontal arrow with filled triangular head from (270, 115) to (448, 115) in `#ad1457`; label above, 12px `#999` centered at (360, 105): "“we are stopping a transfer”"; label below, 12px `#999` centered at (360, 132): "“please stay on the line”".
- **Spoof annotation (13px `#ad1457`, centered at (360, 205)):** "the caller ID is spoofed — it matches the bank's genuine number".
- **Bottom line (bold 14px `#e65100`, centered, y=250):** "Name, number, last digits — everything checkable in the moment checks out."
- **Caption (bottom center, 13px `#999`, y=285):** "Breach lists supply the details; the phone network lets the displayed number be faked."

## 2. The trigger: their login, your code

The genuine code is the thief's missing piece — the call exists to fetch it.

- **Mechanism:** the thief enters Alice's password on the real site.
- **Fact:** the password came from an earlier breach list.
- **Mechanism:** the real site sends a genuine code to Alice's phone.
- **Scene:** the caller asks her to read back the code just sent.
- **Risk:** the code typed by the thief completes their sign-in.
- **Fact:** the theft fits inside the code's short validity window.

**Key point:** **Risk:** the code arrives from the bank's genuine number, in the thread with past real codes — the message cannot reveal who triggered it.

### Visualization (canvas `c2`, 720×300)

Flow diagram: four boxes left to right from the thief's login attempt to the thief's completed sign-in, with a dashed elbow underneath showing the code's round trip back by voice.

- **Title (bold 16px `#0277bd`, top center, y=20):** "Their login attempt, her code".
- **Boxes** 130×54 at y=88 (fill = box color at 0.12 alpha, stroke = box color width 2; title bold 14px in box color centered at y=110; sub-line 12px `#666` centered at y=128):
  | Title | sub-line | color | x |
  |---|---|---|---|
  | Thief | enters a stolen password | #ad1457 | 30 |
  | Real site | sends a genuine code | #0277bd | 210 |
  | Alice's phone | the real code arrives | #6a1b9a | 390 |
  | Thief signs in | their session opens | #ad1457 | 570 |
- **Arrows:** width-1.5 horizontal arrows with filled triangular heads at y=115: from (160,115) to (208,115) in `#ad1457`; from (340,115) to (388,115) in `#0277bd`; from (520,115) to (568,115) in `#ad1457`. Arrow labels 12px `#999` centered at y=105: "login attempt" at (184, 105); "code sent" at (364, 105); "read aloud" at (544, 105).
- **Round-trip elbow:** dashed `#999` width-1.5 polyline (635,142) → (635,200) → (275,200) → (275,152), then filled `#999` triangle head pointing up with points (271,154), (279,154), (275,146); label "the code travels back by voice and completes the thief's sign-in" 13px `#999` centered at (455, 218).
- **Bottom line (bold 14px `#e65100`, centered, y=262):** "The code checked possession of the phone — and its owner gave the answer away."
- **Caption (bottom center, 13px `#999`, y=285):** "The whole exchange fits inside the code's short validity window."

## 3. What limits the damage: the code never travels by voice

The strongest habits keep the code out of the conversation entirely.

- **Defense:** a code is only typed into the screen that asked for it.
- **Fact:** banks never call to ask for a code to be read back.
- **Fact:** a read-back request is itself the tell.
- **Defense:** hanging up and dialing the card's number breaks spoofing.
- **Defense:** a prompt naming the action beats a bare code.
- **Defense:** device-bound sign-in leaves nothing to ask for (passkey).
- **Defense:** reporting the call lets the bank freeze the attempt.

**Key point:** **Win:** one habit defeats the whole scheme — the code goes into a screen, never into a conversation.

### Visualization (canvas `c3`, 720×300)

Layered-defense rows: one row per habit or setup, item text and sub-line on the left, outcome pill on the right in red (passes) or green (blocked / refused / nothing to steal).

- **Title (bold 16px `#0277bd`, top center, y=20):** "Layered checks: how far the read-back scam gets".
- **Rows** at y = 52, 100, 148, 196 (item text bold 14px `#2c3e50` left-aligned at (50, y+10); sub-line 12px `#666` left-aligned at (50, y+27); outcome pill = rect at x=530, width 130, height 26 at row y, fill = pill color at 0.12 alpha, stroke = pill color width 2, pill text bold 13px in pill color centered at (595, y+17)):
  | Item | sub-line | pill text | color |
  |---|---|---|---|
  | Code read aloud to the caller | the thief's sign-in completes | passes | #ad1457 |
  | Hang up, dial the number on the card | the spoofed line is gone | blocked | #33691e |
  | Prompt that names the action | an unexpected approval is refused | refused | #33691e |
  | Sign-in bound to the device | no code exists for the caller to request | nothing to steal | #33691e |
- **Bottom line (bold 14px `#0277bd`, centered, y=252):** "Every safe row keeps the code off the phone line entirely."
- **Caption (bottom center, 13px `#999`, y=285):** "A quick report lets the bank freeze the sign-in before money moves."

## Regeneration instructions

- **Layout:** backlog detail-page style — h1 with `border-bottom: 2px solid #0288d1`, `.subtitle`, one `.intro` callout, then one `.lang-section` per numbered h2 (also underlined in `#0288d1`). Each section holds a `table.layout` (border-collapse, full width) with one `<tr>`: left `td.text-col` (45%) for paragraphs/bullets/`.key-point`, right `td.viz-col` (55%) for the canvas.
- **Bullet style:** every bullet is one non-wrapping line — a bold colored label plus a phrase of roughly 55 characters or fewer; in HTML the label is `<span class="pt-label" style="color:COLOR">Label:</span>` with `.pt-label { font-weight: bold; }`. Long ideas are split into more labeled bullets, never wrapped. Lead paragraphs are at most one short sentence.
- **Label colors by meaning:** `#0277bd` primary = mechanism/fact (Fact, Mechanism); `#33691e` green = defense/win (Defense, Win); `#ad1457` red = risk/loss (Risk); `#e65100` orange = scene/context (Scene). Key-point boxes open with the same colored bold lead word (Risk, Win) followed by one short sentence.
- **Page CSS:** body system-ui sans-serif, background `#fff`, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#0277bd`; h2 1.3rem `#0277bd`; `.subtitle` `#666` 0.95rem; `.intro` background `#f0f4f8` with `border-left: 3px solid #0288d1`; `.key-point` background `#f8f9fa` with `border-left: 3px solid #ad1457`, 0.9rem; `.pt-label` bold; ul 0.92rem; canvases `width: 100%` with `1px solid #e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic sizes `c1` 720×300, `c2` 720×300, `c3` 720×300; a shared `setupCanvas(id, w, h)` helper sizes the backing store from the displayed width times `window.devicePixelRatio` and scales the context accordingly. Each chart is a named `drawC1()`/`drawC2()`/`drawC3()` function; a `renderAll()` call runs them once and again on window resize (debounced 150ms) so canvases stay sharp after resize. All chart data is literal hardcoded coordinates — no randomness, no dates.
- **Palette:** primary `#0277bd`, secondary `#0288d1`, green `#33691e`, red `#ad1457`, orange `#e65100`, plus `#6a1b9a`, gray text `#666`/`#999`.
- **Content rule:** tone stays mechanical and neutral — describe the mechanism, no drama words, no attributed intent, no absolutes where they do not hold. Each technical term (spoofing, passkey) appears at most once, in parentheses. Fictional naming only (Alice); no real company names; no realistic credential strings anywhere — refer to "the code", never a plausible-looking value. This page covers only the social-engineering read-back scam; number-takeover attacks are out of scope here.
- In regenerated HTML, any card/page links use `.html` extensions (this page has none).
