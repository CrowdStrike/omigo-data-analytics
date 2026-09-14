# MFA & Passkeys

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** MFA & Passkeys

**Subtitle:** A phishing page can steal a password and a 6-digit code in seconds — a passkey signs only for the real site, so there is nothing to steal

## The Fake Login Page That Steals the Code Too

**Tags:** `core idea` (blue), `phishing` (red), `origin check` (green)

- **The lure** — a coffee shop manager gets an "urgent payroll" email linking to `payro11.example`, a lookalike of `payroll.example`
- **The steal** — she types her password `latte#9` and the 6-digit code `481292` from her authenticator app
- **The relay** — the fake page forwards both to the real site in 9 seconds, well inside the code's 30-second window
- **The takeover** — the attacker's browser is now logged in; the code "worked" exactly once, for the wrong person
- **The passkey** — her passkey was registered to `payroll.example`; on `payro11.example` the browser refuses to sign
- **Nothing to relay** — no secret ever crosses the wire, so the fake page captures nothing usable

*Example (italic):* The same lookalike page that beat her password-plus-code in 9 seconds gets zero bytes from her passkey — the origin string in the signature simply doesn't match.

**Key point:** Codes are secrets a human can be tricked into retyping on the wrong site; a passkey is a signature the browser only produces for the exact origin it was registered to.

### Visualization (canvas `c1`, 720×300)

Two-row flow diagram: password + 6-digit code relayed through the fake page (attacker gets in) vs a passkey login where the browser's origin check stops the signature.

- **Title (bold 15px, `#1a5276`, top center):** "Same Fake Page, Two Outcomes: Codes Relay, Passkeys Refuse".
- **Row 1 (y=95), label 12px `#444` at x=20:** "password + code"; blue `#2a78d6` rounded box at x=150 labeled "types latte#9 + 481292" (12px), 3px arrow to an orange `#d95926` box at x=350 labeled "payro11.example relays in 9s", 3px arrow to a red `#e74c3c` box at x=555 labeled "attacker logged in" with bold 12px red "✗ account taken".
- **Row 2 (y=205), label:** "passkey"; blue box at x=150 labeled "passkey bound to payroll.example", 3px arrow to a green `#008300` box at x=350 labeled "browser checks origin: payro11 ≠ payroll", then a stubby dashed `#6b7280` arrow to a green box at x=555 labeled "no signature sent" with bold 12px green "✓ nothing stolen".
- **Box style:** 150–175px wide, 44px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(217,89,38,0.12)` / `rgba(231,76,60,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=270):** "the code is a secret you can retype; the signature is bound to the real origin".
- **Caption (12px `#444`, bottom right):** "9-second relay illustrative".

## Know, Have, Are: Lining Up the Second Factors

**Tags:** `worked example` (blue), `three factors` (green), `comparison` (orange)

- **Know** — a password or PIN lives in your head, so it can be guessed, leaked, or retyped on a fake page
- **Have** — a phone (SMS, TOTP app, push prompt) or a hardware key proves you hold a specific device
- **Are** — a fingerprint or face unlocks the device locally; it never leaves the phone or laptop
- **The codes** — SMS and TOTP both produce a 6-digit number a human can hand to a fake page in real time
- **The push** — a push prompt asks "was this you?"; a tired thumb can tap Approve on an attacker's login
- **The passkey** — a device-held private key signs a challenge that names the origin, so relaying is useless

*Example (italic):* Score four methods on three tests — stops a leaked password, stops a real-time relay, stops prompt-spam — and only the passkey passes all three.

**Key point:** All second factors beat a leaked password alone; only origin-bound signatures (passkeys, hardware keys) also beat a live phishing relay.

### Visualization (canvas `c2`, 720×300)

Scorecard grid: four methods (rows) against three attack tests (columns), each cell a green check or red cross.

- **Title (bold 15px, `#1a5276`, top center):** "Four Second Factors, Three Attacks: Only Passkeys Sweep the Row".
- **Grid geometry:** row labels 12px `#2c3e50` at x=30, rows at y = 100, 145, 190, 235: "SMS code", "TOTP app", "Push prompt", "Passkey"; column headers bold 12px `#444` at y=70 centered on x = 300, 460, 620: "stops leaked password", "stops live relay", "stops prompt spam"; light `#e5e9ef` 1px separator lines between rows.
- **Cells:** bold 16px marks centered on the (column, row) intersections — SMS: `["✓","✗","✗"]`, TOTP: `["✓","✗","✗"]`, Push: `["✓","✗","✗"]`, Passkey: `["✓","✓","✓"]`; checks green `#008300`, crosses red `#e74c3c`.
- **Row highlight:** passkey row background band `rgba(0,131,0,0.08)` from x=20 to x=700, height 34px.
- **Annotation (bold 13px aqua `#199e70`, bottom center near y=272):** "codes and taps can be handed over; an origin-bound signature cannot".
- **Caption (12px `#444`, bottom right):** "scorecard schematic".

## One Leaked Password Shouldn't End the Account

**Tags:** `where it's used` (blue), `account takeover` (red), `defense in depth` (green)

- **The leak** — password dumps and reuse mean attackers often start with a valid password already in hand
- **The stakes** — a taken-over account means drained balances, sent-as-you email, and reset chains into other sites
- **The drill** — imagine 1,000 accounts hit with a convincing relay-phishing campaign, each defense in place
- **Password only** — 760 of the 1,000 fall: nothing stands between the leaked password and the login
- **Codes help** — SMS drops it to 240, a TOTP app to 190, push prompts to 130 — better, but all relayable
- **Passkeys end it** — 0 of the 1,000 fall to the relay, because there is no secret for the page to capture

*Example (italic):* In the 1,000-account drill the jump from nothing to SMS saves 520 accounts, but only the passkey column reads zero.

**Key point:** Any MFA collapses the easy attacks, but against a live phishing relay the takeover count only reaches zero when the second factor is unphishable by design.

### Visualization (canvas `c3`, 720×300)

Vertical bar chart: takeovers out of 1,000 relay-phished accounts under five defenses.

- **Title (bold 15px, `#1a5276`, top center):** "Relay-Phishing Drill: Takeovers per 1,000 Accounts".
- **Axes:** origin x=70, baseline y=245, plot width 590, plot height 180; y = 0 to 800, gridlines `#e5e9ef` at 200/400/600/800 with 12px `#444` labels; x = five bars centered at x = 140, 255, 370, 485, 600 with 12px `#444` labels "password only", "SMS", "TOTP", "push", "passkey".
- **Bars:** 64px wide, values `[760, 240, 190, 130, 0]`; colors red `#e74c3c`, orange `#d95926`, yellow `#c98500`, blue `#2a78d6`, green `#008300`; bold 12px matching-color value labels above each bar (the passkey bar gets "0" at the baseline).
- **Annotation (bold 13px green `#008300`, near x=600, y=110):** "0 — nothing to relay, nothing to replay".
- **Caption (12px `#444`, bottom right):** "takeover counts illustrative".

## "The Code Proves It's Me" — and Other Traps

**Tags:** `common mistake` (red), `SMS phishing` (orange), `MFA fatigue` (blue)

- **The belief** — people treat a 6-digit code as proof of identity; it only proves someone has the code right now
- **SMS is weakest** — codes arrive over a channel that can be relayed, and SIM-swaps can move the number itself
- **TOTP relays too** — the authenticator app is offline, but the human still retypes its output onto whatever page asks
- **Fatigue attacks** — attackers replay a stolen password all night, firing push prompts until a sleepy tap approves
- **The 23rd tap** — in the drill below the victim denies 22 prompts between 1:00 and 1:37am and approves the 23rd
- **The fix** — number-matching prompts blunt fatigue; passkeys remove the retypable secret entirely

*Example (italic):* Twenty-eight push prompts land between 1:00 and 1:45am; the first 22 get denied, prompt 23 gets a bleary Approve at 1:37am, and the last 5 no longer matter.

**Common mistake:** Believing "we have MFA" closes the phishing hole. If the factor is a code a human retypes or a prompt a human taps, a patient attacker phishes or fatigues straight through it.

### Visualization (canvas `c4`, 720×300)

Timeline dot chart: 28 push prompts between 1:00am and 1:45am, denied taps in blue, the single approval at prompt 23 in red.

- **Title (bold 15px, `#1a5276`, top center):** "MFA Fatigue: 22 Denials, Then One 1:37am Approve".
- **Axes:** origin x=60, baseline y=230, plot width 600, plot height 150; x = time 1:00 to 1:45 with 12px `#444` tick labels every 15 min ("1:00", "1:15", "1:30", "1:45"); y = prompt number 0 to 30, gridlines `#e5e9ef` at 10/20/30 with 12px `#444` labels.
- **Denied dots:** blue `#2a78d6` 5px-radius dots for prompts 1–22 at minutes `[0,2,4,5,7,9,10,12,14,15,17,19,20,22,24,26,27,29,31,33,35,36]` with prompt numbers 1..22 on the y scale.
- **Approve dot:** red `#e74c3c` 8px-radius dot at minute 37, prompt 23, with bold 13px red label "Approve — account taken" to its right.
- **Trailing dots:** mute `#6b7280` 5px dots for prompts 24–28 at minutes `[39,40,42,43,45]` (attacker already in).
- **Annotation (bold 13px orange `#d95926`, near x=1:10, y=70):** "the password was already stolen — the prompts are the whole attack".
- **Caption (12px `#444`, bottom right):** "prompt times illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); the 9-second relay, the takeover counts `[760, 240, 190, 130, 0]`, and the 28 prompt times with the approval at prompt 23 / minute 37 are invented and labeled illustrative; the scorecard marks (SMS/TOTP/push fail "stops live relay" and "stops prompt spam", passkey passes all three) reflect the standard defensive-security consensus, not measurements.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
