# Invoice & Wire Fraud in Email Threads

**Page type:** detail page (backlog kusto-style two-column layout: one `.lang-section` per numbered h2, text left ~45%, canvas right ~55%)
**HTML title tag:** Invoice & Wire Fraud in Email Threads

**Subtitle:** The middleman does not intercept the wire — they join the conversation weeks earlier, wait for the money moment, and edit one field: the account number.

**Intro callout (blue-left-border box):** This is a man-in-the-middle on trust, not on wires: the hijacked email thread IS the trust signal, and that is what gets forged. Widely known as business email compromise.

## 1. The setup: threads carry authority

Alice is buying a home; Bob is the escrow agent handling the money.

- **Scene:** months of email — inspection, loan, closing date.
- **Fact:** threads accumulate names, deal details, and tone.
- **Fact:** people judge legitimacy by the thread, not the address.
- **Scene:** big transfers are scheduled and announced in-thread.
- **Fact:** anyone reading the thread can read the schedule too.

**Key point (red-left-border box):** **Risk:** the thread itself is the trust signal — and a thread can be copied.

### Visualization (canvas `c1`, 720×300)

Vertical email-thread diagram with a silent reader attached to the mailbox.

- **Title (bold 13px `#1a5276`, top center):** "One thread, weeks of context — and a silent extra reader".
- **Thread label (11px `#666`, centered at (200, 42)):** "Alice ↔ Bob escrow thread".
- **Messages** — four boxes 300×36 at x=50, y = 52, 98, 144, 190 (fill = message color at 0.12 alpha, stroke = message color width 2; text 11px in message color, left-aligned at x=62, baseline at box y+22):
  | Text | color | y |
  |---|---|---|
  | Alice: inspection done, on track | #2980b9 | 52 |
  | Bob: closing set for the 14th | #1a5276 | 98 |
  | Alice: loan approved, funds ready | #2980b9 | 144 |
  | Bob: wire details coming next week | #1a5276 | 190 |
- **Silent reader icon (all `#e74c3c`):** head = circle center (560, 105) radius 13, fill at 0.12 alpha then stroked width 2; shoulders = stroked width-2 upper half-arc centered (560, 148) radius 22 (from PI to 2·PI).
- **Reader labels (centered at x=560):** bold 12px `#e74c3c` "silent reader" at y=185; 11px `#e74c3c` "reads for weeks," at y=202 and "sends nothing" at y=216.
- **Dashed lines:** `#e74c3c` width 1, dash pattern [4,3], from the right edge of each message — (356, 70), (356, 116), (356, 162), (356, 208) — converging to (532, 120); reset dash after.
- **Bottom line (bold 12px `#e67e22`, centered, y=252):** "Every message adds context the attacker will later copy."
- **Caption (bottom center, 11px `#999`, y=285):** "Legitimacy is judged by the thread — not by the sender's address."

## 2. The trick: one edited field at the right moment

The break-in happens weeks before the money moves.

- **Mechanism:** entry is commonly a stolen password (phishing).
- **Scene:** the attacker reads quietly and learns the schedule.
- **Edit:** at the money moment: "updated wire instructions".
- **Mechanism:** sent from the real mailbox, or one letter off.
- **Mechanism:** the message copies the thread's history and tone.
- **Fact:** it reads as a continuation, not a new contact.
- **Edit:** only one number changed: the destination account.

**Key point:** **Risk:** everything a reader checks still matches — only the account number is new.

### Visualization (canvas `c2`, 720×300)

Side-by-side schematic message cards — identical labeled fields, one field highlighted red.

- **Title (bold 13px `#1a5276`, top center):** "Two messages, one changed field".
- **Cards** — two boxes 280×185 at y=48 (fill = card color at 0.06 alpha, stroke = card color width 2; header bold 12px in card color centered at card x+140, y=70):
  | Header | color | x |
  |---|---|---|
  | Original wire instructions | #27ae60 | 50 |
  | The attacker's version | #e74c3c | 390 |
- **Fields** — five lines per card, 11px, left-aligned at card x+18, baselines y = 98, 124, 150, 176, 202:
  - Left card (all `#2c3e50`): "from: bob@escrow-co" / "subject: closing — wire instructions" / "amount: (same)" / "bank name: (same)" / "account: (original)".
  - Right card: "from: bob@escrovv-co — one letter off" in `#e67e22`; then "subject: closing — wire instructions", "amount: (same)", "bank name: (same)" in `#2c3e50`; last row "account: (changed)" in bold 11px `#e74c3c` over a highlight rect at (398, 188, 264, 22) filled `#e74c3c` at 0.15 alpha.
- **Bottom line (bold 12px `#e74c3c`, centered, y=255):** "Everything matches the thread — except where the money goes."
- **Caption (bottom center, 11px `#999`, y=285):** "Schematic — no real names or numbers."

## 3. What stops it: leave the channel to verify

One habit defeats the whole scheme.

- **Defense:** call a number you already had on file.
- **Defense:** never trust a phone number inside the email.
- **Defense:** verify before acting on changed payment details.
- **Risk:** wires move fast; recovery is often hours, not days.
- **Fact:** some banks match recipient name to the account.
- **Fact:** that name check varies by country and bank.
- **Defense:** freeze payment-detail changes near closing.
- **Defense:** require dual approval for any new payee.

**Key point:** **Win:** a call on a known number travels a channel the attacker does not hold (out-of-band verification).

### Visualization (canvas `c3`, 720×300)

Verification-loop diagram — the email channel with a red edit point, bypassed by a green phone-call arc.

- **Title (bold 13px `#1a5276`, top center):** "Verification travels outside the email channel".
- **End boxes** 110×50 at y=125 (fill `#1a5276` at 0.12 alpha, stroke `#1a5276` width 2; name bold 13px `#1a5276` centered at y=150; sub-line 10px `#666` centered at y=166):
  | Name | sub-line | x |
  |---|---|---|
  | Alice | about to wire | 40 |
  | Bob | escrow agent | 570 |
- **Email channel:** `#999` width-1.5 horizontal lines from (150, 150) to (300, 150) and from (420, 150) to (570, 150); label 11px `#666` centered at (360, 192): "email channel — compromised".
- **Edit point box:** 120×46 at (300, 127), fill `#e74c3c` at 0.12 alpha, stroke `#e74c3c` width 2; bold 12px `#e74c3c` "edit point" centered at (360, 146); 10px `#e74c3c` "account swapped" centered at (360, 162).
- **Green arc:** quadratic curve from (95, 122) to (625, 122) with control point (360, 18), stroke `#27ae60` width 2.5; small filled `#27ae60` arrowhead at the Bob end.
- **Arc labels (centered at x=360):** bold 12px `#27ae60` "phone call to a known number" at y=90; 11px `#27ae60` "not one from the email" at y=106.
- **Bottom line (bold 12px `#27ae60`, centered, y=250):** "Confirm the account on the call — then send the wire."
- **Caption (bottom center, 11px `#999`, y=285):** "The check must travel a channel the middleman does not hold."

## Regeneration instructions

- **Layout:** backlog detail-page style — h1 with `border-bottom: 2px solid #2980b9`, `.subtitle`, one `.intro` callout, then one `.lang-section` per numbered h2 (also blue-underlined). Each section holds a `table.layout` (border-collapse, full width) with one `<tr>`: left `td.text-col` (45%) for paragraphs/bullets/`.key-point`, right `td.viz-col` (55%) for the canvas.
- **Bullet style:** every bullet is one non-wrapping line — a bold colored label plus a phrase of roughly 55 characters or fewer; in HTML the label is `<span class="pt-label" style="color:COLOR">Label:</span>` with `.pt-label { font-weight: bold; }`. Long ideas are split into more labeled bullets, never wrapped. Lead paragraphs are at most one short sentence.
- **Label colors by meaning:** `#1a5276` blue = mechanism/fact (Fact, Mechanism); `#27ae60` green = defense/win (Defense, Win); `#e74c3c` red = risk/loss (Risk, Edit); `#e67e22` orange = scene/context (Scene). Key-point boxes open with the same colored bold lead word (Risk, Win) followed by one short sentence.
- **Page CSS:** body system-ui sans-serif, background `#fff`, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; h2 1.3rem `#1a5276`; `.subtitle` `#666` 0.95rem; `.intro` background `#f0f4f8` with `border-left: 3px solid #2980b9`; `.key-point` background `#f8f9fa` with `border-left: 3px solid #e74c3c`, 0.9rem; `.pt-label` bold; ul 0.92rem; canvases `width: 100%` with `1px solid #e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic sizes `c1` 720×300, `c2` 720×300, `c3` 720×300; a shared `setupCanvas(id, w, h)` helper sizes the backing store from the displayed width times `window.devicePixelRatio` and scales the context accordingly. All data arrays are literal and hardcoded — no `Math.random`, no `Date.now`.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, plus `#2980b9`, `#8e44ad`, gray text `#666`/`#999`.
- **Content rule:** all names, addresses, and account fields are schematic and fictional (Alice/Bob, `bob@escrow-co` vs `bob@escrovv-co` as a lookalike-domain illustration only); never render realistic account numbers, routing numbers, or real-entity email addresses anywhere on the page. Tone stays mechanical and neutral — describe the process, not the victims.
- In regenerated HTML, any card/page links use `.html` extensions (this page has none).
