# Recovery Credentials

**Page type:** detail page (backlog kusto-style two-column layout: one `.lang-section` per numbered h2, text left ~45%, canvas right ~55%)
**HTML title tag:** Recovery Credentials

**Subtitle:** Reset links, one-time codes, backup keys, and every other way back into an account are credentials in their own right — and because an attacker gets to pick the weakest accepted path in, they set the account's real strength.

**Intro callout (blue-left-border box):** **Weakest path:** the attacker picks the weaker of front door and recovery, so the weakest accepted path sets the account's real strength — recovery artifacts deserve primary-credential scrutiny.

## 1. Recovery credentials are credentials

Every account ships with more than one entrance.

- **Front door:** the primary credential plus whatever MFA guards it.
- **Recovery path:** humans forget passwords, lose devices, change numbers.
- **Same target:** both paths end at the same account.
- **Attacker's pick:** free to attempt whichever path is cheaper.
- **Bearer secrets:** reset links, code sets, printed backup keys.
- **Same questions:** entropy, lifetime, storage, and how they die.
- **History:** a fraction of the scrutiny passwords received.
- **Result:** account-takeover attacks concentrate here.

**Key point (red-left-border box):** **Minimum rule:** effective strength is the minimum over every accepted path — a hardware-key door behind a guessable reset is a guessable account.

### Visualization (canvas `c1`, 720×300)

Two-door diagram: front door and recovery path both leading into one account box, with the attacker choosing the weaker arrow.

- **Title (bold 13px `#1a5276`, top center):** "The weakest accepted path sets the real strength".
- **Path boxes** 200×70 at x=40 (fill = color at 0.12 alpha, stroke = color width 2; title bold 13px in color at box top; sub-line 11px `#2c3e50` below it):
  | Title | color | y | sub-line |
  |---|---|---|---|
  | Front door | #27ae60 | 60 | password + MFA |
  | Recovery path | #e74c3c | 180 | email reset link |
- **Account box** 160×100 at x=520, y=100: fill `#1a5276` at 0.12 alpha, stroke `#1a5276` width 2, label "Account" bold 14px `#1a5276` centered.
- **Arrows:** front-door arrow `#bbb` width 1.5 from (240, 95) to (512, 130); recovery arrow `#e74c3c` width 2.5 from (240, 215) to (512, 170); both with filled triangular arrowheads (7px) in the line color.
- **Attacker label:** bold 11px `#e74c3c` at (330, 245), left-aligned: "attacker picks this one".
- **Caption (bottom center, 11px `#999`):** "Effective strength = min(front door, every accepted recovery path)".

## 2. The recovery method catalog

- **Secondary email:** proves control of another mailbox.
- **Fails when:** that mailbox is weaker, abandoned, or in a loop.
- **Recovery phone:** proves control of a phone number.
- **Fails to:** SIM swap, recycling, carrier social engineering.
- **Security questions:** knowledge fallback that persists anyway.
- **Flaw:** answers are low-entropy and publicly researchable.
- **Scope note:** their full design is another page's topic.
- **One-time codes:** prove possession of a set issued at enrollment.
- **Fails when:** the stash is copied or quietly exhausted.
- **Printed backup key:** proves possession of an offline artifact.
- **Fails to:** loss, fire, or one photograph of the printout.
- **Trusted contacts:** a quorum of designated friends vouches.
- **Fails when:** the quorum is befriended, hacked, or stale.
- **Government ID + liveness:** proves legal identity, high-value accounts.
- **Checked by:** a human reviewer or a model.
- **Fails to:** forged documents and injected synthetic imagery.
- **Cost:** carries a real privacy cost.
- **Support agent:** proves only the ability to persuade a human.
- **Risk:** the social-engineering surface of the whole catalog.
- **Time-delayed window:** notify-then-allow pause; adds no new proof.
- **Win:** the owner is notified and can object in time.
- **Fails when:** the notice lands on an attacker-held channel.

**Key point:** **Proof of control:** each method proves control of a mailbox, number, object, circle, identity, or trust — and fails exactly when that thing is stolen or faked.

### Visualization (canvas `c2`, 720×360)

Horizontal bar spectrum: resistance to account takeover by recovery method.

- **Title (bold 13px `#1a5276`, top center):** "Resistance to account takeover, by recovery method (illustrative)".
- **Bars:** 22px tall, 8px gap, starting y=40; labels right-aligned 12px `#2c3e50` ending at x=250; track `#f0f0f0` 340px max starting at x=262; bar fill = row color at 0.6 alpha with 1px solid stroke in the row color; value in bold 11px `#2c3e50` after the bar.
- **Data (label, value, color):**
  | Recovery method | value | color |
  |---|---|---|
  | Time-delayed window (notify-then-allow) | 88 | #27ae60 |
  | Printed backup key | 80 | #27ae60 |
  | Government-ID + liveness check | 74 | #2980b9 |
  | One-time recovery codes | 70 | #2980b9 |
  | Trusted-contact quorum | 55 | #2980b9 |
  | Secondary email (fresh, strong) | 45 | #e67e22 |
  | Recovery phone (SMS / voice) | 30 | #e67e22 |
  | Support-agent manual recovery | 15 | #e74c3c |
  | Security questions | 8 | #e74c3c |
- **Caption (bottom center, 11px `#999`):** "Historical defaults cluster at the bottom; current norms add possession proofs and deliberate delay".

## 3. Reset links as bearer tokens

A password-reset link is a single-use bearer credential delivered over email.

- **Bearer proof:** whoever holds the link holds the account.
- **Window:** that power lasts for the link's whole lifetime.
- **TTL:** expire in minutes to a few hours, not days.
- **Why:** exposure lasts as long as a leaked link stays live.
- **Single use:** consume the token on first successful use.
- **Win:** replay from inbox, proxy log, or screen fails.
- **Invalidation:** any use or password change voids all links.
- **Why:** no attacker can hold a spare link in reserve.
- **Session kill:** a completed reset ends all active sessions.
- **Why:** otherwise an attacker inside just stays logged in.

**Key point:** **Standing backdoor:** a long-lived or reusable reset link is not a convenience — it survives in inboxes indefinitely.

### Visualization (canvas `c3`, 720×320)

Schematic anatomy of a reset link (labeled boxes only — no realistic URL or token text) plus a four-step lifecycle chain.

- **Title (bold 13px `#1a5276`, top center):** "Anatomy and lifecycle of a reset link (schematic)".
- **Anatomy segments:** three adjacent boxes at y=50, height 44 (fill = color at 0.12 alpha, stroke = color width 2, label bold 11px in color, centered):
  | Label | color | x | width |
  |---|---|---|---|
  | reset endpoint | #1a5276 | 60 | 170 |
  | single-use random token | #e67e22 | 230 | 250 |
  | server-side expiry (TTL) | #2980b9 | 480 | 180 |
- **Mid-line (11px `#666`, centered at y=128):** "a bearer credential delivered over email — possession of the link is the whole proof".
- **Lifecycle chain:** four boxes 150×54 at y=180 (fill = color at 0.12 alpha, stroke = color width 2, two-line label bold 11px in color, centered), connected by `#bbb` width-1.5 arrows with 6px arrowheads in the 25px gaps between boxes:
  | Lines | color | x |
  |---|---|---|
  | Issued / TTL starts | #1a5276 | 30 |
  | Clicked once / token consumed | #27ae60 | 205 |
  | All outstanding / links voided | #e67e22 | 380 |
  | Active sessions / killed | #e74c3c | 555 |
- **Caption (bottom center, 11px `#999`):** "A link that survives reuse or outlives its window is a standing backdoor".

## 4. One-time recovery codes and backup keys

- **Shown once:** the code set is displayed once at enrollment.
- **Hashed at rest:** stored hashed like passwords.
- **No re-display:** the server verifies but can never re-show.
- **Consumed on use:** each code dies the moment it is accepted.
- **Win:** caps what a partial leak is worth.
- **Backup key:** a printed long random key, the last resort.
- **By design:** deliberately inconvenient, off every online channel.
- **Strength:** useful only with physical access to Alice's copy.
- **Open question:** how many codes to issue per set.
- **Rule:** regenerating a set fully invalidates the old one.
- **Detect:** a stash running low before it hits zero.
- **Worse:** codes consumed by someone other than the owner.

**Key point:** **Small passwords:** a code set is a batch of single-use passwords — hashing, no re-display, and breach response apply unchanged.

### Visualization (canvas `c4`, 720×300)

Slot-grid lifecycle of a ten-code recovery set (generic slot labels only — no plausible code text).

- **Title (bold 13px `#1a5276`, top center):** "One-time recovery codes: shown once, hashed at rest, consumed on use".
- **Slot grid:** ten boxes 100×36, five per row, x = 40 + column × 130 (columns 0–4), rows at y=60 and y=110. Slots 1–4 are used: fill `#f0f0f0`, stroke `#ccc` width 1.5, label "code N" 11px `#999` centered, plus a `#e74c3c` width-2 diagonal strike from top-left to bottom-right corner. Slots 5–10 are unused: fill `#27ae60` at 0.10 alpha, stroke `#27ae60` width 1.5, label "code N" 11px `#2c3e50` centered.
- **Annotation lines (centered):**
  - bold 11px `#e67e22` at y=185: "Regenerating a new set invalidates the old set entirely".
  - 11px `#666` at y=210: "Stored hashed like passwords — the server can verify but never re-display them".
  - 11px `#e74c3c` at y=235: "Alert the owner when few codes remain, or when codes burn faster than expected".
- **Caption (bottom center, 11px `#999`):** "A stash that runs out — or gets copied — silently is the failure mode to detect".

## 5. The recovery graph — loops, self-loops, stale nodes

Recovery links between accounts form a directed graph.

- **Nodes and arrows:** each node an account, each arrow "recovers via".
- **Graph rule:** security equals the weakest reachable node.
- **Cycle:** no node in a loop has a trustworthy root.
- **Only fix:** a loop is rebuilt only from outside itself.

**Key point (red-left-border box):** **Circular dependency:** A recovers via B while B recovers via A — compromise either, lose both; neither rebuilds the other, two credentials that are really one.

- **Self-loop:** a mailbox that recovers via its own address.
- **Effect:** the reset goes to whoever locked the owner out.
- **Verdict:** no recovery path at all.
- **Stale secondary:** a forgotten email on an expired or recycled domain.
- **Effect:** it silently becomes the strongest credential.
- **Why:** registering the old address inherits its power.

**Illustrative Example (italic `.example` line):** Alice's password manager resets via her mailbox, her mailbox resets via SMS, and her phone number can be reassigned by a persuasive call to Vendor A's support desk — so the real strength of everything above is that one phone call.

### Visualization (canvas `c5`, 720×320)

Small directed graph: nodes are accounts, arrows mean "recovers via", with one two-node cycle highlighted in red, one self-loop, and one stale node.

- **Title (bold 13px `#1a5276`, top center):** "Recovery links form a directed graph — a cycle has no trustworthy root".
- **Node boxes** 130×40 (fill = color at 0.12 alpha, stroke = color width 2, label bold 12px in color, centered):
  | Label | color | x | y |
  |---|---|---|---|
  | Bank | #1a5276 | 40 | 60 |
  | Mailbox A | #e74c3c | 290 | 60 |
  | Mailbox B | #e74c3c | 540 | 60 |
  | Mailbox C | #8e44ad | 40 | 190 |
  | Phone number | #2980b9 | 290 | 190 |
  | Stale mailbox | #e67e22 | 540 | 190 |
- **Edges** (straight arrows with 6px filled arrowheads at the destination end):
  - Bank → Mailbox A: `#bbb` width 1.5 from (170, 80) to (282, 80).
  - Mailbox A → Mailbox B (cycle): `#e74c3c` width 2 from (420, 72) to (532, 72).
  - Mailbox B → Mailbox A (cycle): `#e74c3c` width 2 from (540, 88) to (428, 88).
  - Mailbox A → Phone number: `#bbb` width 1.5 vertical from (355, 100) to (355, 182).
  - Mailbox B → Stale mailbox: `#e67e22` width 2 vertical from (605, 100) to (605, 182).
- **Self-loop on Mailbox C:** `#8e44ad` width-2 arc of radius 14 centered at (105, 176), drawn from 150° through the top to 30°, with a 5px filled arrowhead at the end pointing back toward the box top; label "recovers via itself" 10px `#8e44ad` centered at (105, 150).
- **Cycle annotation:** bold 11px `#e74c3c` centered at (480, 130): "compromise either — lose both".
- **Stale annotation:** 10px `#e67e22` right-aligned ending at (595, 145): "recycled domain".
- **Legend (11px `#666`, left-aligned at (40, 262)):** "arrow = recovers via".
- **Caption (bottom center, 11px `#999`):** "The security of any account is the security of the weakest node reachable from it".

## 6. Culture decides what works — regional recovery design

Which root of trust recovery stands on is cultural and infrastructural, not technical.

- **Root of trust:** recovery assumes a root the user controls.
- **Candidates:** mailbox, phone, street address, national ID.
- **Consequence:** one design succeeds in a market and fails in another.
- **Japan-style:** postal, carrier, and in-person roots.
- **Postal:** mailed codes to a registered street address.
- **In person:** recovery at a bank branch or carrier shop.
- **Carrier:** mobile-carrier-verified identity.
- **Trait:** slow but bound to a physical address and a face.
- **US-style:** email resets, phone resets, plus support-agent calls.
- **Win:** fast and self-service.
- **Risk:** sits right on the social-engineering surface.
- **European eID:** national electronic-ID login as the root.
- **Win:** inherits the government identity system's strength.
- **Cost:** enrollment friction comes along with it.
- **Shared arc:** knowledge checks and agent chats are fading.
- **Toward:** possession proofs plus deliberate waiting periods.
- **Forms:** notify-then-allow delays on high-value accounts.

**Key point:** **Local bet:** a recovery design bets on the root the local user controls — the wrong bet is unusable or trivially social-engineered.

### Visualization (canvas `c6`, 720×300)

Mapping diagram: four region/era styles, each with an arrow to its trusted recovery roots.

- **Title (bold 13px `#1a5276`, top center):** "Trusted recovery roots, by region and era (illustrative)".
- **Rows:** labeled boxes 190×34 at x=30, centered on y=60/115/170/225 (fill = color at 0.12 alpha, stroke = color width 1.5, label bold 12px in color, centered); `#bbb` width-1.5 arrow from x=220 to x=265 with a filled arrowhead, then a left-aligned 12px `#2c3e50` root list at x=282:
  | Style | color | roots |
  |---|---|---|
  | Historical US-style | #e74c3c | email reset · phone reset · agent call |
  | Japan-style | #1a5276 | postal code to address · carrier ID · in-person |
  | European eID-style | #27ae60 | national electronic-ID login |
  | Current high-value norm | #2980b9 | possession proof + notify-then-allow delay |
- **Caption (bottom center, 11px `#999`):** "Which root a user reliably controls is cultural and infrastructural, not technical".

## Regeneration instructions

- **Layout:** backlog detail-page style — h1 with `border-bottom: 2px solid #2980b9`, `.subtitle`, one `.intro` callout, then one `.lang-section` per numbered h2 (also blue-underlined). Each section holds a `table.layout` (border-collapse, full width) with one `<tr>`: left `td.text-col` (45%) for paragraphs/bullets/`.key-point`/`.example`, right `td.viz-col` (55%) for the canvas.
- **Bullet style:** every bullet is `- **Label:** phrase` — a bold colored label naming the concept plus a phrase short enough to fit on one line in the 45% text column (roughly ≤55 characters). Never let a bullet wrap; split long content into more labeled bullets instead of deleting facts. Lead paragraphs are at most one short sentence.
- **Label colors:** in HTML each bullet label renders as `<span class="pt-label" style="color:...">Label:</span> phrase`, with the color chosen by meaning — `#1a5276` blue = design/fact, `#27ae60` green = win/strength, `#e74c3c` red = flaw/risk, `#e67e22` orange = context/history/trend. The intro callout and each `.key-point` open the same way with a bold colored lead word (`<strong style="color:...">Word:</strong>`) followed by one short sentence.
- **Page CSS:** body system-ui sans-serif, background `#fff`, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; h2 1.3rem `#1a5276`; `.subtitle` `#666` 0.95rem; `.intro` background `#f0f4f8` with `border-left: 3px solid #2980b9`; `.key-point` background `#f8f9fa` with `border-left: 3px solid #e74c3c`, 0.9rem; `.example` italic `#555` 0.9rem; ul 0.92rem; `.pt-label { font-weight: 600; }`; canvases `width: 100%` with `1px solid #e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic sizes `c1` 720×300, `c2` 720×360, `c3` 720×320, `c4` 720×300, `c5` 720×320, `c6` 720×300; a shared `setupCanvas(id, w, h)` helper sizes the backing store from the displayed width times `window.devicePixelRatio` and scales the context accordingly.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, plus `#2980b9`, `#8e44ad`, gray text `#666`/`#999`.
- **Content rule:** all credential anatomy is schematic labeled boxes — never realistic URLs, tokens, or example recovery codes; no company or product names, regions named only qualitatively and factually.
- In regenerated HTML, any card/page links use `.html` extensions (this page has none).
