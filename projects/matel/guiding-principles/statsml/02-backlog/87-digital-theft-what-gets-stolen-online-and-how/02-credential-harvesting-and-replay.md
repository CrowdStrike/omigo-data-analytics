# Credential Harvesting & Replay

**Page type:** detail page (backlog kusto-style two-column layout: one `.lang-section` per numbered h2, text left ~45%, canvas right ~55%)
**HTML title tag:** Credential Harvesting & Replay

**Subtitle:** Saved passwords and signed-in sessions live as small files on your machine — copy those files, and every account they open travels with them.

**Intro callout (blue-left-border box):** No account site is broken into at any point — one infected laptop hands over the saved keys to every account it remembers.

## 1. Harvesting at scale

One infected laptop hands over every saved login at once.

- **Fact:** browsers keep saved passwords in one local file.
- **Mechanism:** malware quietly copies that whole file (infostealer).
- **Mechanism:** it also copies the small signed-in notes (session cookies).
- **Fact:** one infection covers every account saved on the machine.
- **Risk:** harvested batches are bundled and sold as bulk lists.
- **Scene:** Alice loses dozens of account keys in a few seconds.

**Key point (red-left-border box):** **Risk:** one laptop infection hands over the keys to dozens of accounts at once — none of those accounts is ever touched directly.

### Visualization (canvas `c1`, 720×300)

Fan-out schematic: one infected laptop at left, arrows fanning out to a column of harvested account boxes at right.

- **Title (bold 16px `#00695c`, top center, y=20):** "One infection, many accounts — everything saved leaves at once".
- **Laptop box** 160×70 at (40, 105), color `#ad1457` (fill = color at 0.12 alpha, stroke = color width 2): title "Alice's laptop" bold 14px in box color centered at (120, 133); sub-line "malware inside" 12px `#666` centered at (120, 152).
- **Label under laptop** (bold 13px `#ad1457`, centered at (120, 195)): "one pass, all keys".
- **Account boxes** 200×36 at x=430, color `#00695c` (fill = color at 0.12 alpha, stroke = color width 1.5), text bold 13px in box color centered at (530, box y+22):
  | Text | y |
  |---|---|
  | Email — login + session | 44 |
  | Bank — login | 86 |
  | Shopping — login + session | 128 |
  | Work chat — session | 170 |
  | Cloud drive — login | 212 |
- **Arrows:** `#ad1457` width 1.5 with filled triangular heads, one per account box, from (200, 140) to (428, box y+18).
- **Bottom line (bold 14px `#e65100`, centered, y=265):** "The account sites are never contacted — the copying is all local."
- **Caption (bottom center, 13px `#999`, y=285):** "Each box is an account whose saved key now exists in two places."

## 2. Replay: skipping the login entirely

A stolen signed-in session resumes the account without any login.

- **Fact:** after a login, the browser keeps a small proof of it.
- **Mechanism:** that proof can be pasted into another browser.
- **Mechanism:** the account resumes there as-is (replay attack).
- **Fact:** the password itself is never typed anywhere.
- **Risk:** the second sign-in step never triggers (two-factor).
- **Risk:** the real owner sees no prompt and no login alert.
- **Scene:** Bob's tabs reopen on a machine he has never seen.

**Key point:** **Risk:** proof of a past login is treated as the login itself — the door checks never run a second time.

### Visualization (canvas `c2`, 720×300)

Two-path diagram: normal login route above in green passes two checks; replay route below in red goes straight to the open account.

- **Title (bold 16px `#00695c`, top center, y=20):** "Two ways in — the replay path skips both checks".
- **Row labels** (bold 14px, left-aligned at x=40): "Normal login" in `#558b2f` at y=48; "Replay" in `#ad1457` at y=148.
- **Boxes** 130×50 (fill = box color at 0.12 alpha, stroke = box color width 2; title bold 14px in box color centered at box y+21; sub-line 12px `#666` centered at box y+38):
  | Row | Title | sub-line | color | x | y |
  |---|---|---|---|---|---|
  | top | Browser | asks to sign in | #00695c | 40 | 58 |
  | top | Password check | step one | #558b2f | 215 | 58 |
  | top | Code check | step two, on phone | #558b2f | 390 | 58 |
  | top | Account | open | #00897b | 565 | 58 |
  | bottom | Other browser | pastes stolen proof | #ad1457 | 40 | 158 |
  | bottom | Account | open | #00897b | 565 | 158 |
- **Arrows:** width-1.5 horizontal arrows with filled triangular heads; top row in `#558b2f` from (170,83) to (213,83), from (345,83) to (388,83), and from (520,83) to (563,83); bottom row one long `#ad1457` width-2 arrow from (170,183) to (563,183).
- **Skip label** (bold 13px `#ad1457`, centered at (365, 173)): "no password, no code".
- **Bottom line (bold 14px `#e65100`, centered, y=262):** "Both checks guard the act of logging in — replay never logs in."
- **Caption (bottom center, 13px `#999`, y=285):** "To the site, this looks like the same signed-in visit continuing."

## 3. What limits the damage

Damage control means shrinking how long and where stolen proof works.

- **Defense:** "sign out everywhere" cancels every active session.
- **Defense:** short lifetimes make sessions expire on their own.
- **Defense:** proof tied to one device fails elsewhere (device binding).
- **Defense:** alerts flag sign-ins from new devices and places.
- **Fact:** changing the password may leave old sessions running.
- **Risk:** a long-lived session can outlast the cleanup by weeks.

**Key point:** **Win:** expiry, cancellation, or a device check — any one of the three closes the replay window.

### Visualization (canvas `c3`, 720×300)

Timeline bars: three horizontal bars starting at the same theft moment, each shortened by a different defense.

- **Title (bold 16px `#00695c`, top center, y=20):** "How long stolen proof stays usable — three timelines".
- **Theft marker:** dashed (`[5,4]`) `#666` width-1 vertical line at x=180 from y=52 to y=212; label "proof stolen" 13px `#666` centered at (180, 44).
- **Rows** (bar height 22 starting at x=180; fill = row color at 0.2 alpha, stroke = row color width 1.5; row label bold 14px in row color left-aligned at x=40, baseline bar y+15; annotation 13px in row color left-aligned at (185, bar y+36)):
  | Label | bar y | bar width | annotation |
  |---|---|---|---|
  | Long lifetime (#ad1457) | 65 | 460 | stolen proof still works weeks later |
  | Short lifetime (#e65100) | 120 | 150 | expires on its own — small replay window |
  | Remote sign-out (#558b2f) | 175 | 80 | "sign out everywhere" cancels it at once |
- **Row extras:** row 2 — dashed `#ccc` width-1 line from (330, 131) to (640, 131) marking the time the proof no longer covers; row 3 — solid `#558b2f` width-3 vertical cut line at x=260 from y=170 to y=202.
- **Bottom line (bold 14px `#00695c`, centered, y=255):** "Each defense shortens the same bar — less usable time, less damage."
- **Caption (bottom center, 13px `#999`, y=285):** "A device-bound session is a bar of zero length on any other machine."

## Regeneration instructions

- **Layout:** backlog detail-page style — h1 with `border-bottom: 2px solid #00897b`, `.subtitle`, one `.intro` callout, then one `.lang-section` per numbered h2 (also blue-underlined). Each section holds a `table.layout` (border-collapse, full width) with one `<tr>`: left `td.text-col` (45%) for paragraphs/bullets/`.key-point`, right `td.viz-col` (55%) for the canvas.
- **Bullet style:** every bullet is one non-wrapping line — a bold colored label plus a phrase of roughly 55 characters or fewer; in HTML the label is `<span class="pt-label" style="color:COLOR">Label:</span>` with `.pt-label { font-weight: bold; }`. Long ideas are split into more labeled bullets, never wrapped. Lead paragraphs are at most one short sentence.
- **Label colors by meaning:** `#00695c` teal = mechanism/fact (Fact, Mechanism); `#558b2f` green = defense/win (Defense, Win); `#ad1457` magenta = risk/loss (Risk, Seen); `#e65100` orange = scene/context (Scene). Key-point boxes open with the same colored bold lead word (Risk, Win) followed by one short sentence.
- **Page CSS:** body system-ui sans-serif, background `#fff`, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#00695c`; h2 1.3rem `#00695c`; `.subtitle` `#666` 0.95rem; `.intro` background `#f0f4f8` with `border-left: 3px solid #00897b`; `.key-point` background `#f8f9fa` with `border-left: 3px solid #ad1457`, 0.9rem; `.pt-label` bold; ul 0.92rem; canvases `width: 100%` with `1px solid #e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic sizes `c1` 720×300, `c2` 720×300, `c3` 720×300; a shared `setupCanvas(id, w, h)` helper sizes the backing store from the displayed width times `window.devicePixelRatio` and scales the context accordingly. Each canvas is drawn by a named function (`drawC1`, `drawC2`, `drawC3`); a `renderAll()` runs once on load and again on window resize (debounced 150ms) so canvases stay sharp. All chart data is literal hardcoded coordinates — no randomness, no dates.
- **Palette:** primary teal `#00695c`, green `#558b2f`, magenta `#ad1457`, orange `#e65100`, plus `#00897b`, `#5e35b1`, gray text `#666`/`#999`.
- **Content rule:** tone stays mechanical and neutral — describe the mechanism, no drama words, no attributed intent, no absolutes. Fictional names only (Alice, Bob). Each technical term (infostealer, session cookies, replay attack, two-factor, device binding) appears at most once, in parentheses. No realistic credential strings, tokens, or key=value syntax anywhere on the page — generic placeholders like "(session cookie)" only.
- In regenerated HTML, any card/page links use `.html` extensions (this page has none).
