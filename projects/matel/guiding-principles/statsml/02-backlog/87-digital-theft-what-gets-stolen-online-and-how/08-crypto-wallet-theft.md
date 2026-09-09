# Crypto Wallet Theft

**Page type:** detail page (backlog kusto-style two-column layout: one `.lang-section` per numbered h2, text left ~45%, canvas right ~55%)
**HTML title tag:** Crypto Wallet Theft

**Subtitle:** Whoever learns the recovery words owns the money — and unlike a bank transfer, nobody can reverse the move.

**Intro callout (blue-left-border box):** No vault is drilled and no account is hacked — the words are asked for politely, and the ledger treats the transfer that follows as the owner's own.

## 1. The phrase is the money

A wallet is controlled by a short list of ordinary words.

- **Fact:** control is a short list of common words (recovery phrase).
- **Fact:** anyone holding the words can move the funds.
- **Fact:** the words work from any device, in any place.
- **Mechanism:** no institution sits between the words and the balance.
- **Mechanism:** holding the secret is the ownership itself.
- **Risk:** there is no account desk to call after a loss.

**Key point (red-left-border box):** **Risk:** whoever learns the words owns the money — copying them is the whole theft.

### Visualization (canvas `c1`, 720×300)

Two-path comparison: bank transfer above in green ends at a dispute desk; crypto transfer below in red ends at a "Final" box with no third step to appeal.

- **Title (bold 16px `#827717`, top center):** "Both paths record the move — only one keeps a reverse gear".
- **Row labels** (bold 14px, left-aligned at x=40): "Bank transfer" in `#00695c` at y=48; "Crypto transfer" in `#ad1457` at y=148.
- **Boxes** 130×50 (fill = box color at 0.12 alpha, stroke = box color width 2; title bold 14px in box color centered at box y+21; sub-line 12px `#666` centered at box y+38):
  | Row | Title | sub-line | color | x | y |
  |---|---|---|---|---|---|
  | top | Alice's bank | sends the money | #827717 | 60 | 58 |
  | top | Central ledger | entry recorded | #9e9d24 | 290 | 58 |
  | top | Dispute desk | entry can be reversed | #00695c | 520 | 58 |
  | bottom | Alice's wallet | signs with the words | #827717 | 60 | 158 |
  | bottom | Shared ledger | entry recorded | #9e9d24 | 290 | 158 |
  | bottom | Final | no desk, no undo | #ad1457 | 520 | 158 |
- **Arrows:** width-1.5 horizontal arrows with filled triangular heads; top row in `#00695c` from (190,83) to (288,83) and from (420,83) to (518,83); bottom row in `#ad1457` from (190,183) to (288,183) and from (420,183) to (518,183).
- **Bottom line (bold 14px `#e65100`, centered, y=262):** "The first two steps match — the crypto path simply has no third."
- **Caption (bottom center, 13px `#999`, y=285):** "The crypto entry is final the moment the shared ledger accepts it."

## 2. How the phrase gets taken

Nobody breaks a lock; the owner is led to hand over the key.

- **Scene:** a fake "support" chat offers help with the wallet.
- **Mechanism:** the helper walks Alice through revealing the words.
- **Scene:** a lookalike app asks for the words at first launch.
- **Mechanism:** hidden code swaps a copied address (clipboard swap).
- **Risk:** the payment lands at the swapped address instead.
- **Risk:** a confirmed transfer has no dispute, recall, or undo.

**Key point:** **Risk:** every route ends the same — the money moves once, and the move is final.

### Visualization (canvas `c2`, 720×300)

Convergence diagram: three theft routes on the left, each with its own color, all pointing into one box on the right where control of the funds changes hands.

- **Title (bold 16px `#827717`, top center):** "Three routes converge on the same secret".
- **Source boxes** 190×50 at x=50 (fill = box color at 0.12 alpha, stroke = box color width 2; title bold 14px in box color centered at box y+21; sub-line 12px `#666` centered at box y+38):
  | Title | sub-line | color | y |
  |---|---|---|---|
  | Fake support chat | "read me your words" | #ad1457 | 48 |
  | Lookalike wallet app | asks the words up front | #e65100 | 118 |
  | Clipboard swap | pasted address replaced | #4527a0 | 188 |
- **Target box** 190×70 at x=480, y=115, color `#ad1457`: title "Control changes hands" bold 14px centered at (575, 145); sub-line 12px `#666` "the funds move once" centered at (575, 165).
- **Arrows:** width-2 arrows with filled heads, each in its source box color, from the source box right edge to the target box left edge: (240, 73) to (478, 140); (240, 143) to (478, 150); (240, 213) to (478, 160).
- **Bottom line (bold 14px `#e65100`, centered, y=266):** "No lock is broken — each route ends with the owner's own tap."
- **Caption (bottom center, 13px `#999`, y=286):** "After the confirmation there is no dispute desk anywhere on the path."

## 3. What limits the damage

Every defense keeps the words away from anything that asks.

- **Defense:** the words are never typed into a site or chat.
- **Defense:** a sealed device holds the secret (hardware wallet).
- **Fact:** the device signs transfers without showing the words.
- **Fact:** no legitimate helper needs the words to give help.
- **Defense:** the full destination address is read before confirming.
- **Defense:** funds are split so no one phrase controls it all.
- **Scene:** Bob keeps a small daily wallet, savings apart.

**Key point:** **Win:** a phrase that is never shown cannot be copied — and a split caps any single loss.

### Visualization (canvas `c3`, 720×300)

Defense table: one row per habit — habit name, what it does, and a status pill showing what the habit blocks or caps.

- **Title (bold 16px `#827717`, top center):** "Four habits and what each one blocks".
- **Rows** at y = 58, 103, 148, 193 (habit name bold 14px `#827717` left-aligned at x=40, baseline row y+19; description 13px `#666` left-aligned at x=200, baseline row y+19; status pill = rect at x=520, width 150, height 26 at row y+2, fill = pill color at 0.12 alpha, stroke = pill color width 2, pill text bold 13px in pill color centered at (595, row y+19)):
  | Habit | description | pill text | pill color |
  |---|---|---|---|
  | Hardware wallet | the words never leave the device | blocked | #00695c |
  | Never-type rule | no site or chat ever sees the words | blocked | #00695c |
  | Address check | the full destination is read first | blocked | #00695c |
  | Split funds | no single phrase controls everything | loss capped | #e65100 |
- **Bottom line (bold 14px `#00695c`, centered, y=255):** "A secret that is never shown cannot be copied."
- **Caption (bottom center, 13px `#999`, y=285):** "The split does not prevent a theft — it caps what one phrase can lose."

## Regeneration instructions

- **Layout:** backlog detail-page style — h1 with `border-bottom: 2px solid #9e9d24`, `.subtitle`, one `.intro` callout, then one `.lang-section` per numbered h2 (also blue-underlined). Each section holds a `table.layout` (border-collapse, full width) with one `<tr>`: left `td.text-col` (45%) for paragraphs/bullets/`.key-point`, right `td.viz-col` (55%) for the canvas.
- **Bullet style:** every bullet is one non-wrapping line — a bold colored label plus a phrase of roughly 55 characters or fewer; in HTML the label is `<span class="pt-label" style="color:COLOR">Label:</span>` with `.pt-label { font-weight: bold; }`. Long ideas are split into more labeled bullets, never wrapped. Lead paragraphs are at most one short sentence.
- **Label colors by meaning:** `#827717` blue = mechanism/fact (Fact, Mechanism); `#00695c` green = defense/win (Defense, Win); `#ad1457` red = risk/loss (Risk, Seen); `#e65100` orange = scene/context (Scene). Key-point boxes open with the same colored bold lead word (Risk, Win) followed by one short sentence.
- **Page CSS:** body system-ui sans-serif, background `#fff`, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#827717`; h2 1.3rem `#827717`; `.subtitle` `#666` 0.95rem; `.intro` background `#f0f4f8` with `border-left: 3px solid #9e9d24`; `.key-point` background `#f8f9fa` with `border-left: 3px solid #ad1457`, 0.9rem; `.pt-label` bold; ul 0.92rem; canvases `width: 100%` with `1px solid #e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic sizes `c1` 720×300, `c2` 720×300, `c3` 720×300; a shared `setupCanvas(id, w, h)` helper sizes the backing store from the displayed width times `window.devicePixelRatio` and scales the context accordingly. Each chart is drawn by a named function (`drawC1`–`drawC3`); a `renderAll()` call runs once on load and again on window resize (debounced 150ms) so canvases stay sharp. All chart data is literal hardcoded coordinates — no randomness, no dates.
- **Palette:** primary blue `#827717`, green `#00695c`, red `#ad1457`, orange `#e65100`, plus `#9e9d24`, `#4527a0`, gray text `#666`/`#999`.
- **Content rule:** tone stays mechanical and neutral — describe the mechanism, no drama words, no attributed intent, no absolutes. Each technical term (recovery phrase, clipboard swap, hardware wallet) appears at most once, in parentheses. Fictional names only (Alice, Bob). No realistic credential strings and no example recovery words anywhere on the page.
- In regenerated HTML, any card/page links use `.html` extensions (this page has none).
