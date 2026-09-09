# ATM & Gas Pump Card Skimmers

**Page type:** detail page (backlog kusto-style two-column layout: one `.lang-section` per numbered h2, text left ~45%, canvas right ~55%)
**HTML title tag:** ATM & Gas Pump Card Skimmers

**Subtitle:** A second card reader hiding in front of the real one — your transaction goes through normally, and that is exactly why it works.

**Intro callout (blue-left-border box):** The skimmer blocks nothing and changes nothing; it only copies. A man-in-the-middle that lets the message through untouched is invisible to both ends.

## 1. The setup: the machine you trust by touch

The machine looks right, sits in the right place, and works.

- **Scene:** an ATM is judged by its looks and its location.
- **Fact:** the stripe is a static recording of fixed data.
- **Risk:** reading the stripe once is the same as owning it.
- **Risk:** the PIN travels through fingers in plain sight.
- **Setup:** the card must pass any reader placed in front.

**Key point (red-left-border box):** **Fact:** a perfect copy of a static recording is as good as the original card.

### Visualization (canvas `c1`, 720×300)

Cross-section schematic: a thin overlay reader sits in front of the real card slot, and a pinhole camera sits above the keypad.

- **Title (bold 13px `#1a5276`, top center):** "Cross-section: a second reader in front of the real slot".
- **ATM body:** rect (430, 45, 250, 215), fill `#999` at 0.08 alpha, stroke `#999` width 1.5; label "ATM body" 11px `#999` centered at (555, 62).
- **Real reader:** rect (432, 115, 150, 36), fill `#1a5276` at 0.12 alpha, stroke `#1a5276` width 2; label "real reader" bold 12px `#1a5276` centered at (507, 133); sub-line "inside the machine" 10px `#666` centered at (507, 147).
- **Overlay reader:** rect (398, 108, 34, 50) attached in front of the face, fill `#e74c3c` at 0.15 alpha, stroke `#e74c3c` width 2.
- **Overlay annotation:** "overlay reader — copies the stripe" bold 12px `#e74c3c` centered at (235, 72); connector line `#e74c3c` width 1 from (330, 78) to (408, 106).
- **Card:** rect (120, 124, 160, 18), fill `#2980b9` at 0.12 alpha, stroke `#2980b9` width 2; label "card — stripe holds fixed data" 11px `#2980b9` centered at (200, 112).
- **Card arrow:** gray `#bbb` width-1.5 arrow with filled head from (285, 133) to (395, 133).
- **Keypad:** rect (398, 205, 34, 34), fill `#666` at 0.08 alpha, stroke `#666` width 2; label "keypad" 10px `#666` centered at (415, 252).
- **Pinhole camera:** filled `#e74c3c` rect (404, 176, 12, 9); dashed `#e74c3c` width-1 sight line (dash 3,3) from (410, 185) to (413, 205).
- **Camera annotation:** "pinhole camera — films the PIN" bold 12px `#e74c3c` centered at (230, 185); connector line `#e74c3c` width 1 from (325, 182) to (400, 180).
- **Caption (bottom center, 11px `#999`, y=285):** "Nothing is blocked or altered — the same swipe is simply read twice."

## 2. The trick: copy the stripe, film the PIN

- **Mechanism:** the overlay reads the stripe as the card slides by.
- **Mechanism:** a keypad overlay or pinhole camera takes the PIN.
- **Fact:** thin readers fit inside the chip slot too (shimmers).
- **Scene:** gas pumps sit unattended overnight.
- **Trend:** pumps are commonly reported as frequent targets.
- **Stolen:** stripe copy + PIN video = a working clone card.
- **Risk:** the clone works at any stripe-only machine.

**Key point:** **Risk:** the transaction succeeds normally — that success is what keeps the copy invisible.

### Visualization (canvas `c2`, 720×300)

Two-lane flow diagram: the visible transaction on top, the hidden copy underneath.

- **Title (bold 13px `#1a5276`, top center):** "One swipe, two records: the visible lane and the hidden lane".
- **Lane 1 header (bold 12px `#1a5276`, left-aligned at (30, 62)):** "What you see".
- **Lane 1 boxes** 150×55 at y=72 (fill = box color at 0.12 alpha, stroke = box color width 2; title bold 12px in box color centered at y=96; sub-line 10px `#666` centered at y=114):
  | Title | sub-line | color | x |
  |---|---|---|---|
  | card goes in | reader accepts it | #1a5276 | 120 |
  | PIN entered | keypad works normally | #2980b9 | 320 |
  | cash comes out | transaction succeeds | #27ae60 | 520 |
- **Lane 1 arrows:** `#bbb` width-1.5 arrows with filled heads from (270, 100) to (312, 100) and from (470, 100) to (512, 100).
- **Lane 2 header (bold 12px `#e74c3c`, left-aligned at (30, 162)):** "What also happened — same swipe".
- **Lane 2 boxes** 150×55 at y=172, all `#e74c3c` (same box style; title bold 12px centered at y=196; sub-line 10px `#666` centered at y=214):
  | Title | sub-line | x |
  |---|---|---|
  | stripe copied | by the overlay reader | 120 |
  | PIN filmed | by the pinhole camera | 320 |
  | clone card made | ready for stripe-only use | 520 |
- **Lane 2 arrows:** same style, from (270, 200) to (312, 200) and from (470, 200) to (512, 200).
- **Bottom line (bold 12px `#e67e22`, centered, y=258):** "The normal transaction on top is what hides the copy underneath."
- **Caption (bottom center, 11px `#999`, y=285):** "A copy that blocks nothing raises no alarm at either end."

## 3. What stops it: answers that expire

- **Risk:** a static recording replays perfectly, forever.
- **Defense:** a chip computes a fresh answer each time (cryptogram).
- **Defense:** a recorded answer is worthless next transaction.
- **Defense:** tap payments use the same expiring answer.
- **Defense:** banks flag one card used in two distant places.
- **Risk:** the gap: wherever stripe fallback is still accepted.

**Key point:** **Defense:** the fix is not hiding the data — it is making any copy expire before it can be reused.

### Visualization (canvas `c3`, 720×300)

Side-by-side comparison: the same question asked twice — the stripe repeats its answer, the chip never does.

- **Title (bold 13px `#1a5276`, top center):** "Same question twice: static recording vs one-time answer".
- **Divider:** vertical line `#ddd` width 1 from (360, 40) to (360, 235).
- **Column headers (bold 13px, y=52):** "Magnetic stripe — static recording" in `#e74c3c` centered at x=185; "Chip — one-time answer" in `#27ae60` centered at x=540.
- **Row labels (11px `#666`, right-aligned):** "transaction 1" ending at x=150 (left) / x=505 (right), baseline y=107; "transaction 2" same x, baseline y=162.
- **Answer boxes** 110×34 (fill = column color at 0.12 alpha, stroke = column color width 2; text bold 12px in column color, centered):
  | Column | box rects | texts |
  |---|---|---|
  | stripe (#e74c3c) | (165, 85) and (165, 140) | "answer: A" at (220, 107); "answer: A" at (220, 162) |
  | chip (#27ae60) | (520, 85) and (520, 140) | "answer: B" at (575, 107); "answer: C" at (575, 162) |
- **Verdicts (bold 12px, y=207):** "same answer — a copy replays it" in `#e74c3c` centered at (185, 207); "new answer — a copy goes stale" in `#27ae60` centered at (540, 207).
- **Bottom line (bold 12px `#1a5276`, centered, y=248):** "The stripe repeats itself; the chip never does."
- **Caption (bottom center, 11px `#999`, y=285):** "The defense is not hiding the conversation — it is making a recording of it worthless."

## Regeneration instructions

- **Layout:** backlog detail-page style — h1 with `border-bottom: 2px solid #2980b9`, `.subtitle`, one `.intro` callout, then one `.lang-section` per numbered h2 (also blue-underlined). Each section holds a `table.layout` (border-collapse, full width) with one `<tr>`: left `td.text-col` (45%) for paragraphs/bullets/`.key-point`, right `td.viz-col` (55%) for the canvas.
- **Bullet style:** every bullet is one non-wrapping line — a bold colored label plus a phrase of roughly 55 characters or fewer; in HTML the label is `<span class="pt-label" style="color:COLOR">Label:</span>` with `.pt-label { font-weight: bold; }`. Long ideas are split into more labeled bullets, never wrapped. Lead paragraphs are at most one short sentence.
- **Label colors by meaning:** `#1a5276` blue = mechanism/fact (labels: Setup, Fact, Mechanism); `#27ae60` green = defense/win (Defense, Win); `#e74c3c` red = risk/loss (Risk, Stolen); `#e67e22` orange = scene/context/history (Scene, History, Trend). Key-point boxes open with the same colored bold lead word (Fact, Risk, Defense) followed by one short sentence.
- **Page CSS:** body system-ui sans-serif, background `#fff`, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; h2 1.3rem `#1a5276`; `.subtitle` `#666` 0.95rem; `.intro` background `#f0f4f8` with `border-left: 3px solid #2980b9`; `.key-point` background `#f8f9fa` with `border-left: 3px solid #e74c3c`, 0.9rem; `.pt-label` bold; ul 0.92rem; canvases `width: 100%` with `1px solid #e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic sizes `c1` 720×300, `c2` 720×300, `c3` 720×300; a shared `setupCanvas(id, w, h)` helper sizes the backing store from the displayed width times `window.devicePixelRatio` and scales the context accordingly. All data is literal and hardcoded — no `Math.random`, no `Date.now`.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, plus `#2980b9`, `#8e44ad`, gray text `#666`/`#999`.
- **Tone rule:** mechanical and neutral — describe the mechanism only; no drama words, no attributed intent, no named companies; people are Alice/Bob if needed.
- **Content rule:** no realistic card numbers or PINs anywhere — schematic labels only ("answer: A", "card", "keypad"); a technical term appears at most once, in parentheses (shimmers, cryptogram).
- In regenerated HTML, any card/page links use `.html` extensions (this page has none).
