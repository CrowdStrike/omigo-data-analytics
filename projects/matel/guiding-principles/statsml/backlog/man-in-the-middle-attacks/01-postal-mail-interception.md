# Postal Mail Interception & Check Washing

**Page type:** detail page (backlog kusto-style two-column layout: one `.lang-section` per numbered h2, text left ~45%, canvas right ~55%)
**HTML title tag:** Postal Mail Interception & Check Washing

**Subtitle:** The oldest man-in-the-middle — a letter travels through many hands, and a paper check is just an instruction anyone holding it can try to rewrite.

**Intro callout (blue-left-border box):** This attack predates computers. It shows the core of every man-in-the-middle: a trusted channel with an untrusted stretch in the middle.

## 1. The setup: a check is an instruction letter

A paper check is a payment instruction sent through the mail.

- **Scene:** outgoing mail waits unattended in boxes.
- **Fact:** a check names a payee and an amount in plain ink.
- **Fact:** the sender assumes only the bank ever reads it.
- **Mechanism:** the letter passes through many hands in transit.
- **Risk:** whoever holds the paper holds the instruction.

**Key point (red-left-border box):** **Fact:** the check's power is in what it says, not who carries it — anyone holding it can try to rewrite it.

### Visualization (canvas `c1`, 720×300)

Four-box flow diagram of a mailed check's path, with the unattended mailbox stretch highlighted orange.

- **Title (bold 13px `#1a5276`, top center):** "The path of a mailed check — and its weak stretch".
- **Boxes** 140×70 at y=110 (fill = box color at 0.12 alpha, stroke = box color width 2; title bold 13px in box color, centered at box y+25; sub-line 10px `#666` centered at box y+45):
  | Title | sub-line | color | x |
  |---|---|---|---|
  | Sender | writes the instruction | #1a5276 | 25 |
  | Mailbox | waits unattended | #e67e22 | 205 |
  | Postal system | many hands in transit | #2980b9 | 385 |
  | Bank | reads and pays | #27ae60 | 565 |
- **Arrows:** `#bbb` width-1.5 horizontal arrows with filled triangular heads, from (165,145) to (200,145), (345,145) to (380,145), and (525,145) to (560,145).
- **Highlight:** dashed `#e67e22` width-2 rectangle (dash pattern 6,4) from (193, 95) sized 164×100, framing the Mailbox box; below it, bold 12px `#e67e22` centered label "the unattended stretch" at (275, 215).
- **Bottom line (bold 12px `#1a5276`, centered, y=250):** "The sender assumes only the bank reads it — but the letter sits in the open first."
- **Caption (bottom center, 11px `#999`, y=285):** "Every man-in-the-middle: a trusted channel with an untrusted stretch in the middle."

## 2. The trick: wash, rewrite, cash

The attack rewrites the instruction while it is in transit.

- **Stolen:** mail lifted from collection boxes and mailboxes.
- **Scene:** a raised flag signals outgoing checks inside.
- **Mechanism:** common solvents commonly lift ordinary pen ink.
- **Fact:** the printed form and signature area survive the wash.
- **Stolen:** payee and amount rewritten, usually much larger.
- **Mechanism:** forwarding fraud redirects a household's whole mail.

**Key point:** **Risk:** the bank sees a normal-looking check with a real signature — the paper itself carries no proof of the rewrite.

### Visualization (canvas `c2`, 720×300)

Schematic check anatomy — labeled boxes only; absolutely no realistic names, account numbers, or routing numbers anywhere.

- **Title (bold 13px `#1a5276`, top center):** "Check washing: which fields get rewritten (schematic)".
- **Outer check outline:** rectangle stroke `#999` width 2 from (80, 55) sized 560×175; small left-aligned 10px `#999` label "check (schematic)" at (95, 72).
- **Field boxes** (fill = field color at 0.12 alpha, stroke = field color width 2; label bold 13px in field color centered at box y+18; sub-line 10px `#666` centered at box y+34):
  | Label | sub-line | color | x | y | w | h |
  |---|---|---|---|---|---|---|
  | payee | washed & rewritten | #e74c3c | 110 | 90 | 320 | 42 |
  | amount | washed & rewritten | #e74c3c | 460 | 90 | 150 | 42 |
  | date | unchanged | #666 | 110 | 160 | 140 | 42 |
  | signature | left in place | #27ae60 | 400 | 160 | 210 | 42 |
- **Bottom line (bold 12px `#e74c3c`, centered, y=255):** "Ordinary pen ink commonly washes out — the printed form and signature stay."
- **Caption (bottom center, 11px `#999`, y=285):** "Schematic — no real check data shown."

## 3. What stops it

Every defense removes either the stretch or the rewrite.

- **Defense:** gel ink bonds to paper fibers and resists washing.
- **Defense:** drop mail inside the post office — no exposed leg.
- **Defense:** bank alerts shrink discovery from weeks to hours.
- **Fact:** businesses pre-register every check they issue.
- **Defense:** checks not on the list are rejected (positive pay).
- **Win:** electronic payment removes the paper instruction.

**Key point:** **Win:** the deepest fix removes the middle entirely — no paper traveling, nothing to intercept.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart of discovery lag under three habits — illustrative bar lengths.

- **Title (bold 13px `#1a5276`, top center):** "Discovery lag: time until the rewrite is noticed (illustrative)".
- **Bars:** 34px tall, 26px gap, starting y=75; labels right-aligned 11px `#2c3e50` ending at x=250; track `#f0f0f0` 380px wide starting at x=262; bar fill = row color at 0.6 alpha with 1px solid stroke in row color; span word in bold 11px `#2c3e50` placed 8px after the bar end.
- **Data (label, bar length px, span word, color):**
  | Habit | length | span | color |
  |---|---|---|---|
  | paper statement review | 320 | weeks | #e74c3c |
  | online banking habit | 130 | days | #e67e22 |
  | instant alerts | 40 | hours | #27ae60 |
- **Bottom line (bold 12px `#27ae60`, centered, y=250):** "The rewrite is the same — the alert just shortens how long it goes unseen."
- **Caption (bottom center, 11px `#999`, y=285):** "Illustrative bar lengths — not to scale."

## Regeneration instructions

- **Layout:** backlog detail-page style — h1 with `border-bottom: 2px solid #2980b9`, `.subtitle`, one `.intro` callout, then one `.lang-section` per numbered h2 (also blue-underlined). Each section holds a `table.layout` (border-collapse, full width) with one `<tr>`: left `td.text-col` (45%) for paragraphs/bullets/`.key-point`, right `td.viz-col` (55%) for the canvas.
- **Bullet style:** every bullet is one non-wrapping line — a bold colored label plus a phrase of roughly 55 characters or fewer; in HTML the label is `<span class="pt-label" style="color:COLOR">Label:</span>` with `.pt-label { font-weight: bold; }`. Long ideas are split into more labeled bullets, never wrapped. Lead paragraphs are at most one short sentence.
- **Label colors by meaning:** `#1a5276` blue = mechanism/fact (Fact, Mechanism); `#27ae60` green = defense/win (Defense, Win); `#e74c3c` red = risk/loss (Risk, Stolen); `#e67e22` orange = scene/context (Scene). Key-point boxes open with the same colored bold lead word followed by one short sentence.
- **Tone:** the topic is inherently about crime — keep it mechanical and neutral; describe mechanism only, no drama words, no attributed intent. Hedges (usually, commonly) stay on unsourced claims; invented magnitudes are labeled "illustrative".
- **Content rule:** the check diagram is always schematic labeled boxes — never render realistic names, amounts, account numbers, or routing numbers anywhere on the page.
- **Page CSS:** body system-ui sans-serif, background `#fff`, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; h2 1.3rem `#1a5276`; `.subtitle` `#666` 0.95rem; `.intro` background `#f0f4f8` with `border-left: 3px solid #2980b9`; `.key-point` background `#f8f9fa` with `border-left: 3px solid #e74c3c`, 0.9rem; `.pt-label` bold; ul 0.92rem; canvases `width: 100%` with `1px solid #e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic sizes `c1` 720×300, `c2` 720×300, `c3` 720×300; a shared `setupCanvas(id, w, h)` helper sizes the backing store from the displayed width times `window.devicePixelRatio` and scales the context accordingly. Literal hardcoded data arrays only — no Math.random or Date.now.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, plus `#2980b9`, `#8e44ad`, gray text `#666`/`#999`, bar fill via row color at 0.6 alpha on `#f0f0f0` tracks.
- In regenerated HTML, any card/page links use `.html` extensions (this page has none).
