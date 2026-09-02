# Payment Card Theft

**Page type:** detail page (backlog kusto-style two-column layout: one `.lang-section` per numbered h2, text left ~45%, canvas right ~55%)
**HTML title tag:** Payment Card Theft

**Subtitle:** Card numbers lifted from checkout pages or breach dumps are bundled, sold in bulk, and quietly tested with small charges before the real spending starts.

**Intro callout (blue-left-border box):** No one touches Alice's card — a copied number works at a distance, so both ends of the sale look normal until a charge looks wrong.

## 1. Where numbers leak

The copy happens at the moment of a normal purchase, or long after it.

- **Scene:** Alice pays on a retail company's checkout page.
- **Mechanism:** a small added script copies the card as typed (e-skimming).
- **Mechanism:** breach dumps spill stored card records in bulk.
- **Fact:** the purchase itself completes normally either way.
- **Fact:** the shop's own logs show only an ordinary sale.
- **Seen:** card networks often flag the leak before the shop does.

**Key point (red-left-border box):** **Risk:** shopper and shop both see a normal sale — the copy leaves no trace at the counter.

### Visualization (canvas `c1`, 720×300)

Checkout-page diagram: the normal charge flows left to right; a copy tap on the checkout page routes the typed card down to a collector box.

- **Title (bold 16px `#0d47a1`, top center, y=20):** "One added script on the checkout page copies the card as typed".
- **Boxes** 160×54 (fill = box color at 0.12 alpha, stroke = box color width 2; title bold 14px in box color centered at box y+22; sub-line 12px `#666` centered at box y+40):
  | Title | sub-line | color | x | y |
  |---|---|---|---|---|
  | Alice's browser | types card details | #0d47a1 | 30 | 80 |
  | Checkout page | added script runs here | #1976d2 | 280 | 80 |
  | Payment company | charge approved | #2e7d32 | 530 | 80 |
  | Collector box | copy arrives here | #ad1457 | 280 | 200 |
- **Flow arrows** (width 1.5, filled triangular heads; 12px `#666` labels centered above): `#0d47a1` arrow from (190, 107) to (278, 107), label "card details" at (234, 97); `#2e7d32` arrow from (440, 107) to (528, 107), label "normal charge" at (484, 97).
- **Copy tap:** filled `#ad1457` circle radius 4 at (360, 134); solid `#ad1457` width-2 downward arrow from (360, 142) to (360, 198); label "silent copy" bold 13px `#ad1457` left-aligned at (372, 172).
- **Bottom line (bold 14px `#f57c00`, centered, y=272):** "The purchase succeeds — the copy is a side effect, not a failure."
- **Caption (bottom center, 13px `#999`, y=290):** "The shop's logs show an ordinary sale; the card network often notices first."

## 2. The resale pipeline

A copied number is inventory first and a payment method second.

- **Mechanism:** numbers are bundled by bank, country, and card type.
- **Fact:** bundles are sold in bulk, priced by freshness.
- **Mechanism:** tiny charges probe which cards still work (card testing).
- **Fact:** a card that passes the tiny test resells for more.
- **Risk:** working cards fund online buys and gift-card chains.
- **Seen:** a small strange charge is often the only early warning.

**Key point:** **Risk:** the tiny test charge comes first — catching it early cuts off the big spend that follows.

### Visualization (canvas `c2`, 720×300)

Pipeline flow: four stage boxes left to right, with the test-charge stage split into a declined path and an approved path, and a note that the test is the visible moment.

- **Title (bold 16px `#0d47a1`, top center, y=20):** "From stolen batch to spending — the tiny test comes first".
- **Stage boxes** 140×54 at y=70 (fill = box color at 0.12 alpha, stroke = box color width 2; title bold 14px in box color centered at y=92; sub-line 12px `#666` centered at y=110):
  | Title | sub-line | color | x |
  |---|---|---|---|
  | Stolen batch | numbers in bulk | #ad1457 | 25 |
  | Bundle & sort | by bank, country, type | #f57c00 | 205 |
  | Test charges | tiny amounts probe | #00838f | 385 |
  | Cards spent | online buys, gift cards | #ad1457 | 565 |
- **Stage arrows:** `#666` width-1.5 horizontal arrows with filled heads from (167, 97) to (203, 97), from (347, 97) to (383, 97), and from (527, 97) to (563, 97).
- **Test outcomes** (centered at x=455 under the Test charges box): "declined — number dropped" 12px `#999` at y=150; "approved — resale price rises" 12px `#ad1457` at y=168.
- **Early-warning note (bold 13px `#f57c00`, centered at (455, 198)):** "this small charge is Alice's early warning".
- **Bottom line (bold 14px `#f57c00`, centered, y=262):** "A tiny unfamiliar charge often arrives days before the real spending."
- **Caption (bottom center, 13px `#999`, y=285):** "Cards that pass the test are the ones worth money; the rest are discarded."

## 3. What limits the damage

Each layer either stops the charge or makes the copied number worth less.

- **Defense:** instant alerts turn every charge into a checkpoint.
- **Defense:** one-use numbers die after a single purchase (virtual cards).
- **Defense:** per-merchant numbers cap what one leak can spend.
- **Defense:** phone wallets send a stand-in number (tokenization).
- **Fact:** the real number never reaches the shop's page.
- **Defense:** disputed charges shift the loss off the cardholder.
- **Fact:** the merchant or bank absorbs a contested charge.

**Key point:** **Win:** most losses land on the merchant or bank, not Alice — and each layer shrinks what one leak is worth.

### Visualization (canvas `c3`, 720×300)

Defense ledger: one row per layer with a name, a one-line effect, and a status pill; thin separators between rows.

- **Title (bold 16px `#0d47a1`, top center, y=20):** "Four layers, and what a stolen number runs into at each".
- **Rows** at y = 52, 100, 148, 196 (name bold 14px `#2e7d32` left-aligned at x=40, baseline row y+16; effect 12px `#666` left-aligned at x=40, baseline row y+32; status pill = rect at x=520, width 160, height 26 at row y+3, fill = pill color at 0.12 alpha, stroke = pill color width 2, pill text bold 13px in pill color centered at (600, row y+20)):
  | Name | effect | pill text | pill color |
  |---|---|---|---|
  | Instant charge alerts | every charge pings the phone in seconds | caught early | #f57c00 |
  | One-use virtual number | the number dies after a single purchase | charge blocked | #2e7d32 |
  | Phone-wallet stand-in | the real number never reaches the shop | nothing to steal | #2e7d32 |
  | Dispute rights | contested charges are reversed | loss shifted | #1976d2 |
- **Separators:** `#e0e0e0` width-1 horizontal lines from x=40 to x=680 at y = 92, 140, 188.
- **Bottom line (bold 14px `#0d47a1`, centered, y=258):** "Each layer either stops the charge or shrinks what one leak is worth."
- **Caption (bottom center, 13px `#999`, y=285):** "With alerts, stand-in numbers, and dispute rights, the copied number keeps losing value."

## Regeneration instructions

- **Layout:** backlog detail-page style — h1 with `border-bottom: 2px solid #1976d2`, `.subtitle`, one `.intro` callout, then one `.lang-section` per numbered h2 (also blue-underlined). Each section holds a `table.layout` (border-collapse, full width) with one `<tr>`: left `td.text-col` (45%) for paragraphs/bullets/`.key-point`, right `td.viz-col` (55%) for the canvas.
- **Bullet style:** every bullet is one non-wrapping line — a bold colored label plus a phrase of roughly 55 characters or fewer; in HTML the label is `<span class="pt-label" style="color:COLOR">Label:</span>` with `.pt-label { font-weight: bold; }`. Long ideas are split into more labeled bullets, never wrapped. Lead paragraphs are at most one short sentence.
- **Label colors by meaning:** `#0d47a1` blue = mechanism/fact (Fact, Mechanism); `#2e7d32` green = defense/win (Defense, Win); `#ad1457` red = risk/loss (Risk, Seen); `#f57c00` orange = scene/context (Scene). Key-point boxes open with the same colored bold lead word (Risk, Win) followed by one short sentence.
- **Page CSS:** body system-ui sans-serif, background `#fff`, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#0d47a1`; h2 1.3rem `#0d47a1`; `.subtitle` `#666` 0.95rem; `.intro` background `#f0f4f8` with `border-left: 3px solid #1976d2`; `.key-point` background `#f8f9fa` with `border-left: 3px solid #ad1457`, 0.9rem; `.pt-label` bold; ul 0.92rem; canvases `width: 100%` with `1px solid #e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic sizes `c1` 720×300, `c2` 720×300, `c3` 720×300; a shared `setupCanvas(id, w, h)` helper sizes the backing store from the displayed width times `window.devicePixelRatio` and scales the context accordingly; each canvas is drawn by a named function (`drawC1`..`drawC3`) and a `renderAll()` runs at load and again on window resize (debounced 150ms) so canvases stay sharp. All chart data is literal hardcoded coordinates — no randomness, no dates.
- **Palette:** primary blue `#0d47a1`, green `#2e7d32`, red `#ad1457`, orange `#f57c00`, plus `#1976d2`, `#00838f`, gray text `#666`/`#999`.
- **Content rule:** tone stays mechanical and neutral — describe the mechanism, no drama words, no attributed intent, no absolutes ("often", not "always"). Each technical term (e-skimming, card testing, virtual cards, tokenization) appears at most once, in parentheses. Fictional naming only (Alice, "a retail company"); no realistic card numbers or credential strings anywhere on the page.
- In regenerated HTML, any card/page links use `.html` extensions (this page has none).
