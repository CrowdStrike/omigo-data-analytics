# QR Code Sticker Swaps

**Page type:** detail page (backlog kusto-style two-column layout: one `.lang-section` per numbered h2, text left ~45%, canvas right ~55%)
**HTML title tag:** QR Code Sticker Swaps

**Subtitle:** A QR code is a web address written in a language only cameras read — swapping the sticker swaps the destination, and no human can see the difference.

**Intro callout (blue-left-border box):** The cheapest man-in-the-middle in this collection: a printed sticker. It attacks the pointer, not the connection — you are routed to the middleman before any network is involved.

## 1. The setup: a pointer humans cannot read

A QR square is nothing more than a web address in machine-readable form.

- **Setup:** the square encodes a web address (URL).
- **Fact:** cameras read it; human eyes cannot.
- **Fact:** two codes for different sites look identical.
- **Mechanism:** trust comes from where the code sits.
- **Scene:** a meter, a menu, a poster lends its authority.

**Key point (red-left-border box):** **Fact:** the code inherits trust from its surface — it carries none of its own.

### Visualization (canvas `c1`, 720×300)

Two schematic QR-like blocky squares side by side — abstract block grids, deliberately NOT scannable codes.

- **Title (bold 13px `#1a5276`, top center):** "Two squares, two destinations".
- **Grids:** two 10×10 block grids, cell size 14px (grid = 140×140), top at y=60; left grid at x=170, right grid at x=410. Cells drawn from literal hardcoded 0/1 string arrays (10 strings of 10 chars each per grid; the two grids differ in a handful of cells). Filled cells `#2c3e50`; each grid outlined with a width-2 stroke — left grid `#27ae60`, right grid `#e74c3c`.
- **Left grid pattern (rows):** `1101100101`, `0011010110`, `1100101101`, `0110110010`, `1011001101`, `0101101011`, `1010010110`, `0110101001`, `1001011010`, `1101100110`.
- **Right grid pattern (rows):** `1101100101`, `0011011110`, `1100101101`, `0110100010`, `1011001101`, `0101111011`, `1010010110`, `0100101001`, `1001011010`, `1101101110`.
- **Labels (bold 12px, centered under each grid at y=225):** "official payment page" in `#27ae60` at x=240; "copy that points elsewhere" in `#e74c3c` at x=480.
- **Bottom line (bold 12px `#e67e22`, centered, y=255):** "same to your eye — different to the camera".
- **Caption (bottom center, 11px `#999`, y=285):** "Abstract block grids for illustration — not real codes; a person cannot tell the two apart."

## 2. The trick: paste over the real one

One printed sticker on top of a real code reroutes every payment behind it.

- **Scene:** parking-meter swaps reported in several cities.
- **Scene:** menu codes and fake "pay fine" parking tickets too.
- **Mechanism:** the fake page mimics the real payment flow.
- **Taken:** card details go straight to the middleman.
- **Risk:** the parking itself is still unpaid.
- **Risk:** discovery comes later — a notice or card charges.

**Key point (red-left-border box):** **Risk:** nothing fails at scan time — the victim sees a normal-looking payment page.

### Visualization (canvas `c2`, 720×300)

Four-box flow of the hijacked payment, with the real path shown dashed and unused underneath.

- **Title (bold 13px `#1a5276`, top center):** "One sticker reroutes the whole payment".
- **Boxes** 155×65 at y=70 (fill = box color at 0.12 alpha, stroke = box color width 2; title bold 13px in box color centered at y=98; sub-line 10px `#666` centered at y=118):
  | Title | sub-line | color | x |
  |---|---|---|---|
  | Driver | scans the code on the meter | #1a5276 | 15 |
  | Sticker on top | pasted over the real panel | #e74c3c | 195 |
  | Lookalike page | copies the real pay screen | #8e44ad | 375 |
  | Middleman | keeps the card details | #e74c3c | 555 |
- **Arrows:** `#bbb` width-1.5 horizontal arrows with filled triangular heads at y=102, from (170,102) to (193,102), (350,102) to (373,102), (530,102) to (553,102).
- **Dashed real path:** green dashed rectangle (`#27ae60`, width 2, dash [6,4]) at x=195, y=185, 335×55; inside, bold 13px `#27ae60` centered title "Real payment page" at y=208 and 10px `#666` sub-line "dashed = never reached; the parking stays unpaid" at y=228. A dashed `#27ae60` width-1.5 path from below the Driver box: vertical line (92,135)→(92,212), then horizontal (92,212)→(185,212), with a small filled green arrowhead at (193,212).
- **Bottom line (bold 12px `#e74c3c`, centered, y=262):** "the fine still arrives — the payment went to the wrong hands."
- **Caption (bottom center, 11px `#999`, y=287):** "The attack happens before any network connection — the pointer itself was swapped."

## 3. What stops it: read the address, not the square

The one human checkpoint sits between the scan and the payment.

- **Defense:** the phone shows the address before opening.
- **Fact:** reading that line is the one human checkpoint.
- **Defense:** the official app skips the sticker entirely.
- **Defense:** raised edges or a sticker layer can reveal a swap.
- **Defense:** tamper-evident printing marks any overlay.
- **Defense:** short official domains are easy to recognize.

**Key point (red-left-border box):** **Win:** the preview turns an invisible swap into one readable line of text.

### Visualization (canvas `c3`, 720×300)

Checkpoint diagram: scan → magnified address preview in the center → open or stop.

- **Title (bold 13px `#1a5276`, top center):** "The address preview is the single gate".
- **Scan box:** 120×60 at (30, 115), color `#1a5276` (fill 0.12 alpha, stroke width 2); bold 13px title "Scan" centered at y=140; 10px `#666` sub-line "camera reads the square" at y=158.
- **Preview box (magnified center):** 320×150 at (200, 70), stroke `#2980b9` width 2, fill `#2980b9` at 0.06 alpha; 11px `#666` centered header "address preview" at y=90. Two candidate address lines, centered at x=360:
  - bold 14px `#27ae60` at y=125: "✓ meter-pay.example"
  - bold 14px `#e74c3c` at y=160: "✗ meterr-pay.example"
  - 10px `#666` centered at y=195: "one extra letter — read it slowly".
- **Outcome boxes** 120×50 (fill 0.12 alpha, stroke width 2; bold 13px title centered at y+22; 10px `#666` sub-line at y+38):
  | Title | sub-line | color | x | y |
  |---|---|---|---|---|
  | Open | address matches | #27ae60 | 570 | 85 |
  | Stop | anything looks off | #e74c3c | 570 | 165 |
- **Arrows:** `#bbb` width-1.5 arrows with filled heads: (150,145) to (198,145); (520,110) to (568,110); (520,190) to (568,190).
- **Bottom line (bold 12px `#1a5276`, centered, y=255):** "reading one line of text is the only human checkpoint."
- **Caption (bottom center, 11px `#999`, y=285):** "Fictional domains for illustration — the lookalike differs by one letter."

## Regeneration instructions

- **Layout:** backlog detail-page style — h1 with `border-bottom: 2px solid #2980b9`, `.subtitle`, one `.intro` callout, then one `.lang-section` per numbered h2 (also blue-underlined). Each section holds a `table.layout` (border-collapse, full width) with one `<tr>`: left `td.text-col` (45%) for paragraphs/bullets/`.key-point`, right `td.viz-col` (55%) for the canvas.
- **Bullet style:** every bullet is one non-wrapping line — a bold colored label plus a phrase of roughly 55 characters or fewer; in HTML the label is `<span class="pt-label" style="color:COLOR">Label:</span>` with `.pt-label { font-weight: bold; }`. Long ideas are split into more labeled bullets, never wrapped. Lead paragraphs are at most one short sentence.
- **Label colors by meaning:** `#1a5276` blue = mechanism/fact (Setup, Fact, Mechanism); `#27ae60` green = defense/win (Defense, Win); `#e74c3c` red = risk/loss (Risk, Taken); `#e67e22` orange = scene/context (Scene). Key-point boxes open with the same colored bold lead word followed by one short sentence.
- **Page CSS:** body system-ui sans-serif, background `#fff`, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; h2 1.3rem `#1a5276`; `.subtitle` `#666` 0.95rem; `.intro` background `#f0f4f8` with `border-left: 3px solid #2980b9`; `.key-point` background `#f8f9fa` with `border-left: 3px solid #e74c3c`, 0.9rem; `.pt-label` bold; ul 0.92rem; canvases `width: 100%` with `1px solid #e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic sizes `c1` 720×300, `c2` 720×300, `c3` 720×300; a shared `setupCanvas(id, w, h)` helper sizes the backing store from the displayed width times `window.devicePixelRatio` and scales the context accordingly. All data arrays are literal and hardcoded — no `Math.random` or `Date.now`.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, plus `#2980b9`, `#8e44ad`, gray text `#666`/`#999`.
- **Content rule:** never draw a real scannable QR code — the block grids are abstract literal patterns only; all domains are fictional `.example`-style; unsourced patterns keep their hedges ("reported in several cities", "can reveal").
- In regenerated HTML, any card/page links use `.html` extensions (this page has none).
