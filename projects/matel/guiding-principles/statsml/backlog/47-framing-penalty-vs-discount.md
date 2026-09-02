# Framing: Penalty vs Discount

**Page type:** detail page (backlog-style 2-col text/viz layout: numbered h2 sections, text left ~45%, canvas right ~55%, closing key-point)
**HTML title tag:** Framing: Penalty vs Discount

**Subtitle:** Identical math, opposite psychology — loss-frame pricing destroys relationships while gain-frame pricing builds loyalty

## Intro callout

Companies charge a 5% penalty for late payment and get fights, support tickets, and churn. Bake the same 5% into the base price and offer a 5% discount for autopay instead: same cash flow, zero fights. The framing is the product experience, even when the arithmetic is identical.

## 1. The Observation

- **Penalty frame:** charge `5% penalty for late payment` (even by a single day). Result: fights, support tickets, churn, resentment. Customers feel punished.
- **Discount frame:** bake the 5% into the base price upfront and offer `5% discount for autopay`. Result: customers feel rewarded for choosing autopay.

Key-point callout: **Same money. Opposite emotional response.** The cash flow is identical — only the reference point moves.

### Visualization (canvas `c1`, 720×320)

Flow diagram: two three-box rows showing the same cash flow through opposite frames.

- **Title (bold 14px `#1a5276`, top center):** "Two Frames, Same Cash Flow"
- **Row 1 — Loss frame** (bold red 13px left label "Loss frame" at x=15, y=62; boxes 60px tall at y=72):
  1. White box, blue `#1a5276` stroke, 180 wide at x=15: "Price: $100" (bold) / "+5% late penalty".
  2. Orange `#e67e22` arrow → box at x=262, 190 wide, fill `rgba(231,76,60,0.12)`, red `#e74c3c` stroke: "Customer feels" / "punished".
  3. Orange arrow → solid red box at x=519, 186 wide, white text: "fights, tickets," / "churn, resentment".
- **Row 2 — Gain frame** (bold green 13px left label "Gain frame" at y=180; boxes at y=190):
  1. White box, blue stroke: "Price: $105" (bold) / "-5% autopay discount".
  2. Orange arrow → box fill `rgba(39,174,96,0.12)`, green `#27ae60` stroke: "Customer feels" / "rewarded".
  3. Orange arrow → solid green box, white text: "goodwill, opt-in," / "zero fights".
- **Caption (13px `#444`, centered at y=295):** "identical revenue either way — only the reference point differs"

## 2. Why It Works — Prospect Theory

Prospect Theory (Kahneman & Tversky, 1979) explains the asymmetry:

- Losses hurt ~2× more than equivalent gains feel good
- Penalty = loss frame → anger, "unfair", dispute
- Discount = gain frame → reward, "smart choice", opt-in

Key-point callout: **Deeper pattern:** a company choosing the penalty frame is optimizing for short-term extraction at the cost of long-term relationship. The discount frame costs nothing extra but earns goodwill.

### Visualization (canvas `c2`, 720×340)

Curve chart: the S-shaped prospect-theory value function around a reference point.

- **Title (bold 14px `#1a5276`, top center):** "Prospect Theory Value Function"
- **Axes:** gray `#999` 1px cross centered at (w/2, 175); horizontal half-length 300, vertical half-length 120. Labels in 13px `#444`: "losses" (left end), "gains" (right end), "felt value" (near top of vertical axis).
- **Gain curve:** green `#27ae60`, 2.5px, concave power curve from origin into the upper-right quadrant: v = 55·(x/280)^0.6 over x in 0..280.
- **Loss curve:** red `#e74c3c`, 2.5px, convex and roughly twice as steep into the lower-left quadrant: v = 110·(x/280)^0.6 (plotted at cx−x, cy+v).
- **Markers:** at symmetric distance dx=130 from the reference point, dashed drop-lines (dash 4/4, 1px) in green (up to the gain curve, over to the y-axis) and red (down to the loss curve, over to the y-axis).
- **Annotations:** bold green 13px "5% discount: feels good" near the gain marker; bold red 13px "5% penalty: hurts ~2× more" near the loss marker.
- **Caption (13px `#444`, bottom center):** "same 5% distance from the reference point — the loss side is roughly twice as steep"

## 3. Same Pattern Elsewhere

- **Gas station:** "credit card surcharge" vs "cash discount" — legally different framing, same ¢/gallon gap
- **Insurance:** "penalty for lapse" vs "loyalty discount for continuous coverage"
- **SaaS:** "price increase after promo" vs "introductory discount for new members"
- **Shipping:** "$5 shipping fee" vs "free shipping on orders over $35" (price baked in)

Example (italic): This connects to the decoy effect, subscription traps, and charm pricing — all cases where perception engineering matters more than arithmetic.

### Visualization (canvas `c3`, 720×320)

Paired-frame table diagram: four rows, each with a loss-frame box "=" gain-frame box.

- **Title (bold 14px `#1a5276`, top center):** "Loss Frame vs Gain Frame — Same Gap, Different Label"
- **Column headers** (bold 13px, y=55): red "loss frame" centered at x=300; green "gain frame" centered at x=555.
- **Rows** (starting y=68, row height 42 + 12 gap; domain label bold 13px `#222` left at x=15; loss box 220 wide at x=190, fill `rgba(231,76,60,0.10)`, red `#e74c3c` stroke; bold gray "=" at x=427; gain box 220 wide at x=445, fill `rgba(39,174,96,0.10)`, green `#27ae60` stroke; 12px `#222` centered box text):
  1. Gas station: "credit card surcharge" = "cash discount"
  2. Insurance: "penalty for lapse" = "loyalty discount"
  3. SaaS: "price increase after promo" = "introductory discount"
  4. Shipping: "$5 shipping fee" = "free shipping over $35"
- **Caption (13px `#444`, centered at y=300):** "each pair is arithmetically identical — the frame decides how it feels"

## Closing key-point

**Status:** Raw thought. Needs expansion with more examples. Possible home: `applied-game-theory-behavioral-design/` as "framing-effects-prospect-theory" covering this example plus gas stations, insurance, and SaaS promo framing.

## Regeneration instructions

- **Template:** backlog detail-page layout — h1 with 2px `#2980b9` bottom border, `.subtitle`, `.intro` callout (background `#f0f4f8`, left border `3px solid #2980b9`, 8px 12px padding, 0.9rem), then one `.lang-section` per numbered section: h2 (1.3rem `#1a5276`, 2px `#2980b9` bottom border) followed by a `table.layout` row with `td.text-col` (45%) and `td.viz-col` (55%) holding the canvas. The closing status note is a standalone `.key-point` div after the last section.
- **Callout styles:** `.key-point` — background `#f8f9fa`, left border `3px solid #e74c3c`, 8px 12px padding, 0.9rem. `.example` — italic, `#555`, 0.9rem. `code` — background `#f4f4f4`, 2px 6px padding, 3px radius.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `ul` 0.92rem; canvases `width: 100%` with `1px solid #e0e0e0` border, 4px radius.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`; grays `#444`/`#666`/`#999`.
- **Canvas:** intrinsic width 720, heights 320/340/320 for c1/c2/c3; a shared `setup(id, hgt)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates; a shared horizontal `arrow` helper draws 2px lines with filled triangular heads.
- No nav bar, no back/home links. In regenerated HTML, any card links use `.html` extensions.
