# 6. Charm Pricing & Number Psychology

**Page type:** detail page (two-column obj-table layout: text left 45%, canvas right 55%, one h2 + table per section, philosophy callouts top and bottom)
**HTML title tag:** 6. Charm Pricing & Number Psychology

**Subtitle:** $19.99, $97, $199. Why the brain reads prices left-to-right and anchors on the first digit — and how entire pricing tiers are built on cognitive thresholds, not cost.

## Callout (philosophy box, top)

**The left-digit effect:** The brain encodes $19.99 as "nineteen-something" and $20.00 as "twenty." The perceived gap between $19.99 and $20.00 feels much larger than the gap between $19.98 and $19.99 — even though both are one cent. Pricing strategy exploits this asymmetry at every threshold.

## Section 1: The Left-Digit Effect

**Obj-title:** Why $19.99 ≠ $20.00 in Your Brain

Math-box 1:

**How the brain processes prices:**

1. Read left-to-right
2. Encode the *first digit* immediately
3. Subsequent digits get less attention
4. "19.99" → encoded as "teens" / "nineteen"
5. "20.00" → encoded as "twenties"

`The magnitude encoding happens before you finish reading.`

Math-box 2:

**Research (Thomas & Morwitz, 2005):**

- $2.99 vs $3.00: participants estimated the gap as ~$0.30 (10× actual)
- $3.59 vs $3.60: participants estimated gap correctly (~$0.01)

The effect ONLY fires at digit-change boundaries.
$4.99→$5.00 feels huge. $4.49→$4.50 feels like nothing.

`Thresholds: $X.99 → $(X+1).00 is the cliff.`

Bullets:

- **Gas stations:** $3.999/gallon — exploits the same effect at the THIRD digit
- **Why not $19.97?** Some marketers use $X7 (feels "calculated, not manipulative"). Others use $X9 (maximizes left-digit anchoring).
- **Luxury exception:** $200 (round) signals "premium." $199.99 signals "deal." Context matters.

### Visualization (canvas `canvas1`, 720×380)

Bar chart of perceived gap at each one-cent price step, with the digit-change step spiking.

- **Title (bold 14px, top center, `#1a5276`):** "Perceived Gap vs Actual Gap".
- **Axes:** origin (100, 300), plot 520×230, `#1a5276` 1.5px; x-axis label "Price point" (11px `#666`).
- **Data:** price points `$19.97, $19.98, $19.99, $20.00, $20.01, $20.02`; relative perceived-gap bar heights `[0.15, 0.15, 0.85, 0.15, 0.15, 0.15]` of plot height; bars 60px wide, 25px gap.
- **Bar style:** index 2 ($19.99, the digit-change step) stroked `#e74c3c` with fill `rgba(231,76,60,0.3)`; all others stroked `rgba(26,82,118,0.35)` with fill `rgba(26,82,118,0.15)`. Price labels 10px `#333` below bars. Gap label above each bar except the last: "$0.01" in 10px `#666`, except index 2 which reads "HUGE" in bold 11px `#e74c3c`.
- **Top annotation (bold 12px `#e74c3c`, centered at y=50):** "← All gaps are $0.01. Only this one FEELS big. →".
- **Actual-gap line:** dashed green horizontal line (`#27ae60`, 1.5px, dash 4/3) at the 0.15 bar-height level; right-aligned 10px green label above it: "Actual gap (constant $0.01)".
- **Bottom captions (11px `#666`, centered):** "Perceived gap between $19.99 → $20.00 is ~6× larger than $19.98 → $19.99" (y=345); "Same one-cent difference. Completely different psychological weight." (y=363).

## Section 2: Cognitive Price Thresholds

**Obj-title:** The Invisible Walls in Your Head

Math-box 1:

**Major thresholds (digital products):**

- Under $5: impulse buy. No deliberation.
- $5–$20: minor decision. Compare briefly.
- $20–$50: evaluate. Read reviews.
- $50–$100: research. Compare alternatives.
- $100+: serious purchase. Sleep on it.

`Products price just below the next threshold.`

Math-box 2:

**Why SaaS uses $9, $29, $49, $99:**

Not because features cost that much.
Because these sit just BELOW cognitive walls:

$9 → "under $10" (impulse zone)
$29 → "under $30" (low consideration)
$49 → "under $50" (medium consideration)
$99 → "under $100" (serious but not triple-digit)

`The price architecture matches brain thresholds, not costs.`

Bullets:

- **App stores:** $0.99, $1.99, $4.99 — always below the next dollar
- **Restaurants:** $14.95 not $15.00 — below the "fifteen" threshold
- **Real estate:** $499,000 not $500,000 — the effect works even at massive scale

### Visualization (canvas `canvas2`, 720×380)

Inverted-pyramid tier diagram: five centered bars narrowing with price, one per cognitive threshold.

- **Title (bold 14px, top center, `#1a5276`):** "Cognitive Price Thresholds".
- **Tier bars (centered horizontally; start y=50, row height 56, bar height rowH−8; fill = tier color + `18` alpha, 1.5px stroke in tier color; range bold 12px in tier color at bar left, behavior 12px `#333` below it, example prices 11px `#666` right-aligned at bar right):**
  - "Under $5" / "Impulse buy" / "$4.99", `#27ae60`, width 580
  - "$5–$20" / "Quick decision" / "$9, $14.99, $19", `#2980b9`, width 480
  - "$20–$50" / "Compare options" / "$29, $39, $49", `#e67e22`, width 370
  - "$50–$100" / "Research first" / "$59, $79, $99", `#8e44ad`, width 250
  - "$100+" / "Sleep on it" / "$149, $199, $299", `#e74c3c`, width 150
- **Friction labels (11px `#999`, centered below tiers):** "← Less friction" and "More friction →" (both drawn centered at the same point, overlapping).
- **Bottom captions (centered):** bold 11px `#1a5276` at y=348: "Products price just BELOW each threshold. $49 not $52. $99 not $110."; 11px `#666` at y=366: "The price architecture matches brain thresholds, not production costs."

## Section 3: Anchoring in Pricing Pages

**Obj-title:** The First Number You See Sets the Scale

Math-box 1:

**Anchoring (Tversky & Kahneman, 1974):**

The first number presented biases all subsequent judgments.

"Was $299, now $149" → $149 feels like a steal
"$149" alone → feels expensive

The $299 is the anchor. It may never have been sold at that price.
`The reference point is manufactured.`

Math-box 2:

**Pricing page architecture:**

Show the most expensive plan FIRST (or most prominently).

Enterprise: $199/mo (the anchor)
Pro: $49/mo ← "75% less than Enterprise!"
Basic: $9/mo

Without Enterprise visible, $49 feels expensive.
With Enterprise visible, $49 feels like the smart middle.

`The expensive tier might have 0 customers. Its job is to sell the middle tier.`

Bullets:

- **Wine lists:** $200 bottle exists to make $60 bottle feel reasonable
- **"Compare at $X":** Retail stores show "original price" that no one ever paid
- **Negotiation:** First number named anchors the entire discussion. Name a high anchor first.

### Visualization (canvas `canvas3`, 720×380)

Two pricing-card mockups (without vs with anchor) above an annotated number line.

- **Title (bold 14px, top center, `#1a5276`):** "Anchoring Effect on Pricing Pages".
- **Left scenario (header bold 12px `#e74c3c`, centered at x=190, y=50):** "WITHOUT anchor". One card (x=90, y=65, 200×80, fill `#fafafa`, 1px `#ddd` border) with bold 24px `#333` "$49/mo" and 12px `#666` "Pro Plan". Below at y=170, 11px `#e74c3c`: "Feels: \"That's expensive\"".
- **Right scenario (header bold 12px `#27ae60`, centered at x=530, y=50):** "WITH anchor". Two cards:
  - Enterprise anchor card (x=410, y=65, 110×70, fill `#f0f0f0`, 1px `#ccc` border): bold 16px `#999` "$199/mo", 10px "Enterprise".
  - Pro target card (x=535, y=60, 130×80, fill `#eef7ee`, 2px `#27ae60` border): bold 20px `#27ae60` "$49/mo", 11px `#333` "Pro ★ POPULAR".
  - Below at y=170, 11px `#27ae60`: "Feels: \"75% cheaper — great deal!\"".
- **Comparison line (bold 13px `#1a5276`, centered, y=210):** "Same $49. Completely different perception.".
- **Number line:** horizontal `#1a5276` 1.5px line at y=240, width 520 centered; tick marks with 11px `#333` labels at relative positions: "$0" (0), "$49" (0.245), "$100" (0.5), "$199" (0.995).
- **Number-line annotations (centered):** bold 11px `#27ae60`: "$49 sits at 25% of the $199 anchor"; 11px `#666`: "\"I'm saving 75%\" — even though nobody actually pays $199".
- **Bottom captions (centered):** bold 12px `#1a5276` at y=345: "The expensive tier may have 0 customers. Its job is to sell the middle tier."; 11px `#666` at y=365: "Same principle as the decoy effect — but using numbers instead of options.".

## Regeneration instructions

- **Layout:** detail page. h1 + `.subtitle`, `.philosophy` callout, then per section: `<h2>N. Title</h2>` (numbered, bottom border `2px solid #2980b9`) followed by a `.obj-table` with one `<tr>`: left `<td>` (45%) holds `.obj-title`, two `.math-box` divs, and a `<ul>`; right `<td>` (55%, centered) holds the canvas. Closing `.philosophy` callout after the last section.
- **Page CSS:** body `-apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif`, white background, text `#2a2a2a`, padding 40px 20px, line-height 1.6; h1 1.8em `#1a5276`; h2 1.4em `#1a5276` with `border-bottom: 2px solid #2980b9`; subtitle `#666` 1.05em; `.obj-table td` border `1px solid #e0e0e0`, padding 20px 24px, vertical-align middle; `.obj-title` 1.05em weight 600 `#1a5276`; `ul` 0.9em `#333`; `strong` `#1a5276`; `.philosophy` background `#f0f4f8`, left border `4px solid #2980b9`, padding 12px 16px, 0.9em; `.math-box` background `#f8fafb`, border `1px solid #e0e0e0`, radius 6px, padding 16px 20px, 0.9em; `.math-box code` background `#eef2f7`, padding 2px 6px, radius 3px. No nav bar, no back/home links.
- **Palette:** primary blue `#1a5276`, accent blue `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, purple `#8e44ad`, bar fill `rgba(26,82,118,0.35)`.
- **Canvas:** intrinsic width/height attributes 720×380; shared `setupCanvas(id, w, h)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- In regenerated HTML, any card links use `.html` extensions (this page has none).
