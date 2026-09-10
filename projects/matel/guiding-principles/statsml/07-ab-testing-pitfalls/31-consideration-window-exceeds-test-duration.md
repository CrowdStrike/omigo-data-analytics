# Consideration Window Exceeds Test Duration

**Page type:** detail page (two-column obj-table layout: text left ~40%, canvas right ~60%, one row per section)
**HTML title tag:** Consideration Window Exceeds Test Duration — A/B Testing Pitfalls

**Subtitle:** Measurement Flaw — High-price purchases have research cycles of weeks or months. A 2-week A/B test captures noise from pre-test decisions leaking in and in-test decisions leaking out, with the distortion scaling with item price.

## Section 1: The Core Problem

- A user researching a $2,000 laptop spends 3–6 weeks comparing options. A user buying a $15 phone case decides in one session.
- A/B test runs for 14 days. The phone-case buyer's entire journey (search → checkout) fits cleanly within the window. The laptop buyer started researching 4 weeks before the test and might not purchase until 2 weeks after.
- Ideally you'd only count journeys where the first search AND the final payment both fall within the test window. But that's impractical — it would exclude the highest-value transactions entirely.
- Result: your test metric is dominated by low-price impulse purchases (clean signal) and polluted by high-price purchases that are either pre-decided (attributed to test but test had no influence) or still-pending (test influenced them but conversion happens after test ends).

**Callout (`.philosophy` box):** **Key insight:** The higher the item price, the longer the consideration window, and the less likely the full purchase journey fits inside any practical test duration. Price and noise are positively correlated — and high-price items dominate revenue metrics.

### Visualization (canvas `c1`, 720×340)

Scatter plot of item price vs. consideration window, with the 14-day test window shaded — only cheap items fit inside.

- **Title (bold 14px `#1a5276`, top center):** "Item Price vs. Consideration Window".
- **Axes:** origin at (80, 280), plot area 580 wide × 220 tall; L-shaped axes in `#333`, width 1.5. Rotated y-axis label "Item Price" (gray `#333`, 12px) at left; x-axis label "Consideration Window (days)" centered below.
- **Scales:** x from 0 to 70 days (days clamped at 70), y from 0 to $5,500.
- **Test window shading:** 0–14 days region filled `rgba(39,174,96,0.08)`, right edge a dashed green line (`#27ae60`, dash 5/3, width 2) at day 14; two-line bold green label inside near top: "Test window" / "(14 days)".
- **Data points (6px filled circles), each with a label "Name ($price)" to its right (10px):**
  - Green `#27ae60` (fits in window): Phone case (1 day, $15), T-shirt (2, $30), Book (3, $50), Headphones (5, $80), Shoes (7, $150), Monitor (14, $300).
  - Red `#e74c3c` (exceeds window): Phone (21, $600), Camera (30, $1000), Laptop (45, $1500), Furniture (60, $3000), Car deposit (90, $5000).
- **Y-axis ticks (gray `#888`, 10px, right-aligned):** $500, $1000, $2000, $3000, $5000.
- **X-axis ticks (gray `#888`, 10px, centered):** 7d, 14d, 30d, 45d, 60d.
- **Legend (below axis, 11px):** green "● Fits in test window", red "● Exceeds test window — noise source".

## Section 2: Three Sources of Noise

- **Pre-test leakage (left-censoring):** User decided to buy before the test started. They happen to convert during the test. Their conversion is counted but the test variant had zero influence — the decision was already made.
- **Post-test leakage (right-censoring):** User was genuinely influenced by the test variant during research, but converts 3 weeks after the test ends. This real signal is never counted. Treatment's effect is underestimated.
- **Mid-test contamination:** User sees Treatment for the first week of their research, then the test ends and they see the default for the remaining 3 weeks before purchasing. Partial exposure, diluted attribution.

**Net effect:** Revenue metrics become extremely noisy. Small improvements to the high-consideration funnel are invisible because they're buried under pre-decided conversions and lost post-test conversions. Tests are biased toward detecting changes that affect impulse purchases only.

### Visualization (canvas `c2`, 720×340)

Timeline diagram showing three purchase journeys relative to a shaded 14-day test window.

- **Title (bold 14px `#1a5276`, top center):** "Three Noise Sources in a 14-Day Test".
- **Timeline:** horizontal band from x=80 to x=640; test window occupies the middle 25%–75% of the band, filled `rgba(39,174,96,0.08)` from y=40 to y=320, bounded by dashed green vertical lines (`#27ae60`, dash 5/3, width 2); bold green label "TEST WINDOW" centered at top of band.
- **Time labels (gray `#888`, 11px):** "4 weeks before" (left of window), "Day 1" (window start), "Day 14" (window end), "4 weeks after" (right of window), along the bottom at y=330.
- **Journey 1 (y≈90), red `#e74c3c`:** bold label "Pre-decided ($1200 camera)"; thick red line (width 3) starting well before the test and ending at a red dot just inside the window start; dot label "💰 buys (but decided weeks ago)"; red annotation below: "↑ Counted but test had no influence".
- **Journey 2 (y≈160), orange `#e67e22`:** bold label "Influenced but late ($1800 laptop)"; thick orange line starting inside the window and extending well past the window end to an orange dot at ~88% of the timeline; dot label "💰 converts after test ends"; orange annotation below: "↑ Real influence but never counted".
- **Journey 3 (y≈230), green `#27ae60`:** bold label "Clean journey ($20 book)"; short thick green line entirely inside the window ending at a green dot; dot label "💰 clean signal".

## Section 3: Why Standard Fixes Don't Work

- **"Run the test longer"** — helps marginally, but a 6-week test still misses a 10-week laptop purchase cycle. And the longer you run, the more seasonal/external confounds creep in.
- **"Use intent metrics"** — add-to-cart, wishlist, time-on-page. Better signal for high-consideration items but doesn't measure actual revenue impact.
- **"Segment by price tier"** — post-hoc segmentation inflates multiple comparison risk. But pre-registered price-tier analysis is valid.
- **"Attribution window"** — count conversions up to 30 days post-exposure. Better, but you're now measuring a mix of test-influenced and pre-decided purchases with no way to separate them.

**Realistic mitigations:** (1) Pre-register separate metrics for impulse vs. considered purchases. (2) Use leading indicators (search depth, comparison actions, cart additions) as primary metric for high-price tiers. (3) Run longer holdout experiments for revenue-critical features. (4) Accept that 2-week tests cannot reliably measure impact on high-consideration purchases — and don't pretend they can.

**The tell:** Revenue per user is "flat" in a 2-week test, but 60-day holdout shows +8%. The short test was too brief to capture the actual purchase decisions it influenced.

### Visualization (canvas `c3`, 720×340)

Two stacked bars comparing transaction volume composition vs. revenue share composition across three purchase tiers.

- **Title (bold 14px `#1a5276`, top center):** "Revenue Contribution vs. Measurement Quality".
- **Left stacked bar** ("Transaction Volume", bold 13px `#333` below bar): 200px wide at x=100, 240px tall from y=50, outer border `#333` width 1.5. Segments top-to-bottom: 70% fill `rgba(39,174,96,0.4)` labeled in green "70% impulse" / "(clean signal)"; 20% fill `rgba(230,126,34,0.4)` labeled in orange "20% medium"; 10% fill `rgba(231,76,60,0.4)` labeled in red "10% high".
- **Right stacked bar** ("Revenue Share", same style, at x=450): 20% green labeled "20% revenue"; 30% orange labeled "30% revenue"; 50% red labeled "50% revenue" / "(noisiest signal)".
- **Right-side annotation (bold 12px red `#e74c3c`, five lines):** "Half your revenue" / "comes from the" / "segment you" / "can't measure" / "in 14 days."

## Section 4: Illustrative Example: Mattress Shopping Measured in One Day

- An online mattress retailer judged its page tests by whether a visitor bought within one day of seeing the new version, but almost nobody buys a mattress that fast. Shoppers typically compare brands, read reviews, and sleep on the decision for several weeks before paying.
- Most of the purchases the new page actually influenced landed weeks later, long after the one-day counting window had closed, so the tests kept reporting "no effect" even for changes that genuinely helped people decide.
- The practical fix is to match the counting window to how long the decision really takes, or to grade the test on earlier signals that do show up quickly — like saving a product, requesting a sample, or returning to the page (leading indicators).

### Visualization (canvas `c4`, 720×300)

Timeline of a 42-day mattress purchase journey against a one-day measurement window.

- **Title (bold 16px `#2a2a2a`, top center):** "Mattress Shopping: Weeks of Deciding, One Day of Counting".
- **Timeline:** gray baseline (`#999`, width 2) from x=70 to x=650 at y=150, spanning day 0 to day 42; tick marks (`#bbb`) with gray labels (14px `#888`) at "day 0", "day 7", "day 14", "day 21", "day 28", "day 35", "day 42".
- **Measurement window:** green sliver from day 0 to day 1, 60px tall above the baseline, fill `rgba(39,174,96,0.25)`, stroke `#27ae60` width 2; bold green label above (14px, left-aligned): "metric counts purchases here (1 day)".
- **Research journey dots (orange `#e67e22`, 6px, labels 14px above each):** day 6 "compares brands", day 15 "reads reviews", day 23 "returns to page"; dashed orange connector line (dash 4/3, width 2) at y=135 running from day 1 to day 31.
- **Purchase dot:** red `#e74c3c`, 8px, at day 31; bold red label above: "buys on day 31 — never counted".
- **Bottom takeaways (centered):** bold red 15px "The counting window closed a month before the customer decided"; below in `#333` 15px: "Match the window to the decision, or measure earlier signals like saves and samples".

## Regeneration instructions

- **Layout:** standard detail-page `.obj-table`: full-width table, one `<tr>` per section; left `<td>` (40%) holds `.obj-title` + bullets/paragraphs, right `<td>` (60%, centered) holds the canvas. Sections 3 and 4 share one `.obj-table` (two `<tr>` rows); sections 1 and 2 are each their own table.
- **Page style:** body system sans-serif, white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.6em `#1a5276`; subtitle `#666` 1.0em; table cell borders `1px solid #e0e0e0`, padding 20px 24px, vertical-align middle; `.obj-title` 1.05em, weight 600, `#1a5276`; `strong` in `#1a5276`. No nav bar, no back/home links.
- **Callout style:** `.philosophy` — background `#f0f4f8`, left border `4px solid #2980b9`, padding 12px 16px, 0.9em.
- **Canvas:** declare intrinsic `width`/`height` attributes as given per chart; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, gray text `#666`/`#333`/`#888`.
