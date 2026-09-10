# Cargo Cult A/B Testing

**Page type:** detail page (two-column obj-table layout: text left ~40%, canvas right ~60%, one row per section)
**HTML title tag:** Cargo Cult A/B Testing — A/B Testing Pitfalls

**Subtitle:** Organizational — The ritual without understanding. Test runs, feature ships regardless.

## Section 1: Organizational — The ritual without understanding. Test runs, feature ships regardless.

- Test run. Results analyzed. Report written. Feature ships... regardless. The TEST is a RITUAL — signals "data-driven culture."
- Decisions made same way as always (gut + politics). Test is cultural performance.
- Signs: No test ever killed a feature. Negative results "complex, need context." Positive results "clear." Asymmetric interpretation.
- "We A/B test everything" = recruiting/investor story. FOLLOWING data is hard/costly. The STORY of being data-driven = 90% of value without pain.

**Correct approach:** Track decision-reversal rate. In genuinely data-driven orgs: 30-50% of tests = NOT shipping.

**The tell:** Decision-reversal rate <10% = tests are decoration. Ask: when did a test LAST change a decision?

### Visualization (canvas `c1`, 720×340)

Circular process-loop diagram with the A/B test as a disconnected island, on light gray background (`#f8f9fa`).

- **Circular flow:** circle of radius 80 centered at (w/2, h/2−5); four arc segments in blue `#1a5276` (width 2) with filled arrowheads connecting four steps positioned at compass points; bold 16px blue labels placed 28px outside the circle: "Decide" (top), "Build" (right), "Ship" (bottom), "Next..." (left).
- **A/B test island:** 120×50 dashed box (dash 5/3) at x=center+170, y=center−45; fill `rgba(231,76,60,0.1)`, stroke `#e74c3c` width 2; bold 18px red text "A/B Test" and 16px red "(island)" inside.
- **Disconnection:** thin light-gray `#bbb` dashed line (dash 3/3) from the circle's right edge to the island box, with a bold 16px red "✗" mark at its midpoint.
- **Bottom label (17px `#555`, centered, 10px above bottom):** "Test is in the process but not in the decision. Decoration."

## Section 2: Real Example: Copying the One-Click Winner

- After Amazon's one-click checkout became famous as a huge revenue driver, many smaller online stores copied the button expecting the same lift for themselves.
- What made it work at the original was invisible context: millions of shoppers with payment cards already on file and years of built-up trust. A first-time visitor to a small store has neither, so the same button has nothing to work with.
- Stores that copied it often measured flat or even negative results, and some shipped it anyway because "it obviously works" — at that point the test had become a formality, not a decision.

### Visualization (canvas `c2`, 720×300)

Two-bar chart around a zero baseline: same feature, positive lift at the original vs negative lift when copied. Light gray background (`#f8f9fa`).

- **Title (bold 17px `#2a2a2a`, centered at y=28):** "Same Button, Different Context".
- **Baseline:** thin gray `#999` line from x=60 to x=660 at y=160, labeled "0%" (14px `#666`) at the left.
- **Scale:** 10 px per percentage point.
- **Bar 1 (above baseline):** 120 wide at x=160, height 80 (+8%); fill `rgba(39,174,96,0.35)`, stroke `#27ae60` width 2; bold 16px green value label "+8%" above; 14px `#333` two-line caption below the baseline: "At the famous original" / "(cards on file, loyal repeat buyers)".
- **Bar 2 (below baseline):** 120 wide at x=440, height 30 (−3%); fill `rgba(231,76,60,0.35)`, stroke `#e74c3c` width 2; bold 16px red value label "-3%" below the bar; 14px `#333` two-line caption below: "Copied to a small store" / "(guest checkouts, first-time buyers)".
- **Takeaway (15px `#555`, centered, 12px above bottom):** "The winner was the context, not the feature — copying the ritual without the ingredients."

## Regeneration instructions

- **Layout:** standard detail-page `.obj-table`: full-width table, one `<tr>` per section; left `<td>` (40%) holds `.obj-title` + bullets/paragraphs, right `<td>` (60%, centered) holds the canvas.
- **Page style:** body system sans-serif, white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.6em `#1a5276`; subtitle `#666`; table cell borders `1px solid #e0e0e0`, padding 20px 24px; `.obj-title` 1.05em, weight 600, `#1a5276`; `strong` in `#1a5276`. No nav bar, no back/home links.
- **Canvas:** declare intrinsic `width`/`height` attributes as given per chart; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, gray text `#666`/`#333`/`#555`.
