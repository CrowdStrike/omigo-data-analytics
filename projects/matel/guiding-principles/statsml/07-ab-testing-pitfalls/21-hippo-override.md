# HiPPO Override

**Page type:** detail page (two-column obj-table layout: text left ~40%, canvas right ~60%, one row per section)
**HTML title tag:** HiPPO Override — A/B Testing Pitfalls

**Subtitle:** Organizational — VP already decided. A/B test is theater to confirm the decision.

## Section 1: Organizational — VP already decided. A/B test is theater to confirm the decision.

- VP wants Feature X. Test shows no effect or negative. Response: "Test wasn't set up right" / "Metric doesn't capture full value." Ships anyway.
- Test was NEVER going to change the decision — only CONFIRM it.
- "Data-driven culture" where decision rights haven't actually moved from HiPPOs to data.

**Correct approach:** Define BEFORE test: "If result is X, we ship. If Y, we don't." Get VP sign-off on the decision rule. If they won't sign — the test is theater.

**The tell:** Has an A/B test EVER killed a VP-sponsored initiative? If not — confirmation tool, not decision tool.

### Visualization (canvas `c1`, 720×340)

Flow diagram on light gray background (`#f8f9fa`): VP wants X → Run A/B test → branch to two outcome boxes → both converge to Ship X.

- **Boxes:** height 32, `16px sans-serif` centered text in `#1a5276`, stroke `#1a5276` width 1.5:
  - "VP wants X" at x=60, y=30, width 130, fill `rgba(26,82,118,0.1)` (default).
  - "Run A/B test" at x=280, y=30, width 130, default fill.
  - "Supports X → Ship" at x=480, y=54, width 150, fill `rgba(39,174,96,0.15)`.
  - "Opposes X → \"Test wrong\"" at x=480, y=140, width 150, fill `rgba(231,76,60,0.15)`.
  - "Ship X" at x=300, y=180, width 120, fill `rgba(231,76,60,0.2)`.
- **Arrows:** gray `#555`, width 1.5, filled triangular arrowheads (length 8, ±0.4 rad): from "VP wants X" to "Run A/B test"; from "Run A/B test" fanning to both branch boxes; from the bottom of each branch box converging to "Ship X".
- **Bottom label (bold 16px, red `#e74c3c`, centered, 12px above bottom):** "Both outcomes = same decision. Not testing."

## Section 2: Real Example: Amazon's Shopping-Cart Recommendations

- Amazon engineer Greg Linden built a prototype that showed product recommendations on the shopping-cart page, and a senior executive ordered the work stopped, worried it would distract shoppers away from finishing their purchase.
- Linden ran an A/B test anyway, and the recommendations won so convincingly that not having the feature was clearly costing Amazon real money.
- The feature shipped and became a standard part of the store — a rare case where data beat the highest-paid person's opinion (the HiPPO), and only because someone actually ran the test instead of obeying it.

### Visualization (canvas `c2`, 720×300)

Three-step horizontal timeline of boxes with arrows, on white (no background fill drawn except default; note: this canvas draws no gray background rectangle).

- **Title (bold 17px, `#2a2a2a`, top center at y=30):** "Amazon Cart Recommendations: Data vs the HiPPO".
- **Boxes:** three 180×80 boxes at y=100, stroke width 2.5, bold 16px two-line centered labels colored to match the stroke:
  - x=50: "Executive:" / "\"Stop the project\"" — fill `rgba(231,76,60,0.15)`, stroke `#e74c3c`.
  - x=270: "Linden runs the" / "A/B test anyway" — fill `rgba(26,82,118,0.12)`, stroke `#1a5276`.
  - x=490: "Recommendations" / "win big → shipped" — fill `rgba(39,174,96,0.15)`, stroke `#27ae60`.
- **Arrows:** gray `#555`, width 2, horizontal between adjacent boxes at mid-height, filled triangular arrowheads.
- **Captions (14px, 24px below each box, centered):** "opinion" in `#e74c3c` under box 1, "evidence" in `#1a5276` under box 2, "outcome" in `#27ae60` under box 3.
- **Takeaway (15px `#333`, bottom center, 16px above bottom):** "The opinion said stop; the experiment said ship — and the experiment was right"

## Regeneration instructions

- **Layout:** standard detail-page `.obj-table`: full-width table, one `<tr>` per section; left `<td>` (40%) holds `.obj-title` + bullets/paragraphs, right `<td>` (60%, centered) holds the canvas.
- **Page style:** body system sans-serif, white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.6em `#1a5276`; subtitle `#666`; table cell borders `1px solid #e0e0e0`, padding 20px 24px; `.obj-title` 1.05em, weight 600, `#1a5276`; `strong` in `#1a5276`. No nav bar, no back/home links.
- **Canvas:** declare intrinsic `width`/`height` attributes as given per chart; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, gray text `#666`/`#333`/`#555`.
