# Naturalistic Fallacy / Appeal to Tradition

**Page type:** detail page (two-column obj-table layout: text left ~40%, two stacked canvases right ~60%, single row)
**HTML title tag:** Naturalistic Fallacy / Appeal to Tradition — Pseudoscience in Data Analysis

**Subtitle:** "Natural = Good" or "We've Always Done It This Way = It's Correct"

## Section 1: "Natural = Good" or "We've Always Done It This Way = It's Correct"

- **"Natural" products:** Arsenic, cyanide, and smallpox are natural. "Natural" is a marketing category, not a synonym for safe or effective — evolution didn't optimize for human comfort.
- **"Traditional" methods:** "We've always cleaned data this way." Tradition is evidence that something survived, not that it's optimal — bloodletting survived 2000 years. Your traditional ETL pipeline might be 10x slower than necessary, and "we've always done it" is not a defense.
- **"Organic" data:** "Use organic growth metrics, not paid." If paid users convert and retain at the same rate, the source doesn't matter — "organic = good" is a value judgment dressed as an analytical distinction.
- **"Human intuition over algorithms":** Human judgment is biased, inconsistent, and fatigued. Where algorithms demonstrably outperform humans (radiology, bail decisions, loan approvals), insisting on human judgment is appeal to nature.
- **"We've never needed X before":** You also never had the current scale, complexity, or competitive landscape before. The world changed, and past absence of need is not current absence of need.

**Why it's pseudoscience:** "Natural" and "traditional" are emotional labels, not evidence — whether something works is determined by testing, and hemlock is both natural and ancient yet will still kill you.

### Visualization (canvas `c1`, 720×340)

Two-column list comparison: deadly natural things vs life-saving synthetic things.

- **Title (bold 17px, `#1a5276`, top center):** "\"Natural\" and \"Traditional\" Are Emotional Labels, Not Evidence".
- **Left box:** rectangle at x=50, y=45, 290×150, fill `rgba(231,76,60,0.1)`, stroke red `#e74c3c` width 1. Header (bold 17px red, centered at x=195, y=65): "Natural:". Below, 17px `#333` centered list at 22px spacing: Arsenic, Cyanide, Smallpox, Hemlock, Asbestos.
- **Right box:** rectangle at x=380, y=45, 290×150, fill `rgba(39,174,96,0.1)`, stroke green `#27ae60` width 1. Header (bold 17px green, centered at x=525, y=65): "Synthetic:". Below, 17px `#333` centered list: Antibiotics, Vaccines, Anesthesia, Water purification, Insulin.
- **Bottom takeaway (bold 17px red `#e74c3c`, centered at y=220):** "\"Natural = good\" is a marketing category, not a scientific one."

### Visualization (canvas `c2`, 720×300)

Three-panel is/ought gap diagram (Hume's Guillotine).

- **Title (bold 17px Arial, `#2c3e50`, top center):** "The Is/Ought Gap (Hume's Guillotine)".
- **Left column ("IS"):** rectangle at x=60, y=35, 180 wide, height h-55, fill `rgba(26,82,118,0.1)`, stroke `#1a5276` width 2. Header (bold 16px `#1a5276`, centered, two lines): "What IS" / "(Descriptive)". Below, 16px centered lines at 20px spacing: "Nature does X", "Tradition is Y", "History shows Z", "Data says W".
- **Right column ("OUGHT"):** rectangle at x=w-240, y=35, 180 wide, same height, fill `rgba(39,174,96,0.1)`, stroke `#27ae60` width 2. Header (bold 16px `#27ae60`, two lines): "What OUGHT" / "(Prescriptive)". Below, 16px centered lines: "We should do X", "Policy must be Y", "The right choice is Z".
- **Middle gap:** the region between the columns filled `rgba(231,76,60,0.1)`; both inner edges drawn as jagged red `#e74c3c` lines (width 2) to suggest a chasm. The jitter is purely decorative and must be deterministic: a seeded Park-Miller LCG `lcg(20250120)` (`s = (s * 16807) % 2147483647`, return `s / 2147483647`) supplies one draw per vertex, giving a horizontal offset in `[0, 10)` px at every 8px vertical step — the left edge offsets right of `gapX`, the right edge offsets left of `rightX`. One generator per chart function; never `Math.random()`, so the figure is byte-identical on every load and resize redraw.
- **Gap labels (centered in the gap):** bold 18px red, three stacked lines around vertical center: "THIS GAP" / "REQUIRES" / "EVIDENCE"; then 16px red: "not just" / "observation".

## Regeneration instructions

- **Layout:** standard detail-page `.obj-table`: full-width table, one `<tr>`; left `<td>` (40%) holds `.obj-title` + bullet list + closing "Why it's pseudoscience" paragraph, right `<td>` (60%, centered) holds two stacked canvases (`c1` 720×340, `c2` 720×300).
- **Page style:** body system sans-serif (-apple-system, BlinkMacSystemFont, 'Segoe UI'), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.6em `#1a5276`; `.subtitle` `#666` 1.0em; p `#333` 0.95em; ul 0.9em `#333`, li margin 6px 0; `strong` in `#1a5276`; table cell borders `1px solid #e0e0e0`, padding 20px 24px, vertical-align middle; `.obj-title` 1.05em, weight 600, `#1a5276`. No nav bar, no back/home links.
- **Canvas:** declare intrinsic `width`/`height` attributes per chart; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. A shared `lcg(seed)` helper sits directly after `setup(id)` and is the only source of pseudo-randomness on the page. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, dark slate `#2c3e50`, gray `#666`/`#333`; tint fills `rgba(231,76,60,0.1)`, `rgba(39,174,96,0.1)`, `rgba(26,82,118,0.1)`.
- In regenerated HTML, any card links use `.html` extensions.
