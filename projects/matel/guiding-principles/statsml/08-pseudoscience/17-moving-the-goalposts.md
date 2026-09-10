# Moving the Goalposts

**Page type:** detail page (two-column obj-table layout: text left ~40%, two stacked canvases right ~60%, single row)
**HTML title tag:** Moving the Goalposts — Pseudoscience in Data Analysis

**Subtitle:** Redefining Success After the Fact So You Never Fail

## Section 1: Redefining Success After the Fact So You Never Fail

- **Product launch:** The goal was "10K signups in first month"; the result was 2K. The new narrative — "engagement is high, and quality of signups matters more than quantity" — redefines success after the result is known, so failure becomes impossible.
- **ML model:** The goal was "beat baseline by 5% accuracy"; the result was -2%. The narrative shifts to "look at the precision on the minority class" — the goalposts move from overall accuracy to a sub-metric that happened to look better.
- **Diet/supplement:** "This will help you lose weight" — no loss; "but it improved your energy" — no change; "but it detoxified your liver" — unmeasurable. Each failed claim is replaced with a vaguer one until it reaches unfalsifiable territory.
- **Strategy:** "This initiative will increase revenue" — revenue flat; "it preserved market share during a downturn" — also flat; "it positioned us for future growth" — unfalsifiable. The initial claim keeps transforming until it can't be disproven.

**Why it's pseudoscience:** Pre-registration exists to prevent this: if success criteria aren't defined before the experiment and can be changed after seeing results, the claim can never fail — and a claim that cannot fail is rationalization, not science.

### Visualization (canvas `c1`, 720×340)

Timeline diagram of goalposts moving right along a horizontal line.

- **Title (bold 17px, `#1a5276`, top center):** "The Goalposts Keep Moving Until Failure Is Impossible".
- **Baseline:** horizontal gray `#999` line (width 2) from x=60 to x=680 at y=120.
- **Goalposts** (vertical 3px lines from y=90 to y=150; label above at y=80 in 17px `#333`, result below at y=170; first three posts/results in red `#e74c3c`, last in orange `#e67e22`):
  1. x=100: "10K signups" / "Failed (2K)"
  2. x=280: "High engagement" / "Failed (avg)"
  3. x=460: "Quality users" / "Failed (same churn)"
  4. x=620: "\"Positioned for growth\"" / "Unfalsifiable!"
- **Arrows:** gray `#999` 20px "→" between the first three goalposts (at post x+60, y=125).
- **Bottom takeaway (bold 17px `#555`, centered at h-8):** "Each failure → redefine success. Eventually reach a claim that cannot be disproven."

### Visualization (canvas `c2`, 720×300)

Split-panel comparison: without vs with pre-registration.

- **Title (bold 16px Arial, `#2c3e50`, top center):** "Lock the definition BEFORE the experiment. What counts as success cannot change."
- **Divider:** vertical light gray `#bdc3c7` line (width 1) at mid-width, from y=25 to h-5.
- **Left panel header (bold 18px Arial red `#e74c3c`, centered at quarter width, y=38):** "WITHOUT Pre-Registration".
- **Left panel:** 4 small goalposts stepping down-right, at (x=80,y=60), (150,92), (220,124), (290,156); each drawn as a vertical 20px line with 16px crossbars top and bottom. First three in gray `#95a5a6` labeled "Goal v1 (missed → move)", "Goal v2 (missed → move)", "Goal v3 (missed → move)" with gray "→" arrows after each; the fourth in red `#e74c3c` labeled "Goal v4 (finally \"hit\")". Labels 16px Arial, left-aligned at post x+12.
- **Right panel header (bold 18px Arial green `#27ae60`, centered at three-quarter width, y=38):** "WITH Pre-Registration".
- **Right panel:** one single fixed goalpost in green `#27ae60` (width 3) at x=midX+120, y=80, height 60, with 24px crossbars; below it, bold 17px green centered labels: "FIXED GOAL" and "(defined before experiment)".
- **Right panel bottom (16px Arial `#2c3e50`, centered, two lines at h-15 and h-2):** "Result: HIT or MISS" / "No redefinition possible".

## Regeneration instructions

- **Layout:** standard detail-page `.obj-table`: full-width table, one `<tr>`; left `<td>` (40%) holds `.obj-title` + bullet list + closing "Why it's pseudoscience" paragraph, right `<td>` (60%, centered) holds two stacked canvases (`c1` 720×340, `c2` 720×300).
- **Page style:** body system sans-serif (-apple-system, BlinkMacSystemFont, 'Segoe UI'), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.6em `#1a5276`; `.subtitle` `#666` 1.0em; p `#333` 0.95em; ul 0.9em `#333`, li margin 6px 0; `strong` in `#1a5276`; table cell borders `1px solid #e0e0e0`, padding 20px 24px, vertical-align middle; `.obj-title` 1.05em, weight 600, `#1a5276`. No nav bar, no back/home links.
- **Canvas:** declare intrinsic `width`/`height` attributes per chart; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, dark slate `#2c3e50`, gray `#95a5a6`/`#999`/`#555`/`#333`.
- In regenerated HTML, any card links use `.html` extensions.
