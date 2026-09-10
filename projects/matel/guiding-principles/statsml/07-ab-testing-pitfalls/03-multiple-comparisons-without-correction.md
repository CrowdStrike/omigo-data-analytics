# Multiple Comparisons Without Correction

**Page type:** detail page (two-column obj-table layout: text left ~40%, canvas right ~60%, one row per section)
**HTML title tag:** Multiple Comparisons Without Correction — A/B Testing Pitfalls

**Subtitle:** Statistical Sin — Test 20 metrics, report the 1 that's significant. That's the expected false positive.

## Section 1: The Problem

- Test measures 20 metrics: clicks, time, scroll, conversions, revenue, sessions, bounce... At α=0.05: EXPECT 1 false positive per 20 tests.
- You find one (scroll depth, p=0.03). "Test improved scroll depth!" No — you found the GUARANTEED false positive.
- **Segment variant:** 5 segments x 4 metrics = 20 comparisons. One hits. "Works for women 25-34 on mobile!" Pure noise dressed as insight.
- **The narrative trap:** Once you FIND a significant subgroup, a narrative is ALWAYS available. "Young men are more tech-savvy!" The story makes the noise feel inevitable.

**Correct approach:** Bonferroni correction (α/N tests). Or pre-register ONE primary metric. Everything else is exploratory (hypothesis-generating, not confirming).

**The tell:** Ask "how many metrics/segments tested?" If only ONE significant result is reported from 20+ tests — that's the expected false positive, not a discovery.

### Visualization (canvas `c1`, 720×340)

Grid of 20 test-result boxes with exactly one "significant" green box, plus a magnifying glass zeroing in on it.

- **Grid:** 5 columns × 4 rows of boxes, each 90×40 px, 8px gap, centered horizontally, starting at y=20.
- **Boxes:** 19 gray boxes — fill `#e8e8e8`, text "p > 0.05" in 16px `#888`; one green box at index 7 (row 2, column 3) — fill `#27ae60`, text "p = 0.03!" in bold 18px white. All boxes stroked `#ccc` width 1.
- **Magnifying glass:** red (`#e74c3c`, width 2.5) circle of radius 14 with a handle stroke, positioned just right of the green box.
- **Top label (bold 16px `#1a5276`, center, y=12):** "20 Metrics Tested".
- **Captions (bottom center, italic 14px `#666`, two lines):** "20 tests x 5% FPR = 1.0 expected false positives." / "You found exactly what probability guaranteed."

## Section 2: Real Example: Green Jelly Beans

- A famous xkcd comic imagines scientists testing whether jelly beans cause acne: 20 colors get tested, 19 show nothing, and the one lucky color (green) makes the newspaper headline.
- Real product dashboards recreate this joke daily — look at 20 metrics after a test and about one of them will look like a winner purely by luck, because each check carries its own 1-in-20 fluke rate.
- Microsoft's experimentation platform team publishes guidance for exactly this reason: choose one primary metric before the test starts, and treat the other 19 as leads to investigate later, not results to announce.

### Visualization (canvas `c2`, 720×300)

Rising curve: chance of at least one false "winner" vs number of things checked.

- **Title (bold 17px `#2a2a2a`, top center at x=360, y=28):** "Check Enough Colors and One Will "Win" by Luck".
- **Plot area:** x=90, y=55, width 560, height 165; gray (`#666`) L-shaped axes.
- **Y labels (14px `#666`, right-aligned):** "0%", "35%", "70%" at bottom, middle, top.
- **X labels:** tick labels at n=1, 5, 10, 15, 20 spaced linearly over the plot width; axis title "number of jelly-bean colors (metrics) checked" centered below.
- **Curve:** `1 − 0.95^n` for n=1…20, orange `#e67e22` stroke width 2.5, y scaled to 0–70% range.
- **Endpoint markers:** blue `#1a5276` dot (radius 5) at n=1 labeled "1 metric: 5% fluke risk" (bold 14px, left-aligned); green `#27ae60` dot (radius 6) at n=20 labeled "20 colors: 64% — hello, green jelly bean" (bold 15px, right-aligned).
- **Takeaway (bottom center, 15px `#333`):** "By the 20th check, a lucky "significant" result is more likely than not — the headline writes itself".

## Regeneration instructions

- **Layout:** standard detail-page `.obj-table`: full-width table, one `<tr>` per section; left `<td>` (40%) holds `.obj-title` + bullets/paragraphs, right `<td>` (60%, centered) holds the canvas.
- **Page style:** body system sans-serif, white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.6em `#1a5276`; subtitle `#666`; table cell borders `1px solid #e0e0e0`, padding 20px 24px; `.obj-title` 1.05em, weight 600, `#1a5276`; `strong` in `#1a5276`. No nav bar, no back/home links.
- **Canvas:** declare intrinsic `width`/`height` attributes as given per chart; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, gray text `#666`/`#333`.
- In regenerated HTML, any card links use `.html` extensions (this detail page has no links).
