# Clutch Players & the Mixture Problem

**Page type:** detail page (two-column obj-table layout: text left 45%, canvas right 55%, one row per section)
**HTML title tag:** Clutch Players & the Mixture Problem

**Subtitle:** A handful of elite tennis players genuinely perform differently under pressure. You'll never find them by averaging over the entire tour — because the population isn't one population.

## Callout (philosophy box)

**Why this is fascinating:** Magnus & Klaassen (1999) found that at the population level, point outcomes in tennis appear i.i.d. (independent and identically distributed). No momentum, no clutch. But this masks a fat-tailed individual effect: rare players (think Nadal on clay at Roland Garros, Federer in Wimbledon finals) show statistically significant state-dependence. The pooled test answers "does the average player show clutch?" when the real question is "does ANY player, and which ones?"

## Section: The Pooled Null

**Obj-title:** The Average Player Test

Take 500 ATP players and model P(win point) as a function of recent history. Run a regression with "won previous point" as a predictor.

**Result:** The coefficient on "won previous point" is near zero (β ≈ -0.003, p = 0.52). Conclusion: no momentum effect at the population level.

The pooled analysis suggests point outcomes are independent — past success doesn't predict future success.

### Visualization (canvas `pooledCanvas`, 720×380)

Single bell curve: population distribution of the momentum coefficient centered at zero.

- **Title (bold 16px `#1a5276`, centered):** "Population Distribution of Momentum Effect"; subtitle line (13px `#666`): "Centered at zero — no clutch effect detected".
- **Curve:** Gaussian-shaped curve centered at x=360 (β=0), baseline y=280, drawn from x=80 to 640 with y = 280 − 1500·exp(−t²/0.5) where t=(x−360)/100; stroke `#1a5276` width 3, area filled `rgba(26,82,118,0.15)`.
- **Axes:** horizontal baseline `#333` width 2 from x=60 to 660; vertical dashed (`#999`, dash 5/5, width 1) center line at x=360 from baseline up to y=50.
- **Labels (14px `#333`):** x-axis "Momentum coefficient β" centered below; tick labels "0" at center, "-0.04" at x=160, "+0.04" at x=560; rotated y label "Frequency" at left.

## Section: The Mixture Underneath

**Obj-title:** Heterogeneity Revealed

The population is NOT homogeneous. Most players (~95%) truly show no momentum — their coefficient IS near zero.

But ~5% have a genuine positive momentum effect (β = 0.03 to 0.08). When you pool them, the 95% zeros overwhelm the 5% real effects.

Math box:

Population = 0.95 × N(0, σ₁²) + 0.05 × N(0.055, σ₂²)

**The mixture LOOKS like a single distribution centered at zero.** The elite clutch players are statistically drowned out.

### Visualization (canvas `mixtureCanvas`, 720×380)

Two overlaid distributions: a tall majority component at zero and a small shifted elite component.

- **Title (bold 16px `#1a5276`, centered):** "The Mixture Decomposed"; subtitle line (13px `#666`): "Two populations — the 95% drowns out the 5%".
- **Majority curve:** centered at x=360, baseline y=280, y = 280 − 1800·exp(−t²/0.4) with t=(x−360)/85, drawn x=80–640; stroke `#1a5276` width 3, fill `rgba(26,82,118,0.15)`.
- **Elite curve:** centered at x=510, much flatter, y = 280 − 200·exp(−t²/0.8) with t=(x−510)/80, drawn x=350–640; stroke `#e74c3c` width 3, fill `rgba(231,76,60,0.15)`.
- **Axes:** horizontal baseline `#333` width 2 from x=60 to 660.
- **Labels (14px `#333`):** x-axis "Momentum coefficient β"; ticks "0" at x=360, "-0.04" at x=160, "+0.08" at x=600; rotated y label "Frequency".
- **Legend (top left, 13px):** blue square swatch + "Majority: no effect (~95% of players)"; red square swatch + "Rare elite: real clutch effect (~5%)".

## Section: Why This Matters Beyond Tennis

**Obj-title:** The Universal Statistical Sin

**Medicine:** "Drug X shows no average effect" → maybe it works brilliantly for 8% of patients with a specific genotype, but is neutral or harmful for the rest.

**Education:** "Teaching method A is no better than B on average" → maybe it's transformative for visual learners (15% of students) but worse for others.

**Machine Learning:** "Feature Z has no predictive power" → maybe it's highly predictive for a rare subpopulation that drives 20% of revenue.

- Pooling heterogeneous groups creates false null conclusions
- The right question: "Does it work for ANYONE?" not "Does it work on average?"
- Test for heterogeneity first, THEN decide if pooling is valid

### Visualization (canvas `stratifiedCanvas`, 720×380)

Side-by-side comparison panels: pooled test vs stratified test.

- **Left panel (background `#f8f8f8`, at x=40, y=60, 280×240):** heading bold 15px `#1a5276` "Pooled Test"; 13px `#333` two lines "Test all 500 players" / "as one group"; solid red result box (`#e74c3c`, 200×60) with white bold 14px "No Effect" and 12px "β = -0.003, p = 0.52".
- **Arrow between panels:** `#1a5276` width 3 with arrowhead, labeled above in bold 12px `#1a5276`: "What you miss".
- **Right panel (background `#f0f8f0`, at x=400, y=60, 280×240):** heading bold 15px `#27ae60` "Stratified Test"; 13px `#333` two lines "Test subgroups" / "separately"; two result boxes: gray box (`#bdc3c7`, 110×50) with bold 13px `#333` "Null for 95%" and 11px "β ≈ 0.001"; green box (`#27ae60`, 110×50) with white bold 13px "Strong for 5%" and 11px "β = 0.03-0.08".
- **Bottom note (italic 12px `#666`, centered):** 'The pooled test answers "average effect" when you need "any effect"'.

## Closing callout (philosophy box)

**The statistical lesson:** When you reject an effect based on a pooled sample, you're implicitly assuming the population is homogeneous. If it isn't — if there's a mixture — your test is answering the wrong question. The right approach: test for heterogeneity first (random effects, mixture models, subgroup analysis), THEN decide if a pooled null is meaningful. A null result from a pooled test is only as valid as the homogeneity assumption underneath it.

## Regeneration instructions

- **Layout:** detail page. h1, `.subtitle`, opening `.philosophy` callout, three `h2` sections (unnumbered) each holding a `.obj-table` (one `<tr>`: left `<td>` 45% with `.obj-title` + paragraphs/bullets/`.math-box`, right `<td>` 55% centered canvas), closing `.philosophy` callout.
- **Page CSS:** body system sans-serif (-apple-system stack), white background, text `#2a2a2a`, padding 40px 20px, line-height 1.6; h1 1.8em `#1a5276`; h2 1.4em `#1a5276` with 2px `#2980b9` bottom border; subtitle `#666` 1.05em; p 0.95em `#333`; `.obj-title` 1.05em weight 600 `#1a5276`; `strong` `#1a5276`; obj-table cells `1px solid #e0e0e0`, padding 20px 24px. No nav bar, no back/home links.
- **Component styles:** `.philosophy` — background `#f0f4f8`, left border `4px solid #2980b9`, padding 12px 16px, 0.9em. `.math-box` — background `#f8fafb`, border `1px solid #e0e0e0`, radius 6px, padding 16px 20px, 0.9em; `code` on `#eef2f7`, padding 2px 6px, radius 3px.
- **Canvases:** three canvases with no width/height attributes in the markup; sizes are set in JS via `setupCanvas(canvas, 720, 380)`, which sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. All three are 720×380.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, gray `#bdc3c7`, text grays `#666`/`#333`; area fills `rgba(26,82,118,0.15)` and `rgba(231,76,60,0.15)`.
- In regenerated HTML, any card links use `.html` extensions (this page has no outbound links).
