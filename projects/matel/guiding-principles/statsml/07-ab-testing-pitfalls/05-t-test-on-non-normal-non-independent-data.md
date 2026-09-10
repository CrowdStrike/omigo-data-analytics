# T-Test on Non-Normal / Non-Independent Data

**Page type:** detail page (two-column obj-table layout: text left ~40%, canvas right ~60%, one row per section)
**HTML title tag:** T-Test on Non-Normal / Non-Independent Data — A/B Testing Pitfalls

**Subtitle:** Statistical Sin — Apply t-test blindly regardless of data distribution or independence.

## Section 1: The Problem

- **T-test assumes:** (1) Data is approximately normal (or N large enough for CLT). (2) Observations are independent. (3) Variances are comparable.
- **Non-normal violations:** Revenue data is extremely heavy-tailed. Time-on-site is right-skewed. Conversion is binary (0/1). The CLT helps with MEANS but NOT with heavy tails — you need enormous N.
- **Non-independence violations:** Same user appears multiple times. Users in social networks influence each other. Pageviews from the same session are correlated. Treating correlated observations as independent INFLATES sample size and underestimates variance.
- **The consequence:** Wrong test on wrong data = wrong p-value. Your "p=0.02" might actually be p=0.15 after accounting for non-independence.

**Correct approach:** For heavy-tailed data: use Mann-Whitney, bootstrap, or trimmed means. For non-independent: cluster-robust standard errors or mixed-effects models. For binary outcomes: proportion test or chi-squared. NEVER default to t-test without checking assumptions.

**The tell:** Ask "what does the distribution of this metric look like?" and "are observations independent?" If nobody checked — the test is invalid.

### Visualization (canvas `c1`, 720×340)

Three distribution shapes side by side: normal (valid for t-test), heavy-tailed, and binary.

- **Panels:** three 200×130 panels, 25px gaps, centered horizontally, top at y=40; each has a bold 18px title above and a bold 18px verdict mark below.
- **Panel 1 — "Normal (t-test assumes)" (green `#27ae60`):** standard Gaussian curve (μ=0, σ=1 over t=(i−50)/15), stroke `#27ae60` width 2, filled `rgba(39,174,96,0.2)`; verdict "✓" in green.
- **Panel 2 — "Heavy-tailed (revenue)" (red `#e74c3c`):** sharp-peaked right-skewed curve — linear rise to a peak at t=0.3 then exponential decay `exp(−0.8·(t−0.3))` over t ∈ [0, 5]; stroke `#e74c3c` width 2, filled `rgba(231,76,60,0.15)`; verdict "✗" in red.
- **Panel 3 — "Binary (conversion)" (red `#e74c3c`):** two vertical bars 40px wide, 60px apart, centered — tall left bar (nearly full height) labeled "0", shorter right bar (~45% height) labeled "1" (labels 16px `#666`); bars filled `rgba(231,76,60,0.4)`, stroked `#e74c3c` width 1.5; verdict "✗" in red.
- **Caption (bottom center, italic 14px `#666`):** "T-test is valid for LEFT only. Applied to all three anyway. Wrong test = wrong answer."

## Section 2: Real Example: Revenue and the One Big Spender

- Money-per-user data at web companies looks nothing like a bell curve: most users spend exactly $0, many spend a little, and a tiny handful spend thousands.
- With data this lopsided, one huge spender landing in one group can single-handedly flip which version looks better, and the standard average-comparison test gets fooled because its assumptions are badly violated.
- Ron Kohavi's book on online experiments, along with engineering blogs at large web companies, recommends capping extreme values (winsorizing) or using resampling methods (the bootstrap) so a single "whale" cannot decide the test.

### Visualization (canvas `c2`, 720×300)

Two grouped bar panels: average revenue per user without vs with one whale.

- **Title (bold 17px `#2a2a2a`, top center at x=360, y=28):** "One Big Spender Flips the "Winner"".
- **Scale:** baseline y=210, max value $3.50 over 140px plot height; bars 80px wide with dollar value labels (bold 16px, bar color) above and group labels (14px `#666`) below.
- **Panel 1 (left, "Without the $5,000 user" — bold 15px `#333` caption below):** Control $2.20 at x=90 (stroke `#1a5276`, fill `rgba(26,82,118,0.35)`); Treatment $2.05 at x=200 (stroke `#e67e22`, fill `rgba(230,126,34,0.25)`). Control ahead.
- **Panel 2 (right, "Same test, one whale lands in treatment"):** Control $2.20 at x=440 (blue, same style); Treatment $3.10 at x=550 (stroke `#e74c3c`, fill `rgba(231,76,60,0.25)`). Bold 15px red "whale" marker above the treatment bar.
- **Baselines:** thin gray (`#999`) lines under each panel (x=70–300 and x=420–650 at y=210); light gray (`#ddd`) vertical divider at x=360 between panels.
- **Takeaway (bottom center, 15px `#333`):** "Averages of skewed money data can hinge on one user — cap extremes or use sturdier comparisons".

## Regeneration instructions

- **Layout:** standard detail-page `.obj-table`: full-width table, one `<tr>` per section; left `<td>` (40%) holds `.obj-title` + bullets/paragraphs, right `<td>` (60%, centered) holds the canvas.
- **Page style:** body system sans-serif, white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.6em `#1a5276`; subtitle `#666`; table cell borders `1px solid #e0e0e0`, padding 20px 24px; `.obj-title` 1.05em, weight 600, `#1a5276`; `strong` in `#1a5276`. No nav bar, no back/home links.
- **Canvas:** declare intrinsic `width`/`height` attributes as given per chart; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, gray text `#666`/`#333`.
- In regenerated HTML, any card links use `.html` extensions (this detail page has no links).
