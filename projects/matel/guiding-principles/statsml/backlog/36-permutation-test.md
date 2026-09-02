# Permutation Test

**Page type:** detail page (backlog-style 2-col layout: h2 sections, text left 45%, canvas right 55%, one layout table per section)
**HTML title tag:** Permutation Test

**Subtitle:** Non-parametric significance testing via label shuffling — no distributional assumptions required

## How It Works

**Problem:** Parametric tests (t-test, ANOVA) assume normality, equal variance, or both. When those assumptions fail — small n, skewed data, heavy tails — p-values become unreliable.

**Solution:** Permutation tests estimate the null distribution empirically. If group labels don't matter, shuffling them shouldn't change the test statistic. Repeat thousands of times to see how extreme the real observation is.

- Compute observed test statistic (mean difference, t-stat, etc.)
- Shuffle group labels randomly (break the group-value link)
- Recompute test statistic on shuffled data
- Repeat 10,000 times → empirical null distribution
- p-value = fraction of permuted statistics ≥ observed

**Example (italic):** Example: Two groups, n=12 each. Observed mean difference = 3.2. After 10,000 permutations, only 30 produced a difference ≥ 3.2. p = 30/10000 = 0.003. Significant — without assuming anything about the shape of the data.

**Key-point callout (red accent):** **Key insight:** The permutation test answers exactly one question: "If group membership didn't matter, how often would I see a statistic this extreme by chance?" No normality, no equal variance, no minimum sample size formula.

### Visualization (canvas `c1`, 720×340)

Histogram of the empirical null distribution with the observed statistic marked.

- **Title (bold 13px system-ui, `#1a5276`, top center):** "Empirical Null Distribution (10,000 permutations)".
- Margins: top 40, right 40, bottom 60, left 70.
- **Bell-shaped bin counts (20 bins):** `[2, 5, 12, 28, 55, 90, 130, 160, 175, 180, 175, 160, 130, 90, 55, 28, 12, 5, 2, 1]`, scaled to max 185. Bars from bin index 16 onward (the extreme tail) filled `rgba(231,76,60,0.5)`; all others `rgba(26,82,118,0.35)`.
- **Observed line:** vertical red `#e74c3c` width 2.5 at the left edge of bin 16, from just above the plot top to the x-axis. Labels right of the line: bold 12px red "Observed = 3.2", 12px red "p = 0.003".
- **Tail label (italic 11px red, centered in the tail region near the axis):** "30 of 10,000 ≥ observed".
- **X-axis:** gray `#ccc` baseline; centered 12px `#444` labels below: "Test Statistic (mean difference)" and "Null: labels shuffled randomly".
- **Y-axis label (rotated 11px `#444`):** "Frequency".

## When to Use

Permutation tests are not always the right tool. They excel in specific situations and have real limitations.

- **Small n:** When sample size is too small for CLT to rescue parametric assumptions (n < 20 per group)
- **Unknown distribution:** Bimodal, heavy-tailed, or truncated data where normality clearly fails
- **Validation:** Run alongside a parametric test — if they disagree, investigate why
- **Non-standard statistics:** Median difference, ratio of variances, any custom metric — permutation works on anything computable

**When NOT to use:**

- Large n where parametric tests are robust anyway (CLT covers you)
- When you need confidence intervals (permutation gives p-values easily, CIs require more machinery)
- Time-series or clustered data where observations aren't exchangeable under H0

**Key-point callout (red accent):** **Critical assumption:** Exchangeability — under the null, swapping labels must be valid. This fails when groups have different variance under H0, or when data has temporal/spatial structure.

### Visualization (canvas `c2`, 720×340)

Decision tree diagram: parametric vs permutation.

- **Title (bold 13px system-ui, `#1a5276`, top center):** "Decision: Parametric vs Permutation".
- **Root node** (centered at w/2, y=55): 180×30 box, fill `rgba(26,82,118,0.1)`, stroke `#1a5276` 1.5px, bold 11px blue text "Data passes normality?".
- **YES branch (left):** green `#27ae60` edge to a node at 25% width, y=130 — 170×30 box, fill `rgba(39,174,96,0.1)`, green stroke, 11px green text "Use parametric (t-test)"; bold 10px green edge label "YES".
- **NO branch (right):** red `#e74c3c` edge to a node at 75% width, y=130 — 170×30 box, fill `rgba(231,76,60,0.1)`, red stroke, 11px red text "n > 30 per group?"; bold 10px red edge label "NO".
- **Right-YES leaf** (60% width, y=205): 190×30 box, fill `rgba(39,174,96,0.1)`, green stroke, 11px green text "CLT covers — parametric OK"; edge label "YES".
- **Right-NO leaf** (88% width, y=205): 150×30 box, fill `rgba(231,76,60,0.15)`, red stroke width 2, bold 11px red text "→ Permutation test"; edge label "NO".
- **Bottom note (bold 12px `#1a5276`, centered, y = h−50):** "Best practice: run both. If they agree → confidence. If they disagree → investigate."
- **Warning (11px orange `#e67e22`, centered, y = h−30):** "⚠ Permutation requires exchangeability: no time-series, no clustering, no paired structure (without adaptation)".

## Practical Considerations

**How many permutations?**

- 1,000 → p-value resolution to 0.001 (fine for screening)
- 10,000 → resolution to 0.0001 (standard for publication)
- 100,000 → needed only when p is expected to be very small
- Rule: use at least 1/α permutations (α=0.05 → minimum 20, but 10,000 is cheap)

**Computational cost:** For simple statistics (mean difference), 10,000 permutations on n=100 takes milliseconds. For complex models (random forest importance), consider approximate methods or fewer permutations with confidence bounds on the p-value.

**Exact vs approximate:** For very small n (n₁ + n₂ ≤ 20), enumerate ALL possible permutations — gives exact p-value. For larger n, random sampling from the permutation space is sufficient.

**Key-point callout (red accent):** **Pipeline integration:** After shape detection fails normality and before final verdict — run permutation test as fallback. If parametric and permutation agree → high confidence. If they disagree → flag for review.

### Visualization (canvas `c3`, 720×340)

Convergence line chart: estimated p-value vs number of permutations on a log x scale.

- **Title (bold 13px system-ui, `#1a5276`, top center):** "p-value Estimate Stabilizes with More Permutations".
- Margins: top 40, right 40, bottom 55, left 70. X: log10 scale from 1 to 10,000 with labels at 10, 100, 1,000, 10,000. Y: p from 0 to 0.08, gridlines (`#f0f0f0`) and labels every 0.02.
- **True p-value line:** horizontal dashed (6/4) green `#27ae60` width 2 at p = 0.032; 11px green label near the right: "True p = 0.032".
- **Convergence path** (red `#e74c3c` line width 2.5 connecting points): `(10, 0.0), (20, 0.05), (50, 0.06), (100, 0.04), (200, 0.045), (500, 0.038), (1000, 0.034), (2000, 0.033), (5000, 0.031), (10000, 0.032)`.
- **Point dots (4px):** first 3 points red `#e74c3c`, next 3 orange `#e67e22`, remaining 4 green `#27ae60`.
- **Annotations (10px):** red "Unstable" near the first point; green "Converged" near (5000, 0.031).
- **Axis labels (12px `#444`):** x "Number of Permutations (log scale)"; y (rotated) "Estimated p-value".
- **Recommendation box** (top-left of plot, 200×40, fill `rgba(26,82,118,0.08)`, `#1a5276` 1px border, 10px blue text, two lines): "Minimum: 1,000 (screening)" / "Standard: 10,000 (publication)".

## Regeneration instructions

- **Layout:** backlog detail page. h1 with bottom border `2px solid #2980b9`, `.subtitle` paragraph (no status badge, no intro box). One `.card-section` per section, each with an h2 (1.3rem `#1a5276`, bottom border `2px solid #2980b9`) and a `table.layout` (full width) with one `<tr>`: left `td.text-col` (45%) text, right `td.viz-col` (55%) canvas.
- **Callout styles:** `.key-point` — background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem. `.example` — italic, `#555`, 0.9rem.
- **Page style:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; ul 0.92rem with 20px left margin. Canvases styled `width: 100%`, border `1px solid #e0e0e0`, radius 4px. No nav bar, no back/home links.
- **Canvas:** each canvas declares intrinsic `width="720" height="340"`; shared `setup(id)` helper reads the width/height attributes, sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`), and calls `ctx.scale` so drawing stays in logical coordinates. Chart fonts use the `system-ui, sans-serif` stack.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, bar fill `rgba(26,82,118,0.35)`, red tail fill `rgba(231,76,60,0.5)`, gray text `#444`, gridlines `#f0f0f0`.
- In regenerated HTML, any card links use `.html` extensions (this page has no outbound links).
