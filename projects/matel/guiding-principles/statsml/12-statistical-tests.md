# Statistical Tests & Metrics Reference

**Page type:** grid page (nav-grid card navigation, auto-fit columns min 300px, cards with topic tags)
**HTML title tag:** Statistical Tests & Metrics Reference

**Subtitle:** When each test is valid, when it breaks, and what to use instead — with real data examples

## Cards

Each card links to a detail page under `statistical-tests/`. The card shows an uppercase category label (`.card-num`), a numbered title, a one-sentence description, and a row of topic tags.

| # | Category | Title | Link | Description | Topics |
|---|----------|-------|------|-------------|--------|
| 1 | HYPOTHESIS | t-test (Student's and Welch's) | [statistical-tests/01-ttest.md](statistical-tests/01-ttest.md) | Whether two groups have significantly different means. Breaks with skew and heavy tails. | means, two-groups, normality |
| 2 | HYPOTHESIS | z-test / Proportion test | [statistical-tests/02-ztest.md](statistical-tests/02-ztest.md) | Whether an observed proportion differs from expected. Fails when np < 10 or clustering present. | proportions, binomial, sample-size |
| 3 | HYPOTHESIS | Chi-squared test | [statistical-tests/03-chi-squared.md](statistical-tests/03-chi-squared.md) | Whether observed frequencies differ from expected. Inflated with sparse cells (expected < 5). | categorical, contingency, independence |
| 4 | HYPOTHESIS | KS test | [statistical-tests/04-ks-test.md](statistical-tests/04-ks-test.md) | Whether two samples come from the same distribution. Power collapses with ties or discrete data. | distributions, CDF, continuous |
| 5 | INFO-THEORY | Cross-entropy | [statistical-tests/05-cross-entropy.md](statistical-tests/05-cross-entropy.md) | Loss function measuring predicted vs true distribution. Catastrophic penalty for overconfident wrong predictions. | loss-function, classification, calibration |
| 6 | INFO-THEORY | Gini impurity | [statistical-tests/06-gini-impurity.md](statistical-tests/06-gini-impurity.md) | Decision tree split criterion. Favors balanced partitions, can miss rare but important classes. | decision-trees, splits, imbalance |
| 7 | INFO-THEORY | Information gain | [statistical-tests/07-information-gain.md](statistical-tests/07-information-gain.md) | Feature selection metric. Biased toward high-cardinality features like IDs. | feature-selection, cardinality, entropy |
| 8 | INFO-THEORY | Entropy | [statistical-tests/08-entropy.md](statistical-tests/08-entropy.md) | Uncertainty measure of a distribution. Same value can mean very different structures. | uncertainty, information-theory, distribution-shape |
| 9 | HYPOTHESIS | ANOVA (F-test) | [statistical-tests/09-anova.md](statistical-tests/09-anova.md) | Whether means differ across 3+ groups. Unreliable with unequal variances or unbalanced designs. | multiple-groups, variance, homoscedasticity |

## Regeneration instructions

To rebuild the HTML from this spec:

- **Template:** nav-grid style (see `docs/statsml/ui-templates/02-nav-grid`). Single page: h1, `.subtitle` paragraph, then one `.nav-grid` of `.nav-card` anchors. No callouts.
- **Layout:** `.nav-grid` is CSS grid, `repeat(auto-fit, minmax(300px, 1fr))`, 16px gap, margin-top 15px.
- **Links:** the table above links to `.md` versions for markdown navigation; in the regenerated HTML each card's `href` is the same path with an `.html` extension instead.
- **Card structure:** `<a href="..." class="nav-card">` containing `<div class="card-num">CATEGORY</div>`, `<h3>N. Title</h3>` (unpadded index matching the file index), `<p>description</p>`, then `<div class="topics">` with one `<span class="topic-tag">` per topic.
- **Card style:** background `#ffffff`, border `1px solid #d8d8d8`, radius 10px, padding 20px, shadow `0 2px 4px rgba(0,0,0,0.05)`; hover: border `#2980b9`, `translateY(-2px)`. `.card-num` color `#2980b9`, 0.75em bold (same color for every category on this page — no per-category coloring script). h3 `#1a3a4a` 1em; description `#555` 0.85em.
- **Topic tag style:** `.topic-tag` background `#f0f0f0`, border `1px solid #ccc`, radius 4px, padding 2px 6px, 0.7em, color `#666`; `.topics` is flex, wrap, 4px gap, margin-top 8px.
- **Page style:** body system sans-serif, background `#f5f5f0`, text `#2a2a2a`, padding 40px, line-height 1.6; h1 1.8em `#2980b9`; subtitle `#666` 1.05em. A `.section-header` rule exists (`#1a5276`, 1.2em, bottom border `2px solid #d0d0d0`) but no section headers are used on the page. No nav bar, no back/home links.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`. No canvases on this page; detail pages use `window.devicePixelRatio` scaling.
