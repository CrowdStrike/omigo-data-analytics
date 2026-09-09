# ML Algorithm Assumptions Reference

**Page type:** grid page (nav-grid card navigation, auto-fit columns min 300px, cards with topic tags and script-colored category labels)
**HTML title tag:** ML Algorithm Assumptions Reference

**Subtitle:** What every ML algorithm requires from your data — and what breaks when violated

## Cards

Each card links to a detail page under `ml-assumptions/`. The card shows an uppercase category label (`.card-num`, colored per category by a small script), a numbered title, a one-sentence description, and a row of topic tags.

| # | Category | Title | Link | Description | Topics |
|---|----------|-------|------|-------------|--------|
| 1 | LINEAR | Logistic Regression | [15-ml-assumptions/01-logistic-regression.md](15-ml-assumptions/01-logistic-regression.md) | Requires linear log-odds relationships; non-linear signal becomes invisible | linearity, multicollinearity, sample-size |
| 2 | LINEAR | Linear Discriminant Analysis (LDA) | [15-ml-assumptions/02-linear-discriminant-analysis-lda.md](15-ml-assumptions/02-linear-discriminant-analysis-lda.md) | Assumes multivariate normality and equal covariance per class | normality, covariance, parametric |
| 3 | PROBABILISTIC | Naive Bayes | [15-ml-assumptions/03-naive-bayes.md](15-ml-assumptions/03-naive-bayes.md) | Requires feature independence; correlated features double-count evidence | independence, density, zero-frequency |
| 4 | TREE | Decision Trees | [15-ml-assumptions/04-decision-trees.md](15-ml-assumptions/04-decision-trees.md) | Axis-aligned splits with no statistical validation; splits on noise | greedy, overfitting, axis-aligned |
| 5 | TREE | Random Forest | [15-ml-assumptions/05-random-forest.md](15-ml-assumptions/05-random-forest.md) | Ensemble masks overfitting but cannot extrapolate beyond training range | extrapolation, black-box, bootstrap |
| 6 | ENSEMBLE | Gradient Boosting Machines (GBM) | [15-ml-assumptions/06-gradient-boosting-machines-gbm.md](15-ml-assumptions/06-gradient-boosting-machines-gbm.md) | Sequential regression trees correcting residuals; amplifies noise, memorizes hard examples | residuals, overfitting, early-stopping |
| 7 | DISTANCE | Support Vector Machines | [15-ml-assumptions/07-support-vector-machines.md](15-ml-assumptions/07-support-vector-machines.md) | Feature scaling required; unscaled features dominate margin entirely | scaling, kernel, outliers |
| 8 | DISTANCE | K-Nearest Neighbors | [15-ml-assumptions/08-k-nearest-neighbors.md](15-ml-assumptions/08-k-nearest-neighbors.md) | Curse of dimensionality; nearest becomes meaningless in high dimensions | scaling, curse-dimensionality, distance |
| 9 | DEEP-LEARNING | Neural Networks | [15-ml-assumptions/09-neural-networks.md](15-ml-assumptions/09-neural-networks.md) | Requires large sample size; small data leads to pure memorization | sample-size, scaling, architecture |
| 10 | REDUCTION | PCA (Principal Component Analysis) | [15-ml-assumptions/10-pca-principal-component-analysis.md](15-ml-assumptions/10-pca-principal-component-analysis.md) | Finds linear max-variance directions; non-linear structure destroyed | linearity, variance, interpretability |
| 11 | CLUSTERING | K-Means Clustering | [15-ml-assumptions/11-k-means-clustering.md](15-ml-assumptions/11-k-means-clustering.md) | Assumes spherical, equal-size clusters; fails on elongated shapes | spherical, equal-variance, initialization |
| 12 | LINEAR | Linear Regression (OLS) | [15-ml-assumptions/12-linear-regression-ols.md](15-ml-assumptions/12-linear-regression-ols.md) | Most assumption-heavy algorithm; linearity, normality, homoscedasticity required | linearity, residuals, homoscedasticity |
| 13 | PROBABILISTIC | EM / Gaussian Mixture Models | [15-ml-assumptions/13-em-gaussian-mixture-models.md](15-ml-assumptions/13-em-gaussian-mixture-models.md) | Assumes Gaussian components; wrong K or initialization causes fake clusters | gaussian, initialization, model-selection |

## Regeneration instructions

To rebuild the HTML from this spec:

- **Template:** nav-grid style (see `docs/statsml/ui-templates/02-nav-grid`). Single page: h1, `.subtitle` paragraph, then one `.nav-grid` of `.nav-card` anchors. No callouts.
- **Layout:** `.nav-grid` is CSS grid, `repeat(auto-fit, minmax(300px, 1fr))`, 16px gap, margin-top 15px.
- **Links:** the table above links to `.md` versions for markdown navigation; in the regenerated HTML each card's `href` is the same path with an `.html` extension instead.
- **Card structure:** `<a class="nav-card" href="...">` containing `<div class="card-num">CATEGORY</div>`, `<h3>N. Title</h3>` (unpadded index matching the file index), `<p>description</p>`, then `<div class="topics">` with one `<span class="topic-tag">` per topic.
- **Category label colors** applied by an inline script that maps each `.card-num` text to a color: LINEAR `#1a5276`; PROBABILISTIC `#8e44ad`; TREE `#27ae60`; ENSEMBLE `#e67e22`; DISTANCE `#e74c3c`; DEEP-LEARNING `#2980b9`; REDUCTION `#16a085`; CLUSTERING `#795548`. CSS default for `.card-num` is `#2980b9` 0.75em bold.
- **Card style:** background `#ffffff`, border `1px solid #d8d8d8`, radius 10px, padding 20px, shadow `0 2px 4px rgba(0,0,0,0.05)`; hover: border `#2980b9`, `translateY(-2px)`. h3 `#1a3a4a` 1em; description `#555` 0.85em.
- **Topic tag style:** `.topic-tag` background `#f0f0f0`, border `1px solid #ccc`, radius 4px, padding 2px 6px, 0.7em, color `#666`; `.topics` is flex, wrap, 4px gap, margin-top 8px.
- **Page style:** body system sans-serif, background `#f5f5f0`, text `#2a2a2a`, padding 40px, line-height 1.6; h1 1.8em `#2980b9`; subtitle `#666` 1.05em. No nav bar, no back/home links.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`. No canvases on this page; detail pages use `window.devicePixelRatio` scaling.
