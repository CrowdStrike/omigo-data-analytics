# Bayesian Methods

**Page type:** grid page (tutorials category grid: single flat 4-column nav-grid of cards with topic tags)
**HTML title tag:** Bayesian Methods

**Subtitle:** Treat what you believe as a distribution, update it with data, and read probabilities straight off the result — from simple priors to full graphical models.

## Cards

Each card links to a topic page under `bayesian-methods/`. The card shows a colored uppercase subcategory label (`.card-num`), a numbered title, a one-line description, and 2-4 topic tag pills. All cards sit in one flat `.nav-grid`; the colored labels carry the grouping.

| # | Category | Title | Link | Description | Topic tags |
|---|----------|-------|------|-------------|------------|
| 1 | BELIEFS & INFERENCE | Priors & Conjugacy | [10-bayesian-methods/01-priors-and-conjugacy.md](10-bayesian-methods/01-priors-and-conjugacy.md) | What you believed before the data arrived, written as a distribution — and how a conjugate prior turns updating into simply adding counts. | prior belief, pseudo-counts, conjugate pairs |
| 2 | BELIEFS & INFERENCE | Credible vs Confidence Intervals | [10-bayesian-methods/02-credible-vs-confidence-intervals.md](10-bayesian-methods/02-credible-vs-confidence-intervals.md) | Two intervals with nearly the same numbers make completely different promises — one rates the recipe over many repeats, the other states a probability about the parameter. | two schools, same numbers, interpretation |
| 3 | BELIEFS & INFERENCE | Hierarchical Bayes | [10-bayesian-methods/03-hierarchical-bayes.md](10-bayesian-methods/03-hierarchical-bayes.md) | When one group has little data, it borrows strength from its siblings — the small store's estimate leans toward the chain average, by exactly as much as its thin data deserve. | partial pooling, borrowing strength, shrinkage |
| 4 | BAYESIAN METHODS IN ACTION | Bayesian A/B Testing | [10-bayesian-methods/04-bayesian-a-b-testing.md](10-bayesian-methods/04-bayesian-a-b-testing.md) | Instead of asking "is this data surprising if nothing changed?", compute the probability the variant is actually better — a number a decision can be built on. | posterior, A/B test, decision-ready |
| 5 | BAYESIAN METHODS IN ACTION | Bayesian Optimization | [10-bayesian-methods/05-bayesian-optimization.md](10-bayesian-methods/05-bayesian-optimization.md) | When every trial is slow and expensive, fit a cheap belief model to the results so far and let its uncertainty choose the next experiment. | surrogate model, expensive trials, smart search |
| 6 | BAYESIAN METHODS IN ACTION | Bayes Nets & Graphical Models | [10-bayesian-methods/06-bayes-nets-and-graphical-models.md](10-bayesian-methods/06-bayes-nets-and-graphical-models.md) | Draw variables as nodes and "depends on" as arrows — multiply along the arrows to answer any probability question, and copy the picture over time to get HMMs and CRFs. | nodes & arrows, independence, HMMs |
| 7 | BAYESIAN METHODS IN ACTION | MCMC Simulation | [10-bayesian-methods/07-mcmc-simulation.md](10-bayesian-methods/07-mcmc-simulation.md) | When the posterior has no formula you can integrate, simulate a walk whose visits pile up into the distribution — then every mean, interval, and tail risk is just counting draws. | posterior draws, counting answers, Monte Carlo error |

## Regeneration instructions

To rebuild the HTML from this spec:

- **Template:** tutorials category grid. Single page: h1, `.subtitle` paragraph, then one flat `.nav-grid` of `.nav-card` anchors (no h2 section headings).
- **Layout:** `.nav-grid` is CSS grid, `repeat(4, 1fr)`, 16px gap, 15px top margin; responsive: 3 columns below 1400px, 2 below 1100px, 1 below 600px.
- **Links:** the tables above link to the `.md` versions for markdown navigation; in the regenerated HTML, each card's `href` is the same path with an `.html` extension instead.
- **Card structure:** `<a class="nav-card" href="...">` containing `<div class="card-num">SUBCATEGORY LABEL</div>`, `<h3>N. Topic Title</h3>` (unpadded index number matching the 2-digit zero-padded file index), `<p>description</p>`, then `<div class="topics">` of `<span class="topic-tag">` pills.
- **Category label colors:** applied by a small script mapping `.card-num` text to color — "BELIEFS & INFERENCE" `#2980b9`, "BAYESIAN METHODS IN ACTION" `#27ae60`; the CSS default for `.card-num` is `#2980b9`, 0.75em bold, 4px bottom margin.
- **Card style:** background `#ffffff`, border `1px solid #d8d8d8`, radius 10px, padding 20px, shadow `0 2px 4px rgba(0,0,0,0.05)`, transition on border-color/transform; hover: border `#2980b9`, `translateY(-2px)`. h3 `#1a3a4a` 1em with 6px bottom margin; description `#555` 0.85em. Topic tags: background `#f0f0f0`, border `1px solid #ccc`, radius 4px, padding 2px 6px, 0.7em `#666`, in a flex-wrap row with 4px gap and 8px top margin.
- **Page style:** body system sans-serif, background `#f5f5f0`, text `#2a2a2a`, padding 40px, line-height 1.6; universal `* { margin:0; padding:0; box-sizing:border-box }` reset; h1 1.8em `#2980b9` with 10px bottom margin; subtitle `#666` 1.05em with 30px bottom margin. No nav bar, no back/home links.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22` (page accents here use `#2980b9` and `#27ae60`).
- **Canvases:** none on this page; any canvases elsewhere in this series use `window.devicePixelRatio` scaling.
