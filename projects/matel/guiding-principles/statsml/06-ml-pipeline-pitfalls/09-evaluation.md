# Evaluation

**Page type:** grid page (card navigation grid, 4 columns, one philosophy callout, cards with category labels and topic tags)
**HTML title tag:** Evaluation — ML Pipeline Pitfalls

**Subtitle:** The number is real and answers a question nobody asked. Wrong metric, spent holdout, and thresholds tuned on the test set.

## Callout (philosophy box)

**Why this matters:** Evaluation failures are not arithmetic failures; the reported figure is usually computed correctly. The problem is what it is a figure *of*. A metric can be optimized while the decision it informs gets worse. A test set consulted repeatedly stops being held out, and the maximum over many peeks is biased upward by construction — choosing the threshold or the model on that set spends it. What is left is a number whose confidence interval no longer covers the truth.

## Cards

Each card links to a detail page under `09-evaluation/`. The card shows a colored uppercase category label, a numbered title, a one-line description, and a row of small topic tags.

| # | Category | Title | Link | Description | Topic tags |
|---|----------|-------|------|-------------|------------|
| 1 | EVALUATION | Wrong Metric for the Problem | [09-evaluation/01-wrong-metric-for-the-problem.md](09-evaluation/01-wrong-metric-for-the-problem.md) | Optimizing a metric that doesn't align with business value | metrics, cost-matrix, threshold |
| 2 | EVALUATION | Overparameterized Models on Small Data | [09-evaluation/02-overparameterized-models-on-small-data.md](09-evaluation/02-overparameterized-models-on-small-data.md) | More parameters than data points — memorization, not learning | overfitting, sample-size, complexity |
| 3 | EVALUATION | Ensemble Leakage (Stacking on Same Data) | [09-evaluation/03-ensemble-leakage-stacking-on-same-data.md](09-evaluation/03-ensemble-leakage-stacking-on-same-data.md) | Meta-model trained on base model predictions from the same data | stacking, leakage, cross-validation |
| 4 | EVALUATION | Threshold Selection on Test Set | [09-evaluation/04-threshold-selection-on-test-set.md](09-evaluation/04-threshold-selection-on-test-set.md) | Choosing classification threshold that maximizes test performance | threshold, validation, overfitting |
| 5 | EVALUATION | Model Selection on the Test Set | [09-evaluation/05-model-selection-on-the-test-set.md](09-evaluation/05-model-selection-on-the-test-set.md) | Every peek spends test-set information, and the reported maximum is a biased estimate | adaptive-overfitting, holdout, validation |

## Regeneration instructions

- **Template:** nav-grid style (see `docs/statsml/ui-templates/02-nav-grid`). Single page: h1, `.subtitle`, a `.philosophy` callout, then one `.nav-grid` of `.nav-card` anchors.
- **Layout:** `.nav-grid` is CSS grid, `repeat(4, 1fr)`, 16px gap, margin-top 15px; responsive: 3 columns below 1400px, 2 columns below 1100px, 1 column below 600px.
- **Links:** the table links to `.md` versions for markdown navigation; in the HTML each card's `href` is the same path with an `.html` extension.
- **Card structure:** `<a class="nav-card" href="...">` containing `<div class="card-num">CATEGORY</div>`, `<h3>N. Title</h3>` (unpadded index matching the file index), `<p>description</p>`, then `<div class="topics">` of `<span class="topic-tag">` per topic.
- **Category label colors:** inline script maps `.card-num` text through a `categoryColors` object and sets `el.style.color`: EVALUATION `#27ae60`. Base `.card-num` style is `#2980b9`, 0.75em, bold.
- **Page style:** body system sans-serif, background `#f5f5f0`, text `#2a2a2a`, padding 40px, line-height 1.6; h1 1.8em `#2980b9`; subtitle `#666` 1.05em. Philosophy callout background `#f0f4f8`, left border `4px solid #2980b9`. No nav bar, no back/home links.
