# Model Evaluation

**Page type:** grid page (tutorials category grid: single flat 4-column nav-grid of cards with topic tags)
**HTML title tag:** Model Evaluation

**Subtitle:** How to tell whether a model is actually good — the scores, the curves, and the fair comparisons.

## Cards

Each card links to a topic page under `model-evaluation/`. The card shows a colored uppercase subcategory label (`.card-num`), a numbered title, a one-line description, and 2-4 topic tag pills. All cards sit in one flat `.nav-grid`; the colored labels carry the grouping. Note: cards 16 and 17 appear after card 7, and card 21 appears after card 15, so the full card order is 1-7, 16, 17, 8-10, 11-15, 21, 18-20.

| # | Category | Title | Link | Description | Topic tags |
|---|----------|-------|------|-------------|------------|
| 1 | CLASSIFICATION SCORES | Accuracy & Why It Misleads | [14-model-evaluation/01-accuracy-and-why-it-misleads.md](14-model-evaluation/01-accuracy-and-why-it-misleads.md) | Getting 99% right sounds great until you learn the model just says "no" to everything. | accuracy, majority class, rare events |
| 2 | CLASSIFICATION SCORES | Precision & Recall | [14-model-evaluation/02-precision-and-recall.md](14-model-evaluation/02-precision-and-recall.md) | Of the ones you flagged, how many were real — and of the real ones, how many did you catch. | precision, recall, false alarms |
| 3 | CLASSIFICATION SCORES | The F1 Score | [14-model-evaluation/03-the-f1-score.md](14-model-evaluation/03-the-f1-score.md) | One number that balances precision and recall — and punishes a model that is great at only one. | harmonic mean, balance, single score |
| 4 | CLASSIFICATION SCORES | The Confusion Matrix | [14-model-evaluation/04-the-confusion-matrix.md](14-model-evaluation/04-the-confusion-matrix.md) | A simple 2x2 table of hits and misses that every classification score is computed from. | true positives, false negatives, 2x2 table |
| 5 | CLASSIFICATION SCORES | ROC & AUC | [14-model-evaluation/05-roc-and-auc.md](14-model-evaluation/05-roc-and-auc.md) | How well the model ranks positives above negatives, summed up across every possible cutoff. | ROC curve, AUC, ranking |
| 6 | CLASSIFICATION SCORES | Precision-Recall Curves | [14-model-evaluation/06-precision-recall-curves.md](14-model-evaluation/06-precision-recall-curves.md) | The better curve to look at when the thing you are hunting for is rare. | PR curve, rare positives, tradeoff |
| 7 | CLASSIFICATION SCORES | Calibration | [14-model-evaluation/07-calibration.md](14-model-evaluation/07-calibration.md) | When the model says "70% chance", does it actually happen 70% of the time? | predicted probability, reliability, overconfidence |
| 8 | CLASSIFICATION SCORES | Sensitivity & Specificity | [14-model-evaluation/08-sensitivity-and-specificity.md](14-model-evaluation/08-sensitivity-and-specificity.md) | Medicine's names for catching the sick and clearing the healthy — and how they map to ML's recall. | sensitivity, specificity, screening tests |
| 9 | CLASSIFICATION SCORES | Multi-Class Metrics | [14-model-evaluation/09-multi-class-metrics.md](14-model-evaluation/09-multi-class-metrics.md) | Four classes, one confusion matrix — and why macro and micro averages of the same model disagree. | macro vs micro, confusion matrix, per-class scores |
| 10 | REGRESSION SCORES | MSE vs MAE | [14-model-evaluation/10-mse-vs-mae.md](14-model-evaluation/10-mse-vs-mae.md) | Two ways to average your errors — one punishes big misses hard, the other treats all misses alike. | squared error, absolute error, outliers |
| 11 | REGRESSION SCORES | R-Squared for Prediction | [14-model-evaluation/11-r-squared-for-prediction.md](14-model-evaluation/11-r-squared-for-prediction.md) | What "explains 80% of the variance" really means — and why a high R-squared can still predict badly. | variance explained, fit vs predict, baseline mean |
| 12 | REGRESSION SCORES | Percentage Errors & MAPE | [14-model-evaluation/12-percentage-errors-and-mape.md](14-model-evaluation/12-percentage-errors-and-mape.md) | Errors as percentages feel natural, but they blow up near zero and favor under-forecasting. | MAPE, relative error, division by zero |
| 13 | HONEST COMPARISON | Baselines: Beat the Dumb Model First | [14-model-evaluation/13-baselines-beat-the-dumb-model-first.md](14-model-evaluation/13-baselines-beat-the-dumb-model-first.md) | Before celebrating your model, check it beats "always predict yesterday" or "always say no". | naive baseline, sanity check, skill score |
| 14 | HONEST COMPARISON | Thresholds & Tradeoffs | [14-model-evaluation/14-thresholds-and-tradeoffs.md](14-model-evaluation/14-thresholds-and-tradeoffs.md) | The model gives a score; you pick the cutoff — and that choice decides who gets flagged. | decision threshold, cost of errors, operating point |
| 15 | HONEST COMPARISON | Class Imbalance | [14-model-evaluation/15-class-imbalance.md](14-model-evaluation/15-class-imbalance.md) | When one outcome is 100x more common than the other, most scores quietly stop meaning what you think. | rare class, base rate, metric choice |
| 16 | HONEST COMPARISON | Comparing Two Models Fairly | [14-model-evaluation/16-comparing-two-models-fairly.md](14-model-evaluation/16-comparing-two-models-fairly.md) | Same data split, same metric, and enough test cases to know the winner is not just luck. | same test set, noise vs skill, significance |
| 17 | HONEST COMPARISON | Offline vs Online Performance | [14-model-evaluation/17-offline-vs-online-performance.md](14-model-evaluation/17-offline-vs-online-performance.md) | A model that shines on historical data can still flop with live users — here is why the gap appears. | holdout vs live, drift, feedback loops |
| 18 | HONEST COMPARISON | Choosing the Metric for the System | [14-model-evaluation/18-choosing-the-metric-for-the-system.md](14-model-evaluation/18-choosing-the-metric-for-the-system.md) | Accuracy, recall, AUC, and calibration each crown a different winner — how predictions are used picks the metric. | metric choice, no single best, decision map |
| 19 | TASK-SPECIFIC METRICS | Ranking Metrics | [14-model-evaluation/19-ranking-metrics.md](14-model-evaluation/19-ranking-metrics.md) | When the model returns a ranked list, position is everything — reward putting the right answers near the top. | precision@k, MRR, NDCG |
| 20 | TASK-SPECIFIC METRICS | Recommendation Metrics | [14-model-evaluation/20-recommendation-metrics.md](14-model-evaluation/20-recommendation-metrics.md) | Accuracy says users liked the list — coverage, diversity, and serendipity say whether it ever showed them anything new. | coverage, diversity, serendipity |
| 21 | TASK-SPECIFIC METRICS | Text Generation Metrics | [14-model-evaluation/21-text-generation-metrics.md](14-model-evaluation/21-text-generation-metrics.md) | BLEU, ROUGE, and perplexity all grade machine-written text with counting — overlap with a reference, or surprise word by word. | BLEU, ROUGE, perplexity |

## Regeneration instructions

To rebuild the HTML from this spec:

- **Template:** tutorials category grid. Single page: h1, `.subtitle` paragraph, then one flat `.nav-grid` of `.nav-card` anchors (no h2 section headings).
- **Layout:** `.nav-grid` is CSS grid, `repeat(4, 1fr)`, 16px gap, 15px top margin; responsive: 3 columns below 1400px, 2 below 1100px, 1 below 600px.
- **Links:** the tables above link to the `.md` versions for markdown navigation; in the regenerated HTML, each card's `href` is the same path with an `.html` extension instead.
- **Card structure:** `<a class="nav-card" href="...">` containing `<div class="card-num">SUBCATEGORY LABEL</div>`, `<h3>N. Topic Title</h3>` (unpadded index number matching the 2-digit zero-padded file index), `<p>description</p>`, then `<div class="topics">` of `<span class="topic-tag">` pills. Cards 16 and 17 are placed after card 7; card 21 is placed after card 15.
- **Category label colors:** applied by a small script mapping `.card-num` text to color — "CLASSIFICATION SCORES" `#2980b9`, "REGRESSION SCORES" `#27ae60`, "HONEST COMPARISON" `#8e44ad`, "TASK-SPECIFIC METRICS" `#d35400`; the CSS default for `.card-num` is `#2980b9`, 0.75em bold, 4px bottom margin.
- **Card style:** background `#ffffff`, border `1px solid #d8d8d8`, radius 10px, padding 20px, shadow `0 2px 4px rgba(0,0,0,0.05)`, transition on border-color/transform; hover: border `#2980b9`, `translateY(-2px)`. h3 `#1a3a4a` 1em with 6px bottom margin; description `#555` 0.85em. Topic tags: background `#f0f0f0`, border `1px solid #ccc`, radius 4px, padding 2px 6px, 0.7em `#666`, in a flex-wrap row with 4px gap and 8px top margin.
- **Page style:** body system sans-serif, background `#f5f5f0`, text `#2a2a2a`, padding 40px, line-height 1.6; universal `* { margin:0; padding:0; box-sizing:border-box }` reset; h1 1.8em `#2980b9` with 10px bottom margin; subtitle `#666` 1.05em with 30px bottom margin. No nav bar, no back/home links.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22` (page accents here use `#2980b9`, `#27ae60`, `#8e44ad`).
- **Canvases:** none on this page; any canvases elsewhere in this series use `window.devicePixelRatio` scaling.
