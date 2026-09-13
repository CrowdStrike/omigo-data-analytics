# ML in Production

**Page type:** grid page (tutorials category grid: single flat 4-column nav-grid of cards with topic tags)
**HTML title tag:** ML in Production

**Subtitle:** What happens to a model after the notebook — where its labels come from, how it serves live traffic, and how it earns its keep once deployed.

## Cards

Each card links to a topic page under `ml-production/`. The card shows a colored uppercase subcategory label (`.card-num`), a numbered title, a one-line description, and 2-4 topic tag pills. All cards sit in one flat `.nav-grid`; the colored labels carry the grouping.

| # | Category | Title | Link | Description | Topic tags |
|---|----------|-------|------|-------------|------------|
| 1 | LABELS & GROUND TRUTH | Human Labeling of Data | [19-ml-in-production/01-human-labeling-of-data.md](19-ml-in-production/01-human-labeling-of-data.md) | Training labels come from people, and people disagree more than you expect — inter-annotator agreement measures how much of your "ground truth" is agreed on. | annotators, agreement, labels are opinions |
| 2 | LABELS & GROUND TRUTH | Ground Truth Is a Myth | [19-ml-in-production/02-ground-truth-is-a-myth.md](19-ml-in-production/02-ground-truth-is-a-myth.md) | Training labels sound like facts, but for the hard cases they are somebody's judgment call written down — even careful experts disagree. | judgment calls, hard cases, label disagreement |
| 3 | LABELS & GROUND TRUTH | Active Learning | [19-ml-in-production/03-active-learning.md](19-ml-in-production/03-active-learning.md) | When labels are expensive, don't label at random — let the model point at the examples it is most unsure about and spend the budget there. | labeling budget, uncertainty, ask smart |
| 4 | TRAINING MEETS SERVING | Train/Serve Skew | [19-ml-in-production/04-train-serve-skew.md](19-ml-in-production/04-train-serve-skew.md) | The same feature computed one way for training and another way for serving — same name, different number, so the model starts wrong on day one. | two pipelines, feature mismatch, silent bug |
| 5 | TRAINING MEETS SERVING | Training vs Serving | [19-ml-in-production/05-training-vs-serving.md](19-ml-in-production/05-training-vs-serving.md) | A model lives twice — once at night, learning from a year of history, and once every morning, answering one live question in milliseconds. | two lives, batch vs live, where failures hide |
| 6 | TRAINING MEETS SERVING | Feature Stores | [19-ml-in-production/06-feature-stores.md](19-ml-in-production/06-feature-stores.md) | Compute each model input once, from one shared recipe, and hand the exact same number to both the training table and the live app. | one recipe, shared features, consistency |
| 7 | DEPLOYMENT & OPS | Model Deployment Patterns | [19-ml-in-production/07-model-deployment-patterns.md](19-ml-in-production/07-model-deployment-patterns.md) | Shadow, canary, and blue-green: first nobody sees the new model, then a small slice does, and there is always a one-switch way back. | shadow, canary, safe rollout |
| 8 | DEPLOYMENT & OPS | Inference Latency Budgets | [19-ml-in-production/08-inference-latency-budgets.md](19-ml-in-production/08-inference-latency-budgets.md) | The total time a model is allowed at answer time — every feature fetch spends from it, so a 50ms model cannot afford a 200ms feature. | serving time, time budget, feature cost |
| 9 | DEPLOYMENT & OPS | Model Monitoring & Drift | [19-ml-in-production/09-model-monitoring-and-drift.md](19-ml-in-production/09-model-monitoring-and-drift.md) | A deployed model is a snapshot of the world on training day — monitoring is how you notice the world moved on while the model stood still. | drift, silent decay, same model, new world |
| 10 | DEPLOYMENT & OPS | Batch Scoring vs Real-Time Inference | [19-ml-in-production/10-batch-scoring-vs-real-time-inference.md](19-ml-in-production/10-batch-scoring-vs-real-time-inference.md) | Compute predictions ahead of time for everyone, or on the spot for whoever asks — a three-way trade between cost, latency, and freshness. | precomputed, on demand, cost vs freshness |
| 11 | STRATEGY & PRACTICE | Data-Centric vs Model-Centric | [19-ml-in-production/11-data-centric-vs-model-centric.md](19-ml-in-production/11-data-centric-vs-model-centric.md) | When a prediction is bad, you can improve the algorithm or improve the data it learns from — and fixing the data is usually the bigger, cheaper win. | two strategies, fix the input, where gains live |
| 12 | STRATEGY & PRACTICE | The Cost of a Feature | [19-ml-in-production/12-the-cost-of-a-feature.md](19-ml-in-production/12-the-cost-of-a-feature.md) | A feature keeps billing you after it ships — subscriptions, pipelines, breakage — so every model input must earn back more than it costs to keep. | hidden costs, features as liabilities, earn your keep |
| 13 | STRATEGY & PRACTICE | Human-in-the-Loop Systems | [19-ml-in-production/13-human-in-the-loop-systems.md](19-ml-in-production/13-human-in-the-loop-systems.md) | The model decides the easy cases on its own and routes the unsure ones to people — whose answers become the training data for tomorrow's model. | thresholds, review queue, feedback loop |
| 14 | STRATEGY & PRACTICE | Propensity Models | [19-ml-in-production/14-propensity-models.md](19-ml-in-production/14-propensity-models.md) | Score each customer's chance of doing something — cancelling, clicking, buying — and only act on the score after calibrating it against reality. | propensity score, calibration, churn / click / buy |

## Regeneration instructions

To rebuild the HTML from this spec:

- **Template:** tutorials category grid. Single page: h1, `.subtitle` paragraph, then one flat `.nav-grid` of `.nav-card` anchors (no h2 section headings).
- **Layout:** `.nav-grid` is CSS grid, `repeat(4, 1fr)`, 16px gap, 15px top margin; responsive: 3 columns below 1400px, 2 below 1100px, 1 below 600px.
- **Links:** the tables above link to the `.md` versions for markdown navigation; in the regenerated HTML, each card's `href` is the same path with an `.html` extension instead.
- **Card structure:** `<a class="nav-card" href="...">` containing `<div class="card-num">SUBCATEGORY LABEL</div>`, `<h3>N. Topic Title</h3>` (unpadded index number matching the 2-digit zero-padded file index), `<p>description</p>`, then `<div class="topics">` of `<span class="topic-tag">` pills.
- **Category label colors:** applied by a small script mapping `.card-num` text to color — "LABELS & GROUND TRUTH" `#2980b9`, "TRAINING MEETS SERVING" `#27ae60`, "DEPLOYMENT & OPS" `#8e44ad`, "STRATEGY & PRACTICE" `#e67e22`; the CSS default for `.card-num` is `#2980b9`, 0.75em bold, 4px bottom margin.
- **Card style:** background `#ffffff`, border `1px solid #d8d8d8`, radius 10px, padding 20px, shadow `0 2px 4px rgba(0,0,0,0.05)`, transition on border-color/transform; hover: border `#2980b9`, `translateY(-2px)`. h3 `#1a3a4a` 1em with 6px bottom margin; description `#555` 0.85em. Topic tags: background `#f0f0f0`, border `1px solid #ccc`, radius 4px, padding 2px 6px, 0.7em `#666`, in a flex-wrap row with 4px gap and 8px top margin.
- **Page style:** body system sans-serif, background `#f5f5f0`, text `#2a2a2a`, padding 40px, line-height 1.6; universal `* { margin:0; padding:0; box-sizing:border-box }` reset; h1 1.8em `#2980b9` with 10px bottom margin; subtitle `#666` 1.05em with 30px bottom margin. No nav bar, no back/home links.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22` (page accents here use `#2980b9`, `#27ae60`, `#8e44ad`, `#e67e22`).
- **Canvases:** none on this page; any canvases elsewhere in this series use `window.devicePixelRatio` scaling.
