# Machine Learning Fundamentals

**Page type:** grid page (tutorials category grid: single flat 4-column nav-grid of cards with topic tags)
**HTML title tag:** Machine Learning Fundamentals

**Subtitle:** The core ideas behind how machines learn from data — explained one concept at a time, with plain examples.

## Cards

Each card links to a topic page under `ml-fundamentals/`. The card shows a colored uppercase subcategory label (`.card-num`), a numbered title, a one-line description, and 2-4 topic tag pills. All cards sit in one flat `.nav-grid`; the colored labels carry the grouping.

| # | Category | Title | Link | Description | Topic tags |
|---|----------|-------|------|-------------|------------|
| 1 | WHAT ML IS | Learning from Examples | [12-ml-fundamentals/01-learning-from-examples.md](12-ml-fundamentals/01-learning-from-examples.md) | Instead of writing rules by hand, show the machine many examples and let it find the pattern itself. | pattern finding, rules vs data, core idea |
| 2 | WHAT ML IS | Supervised vs Unsupervised | [12-ml-fundamentals/02-supervised-vs-unsupervised.md](12-ml-fundamentals/02-supervised-vs-unsupervised.md) | Learning with an answer key versus finding structure in data that has no labels at all. | labels, clustering, learning types |
| 3 | WHAT ML IS | Classification vs Regression | [12-ml-fundamentals/03-classification-vs-regression.md](12-ml-fundamentals/03-classification-vs-regression.md) | Predicting a category like spam-or-not, versus predicting a number like tomorrow's price. | categories, numbers, prediction targets |
| 4 | WHAT ML IS | The Model as a Function | [12-ml-fundamentals/04-the-model-as-a-function.md](12-ml-fundamentals/04-the-model-as-a-function.md) | A trained model is just a function: inputs go in, a prediction comes out, and training tunes its knobs. | inputs and outputs, parameters, mental model |
| 5 | WHAT ML IS | Generative vs Discriminative | [12-ml-fundamentals/05-generative-vs-discriminative.md](12-ml-fundamentals/05-generative-vs-discriminative.md) | Learn what each class looks like and ask which fits better, or skip that and learn only the dividing line. | Bayes rule, decision boundary, classifier families |
| 6 | WHAT ML IS | One-Class, Binary & Multiclass | [12-ml-fundamentals/06-one-class-binary-and-multiclass.md](12-ml-fundamentals/06-one-class-binary-and-multiclass.md) | Count your classes before picking a tool: only normal examples, exactly two answers, or one of many. | problem shapes, one-vs-rest, novelty detection |
| 7 | THE LEARNING SETUP | Train / Validation / Test Split | [12-ml-fundamentals/07-train-validation-test-split.md](12-ml-fundamentals/07-train-validation-test-split.md) | Why you learn on one slice of data, tune on another, and save a final untouched slice for judging. | holdout, honest evaluation, data splits |
| 8 | THE LEARNING SETUP | Cross-Validation | [12-ml-fundamentals/08-cross-validation.md](12-ml-fundamentals/08-cross-validation.md) | Rotate which slice of data is held out so every row gets a turn as the test, then average the scores. | k-fold, small data, stable estimates |
| 9 | THE LEARNING SETUP | Overfitting & Underfitting | [12-ml-fundamentals/09-overfitting-and-underfitting.md](12-ml-fundamentals/09-overfitting-and-underfitting.md) | Memorizing the homework versus not studying enough — both fail the exam for opposite reasons. | memorization, generalization, model fit |
| 10 | THE LEARNING SETUP | The Bias-Variance Tradeoff | [12-ml-fundamentals/10-the-bias-variance-tradeoff.md](12-ml-fundamentals/10-the-bias-variance-tradeoff.md) | Simple models miss the pattern, flexible models chase the noise — good models balance the two errors. | bias, variance, sweet spot |
| 11 | THE LEARNING SETUP | Data Leakage | [12-ml-fundamentals/11-data-leakage.md](12-ml-fundamentals/11-data-leakage.md) | When information from the answer sneaks into the training data, the model looks great — until real use. | leaked answers, too good to be true, evaluation traps |
| 12 | HOW MODELS LEARN | Loss Functions | [12-ml-fundamentals/12-loss-functions.md](12-ml-fundamentals/12-loss-functions.md) | A single number that scores how wrong the model is — training is just the effort to make it smaller. | error score, squared error, what to minimize |
| 13 | HOW MODELS LEARN | Gradient Descent | [12-ml-fundamentals/13-gradient-descent.md](12-ml-fundamentals/13-gradient-descent.md) | Walk downhill on the error landscape one small step at a time until you reach a low point. | downhill steps, optimization, slopes |
| 14 | HOW MODELS LEARN | The Learning Rate | [12-ml-fundamentals/14-the-learning-rate.md](12-ml-fundamentals/14-the-learning-rate.md) | The step size of each downhill move — too big and you overshoot, too small and you never arrive. | step size, convergence, tuning |
| 15 | HOW MODELS LEARN | Regularization | [12-ml-fundamentals/15-regularization.md](12-ml-fundamentals/15-regularization.md) | A penalty for overly complicated models that nudges them toward simpler, more trustworthy answers. | complexity penalty, L1 and L2, simpler models |
| 16 | HOW MODELS LEARN | Early Stopping | [12-ml-fundamentals/16-early-stopping.md](12-ml-fundamentals/16-early-stopping.md) | Quit training the moment the model stops improving on fresh data, before it starts memorizing. | when to stop, validation curve, overfit guard |
| 17 | HOW MODELS LEARN | Hyperparameter Search | [12-ml-fundamentals/17-hyperparameter-search.md](12-ml-fundamentals/17-hyperparameter-search.md) | Knobs you set before training — and why random placement usually beats a neat grid of trials. | tuning knobs, random vs grid, search budget |
| 18 | INFORMATION THEORY | Entropy | [12-ml-fundamentals/18-entropy.md](12-ml-fundamentals/18-entropy.md) | A measure of how mixed-up or unpredictable something is — a fair coin has more of it than a loaded one. | uncertainty, mixedness, coin flips |
| 19 | INFORMATION THEORY | Information Gain | [12-ml-fundamentals/19-information-gain.md](12-ml-fundamentals/19-information-gain.md) | How much a question reduces your uncertainty — the rule decision trees use to pick their next split. | good questions, decision trees, splits |
| 20 | INFORMATION THEORY | Surprise & Bits | [12-ml-fundamentals/20-surprise-and-bits.md](12-ml-fundamentals/20-surprise-and-bits.md) | Rare events carry more information than expected ones, and bits are the currency that measures it. | rare events, bits, information content |
| 21 | INFORMATION THEORY | Cross-Entropy | [12-ml-fundamentals/21-cross-entropy.md](12-ml-fundamentals/21-cross-entropy.md) | The price you pay for predicting with the wrong probabilities — and the loss most classifiers minimize. | wrong beliefs, classification loss, probabilities |
| 22 | INFORMATION THEORY | KL Divergence | [12-ml-fundamentals/22-kl-divergence.md](12-ml-fundamentals/22-kl-divergence.md) | The extra bits you pay per observation for acting on the wrong distribution instead of the true one. | wrong belief, extra bits, distribution distance |

## Regeneration instructions

To rebuild the HTML from this spec:

- **Template:** tutorials category grid. Single page: h1, `.subtitle` paragraph, then one flat `.nav-grid` of `.nav-card` anchors (no h2 section headings).
- **Layout:** `.nav-grid` is CSS grid, `repeat(4, 1fr)`, 16px gap, 15px top margin; responsive: 3 columns below 1400px, 2 below 1100px, 1 below 600px.
- **Links:** the tables above link to the `.md` versions for markdown navigation; in the regenerated HTML, each card's `href` is the same path with an `.html` extension instead.
- **Card structure:** `<a class="nav-card" href="...">` containing `<div class="card-num">SUBCATEGORY LABEL</div>`, `<h3>N. Topic Title</h3>` (unpadded index number matching the 2-digit zero-padded file index), `<p>description</p>`, then `<div class="topics">` of `<span class="topic-tag">` pills.
- **Category label colors:** applied by a small script mapping `.card-num` text to color — "WHAT ML IS" `#e74c3c`, "THE LEARNING SETUP" `#2980b9`, "HOW MODELS LEARN" `#27ae60`, "INFORMATION THEORY" `#8e44ad`; the CSS default for `.card-num` is `#2980b9`, 0.75em bold, 4px bottom margin.
- **Card style:** background `#ffffff`, border `1px solid #d8d8d8`, radius 10px, padding 20px, shadow `0 2px 4px rgba(0,0,0,0.05)`, transition on border-color/transform; hover: border `#2980b9`, `translateY(-2px)`. h3 `#1a3a4a` 1em with 6px bottom margin; description `#555` 0.85em. Topic tags: background `#f0f0f0`, border `1px solid #ccc`, radius 4px, padding 2px 6px, 0.7em `#666`, in a flex-wrap row with 4px gap and 8px top margin.
- **Page style:** body system sans-serif, background `#f5f5f0`, text `#2a2a2a`, padding 40px, line-height 1.6; universal `* { margin:0; padding:0; box-sizing:border-box }` reset; h1 1.8em `#2980b9` with 10px bottom margin; subtitle `#666` 1.05em with 30px bottom margin. No nav bar, no back/home links.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22` (page accents here use `#e74c3c`, `#2980b9`, `#27ae60`, `#8e44ad`).
- **Canvases:** none on this page; any canvases elsewhere in this series use `window.devicePixelRatio` scaling.
