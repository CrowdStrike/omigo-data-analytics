# Machine Learning Algorithms

**Page type:** grid page (tutorials category grid: single flat 4-column nav-grid of cards with topic tags)
**HTML title tag:** Machine Learning Algorithms

**Subtitle:** The classic algorithms explained one concrete example at a time — how each one learns, when it works, and where it breaks.

## Cards

Each card links to a topic page under `ml-algorithms/`. The card shows a colored uppercase subcategory label (`.card-num`), a numbered title, a one-line description, and 2-4 topic tag pills. All cards sit in one flat `.nav-grid`; the colored labels carry the grouping.

| # | Category | Title | Link | Description | Topic tags |
|---|----------|-------|------|-------------|------------|
| 1 | LINEAR MODELS | Linear Regression as ML | [13-ml-algorithms/01-linear-regression-as-ml.md](13-ml-algorithms/01-linear-regression-as-ml.md) | The straight line through your data points is already a learning machine — fit, predict, measure error. | least squares, prediction, loss function |
| 2 | LINEAR MODELS | Logistic Regression | [13-ml-algorithms/02-logistic-regression.md](13-ml-algorithms/02-logistic-regression.md) | Bend the line into an S-curve so its output becomes a probability of yes or no. | classification, sigmoid, probabilities |
| 3 | LINEAR MODELS | The Perceptron | [13-ml-algorithms/03-the-perceptron.md](13-ml-algorithms/03-the-perceptron.md) | The simplest learner that fixes itself one mistake at a time — and the ancestor of every neural network. | decision boundary, update rule, neural nets |
| 4 | LINEAR MODELS | Linear Discriminant Analysis | [13-ml-algorithms/04-linear-discriminant-analysis.md](13-ml-algorithms/04-linear-discriminant-analysis.md) | Find the one tilted direction along which two classes pull apart the most, then separate them there. | projection, class separation, one direction |
| 5 | TREES & ENSEMBLES | Decision Trees | [13-ml-algorithms/05-decision-trees.md](13-ml-algorithms/05-decision-trees.md) | A flowchart of yes/no questions the algorithm asks about your data until it reaches an answer. | splits, impurity, overfitting |
| 6 | TREES & ENSEMBLES | Bagging | [13-ml-algorithms/06-bagging.md](13-ml-algorithms/06-bagging.md) | Train many models on shuffled copies of the same data and let them vote — the noise cancels out. | bootstrap, variance, averaging |
| 7 | TREES & ENSEMBLES | Random Forests | [13-ml-algorithms/07-random-forests.md](13-ml-algorithms/07-random-forests.md) | Bagging plus one trick — each tree only sees some of the columns, so the trees disagree in useful ways. | feature sampling, voting, decorrelation |
| 8 | TREES & ENSEMBLES | Boosting | [13-ml-algorithms/08-boosting.md](13-ml-algorithms/08-boosting.md) | Build weak models one after another, each focusing on the examples the previous ones got wrong. | weak learners, sequential, reweighting |
| 9 | TREES & ENSEMBLES | Gradient Boosting | [13-ml-algorithms/09-gradient-boosting.md](13-ml-algorithms/09-gradient-boosting.md) | Each new tree predicts the leftover error of the ones before it — small corrections that add up. | residuals, learning rate, XGBoost family |
| 10 | TREES & ENSEMBLES | Why Ensembles Win | [13-ml-algorithms/10-why-ensembles-win.md](13-ml-algorithms/10-why-ensembles-win.md) | Why a crowd of mediocre models beats one clever model — as long as they don't all make the same mistake. | diversity, bias-variance, wisdom of crowds |
| 11 | TREES & ENSEMBLES | Regression Trees | [13-ml-algorithms/11-regression-trees.md](13-ml-algorithms/11-regression-trees.md) | Predict a number by splitting rows into groups and answering with each group's mean — the little trees inside boosting. | variance splits, leaf means, boosting atoms |
| 12 | DISTANCE-BASED | k-Nearest Neighbors | [13-ml-algorithms/12-k-nearest-neighbors.md](13-ml-algorithms/12-k-nearest-neighbors.md) | No training at all — to classify a new point, just look at what its closest neighbors are. | lazy learning, choosing k, local patterns |
| 13 | DISTANCE-BASED | k-Means Clustering | [13-ml-algorithms/13-k-means-clustering.md](13-ml-algorithms/13-k-means-clustering.md) | Drop k pins on the map, assign every point to its nearest pin, move the pins, repeat until stable. | centroids, unsupervised, choosing k |
| 14 | DISTANCE-BASED | Hierarchical Clustering | [13-ml-algorithms/14-hierarchical-clustering.md](13-ml-algorithms/14-hierarchical-clustering.md) | Merge the two closest points, then the next closest, building a family tree of your data. | dendrogram, linkage, no fixed k |
| 15 | DISTANCE-BASED | Distance Metrics | [13-ml-algorithms/15-distance-metrics.md](13-ml-algorithms/15-distance-metrics.md) | What "close" actually means — the same two points can be near or far depending on how you measure. | euclidean, cosine, scaling matters |
| 16 | DISTANCE-BASED | DBSCAN | [13-ml-algorithms/16-dbscan.md](13-ml-algorithms/16-dbscan.md) | Grow clusters by chaining nearby dense points — curved shapes come out whole, and stray points get labeled noise. | density, noise points, no fixed k |
| 17 | PROBABILISTIC | Naive Bayes | [13-ml-algorithms/17-naive-bayes.md](13-ml-algorithms/17-naive-bayes.md) | Flip prediction around: how likely is this evidence under each class? Pretend clues are independent and it still works. | Bayes rule, spam filters, independence |
| 18 | PROBABILISTIC | Gaussian Mixture Models | [13-ml-algorithms/18-gaussian-mixture-models.md](13-ml-algorithms/18-gaussian-mixture-models.md) | Assume your data is several bell curves blended together, then work out which curve each point came from. | soft clustering, EM algorithm, bell curves |
| 19 | DIMENSIONALITY | The Curse of Dimensionality | [13-ml-algorithms/19-the-curse-of-dimensionality.md](13-ml-algorithms/19-the-curse-of-dimensionality.md) | Add enough columns and everything becomes far from everything else — distance itself stops meaning much. | high dimensions, sparsity, distance breakdown |
| 20 | DIMENSIONALITY | PCA: Squashing Dimensions | [13-ml-algorithms/20-pca-squashing-dimensions.md](13-ml-algorithms/20-pca-squashing-dimensions.md) | Find the directions where your data varies the most and keep only those — fewer columns, most of the story. | variance, projection, compression |
| 21 | DIMENSIONALITY | t-SNE & UMAP | [13-ml-algorithms/21-t-sne-and-umap.md](13-ml-algorithms/21-t-sne-and-umap.md) | Flatten hundreds of dimensions into a 2D picture that keeps neighbors together — great for looking, risky for measuring. | visualization, neighborhoods, read with care |
| 22 | MARGINS & KERNELS | Support Vector Machines (SVM) | [13-ml-algorithms/22-support-vector-machines-svm.md](13-ml-algorithms/22-support-vector-machines-svm.md) | Separate two groups with the widest possible street — only the few points touching its edges decide where it goes. | max margin, support vectors, decision boundary |
| 23 | MARGINS & KERNELS | The Kernel Trick | [13-ml-algorithms/23-the-kernel-trick.md](13-ml-algorithms/23-the-kernel-trick.md) | Problems a line can't split become easy in a bigger space — kernels compute there without ever building a single coordinate. | feature map, similarity, nonlinear |
| 24 | PATTERNS & RATINGS | Collaborative Filtering | [13-ml-algorithms/24-collaborative-filtering.md](13-ml-algorithms/24-collaborative-filtering.md) | "People like you also bought" — find customers whose past ratings match yours and recommend what they loved. | ratings matrix, taste neighbors, recommendations |
| 25 | PATTERNS & RATINGS | Anomaly Detection | [13-ml-algorithms/25-anomaly-detection.md](13-ml-algorithms/25-anomaly-detection.md) | Isolation forests find weird points with random cuts — a loner gets fenced off in two or three slices, a crowd point takes many. | isolation forest, outliers, unsupervised |
| 26 | PATTERNS & RATINGS | Association Rules | [13-ml-algorithms/26-association-rules.md](13-ml-algorithms/26-association-rules.md) | Count how often items share a basket — support says how common, confidence how reliable, lift whether it beats chance. | support, confidence, lift |
| 27 | PATTERNS & RATINGS | Apriori & FP-growth | [13-ml-algorithms/27-apriori-and-fp-growth.md](13-ml-algorithms/27-apriori-and-fp-growth.md) | Find every item combo that keeps showing up without counting all 2^n — prune candidates or compress receipts into a tree. | frequent itemsets, pruning, market baskets |
| 28 | PATTERNS & RATINGS | Elo Ratings | [13-ml-algorithms/28-elo-ratings.md](13-ml-algorithms/28-elo-ratings.md) | A running guess of skill — each match compares the result to what was expected, and the size of the surprise moves the number. | expected score, update rule, rankings |

## Regeneration instructions

To rebuild the HTML from this spec:

- **Template:** tutorials category grid. Single page: h1, `.subtitle` paragraph, then one flat `.nav-grid` of `.nav-card` anchors (no h2 section headings).
- **Layout:** `.nav-grid` is CSS grid, `repeat(4, 1fr)`, 16px gap, 15px top margin; responsive: 3 columns below 1400px, 2 below 1100px, 1 below 600px.
- **Links:** the tables above link to the `.md` versions for markdown navigation; in the regenerated HTML, each card's `href` is the same path with an `.html` extension instead.
- **Card structure:** `<a class="nav-card" href="...">` containing `<div class="card-num">SUBCATEGORY LABEL</div>`, `<h3>N. Topic Title</h3>` (unpadded index number matching the 2-digit zero-padded file index), `<p>description</p>`, then `<div class="topics">` of `<span class="topic-tag">` pills.
- **Category label colors:** applied by a small script mapping `.card-num` text to color — "LINEAR MODELS" `#2980b9`, "TREES & ENSEMBLES" `#27ae60`, "DISTANCE-BASED" `#e67e22`, "PROBABILISTIC" `#8e44ad`, "DIMENSIONALITY" `#e74c3c`, "MARGINS & KERNELS" `#16a085`, "PATTERNS & RATINGS" `#d35400`; the CSS default for `.card-num` is `#2980b9`, 0.75em bold, 4px bottom margin.
- **Card style:** background `#ffffff`, border `1px solid #d8d8d8`, radius 10px, padding 20px, shadow `0 2px 4px rgba(0,0,0,0.05)`, transition on border-color/transform; hover: border `#2980b9`, `translateY(-2px)`. h3 `#1a3a4a` 1em with 6px bottom margin; description `#555` 0.85em. Topic tags: background `#f0f0f0`, border `1px solid #ccc`, radius 4px, padding 2px 6px, 0.7em `#666`, in a flex-wrap row with 4px gap and 8px top margin.
- **Page style:** body system sans-serif, background `#f5f5f0`, text `#2a2a2a`, padding 40px, line-height 1.6; universal `* { margin:0; padding:0; box-sizing:border-box }` reset; h1 1.8em `#2980b9` with 10px bottom margin; subtitle `#666` 1.05em with 30px bottom margin. No nav bar, no back/home links.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22` (plus `#2980b9` and `#8e44ad` label accents).
- **Canvases:** none on this page; any canvases elsewhere in this series use `window.devicePixelRatio` scaling.
