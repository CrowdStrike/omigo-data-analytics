# Calculus & Optimization

**Page type:** grid page (tutorials category grid: single flat 4-column nav-grid of cards with topic tags)
**HTML title tag:** Calculus & Optimization

**Subtitle:** How machines find the best answer — feel which way is downhill, decide how big a step to take, and search by trial when there is no slope to follow.

## Cards

Each card links to a topic page under `calculus-optimization/`. The card shows a colored uppercase subcategory label (`.card-num`), a numbered title, a one-line description, and 2-4 topic tag pills. All cards sit in one flat `.nav-grid`; the colored labels carry the grouping.

| # | Category | Title | Link | Description | Topic tags |
|---|----------|-------|------|-------------|------------|
| 1 | FOUNDATIONS | The Chain Rule | [04-calculus-and-optimization/01-the-chain-rule.md](04-calculus-and-optimization/01-the-chain-rule.md) | The end-to-end sensitivity of a pipeline is the product of each stage's local slope — the rule backpropagation mechanizes through a neural net. | local slopes, multiply through, backpropagation |
| 2 | FOUNDATIONS | Convexity | [04-calculus-and-optimization/02-convexity.md](04-calculus-and-optimization/02-convexity.md) | A convex cost curve is one bowl with one valley — walk downhill from anywhere and you land at the best answer; a wavy curve can trap you in a false dip. | one bowl, global minimum, local traps |
| 3 | GRADIENT METHODS | Gradient Descent in Many Dimensions | [04-calculus-and-optimization/03-gradient-descent-in-many-dimensions.md](04-calculus-and-optimization/03-gradient-descent-in-many-dimensions.md) | "Feel the slope, step downhill, repeat" works everywhere — but in many dimensions the landscape is ruled by ravines and saddle points, not deep traps. | downhill steps, saddle points, ravines |
| 4 | GRADIENT METHODS | Line Search | [04-calculus-and-optimization/04-line-search.md](04-calculus-and-optimization/04-line-search.md) | Gradient descent knows which way is downhill but not how far to go — a line search takes a bold trial step and shrinks it until the altitude drop is good enough. | step size, backtracking, trial and shrink |
| 5 | GRADIENT METHODS | Newton's Method | [04-calculus-and-optimization/05-newtons-method.md](04-calculus-and-optimization/05-newtons-method.md) | Guess, measure the slope, and jump to where the tangent hits zero — using curvature turns a slow guessing game into a solver that doubles its correct digits every step. | tangent jump, curvature, fast convergence |
| 6 | GRADIENT METHODS | Momentum & Adam | [04-calculus-and-optimization/06-momentum-and-adam.md](04-calculus-and-optimization/06-momentum-and-adam.md) | Plain gradient descent bounces across narrow valleys instead of moving forward — momentum remembers direction to cancel the bounce, and Adam gives every knob its own step size. | velocity, adaptive steps, optimizers |
| 7 | GRADIENT METHODS | Stochastic Gradient Descent | [04-calculus-and-optimization/07-stochastic-gradient-descent.md](04-calculus-and-optimization/07-stochastic-gradient-descent.md) | You rarely need the exact downhill direction — a thousand cheap, noisy steps computed from single examples beat one perfect step that reads all the data. | noisy steps, mini-batches, cheap updates |
| 8 | CONSTRAINED OPTIMIZATION | Lagrange Multipliers | [04-calculus-and-optimization/08-lagrange-multipliers.md](04-calculus-and-optimization/08-lagrange-multipliers.md) | To optimize while stuck on a constraint, walk along it until the objective's gradient lines up with the constraint's — at that point no allowed step can improve you. | constraints, gradient alignment, shadow price |
| 9 | SEARCH WITHOUT SLOPES | Simulated Annealing | [04-calculus-and-optimization/09-simulated-annealing.md](04-calculus-and-optimization/09-simulated-annealing.md) | Escape a shallow valley by sometimes accepting a worse move — be adventurous while the search is "hot", turn strict as it cools, and you can cross hills a greedy search never climbs. | accept worse moves, cooling schedule, escape traps |
| 10 | SEARCH WITHOUT SLOPES | Genetic Algorithms | [04-calculus-and-optimization/10-genetic-algorithms.md](04-calculus-and-optimization/10-genetic-algorithms.md) | Breed better answers instead of calculating them — keep the best candidates, mix pairs, tweak at random, and repeat until the scores stop climbing. | populations, crossover, mutation |

## Regeneration instructions

To rebuild the HTML from this spec:

- **Template:** tutorials category grid. Single page: h1, `.subtitle` paragraph, then one flat `.nav-grid` of `.nav-card` anchors (no h2 section headings).
- **Layout:** `.nav-grid` is CSS grid, `repeat(4, 1fr)`, 16px gap, 15px top margin; responsive: 3 columns below 1400px, 2 below 1100px, 1 below 600px.
- **Links:** the tables above link to the `.md` versions for markdown navigation; in the regenerated HTML, each card's `href` is the same path with an `.html` extension instead.
- **Card structure:** `<a class="nav-card" href="...">` containing `<div class="card-num">SUBCATEGORY LABEL</div>`, `<h3>N. Topic Title</h3>` (unpadded index number matching the 2-digit zero-padded file index), `<p>description</p>`, then `<div class="topics">` of `<span class="topic-tag">` pills.
- **Category label colors:** applied by a small script mapping `.card-num` text to color — "FOUNDATIONS" `#2980b9`, "GRADIENT METHODS" `#27ae60`, "CONSTRAINED OPTIMIZATION" `#8e44ad`, "SEARCH WITHOUT SLOPES" `#e67e22`; the CSS default for `.card-num` is `#2980b9`, 0.75em bold, 4px bottom margin.
- **Card style:** background `#ffffff`, border `1px solid #d8d8d8`, radius 10px, padding 20px, shadow `0 2px 4px rgba(0,0,0,0.05)`, transition on border-color/transform; hover: border `#2980b9`, `translateY(-2px)`. h3 `#1a3a4a` 1em with 6px bottom margin; description `#555` 0.85em. Topic tags: background `#f0f0f0`, border `1px solid #ccc`, radius 4px, padding 2px 6px, 0.7em `#666`, in a flex-wrap row with 4px gap and 8px top margin.
- **Page style:** body system sans-serif, background `#f5f5f0`, text `#2a2a2a`, padding 40px, line-height 1.6; universal `* { margin:0; padding:0; box-sizing:border-box }` reset; h1 1.8em `#2980b9` with 10px bottom margin; subtitle `#666` 1.05em with 30px bottom margin. No nav bar, no back/home links.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22` (page accents here use `#2980b9`, `#27ae60`, `#8e44ad`, `#e67e22`).
- **Canvases:** none on this page; any canvases elsewhere in this series use `window.devicePixelRatio` scaling.
