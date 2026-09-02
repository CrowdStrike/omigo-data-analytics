# Feedback Loops & Structural Biases

**Page type:** detail page (one `.bias-section` per bias, each a two-column layout table: text left 45%, canvas right 55%)
**HTML title tag:** Feedback Loops & Structural Biases

**Subtitle:** How predictions, omissions, and structural position create self-reinforcing distortions that compound over time.

## 1. Self-Fulfilling Prophecy

A model's prediction causes the very outcome it predicted, then that outcome reinforces the model in retraining. The feedback loop makes it impossible to observe the counterfactual.

- Model predicts churn → team deprioritizes customer → customer churns → model "validated"
- Credit score low → denied credit → can't build history → score stays low
- Counterfactual is unobservable: what would have happened without the prediction?

**Impact:** Creates reinforcing loops that appear to validate models that are actually causing outcomes. Requires randomized holdouts or causal inference to detect. *(key-point box)*

*Example: Churn model flags 1000 users. Team ignores them. 800 churn. Model retrained on this data shows 80% precision—but the precision was manufactured by the action.*

### Visualization (canvas `c1`, 720×300)

Circular feedback-loop diagram: four colored arc segments around a ring with arrowheads, four node labels.

- **Title (bold 14px `#1a5276`, top center):** "Self-Reinforcing Prediction Loop".
- **Ring:** center (360, 160), radius 100; faint background ring stroke `rgba(26,82,118,0.15)` width 20.
- **Arc segments** (width 4, arrowhead triangles at each end), one per quadrant starting at top and going clockwise; colors in order: red `#e74c3c`, orange `#e67e22`, blue `#1a5276`, green `#27ae60`.
- **Node labels** (bold 13px, colored to match their segment): "Prediction" (top), "Action" (right), "Outcome" (bottom), "Training Data" (left).
- **Center text (11px `#7f8c8d`):** "feedback loop" / "(no counterfactual)".

## 2. Omitted Variable Bias

When a confounding variable is left out of the model, the estimated effect of included variables absorbs the confounder's influence, producing a biased and inconsistent estimate.

- "Education → Income" effect is overstated when "Parental Wealth" is omitted
- Parental wealth drives both education access and income independently
- The observed coefficient conflates direct effect with confounded path

**Impact:** Policy decisions based on overstated coefficients waste resources. Adding controls or using instrumental variables is the remedy. *(key-point box)*

*Example: Naive regression shows each year of education adds $8k income. After controlling for parental wealth, the true effect is $3k.*

### Visualization (canvas `c2`, 720×300)

Causal DAG with three ellipse nodes and arrows.

- **Title (bold 14px `#1a5276`, top center):** "Omitted Variable Creates Overstated Effect".
- **Nodes** (ellipses 55×22 rx/ry, bold 14px centered labels): "Education" at (180, 200) and "Income" at (540, 200) — fill `#eaf2f8`, stroke/text `#1a5276`; "Parental Wealth" at (360, 80) — fill `#fdebd0`, stroke/text orange `#e67e22`.
- **Arrows:** solid red `#e74c3c` width 3 with arrowhead from Education → Income (observed, overstated); dashed (6/4) orange `#e67e22` width 2.5 arrows from Parental Wealth → Education and Parental Wealth → Income (hidden confounder paths). Arrows trimmed to start/end outside the node ellipses.
- **Legend (11px, bottom):** red solid swatch + "Observed (overstated)"; orange dashed line + "Hidden confounder paths".

## 3. Collider Bias

Conditioning on a common effect (collider) of two independent causes induces a spurious association between those causes. The bias arises from selection: restricting analysis to a subset defined by the collider opens a non-causal path.

- Talent and looks are independent in the population
- Among famous people (fame requires talent OR looks), they appear negatively correlated
- Famous people who lack talent tend to be good-looking, and vice versa

**Impact:** Any filter on the dataset that is caused by two or more variables opens spurious paths. Hospital data, accepted applicants, published papers—all collider-conditioned. *(key-point box)*

*Example: Among hospitalized patients, disease severity and age appear correlated—but only because admission requires either severe disease OR old age.*

### Visualization (canvas `c3`, 720×300)

Collider DAG: two independent causes pointing into a conditioned common effect.

- **Title (bold 14px `#1a5276`, top center):** "Collider Bias: Conditioning on Common Effect".
- **Nodes** (ellipses 48×22, bold 14px labels): "Talent" at (180, 90) and "Looks" at (540, 90) — fill `#eaf2f8`, stroke/text `#1a5276`; "Fame" at (360, 210) — fill `#fdebd0`, orange stroke width 2.5 plus a second outer ellipse (54×28, width 1.5) as a double border indicating conditioning, orange text, with 10px orange label "[conditioned on]" below the node.
- **Causal arrows:** solid `#1a5276` width 2.5 with arrowheads: Talent → Fame and Looks → Fame.
- **Spurious link:** dashed (5/4) red `#e74c3c` width 2.5 horizontal line between Talent and Looks at y=90, with 11px red labels above it: "(appears when conditioning on Fame)" at y=60 and "spurious negative correlation" at y=75.
- **Legend (11px, bottom):** blue solid swatch + "Causal path"; red dashed line + "Spurious (selection-induced)".

## 4. Position Bias → CTR Feedback Loop

Items shown in top positions get more clicks simply because they're visible. The system interprets clicks as relevance, boosts rank, which generates more clicks—a rich-get-richer (Matthew effect) loop.

- Position 1 gets 30% CTR regardless of quality; position 5 gets 4%
- Video sharing site autoplay counts as "engagement" though user never chose it
- New items never get impressions → permanent cold start starvation

**Impact:** Unlike credit scores, this loop operates at massive scale and compounds hourly. Mitigation requires explicit exploration (epsilon-greedy, Thompson sampling) or position-debiased CTR models. *(key-point box)*

*Example: Search result at position 1 "wins" every retraining cycle. After 10 cycles, removal shows it had no quality advantage—only position advantage.*

### Visualization (canvas `c4`, 720×300)

Circular feedback-loop diagram (same ring style as c1) with a starvation note at the bottom.

- **Title (bold 14px `#1a5276`, top center):** "Position Bias: Rich-Get-Richer Loop".
- **Ring:** center (360, 150), radius 90; background ring stroke `rgba(26,82,118,0.12)` width 22; four arc segments width 4 with arrowheads; colors in order: blue `#1a5276`, green `#27ae60`, orange `#e67e22`, purple `#8e44ad`.
- **Node labels** (bold 12px, colored to match): "High Position" (top), "More Clicks" (right), "Higher CTR Score" (bottom), "Boosted Rank" (left).
- **Center text:** bold 11px red "rich-get-richer" / 10px `#7f8c8d` "(Matthew effect)".
- **Bottom note (11px red, centered at y=280):** "New items never reach top positions → no clicks → no signal → permanent cold start".

## Regeneration instructions

- **Layout:** single detail page; h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.bias-section` blocks (40px bottom margin). Each section: `<h2>` numbered title with 2px `#2980b9` bottom border, then a `table.layout` (border-collapse, one `<tr>`) with `.text-col` td (45%) holding paragraph + `<ul>` + `.key-point` box + italic `.example` paragraph, and `.viz-col` td (55%) holding the canvas.
- **Boxes:** `.key-point` — background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem, `strong` label inside. `.example` — italic, `#555`, 0.9rem, no box.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; h2 1.3rem `#1a5276`; subtitle `#666` 0.95rem; lists 0.92rem. Canvases `width: 100%`, `1px solid #e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300 each; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper drawing at 720×300 logical size. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, purple `#8e44ad`, node fills `#eaf2f8` (blue tint) and `#fdebd0` (orange tint), muted gray `#7f8c8d`.
- In regenerated HTML, any card links use `.html` extensions (this page has no outbound links).
