# Drafts — Ideas & Sketches

**Page type:** grid page (card navigation grid, auto-fit columns min 300px, cards carry topic-tag pills)
**HTML title tag:** Drafts — Ideas & Sketches

**Subtitle:** Rough ideation on pipeline design, CNN shape classifiers, feature ontology, and whatever else comes up.

## Cards

Each card links to a sibling page in the same folder. The card shows a colored uppercase category label (`.card-num`), a numbered title, a description, and a row of topic-tag pills. Note: there is no card 17.

| # | Category | Title | Link | Description | Topics |
|---|----------|-------|------|-------------|--------|
| 1 | FOUNDATION | STATSML — Core Principles & Architecture | [01-core-principles.md](01-core-principles.md) | Goals, multi-candidate approach, data model, split inheritance, verify-then-apply. The full philosophy. | principles, architecture, philosophy |
| 2 | FOUNDATION | STATSML — Intuition & Key Objectives | [02-intuition-objectives.md](02-intuition-objectives.md) | Visual summary of 8 key objectives with canvas visualizations. | intuition, visual |
| 3 | FOUNDATION | Discussion Backlog | [03-discussion-backlog.md](03-discussion-backlog.md) | Future ideas: 1-level conditioning, type-aware buckets, archetype library, confidence decay. | ideas, future work |
| 4 | ARCHITECTURE | Pipeline Architecture | [04-pipeline-architecture.md](04-pipeline-architecture.md) | Multi-model candidates, range-based significance, feature expansion, evidence aggregation. | model registry, feature expansion, cascade |
| 5 | ARCHITECTURE | Feature Data Model & Test Registry | [05-feature-data-model.md](05-feature-data-model.md) | JSON schema: FeatureProfile, CandidateModel, SignificantRange, test preconditions, reasoning trace. | JSON schema, preconditions, hierarchy |
| 6 | CNN | CNN Shape Classification | [06-cnn-shape-classification.md](06-cnn-shape-classification.md) | Train CNN to classify distribution shapes — MNIST for distributions. 1D conv backbone, dual heads. | CNN, noise tolerance, shape taxonomy |
| 7 | CNN | Distribution Gallery | [07-distribution-gallery.md](07-distribution-gallery.md) | Real-world distributions with SE bands. 3-per-row grid across all shape classes. CNN training reference. | examples, SE bands, shape classes |
| 8 | CNN | Data Type Classification | [08-data-type-classification.md](08-data-type-classification.md) | Independent scores for num, bin, cat. Not softmax — multiple types simultaneously. | type scores, independent, num/bin/cat |
| 9 | CNN | Shape Classification Hierarchy | [09-shape-classification-hierarchy.md](09-shape-classification-hierarchy.md) | Relationships between shape classes. Parent/child, confusion boundaries, compound shapes. | hierarchy, classes, compound |
| 10 | DEEP DIVE | T-Test Multi-Candidate Verification | [10-ttest-precondition-design.md](10-ttest-precondition-design.md) | Multiple candidate interpretations with different parameter combos, validated across samples. Trust rating. | multi-candidate, trust rating, no magic numbers |
| 11 | DEEP DIVE | Distribution Matching | [11-distribution-matching.md](11-distribution-matching.md) | Test every feature against all known distribution families. MLE fitting, AIC weights, KS distance. | MLE, AIC weights |
| 12 | DEEP DIVE | Bin Sizing: Multi-Resolution | [12-bin-sizing-strategy.md](12-bin-sizing-strategy.md) | How bin width changes shape. Adaptive formulas + static density-guaranteed sizes. Persistence scoring. | multi-resolution, persistence, shape stability |
| 13 | DEEP DIVE | Range Splitting | [13-range-splitting.md](13-range-splitting.md) | Split at significant gaps. Each segment classified independently — not limited to Gaussian components. | gap detection, variance validation, recursive |
| 14 | DEEP DIVE | Bimodal Valley Split | [14-bimodal-valley-split.md](14-bimodal-valley-split.md) | Real twin peaks vs noise? Valley depth criterion, minimum sample guards, gap splitting interaction. | valley depth, both-peaks test, false bimodal |
| 15 | DEEP DIVE | Distribution Encoding Catalog | [15-distribution-encoding-catalog.md](15-distribution-encoding-catalog.md) | All ways to encode a distribution: histograms, percentile-width, CNN images, structural decomposition. | encoding, percentile-width, multi-resolution |
| 16 | DEEP DIVE | Meta-Distribution: Y-Axis Projection | [16-meta-distribution-y-projection.md](16-meta-distribution-y-projection.md) | Histogram of bin heights — second-order signal. Concentration degree, spike severity, structural anomalies. | meta, gini, y-projection |
| 17 | PSYCHOLOGY | Data & Psychology | [17-data-and-psychology.md](17-data-and-psychology.md) | How cognitive biases, behavioral economics, and decision psychology manifest in data patterns. Panic selling, anchoring, decoy effects, loss aversion — the human OS visible in distributions. | behavioral finance, cognitive bias, decision making |

## Regeneration instructions

To rebuild the HTML from this spec:

- **Template:** nav-grid style. Single page: `h1`, `.subtitle` paragraph, then one `.nav-grid` of `.nav-card` anchors.
- **Layout:** `.nav-grid` is CSS grid, `repeat(auto-fit, minmax(300px, 1fr))`, 16px gap.
- **Links:** the table above links to the `.md` versions for navigation in markdown; in the regenerated HTML, each card's `href` is the same path with an `.html` extension instead.
- **Card structure:** `<a class="nav-card" href="...">` containing `<div class="card-num" style="color:CATEGORY_COLOR">CATEGORY</div>`, `<h3>N. Title</h3>` (unpadded index number matching the file index), `<p>description</p>`, and a `<div class="topics">` of `<span class="topic-tag">` pills.
- **Category label colors:** FOUNDATION `#1a5276`; ARCHITECTURE `#2980b9`; CNN `#e67e22`; DEEP DIVE `#8e44ad`; PSYCHOLOGY `#e74c3c`.
- **Card style:** background `#ffffff`, border `1px solid #d8d8d8`, radius 10px, padding 20px, shadow `0 2px 4px rgba(0,0,0,0.05)`; hover: border `#8e44ad`, `translateY(-2px)`. `.card-num` 0.72em weight 700 uppercase letter-spacing 0.5px; h3 `#1a3a4a` 1em; description `#555` 0.85em. `.topics` flex wrap gap 4px, margin-top 8px; `.topic-tag` background `#f0f0f0`, border `1px solid #ccc`, radius 4px, padding 2px 6px, 0.7em, `#666`.
- **Page style:** body system sans-serif, background `#f5f5f0`, text `#2a2a2a`, padding 40px, line-height 1.6; h1 1.8em `#8e44ad`; subtitle `#666` 1.05em. No nav bar, no back/home links.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- No canvases on this page; canvases elsewhere use `window.devicePixelRatio` scaling.
