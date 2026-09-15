# Distribution Shift

**Page type:** grid page (card navigation grid, 4 columns, one philosophy callout, cards with category labels and topic tags)
**HTML title tag:** Distribution Shift — ML Pipeline Pitfalls

**Subtitle:** The model was right when it shipped. The data moved, the coefficients did not, and accuracy decays with nothing in the logs.

## Callout (philosophy box)

**Why this matters:** There is no bug in any of these. The code that was correct on Tuesday is still correct on Friday and its predictions are worse, because the thing being predicted changed shape underneath it. Class balance drifts, the input-to-outcome relationship decays while the inputs themselves look untouched, and a 99/1 split makes accuracy a number that rewards predicting nothing. Monitoring inputs alone will not catch the case where the inputs are stable and the relationship is what rotted.

## Cards

Each card links to a detail page under `04-distribution-shift/`. The card shows a colored uppercase category label, a numbered title, a one-line description, and a row of small topic tags.

| # | Category | Title | Link | Description | Topic tags |
|---|----------|-------|------|-------------|------------|
| 1 | DISTRIBUTION | Class Imbalance Mishandling | [04-distribution-shift/01-class-imbalance-mishandling.md](04-distribution-shift/01-class-imbalance-mishandling.md) | Treating 95/5 class ratio naively — model predicts majority class always | imbalance, metrics, oversampling |
| 2 | DISTRIBUTION | Label Shift (Train ≠ Deploy Distribution) | [04-distribution-shift/02-label-shift-train-not-equals-deploy-distribution.md](04-distribution-shift/02-label-shift-train-not-equals-deploy-distribution.md) | Class proportions in production differ from training | distribution, calibration, deployment |
| 3 | DISTRIBUTION | Extreme Imbalance Scalability Problem | [04-distribution-shift/03-extreme-imbalance-scalability-problem.md](04-distribution-shift/03-extreme-imbalance-scalability-problem.md) | Rare positives + statistical requirements = need massive negative data | imbalance, scalability, sampling |
| 4 | DISTRIBUTION | Concept Drift (Relationship Decays, Inputs Stable) | [04-distribution-shift/04-concept-drift-relationship-decays.md](04-distribution-shift/04-concept-drift-relationship-decays.md) | The feature-to-target relationship moves while every input monitor stays green | concept-drift, monitoring, retraining |

## Regeneration instructions

- **Template:** nav-grid style (see `docs/statsml/ui-templates/02-nav-grid`). Single page: h1, `.subtitle`, a `.philosophy` callout, then one `.nav-grid` of `.nav-card` anchors.
- **Layout:** `.nav-grid` is CSS grid, `repeat(4, 1fr)`, 16px gap, margin-top 15px; responsive: 3 columns below 1400px, 2 columns below 1100px, 1 column below 600px.
- **Links:** the table links to `.md` versions for markdown navigation; in the HTML each card's `href` is the same path with an `.html` extension.
- **Card structure:** `<a class="nav-card" href="...">` containing `<div class="card-num">CATEGORY</div>`, `<h3>N. Title</h3>` (unpadded index matching the file index), `<p>description</p>`, then `<div class="topics">` of `<span class="topic-tag">` per topic.
- **Category label colors:** inline script maps `.card-num` text through a `categoryColors` object and sets `el.style.color`: DISTRIBUTION `#8e44ad`. Base `.card-num` style is `#2980b9`, 0.75em, bold.
- **Page style:** body system sans-serif, background `#f5f5f0`, text `#2a2a2a`, padding 40px, line-height 1.6; h1 1.8em `#2980b9`; subtitle `#666` 1.05em. Philosophy callout background `#f0f4f8`, left border `4px solid #2980b9`. No nav bar, no back/home links.
