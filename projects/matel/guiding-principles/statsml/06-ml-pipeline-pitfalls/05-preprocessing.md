# Preprocessing

**Page type:** grid page (card navigation grid, 4 columns, one philosophy callout, cards with category labels and topic tags)
**HTML title tag:** Preprocessing — ML Pipeline Pitfalls

**Subtitle:** Fit on everything, evaluate on part of it. A scaler, an imputer, or a selector that saw the test set has already spent it.

## Callout (philosophy box)

**Why this matters:** Preprocessing feels like plumbing, which is why it is where leakage hides most comfortably. A scaler fit before the split carries the test mean into training; an imputer fills training gaps with a statistic computed from held-out rows; feature selection run on the full table picks winners partly because they happen to work on the test set. Each step is individually defensible, and the reported score is optimistic in a way no amount of cross-validation inside the leak can detect.

## Cards

Each card links to a detail page under `05-preprocessing/`. The card shows a colored uppercase category label, a numbered title, a one-line description, and a row of small topic tags.

| # | Category | Title | Link | Description | Topic tags |
|---|----------|-------|------|-------------|------------|
| 1 | PREPROCESSING | Scaling Before Splitting | [05-preprocessing/01-scaling-before-splitting.md](05-preprocessing/01-scaling-before-splitting.md) | Normalizing with global mean/std leaks test statistics into train | leakage, scaling, pipeline |
| 2 | PREPROCESSING | Imputation Leakage | [05-preprocessing/02-imputation-leakage.md](05-preprocessing/02-imputation-leakage.md) | Filling missing values using information from the full dataset | leakage, missing-data, imputation |
| 3 | PREPROCESSING | Multicollinearity Blindness | [05-preprocessing/03-multicollinearity-blindness.md](05-preprocessing/03-multicollinearity-blindness.md) | Correlated features inflate importance and destabilize coefficients | correlation, VIF, feature-importance |
| 4 | PREPROCESSING | Feature Selection on Full Dataset | [05-preprocessing/04-feature-selection-on-full-dataset.md](05-preprocessing/04-feature-selection-on-full-dataset.md) | Selecting features using test data produces optimistic performance | leakage, feature-selection, cross-validation |

## Regeneration instructions

- **Template:** nav-grid style (see `docs/statsml/ui-templates/02-nav-grid`). Single page: h1, `.subtitle`, a `.philosophy` callout, then one `.nav-grid` of `.nav-card` anchors.
- **Layout:** `.nav-grid` is CSS grid, `repeat(4, 1fr)`, 16px gap, margin-top 15px; responsive: 3 columns below 1400px, 2 columns below 1100px, 1 column below 600px.
- **Links:** the table links to `.md` versions for markdown navigation; in the HTML each card's `href` is the same path with an `.html` extension.
- **Card structure:** `<a class="nav-card" href="...">` containing `<div class="card-num">CATEGORY</div>`, `<h3>N. Title</h3>` (unpadded index matching the file index), `<p>description</p>`, then `<div class="topics">` of `<span class="topic-tag">` per topic.
- **Category label colors:** inline script maps `.card-num` text through a `categoryColors` object and sets `el.style.color`: PREPROCESSING `#2980b9`. Base `.card-num` style is `#2980b9`, 0.75em, bold.
- **Page style:** body system sans-serif, background `#f5f5f0`, text `#2a2a2a`, padding 40px, line-height 1.6; h1 1.8em `#2980b9`; subtitle `#666` 1.05em. Philosophy callout background `#f0f4f8`, left border `4px solid #2980b9`. No nav bar, no back/home links.
