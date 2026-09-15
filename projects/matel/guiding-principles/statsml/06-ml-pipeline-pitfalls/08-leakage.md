# Leakage

**Page type:** grid page (card navigation grid, 4 columns, one philosophy callout, cards with category labels and topic tags)
**HTML title tag:** Leakage — ML Pipeline Pitfalls

**Subtitle:** The score is excellent because the answer was in the inputs. Future information, label proxies, and contaminated splits.

## Callout (philosophy box)

**Why this matters:** Leakage is the one failure mode that announces itself as success. Validation accuracy jumps, the feature importance chart looks decisive, and the cause is that the model was shown something it will not have at prediction time — a column derived from the label, a row that exists in both splits, a time series shuffled so the future sits in the training set. The tell is a result that is too good, and the instinct to celebrate it is why this category is expensive.

## Cards

Each card links to a detail page under `08-leakage/`. The card shows a colored uppercase category label, a numbered title, a one-line description, and a row of small topic tags.

| # | Category | Title | Link | Description | Topic tags |
|---|----------|-------|------|-------------|------------|
| 1 | LEAKAGE | Data Leakage (Future Info in Training) | [08-leakage/01-data-leakage-future-info-in-training.md](08-leakage/01-data-leakage-future-info-in-training.md) | Model uses information not available at prediction time | leakage, future-info, train-prod-gap |
| 2 | LEAKAGE | Target Leakage (Label Encoded in Features) | [08-leakage/02-target-leakage-label-encoded-in-features.md](08-leakage/02-target-leakage-label-encoded-in-features.md) | A feature is a direct proxy or consequence of the label | leakage, causality, proxy |
| 3 | LEAKAGE | Train/Test Contamination | [08-leakage/03-train-test-contamination.md](08-leakage/03-train-test-contamination.md) | Information from test set leaks into training process | leakage, entity-split, memorization |
| 4 | LEAKAGE | Temporal Leakage (Shuffling Time Series) | [08-leakage/04-temporal-leakage-shuffling-time-series.md](08-leakage/04-temporal-leakage-shuffling-time-series.md) | Random train/test split on time-ordered data lets model see the future | leakage, temporal, time-series |
| 5 | LEAKAGE | Information Leakage (Comprehensive) | [08-leakage/05-information-leakage-comprehensive.md](08-leakage/05-information-leakage-comprehensive.md) | The umbrella category: any info crossing a boundary it shouldn't | leakage, boundary, universal-test |

## Regeneration instructions

- **Template:** nav-grid style (see `docs/statsml/ui-templates/02-nav-grid`). Single page: h1, `.subtitle`, a `.philosophy` callout, then one `.nav-grid` of `.nav-card` anchors.
- **Layout:** `.nav-grid` is CSS grid, `repeat(4, 1fr)`, 16px gap, margin-top 15px; responsive: 3 columns below 1400px, 2 columns below 1100px, 1 column below 600px.
- **Links:** the table links to `.md` versions for markdown navigation; in the HTML each card's `href` is the same path with an `.html` extension.
- **Card structure:** `<a class="nav-card" href="...">` containing `<div class="card-num">CATEGORY</div>`, `<h3>N. Title</h3>` (unpadded index matching the file index), `<p>description</p>`, then `<div class="topics">` of `<span class="topic-tag">` per topic.
- **Category label colors:** inline script maps `.card-num` text through a `categoryColors` object and sets `el.style.color`: LEAKAGE `#e74c3c`. Base `.card-num` style is `#2980b9`, 0.75em, bold.
- **Page style:** body system sans-serif, background `#f5f5f0`, text `#2a2a2a`, padding 40px, line-height 1.6; h1 1.8em `#2980b9`; subtitle `#666` 1.05em. Philosophy callout background `#f0f4f8`, left border `4px solid #2980b9`. No nav bar, no back/home links.
