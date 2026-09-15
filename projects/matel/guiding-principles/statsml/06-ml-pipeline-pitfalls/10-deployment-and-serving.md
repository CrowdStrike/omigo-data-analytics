# Deployment & Serving

**Page type:** grid page (card navigation grid, 4 columns, one philosophy callout, cards with category labels and topic tags)
**HTML title tag:** Deployment & Serving — ML Pipeline Pitfalls

**Subtitle:** Offline it worked. The training path and the serving path are different code, and they disagree.

## Callout (philosophy box)

**Why this matters:** A model in production is two implementations of the same feature logic that must agree forever: one batch, one online. They drift. A cached feature goes stale, a float rounds differently, a category the encoder never saw arrives and crashes or silently maps to zero, and the training set was drawn from a population the live traffic no longer resembles. The offline score remains a true statement about the offline pipeline and stops being a prediction about production behavior.

## Cards

Each card links to a detail page under `10-deployment-and-serving/`. The card shows a colored uppercase category label, a numbered title, a one-line description, and a row of small topic tags.

| # | Category | Title | Link | Description | Topic tags |
|---|----------|-------|------|-------------|------------|
| 1 | DEPLOYMENT | Non-Representative Training Data | [10-deployment-and-serving/01-non-representative-training-data.md](10-deployment-and-serving/01-non-representative-training-data.md) | Model trained on one population, deployed on a different one | representation, bias, subgroups |
| 2 | DEPLOYMENT | Stale Feature (Cached / Not Updated) | [10-deployment-and-serving/02-stale-feature-cached-not-updated.md](10-deployment-and-serving/02-stale-feature-cached-not-updated.md) | Feature value is outdated because refresh pipeline failed or lagged | staleness, cache, freshness |
| 3 | DEPLOYMENT | Backfill Bias (Data Filled Retroactively) | [10-deployment-and-serving/03-backfill-bias-data-filled-retroactively.md](10-deployment-and-serving/03-backfill-bias-data-filled-retroactively.md) | Historical data "corrected" with knowledge of the future | backfill, point-in-time, revision |
| 4 | DEPLOYMENT | Sampling Rate Changes | [10-deployment-and-serving/04-sampling-rate-changes.md](10-deployment-and-serving/04-sampling-rate-changes.md) | Collection frequency changes create fake trends in aggregated data | sampling, measurement, normalization |
| 5 | DEPLOYMENT | Floating Point / Precision Loss | [10-deployment-and-serving/05-floating-point-precision-loss.md](10-deployment-and-serving/05-floating-point-precision-loss.md) | Numerical precision issues create silent errors in aggregations | precision, NaN, float |
| 6 | DEPLOYMENT | Training-Serving Skew (Batch vs Online) | [10-deployment-and-serving/06-training-serving-skew-batch-vs-online.md](10-deployment-and-serving/06-training-serving-skew-batch-vs-online.md) | Features computed differently in training (batch) vs serving (real-time) | serving, skew, batch-vs-online |
| 7 | DEPLOYMENT | Feature Store Staleness | [10-deployment-and-serving/07-feature-store-staleness.md](10-deployment-and-serving/07-feature-store-staleness.md) | Feature store serves outdated values because refresh is unmonitored | feature-store, staleness, SLA |
| 8 | DEPLOYMENT | Volatile State in Real-Time Systems | [10-deployment-and-serving/08-volatile-state-in-real-time-systems.md](10-deployment-and-serving/08-volatile-state-in-real-time-systems.md) | Data changes between API calls — model sees inconsistent state | real-time, staleness, snapshot |
| 9 | DEPLOYMENT | Unseen Categories at Serving Time | [10-deployment-and-serving/09-unseen-categories-at-serving-time.md](10-deployment-and-serving/09-unseen-categories-at-serving-time.md) | A new category arrives in production and the encoder crashes or silently mis-maps it | encoding, vocabulary, serving |

## Regeneration instructions

- **Template:** nav-grid style (see `docs/statsml/ui-templates/02-nav-grid`). Single page: h1, `.subtitle`, a `.philosophy` callout, then one `.nav-grid` of `.nav-card` anchors.
- **Layout:** `.nav-grid` is CSS grid, `repeat(4, 1fr)`, 16px gap, margin-top 15px; responsive: 3 columns below 1400px, 2 columns below 1100px, 1 column below 600px.
- **Links:** the table links to `.md` versions for markdown navigation; in the HTML each card's `href` is the same path with an `.html` extension.
- **Card structure:** `<a class="nav-card" href="...">` containing `<div class="card-num">CATEGORY</div>`, `<h3>N. Title</h3>` (unpadded index matching the file index), `<p>description</p>`, then `<div class="topics">` of `<span class="topic-tag">` per topic.
- **Category label colors:** inline script maps `.card-num` text through a `categoryColors` object and sets `el.style.color`: DEPLOYMENT `#d35400`. Base `.card-num` style is `#2980b9`, 0.75em, bold.
- **Page style:** body system sans-serif, background `#f5f5f0`, text `#2a2a2a`, padding 40px, line-height 1.6; h1 1.8em `#2980b9`; subtitle `#666` 1.05em. Philosophy callout background `#f0f4f8`, left border `4px solid #2980b9`. No nav bar, no back/home links.
