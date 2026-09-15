# Feature Engineering

**Page type:** grid page (card navigation grid, 4 columns, one philosophy callout, cards with category labels and topic tags)
**HTML title tag:** Feature Engineering — ML Pipeline Pitfalls

**Subtitle:** The feature is computable now and was not computable then. Look-ahead, off-by-one windows, and columns filled in after the outcome.

## Callout (philosophy box)

**Why this matters:** A feature is a promise that the value will exist, with that value, at prediction time. Engineering breaks the promise quietly: a rolling window that includes the current row, an aggregate whose boundary is off by one period, a field that operations backfill only after the event resolves. Offline the column is dense and predictive. In production it is empty, stale, or arrives too late — and the offline score was measuring a feature that will not exist when it matters.

## Cards

Each card links to a detail page under `06-feature-engineering/`. The card shows a colored uppercase category label, a numbered title, a one-line description, and a row of small topic tags.

| # | Category | Title | Link | Description | Topic tags |
|---|----------|-------|------|-------------|------------|
| 1 | FEATURE-ENG | Look-Ahead Bias in Feature Engineering | [06-feature-engineering/01-look-ahead-bias-in-feature-engineering.md](06-feature-engineering/01-look-ahead-bias-in-feature-engineering.md) | Feature uses information from the same row's future or outcome | leakage, temporal, target-encoding |
| 2 | FEATURE-ENG | Off-By-One in Windowing / Aggregation | [06-feature-engineering/02-off-by-one-in-windowing-aggregation.md](06-feature-engineering/02-off-by-one-in-windowing-aggregation.md) | Inclusive vs exclusive boundaries — feature looks ahead or misses data | windowing, boundary, leakage |
| 3 | FEATURE-ENG | Feature Computed After Target Event | [06-feature-engineering/03-feature-computed-after-target-event.md](06-feature-engineering/03-feature-computed-after-target-event.md) | Feature uses data generated AFTER the outcome already happened | leakage, temporal, causality |
| 4 | FEATURE-ENG | Duplicate / Highly Correlated Features | [06-feature-engineering/04-duplicate-highly-correlated-features.md](06-feature-engineering/04-duplicate-highly-correlated-features.md) | Near-identical features make models split on arbitrary copies | correlation, dedup, interpretability |

## Regeneration instructions

- **Template:** nav-grid style (see `docs/statsml/ui-templates/02-nav-grid`). Single page: h1, `.subtitle`, a `.philosophy` callout, then one `.nav-grid` of `.nav-card` anchors.
- **Layout:** `.nav-grid` is CSS grid, `repeat(4, 1fr)`, 16px gap, margin-top 15px; responsive: 3 columns below 1400px, 2 columns below 1100px, 1 column below 600px.
- **Links:** the table links to `.md` versions for markdown navigation; in the HTML each card's `href` is the same path with an `.html` extension.
- **Card structure:** `<a class="nav-card" href="...">` containing `<div class="card-num">CATEGORY</div>`, `<h3>N. Title</h3>` (unpadded index matching the file index), `<p>description</p>`, then `<div class="topics">` of `<span class="topic-tag">` per topic.
- **Category label colors:** inline script maps `.card-num` text through a `categoryColors` object and sets `el.style.color`: FEATURE-ENG `#e67e22`. Base `.card-num` style is `#2980b9`, 0.75em, bold.
- **Page style:** body system sans-serif, background `#f5f5f0`, text `#2a2a2a`, padding 40px, line-height 1.6; h1 1.8em `#2980b9`; subtitle `#666` 1.05em. Philosophy callout background `#f0f4f8`, left border `4px solid #2980b9`. No nav bar, no back/home links.
