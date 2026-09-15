# Data Quality

**Page type:** grid page (card navigation grid, 4 columns, one philosophy callout, cards with category labels and topic tags)
**HTML title tag:** Data Quality — ML Pipeline Pitfalls

**Subtitle:** Values inside the valid range and still wrong. Range checks pass, so nothing fires — the contradiction is between fields, not inside one.

## Callout (philosophy box)

**Why this matters:** Most quality tooling checks one column at a time against a plausible interval, which catches typos and misses everything interesting. A weight of 70 kg is valid; a weight of 70 kg on a two-year-old is not, and no single-column rule can see it. The same blind spot covers deleting outliers before asking what produced them, and aggregation that averages away the pattern you were hired to find. The value is legal. The record is impossible.

## Cards

Each card links to a detail page under `03-data-quality/`. The card shows a colored uppercase category label, a numbered title, a one-line description, and a row of small topic tags.

| # | Category | Title | Link | Description | Topic tags |
|---|----------|-------|------|-------------|------------|
| 1 | DATA-QUALITY | Wrong Missing Data Assumptions | [03-data-quality/01-wrong-missing-data-assumptions.md](03-data-quality/01-wrong-missing-data-assumptions.md) | Treating all missing data the same when "why it's missing" matters | missing-data, MCAR, MNAR |
| 2 | DATA-QUALITY | Aggregation Destroys Signal | [03-data-quality/02-aggregation-destroys-signal.md](03-data-quality/02-aggregation-destroys-signal.md) | Summarizing into one number loses the pattern that matters | aggregation, mean, signal-loss |
| 3 | DATA-QUALITY | Outlier Handling Before Understanding | [03-data-quality/03-outlier-handling-before-understanding.md](03-data-quality/03-outlier-handling-before-understanding.md) | Removing outliers because they're "weird" before asking why they exist | outliers, signal, subpopulation |
| 4 | DATA-QUALITY | Rare Category Explosion | [03-data-quality/04-rare-category-explosion.md](03-data-quality/04-rare-category-explosion.md) | One-hot encoding high-cardinality features creates thousands of sparse columns | cardinality, encoding, sparsity |
| 5 | DATA-QUALITY | Incorrect Values That Pass Every Check | [03-data-quality/05-incorrect-values-that-pass-every-check.md](03-data-quality/05-incorrect-values-that-pass-every-check.md) | The value was wrong at the source — sentinels and impossible numbers clear every schema check | sentinels, plausibility, validation |
| 6 | DATA-QUALITY | Cross-Field Contradictions | [03-data-quality/06-cross-field-contradictions.md](03-data-quality/06-cross-field-contradictions.md) | Every field is individually valid but the combination is impossible | row-invariants, consistency, contradictions |

## Regeneration instructions

- **Template:** nav-grid style (see `docs/statsml/ui-templates/02-nav-grid`). Single page: h1, `.subtitle`, a `.philosophy` callout, then one `.nav-grid` of `.nav-card` anchors.
- **Layout:** `.nav-grid` is CSS grid, `repeat(4, 1fr)`, 16px gap, margin-top 15px; responsive: 3 columns below 1400px, 2 columns below 1100px, 1 column below 600px.
- **Links:** the table links to `.md` versions for markdown navigation; in the HTML each card's `href` is the same path with an `.html` extension.
- **Card structure:** `<a class="nav-card" href="...">` containing `<div class="card-num">CATEGORY</div>`, `<h3>N. Title</h3>` (unpadded index matching the file index), `<p>description</p>`, then `<div class="topics">` of `<span class="topic-tag">` per topic.
- **Category label colors:** inline script maps `.card-num` text through a `categoryColors` object and sets `el.style.color`: DATA-QUALITY `#795548`. Base `.card-num` style is `#2980b9`, 0.75em, bold.
- **Page style:** body system sans-serif, background `#f5f5f0`, text `#2a2a2a`, padding 40px, line-height 1.6; h1 1.8em `#2980b9`; subtitle `#666` 1.05em. Philosophy callout background `#f0f4f8`, left border `4px solid #2980b9`. No nav bar, no back/home links.
