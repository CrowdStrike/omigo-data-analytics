# Features & Data Prep

**Page type:** grid page (tutorials category grid: single flat 4-column nav-grid)
**HTML title tag:** Features & Data Prep

**Subtitle:** Getting raw data ready for a model — cleaning it up, reshaping it, building new columns, and deciding which ones to keep.

## Cards

Each card links to a topic page under `features-data-prep/`. The card shows a colored uppercase subcategory label (`.card-num`), a numbered title, a one-line description, and a row of topic-tag pills. All cards sit in one flat `.nav-grid`; the colored labels carry the grouping.

| # | Category | Title | Link | Description | Tags |
|---|----------|-------|------|-------------|------|
| 1 | CLEANING | Missing Values | [15-features-and-data-prep/01-missing-values.md](15-features-and-data-prep/01-missing-values.md) | What to do when cells are blank — and why the reason they are blank matters more than the fix. | imputation, missingness, defaults |
| 2 | CLEANING | Duplicates | [15-features-and-data-prep/02-duplicates.md](15-features-and-data-prep/02-duplicates.md) | Spotting rows that describe the same thing twice, and choosing which copy to keep. | dedup, keys, double counting |
| 3 | CLEANING | Inconsistent Categories | [15-features-and-data-prep/03-inconsistent-categories.md](15-features-and-data-prep/03-inconsistent-categories.md) | When "NY", "New York", and "new york" are three different values that should be one. | standardization, typos, mapping tables |
| 4 | CLEANING | Unit & Type Errors | [15-features-and-data-prep/04-unit-and-type-errors.md](15-features-and-data-prep/04-unit-and-type-errors.md) | Dollars mixed with cents, numbers stored as text — small mismatches that quietly wreck averages. | units, data types, parsing |
| 5 | CLEANING | Missing-Data Mechanisms | [15-features-and-data-prep/05-missing-data-mechanisms.md](15-features-and-data-prep/05-missing-data-mechanisms.md) | Why a value is missing decides what is safe to do about it — the same blank cell can be harmless or quietly poison your average. | MCAR, MAR, MNAR, missingness |
| 6 | TRANSFORMING | Scaling & Normalization | [15-features-and-data-prep/06-scaling-and-normalization.md](15-features-and-data-prep/06-scaling-and-normalization.md) | Putting columns with wildly different ranges onto a common scale so no single one dominates. | standardize, min-max, z-score |
| 7 | TRANSFORMING | Log Transforms | [15-features-and-data-prep/07-log-transforms.md](15-features-and-data-prep/07-log-transforms.md) | Taming skewed values like income or page views by working with their logarithms instead. | skew, long tail, multiplicative |
| 8 | TRANSFORMING | Encoding Categoricals | [15-features-and-data-prep/08-encoding-categoricals.md](15-features-and-data-prep/08-encoding-categoricals.md) | Turning labels like "red / green / blue" into numbers a model can use without inventing a fake order. | one-hot, ordinal, target encoding |
| 9 | TRANSFORMING | Binning | [15-features-and-data-prep/09-binning.md](15-features-and-data-prep/09-binning.md) | Grouping a continuous value like age into buckets — what you gain in simplicity and lose in detail. | buckets, quantiles, discretization |
| 10 | CREATING | Feature Engineering | [15-features-and-data-prep/10-feature-engineering.md](15-features-and-data-prep/10-feature-engineering.md) | Building new columns from raw data that make patterns easier for a model to find. | derived columns, domain knowledge, signal |
| 11 | CREATING | Ratios & Rates as Features | [15-features-and-data-prep/11-ratios-and-rates-as-features.md](15-features-and-data-prep/11-ratios-and-rates-as-features.md) | Why "purchases per visit" often beats raw counts — and the divide-by-zero traps that come with it. | ratios, rates, denominators |
| 12 | CREATING | Date & Time Features | [15-features-and-data-prep/12-date-and-time-features.md](15-features-and-data-prep/12-date-and-time-features.md) | Pulling day-of-week, hour, and "time since last event" out of timestamps to capture rhythms in behavior. | timestamps, seasonality, recency |
| 13 | CREATING | Text to Features | [15-features-and-data-prep/13-text-to-features.md](15-features-and-data-prep/13-text-to-features.md) | Turning free-form text like reviews or titles into numbers, from simple word counts to embeddings. | bag of words, tf-idf, embeddings |
| 14 | SELECTING | Feature Importance | [15-features-and-data-prep/14-feature-importance.md](15-features-and-data-prep/14-feature-importance.md) | Measuring how much each column actually contributes to a model's predictions. | permutation, contribution, interpretation |
| 15 | SELECTING | Feature Selection | [15-features-and-data-prep/15-feature-selection.md](15-features-and-data-prep/15-feature-selection.md) | Choosing which columns to keep — fewer, better features often beat throwing everything in. | filter methods, wrappers, overfitting |
| 16 | SELECTING | Redundant Features | [15-features-and-data-prep/16-redundant-features.md](15-features-and-data-prep/16-redundant-features.md) | When two columns say nearly the same thing, keeping both adds noise instead of information. | correlation, collinearity, pruning |

## Regeneration instructions

To rebuild the HTML from this spec:

- **Template:** tutorials category grid page (nav-grid style, see `docs/statsml/ui-templates/02-nav-grid`). Single page: h1, `.subtitle` paragraph, then one flat `.nav-grid` of `.nav-card` anchors (no h2 section headings).
- **Layout:** `.nav-grid` is CSS grid, `repeat(4, 1fr)`, 16px gap, 15px top margin; responsive: 3 columns below 1400px, 2 below 1100px, 1 below 600px.
- **Links:** the tables above link to `.md` versions for markdown navigation; in the regenerated HTML, each card's `href` is the same path with an `.html` extension instead (files zero-padded, e.g. `features-data-prep/01-missing-values.html`).
- **Card structure:** `<a class="nav-card" href="...">` containing `<div class="card-num">SUBCATEGORY</div>`, `<h3>N. Title</h3>` (unpadded index number matching the file index, running 1..16 across the whole page), `<p>description</p>`, and `<div class="topics">` with one `<span class="topic-tag">` per tag.
- **Category label colors** (applied by a small script mapping `.card-num` text to color): CLEANING `#e74c3c`, TRANSFORMING `#2980b9`, CREATING `#27ae60`, SELECTING `#8e44ad`; default `.card-num` color `#2980b9`.
- **Card style:** background `#ffffff`, border `1px solid #d8d8d8`, radius 10px, padding 20px, shadow `0 2px 4px rgba(0,0,0,0.05)`; hover: border `#2980b9`, `translateY(-2px)`. `.card-num` 0.75em bold; h3 `#1a3a4a` 1em; description `#555` 0.85em. `.topic-tag`: background `#f0f0f0`, border `1px solid #ccc`, radius 4px, padding 2px 6px, 0.7em, `#666`; `.topics` is flex with 4px gap, 8px top margin.
- **Page style:** body system sans-serif, background `#f5f5f0`, text `#2a2a2a`, padding 40px, line-height 1.6; h1 1.8em `#2980b9`; subtitle `#666` 1.05em. No nav bar, no back/home links.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- No canvases on this page; where canvases appear elsewhere in this project they use `window.devicePixelRatio` scaling.
