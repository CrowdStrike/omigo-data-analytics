# ML & Data Pipeline Pitfalls

**Page type:** grid page (4-column card navigation grid; each card opens a per-stage sub-grid, not a detail page)
**HTML title tag:** ML & Data Pipeline Pitfalls

**Subtitle:** Practical mistakes that happen in real data pipelines, ML model development, and statistical analysis — things that burn practitioners daily. Grouped by the stage of the pipeline where the damage is done.

## Cards

Each card links to a category grid page under `06-ml-pipeline-pitfalls/`, which in turn holds the individual pitfall detail pages in a folder of the same name. Card order follows the pipeline: ingestion first, serving last, with `Misc` held at the end.

| # | Category | Title | Link | Description | Topic tags |
|---|----------|-------|------|-------------|------------|
| 1 | DATA INGESTION & ETL | Data Ingestion & ETL | [06-ml-pipeline-pitfalls/01-data-ingestion-and-etl.md](06-ml-pipeline-pitfalls/01-data-ingestion-and-etl.md) | The job succeeded and loaded the wrong rows — duplication, fanout, partial windows. | duplicates, joins, fanout, completeness |
| 2 | SCHEMA & SEMANTICS | Schema & Semantics | [06-ml-pipeline-pitfalls/02-schema-and-semantics.md](06-ml-pipeline-pitfalls/02-schema-and-semantics.md) | The column kept its name and changed its meaning; every type check still passes. | schema, units, timezone, NULL-semantics |
| 3 | DATA QUALITY | Data Quality | [06-ml-pipeline-pitfalls/03-data-quality.md](06-ml-pipeline-pitfalls/03-data-quality.md) | Values inside the valid range and still impossible once you read two fields together. | missing-data, outliers, plausibility, contradictions |
| 4 | DISTRIBUTION SHIFT | Distribution Shift | [06-ml-pipeline-pitfalls/04-distribution-shift.md](06-ml-pipeline-pitfalls/04-distribution-shift.md) | No bug changed; the data moved and the model quietly got worse. | imbalance, concept-drift, calibration |
| 5 | PREPROCESSING | Preprocessing | [06-ml-pipeline-pitfalls/05-preprocessing.md](06-ml-pipeline-pitfalls/05-preprocessing.md) | Fit on everything, evaluated on part of it — the holdout was spent before training. | scaling, imputation, feature-selection, leakage |
| 6 | FEATURE ENGINEERING | Feature Engineering | [06-ml-pipeline-pitfalls/06-feature-engineering.md](06-ml-pipeline-pitfalls/06-feature-engineering.md) | The feature is computable now and was not computable at prediction time. | look-ahead, windowing, point-in-time |
| 7 | LABELS & GROUND TRUTH | Labels & Ground Truth | [06-ml-pipeline-pitfalls/07-labels-and-ground-truth.md](06-ml-pipeline-pitfalls/07-labels-and-ground-truth.md) | The target records a decision, not the truth, so everything calibrates to a bent ruler. | label-noise, censoring, target-drift |
| 8 | LEAKAGE | Leakage | [06-ml-pipeline-pitfalls/08-leakage.md](06-ml-pipeline-pitfalls/08-leakage.md) | The only failure mode that looks like success: the answer was already in the inputs. | future-info, target-leakage, contamination |
| 9 | EVALUATION | Evaluation | [06-ml-pipeline-pitfalls/09-evaluation.md](06-ml-pipeline-pitfalls/09-evaluation.md) | The number is computed correctly and answers a question nobody asked. | metrics, holdout, threshold, overfitting |
| 10 | DEPLOYMENT & SERVING | Deployment & Serving | [06-ml-pipeline-pitfalls/10-deployment-and-serving.md](06-ml-pipeline-pitfalls/10-deployment-and-serving.md) | Training and serving are two implementations of one feature, and they disagree. | serving-skew, staleness, feature-store |
| 11 | MISC | Misc | [06-ml-pipeline-pitfalls/11-misc.md](06-ml-pipeline-pitfalls/11-misc.md) | Staging area for pitfalls whose category does not have enough members yet. | fairness, selection, uncategorized |

## Regeneration instructions

- **Template:** nav-grid style (see `docs/statsml/ui-templates/02-nav-grid`). Single page: h1, `.subtitle`, then one `.nav-grid` of `.nav-card` anchors. No callout on this page.
- **Layout:** `.nav-grid` is CSS grid, `repeat(4, 1fr)`, 16px gap, margin-top 15px; responsive: 3 columns below 1400px, 2 columns below 1100px, 1 column below 600px.
- **Links:** the table links to `.md` versions for markdown navigation; in the HTML each card's `href` is the same path with an `.html` extension.
- **Card structure:** `<a class="nav-card" href="...">` containing `<div class="card-num">CATEGORY</div>`, `<h3>N. Title</h3>` (unpadded index matching the category file index), `<p>description</p>`, then `<div class="topics">` of `<span class="topic-tag">` per topic.
- **Category label colors:** inline script maps `.card-num` text through a `categoryColors` object and sets `el.style.color`: DATA INGESTION & ETL `#16a085`, SCHEMA & SEMANTICS `#1a5276`, DATA QUALITY `#795548`, DISTRIBUTION SHIFT `#8e44ad`, PREPROCESSING `#2980b9`, FEATURE ENGINEERING `#e67e22`, LABELS & GROUND TRUTH `#f39c12`, LEAKAGE `#e74c3c`, EVALUATION `#27ae60`, DEPLOYMENT & SERVING `#d35400`, MISC `#7f8c8d`. Base `.card-num` style is `#2980b9`, 0.75em, bold.
- **Card style:** background `#ffffff`, border `1px solid #d8d8d8`, radius 10px, padding 20px, shadow `0 2px 4px rgba(0,0,0,0.05)`; hover: border `#2980b9`, `translateY(-2px)`. h3 `#1a3a4a` 1em; description 0.85em `#555`. Topic tags: background `#f0f0f0`, border `1px solid #ccc`, radius 4px, padding 2px 6px, 0.7em `#666`, flex-wrapped with 4px gap.
- **Page style:** body system sans-serif, background `#f5f5f0`, text `#2a2a2a`, padding 40px, line-height 1.6; h1 1.8em `#2980b9`; subtitle `#666` 1.05em. No nav bar, no back/home links.
