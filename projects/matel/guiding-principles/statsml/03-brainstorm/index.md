# Reference — Background Knowledge & Catalogs

**Page type:** grid page (auto-fit card navigation grid, min 300px columns)
**HTML title tag:** Reference — Background Knowledge & Catalogs

**Subtitle:** Common knowledge cataloged: statistical foundations, data profiling background, pitfalls, and measurement patterns.

## Cards

Each card links to a detail page in this folder. The card shows a blue uppercase category label (`.card-num`), a numbered title, a description, and a row of small topic tags.

| # | Category | Title | Link | Description | Topics |
|---|----------|-------|------|-------------|--------|
| 1 | PROFILING | Feature Type Detection | [01-feature-type-detection.md](01-feature-type-detection.md) | How to determine if a feature is categorical, discrete numeric, or continuous — without assumptions. | unique value ratio, gap analysis, type inference |
| 2 | PROFILING | Value Existence Mapping | [02-value-existence-mapping.md](02-value-existence-mapping.md) | Finding where data actually lives on the number line. Clusters, gaps, dense regions, isolated points. | density clusters, gap detection, sparse regions |
| 3 | PROFILING | Shape Detection | [03-shape-detection.md](03-shape-detection.md) | Identifying actual distribution shape: hills, twin peaks, multiple peaks, flat, spike-and-tail. | peak finding, valley depth, symmetry |
| 4 | PROFILING | Sample Sufficiency | [04-sample-sufficiency.md](04-sample-sufficiency.md) | Minimum data points per region to make statistical claims. Base rate correction, confidence bounds. | minimum n, base rate, power |
| 5 | PROFILING | Separation Scenarios | [05-separation-scenarios.md](05-separation-scenarios.md) | How pos/neg class distributions relate. When to use a feature, parts of it, or reject it. | full separation, partial, bucket purity |
| 6 | PROFILING | Bucket Strategy & Decision | [06-bucket-strategy-and-decision.md](06-bucket-strategy-and-decision.md) | Adaptive bucketing that follows data density and shape. Final decision criteria for feature selection. | adaptive bins, purity threshold, stability |
| 7 | PROFILING | Temporal Dynamics | [07-temporal-dynamics.md](07-temporal-dynamics.md) | Shape drift, seasonality, concept drift, changepoints, windowed profiling strategies. | drift, seasonality, changepoints |

## Regeneration instructions

To rebuild the HTML from this spec:

- **Template:** nav-grid style (see `docs/statsml/ui-templates/02-nav-grid`). Single page: h1, `.subtitle` paragraph, then one `.nav-grid` of `.nav-card` anchors. No section headers used on this page (a `.section-header` style exists: `#1a5276`, 1.2em, `2px solid #d0d0d0` bottom border).
- **Layout:** `.nav-grid` is CSS grid, `repeat(auto-fit, minmax(300px, 1fr))`, 16px gap.
- **Links:** the table above links to the `.md` versions for navigation in markdown; in the regenerated HTML, each card's `href` is the same path with an `.html` extension instead.
- **Card structure:** `<a class="nav-card" href="...">` containing `<div class="card-num">CATEGORY</div>` (all cards use PROFILING here), `<h3>N. Title</h3>` (unpadded index number matching the file index), `<p>description</p>`, then `<div class="topics">` with one `<span class="topic-tag">` per topic listed in the Topics column.
- **Card style:** background `#ffffff`, border `1px solid #d8d8d8`, radius 10px, padding 20px, box-shadow `0 2px 4px rgba(0,0,0,0.05)`; hover: border `#2980b9`, `translateY(-2px)`. `.card-num` `#2980b9` 0.75em bold; h3 `#1a3a4a` 1em; description `#555` 0.85em. Topic tags: background `#f0f0f0`, border `1px solid #ccc`, radius 4px, padding 2px 6px, 0.7em `#666`, laid out in a flex-wrap row with 4px gap.
- **Page style:** body `-apple-system` sans-serif, background `#f5f5f0`, text `#2a2a2a`, padding 40px, line-height 1.6; h1 1.8em `#2980b9`; subtitle `#666` 1.05em. No nav bar, no back/home links.
- **Palette:** primary blue `#1a5276`, secondary blue `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`. No canvases on this page; canvases elsewhere use `window.devicePixelRatio` scaling.
