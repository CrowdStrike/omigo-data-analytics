# Brainstorm — Design Explorations & Profiling Pipeline

**Page type:** grid page (nav-card grid, 4 columns, cards with topic tags)
**HTML title tag:** Brainstorm — Design Explorations & Profiling Pipeline

**Subtitle:** Design brainstorms on feature profiling, shape detection, bucket strategy, and measurement patterns.

## Cards

Each card links to a detail page under `brainstorm/`. Each card shows an uppercase category label (colored green via a small script mapping PROFILING → `#27ae60`), a numbered title, a one-to-two sentence description, and a row of topic tags.

| # | Category | Title | Link | Description | Topics |
|---|----------|-------|------|-------------|--------|
| 1 | PROFILING | Feature Type Detection | [03-brainstorm/01-feature-type-detection.md](03-brainstorm/01-feature-type-detection.md) | How to determine if a feature is categorical, discrete numeric, or continuous — without assumptions. | unique value ratio, gap analysis, type inference |
| 2 | PROFILING | Value Existence Mapping | [03-brainstorm/02-value-existence-mapping.md](03-brainstorm/02-value-existence-mapping.md) | Finding where data actually lives on the number line. Clusters, gaps, dense regions, isolated points. | density clusters, gap detection, sparse regions |
| 3 | PROFILING | Shape Detection | [03-brainstorm/03-shape-detection.md](03-brainstorm/03-shape-detection.md) | Identifying actual distribution shape: hills, twin peaks, multiple peaks, flat, spike-and-tail. | peak finding, valley depth, symmetry |
| 4 | PROFILING | Sample Sufficiency | [03-brainstorm/04-sample-sufficiency.md](03-brainstorm/04-sample-sufficiency.md) | Minimum data points per region to make statistical claims. Base rate correction, confidence bounds. | minimum n, base rate, power |
| 5 | PROFILING | Separation Scenarios | [03-brainstorm/05-separation-scenarios.md](03-brainstorm/05-separation-scenarios.md) | How pos/neg class distributions relate. When to use a feature, parts of it, or reject it. | full separation, partial, bucket purity |
| 6 | PROFILING | Bucket Strategy & Decision | [03-brainstorm/06-bucket-strategy-and-decision.md](03-brainstorm/06-bucket-strategy-and-decision.md) | Adaptive bucketing that follows data density and shape. Final decision criteria for feature selection. | adaptive bins, purity threshold, stability |
| 7 | PROFILING | Temporal Dynamics | [03-brainstorm/07-temporal-dynamics.md](03-brainstorm/07-temporal-dynamics.md) | Shape drift, seasonality, concept drift, changepoints, windowed profiling strategies. | drift, seasonality, changepoints |

## Regeneration instructions

To rebuild the HTML from this spec:

- **Template:** nav-grid style (see `docs/statsml/ui-templates/02-nav-grid`). Single page: h1, `.subtitle` paragraph, one `.nav-grid` of `.nav-card` anchors. (A `.section-header` CSS rule exists — `#1a5276`, 1.2em, 2px solid `#d0d0d0` bottom border — but no section header element is used on the page.)
- **Layout:** `.nav-grid` is CSS grid, `repeat(4, 1fr)`, 16px gap, margin-top 15px; responsive: 3 columns below 1400px, 2 columns below 1100px, 1 column below 600px.
- **Links:** the table above links to `.md` versions for navigation in markdown; in the regenerated HTML, each card's `href` is the same path with an `.html` extension.
- **Card structure:** `<a class="nav-card" href="...">` containing `<div class="card-num">PROFILING</div>` (no inline color; see script note), `<h3>N. Title</h3>` (unpadded index number matching the file index), `<p>description</p>`, and `<div class="topics">` with one `<span class="topic-tag">` per topic.
- **Category color script:** a small inline `<script>` at the end of `<body>` defines `categoryColors = { "PROFILING": "#27ae60" }` and iterates over all `.card-num` elements, setting `style.color` when the trimmed text matches a key. Default `.card-num` CSS color is `#2980b9`, 0.75em bold.
- **Card style:** background `#ffffff`, border `1px solid #d8d8d8`, radius 10px, padding 20px, box-shadow `0 2px 4px rgba(0,0,0,0.05)`; hover: border `#2980b9`, `translateY(-2px)`. h3 `#1a3a4a` 1em; description `#555` 0.85em. Topic tags: background `#f0f0f0`, border `1px solid #ccc`, radius 4px, padding 2px 6px, 0.7em, `#666`.
- **Page style:** body system sans-serif, background `#f5f5f0`, text `#2a2a2a`, padding 40px, line-height 1.6; h1 1.8em `#2980b9`; subtitle `#666` 1.05em. No nav bar, no back/home links.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Canvases:** none on this page; site-wide canvases use `window.devicePixelRatio` scaling.
