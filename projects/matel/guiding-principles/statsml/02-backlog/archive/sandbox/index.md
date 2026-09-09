# Sandbox — Experiments & Prototypes

**Page type:** grid page (sectioned card navigation grid, auto-fit columns min 300px)
**HTML title tag:** Sandbox — Experiments & Prototypes

**Subtitle:** Concepts being tried on real data. Pipeline runs, CNN shape classifiers, encoding experiments.

## Section: Datasets & Pipeline Execution

Each card shows a green uppercase category label (`.card-num`), an unnumbered title, a one-sentence description, and a row of topic tag pills.

| Category | Title | Link | Description | Topics |
|----------|-------|------|-------------|--------|
| PIPELINE TRACE | Architecture Examples — Real Data Through Pipeline | [01-architecture-examples.md](01-architecture-examples.md) | Ames Housing and Adult Census features processed through the full pipeline with SE bands. | Ames, Adult, pipeline trace |
| DATASET | Ames Housing Dataset | [03-ames-housing-data.md](03-ames-housing-data.md) | Zero-inflated, right-skew, gap/valley splitting candidates. | housing, numeric, validation |
| EXECUTION | Ames Housing — Full Pipeline Execution | [04-ames-pipeline-execution.md](04-ames-pipeline-execution.md) | Every numeric feature processed: type scores, CNN shape, splitting, test selection. | features, CNN, full trace |
| DATASET | Adult Census Dataset | [05-adult-census-data.md](05-adult-census-data.md) | Extreme zero-inflation, spike distributions, binary target. | census, numeric, spike |
| EXECUTION | Adult Census — Full Pipeline Execution | [06-adult-pipeline-execution.md](06-adult-pipeline-execution.md) | All features processed: mixed types, spike, bimodal, zero-inflated handling. | features, CNN, mixed types |

## Section: CNN Classifier Experiments

| Category | Title | Link | Description | Topics |
|----------|-------|------|-------------|--------|
| CNN RESULTS | CNN Results — What Works and Where It Struggles | [02-cnn-results.md](02-cnn-results.md) | Visual analysis: rendered silhouettes with prediction bars, confusion pairs, confidence calibration. | top-2, confusion pairs, boundary cases |
| CLASSIFIER | Shape Classifier — Combined Report | [07-shape-classifier-report.md](07-shape-classifier-report.md) | 11-class generative model. 96.6% mean recall. Real data validation on Ames + Adult. | 11 classes, multi-label, real data |
| ANALYSIS | Input Encoding Comparison | [09-input-encoding-comparison.md](09-input-encoding-comparison.md) | Histogram-based vs raw value input for CNN shape classification. | encoding, histogram, raw input |

## Regeneration instructions

- **Template:** nav-grid style (see `docs/statsml/ui-templates/02-nav-grid`) with section headers. Page: h1, `.subtitle` paragraph, then two `.section-header` h2 elements each followed by a `.nav-grid` of `.nav-card` anchors. No callout box.
- **Layout:** `.nav-grid` is CSS grid `repeat(auto-fit, minmax(300px, 1fr))`, 16px gap; `.section-header` `#1a5276`, 1.2em, margin `35px 0 15px 0`, `2px solid #d0d0d0` bottom border, 8px padding-bottom.
- **Links:** the tables above link to `.md` siblings; in the regenerated HTML each card's `href` is the same relative path with an `.html` extension (e.g. `01-architecture-examples.html`).
- **Card structure:** `<a class="nav-card" href="...">` containing `<div class="card-num">CATEGORY</div>`, `<h3>Title</h3>` (unnumbered), `<p>description</p>`, and `<div class="topics">` with one `<span class="topic-tag">` per topic.
- **Card style:** background `#ffffff`, border `1px solid #d8d8d8`, radius 10px, padding 20px, shadow `0 2px 4px rgba(0,0,0,0.05)`; hover: border `#27ae60`, `translateY(-2px)`. `.card-num` green `#27ae60`, 0.75em bold; h3 `#1a3a4a` 1em; p `#555` 0.85em. Topic tags: background `#f0f0f0`, border `1px solid #ccc`, radius 4px, padding 2px 6px, 0.7em, `#666`, flex-wrap row with 4px gap.
- **Page style:** body -apple-system/BlinkMacSystemFont/'Segoe UI'/Roboto sans-serif, background `#f5f5f0`, text `#2a2a2a`, padding 40px, line-height 1.6; h1 green `#27ae60` 1.8em; subtitle `#666` 1.05em. No nav bar, no back/home links.
- **Palette:** #1a5276 primary blue, #27ae60 green, #e74c3c red, #e67e22 orange. No canvases on this page (site canvases use `window.devicePixelRatio` scaling).
