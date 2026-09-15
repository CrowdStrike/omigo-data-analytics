# Product Analytics

**Page type:** grid page (tutorials category grid: single flat 4-column nav-grid of cards with topic tags)
**HTML title tag:** Product Analytics

**Subtitle:** How teams turn raw usage data into decisions — what happened, which users stay, and which single number to push without breaking everything else.

## Cards

Each card links to a topic page under `product-analytics/`. The card shows a colored uppercase subcategory label (`.card-num`), a numbered title, a one-line description, and 2-4 topic tag pills. All cards sit in one flat `.nav-grid`; the colored labels carry the grouping.

| # | Category | Title | Link | Description | Topic tags |
|---|----------|-------|------|-------------|------------|
| 1 | FOUNDATIONS | Descriptive, Predictive, Prescriptive | [56-product-analytics/01-descriptive-predictive-prescriptive.md](56-product-analytics/01-descriptive-predictive-prescriptive.md) | Analytics climbs a three-rung ladder — what happened, what will happen, what to do about it — and every rung stands on the one below. | three rungs, what happened, what to do |
| 2 | FOUNDATIONS | What Makes a Good Metric | [56-product-analytics/02-what-makes-a-good-metric.md](56-product-analytics/02-what-makes-a-good-metric.md) | A good metric passes four tests — measurable, sensitive, hard to game, aligned — and most dashboard numbers fail at least one. | four tests, gaming, alignment |
| 3 | USER JOURNEYS | Funnels & Conversion | [56-product-analytics/03-funnels-and-conversion.md](56-product-analytics/03-funnels-and-conversion.md) | A funnel counts how many users survive each step toward a goal — and "conversion" means nothing until you say which step is the denominator. | step drop-off, denominator, purchase path |
| 4 | USER JOURNEYS | Retention & Cohorts | [56-product-analytics/04-retention-and-cohorts.md](56-product-analytics/04-retention-and-cohorts.md) | Group users by when they signed up and track each group separately — one blended "% active" number can rise while every group quietly gets worse. | signup cohorts, blended average, churn |
| 5 | METRIC DESIGN | North Star & Guardrail Metrics | [56-product-analytics/05-north-star-and-guardrail-metrics.md](56-product-analytics/05-north-star-and-guardrail-metrics.md) | Pick one metric that captures delivered value and push it hard — while a short list of guardrail metrics makes sure the push never hurts the user. | one metric, guardrails, delivered value |
| 6 | METRIC DESIGN | Ratio Metrics | [56-product-analytics/06-ratio-metrics.md](56-product-analytics/06-ratio-metrics.md) | A blended ratio like overall conversion rate can fall even when every segment improves — because the mix of traffic shifted, not the performance. | mix shift, segments, simpson's paradox |

## Regeneration instructions

To rebuild the HTML from this spec:

- **Template:** tutorials category grid. Single page: h1, `.subtitle` paragraph, then one flat `.nav-grid` of `.nav-card` anchors (no h2 section headings).
- **Layout:** `.nav-grid` is CSS grid, `repeat(4, 1fr)`, 16px gap, 15px top margin; responsive: 3 columns below 1400px, 2 below 1100px, 1 below 600px.
- **Links:** the tables above link to the `.md` versions for markdown navigation; in the regenerated HTML, each card's `href` is the same path with an `.html` extension instead.
- **Card structure:** `<a class="nav-card" href="...">` containing `<div class="card-num">SUBCATEGORY LABEL</div>`, `<h3>N. Topic Title</h3>` (unpadded index number matching the 2-digit zero-padded file index), `<p>description</p>`, then `<div class="topics">` of `<span class="topic-tag">` pills.
- **Category label colors:** applied by a small script mapping `.card-num` text to color — "FOUNDATIONS" `#2980b9`, "USER JOURNEYS" `#27ae60`, "METRIC DESIGN" `#8e44ad`; the CSS default for `.card-num` is `#2980b9`, 0.75em bold, 4px bottom margin.
- **Card style:** background `#ffffff`, border `1px solid #d8d8d8`, radius 10px, padding 20px, shadow `0 2px 4px rgba(0,0,0,0.05)`, transition on border-color/transform; hover: border `#2980b9`, `translateY(-2px)`. h3 `#1a3a4a` 1em with 6px bottom margin; description `#555` 0.85em. Topic tags: background `#f0f0f0`, border `1px solid #ccc`, radius 4px, padding 2px 6px, 0.7em `#666`, in a flex-wrap row with 4px gap and 8px top margin.
- **Page style:** body system sans-serif, background `#f5f5f0`, text `#2a2a2a`, padding 40px, line-height 1.6; universal `* { margin:0; padding:0; box-sizing:border-box }` reset; h1 1.8em `#2980b9` with 10px bottom margin; subtitle `#666` 1.05em with 30px bottom margin. No nav bar, no back/home links.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22` (page accents here use `#2980b9`, `#27ae60`, `#8e44ad`).
- **Canvases:** none on this page; any canvases elsewhere in this series use `window.devicePixelRatio` scaling.
