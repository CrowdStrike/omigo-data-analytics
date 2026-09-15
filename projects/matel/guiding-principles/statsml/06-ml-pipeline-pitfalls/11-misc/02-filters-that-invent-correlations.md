# Filters That Invent Correlations (Berkson's Paradox)

**Page type:** grid page (nav-card grid, auto-fit columns min 300px, per-card category labels and topic tags)
**HTML title tag:** Filters That Invent Correlations — ML Pipeline Pitfalls

**Subtitle:** Every row your pipeline drops changes the relationships between the rows that stay — a `WHERE` clause, an inner join, or an approval gate can manufacture a correlation out of two things that have nothing to do with each other.

## Callout (philosophy box)

**Why this matters:** Most pipeline bugs corrupt a value. This one corrupts a *relationship*, and every value in the table stays correct while it happens. If rows enter your dataset only when they clear a bar that two features both contribute to, those two features will look like a trade-off inside the table no matter how independent they are outside it. Nothing is missing, nothing is null, no test fires — the correlation matrix is just wrong. The fix is never in the modeling code; it is in knowing what the door was.

## Cards

| # | Category | Title | Link | Description | Topics |
|---|----------|-------|------|-------------|--------|
| 1 | INTRO | The Door Makes the Trade-Off | [02-filters-that-invent-correlations/01-the-door-makes-the-trade-off.md](02-filters-that-invent-correlations/01-the-door-makes-the-trade-off.md) | Start here: why "good at either one" turns two unrelated skills into rivals, using pro basketball rosters. | selection-bias, collider, intuition |
| 2 | QUERY | Filters and Joins That Cut a Corner | [02-filters-that-invent-correlations/02-filters-and-joins-that-cut-a-corner.md](02-filters-that-invent-correlations/02-filters-and-joins-that-cut-a-corner.md) | A `WHERE` clause, an inner join, and a drop-nulls step are all entry rules — and each bends the correlation matrix. | WHERE-clause, inner-join, drop-nulls |
| 3 | LABELS | Training Only on Who Got Approved | [02-filters-that-invent-correlations/03-training-only-on-who-got-approved.md](02-filters-that-invent-correlations/03-training-only-on-who-got-approved.md) | Labels exist only for applicants a gate let through, so the model learns the gate's shape, not the world's. | approved-only, reject-inference, labels |
| 4 | FEEDBACK | The Model Filters Its Own Next Dataset | [02-filters-that-invent-correlations/04-the-model-filters-its-own-next-dataset.md](02-filters-that-invent-correlations/04-the-model-filters-its-own-next-dataset.md) | Each retrain sees only rows the last version let through, and the feature it trusted most goes flat. | feedback-loop, retraining, exploration |

## Regeneration instructions

To rebuild the HTML from this spec:

- **Template:** nav-grid style (see `docs/statsml/ui-templates/02-nav-grid`). Single page: h1, `.subtitle`, one `.philosophy` callout, one `.nav-grid` of `.nav-card` anchors.
- **Layout:** `.nav-grid` is CSS grid, `repeat(auto-fit, minmax(300px, 1fr))`, 16px gap, margin-top 15px.
- **Links:** the table above links to `.md` versions; in the regenerated HTML each card's `href` is the same path with an `.html` extension.
- **Card structure:** `<a class="nav-card" href="...">` containing `<div class="card-num">CATEGORY</div>`, `<h3>N. Title</h3>` (unpadded index matching the file index), `<p>description</p>`, then `<div class="topics">` of `<span class="topic-tag">` per topic.
- **Category label colors:** inline script maps `.card-num` text through a `categoryColors` object — INTRO `#2980b9`, QUERY `#16a085`, LABELS `#f39c12`, FEEDBACK `#8e44ad`.
- **Card style:** background `#ffffff`, border `1px solid #d8d8d8`, radius 10px, padding 20px, shadow `0 2px 4px rgba(0,0,0,0.05)`; hover: border `#2980b9`, `translateY(-2px)`. Card-num 0.75em bold; h3 `#1a3a4a` 1em; description 0.85em `#555`. Topic tags: background `#f0f0f0`, border `1px solid #ccc`, radius 4px, padding 2px 6px, 0.7em `#666`, flex-wrapped 4px gap.
- **Callout style:** `.philosophy` — background `#f0f4f8`, left border `4px solid #2980b9`, padding 12px 16px, 0.9em, text `#222`.
- **Page style:** body system sans-serif, background `#f5f5f0`, text `#2a2a2a`, padding 40px, line-height 1.6; h1 1.8em `#2980b9`; subtitle `#666` 1.05em. No nav bar, no back/home links.
