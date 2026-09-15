# Labels & Ground Truth

**Page type:** grid page (card navigation grid, 4 columns, one philosophy callout, cards with category labels and topic tags)
**HTML title tag:** Labels & Ground Truth — ML Pipeline Pitfalls

**Subtitle:** The target is a recorded decision, not the truth. Noise, censoring, and definition drift move the thing being measured.

## Callout (philosophy box)

**Why this matters:** Everything downstream is calibrated against the label, so a bent label bends the whole enterprise while every metric stays internally consistent. “Not yet” gets recorded as “no” when the outcome window is still open; the definition of the target shifts between quarters and the two halves of the training set answer different questions; only positives were ever written down. These are not modeling errors. The model faithfully learns a target that does not mean what its name says.

## Cards

Each card links to a detail page under `07-labels-and-ground-truth/`. The card shows a colored uppercase category label, a numbered title, a one-line description, and a row of small topic tags.

| # | Category | Title | Link | Description | Topic tags |
|---|----------|-------|------|-------------|------------|
| 1 | LABELS | Label Noise (Incorrect Ground Truth) | [07-labels-and-ground-truth/01-label-noise-incorrect-ground-truth.md](07-labels-and-ground-truth/01-label-noise-incorrect-ground-truth.md) | The target variable itself is wrong for 5-20% of records | labels, noise, annotation |
| 2 | LABELS | Target Definition Drift | [07-labels-and-ground-truth/02-target-definition-drift.md](07-labels-and-ground-truth/02-target-definition-drift.md) | What "positive" means changes over time, invalidating the label | target, drift, versioning |
| 3 | LABELS | Positive-Only Data Collection | [07-labels-and-ground-truth/03-positive-only-data-collection.md](07-labels-and-ground-truth/03-positive-only-data-collection.md) | No deliberate negative class strategy — "negatives" contain undiagnosed positives | negative-class, collection, contamination |
| 4 | LABELS | Experiment Provenance Ignored | [07-labels-and-ground-truth/04-experiment-provenance-ignored.md](07-labels-and-ground-truth/04-experiment-provenance-ignored.md) | A/B test data mixed into training without tracking which experiment generated each row | A/B-test, provenance, confounding |
| 5 | LABELS | Label Maturity (Censoring Window) | [07-labels-and-ground-truth/05-label-maturity-censoring-window.md](07-labels-and-ground-truth/05-label-maturity-censoring-window.md) | The outcome has not had time to happen yet, so "not yet" is recorded as "no" | censoring, maturity, survival-analysis |

## Regeneration instructions

- **Template:** nav-grid style (see `docs/statsml/ui-templates/02-nav-grid`). Single page: h1, `.subtitle`, a `.philosophy` callout, then one `.nav-grid` of `.nav-card` anchors.
- **Layout:** `.nav-grid` is CSS grid, `repeat(4, 1fr)`, 16px gap, margin-top 15px; responsive: 3 columns below 1400px, 2 columns below 1100px, 1 column below 600px.
- **Links:** the table links to `.md` versions for markdown navigation; in the HTML each card's `href` is the same path with an `.html` extension.
- **Card structure:** `<a class="nav-card" href="...">` containing `<div class="card-num">CATEGORY</div>`, `<h3>N. Title</h3>` (unpadded index matching the file index), `<p>description</p>`, then `<div class="topics">` of `<span class="topic-tag">` per topic.
- **Category label colors:** inline script maps `.card-num` text through a `categoryColors` object and sets `el.style.color`: LABELS `#f39c12`. Base `.card-num` style is `#2980b9`, 0.75em, bold.
- **Page style:** body system sans-serif, background `#f5f5f0`, text `#2a2a2a`, padding 40px, line-height 1.6; h1 1.8em `#2980b9`; subtitle `#666` 1.05em. Philosophy callout background `#f0f4f8`, left border `4px solid #2980b9`. No nav bar, no back/home links.
