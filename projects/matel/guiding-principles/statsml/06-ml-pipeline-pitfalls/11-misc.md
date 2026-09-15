# Misc

**Page type:** grid page (card navigation grid, 4 columns, one philosophy callout, cards with category labels and topic tags)
**HTML title tag:** Misc — ML Pipeline Pitfalls

**Subtitle:** Pitfalls that do not yet belong to a category. Held here deliberately until enough related pages exist to justify one.

## Callout (philosophy box)

**Why this matters:** This page is a staging area, not a theme. Each entry is a real pitfall whose natural category currently has too few members to be worth a grid of its own — and a one-card grid is worse than no grid. The rule is to leave them visible here rather than force them into a category they do not fit, and to promote a group out into its own page once it reaches roughly four related entries.

## Cards

Each card links to a detail page under `11-misc/`. The card shows a colored uppercase category label, a numbered title, a one-line description, and a row of small topic tags.

| # | Category | Title | Link | Description | Topic tags |
|---|----------|-------|------|-------------|------------|
| 1 | FAIRNESS | Proxy Variables (Hidden Sensitive Attributes) | [11-misc/01-proxy-variables-hidden-sensitive-attributes.md](11-misc/01-proxy-variables-hidden-sensitive-attributes.md) | Model learns protected attributes through correlated features | fairness, proxy, discrimination |
| 2 | SELECTION | Filters That Invent Correlations (Berkson's Paradox) | [11-misc/02-filters-that-invent-correlations.md](11-misc/02-filters-that-invent-correlations.md) | Sub-grid: how row filters, joins, and approval gates manufacture relationships that do not exist | selection, collider, fake-correlation |

## Regeneration instructions

- **Template:** nav-grid style (see `docs/statsml/ui-templates/02-nav-grid`). Single page: h1, `.subtitle`, a `.philosophy` callout, then one `.nav-grid` of `.nav-card` anchors.
- **Layout:** `.nav-grid` is CSS grid, `repeat(4, 1fr)`, 16px gap, margin-top 15px; responsive: 3 columns below 1400px, 2 columns below 1100px, 1 column below 600px.
- **Links:** the table links to `.md` versions for markdown navigation; in the HTML each card's `href` is the same path with an `.html` extension.
- **Card structure:** `<a class="nav-card" href="...">` containing `<div class="card-num">CATEGORY</div>`, `<h3>N. Title</h3>` (unpadded index matching the file index), `<p>description</p>`, then `<div class="topics">` of `<span class="topic-tag">` per topic.
- **Category label colors:** inline script maps `.card-num` text through a `categoryColors` object and sets `el.style.color`: FAIRNESS `#c0392b`, SELECTION `#8e44ad`. Base `.card-num` style is `#2980b9`, 0.75em, bold.
- **Page style:** body system sans-serif, background `#f5f5f0`, text `#2a2a2a`, padding 40px, line-height 1.6; h1 1.8em `#2980b9`; subtitle `#666` 1.05em. Philosophy callout background `#f0f4f8`, left border `4px solid #2980b9`. No nav bar, no back/home links.
