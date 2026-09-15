# ML & Data Pipeline Pitfalls — Folder Instructions

## Structure

This folder is two levels deep, by design:

- `../06-ml-pipeline-pitfalls.html` — hub grid, one card per pipeline stage (11 cards).
- `NN-<stage>.html` — a category grid page per stage, one card per pitfall.
- `NN-<stage>/NN-<pitfall>.html` — the pitfall detail pages.

Card index numbers restart at 1 inside each category and must match the numeric
prefix of the file they link to. Filenames are derived from the card title, so a
retitled card means a renamed file (and a renamed sibling `.md`).

One detail page is itself a grid with children of its own —
`11-misc/02-filters-that-invent-correlations` — so nesting below a detail page is
allowed where a single pitfall needs several pages.

## Categories

Order follows the pipeline: ingestion first, serving last, `11-misc` always last.

| # | Category |
|---|----------|
| 1 | Data Ingestion & ETL |
| 2 | Schema & Semantics |
| 3 | Data Quality |
| 4 | Distribution Shift |
| 5 | Preprocessing |
| 6 | Feature Engineering |
| 7 | Labels & Ground Truth |
| 8 | Leakage |
| 9 | Evaluation |
| 10 | Deployment & Serving |
| 11 | Misc |

## Critical: check whether `11-misc` has earned a new category

`11-misc` is a staging area, not a theme. It holds pitfalls whose natural category
has too few members to justify a grid page of its own — a one-card grid is worse
than no grid, so these wait here instead.

**Every time a card is added to `11-misc`, re-read the whole page and ask whether
its contents now cluster.** When roughly four entries share a theme, promote them:
create the category grid page, move the detail pages into its folder, renumber
them from 1, and add a card to the hub grid.

Do not do the reverse — do not force a pitfall into an ill-fitting category just
to keep `11-misc` short. A wrong category is a navigation defect; a page sitting
in `11-misc` is only an unfinished one.

Currently waiting there, with the theme each would seed:

- **Proxy Variables (Hidden Sensitive Attributes)** — a fairness/disparate-impact group.
- **Filters That Invent Correlations (Berkson's Paradox)** — a selection-bias group.
