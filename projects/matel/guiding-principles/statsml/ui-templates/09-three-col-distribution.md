# Template 09 — Three-Col Distribution Detail

**Use for:** distribution-shape detail pages: one concept per row with text plus TWO charts — the raw distribution and a derived insight view (ECDF, cumulative share, comparison).

**Source pages:** all of `real-world-distributions/` (the folder's CLAUDE.md marks this layout "Do Not Convert" — it is first-class). A 3-col row variant also appears in ~56 tutorials pages (`td.text-col3`/`td.viz-col3`).

## Anatomy

- Centered `h1` + centered `.subtitle`.
- One `<table class="obj-table">` per concept, single `<tr>` with three cells:
  - **text 38%** — `.pitfall-label` (uppercase shape nickname, colored per concept), `<h3>` title (1.0em, unnumbered), 2-3 sentence paragraph, one-line labeled bullets.
  - **primary canvas 31%** — the distribution itself, canvas 420×340.
  - **insight canvas 31%** — derived view (ECDF, zoom, overlay), canvas 400×340.
- Cell borders `1px solid #2980b9` (blue, not gray); no zebra striping.

## Type & color

- Font: Type A apple-em stack, but body sized in px (body 14px) — the one px-sized family; h1 centered default size.
- Background `#f9f9f9` (the only non-white detail family); text `#333`; accent `#1a5276` (themeable per THEMES.md).

## Viz / js

- Shared: `js/base.js` + `js/three-col-dist.js` (`drawHistogram` with overlays/density/SE band, `drawBarChart`; seeded `mulberry32`, `randn`, `randExp` from base).
- Standard per chart: density line `#1e8449` + 95% SE band; legend swatches; L-shaped gray axes; margins top 40 / right 20 / bottom 45 / left 50.
- Charts are deterministic (seeded RNG, never Math.random).
