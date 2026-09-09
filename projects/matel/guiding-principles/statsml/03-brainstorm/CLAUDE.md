# 03-brainstorm/

Feature-profiling deep-dive pages (feature types, value mapping, shape detection, sample sufficiency, separation, buckets, temporal dynamics) + a nav-grid index. Chart-heavy: 6-13 canvases per page.

## Format (migrated 2026-08-29)

- Three-file pages: `NN-topic.txt.md` + `NN-topic.viz.md` + html. See `../ui-templates/FORMAT.md`. Transition state: `NN-topic.html` = fenced original (comparison), `NN-topic.v2.html` = ships (shared js).
- **Template:** 04-two-col-catalog-badges, brainstorm profiling variant — but per-page drift is real: some pages have th-header rows and badge chips (01), others plain obj-table + toc (02, 04, 07). Each page's viz.md header records its actual variant; trust the viz.md over the folder default.
- **Theme:** house-blue. Chart text on 06/07 is deliberately 17px — preserve.
- **Charts:** local `setup(id)` returns `{ctx, W/w, H/h}` (object convention) and stays fenced as LIB in BOTH html variants — it intentionally shadows base.js's bare-ctx `setup` alias in v2. v2 pages load `../ui-templates/js/base.js` and register all charts via `registerChart` (adds resize re-render the originals lacked). Seeded Lehmer `rng()` where present lives inside its chart's VIZ fence, not LIB.
- Index grid: flat mode, 7 cards, card numbering matches file numbering.

## Folder-cleanup queue (do with user review, before deleting originals + old .md)

- `01-feature-type-detection.html` footer links "Step 2" to `02-value-existence-mapping.html` — href now resolves, but it still violates the no-cross-reference-links rule; likely drop the link, keep text.
- `04-...` chart c5a plots 4 of the 5 shapes its text lists (Tail missing) — text/viz mismatch to resolve.
- `07-...` sec-5 caption says "collapsed → slow recovery" but the code draws a monotonic decline — caption/code mismatch.
- Dead `.nav` CSS in originals (global TODO); 03's v2 already dropped it, other v2s still carry it.
- index.html h1 uses `#2980b9` (secondary) — decide whether to align to `#1a5276`.
