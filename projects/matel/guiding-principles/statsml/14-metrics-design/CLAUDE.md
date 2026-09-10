# Metrics Design

Good/bad metrics, vanity metrics, reporting patterns, granularity, and frequency.

## Page Format (migrated)

Pages follow the three-file model — `NN-topic.txt.md` (verbatim text), `NN-topic.viz.md` (per-canvas regen briefs), `NN-topic.v2.html` (fenced page on shared `ui-templates/js/base.js`). Spec: `../ui-templates/FORMAT.md`.

- **Template default:** 05-two-col-catalog-clean (per-row `.obj-table`, no thead, no badges; 40/60 text/viz split)
- Originals (`NN-topic.html`, old `NN-topic.md`) are kept untouched until the user's bulk review — never edit or delete them; all edits go to the three-file set per FORMAT.md's edit workflows.
