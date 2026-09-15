# Page Format — Three Files, Fenced Blocks

Every content page is ONE entity in three files (same basename, same folder), always in sync:

| File | Role |
|---|---|
| `page.txt.md` | Verbatim page text only — headings, labels, bullets. The review/summary surface. Nothing about viz. |
| `page.viz.md` | Regeneration brief per canvas: data recipe + key quantities, template used, colors/theme, insight shown, hard-won code snippets. Goal: html regen with near-zero thinking. |
| `page.html` | Rendered page. All page-specific content inline; only external reference = shared template js. Fenced blocks per section. |

Pages without charts: grids still get a `viz.md` when they have structure worth specifying (sectioned/flat mode, label color map, numbering rules); pure-prose pages (folk-wisdom) need only `txt.md` + `html`.

**Migration transition:** during migration a page temporarily has BOTH `page.html` (the untouched original, kept for comparison — only waves 1.1/1.2 fenced originals in place) and `page.v2.html` (regenerated on the shared template js, carries the fences — this is what ships). Migration is create-only: originals and old single `.md` files are never modified or deleted by agents; the user deletes them in one bulk review at the end.

## Fences (format-agnostic — same meaning in every template)

```html
<!-- ==== SEC-1 TEXT ==== -->
  ...section markup...
<!-- ==== /SEC-1 TEXT ==== -->
```
```javascript
// ==== SEC-1 VIZ canvas1 ====
  ...data + draw calls (or bespoke chart code)...
// ==== /SEC-1 VIZ canvas1 ====
```

- Grep the fence, read only the block, edit only the block.
- Every canvas gets its own VIZ fence; bespoke/custom code lives INSIDE its fence (preserved exactly — e.g. sports-wearable top widget).

## Anchors & numbering

- Sections carry the same `[sec-N]` ID across txt.md, viz.md, and html fences.
- Detail-page section IDs are **append-only**: new sections take the next number; removals leave gaps; never renumber.
- **Grid cards are the opposite**: card numbers match file index numbers and must read ascending down the page — inserting a card mid-section renumbers later cards AND their files. Each grid's viz.md restates this rule with any grid-specific instructions.
- **Detail-page titles carry NO index number** — `<title>` and `<h1>` are the bare topic name; the `N.` index lives only on the grid card (and in the filename).

## viz.md header

```markdown
# <Page Title> — Viz Briefs
**Template:** 06-sectioned-cards-callout
**Shared js:** ui-templates/js/base.js
**Theme:** house-blue   (or any THEMES.md accent theme)
```

Per-canvas brief: `## [sec-N] canvasId — Title`, then Type / Data / Colors / Shows / Notes bullets.

## Edit workflows

- **Text edit:** edit txt.md section → edit same words in the html `SEC-N TEXT` fence → glance at viz.md briefs for that section (claim changed → viz edit too). All three files leave agreeing.
- **Viz edit:** edit the viz.md brief → regenerate only the html `SEC-N VIZ` fence from brief + template defaults.
- **Full regen:** read ALL THREE files; the old html is an input (skeleton, working custom code) — rebuild what changed, carry forward the rest.
- **Move:** move all three files together; fix the one `<script src>` depth; check both folders' CLAUDE.md for move instructions.

## Preservation rules

- Colored labels, chips, colored bullet leads, mixed fonts = information, never strip (THEMES.md lists the vocabularies).
- One-off custom layouts/elements stay custom — fence them, don't force them into a template.
- Archive folders: mechanical steps only; ask before any template consolidation.
