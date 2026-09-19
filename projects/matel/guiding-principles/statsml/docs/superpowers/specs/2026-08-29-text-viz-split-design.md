# Design: Text/Viz Split for HTML Pages

**Date:** 2026-08-29 (updated with corpus census + concrete catalog)
**Status:** Draft — pending user review
**Problem:** Content pages mix ~200 lines of text with ~1,700 lines of inline chart js in one html file. Editing one sentence means reading a 100KB file; regenerating a page means re-deriving every chart from scratch. The sibling `.md` mirrors canvas implementation details (colors, margins, dash patterns) instead of describing the viz. With 2,000+ pages, token cost per edit does not scale.

## Governing Principle: Bounded Token Cost

Every routine operation must have a bounded token cost that does not grow with page size or corpus size.

| Operation | Today | With this format |
|-----------|-------|------------------|
| Review/summarize text | Read 1,900-line html or spec-heavy md | Read ~120-line txt.md only |
| Edit one section's text | Read + rewrite whole html | Edit txt.md section + one ~15-line html fence |
| Change one viz | Rethink from scratch, regen whole page | Edit one viz.md brief + regen one ~30-line fenced block |
| Regenerate whole html | Heavy reasoning | Mechanical assembly from txt.md + viz.md + existing html |
| House restyle | Touch 2,000+ files' inline code | Edit shared template js once |

Corollary rules: grep before read (fences make every block addressable); read only the block, edit only the block; boilerplate lives once in the template layer.

## The Three-File Page Model

Every content page is one entity in three files — same basename, same folder, always in sync:

| File | Role | Contains |
|------|------|----------|
| `page.txt.md` | Text source & review surface | Verbatim page text. Nothing about viz. Scannable end-to-end. |
| `page.viz.md` | Regeneration brief | Per-canvas briefs: data recipe + key quantities, template + theme used, colors, insight, hard-won code snippets. For grids: structure spec (sectioned/flat mode, label color map, numbering rules). |
| `page.html` | Rendered page | All page-specific content inline; only external reference is shared template js. Fenced greppable blocks. May embed arbitrary custom js inside VIZ fences. |

Rules:

- **Atomic entity** — created, edited, moved, deleted together; never disagree. No drift machinery: sync is a workflow invariant.
- **Shared `[sec-N]` anchors** across all three files. Detail-page section IDs are append-only (gaps ok, never renumber).
- **Grid card numbering is the opposite:** card numbers match file index numbers, ascending down the page; mid-section insertion renumbers later cards and files. Rule lives in FORMAT.md and each grid's viz.md.
- **One home per fact** — text in txt.md, viz knowledge in viz.md, house style in the template layer.
- Pure-prose pages (folk-wisdom) need only txt.md + html.

## File Formats

Full conventions: `ui-templates/FORMAT.md` (fence vocabulary, viz.md header with Template/Shared js/Theme, workflows). Summary of a viz.md brief:

```markdown
## [sec-1] canvas1 — Weekday vs Weekend Sleep Histogram
- **Type:** overlaid histogram — standard `drawHistogram` (js/three-col-dist.js)
- **Data:** weekday 600 samples tight around 6.75h (sd 0.4); weekend bimodal — 250 near 7h, 150 near 9.5h
- **Colors:** house blue + red overlay (theme: house-blue)
- **Shows:** alarm compresses weekday variation; weekend reveals two sleeper groups
- **Notes:** hard-won code/reasoning worth preserving verbatim
```

Brief content rules: insight + load-bearing numbers; template named, not described; **code allowed when it earns its place** (the test is regeneration cost — snippets that took real effort go in Notes; boilerplate the template provides does not).

## Edit Workflows

- **Text edit (sec N):** txt.md section → same words in html `SEC-N TEXT` fence → check viz.md briefs for that section. All three leave agreeing.
- **Viz edit (sec N):** viz.md brief → regenerate only the html `SEC-N VIZ` fence.
- **Add/remove section:** all three files in one operation; IDs append-only (detail) / renumber-ascending (grid cards).
- **Full regen:** read ALL THREE files — the old html is an input (working skeleton, correct custom code); rebuild what changed, carry forward the rest.
- **House restyle:** edit shared template js once.
- **Move:** three files together + fix one `<script src>` depth + check both folders' CLAUDE.md.

## Template Layer (BUILT — see `ui-templates/`)

Corpus census (2026-08, 5-agent sampled survey + signature greps over all ~2,250 pages): the corpus reduces to **~9 real templates + ~30 one-offs**. Catalog now in `ui-templates/README.md` with entries 01-10 (md+html sample each):

| Entry | Family | ~Pages |
|---|---|---|
| 06 sectioned-cards | card-section detail (tutorials, pitfalls, backlog+security subfolders) | ~1,530 |
| 05 two-col-catalog-clean | obj-table detail (domains, metrics, ml-assumptions, ab-testing, ...) | ~330 |
| 10 qa-obj-table (NEW) | Q&A rows, payload blocks (recently-added-misc, privacy-policies) | ~220 |
| 02 nav-grid | beige hubs (tutorials grids, root, backlog surveys) | ~86 |
| 09 three-col-distribution (NEW) | real-world-distributions ("Do Not Convert" — first-class) | ~34 |
| 07 claim-dissection | folk-wisdom, prose-only | 26 |
| 08 card-grid-hub (NEW) | white sectioned/flat hubs | ~17 |
| 04 badges/profiling | brainstorm deep-dives | 7 |
| 01 landing | index.html | 1 |

Supporting files (created):

- **`ui-templates/js/base.js`** — unified `setupCanvas`/`setup` (ends the 400+-file duplication under two names), `registerChart` + resize re-render, seeded `mulberry32`/`randn`/`randExp`, house primitives.
- **`ui-templates/js/three-col-dist.js`** — `drawHistogram` (overlays/density/SE band) + `drawBarChart`, extracted verbatim from the reference pages (duplicated today in all 34 distribution pages).
- **`ui-templates/THEMES.md`** — 19 named accent themes (generalizing digital-theft's per-page hues; body text + semantic red/green/orange never theme); colored-label vocabularies (information-bearing, never strip); the two font systems (Type A apple-em, Type B system-rem).
- **`ui-templates/FORMAT.md`** — three-file model, fence vocabulary, anchor/numbering rules, edit workflows, preservation rules.

Per-template js beyond these two is added when a family's migration begins (e.g., card-section pages mostly need only base.js).

### Preservation rules (hard)

- **Colored labels and mixed fonts/colors are content**: category card labels, semantic chips (.pt-label, .lbl-*, .domain-*), colored bullet leads, pitfall labels — migration never strips or normalizes them.
- **Custom UI stays custom**: bespoke elements (sports-wearable top widget, mermaid causal-chains, domains A/B/C, archive dashboards) are fenced but never flattened into a template.
- **Accent variety is encouraged**: pages may adopt any THEMES.md accent; digital-theft is the reference; grid label colors follow the category palette.
- **Archive folders**: mechanical steps only (fencing, split); any template consolidation is a question for the user first.

### Folder CLAUDE.md integration

- Folder CLAUDE.md holds folder defaults (template, theme policy) + move-in/move-out instructions; viz.md's declaration wins for its page.
- Folder CLAUDE.md files that embed canvas-level details get those relocated to the template layer during that folder's migration; they keep content focus, TODOs, move instructions.

## Migration Plan — Full Corpus, Folder by Folder

Flaky-infra design: phases small, independently verifiable, resumable.

- **Phase 0a — census: DONE** (this doc + README census table is the family map).
- **Phase 0b — template layer: DONE** (catalog 01-10, js/, THEMES.md, FORMAT.md).
- **Phase 0c — checklist file:** generate the migration checklist (folder → pages → family → status) from the census before the first folder migrates.

**Per-folder phases** (one folder at a time, smallest first to shake out the recipe):
1. Split each page's `.md` into txt.md + viz.md (text mechanical; briefs are the judgment step).
2. Fence the html; swap duplicated utilities for the template js reference; bespoke code stays inline in its fence.
3. Verify per page: txt.md matches html text; every canvas has a brief; fences pair. Mark checklist.
4. Update the folder's CLAUDE.md (strip relocated canvas details; add template default + convention pointer).
5. Folder cleanup after the whole folder verifies: delete old single `.md` files.

**Resumability:** checklist file is the single source of migration state; per-page steps idempotent (fenced pages detected, skipped); folders reviewed by the user as units. Subagent batches: one page per agent, max 5 parallel.

**Git:** the user handles all git; work happens on the user's branch (currently `statsml_v5`); recovery from history goes through the user.

## Housekeeping completed alongside design

- Deleted `24-technology-causal-chains-v2.html/.md` (unreferenced duplicate).
- Fixed duplicate indexes in common-bad-practices: `19-slow-walking`→38, `20-strategic-incompetence`→39 (+.md), grid cards 38/39 added to `09-common-bad-practices.html/.md`.

## Decisions Log

| Decision | Choice |
|----------|--------|
| md role | Human brief, not canvas-code mirror |
| File split | Three files: txt.md / viz.md / html; atomic entity, always in sync |
| html form | Single file, page content fully inline, greppable fences; only template js external |
| Shared js | Template-layer only: base.js + per-template files; floor not ceiling |
| Intent detail | Insight + key quantities; code snippets when they cut regen cost |
| Grid viz.md | Yes — holds grid structure spec + numbering rules |
| Section IDs | Detail: append-only. Grid cards: renumber to keep ascending |
| Theming | 19 named accent themes; variety across pages encouraged; semantics/body never theme |
| Preservation | Colored labels = content; custom UI stays custom; archives ask-first |
| Migration | Full corpus, folder-by-folder, checklist-driven, resumable; old .md deleted at folder cleanup |
