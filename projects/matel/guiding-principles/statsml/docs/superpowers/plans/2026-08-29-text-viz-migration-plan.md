# Migration Plan: Text/Viz Split — Folder → Template Map & Sequencing

**Design:** `docs/superpowers/specs/2026-08-29-text-viz-split-design.md`
**Goal:** migrate every content page to the three-file model (txt.md / viz.md / html with fences + shared js). Pilot folders run page-by-page to harden the recipe; the bulk then runs with automation.
**State tracking:** the Status column of the wave tables below is the single source of migration state (`todo → in-progress → done → verified`). A run that dies resumes at the first unmarked page of the current folder.

## Folder → Template Map

| Folder / target | Template | Pages | Notes |
|---|---|---|---|
| tutorials/<subfolder>/ (64 subfolders) | 06 card-section | 1,207 | 79% byte-identical CSS; micro-forks: 3-col rows (~56 pages, use 09-style cells), code-chip rule, canvas height |
| tutorials/ top-level grids | 02 nav-grid | 64 | 61 byte-identical; 3 sectioned variants |
| domains/ | 05 obj-table | 155 | 40/60 and 45/55 sub-variants; keep per-page split as-is |
| domains/A,B,C-*.html | one-off | 3 | fence only, never templated |
| metrics/ | 05 obj-table | 13 | long pages (up to 27 rows) |
| ml-assumptions/ | 05 obj-table | 13 | single-table variant, tag pills |
| common-bad-practices/ | 05 obj-table | 39 | indexes now clean (38/39 fix done) |
| pseudoscience/ | 05 obj-table | 24 | |
| ab-testing/ | 05 obj-table | 32 | tight-padding variant |
| interesting-problems-paradoxes/ | 05 obj-table | 15 | |
| applied-game-theory-behavioral-design/ | 05 obj-table (+math-box) | 6 | philosophy + math-box extras |
| ml-pipeline-pitfalls/ | 06 card-section | 52 | exactly 3 sections/page |
| anti-pattern-pairs/ | 06 card-section | 31 | plain variant (no pills) |
| most-powerful-signals/ | 06 card-section | 11 | 8-10 sections, 9-10 charts/page |
| statistical-paradoxes/ | 06 card-section | 14 | |
| statistical-tests/ | 06 card-section | 9 | rich callout kit (failure/alt-note/decision-table) |
| cognitive-biases/ | 06 card-section | 22 | 13-causal-reasoning has quiz-style extras — keep custom |
| backlog/ top-level details | 06 card-section | ~76 | |
| backlog/digital-theft/ | 06 card-section + accent themes | 19 | reference for THEMES.md; per-page accents preserved |
| backlog/simulation-models/, man-in-the-middle-attacks/, credential-token-design/, data-acquisition-methods/ (+publicly-available-data/), file-formats/, data-query-languages/ | 06 card-section | ~70 | newer files share setupCanvas(id,w,h) |
| backlog/platform-privacy-policies/ | 10 qa-obj-table | 21 | disclaimer box |
| backlog/programming-languages/ | 10 qa-obj-table | 10 | tiny, mostly canvas-free |
| backlog/ survey/hub pages | 02 nav-grid | ~7 | |
| backlog/archive/** | 06 card-section (mechanical) | ~28 | REVISED 2026-08-30: run without asking — mechanical Pass A like any folder; any consolidation happens in the user's bulk review, not during migration |
| recently-added-misc/ subfolders (tracking 85, apis-fetch 74, apis-activity 13, data-exports 12, sport-wearables 8) | 10 qa-obj-table | ~192 | sport-wearables top widget = protected custom element |
| recently-added-misc/ hub pages | 08 card-grid-hub / 02 nav-grid | ~7 | per page |
| real-world-distributions/ | 09 three-col-distribution | 34 | "Do Not Convert" layout — first-class; swap in js/three-col-dist.js |
| folk-wisdom/ | 07 claim-dissection | 26 | prose-only: txt.md + html, no viz.md, no js |
| brainstorm/ | 04 badges/profiling | 7 (+index → 02) | 11-13 charts/page |
| root hub pages | 01 / 02 / 08 per page | 26 | index.html → 01; nav-grid hubs → 02; white card hubs → 08 |
| 24-technology-causal-chains.html | one-off (mermaid) | 1 | fence only |

## Sequencing — Waves

### Wave 1 — Pilot, page-by-page (manual quality bar; one folder per template family)

Purpose: exercise every template's recipe once, small folders first; findings feed FORMAT.md + the automation scripts.

| Order | Folder | Template exercised | Pages | Status |
|---|---|---|---|---|
| 1.1 | applied-game-theory-behavioral-design/ | 05 (+math-box) | 6 | done — v2 pages generated with shared js (−4%); pending user review |
| 1.2 | brainstorm/ | 04 + 02 index | 8 | done — 8 parallel agents, all verified; cleanup queue in folder CLAUDE.md; pending user review |
| 1.3 | most-powerful-signals/ | 06 (chart-heavy) | 11 | verified — 11/11 pages pass mechanical checks (fences, briefs, node --check, text match) |
| 1.4 | statistical-tests/ | 06 (rich callouts) | 9 | verified — 9/9 pass; folder-wide broken back-link `../13-statistical-tests.html` preserved (grid is 12-), flagged for bulk review |
| 1.5 | folk-wisdom/ (first 5 pages) | 07 (prose-only) | 5 | verified — 5/5 pass; txt.md + v2.html only, no viz.md/js |
| 1.6 | backlog/programming-languages/ | 10 (minimal) | 10 | verified — 10/10 pass; 7 pages canvas-free (txt.md+v2 only), 3 have one canvas each |
| 1.7 | real-world-distributions/ (first 3 pages) | 09 + shared js swap | 3 | verified — 3/3 pass; LESSON: three-col-dist.js swap fully applies only to the healthtech/ecommerce code lineage (01's drawBarChart was byte-identical and swapped); 02/03 draw helpers are a different variant and stay page-local in LIB fences; rng deletions required where `let rng` would collide with base.js |
| 1.8 | backlog/digital-theft/ (first 3 pages) | 06 + accent theme | 3 | verified — 3/3 pass; accents recorded in viz.md (01 deep-purple, 02 teal, 03 indigo; 02/03 secondary hexes drift from THEMES.md table, preserved as-is) |

**Gate 1:** user reviews pilot output; FORMAT.md and template entries updated with every lesson; per-family fencing/split recipes written down as scripts where mechanical.

### Wave 2 — Semi-automated, folder-by-folder (scripts do mechanical parts; agent judgment per page for briefs)

| Order | Folders | Pages | Status |
|---|---|---|---|
| 2.1 | metrics/, ml-assumptions/ | 26 | verified — 13/13 + 13/13 pass mechanical checks (docs/superpowers/verify_folder.py; exact text match, zero warnings) |
| 2.2 | statistical-paradoxes/, interesting-problems-paradoxes/ | 29 | verified — 14/14 + 15/15 pass; notable original bugs flagged: ipp/01 duplicate canvas id, ipp/18 wrong day-20/24 share figures, ipp/09 chart content drawn past canvas height |
| 2.3 | pseudoscience/, cognitive-biases/ | 46 | verified — 24/24 + 22/22 pass (13-causal-reasoning quiz page kept custom: quiz turned out static markup, drawing helpers protected in LIB fences; its viz.md uses grouped-range brief headers) |
| 2.4 | ab-testing/, anti-pattern-pairs/ | 63 | verified — 32/32 + 31/31 pass mechanical checks, exact text match |
| 2.5 | folk-wisdom/ (rest), real-world-distributions/ (rest) | 52 | Pass A verified — folk-wisdom 26/26 (txt+v2, prose-only), rwd 34/34 txt+viz pass --md-only; rwd v2.html exists for 22/34 from the earlier v2-first ordering (rest in Pass B) |
| 2.6 | ml-pipeline-pitfalls/, common-bad-practices/ | 91 | Pass A verified — 52/52 + 39/39 pass --md-only (91 agents, 0 errors); original chart bugs recorded in Improve bullets (mpp 13/15 bar overflow, 17 tick label; cbp 07 impossible accuracy bin, 16 undrawn labels) |
| 2.7 | backlog/ details + remaining subfolders + hubs | ~192 | Pass A verified — 192 agents, 0 errors; all 9 folder groups pass --md-only: backlog 83/83 (74 details + 9 hub structure-specs), digital-theft 19/19, simulation-models 12/12, mitm 10/10, credential-token-design 9/9, data-acquisition 14/14, file-formats 13/13, query-languages 11/11, privacy-policies 21/21; original content bugs recorded in Improve bullets (e.g. 04-skew LEFT/RIGHT split contradicts caption, 10-decision-tree reversed canvas ids) |
| 2.8 | root hub pages | 24 | Pass A verified — 24/24 pass --md-only (index + 01–23; grids got structure-spec viz.md, detail pages 11/16 got chart briefs); 24-technology-causal-chains deferred to wave 4 (mermaid one-off) |

**Gate 2:** recipes now proven across all templates at scale; automation scripts finalized (fencing, md split, utility swap, verification); failure/resume behavior tested.

### Wave 3 — Bulk automation (the big uniform folders)

| Order | Folders | Pages | Status |
|---|---|---|---|
| 3.1 | domains/ | 158 | Pass A verified — 155/155 numbered pages pass --md-only (155 agents, 0 errors; A/B/C one-offs → wave 4); many original chart bugs recorded in briefs/Improve (e.g. 003 canvas6 ReferenceError, 002 clipped axis labels, 017 pie wedges vs legend mismatch) |
| 3.2 | recently-added-misc/ (all subfolders + hubs) | ~199 | todo |
| 3.3 | tutorials/ grids | 64 | todo |
| 3.4 | tutorials/ subfolders, batched ~5 subfolders per run | 1,207 | todo |

Bulk runs are still folder-by-folder units (resume-safe); pages run as parallel subagent batches — ONE page per agent, 10 in parallel (user-raised from 5 for this migration).

### Wave 4 — Special cases (with user)

| Item | Action | Status |
|---|---|---|
| backlog/archive/** | mechanical only, NO asking (user decision 2026-08-30); consolidation deferred to bulk review | todo |
| domains/A,B,C, mermaid page, sport-wearables widget pages | fence-only pass; verify custom elements untouched | todo |
| Folder CLAUDE.md updates | per folder at its migration time (strip relocated canvas details, add template default + FORMAT.md pointer) | rolling |

## Per-folder recipe (all waves) — REVISED 2026-08-29: create-only, bulk review

**REVISED 2026-08-30 (user decision, mid-wave-2.5):** split the per-page work into two passes. **Pass A (current): html → txt.md + viz.md only** — v2.html generation is deferred to a later dedicated wave (Pass B). Subagents read ONLY the original html (worked-example/FORMAT.md/base.js reads were overflowing agent context); all conventions are embedded in the agent prompt. Concurrency: **1 page per agent, 5 agents in parallel**. Pages that already have some or all output files from earlier runs keep them (skip if txt+viz both exist; v2.html files already produced under the earlier v2-first ordering stay and are counted toward Pass B).

**Execution mode (user decision, reaffirmed 2026-08-30: "do all the waves, don't stop / without asking"):** run the whole remaining plan straight through with NO per-folder review gates and NO user checkpoints anywhere — including backlog/archive (mechanical migration; consolidation questions dropped in favor of the bulk review). The user reviews everything in bulk at the end. Safe because the recipe is CREATE-ONLY: originals (`NN.html`, `NN.md`) are never modified from wave 1.3 onward; all output goes to new files; rollback = delete the new files. (History exception: waves 1.1/1.2 fenced their originals in place, and 1.1 stripped title indexes in originals — visible in the git diff.) Session env: `CLAUDE_CODE_MAX_SUBAGENTS_PER_SESSION=20000` (set in .claude/settings.json env block).

0. Pages are processed by subagents: one html page per agent (one conversion per agent), 5 in parallel.
1. (Pass A) Read the original html ONLY. Create `NN-topic.txt.md` (verbatim text, `[sec-N]` anchors, no index number in title lines) and `NN-topic.viz.md` (template/theme header + per-canvas briefs per FORMAT.md; grids get structure spec; prose-only pages get txt.md only). Briefs may include an ASCII-art **Sketch** block when it captures the viz intention better than prose, and an optional **Improve** bullet recording how a regeneration could do better (original never changed).
2. (Pass B, later dedicated wave) Create `NN-topic.v2.html` — the new-structure page: fenced TEXT/VIZ/LIB blocks, `<script src>` to ui-templates/js/base.js (or the template's js), chart wiring per folder pattern, incompatible page-local helpers kept fenced as LIB, boilerplate that base.js provides removed, `<title>`/`<h1>` without index numbers. The ORIGINAL html is NOT edited.
3. Verify per page: (Pass A) txt.md text matches original text; every canvas has a viz.md brief. (Pass B) v2 text matches original text; every canvas has its own VIZ fence; fences pair; `node --check` on the v2 inline script. No render checks.
4. Update folder CLAUDE.md (create-only exception: this file may be created/updated).
5. Do NOT delete old `.md` or original html anywhere — deletions happen only in the user's bulk review at the end. Mark folder done in this plan's Status column.

## Verification & rollback

- Idempotent steps: already-fenced/split pages detected and skipped on resume.
- The user commits per folder (all git by user) — a bad batch never leaks past its folder; rollback = user reverts the folder's commit.
