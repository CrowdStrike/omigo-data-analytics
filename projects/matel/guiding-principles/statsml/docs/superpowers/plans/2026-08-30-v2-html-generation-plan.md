# Pass B Plan: v2.html Generation — Workflow-Orchestrated, Wave-Mirrored

> **For agentic workers:** execute folder-by-folder with the Workflow tool script in this document. Status column of the wave tables below is the single source of Pass B state (`todo → in-progress → done → verified`). A run that dies resumes at the first unmarked folder; within a folder, skip-if-exists makes every re-run idempotent.

**Goal:** create `NN-topic.v2.html` for every content page, generated from the ORIGINAL `NN-topic.html` only, mirroring the wave grouping of `2026-08-29-text-viz-migration-plan.md` (Pass A).

**Companion plan:** `docs/superpowers/plans/2026-08-29-text-viz-migration-plan.md` (Pass A: txt.md + viz.md). Another session is executing Pass A concurrently. This plan owns ONLY v2.html files; it never touches Pass A outputs, folder CLAUDE.md files, or the Pass A plan document.

## Locked decisions (user, 2026-08-30)

- **Input = original html only.** Generation agents read ONLY the original `NN-topic.html`. They read NO `.md` files anywhere — not txt.md, not viz.md, not FORMAT.md, not folder CLAUDE.md — and no other files either: `ui-templates/js/base.js` is not read; its contract is embedded verbatim in the agent prompt (below).
- **Trail behind Pass A.** A folder is eligible only when Pass A has finished it. Detected from disk, never by reading md content: folder is ready when every original `NN*.html` has a sibling `NN*.txt.md` (existence check only, e.g. via `ls`/`find`). This avoids two sessions writing in the same folder at once.
- **Skip existing v2.** ~258 v2.html files already exist from the earlier v2-first ordering (all of waves 1.1–1.6, 2.1–2.4, folk-wisdom, rwd 22/34, digital-theft 3/19). If `NN.v2.html` exists, the page is skipped — never regenerated, never overwritten.
- **No gates.** Run all waves straight through with no per-folder checkpoints; the user reviews everything in bulk at the end. Safe because the recipe is CREATE-ONLY: originals are never modified; rollback = delete the new v2 files.
- **Concurrency:** 1 page per agent, exactly 5 agents in parallel (enforced by chunked `parallel()` in the workflow script).
- **Model:** workflow agents must run on `claude-mythos-5[1m]`. That is this session's model, so `agent()` calls OMIT the `model` option — agents inherit it. Do not pass any model override.
- **No git.** User does all commits (per folder). Never run git commands.
- **No folder CLAUDE.md edits in Pass B.** The Pass A session owns those updates; skipping them here avoids write collisions.
- **No render checks** — no browsers, no screenshots. Verification is mechanical only (verify_folder.py).

## Orchestrator recipe — one folder at a time, in wave order

For each folder row in the wave tables (top to bottom), mark it `in-progress`, then:

1. **Readiness check (Pass A trail):**
   ```bash
   cd <statsml-root>
   ls <folder>/*.html | grep -v '\.v2\.html$' | sed 's/\.html$/.txt.md/' | xargs ls 2>&1 | grep -c 'No such'
   ```
   Expected: `0`. If nonzero, Pass A hasn't finished this folder — skip to the next eligible folder row and come back later (re-check on each pass through the tables; if nothing is eligible, wait and re-check every ~15 min).
2. **Build the page list** (originals missing a v2 sibling):
   ```bash
   for f in <folder>/*.html; do case "$f" in *.v2.html) continue;; esac; [ -f "${f%.html}.v2.html" ] || echo "$(basename $f)"; done
   ```
3. **Run the workflow** below with `args = { folder: "<folder>", baseJsPath: "<see per-folder notes>", pages: [<list from step 2>], notes: "<folder-specific notes row, verbatim>" }`. Pass `pages` as a real JSON array, not a string.
4. **Verify the folder:**
   ```bash
   python3 docs/superpowers/verify_folder.py <folder> --v2-only
   ```
   Expected: `N/N pass`, zero errors. (For folk-wisdom-style prose folders add `--prose-only`.)
5. **Fix failures:** for each failing page, dispatch one fix agent (same embedded prompt + the verifier's error lines appended, and permission to overwrite that one v2 file it just created), max 5 in parallel; re-run step 4. Repeat until clean.
6. Mark the folder `done` in this plan's Status column; after the verifier passes, mark `verified`. Then next folder.

Session env already set: `CLAUDE_CODE_MAX_SUBAGENTS_PER_SESSION=20000`.

## Workflow script (one invocation per folder)

```js
export const meta = {
  name: 'v2-gen-folder',
  description: 'Generate NN.v2.html from original NN.html for one folder — create-only, 1 page/agent, 5 in parallel',
  phases: [
    { title: 'Generate', detail: 'one agent per page, chunks of 5' },
  ],
}
// args: { folder: string, baseJsPath: string, pages: string[], notes: string }
const RESULT = {
  type: 'object',
  properties: {
    page: { type: 'string' },
    status: { type: 'string', enum: ['created', 'skipped', 'error'] },
    canvases: { type: 'integer' },
    libKept: { type: 'boolean' },
    notes: { type: 'string' },
  },
  required: ['page', 'status', 'canvases', 'notes'],
}
phase('Generate')
const results = []
for (let i = 0; i < args.pages.length; i += 5) {
  const chunk = args.pages.slice(i, i + 5)
  const out = await parallel(chunk.map(p => () =>
    agent(buildPrompt(args.folder, p, args.baseJsPath, args.notes),
          { label: `v2:${p}`, phase: 'Generate', schema: RESULT })))
  results.push(...out.filter(Boolean))
  log(`${Math.min(i + 5, args.pages.length)}/${args.pages.length} pages processed`)
}
const errors = results.filter(r => r.status === 'error')
if (errors.length) log(`ERRORS on ${errors.length} pages: ${errors.map(e => e.page).join(', ')}`)
return { folder: args.folder, results }

function buildPrompt(folder, page, baseJsPath, notes) {
  return `You convert ONE html page to its v2 (fenced) form. Work in the statsml root; the page is ${folder}/${page}.

INPUT RULES (strict):
- Read ONLY ${folder}/${page}. Read NO .md file of any kind (no txt.md, viz.md, FORMAT.md, CLAUDE.md, README.md), no other html page, no template file, no js file. Everything you need is in this prompt.
- If ${folder}/${page.replace(/\.html$/, '.v2.html')} already exists, STOP immediately and return {"page":"${page}","status":"skipped","canvases":0,"notes":"v2 exists"}.

OUTPUT: create ${folder}/${page.replace(/\.html$/, '.v2.html')} — a full standalone html page. CREATE-ONLY: never edit ${page} or any other existing file. Never run git.

base.js CONTRACT (loaded via <script src="${baseJsPath}"></script>, placed immediately before your inline <script>). It provides — so your page must NOT redefine byte-equivalent versions of:
- setupCanvas(id, w, h) -> bare high-DPI-scaled ctx (reads width/height attrs when w/h omitted; sizes backing store to displayed width x devicePixelRatio; redraw-safe)
- setup(id, w, h) -> alias of setupCanvas (RETURNS BARE ctx, not an object)
- registerChart(fn) -> pushes fn, draws now, re-draws on window resize (debounced)
- __renderCharts(), __charts
- mulberry32(seed); var rng = mulberry32(42); randn(); randExp(lambda)
- roundRectPath(ctx,x,y,w,h,r); drawArrow(ctx,x1,y1,x2,y2,color,width); drawAxes(ctx,margin,plotW,plotH,color)

CONVERSION RULES:
1. Keep the original <head> CSS, minus rules for elements the page no longer contains (e.g. dead .nav blocks when there is no <div class="nav">). Keep the body markup and ALL visible text VERBATIM — the verifier diffs normalized visible text of v2 against the original and any mismatch fails the page.
2. <title> and <h1>: strip a leading index number ("13. Foo" -> "Foo"). This is the ONLY permitted text change.
3. TEXT fences (html comments):
   <!-- ==== PAGE-HEAD TEXT ==== --> ... <!-- ==== /PAGE-HEAD TEXT ==== --> around the h1 + subtitle block;
   <!-- ==== SEC-N TEXT ==== --> ... <!-- ==== /SEC-N TEXT ==== --> around each section (its h2/heading plus that section's content block), numbered 1..N in page order. Grid/hub pages with no sections: fence the whole card grid as SEC-1 TEXT.
4. Add <script src="${baseJsPath}"></script> right before the inline script. Canvas-free pages get NO script tags at all (no base.js, no inline script).
5. Inline script:
   - DELETE page-local code that base.js already provides with the same contract: setupCanvas/setup returning bare ctx, registerChart/__charts/resize-redraw wiring, mulberry32/rng/randn/randExp, manual end-of-script "draw all charts" calls.
   - KEEP page-local helpers whose contract differs (e.g. a setup(id) that returns {ctx,w,h}, custom draw helpers) inside fences:
     // ==== LIB (one-line reason) ====  ...  // ==== /LIB ====
     Page script runs after base.js, so page-level declarations override it — that is the intended mechanism.
   - rng collision rule: base.js declares "var rng". A page-local "let rng" or "const rng" at top level COLLIDES (SyntaxError). If the local is mulberry32(42)-equivalent, delete it; otherwise rename it (e.g. rngLocal) and update its uses inside this page only.
   - Wrap each canvas's drawing code as registerChart(function(){ ... }); enclosed in its own fence pair:
     // ==== SEC-N VIZ <canvasId> ====  ...  // ==== /SEC-N VIZ <canvasId> ====
     Exactly one VIZ fence per <canvas id>; N = the section the canvas sits in.
   - Never introduce Math.random()/Date.now(); keep the original chart data and drawing logic unchanged otherwise.
6. Folder-specific notes for this run (apply if relevant): ${notes}

SELF-CHECK before returning (do all three):
a) grep the v2 file: every fence opener has its matching closer; every <canvas id="X"> has exactly one "VIZ X" fence.
b) Extract the inline script body to a temp file and run: node --check <tmpfile>. Fix until clean. Skip if the page has no script.
c) Re-scan that no visible text was dropped or reworded (title index strip excepted).

Return ONLY the JSON result object: {"page":"${page}","status":"created","canvases":<count>,"libKept":<bool>,"notes":"<anything notable: helpers kept in LIB, rng rename, dead CSS dropped, original bugs seen (never fixed in place)>"}.`
}
```

Iterate on the script via the persisted `scriptPath` + `resumeFromRunId` if a folder run dies mid-way (cached agent results replay; skip-if-exists covers the rest).

## Sequencing — Waves (mirrored from Pass A plan)

Rows whose v2 files fully exist from the earlier v2-first ordering are pre-marked. Every row still runs through the skip-if-exists recipe when its turn comes (cheap no-op when complete), so partial rows self-heal.

### Wave 1 — pilots (v2 mostly pre-existing)

| Order | Folder | Pages | v2 Status |
|---|---|---|---|
| 1.1 | applied-game-theory-behavioral-design/ | 6 | verified (pre-existing 6/6) |
| 1.2 | brainstorm/ | 8 | verified — 8/8 pass --v2-only (index.v2 generated 2026-08-30; canvas-free, dead .nav CSS dropped) |
| 1.3 | most-powerful-signals/ | 11 | verified (pre-existing 11/11) |
| 1.4 | statistical-tests/ | 9 | verified (pre-existing 9/9) |
| 1.5 | folk-wisdom/ (first 5) | 5 | verified (pre-existing; covered by 26/26) |
| 1.6 | backlog/programming-languages/ | 10 | verified (pre-existing 10/10) |
| 1.7 | real-world-distributions/ (first 3) | 3 | verified (pre-existing; in the 22/34) |
| 1.8 | backlog/digital-theft/ (first 3) | 3 | verified (pre-existing 3/19) |

### Wave 2 — folder-by-folder

| Order | Folders | Pages | v2 Status |
|---|---|---|---|
| 2.1 | metrics/, ml-assumptions/ | 26 | verified (pre-existing 13+13) |
| 2.2 | statistical-paradoxes/, interesting-problems-paradoxes/ | 29 | verified (pre-existing 14+15) |
| 2.3 | pseudoscience/, cognitive-biases/ | 46 | verified (pre-existing 24+22; 13-causal-reasoning quiz helpers live in LIB fences) |
| 2.4 | ab-testing/, anti-pattern-pairs/ | 63 | verified (pre-existing 32+31) |
| 2.5 | folk-wisdom/ (rest), real-world-distributions/ (rest) | 52 | verified — folk-wisdom 26/26 pre-existing; rwd 34/34 pass --v2-only (12 generated 2026-08-30; all non-swappable lineage, helpers in LIB; 33 uses seed 77, 34 seed 99 — preserved) |
| 2.6 | ml-pipeline-pitfalls/, common-bad-practices/ | 91 | verified — 52/52 + 39/39 pass --v2-only (2026-08-30; object-setup kept in LIB; dead .nav CSS dropped on cbp pages; original bugs reproduced & noted, e.g. mpp 13/15 bar overflow, cbp 07 impossible accuracy bin) |
| 2.7 | backlog/ details + remaining subfolders + hubs | ~180 | in-progress — digital-theft verified 19/19 (accents preserved); backlog top-level 83 pages running; ready next: platform-privacy-policies 21, data-acquisition-methods 14, file-formats 13, simulation-models 12, data-query-languages 11, man-in-the-middle-attacks 10, credential-token-design 9; programming-languages grew 10→20 originals, new 10 await Pass A |
| 2.8 | root hub pages | 26 | todo (trail Pass A) |

### Wave 3 — bulk (trail Pass A folder-by-folder)

| Order | Folders | Pages | v2 Status |
|---|---|---|---|
| 3.1 | domains/ | 158 | todo |
| 3.2 | recently-added-misc/ (all subfolders + hubs) | ~199 | todo |
| 3.3 | tutorials/ grids | 64 | todo |
| 3.4 | tutorials/ subfolders, batched per subfolder | 1,207 | todo |

Wave 3 stays 1 page per agent, 5 in parallel (user setting for Pass B; do NOT raise to 10 — that was a Pass A setting).

### Wave 4 — special cases

| Item | Action | v2 Status |
|---|---|---|
| backlog/archive/** | mechanical, no asking (standing user decision) | todo |
| domains/A,B,C-*.html; 24-technology-causal-chains.html (mermaid); sport-wearables widget pages | fence-only v2: TEXT fences + LIB-fence the entire custom script untouched; NO base.js swap, NO helper deletion | todo |

## Per-folder notes (passed as `args.notes` and `args.baseJsPath`)

| Folder(s) | baseJsPath | Notes |
|---|---|---|
| root-level folders (metrics, domains, ab-testing, …) | `../ui-templates/js/base.js` | standard recipe |
| backlog/, recently-added-misc/, tutorials/ SUBfolders | `../../ui-templates/js/base.js` | depth-adjusted path |
| root hub pages (statsml root itself) | `ui-templates/js/base.js` | grids are usually canvas-free -> TEXT fences only, no scripts |
| folk-wisdom/ | n/a | prose-only: TEXT fences, no scripts; verify with `--prose-only --v2-only` |
| real-world-distributions/ remainder | `../ui-templates/js/base.js` | Wave-1.7 lesson: only the healthtech/ecommerce code lineage may swap helpers for `../ui-templates/js/three-col-dist.js` (add it as a second script src when the page's drawBarChart is byte-identical to it); other lineages keep helpers page-local in LIB fences; watch `let rng` collisions |
| backlog/digital-theft/ remainder | `../../ui-templates/js/base.js` | per-page accent themes: keep each page's accent hexes exactly as in the original |
| statistical-tests/, cognitive-biases/13 | `../ui-templates/js/base.js` | rich callout kits / quiz markup: static html stays verbatim inside TEXT fences; incompatible draw helpers stay in LIB |
| sport-wearables/ | `../../ui-templates/js/base.js` | top widget = protected custom element -> fence-only treatment for that block (wave 4 row) |
| tutorials/ subfolders | `../../ui-templates/js/base.js` | 06 card-section family; ~56 pages use 3-col rows — markup verbatim, no restructuring |

## Verification & rollback

- Per page: agent self-check (fence pairing, per-canvas VIZ fence, `node --check`). Per folder: `python3 docs/superpowers/verify_folder.py <folder> --v2-only` must pass with zero errors before the row is marked `verified`. No render checks ever.
- Original chart bugs found while converting are recorded in the agent's `notes` output only — never fixed in the original, never silently fixed in v2 (v2 reproduces the original drawing logic).
- Everything is create-only: rollback for any folder = delete its new `*.v2.html` files. The user commits per folder; a bad batch never leaks past its folder.
