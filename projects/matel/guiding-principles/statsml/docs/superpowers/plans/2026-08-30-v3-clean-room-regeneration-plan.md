# Pass B (v3 Clean-Room Regeneration) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. This plan runs straight through with NO user checkpoints (user decision 2026-08-30: "don't stop"; reaffirmed "dont stop for picking next wave" — when a wave finishes and verifies, launch the next eligible wave immediately, no pause, no asking).

**Goal:** Generate `NN-topic.v3.html` for every content page from its `NN-topic.txt.md` + `NN-topic.viz.md` alone (clean room — original html and v2.html are never read), trailing the Pass A session wave by wave.

**Architecture:** A Workflow script fans out one page per agent, 5 in parallel, chunked with `parallel()` barriers exactly like the Pass A `migrate-folder-pages.workflow.js`. A standalone verifier (`verify_v3.py`) applies the same mechanical bar as v2 pages. A trailing loop re-reads the Pass A plan's Status column between waves and only processes waves marked verified there.

**Tech Stack:** Claude Code Workflow tool (agents inherit the session model — run this plan from a session set to `claude-mythos-5[1m]`; never pass a `model` override in `agent()` calls), python3 for verification, node --check for JS syntax.

**Companion plan (Pass A, owned by the OTHER session — read-only for us):** `docs/superpowers/plans/2026-08-29-text-viz-migration-plan.md`. Never edit that file; v3 state lives in THIS file's Status columns.

---

## Decisions (user Q&A, 2026-08-30)

1. **Inputs — md-only clean room.** A v3 agent reads ONLY: the page's `.txt.md`, its `.viz.md` (when it exists), and the folder's ui-template file (e.g. `ui-templates/06-sectioned-cards-callout.html`) for CSS/layout. It must NOT open the original `NN.html`, `NN.v2.html`, the old `NN.md`, FORMAT.md, or any sibling page. Under-specified briefs get a minimal reasonable choice recorded in the agent's `gaps` output — never a peek at the original.
2. **Scope — all completed waves.** Pages that already have a `v2.html` (waves 1.3–2.4, folk-wisdom, 22 rwd pages) get a v3 too; v2 (fenced restructure of the original) and v3 (regenerated from md) coexist until the user's bulk review.
3. **Trailing — poll continuously.** A wave is eligible when its Status in the Pass A plan contains `verified` (including `Pass A verified`). After exhausting eligible waves, re-read the Pass A plan every ~15 minutes; stop only when every wave through 3.4 is v3-done here (wave 4 is out of v3 scope, see below).
4. **Verification — same bar as v2.** Fences pair; every canvas has a VIZ fence and a viz.md brief; `node --check` passes; visible text matches the ORIGINAL html (SequenceMatcher ratio ≥ 0.985 = warn, below = fail). The verifier reads the original for text comparison — that is verification, not generation, so the clean room is intact.

**Standing constraints (inherited from Pass A):** CREATE-ONLY — originals, old `.md`, txt/viz md, and v2.html are never modified; the only files this plan creates are `*.v3.html` (+ the two tool files below) and edits to THIS plan file. No git commands (user does all git). No render/browser checks. Concurrency: **1 page per agent, 5 in parallel** (user-specified for v3; do NOT raise to 10 even though Pass A wave 3 uses 10). Session env already has `CLAUDE_CODE_MAX_SUBAGENTS_PER_SESSION=20000`.

**Out of v3 scope:** Wave 1.1/1.2 (pilot folders, fenced in place, pending user review), and Wave 4 fence-only specials (`domains/A,B,C-*.html`, `24-technology-causal-chains.html` (mermaid), sport-wearables widget pages) — these are protected custom pages with no faithful md regeneration; the Pass A session handles their fence-only pass. Folder CLAUDE.md updates also stay with the Pass A session (avoids two sessions writing the same file).

**Grid/hub href rule:** hub v3 pages rebuild the card grid from the viz.md structure-spec; hrefs are copied VERBATIM from the spec (they point at original `NN-topic.html`). Retargeting links to v3 siblings is a bulk-review decision, not ours.

**Markdown-first compliance:** `txt.md` + `viz.md` ARE the md sources for `v3.html` (three-file model per `docs/superpowers/specs/2026-08-29-text-viz-split-design.md`); no separate `v3.md` is created.

---

## Task 1: Workflow script `docs/superpowers/regenerate-v3-pages.workflow.js`

**Files:**
- Create: `docs/superpowers/regenerate-v3-pages.workflow.js`

- [ ] **Step 1: Write the script** with the exact content below.

```js
export const meta = {
  name: 'regenerate-v3-pages',
  description: 'Pass B clean room: generate NN-topic.v3.html from txt.md + viz.md only, one page per agent, 5 in parallel',
  phases: [
    { title: 'Regenerate', detail: 'one page per agent, batches of 5' },
  ],
}

// args: single-folder: { baseDir, folder, pages, template, sharedJs, depth, vizmd, folderRules }
//       multi-folder:  { baseDir, folders: [{ folder, pages, template, sharedJs, depth, vizmd, folderRules }, ...] }
// template  = ui-templates file name, e.g. '06-sectioned-cards-callout.html'
// sharedJs  = 'base.js' or 'three-col-dist.js' (what <script src> should load; viz.md header may override per page)
// depth     = number of path segments in folder ('metrics' -> 1, 'backlog/digital-theft' -> 2); src prefix = '../'.repeat(depth)
// vizmd     = false for prose-only folders (no viz.md, no <script>)
const BASE = args.baseDir
const FOLDERS = args.folders || [{
  folder: args.folder, pages: args.pages, template: args.template,
  sharedJs: args.sharedJs, depth: args.depth, vizmd: args.vizmd, folderRules: args.folderRules,
}]

const SCHEMA = {
  type: 'object',
  properties: {
    page: { type: 'string' },
    status: { type: 'string', enum: ['created', 'skipped', 'failed'] },
    canvases: { type: 'number' },
    gaps: { type: 'string' },
    notes: { type: 'string' },
  },
  required: ['page', 'status', 'gaps', 'notes'],
}

function prompt(F, name) {
  const FOLDER = F.folder
  const REL = '../'.repeat(F.depth || 1)
  const VIZMD = F.vizmd !== false
  const chartSpec = VIZMD ? `
CHARTS — from ${name}.viz.md:
- Load the shared js before your chart code: <script src="${REL}ui-templates/js/${F.sharedJs || 'base.js'}"></script> (if the viz.md **Shared js:** header names a different file, use that one instead). base.js already provides: setupCanvas(id,w,h) -> dpr-scaled bare ctx, setup() alias, registerChart(fn) (draw now + redraw on resize), mulberry32(seed)/rng/randn()/randExp(), roundRectPath, drawArrow. Do NOT re-implement these; do NOT use Math.random() or Date.now() — seeded rng only.
- One <canvas id="..."> per brief, same ids, same document order, inside the section the brief's [sec-N] names. Use the exact logical width/height, data values, hexes, fonts, annotation positions, footer lines the brief quotes. A **Sketch** block shows the intended composition — follow it. Ignore **Improve** bullets: regenerate what the brief specifies, not the improvement.
- If the viz.md has a "**Page-local lib note:**", implement those helpers inside a LIB fence before the chart code.
- Wrap each chart's code in fence comments inside the single inline <script> (after the src tag):
  // ==== SEC-<N> VIZ <canvasId> ====
  registerChart(function () { var ctx = setupCanvas('<canvasId>', <w>, <h>); if (!ctx) return; ... });
  // ==== /SEC-<N> VIZ <canvasId> ====
- Page-local helpers (if any):
  // ==== LIB (page-local: <short reason>) ====
  ...
  // ==== /LIB ====
- **Theme:** header in viz.md names the accent theme; its hexes are quoted in the briefs — use them. House palette default: #1a5276 primary, #27ae60 green, #e74c3c red, #e67e22 orange, rgba(26,82,118,0.35) bar fill.` : `
NO CHARTS — this is a prose-only page (no viz.md, no <canvas>, no <script> at all).`

  return `You are regenerating ONE documentation page as a NEW html file, in a CLEAN-ROOM test of the markdown sources. Base dir: ${BASE}. Your page: ${FOLDER}/${name}

READ ONLY these files (via Read):
1. ${BASE}/${FOLDER}/${name}.txt.md — the page's verbatim text, sectioned as "## [sec-N] <heading>".${VIZMD ? `
2. ${BASE}/${FOLDER}/${name}.viz.md — per-canvas regeneration briefs (Type/Data/Colors/Shows/Notes/Sketch).` : ''}
${VIZMD ? '3' : '2'}. ${BASE}/ui-templates/${F.template} — the folder's layout template: copy its CSS/markup conventions (fonts, colors, section/card/table structure, tag pills, callout boxes).

CLEAN ROOM — you must NOT open: ${name}.html (the original), ${name}.v2.html, ${name}.md, FORMAT.md, THEMES.md, base.js, or ANY sibling page. Everything you need is in the three files above plus this prompt. If a brief under-specifies something (a position, a color, a scale), make the minimal reasonable choice and record it in your "gaps" output — do NOT look anywhere else.

CREATE exactly one new file: ${BASE}/${FOLDER}/${name}.v3.html — a complete standalone page:
- <title> and <h1> = the txt.md title lines (they are already index-number-free; keep them that way).
- Subtitle paragraph if txt.md has a "**Subtitle:**" line.
- One section per "## [sec-N]" in txt.md, in order, using the template's section/card structure. ALL text VERBATIM from txt.md — no rewording, no summarizing, no additions. Bold labels stay bold; "**Tags:** name (color), ..." becomes the template's tag pills; "**Example:**"/"**Key point:**" lines become the template's callout boxes; markdown tables become html tables.
- Fence every text block with html comments:
  <!-- ==== PAGE-HEAD TEXT ==== --> h1 + subtitle <!-- ==== /PAGE-HEAD TEXT ==== -->
  <!-- ==== SEC-<N> TEXT ==== --> that section's text markup <!-- ==== /SEC-<N> TEXT ==== -->
- No nav elements, no back/home links, no cross-page links (grid/hub structure-specs are the exception: reproduce their card links verbatim as specified).
${chartSpec}

FOLDER RULES:
${F.folderRules}

CONSTRAINTS:
- CREATE-ONLY: never modify or delete ANY existing file. No git commands. No render/browser checks. Temp files go in /tmp, not the repo.
- If ${name}.v3.html already exists, do nothing and return status "skipped".

VERIFY before returning (mechanically, with a small python script via Bash — do not re-read whole files into context):
- Every visible text token of ${name}.txt.md (strip markdown syntax, the [sec-N] anchors, and "HTML title tag:"/"Subtitle:"/"Tags:" label scaffolding) appears in the v3 html's visible text (strip tags/script/style), and vice versa — the v3 must add no text of its own beyond template-implied labels.${VIZMD ? `
- Every brief's canvas id has exactly one <canvas> and one "SEC-<N> VIZ <id>" fence pair; all fences pair up.
- node --check passes on the inline <script> body.` : ''}
Fix any failure before returning.

Return JSON: page, status (created|skipped|failed), canvases (count in v3), gaps (semicolon-separated list of under-specified points you had to decide yourself, or "none"), notes (1-2 sentences incl. verification result).`
}

const all = []
for (const F of FOLDERS) {
  const PAGES = F.pages
  const results = []
  for (let i = 0; i < PAGES.length; i += 5) {
    const chunk = PAGES.slice(i, i + 5)
    log(`${F.folder} batch ${Math.floor(i / 5) + 1}/${Math.ceil(PAGES.length / 5)}: ${chunk.join(', ')}`)
    const batch = await parallel(chunk.map(p => () =>
      agent(prompt(F, p), { label: `${F.folder}/${p}`, phase: F.folder, schema: SCHEMA })))
    results.push(...batch)
  }
  const out = results.filter(Boolean)
  log(`${F.folder}: ${out.filter(r => r.status === 'created').length} created, ${out.filter(r => r.status === 'skipped').length} skipped, ${out.filter(r => r.status === 'failed').length} failed`)
  all.push(...out)
}
return all
```

- [ ] **Step 2: Syntax-check it**

Run: `node --check docs/superpowers/regenerate-v3-pages.workflow.js`
Expected: exit 0, no output. (The `export const meta` and bare `args`/`log`/`agent` globals are workflow-runtime constructs; `node --check` only parses, so it passes.)

## Task 2: Verifier `docs/superpowers/verify_v3.py`

**Files:**
- Create: `docs/superpowers/verify_v3.py` (standalone — verify_folder.py is actively used/edited by the Pass A session; do NOT modify it. Reuse its helpers by import.)

- [ ] **Step 1: Write the verifier** with the exact content below.

```python
#!/usr/bin/env python3
"""Folder-level mechanical verification for Pass B v3 (clean-room) pages.

Per NN-topic.v3.html in the folder — same bar as v2:
  1. sibling .txt.md exists; .viz.md exists when the page has canvases
  2. fences pair (TEXT / VIZ / LIB open+close)
  3. every <canvas id> has its id in viz.md and a VIZ fence; no duplicate ids
  4. node --check passes on inline <script> blocks
  5. visible text matches the ORIGINAL NN-topic.html (normalized; index-number
     removal tolerated; ratio >= 0.985 -> warn, below -> FAIL)

Usage: verify_v3.py <folder> [--prose-only]
Exit 0 = all pass. Prints PASS/FAIL per page with reasons.
"""
import re
import sys
from difflib import SequenceMatcher
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from verify_folder import visible_text, check_fences, inline_scripts, node_check


def verify_v3_page(v3: Path, prose_only: bool):
    errs, warns = [], []
    base = v3.name[:-len('.v3.html')]
    folder = v3.parent
    orig = folder / f'{base}.html'
    txt = folder / f'{base}.txt.md'
    viz = folder / f'{base}.viz.md'

    src = v3.read_text(encoding='utf-8', errors='replace')
    if not txt.exists():
        errs.append('missing .txt.md')
    canvases = re.findall(r'<canvas[^>]*\bid="([^"]+)"', src)
    if canvases and not viz.exists() and not prose_only:
        errs.append(f'{len(canvases)} canvases but no .viz.md')
    if len(canvases) != len(set(canvases)):
        dupes = sorted({c for c in canvases if canvases.count(c) > 1})
        errs.append(f'duplicate canvas ids: {dupes}')

    fe, _ = check_fences(src)
    errs += fe

    if canvases:
        if viz.exists():
            vsrc = viz.read_text(encoding='utf-8', errors='replace')
            missing = [c for c in set(canvases) if not re.search(r'\b' + re.escape(c) + r'\b', vsrc)]
            if missing:
                errs.append(f'canvas ids missing from viz.md: {sorted(missing)}')
            # clean-room completeness: every briefed canvas must exist in v3
            briefed = re.findall(r'^##\s*\[sec-\d+\]\s+(\S+)', vsrc, re.M)
            absent = [c for c in briefed if c not in set(canvases)]
            if absent:
                errs.append(f'viz.md briefs with no canvas in v3: {sorted(absent)}')
        for c in set(canvases):
            if not re.search(r'VIZ\s+' + re.escape(c) + r'\b', src):
                errs.append(f'canvas {c} has no VIZ fence')

    for js in inline_scripts(src):
        ok, msg = node_check(js)
        if not ok:
            errs.append(f'node --check failed: {"; ".join(msg)}')

    if orig.exists():
        a = visible_text(orig.read_text(encoding='utf-8', errors='replace'))
        b = visible_text(src)
        if a != b:
            sm = SequenceMatcher(None, a, b)
            r = sm.ratio()
            diffs = [f'{tag} orig[{a[i1:i2][:60]!r}] v3[{b[j1:j2][:60]!r}]'
                     for tag, i1, i2, j1, j2 in sm.get_opcodes() if tag != 'equal'][:4]
            if r < 0.985:
                errs.append(f'text mismatch ratio={r:.4f}: ' + ' | '.join(diffs))
            else:
                warns.append(f'text near-match ratio={r:.4f}: ' + ' | '.join(diffs))
    else:
        warns.append('no original html to compare')
    return errs, warns


def main():
    args = [a for a in sys.argv[1:] if not a.startswith('--')]
    prose_only = '--prose-only' in sys.argv
    folder = Path(args[0])
    pages = sorted(folder.glob('*.v3.html'))
    if not pages:
        print(f'{folder}: no v3 pages found')
        sys.exit(2)
    nfail = 0
    for pg in pages:
        errs, warns = verify_v3_page(pg, prose_only)
        status = 'FAIL' if errs else 'PASS'
        if errs:
            nfail += 1
        line = f'{status} {pg.name}'
        for e in errs:
            line += f'\n      ERR  {e}'
        for w in warns:
            line += f'\n      warn {w}'
        print(line)
    print(f'== {folder}: {len(pages) - nfail}/{len(pages)} pass ==')
    sys.exit(1 if nfail else 0)


if __name__ == '__main__':
    main()
```

- [ ] **Step 2: Verify it fails correctly on a folder with no v3 pages yet**

Run: `python3 docs/superpowers/verify_v3.py most-powerful-signals`
Expected: `most-powerful-signals: no v3 pages found`, exit 2. (Green path is exercised by Task 3's first wave.)

## Task 3: Trailing execution loop

**Files:**
- Modify: this plan file's wave-table Status columns (only file we edit repeatedly)
- Read (never write): `docs/superpowers/plans/2026-08-29-text-viz-migration-plan.md`

Repeat until every wave through 3.4 below is `verified`:

- [ ] **Step 1: Poll Pass A state.** Read the Pass A plan's wave tables. A wave is ELIGIBLE for v3 when its Pass A Status contains `verified`. Process eligible waves in table order, lowest first, skipping waves already `done`/`verified` below.
- [ ] **Step 2: Mark the wave `in-progress`** in this file's table.
- [ ] **Step 3: Derive each folder's page list** (Pass A output = ground truth; also idempotent — pages with an existing v3 are skipped by the agent):

```bash
ls <folder>/*.txt.md | sed -E 's|.*/||; s|\.txt\.md$||'
```

For the partial pilot waves (1.5 first 5, 1.7 first 3, 1.8 first 3) take the first N of that list; the 2.x waves re-run the full folder and skip existing v3s.

- [ ] **Step 4: Invoke the workflow** — one Workflow call per wave, multi-folder args, `scriptPath: docs/superpowers/regenerate-v3-pages.workflow.js`, `args` per the Folder Args Reference below. Wait for the completion notification; do not poll it. If any agent returns `failed`, re-run just that page (resume-safe: existing v3s skip).
- [ ] **Step 5: Verify each folder**

```bash
python3 docs/superpowers/verify_v3.py <folder>            # chart folders
python3 docs/superpowers/verify_v3.py folk-wisdom --prose-only
```

Expected: `== <folder>: N/N pass ==`, exit 0. On FAIL: delete nothing; re-dispatch ONLY the failing page with the error appended to folderRules; re-verify.

- [ ] **Step 6: Mark the wave `verified`** in this file's table with pass counts and any notable `gaps` reported by agents.
- [ ] **Step 7: When no wave is eligible** and waves remain: `sleep 900`, then go to Step 1. Stop when 1.3–3.4 are all `verified` here (2.8/3.x become eligible only after the Pass A session verifies them). Report a final summary to the user.

## Sequencing — Waves (mirrors Pass A; Status column = v3 state)

### Wave 1 — pilot folders (1.1/1.2 excluded from v3 scope)

| Order | Folder | Template | Pages | Status |
|---|---|---|---|---|
| 1.3 | most-powerful-signals/ | 06 | 11 | verified — 11/11 pass verify_v3.py; benign warns: "Example:" label rendered visibly (originals use unlabeled italics; ratio ≥0.992), straight quotes on p10; agents' invented tag-pill CSS varies per page (template 06 has none) — standardized in later waves' folderRules |
| 1.4 | statistical-tests/ | 06 | 9 | verified — 9/9 pass; warns = intentionally-dropped broken back-link (no-nav rule; link was already flagged broken in Pass A) + one visible "Overview" heading |
| 1.5 | folk-wisdom/ (first 5) | 07 prose-only | 5 | verified — 5/5 pass --prose-only, zero warns; fastest wave (~3 min) |
| 1.6 | backlog/programming-languages/ | 10 | 10 | verified — 9/10 mechanical pass + 03-java confirmed-pass (verifier artifact: original's raw `<=` inside `<pre>` is eaten by the checker's tag-stripper; v3 escapes it properly, text is complete) |
| 1.7 | real-world-distributions/ (first 3) | 09 | 3 | verified — 3/3 pass; 44 canvases total; benign warn on 03 ("Source:" labels rendered visibly); agents correctly read three-col-dist.js for helper signatures (shared runtime, allowed) |
| 1.8 | backlog/digital-theft/ (first 3) | 06 + accents | 3 | in-progress — 2/3 pass; 03 failed (printed literal "Key-point —" labels), deleted + re-dispatched inside the wave-2.1 run; prompt now says callout labels are type scaffolding, never visible text |

### Wave 2

| Order | Folders | Pages | Status |
|---|---|---|---|
| 2.1 | metrics/, ml-assumptions/ | 26 | in-progress (run wf_cd7e969c-67b, includes the 1.8 re-dispatch) |
| 2.2 | statistical-paradoxes/, interesting-problems-paradoxes/ | 29 | todo |
| 2.3 | pseudoscience/, cognitive-biases/ | 46 | todo |
| 2.4 | ab-testing/, anti-pattern-pairs/ | 63 | todo |
| 2.5 | folk-wisdom/ (rest), real-world-distributions/ (rest) | 52 | todo |
| 2.6 | ml-pipeline-pitfalls/, common-bad-practices/ | 91 | todo |
| 2.7 | backlog/ details + hubs + remaining subfolders | ~192 | todo |
| 2.8 | root hub pages | 26 | blocked — Pass A todo |

### Wave 3

| Order | Folders | Pages | Status |
|---|---|---|---|
| 3.1 | domains/ | 158 | blocked — Pass A todo |
| 3.2 | recently-added-misc/ (all subfolders + hubs) | ~199 | blocked — Pass A todo |
| 3.3 | tutorials/ grids | 64 | blocked — Pass A todo |
| 3.4 | tutorials/ subfolders (~5 subfolders per run) | 1,207 | blocked — Pass A todo |

### Wave 4 — NOT in v3 scope (fence-only specials + folder CLAUDE.md, handled by Pass A session)

## Folder Args Reference

`baseDir` is always the statsml root. `depth` = path segments in folder. `vizmd: false` only where noted. Templates by Pass A folder→template map.

| Folder | template | sharedJs | depth | folderRules (pass verbatim, plus any re-dispatch notes) |
|---|---|---|---|---|
| most-powerful-signals | 06-sectioned-cards-callout.html | base.js | 1 | Chart-heavy: 8-10 sections, 9-10 charts/page; tag pills per section. |
| statistical-tests | 06-sectioned-cards-callout.html | base.js | 1 | Rich callout kit: failure-box / alt-note / decision-table callouts appear as labeled bold lines in txt.md — render each as a distinct styled callout per the template. |
| folk-wisdom | 07-claim-dissection-cards.html | — | 1 | Prose-only (vizmd:false): txt.md + v3.html only, no script tag. |
| backlog/programming-languages | 10-qa-obj-table.html | base.js | 2 | Mostly canvas-free; pages without a viz.md get no script tag; 3 pages have one canvas each. |
| real-world-distributions | 09-three-col-distribution.html | three-col-dist.js | 1 | Three-col layout is first-class. viz.md Shared js header decides per page: three-col-dist.js lineage vs page-local helpers in LIB fences. Never let a LIB `let rng` collide with base.js rng — use var or rename. |
| backlog/digital-theft | 06-sectioned-cards-callout.html | base.js | 2 | Per-page accent theme: viz.md header names it and quotes hexes — use exactly those (02/03 drift from THEMES.md is intentional). |
| metrics | 05-two-col-catalog-clean.html | base.js | 1 | Objection-table layout; long pages up to 27 rows. |
| ml-assumptions | 05-two-col-catalog-clean.html | base.js | 1 | Single-table variant with tag pills. |
| statistical-paradoxes | 06-sectioned-cards-callout.html | base.js | 1 | — |
| interesting-problems-paradoxes | 05-two-col-catalog-clean.html | base.js | 1 | Briefs may note original-chart bugs in Improve bullets — regenerate the brief's spec, not the bug fix, not the bug (Improve is ignored). |
| pseudoscience | 05-two-col-catalog-clean.html | base.js | 1 | — |
| cognitive-biases | 06-sectioned-cards-callout.html | base.js | 1 | 13-causal-reasoning: quiz-style static markup; its viz.md uses grouped-range brief headers — one VIZ fence per canvas id listed in the range header. |
| ab-testing | 05-two-col-catalog-clean.html | base.js | 1 | Tight-padding variant of 05. |
| anti-pattern-pairs | 06-sectioned-cards-callout.html | base.js | 1 | Plain variant: no tag pills. |
| ml-pipeline-pitfalls | 06-sectioned-cards-callout.html | base.js | 1 | Exactly 3 sections/page. |
| common-bad-practices | 06-sectioned-cards-callout.html | base.js | 1 | — |
| backlog (top-level details) | 06-sectioned-cards-callout.html | base.js | 1 | Kusto-style 2-col text/viz layout; no index numbers in title/h1. |
| backlog (hub/survey pages) | 02-nav-grid.html | base.js | 1 | Rebuild grid from viz.md structure-spec; card hrefs VERBATIM from spec; card index numbers must match file index numbers. |
| backlog/simulation-models, man-in-the-middle-attacks, credential-token-design, data-acquisition-methods, file-formats, data-query-languages | 06-sectioned-cards-callout.html | base.js | 2 | Newer lineage: charts written against setupCanvas(id,w,h). |
| backlog/platform-privacy-policies | 10-qa-obj-table.html | base.js | 2 | Include the disclaimer box (its text is in txt.md). |
| (wave 2.8/3 folders) | per Pass A folder→template map | base.js | per path | Derive at eligibility time from the Pass A plan's map + folder notes. |

## Verification & rollback

- **LESSON (2026-08-30, waves 1.3/1.7):** a workflow stall watchdog kills any agent that goes ~3 min without a tool call. TWO failure modes seen: (a) one giant Write of a 30KB+ page (wave 1.3), (b) long silent composition before the first Write on brief-dense pages (wave 1.7, 15–20 charts/page — 11 agent starts, 0 completions). The workflow prompt therefore REQUIRES: chunk-1 Write immediately after reading inputs, compose one section at a time interleaved with appends (quoted-heredoc `cat >>`), no inter-tool-call gap over ~1 min. Keep this in any future prompt amendment. (Same failure hit the Pass A session's v2 generation.)
- Idempotent: agents skip pages whose `v3.html` exists; a dead run resumes by re-invoking the same wave.
- Rollback = delete `*.v3.html` (only files created); originals, md sources, and v2 pages untouched.
- User commits per folder (all git by user).
- Collision guard with the Pass A session: we never write txt/viz/v2/originals/its plan file; it never writes `*.v3.html`. The only shared read is verify_folder.py (we import, never edit).
