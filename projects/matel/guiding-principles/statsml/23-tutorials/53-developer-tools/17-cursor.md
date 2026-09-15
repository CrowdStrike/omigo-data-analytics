# Cursor

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Cursor

**Subtitle:** Cursor forks VS Code and redesigns the editor's core moves — completing, editing, refactoring — around an AI model that has read your whole codebase

## The Editor Rebuilt Around the Model

**Tags:** `core idea` (blue), `AI-native editor` (green), `VS Code fork` (orange)

- **The fork** — Cursor starts as a VS Code fork, so extensions, themes, and keybindings carry over
- **The redesign** — instead of a chat panel bolted onto an editor, the editor's primitives are AI-native
- **Tab** — predicts multi-line edits and the next place to jump, not just the next word
- **Inline chat** — select code, describe the change in plain words, and a diff appears in place
- **Agent mode** — describe a task once; the editor plans and edits across many files (Composer)
- **The index** — the codebase is indexed so the model sees your repo, not just the open file

*Example (italic):* You type `def normalize_` and Tab proposes the whole function body — using column names from a file you never opened.

**Key point:** Cursor's bet is that once the model is good enough, completion, editing, and refactoring should be rebuilt around it — not tucked into a sidebar.

### Visualization (canvas `c1`, 720×300)

Hub diagram: the editor core in the center, four AI surfaces around it, with the codebase index feeding context to everything.

- **Title (bold 15px, `#1a5276`, top center):** "Four AI Surfaces Built Into One Editor Core".
- **Center box:** rounded box at x=275, y=125, 170×50, fill `rgba(26,82,118,0.12)`, 2px `#1a5276` border, bold 13px `#1a5276` label "editor core" with 11px `#6b7280` sub-label "(VS Code fork)".
- **Surface boxes (170×44, 8px radius, 12px `#2c3e50` text):**
  - "Tab — multi-line edits" at x=60, y=55, fill `rgba(42,120,214,0.15)`, border `#2a78d6`
  - "inline chat — edit in place" at x=490, y=55, fill `rgba(0,131,0,0.12)`, border `#008300`
  - "codebase index — context" at x=60, y=210, fill `rgba(201,133,0,0.15)`, border `#c98500`
  - "agent mode — multi-file" at x=490, y=210, fill `rgba(74,58,167,0.12)`, border `#4a3aa7`
- **Arrows:** 2px `#6b7280` lines with small arrowheads from each surface box's inner corner to the nearest corner of the center box; the index box's arrow is 3px `#c98500` with a 11px `#c98500` label "feeds every surface" along it.
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=285):** "the model is the primitive, not a plugin".

## Renaming a Function Across Six Files

**Tags:** `worked example` (blue), `agent mode` (green)

- **The task** — rename `getUser` to `fetchUserProfile`: 23 call sites spread across 6 files
- **The prompt** — one instruction in agent mode; the index locates every call site, opened or not
- **The false positive** — a plain string search finds 24 hits; the 24th is `getUserAgent`, excluded
- **The diffs** — the agent proposes one reviewable diff per file: 6, 5, 4, 5, 2, 1 edits
- **Hand-check** — 6+5+4+5+2+1 = 23 renames, matching the 23 real call sites exactly

*Example (italic):* One prompt produces six diffs totaling 23 edits — including the two `getUser` mentions in `docs/api.md` that a code-only rename would skip.

**Key point:** Agent mode turns a repo-wide refactor into one instruction plus a per-file diff review — the index finds the sites; you still approve each change.

### Visualization (canvas `c2`, 720×300)

Horizontal bar chart: rename edits per file for the `getUser` → `fetchUserProfile` refactor, with the excluded false positive marked.

- **Title (bold 15px, `#1a5276`, top center):** "One Prompt, 23 Call Sites, 6 Files".
- **Axis:** bars start at x=230, 2px `#999` vertical baseline there; scale 60px per edit, max width 360.
- **Rows (16px-tall bars at y = 70, 105, 140, 175, 210, 245), each with a left-aligned 12px `#444` file label at x=20 and an 11px count label at the bar end:**
  - "api-client.js": blue `rgba(42,120,214,0.30)` bar width 360 (6 edits)
  - "routes.js": blue bar width 300 (5 edits)
  - "user-service.js": blue bar width 240 (4 edits)
  - "user.test.js": blue bar width 300 (5 edits)
  - "docs/api.md": green `rgba(0,131,0,0.30)` bar width 120 (2 edits) — 11px green `#008300` note "markdown indexed too"
  - "types.d.ts": blue bar width 60 (1 edit)
- **Excluded hit:** 12px red `#e74c3c` text at x=230, y=272: "✗ getUserAgent — 24th string match, excluded by the index".
- **Annotation (bold 13px green `#008300`, right side near y=70):** "23 of 23 renamed, 0 misses".
- **Caption (12px `#444`, bottom right):** "call-site counts illustrative".

## Why the Index Changes the Job

**Tags:** `where it's used` (blue), `codebase index` (green)

- **The open-file trap** — a model that only sees the current file invents names your repo never uses
- **The fix** — indexing the repo lets one prompt correctly reference code you never opened
- **The comparison** — the 23-site rename: ~25 min by hand, ~14 min pasting files into a chat, ~4 min in agent mode
- **Where you meet it** — a data scientist renaming a feature column across pipelines, notebooks, and tests
- **The quiet cost** — the misses live in docs and configs, and surface as Monday-morning breakage

*Example (italic):* The hand-done rename missed the two mentions in `docs/api.md`; the indexed agent caught them because markdown is indexed alongside code.

**Key point:** The redesign only pays off because of context — an indexed codebase is what turns one instruction into a correct multi-file change instead of a plausible wrong one.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart: time to complete the same 23-site rename three ways, with miss counts noted per bar.

- **Title (bold 15px, `#1a5276`, top center):** "The Same Rename, Three Ways".
- **Axis:** bars start at x=200, 2px `#999` vertical baseline; scale 20px per minute, max width 500; 12px `#444` axis labels "0", "10 min", "20 min" at x=200/400/600 along y=270.
- **Rows (18px-tall bars at y = 80, 150, 220), each with a left-aligned 12px `#444` label at x=20 and an 11px minute label at the bar end:**
  - "manual search & replace": red `rgba(231,76,60,0.30)` bar width 500 (25 min), 12px red `#e74c3c` note "2 doc sites missed"
  - "file-by-file chat plugin": orange `rgba(217,89,38,0.30)` bar width 280 (14 min), 12px `#d95926` note "paste each file by hand"
  - "agent mode + index": green `rgba(0,131,0,0.35)` bar width 80 (4 min), bold 12px green `#008300` note "0 misses"
- **Annotation (bold 13px magenta `#d55181`, right side near y=250):** "the index turns a hunt into one prompt".
- **Caption (12px `#444`, bottom right):** "minutes illustrative".

## Not Just Autocomplete — and Not Done Work Either

**Tags:** `common mistake` (red), `review the diff` (orange)

- **Confusion one** — "it's just autocomplete": Tab predicts edits and jump targets; agents plan across files
- **Confusion two** — "it's just chat": the changes land in your files as diffs, not in a transcript to copy
- **The real risk** — agent mode writes plausible multi-file diffs, and plausible is not the same as correct
- **The habit** — review every generated diff file by file, exactly like a teammate's pull request
- **The mistake** — clicking accept-all on a 6-file change and meeting the broken build an hour later

*Example (italic):* The 6-file rename also "helpfully" changed a default value in `types.d.ts` — only the per-file review caught it before it shipped.

**Common mistake:** Treating agent output as finished work. Cursor presents every change as a reviewable diff for a reason — accept-all without reading trades typing time for debugging time.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: the same 6-file agent diff applied without review (build breaks) vs with per-file review (one line rejected, build green).

- **Title (bold 15px, `#1a5276`, top center):** "The Same 6-File Diff: Accept-All vs Review".
- **Row 1 (y=95), label 12px `#444` at x=20:** "accept all, no review"; blue `#2a78d6` rounded box at x=180 labeled "23 renames + 1 extra change" (12px), 3px arrow to a red `#e74c3c` box at x=430 labeled "extra default change ships" with bold 12px red "✗ build breaks an hour later".
- **Row 2 (y=205), label:** "review each diff"; blue box "23 renames + 1 extra change", 3px arrow to a green `#008300` box at x=360 labeled "reject 1 line in types.d.ts", then arrow to a green box at x=560 labeled "build green" with bold 12px green "✓".
- **Box style:** 150–170px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(231,76,60,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px orange `#d95926`, centered near y=270):** "the agent drafts; the review is still your job".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all values are the hardcoded arrays above (no randomness); the per-file edit counts (6/5/4/5/2/1 = 23, plus the 24th excluded string hit) and the workflow minutes (25 / 14 / 4) are invented and labeled illustrative. Product facts (VS Code fork, Tab multi-line prediction, inline chat, Composer/agent mode, codebase indexing) are publicly documented; do not invent undocumented behavior.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
