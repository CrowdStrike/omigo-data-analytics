# AI Coding Tools

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** AI Coding Tools

**Subtitle:** The same model shows up in three shapes — autocomplete finishing your line, a chat you paste code into, and an agent that edits the files itself

## Three Shapes, One Model Underneath

**Tags:** `core idea` (blue), `coding assistants` (green)

- **Autocomplete** — gray ghost-text finishes the line as you type; you accept or keep typing
- **Chat** — you ask in a side panel, paste code in, copy the answer back yourself
- **Agent** — the tool reads files, edits them, and runs tests in a loop from one instruction
- **Same engine** — all three shapes can sit on the same underlying model
- **What differs** — how much the tool does per request, and who touches the files

*Example (italic):* Copilot began as autocomplete, chat sites popularized the second shape, and Claude Code works as an agent — three interfaces to one kind of engine.

**Key point:** "Which AI coding tool?" is mostly "which shape?" — the shape decides how much happens per request, not the model name.

### Visualization (canvas `c1`, 720×300)

Three side-by-side panels, one per shape, each showing who does what per request.

- **Title (bold 15px, `#1a5276`, top center):** "Three Shapes of Coding Assistant".
- **Panels:** three rounded rects 210×186 at x = `[30, 255, 480]`, y=48; borders 2px in blue `#2a78d6`, aqua `#199e70`, green `#008300`; fills `rgba(42,120,214,0.06)`, `rgba(25,158,112,0.06)`, `rgba(0,131,0,0.06)`; bold 13px colored headers centered at y=72: "autocomplete", "chat", "agent".
  - **Panel 1 lines (12px `#2c3e50`, centered x=135, y=98/116/134):** "you type in the editor", "ghost-text suggests the rest", "you accept key by key"; mini editor sketch: rect 150×34 at (60,148), 1px `#6b7280` border; 12px `#2c3e50` left text "total = pri" at (68,169) followed by 12px `#6b7280` italic "ce + tax" (the ghost).
  - **Panel 2 lines (centered x=360, y=98/116/134):** "you paste code into a panel", "the model answers in text", "you copy the fix back"; two small arrows between "you" and "model": 1.5px `#6b7280` arrow right from (310,155) to (410,155) labeled 11px "paste" above, arrow left from (410,175) to (310,175) labeled 11px "copy" below.
  - **Panel 3 lines (centered x=585, y=98/116/134):** "you give one instruction", "the tool reads, edits, runs", "you review the final diff"; loop sketch: bold 12px `#008300` centered "read → edit → test → repeat" at (585,162).
- **Per-panel footer (bold 11px, centered, y=218):** "hands on files: yours" (blue, panels 1–2 at x=135 and x=360) and "hands on files: the tool's" (green, panel 3 at x=585).
- **Annotation (bold 12px orange `#d95926`, centered at y=262):** "same model underneath — the shape decides how much happens per request".
- **Caption (11px `#444`, bottom right, y=290):** "interfaces simplified".

## One Rename, Three Ways

**Tags:** `worked example` (blue), `autocomplete vs agent` (orange)

- **The errand** — rename a config field used in 5 files of a small project, then re-run the tests
- **Autocomplete** — you open all 5 files and fix each use; it speeds the typing: ~20 hand actions
- **Chat** — you paste each file, copy each answer back, run tests yourself: ~12 hand actions
- **Agent** — one instruction; the tool finds the 5 files, edits, runs the tests: ~2 hand actions
- **The flip side** — the agent's 2 actions are an instruction and a careful diff review

*Example (italic):* The chat version fails if you forget to paste the fifth file — the model can't see a file you never showed it.

**Key point:** Hand actions drop from ~20 to ~2 across the three shapes — but the last action left is reviewing what the tool did.

### Visualization (canvas `c2`, 720×300)

Bar chart: hand actions needed for the same 5-file rename under each shape, with who-runs-the-tests noted under each bar.

- **Title (bold 15px, `#1a5276`, top center):** "Same 5-File Rename: Your Hand Actions (illustrative)".
- **Axes:** baseline y=235, plot top y=70; y = hand actions 0–20 with `#e5e9ef` gridlines at 5/10/15/20 and 12px `#444` right-aligned tick labels at x=64; axis lines 1px `#999` from (70,70) to (70,235) to (660,235).
- **Bars (90px wide, centered x = `[200, 390, 580]`):** heights scaled 8.25px per action:
  - "autocomplete" 20 actions, fill `#2a78d6`
  - "chat" 12 actions, fill `#199e70`
  - "agent" 2 actions, fill `#008300`
  - bold 13px value labels above each bar in the bar's color: "~20", "~12", "~2".
- **X labels:** 12px `#444` main label centered at y=254 ("autocomplete", "chat", "agent") and 11px `#6b7280` descriptor at y=270 ("you visit every file", "you paste & copy", "you instruct & review").
- **Annotation (bold 12px orange `#d95926`, centered at (390, 92)):** "the agent's two actions: one instruction, one diff review".
- **Caption (11px `#444`, bottom right, y=292):** "action counts illustrative".

## Where the Tools Live

**Tags:** `where it's used` (blue), `IDE & CLI` (green)

- **IDE plugin** — Copilot adds autocomplete, chat, and an agent mode inside an existing editor
- **The whole editor** — Cursor is itself an editor, built around chat and agent modes
- **Terminal** — Claude Code, Codex CLI, and Aider run as agents from the command line
- **Web & cloud** — chat sites for snippets; cloud agents work on a repo without your machine
- **Blurred lines** — most named tools now ship more than one shape under one name

*Example (italic):* "We use Copilot" can mean ghost-text, a chat panel, or an agent run — the brand no longer pins down the shape.

**Key point:** Tools differ by surface (editor, terminal, web) and shape (autocomplete, chat, agent) — name-dropping a brand specifies neither.

### Visualization (canvas `c3`, 720×300)

Grid map: rows are surfaces (IDE, terminal, web/cloud), columns are shapes (autocomplete, chat, agent), with named tools placed in the cells they are best known for.

- **Title (bold 15px, `#1a5276`, top center):** "The Tool Map: Surface × Shape".
- **Column headers (bold 12px, centered, y=64):** "autocomplete" `#2a78d6` at x=250, "chat" `#199e70` at x=410, "agent" `#008300` at x=570; **row headers (bold 12px `#1a5276`, right-aligned x=160):** "in the editor" at y=110, "in the terminal" at y=170, "web / cloud" at y=230.
- **Grid lines:** 1px `#e5e9ef` rectangle borders for a 3×3 cell grid spanning x=175..645, rows centered on y=110/170/230 (cell height 52, tops y=84/144/204; column cells 150 wide, left edges x=175, 335, 495).
- **Cell entries (11px `#2c3e50`, centered in each cell, up to two lines):**
  - editor × autocomplete: "Copilot ghost-text"; editor × chat: "Copilot chat, Cursor chat"; editor × agent: "Cursor agent, Copilot agent"
  - terminal × autocomplete: "—" (`#6b7280`); terminal × chat: "—" (`#6b7280`); terminal × agent: "Claude Code, Codex CLI, Aider"
  - web × autocomplete: "—" (`#6b7280`); web × chat: "chat sites"; web × agent: "cloud coding agents"
- **Annotation (bold 12px orange `#d95926`, centered at y=272):** "one brand often spans several cells — ask which shape someone actually means".
- **Caption (11px `#444`, bottom right, y=292):** "placements by best-known use, simplified".

## Picking a Shape for the Job

**Tags:** `rule of thumb` (green), `oversight` (orange)

- **Mid-typing** — autocomplete: you know what to write and want the keystrokes saved
- **A question** — chat: explain this error, draft this function, review this snippet
- **A multi-file errand** — agent: rename, upgrade, fix-and-test across the repo
- **The trade** — the more the tool does alone, the more your job becomes reviewing diffs and tests
- **Not a ladder** — agent mode is overkill for one line; autocomplete can't do an errand

*Example (italic):* Alice drafts a function with autocomplete, asks chat why a test fails, and hands the ten-file version bump to the agent — three shapes in one afternoon.

**Common mistake:** Judging every tool on one axis of "smartness" — a great autocomplete and a great agent are optimized for opposite jobs.

### Visualization (canvas `c4`, 720×300)

Spectrum diagram: the three shapes placed along a "tool does more alone" axis, with your role shifting from typing to reviewing.

- **Title (bold 15px, `#1a5276`, top center):** "More Autonomy, Different Oversight".
- **Axis:** 2px `#6b7280` horizontal arrow from (60,150) to (660,150) with arrowhead; bold 12px `#6b7280` label "tool does more alone →" centered at (360,132).
- **Shape markers:** three rounded rects 130×40 centered at x = `[150, 360, 570]`, top y=160; fills `rgba(42,120,214,0.12)` / `rgba(25,158,112,0.12)` / `rgba(0,131,0,0.12)`, 2px borders `#2a78d6` / `#199e70` / `#008300`; bold 12px centered labels in the border colors: "autocomplete", "chat", "agent" at y=184.
- **Per-shape task labels (11px `#6b7280`, centered above the axis at y=112, same x centers):** "finish this line", "answer this question", "do this errand".
- **Your-role labels (bold 11px, centered below markers at y=222, same x centers):** "you: type & accept" `#2a78d6`, "you: paste & copy" `#199e70`, "you: instruct & review" `#008300`.
- **Annotation (bold 12px orange `#d95926`, centered at y=258):** "your keystrokes shrink, your reviewing grows — oversight never reaches zero".
- **Caption (11px `#444`, bottom right, y=290):** "roles simplified".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** the c2 bars must read ~20 / ~12 / ~2 to match the text; tool placements in c3 stick to well-known documented shapes/surfaces (Copilot in-editor, Cursor as an editor, Claude Code / Codex CLI / Aider in the terminal); no other behavior claims about named tools.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
