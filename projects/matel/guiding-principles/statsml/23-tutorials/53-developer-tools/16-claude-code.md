# Claude Code

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Claude Code

**Subtitle:** Anthropic's coding agent lives in the terminal, not the editor — you describe a task and it reads files, edits them, and runs commands until the task is done

## The Autonomy Ladder: Autocomplete, Chat, Agent

**Tags:** `core idea` (blue), `terminal agent` (green), `autonomy` (orange)

- **Rung one: autocomplete** — an editor plugin suggests the next line; you still type and run everything
- **Rung two: chat** — a chat window explains the fix; you copy, paste, and test it yourself
- **Rung three: agent** — Claude Code takes a whole task and does the reading, editing, and running itself
- **The home** — it is a CLI: you launch it in a terminal inside your project folder, not inside an editor
- **The loop** — it works in cycles of read files, edit files, run commands, check the result, repeat
- **The leash** — it asks permission before running a command; you approve or reject each step

*Example (italic):* "Fix the failing test in the payments module" is one prompt; the agent then performs a 12-step read-edit-run sequence (illustrative) on its own.

**Key point:** Autocomplete finishes your line and chat finishes your question, but a terminal agent finishes your task — the unit of work moves from a keystroke to a goal.

### Visualization (canvas `c1`, 720×300)

Rising-steps diagram: three rounded boxes climbing left to right, one per rung of the autonomy ladder, each captioned with who does the work.

- **Title (bold 15px, `#1a5276`, top center):** "The Autonomy Ladder: From One Line to One Task".
- **Steps (rounded boxes 190×64, 8px radius, 2px borders, bold 13px `#2c3e50` box titles):**
  - "Autocomplete" at (55, 200), border/fill blue `#2a78d6` / `rgba(42,120,214,0.15)`, 12px second line "suggests the next line"
  - "Chat assistant" at (265, 140), border/fill aqua `#199e70` / `rgba(25,158,112,0.12)`, 12px second line "answers; you paste it in"
  - "Terminal agent" at (475, 80), border/fill green `#008300` / `rgba(0,131,0,0.12)`, 12px second line "reads, edits, runs, repeats"
- **Captions (12px `#6b7280`, centered 16px under each box):** "you do every step", "you do most steps", "you approve steps".
- **Arrows:** 3px `#6b7280` arrows from each box's right edge to the next box's left edge, rising with the steps.
- **Annotation (bold 13px violet `#4a3aa7`, at x≈420, y=262):** "one prompt, a 12-step task — the agent walks the steps".
- **Caption (12px `#444`, bottom right):** "step count illustrative".

## Watching It Fix a Failing Test

**Tags:** `worked example` (blue), `read-edit-run loop` (green)

- **The task** — you type: "the refund test is failing, please fix it" and press enter
- **Step 1: read** — it opens `test_refund.py` and sees the test expects a total of 41.30
- **Step 2: run** — it asks to run the test suite; you approve; result: 24 pass, 1 fail (got 41.29)
- **Step 3: read** — it opens `refund.py` and finds the total is truncated instead of rounded to cents
- **Step 4: edit** — it changes one line to round to 2 decimal places and shows you the diff
- **Step 5: run** — it asks to rerun the suite; you approve; result: 25 pass, 0 fail — loop ends

*Example (italic):* The whole fix is 5 steps and 2 permission prompts; the failure count goes 1 → 0 and the expected 41.30 finally matches.

**Key point:** The agentic loop is observable — each read, edit, and run is printed in the terminal, and every command waits for your yes before it executes.

### Visualization (canvas `c2`, 720×300)

Horizontal bar chart of the 5 loop steps top to bottom, bar length = seconds spent (illustrative), colored by action type, with permission-prompt markers on the two run steps.

- **Title (bold 15px, `#1a5276`, top center):** "One Fix, Five Steps: the Read-Edit-Run Loop in Order".
- **Axis:** vertical 2px `#999` baseline at x=250, bars extend right, x scale 0–24 s over 420px; 12px `#444` step labels left-aligned at x=20.
- **Rows (top to bottom at y = 70, 110, 150, 190, 230; bars 18px tall, 11px second-count labels at bar ends):**
  - "1 read test_refund.py": blue `#2a78d6` bar width 88 (5 s)
  - "2 run tests — 24 pass, 1 fail": green `#008300` bar width 350 (20 s), 12px `#6b7280` "⛨ approve?" marker at the bar's left edge
  - "3 read refund.py": blue bar width 140 (8 s)
  - "4 edit: round to cents": orange `#d95926` bar width 70 (4 s)
  - "5 rerun — 25 pass, 0 fail": green bar width 350 (20 s), same "⛨ approve?" marker
- **Legend (12px, top right under title):** blue "read", orange "edit", green "run (asks permission)".
- **Annotation (bold 13px green `#008300`, right of row 5):** "1 fail → 0".
- **Caption (12px `#444`, bottom right):** "durations illustrative; pass counts from the example".

## Any Editor, Because It Works on Files

**Tags:** `where it's used` (blue), `editor-agnostic` (green), `CLAUDE.md` (orange)

- **Plain files** — it edits the files on disk directly, so there is no plugin to install per editor
- **Your editor watches** — VS Code, Vim, or IntelliJ simply see the files change and reload them
- **Project memory** — a `CLAUDE.md` file in the repo holds standing instructions it reads every session
- **What goes in it** — build commands, style rules, "always run the linter" — written once, applied always
- **Fits the terminal** — because it is a CLI, it slots into shells, SSH sessions, and scripts

*Example (italic):* A team writes "run `make test` before declaring any task done" in `CLAUDE.md` once, and every session in that repo follows the rule without being retold.

**Key point:** Operating on files instead of inside an editor is what makes it editor-agnostic — the filesystem is the integration point, and `CLAUDE.md` is the project's standing brief.

### Visualization (canvas `c3`, 720×300)

Hub diagram: project files in the center, Claude Code reading and writing from the left, three editors on the right all seeing the same files; CLAUDE.md feeding the agent from below.

- **Title (bold 15px, `#1a5276`, top center):** "The Filesystem Is the Integration Point".
- **Hub (center):** rounded box 170×64 at (275, 118), border 2px ink `#1a5276`, fill `rgba(26,82,118,0.10)`, bold 13px label "project files on disk".
- **Left:** rounded box 160×56 at (55, 122), border/fill green `#008300` / `rgba(0,131,0,0.12)`, bold 13px "Claude Code (terminal)"; two 3px arrows to the hub labeled 11px `#6b7280` "reads" (upper) and "writes" (lower).
- **Right (three boxes 140×40 at x=545, y = 62, 130, 198):** blue `#2a78d6` borders, fill `rgba(42,120,214,0.12)`, 12px labels "VS Code", "Vim", "IntelliJ"; one 2px `#6b7280` arrow from the hub to each, labeled once (middle arrow) 11px "same files".
- **Below:** rounded box 150×40 at (285, 232), border/fill orange `#d95926` / `rgba(217,89,38,0.12)`, 12px label "CLAUDE.md rules"; dashed (4/3) 2px orange arrow up-left to the Claude Code box, 11px orange label "read each session".
- **Annotation (bold 12px violet `#4a3aa7`, at x≈470, y=272):** "no editor plugin — the files are the API".

## Not Autocomplete: Review the Steps, Don't Rubber-Stamp

**Tags:** `common mistake` (red), `permissions` (orange)

- **The confusion** — treating each permission prompt like an autocomplete popup and hitting yes on reflex
- **Why it exists** — the prompt is the checkpoint where a wrong plan is cheapest to stop
- **The drift** — approve 10 steps unread (illustrative) and a wrong-direction refactor is 10 steps deep
- **The habit** — read the command and the diff at each prompt; reject and redirect the moment it veers
- **The cheap fix** — a rejection at step 2 costs one sentence of redirection, not an afternoon of rework

*Example (italic):* An agent asked to "clean up tests" starts deleting flaky-but-real tests; the user who reads prompt 2 stops it there, the one who rubber-stamps reviews a 10-file diff.

**Common mistake:** Reviewing an agent like autocomplete. Autocomplete risks one bad line; an agent left unreviewed compounds a bad step — the permission prompt is the review, so use it.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: rubber-stamping every prompt (bad plan runs 10 steps) vs reading prompt 2 (bad plan stopped at step 2), shown as step boxes flowing left to right.

- **Title (bold 15px, `#1a5276`, top center):** "The Prompt Is the Checkpoint: Stop at Step 2, Not Step 10".
- **Row 1 (y=95), label 12px `#444` at x=20:** "rubber-stamp"; blue `#2a78d6` rounded box at x=150 labeled "step 1 ok" (12px), 3px arrow to an orange `#d95926` box at x=310 labeled "step 2 veers — approved unread", 3px arrow to a red `#e74c3c` box at x=530 labeled "10 steps of rework" with bold 12px red "✗ afternoon lost".
- **Row 2 (y=205), label:** "read prompt 2"; blue box at x=150 "step 1 ok", 3px arrow to a green `#008300` box at x=310 labeled "step 2 read — rejected", then arrow to a green box at x=530 labeled "one-line redirect, back on track" with bold 12px green "✓".
- **Box style:** 130–180px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(217,89,38,0.14)` / `rgba(231,76,60,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px magenta `#d55181`, centered near y=270):** "autonomy is borrowed, not surrendered — the prompts are where you lend it".
- **Caption (12px `#444`, bottom right):** "step counts illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all positions, widths, and counts are the hardcoded values above (no randomness); step durations, the 12-step task, and the 10-unread-approvals scenario are invented and labeled illustrative; the product facts (terminal CLI, read-edit-run loop, permission prompts before commands, `CLAUDE.md` project instructions, direct file editing) are Anthropic's publicly documented behavior; the 24/1 → 25/0 test counts and the 41.30 vs 41.29 totals are the worked example's own numbers and must match between text and chart.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
