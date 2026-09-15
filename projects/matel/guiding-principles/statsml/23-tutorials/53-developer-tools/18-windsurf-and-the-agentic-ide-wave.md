# Windsurf & the Agentic-IDE Wave

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Windsurf & the Agentic-IDE Wave

**Subtitle:** Windsurf's Cascade bet that the editor itself should plan and act across files, not just autocomplete — within a year every major editor had an agent mode

## The Editor That Takes the Wheel

**Tags:** `core idea` (blue), `agentic flow` (green), `Windsurf` (orange)

- **The assistant** — classic AI coding help suggests the next few lines in the file you're typing in
- **The agent** — you state a goal; the editor plans steps, opens files, edits, and runs commands itself
- **Cascade** — Windsurf (from Codeium, launched 2024) built this in as its core "agentic flow"
- **The memory** — Cascade keeps its own running model of what you're doing, so it acts in context
- **The loop** — plan, edit, run, read the output, fix — the same loop a human developer runs by hand

*Example (italic):* You type "add a retry to every network call in this service"; the agent finds the calls, edits four files, and shows one combined diff.

**Key point:** An agentic IDE doesn't just predict text — it holds a goal, tracks its own actions across files, and works the edit-run-fix loop until the goal is met.

### Visualization (canvas `c1`, 720×300)

Two-lane flow diagram contrasting the assistant loop (one file, human drives every step) with the agentic loop (multi-file, the editor drives and the human reviews).

- **Title (bold 15px, `#1a5276`, top center):** "Assistant Suggests One Line; Agent Works the Whole Loop".
- **Row 1 (boxes centered on y=105), label 12px `#444` at x=20:** "assistant"; blue `#2a78d6` rounded boxes left to right at x=120, 300, 480 labeled "you type" / "it suggests" / "you accept" (12px), 3px `#6b7280` arrows between; 12px `#6b7280` label "one file, you drive" at x=600, y=105.
- **Row 2 (boxes centered on y=210), label:** "agent"; green `#008300` rounded boxes at x=110, 250, 390, 530 labeled "you set a goal" / "it plans" / "edits + runs" / "you review diff", 3px arrows; a curved 2px dashed `#d95926` arrow from "edits + runs" back to "it plans" with 12px orange label "fix loop".
- **Box style:** 110–130px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` (row 1) / `rgba(0,131,0,0.12)` (row 2), 12px `#2c3e50` text.
- **Annotation (bold 13px green `#008300`, centered near y=270):** "the agent drives the loop; you review the result".

## One Rename, Six Files, Zero Manual Edits

**Tags:** `worked example` (blue), `multi-file edit` (green)

- **The goal** — rename the field `customer_id` to `account_id` across a small order service
- **The plan** — the agent searches the repo and finds 23 references spread over 6 files
- **The edits** — it changes all 23 in one pass: 8 in models, 5 in handlers, 4 in queries, 3 in tests, 2 in config, 1 in docs
- **The check** — it runs the 41-test suite; 2 tests fail on an old fixture name, so it fixes those and reruns
- **The result** — second run: 41 of 41 pass; you review one diff instead of hand-editing six files

*Example (italic):* Hand-check the count: 8 + 5 + 4 + 3 + 2 + 1 = 23 edits — the same 23 references the search found, so nothing was missed.

**Key point:** The agent's value is the bookkeeping — finding every reference, editing them consistently, and using the test suite to prove it, while you check one diff at the end.

### Visualization (canvas `c2`, 720×300)

Horizontal bar chart of edits per file for the rename, with a test-run tally on the right showing the fail-then-pass loop.

- **Title (bold 15px, `#1a5276`, top center):** "One Goal, 23 Edits Across 6 Files (illustrative)".
- **Axis:** vertical 2px `#999` baseline at x=200, bars extend right, scale 40px per edit (max width 320 for 8 edits); left-aligned 12px `#444` file labels at x=20.
- **Rows (top to bottom at y = 70, 100, 130, 160, 190, 220):** "models.py" edits `8`, "handlers.py" `5`, "queries.py" `4`, "tests/" `3`, "config" `2`, "docs" `1`; bars 16px tall, fill `rgba(42,120,214,0.30)` with 2px `#2a78d6` edge, bold 12px `#2a78d6` count labels at bar ends.
- **Test tally (right side, x=560):** bold 13px red `#e74c3c` "run 1: 39/41 pass" at y=110; bold 13px green `#008300` "run 2: 41/41 pass" at y=150; 2px dashed `#6b7280` arrow between them with 11px `#6b7280` label "agent fixes fixtures".
- **Annotation (bold 13px violet `#4a3aa7`, near x=210, y=250):** "8+5+4+3+2+1 = 23 — matches the 23 references found".
- **Caption (12px `#444`, bottom right):** "file names and counts illustrative".

## From One Product's Bet to Everyone's Feature

**Tags:** `where it's used` (blue), `2024–25 wave` (green)

- **The bet** — in late 2024 Windsurf shipped Cascade as the product's centerpiece, not a plugin
- **Cursor** — Cursor added its own agent mode to Composer in roughly the same season
- **VS Code** — GitHub Copilot gained an agent mode in VS Code, previewed in early 2025
- **JetBrains** — JetBrains announced Junie, a coding agent for its IDEs, in 2025
- **The pattern** — "agentic IDE" went from one product's differentiator to a category checkbox in about a year

*Example (italic):* A feature that defined one editor in late 2024 appeared, under different names, in every major editor's release notes by mid-2025.

**Key point:** When one product's bet is copied by every incumbent within a year, the bet has become the category — the question shifted from "should an IDE act?" to "how well does yours act?".

### Visualization (canvas `c3`, 720×300)

Timeline chart of agent-mode launches across editors, one dot per product on a shared 2024–2025 axis, showing the cluster.

- **Title (bold 15px, `#1a5276`, top center):** "Agent Modes Arrive Everywhere Within ~a Year".
- **Axis:** horizontal 2px `#999` baseline at y=245, from x=60 to x=660; 12px `#444` tick labels "mid-2024", "late 2024", "early 2025", "mid-2025" at x = 60, 260, 460, 660; gridlines `#e5e9ef` vertical at each tick.
- **Markers (10px filled circles with 12px `#2c3e50` labels above, stems 2px down to the baseline):** Windsurf Cascade — blue `#2a78d6` at x=270, y=110; Cursor agent mode — green `#008300` at x=310, y=160; VS Code Copilot agent mode — orange `#d95926` at x=480, y=110; JetBrains Junie — violet `#4a3aa7` at x=530, y=160.
- **Bracket:** 2px dashed `#6b7280` horizontal bracket from x=270 to x=530 at y=70 with bold 12px `#6b7280` label "under a year" centered above it.
- **Annotation (bold 13px magenta `#d55181`, near x=430, y=215):** "one bet became the checkbox".
- **Caption (12px `#444`, bottom right):** "positions approximate — public announcement seasons, not exact dates".

## Autopilot Still Needs a Pilot

**Tags:** `common mistake` (red), `review` (orange)

- **The mistake** — treating agent mode like autocomplete and clicking accept-all on a 12-file diff
- **The scale** — an agent run can touch hundreds of lines; unread lines are unreviewed changes
- **The risk** — a plausible-looking edit can quietly change behavior far from the file you asked about
- **The habit** — review the agent's diff the way you'd review a junior developer's pull request
- **The tell** — passing tests only prove what the tests cover; they don't prove the diff says what you meant

*Example (italic):* An agent run changes 500 lines (illustrative); a reviewer who skims 50 of them has shipped 450 lines nobody read.

**Common mistake:** Confusing "the agent finished and tests pass" with "the change is correct" — the agentic loop removes the typing, not the responsibility for the diff.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: an agent run merged with accept-all (unread change ships) vs the same run with a real diff review (change caught before merge).

- **Title (bold 15px, `#1a5276`, top center):** "Same Agent Run, Two Endings: Accept-All vs Review".
- **Row 1 (boxes centered on y=95), label 12px `#444` at x=20:** "accept-all"; blue `#2a78d6` rounded box at x=160 labeled "agent edits 12 files" (12px), 3px arrow to a blue box at x=350 labeled "tests pass", 3px arrow to a red `#e74c3c` box at x=530 labeled "merged unread" with bold 12px red "✗ surprise change ships".
- **Row 2 (boxes centered on y=205), label:** "review"; blue box "agent edits 12 files", arrow to a green `#008300` box at x=350 labeled "diff read file by file", arrow to a green box at x=540 labeled "1 bad edit caught" with bold 12px green "✓".
- **Box style:** 140–160px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(231,76,60,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px orange `#d95926`, centered near y=270):** "the agent removes the typing, not the review".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded values above (no randomness); rename edit counts (8/5/4/3/2/1 = 23), test tallies (39/41 then 41/41), and the 500-line/50-read numbers are invented and labeled illustrative; the c2 sum check 8+5+4+3+2+1 = 23 is exact; c3 timeline positions represent publicly announced launch seasons (Windsurf Cascade late 2024; Cursor agent mode late 2024; VS Code Copilot agent mode early 2025; JetBrains Junie 2025) and are labeled approximate — name real products only for these documented facts.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
