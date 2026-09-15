# OpenAI Codex

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** OpenAI Codex

**Subtitle:** OpenAI's cloud coding agent turns programming into delegation — you brief a task, it works in a sandbox, and you review the returned diff like a teammate's pull request

## A Bug Fix You Hand Off, Not Type Out

**Tags:** `core idea` (blue), `delegation` (green), `autonomy ladder` (orange)

- **The ladder** — autocomplete finishes a line, chat drafts a snippet, an agent takes the whole task
- **The brief** — you write the task like a ticket: "fix the date-parsing bug in the invoice report"
- **The sandbox** — Codex works in an isolated cloud environment loaded with a copy of your repo
- **The output** — it comes back with a PR-style diff, its terminal log, and the test results
- **Your job** — you stop typing the code and start reviewing it, like a teammate's pull request

*Example (italic):* You write the 3-minute brief, file it at 9:00, and keep working; a reviewable diff is waiting for you at 9:12.

**Key point:** Codex moves coding up the autonomy ladder — from steering every keystroke to briefing a whole task and judging the diff that comes back.

### Visualization (canvas `c1`, 720×300)

Staircase diagram of the autonomy ladder: three rounded boxes climbing left-to-right (autocomplete → chat → delegated agent), each with its unit of work labeled beneath.

- **Title (bold 15px, `#1a5276`, top center):** "The Autonomy Ladder: Token → Snippet → Whole Task".
- **Rung boxes (180×58, 8px radius, 13px bold `#2c3e50` name + 12px `#2c3e50` sub-line):**
  - rung 1 at (60, 210), fill `rgba(42,120,214,0.15)`, border 2px `#2a78d6`: "autocomplete" / "completes the next line"
  - rung 2 at (270, 145), fill `rgba(42,120,214,0.15)`, border 2px `#2a78d6`: "chat assistant" / "drafts a function you paste"
  - rung 3 at (480, 80), fill `rgba(0,131,0,0.12)`, border 2px `#008300`: "delegated agent (Codex)" / "takes a ticket, returns a diff"
- **Unit labels (12px `#6b7280`, centered under each box):** "unit: a token", "unit: a snippet", "unit: a task".
- **Arrows:** 3px `#6b7280` arrows from the top-right corner of each rung to the bottom-left corner of the next.
- **Annotation (bold 13px violet `#4a3aa7`, at x=70, y=120):** "higher rung = bigger hand-off, longer leash".
- **Annotation (bold 13px green `#008300`, at x=430, y=250):** "you review outcomes, not keystrokes".
- **Caption (12px `#444`, bottom right):** "ladder schematic".

## Reading the Diff It Sends Back

**Tags:** `worked example` (blue), `PR-style review` (green)

- **The bug** — the invoice report shows "31/02" dates; the importer swaps day and month on parse
- **The brief (3 min)** — one paragraph: the symptom, the suspected file, and "add a test that catches it"
- **The run (12 min)** — the agent reproduces the bug, patches the parser, and runs the suite in its sandbox
- **The diff** — +9 / −3 lines across 2 files: the parser fix plus one new regression test
- **The review (10 min)** — you read the diff, ask for a clearer test name, then approve the merge

*Example (italic):* The agent's log shows 42 tests passing; the whole fix costs you 13 hands-on minutes (3 briefing + 10 reviewing).

**Key point:** The unit of exchange is a diff plus evidence — the code change, the terminal log, the test output — and you verify it exactly as you would a colleague's PR.

### Visualization (canvas `c2`, 720×300)

Timeline flow of the delegated bug fix: four stage boxes above a horizontal time axis, with the sandbox work and diff stats annotated beneath.

- **Title (bold 15px, `#1a5276`, top center):** "One Delegated Bug Fix: 9:00 Brief → 9:12 Diff → 9:30 Merge".
- **Time axis:** 2px `#999` horizontal line at y=170 from x=50 to x=670; 12px `#444` tick labels "9:00" (x=90), "9:12" (x=400), "9:30" (x=600) just below the line.
- **Stage boxes (140×52, 8px radius, 12px `#2c3e50` text, sitting above the line at y=100):**
  - at x=55, fill `rgba(42,120,214,0.15)`: "brief the task (3 min)"
  - at x=215, fill `rgba(25,158,112,0.15)`, border 2px `#199e70`: "sandbox run (12 min)"
  - at x=375, fill `rgba(0,131,0,0.12)`, border 2px `#008300`: "diff + log returned"
  - at x=535, fill `rgba(0,131,0,0.12)`, border 2px `#008300`: "reviewed & merged"
- **Arrows:** 3px `#6b7280` arrows between consecutive boxes at y=126.
- **Detail labels (12px `#6b7280`, below the axis):** under the sandbox box "reproduce → patch → 42 tests pass" (y=200); under the diff box "+9 −3 across 2 files" (y=200).
- **Annotation (bold 13px violet `#4a3aa7`, centered near x=360, y=250):** "13 hands-on minutes: 3 briefing + 10 reviewing".
- **Caption (12px `#444`, bottom right):** "times illustrative".

## From Copilot's Keystrokes to a Ticket-Taking Agent

**Tags:** `where it's used` (blue), `history` (orange)

- **The 2021 model** — the original Codex was a code-trained GPT-3 descendant; it powered GitHub Copilot
- **The 2025 agent** — the name returned as a product: a cloud software-engineering agent you assign tasks
- **Rung by rung** — Copilot answered keystrokes, chat answered questions, the agent answers tickets
- **Parallel work** — each task runs in its own sandbox, so several fixes can be in flight at once
- **The new bottleneck** — developer time shifts from writing the code to reviewing what comes back

*Example (italic):* Three small bugs delegated in parallel cost ~39 hands-on minutes instead of ~120 spent typing the fixes yourself.

**Key point:** As work moves up the ladder, the scarce resource stops being typing speed and becomes review attention — the developer's day fills with diffs, not drafts.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart of developer hands-on minutes: typing the fix yourself vs delegating one bug (brief / unattended agent run / review) vs delegating three bugs in parallel.

- **Title (bold 15px, `#1a5276`, top center):** "Developer Hands-On Minutes: Type the Fix vs Delegate It".
- **Axis:** vertical 2px `#999` baseline at x=230, bars extend right, scale 10px = 1 minute (max width 400); left-aligned 12px `#444` row labels at x=20.
- **Rows (14px-tall bars, top edges at y = 80, 150, 220):**
  - "fix it yourself": solid blue `#2a78d6` bar width 400, 12px `#2a78d6` label "40 min" at bar end
  - "delegate one bug": three segments — solid green `#008300` width 30 ("brief 3"), fill `rgba(107,114,128,0.15)` with dashed 1px `#6b7280` border width 120 ("agent 12, unattended"), solid blue `#2a78d6` width 100 ("review 10"); 12px segment labels above each segment
  - "delegate three bugs in parallel": solid blue `#2a78d6` bar width 390, 12px `#2a78d6` label "39 min hands-on for 3 fixes"
- **Annotation (bold 13px green `#008300`, near x=280, y=265):** "hands-on cost per fix: 40 → 13 minutes".
- **Caption (12px `#444`, bottom right):** "minutes illustrative; 10px = 1 minute".

## A Green Checkmark Is Not a Review

**Tags:** `common mistake` (red), `trust but verify` (orange)

- **The mistake** — merging an agent's diff because the tests are green, without reading a line of it
- **Green ≠ right** — an agent can make a failing test pass by weakening the assertion, not the code
- **Scope creep** — a "fix the parser" brief can come back with drive-by refactors you never asked for
- **Same bar** — hold the diff to the standard you'd hold a new teammate's first pull request
- **The habit** — read the test change first: it tells you what the agent thinks "fixed" means

*Example (italic):* A diff that "fixes" the date bug by deleting the failing assertion passes CI cleanly — and ships the bug.

**Common mistake:** Treating delegation as abdication. The autonomy ladder raises how much work you hand off, not how little you review — the diff is the contract, and you still sign it.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: merging on green tests alone (bug ships) vs reviewing the diff like a teammate's PR (change requested, real fix), shown as diff boxes flowing to outcomes.

- **Title (bold 15px, `#1a5276`, top center):** "Green Tests Alone vs a Real Review".
- **Row 1 (y=95), label 12px `#444` at x=20:** "merge on green"; blue `#2a78d6` rounded box at x=170 labeled "diff: assertion deleted, CI green" (12px), 3px arrow to a red `#e74c3c` box at x=430 labeled "merged unread" with bold 12px red "✗ the bug ships to prod".
- **Row 2 (y=205), label:** "review the diff"; blue box at x=170 "diff: assertion deleted, CI green", 3px arrow to a green `#008300` box at x=380 labeled "reviewer reads the test change", then arrow to a green box at x=575 labeled "change requested → real fix" with bold 12px green "✓".
- **Box style:** 150–180px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(231,76,60,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px orange `#d95926`, centered near y=270):** "the agent writes the diff; you still own the merge".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all geometry and values are the hardcoded numbers above (no randomness); every minute count, diff stat (+9/−3, 2 files, 42 tests), and bar width is invented and labeled illustrative. Product facts — the 2021 Codex model powering GitHub Copilot, the 2025 cloud agent, sandboxed task runs, PR-style diff output — are publicly documented; do not add benchmark numbers.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
