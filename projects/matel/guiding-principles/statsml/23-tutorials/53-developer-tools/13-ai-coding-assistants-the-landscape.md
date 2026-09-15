# AI Coding Assistants — The Landscape

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** AI Coding Assistants — The Landscape

**Subtitle:** Every AI coding tool sits on one ladder — suggest, converse, delegate, replace — and each rung moves authorship from you to the tool while raising your review burden

## The Ladder: Suggest, Converse, Delegate, Replace

**Tags:** `core idea` (blue), `autonomy levels` (green), `hub` (orange)

- **The task** — a developer must add rate limiting to a login endpoint before Friday's release
- **Level 1, suggest** — ghost text completes lines as she types; she accepts or rejects token by token
- **Level 2, converse** — a chat grounded in the repo explains and drafts a diff; she applies it herself
- **Level 3, delegate** — an agent takes the whole task, plans, edits files, runs tests, and iterates
- **Level 4, replace** — humans only at requirements and acceptance; today this is largely aspirational
- **One axis** — feature lists differ, but autonomy level is what actually organizes the tool space

*Example (italic):* The same "add rate limiting" task exists at every level — what changes is who authors the code and who reviews it.

**Key point:** The axis that organizes AI coding tools is autonomy: suggest → converse → delegate → replace. Each step up hands more authorship to the tool and more reviewing to you.

### Visualization (canvas `c1`, 720×300)

Ascending staircase diagram: four rounded boxes rising left to right, one per autonomy level, with the human's remaining role labeled under each step.

- **Title (bold 15px, `#1a5276`, top center):** "Four Levels of Autonomy: Who Writes the Code?".
- **Steps (rounded boxes, 150px wide, 54px tall, 8px radius, bold 13px `#2c3e50` two-line labels):** level 1 at (x=40, y=200) fill `rgba(42,120,214,0.15)`, border 2px `#2a78d6`, text "1 SUGGEST / inline ghost text"; level 2 at (x=205, y=152) fill `rgba(25,158,112,0.15)`, border 2px `#199e70`, text "2 CONVERSE / repo-aware chat"; level 3 at (x=370, y=104) fill `rgba(0,131,0,0.15)`, border 2px `#008300`, text "3 DELEGATE / agent takes a task"; level 4 at (x=535, y=56) fill `rgba(217,89,38,0.10)`, border 2px dashed (dash 5/4) `#d95926`, text "4 REPLACE / end-to-end auto".
- **Role captions (12px `#6b7280`, centered 16px under each box):** "you author" / "you edit" / "you review the PR" / "you accept".
- **Connectors:** 2px `#6b7280` arrows from each box's top-right corner to the next box's bottom-left corner.
- **Annotation (bold 12px orange `#d95926`, above level 4 box):** "level 4: mostly aspirational today".
- **Annotation (bold 13px `#4a3aa7` violet, lower right near y=265):** "current frontier: level 3 — terminal agents, agentic IDE modes, PR bots".

## One Rate Limiter, Four Ways

**Tags:** `worked example` (blue), `writing vs reviewing` (green)

- **The clock** — the same login rate-limiter task, timed at each level (all minutes illustrative)
- **Suggest** — she types for 32 min; reading the accepted ghost-text completions adds 6 min of review
- **Converse** — she writes 20 min herself and spends 14 min reading the diffs the chat drafted
- **Delegate** — a 4-min task prompt; the agent edits three files and runs tests; she reviews for 20 min
- **Replace** — 0 min of code; she spends 25 min on acceptance criteria and checking the result
- **Hand-check** — at level 3, review time (20 min) is 5× writing time (4 min): the work moved, it did not vanish

*Example (italic):* Going from suggest to delegate cuts her writing from 32 min to 4, but her reviewing grows from 6 min to 20 — she reads the agent's work like a teammate's pull request.

**Key point:** Higher autonomy does not delete the work — it converts writing time into review time, and reviewing code you did not write is its own skill.

### Visualization (canvas `c2`, 720×300)

Grouped horizontal bar chart: minutes writing (blue) vs minutes reviewing (orange) for the same rate-limiter task at each autonomy level.

- **Title (bold 15px, `#1a5276`, top center):** "Same Task, Four Levels: Writing Time Falls, Review Time Climbs".
- **Layout:** row labels left-aligned 12px `#444` at x=20; bars start at x=175, scale 12.5 px per minute (max width ~440); each row has a blue bar (writing) above an orange bar (reviewing), 13px tall, 4px gap; 11px `#444` minute labels at bar ends.
- **Rows (row tops at y = 62, 112, 162, 212):**
  - "1 suggest": blue `rgba(42,120,214,0.75)` bar width 400 ("32 min"), orange `#d95926` bar width 75 ("6 min")
  - "2 converse": blue bar width 250 ("20 min"), orange bar width 175 ("14 min")
  - "3 delegate": blue bar width 50 ("4 min"), orange bar width 250 ("20 min")
  - "4 replace": blue bar width 0 ("0 min"), orange bar width 312 ("25 min")
- **Legend (12px, top right under title):** blue swatch "minutes writing", orange swatch "minutes reviewing".
- **Annotation (bold 13px `#d95926`, right side near y=175):** "review burden grows with autonomy".
- **Caption (12px `#444`, bottom right):** "minutes illustrative — same endpoint, same developer".

## Four Dials That Size Up Any Tool

**Tags:** `where it's used` (blue), `evaluation` (green)

- **Context** — what the model can see: the open file, the whole repo, or the whole org's code and docs
- **Verification** — does the tool run tests and type-checks, or only emit plausible-looking text?
- **Interface** — editor-embedded ghost text, a chat panel, a terminal agent, or a web PR bot
- **Trust workflow** — how you review what it did; this dial is the price of turning up the other three
- **Adoption** — public developer surveys report rapid mainstream uptake, mostly at levels 1 and 2

*Example (italic):* A terminal agent that runs the test suite after every edit earns level-3 trust; one that only prints confident diffs has level-3 autonomy with level-1 verification — the worst combination.

**Key point:** Any tool on the ladder is judged on the same four dials — context, verification, interface, trust workflow — and verification is what separates "runs the tests" from "sounds right".

### Visualization (canvas `c3`, 720×300)

Matrix diagram: four dimension rows × four autonomy-level columns, each cell a short phrase describing the typical value at that level.

- **Title (bold 15px, `#1a5276`, top center):** "The Four Dials Across the Ladder".
- **Grid:** column headers bold 12px at y=58 over columns centered at x = 210, 345, 480, 615, colored to match c1 ("suggest" `#2a78d6`, "converse" `#199e70`, "delegate" `#008300`, "replace" `#d95926`); row labels bold 12px `#1a5276` left-aligned at x=20 at y = 95, 143, 191, 239: "context", "verification", "interface", "trust workflow".
- **Cells (120px wide, 34px tall, 6px radius, 11px `#2c3e50` centered text, fill `rgba(42,120,214,0.08)` with 1px `#e5e9ef` border):**
  - context row: "open file" / "whole repo" / "repo + runtime" / "org-wide"
  - verification row: "none — you compile" / "you apply and test" / "runs tests itself" / "full CI gate"
  - interface row: "editor inline" / "chat panel" / "terminal / PR bot" / "requirements doc"
  - trust row: "read each line" / "read each diff" / "review the PR" / "acceptance tests"
- **Trust row emphasis:** trust-workflow cells use fill `rgba(217,89,38,0.10)` and 1px `#d95926` border.
- **Annotation (bold 12px `#d95926`, bottom center near y=278):** "the trust row is the one that gets heavier left to right".

## Fashion Is Not a Level Picker

**Tags:** `common mistake` (red), `review discipline` (orange)

- **The fashion trap** — teams push level 3 onto every task because agents are new, not because tasks fit
- **Hallucinated APIs** — generated code can call library functions that do not exist; it compiles in prose only
- **Plausible bugs** — the riskiest output looks right, passes a skim, and is subtly wrong
- **Security patterns** — documented studies find suggested code can echo insecure idioms from training data
- **The norm** — review AI output like a teammate's pull request: run it, test it, question its choices

*Example (italic):* The agent's rate limiter passes its own tests but keys the limit on user-agent instead of client IP — a skim misses it; a teammate-style review catches it.

**Common mistake:** Picking the autonomy level by fashion. Pick it per task riskiness — high autonomy for low-stakes chores, tight human authorship where a subtle bug is expensive.

### Visualization (canvas `c4`, 720×300)

Scatter chart: five example tasks placed by riskiness (x) against the sensible autonomy level (y), showing a downward staircase — riskier tasks get lower autonomy.

- **Title (bold 15px, `#1a5276`, top center):** "Match the Level to the Task, Not the Fashion".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = task riskiness 0–10 (12px `#444` label "task riskiness (illustrative)" centered below axis, ticks at 0/5/10); y = autonomy level 1–4, gridlines `#e5e9ef` at each level, 12px `#444` tick labels "1 suggest" / "2 converse" / "3 delegate" / "4 replace".
- **Points (8px radius filled circles, 12px `#2c3e50` labels beside each):** "boilerplate CRUD test" at (risk 1, level 3) green `#008300`; "rename across 40 files" at (risk 3, level 3) green `#008300`; "caching layer" at (risk 5, level 2) blue `#2a78d6`; "login rate limiter" at (risk 7, level 2) blue `#2a78d6`; "payment retry logic" at (risk 9, level 1) red `#e74c3c`.
- **Trend:** dashed 2px `#6b7280` (dash 5/4) staircase line stepping down through the points from upper-left to lower-right.
- **Annotation (bold 13px red `#e74c3c`, near risk 8, level 3.5):** "riskier task → lower autonomy, heavier review".
- **Caption (12px `#444`, bottom right):** "placements illustrative — the slope is the point".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays and coordinates above (no randomness); the writing/review minutes (32/6, 20/14, 4/20, 0/25), riskiness scores, and scatter placements are invented and labeled illustrative; do not name specific commercial products for undocumented behavior — level descriptions stay generic.
- In regenerated HTML, any card links would use `.html` extensions (this page is a hub in topic only — it contains no links).
