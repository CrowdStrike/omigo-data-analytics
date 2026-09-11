# AI Agent Harnesses

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** AI Agent Harnesses

**Subtitle:** A harness is the rig around the workhorse — in LLM land the same word names the agent scaffold inside coding assistants, the benchmark runner in papers, and two older rigs from testing and training

## One Bug, Two Ways to Ask the Model

**Tags:** `core idea` (blue), `agent harness` (green)

- **The task** — Alice wants "fix the failing test in my repo" done for her, not explained to her
- **Bare API** — the model alone only turns text into text; it cannot open a file or run anything
- **The harness** — a program wrapped around the model that reads files, runs commands, and loops
- **Division of labor** — the model decides the next step; the harness executes it and reports back
- **Named examples** — Claude Code, Codex, Cursor, Kimi Code, and Aider are all agent harnesses

*Example (italic):* Asked over the bare API, the model guesses a patch from memory; inside the harness it first runs the failing test and reads the real error.

**Key point:** The agent harness is the running program around the model — the model thinks; the harness supplies the eyes, hands, and the loop.

### Visualization (canvas `c1`, 720×300)

Side-by-side panels: the bare model API on the left (text in, text out, no hands), and the identical model on the right surrounded by the harness parts with a decide-act-look loop.

- **Title (bold 15px, `#1a5276`, top center):** "The Same Model, Bare vs Inside a Harness".
- **Left panel:** rounded rect x=30, y=52, 310×200, fill `rgba(107,114,128,0.06)`, 2px `#6b7280` border; header bold 13px `#6b7280` centered at (185, 74): "bare model API".
  - Model box: rounded rect x=115, y=120, 140×44, fill `rgba(0,131,0,0.12)`, 2px `#008300` border; bold 12px `#008300` centered label "the model" at y=146.
  - In-arrow: 2px `#6b7280` from (48,142) to (106,142) with arrowhead; 12px `#6b7280` label "prompt in" centered at (78,126). Out-arrow: from (262,142) to (320,142) with arrowhead; label "text out" centered at (292,126).
  - Note (12px `#6b7280`, centered x=185, two lines y=206/224): "no hands: it cannot open a file," / "run a test, or retry anything".
- **Right panel:** rounded rect x=380, y=52, 310×200, fill `rgba(42,120,214,0.06)`, 2px `#2a78d6` border; header bold 13px `#2a78d6` centered at (535, 74): "the same model inside a harness".
  - Model box: rounded rect x=465, y=128, 140×44, same green style, label "the model" at y=154.
  - Four part boxes (130×26, fill `rgba(42,120,214,0.15)`, 1px `#2a78d6` border, 12px `#1a5276` centered labels): "system prompt" at (396,88), "context memory" at (546,88), "tool runner" at (396,212), "guardrails" at (546,212).
  - Connector lines: 1.5px `#6b7280` short lines from each part box's inner corner toward the model box.
  - Loop label (bold 12px `#2a78d6`, centered x=535, y=192): "decide → act → look → repeat".
- **Annotation (bold 12px orange `#d95926`, centered at y=272):** "same model on both sides — the harness supplies the eyes, hands, and the loop".
- **Caption (11px `#444`, bottom right, y=292):** "harness parts simplified".

## Watching the Harness Loop Run

**Tags:** `worked example` (blue), `the loop` (orange)

- **Turn 1** — the harness sends the failing test output; the model answers "open billing.py"
- **Turns 2–3** — the harness returns the file; the model dictates an edit; the harness applies it
- **Turn 4** — the model says "run the tests"; the harness runs them: 1 pass, 1 still failing
- **Turns 5–6** — the model edits tax.py; the harness reruns the tests: all pass, the loop ends
- **Count it** — 6 model calls and 5 tool runs; the model never touched the disk once

*Example (italic):* Every "open", "edit", and "run" in the trace was performed by the harness — the model only ever produced the instruction text.

**Key point:** One bug fix = 6 model calls and 5 tool runs — recount them on the diagram and see exactly who does what.

### Visualization (canvas `c2`, 720×300)

Two-lane zigzag trace: six blue "model call" boxes on the top lane, five green "tool run" boxes on the bottom lane, with diagonal arrows alternating between lanes in time order.

- **Title (bold 15px, `#1a5276`, top center):** "One Bug Fix: 6 Model Calls, 5 Tool Runs".
- **Legend (12px, top left, swatch + label):** blue swatch "model call" at (30,56); green swatch "tool run by the harness" at (30,74).
- **Top lane:** six rounded rects (6px radius) 100×44, tops y=92, left edges x = `[30, 145, 260, 375, 490, 605]`; fill `rgba(42,120,214,0.12)`, 2px `#2a78d6` border; bold 12px `#1a5276` centered two-line labels: "open / billing.py", "edit / line 42", "run the / tests", "edit / tax.py", "run the / tests", "done — / bug fixed".
- **Bottom lane:** five rounded rects 100×40, tops y=180, left edges x = `[88, 203, 318, 433, 548]`; fill `rgba(0,131,0,0.10)`, 2px `#008300` border; 12px `#008300` centered two-line labels: "file / contents", "edit / applied", "1 pass, / 1 fail", "edit / applied", "all / pass".
- **Zigzag arrows:** 1.5px `#6b7280` with small arrowheads; for each i in 0..4 a down-right arrow from the bottom edge of top box i (x=lefts[i]+80, y=136) to the top edge of bottom box i (x=bLefts[i]+30, y=180), and an up-right arrow from the top edge of bottom box i (x=bLefts[i]+70, y=180) to the bottom edge of top box i+1 (x=lefts[i+1]+20, y=136).
- **Annotation (bold 12px orange `#d95926`, centered at y=258):** "count the boxes: the model spoke 6 times; every read, edit, and run was the harness".
- **Caption (11px `#444`, bottom right, y=290):** "trace illustrative".

## One Word, Four Rigs

**Tags:** `where it's used` (blue), `overloaded term` (orange)

- **The origin** — test harness: the rig around code under test, feeding inputs and checking outputs
- **In papers** — eval harness: a benchmark runner that asks a model thousands of questions and scores it
- **In products** — agent harness: the scaffold above — prompt assembly, tool loop, guardrails, retries
- **In training** — training harness: the loop code that feeds data, saves checkpoints, spans many GPUs
- **The thread** — every sense means the rig around the thing being exercised, never the thing itself

*Example (italic):* "We ran the model in our harness" means a benchmark runner in a paper but a coding agent in a product demo — same word, different rig.

**Key point:** Four senses, one metaphor — the harness holds and drives the workhorse; ask which rig is meant before you nod along.

### Visualization (canvas `c3`, 720×300)

2×2 grid of four labeled boxes, one per sense of the word, each stating what it wraps, its job, and where a reader meets it.

- **Title (bold 15px, `#1a5276`, top center):** "One Word, Four Rigs — What Wraps What".
- **Boxes:** four rounded rects (6px radius) 310×92 at (30,52), (380,52), (30,160), (380,160); each with a bold 13px colored header centered at its top (+22) and three 12px `#2c3e50` lines centered at +44, +62, +80.
  - **Box 1 (violet `#4a3aa7`, fill `rgba(74,58,167,0.08)`):** header "test harness — the original"; lines "wraps: code under test", "job: feed inputs, check outputs", "meet it in: CI pipelines".
  - **Box 2 (magenta `#d55181`, fill `rgba(213,81,129,0.08)`):** header "eval harness — in papers"; lines "wraps: a model API", "job: run benchmark suites, score answers", "meet it in: leaderboards & papers".
  - **Box 3 (blue `#2a78d6`, fill `rgba(42,120,214,0.08)`):** header "agent harness — in products"; lines "wraps: a model doing real work", "job: prompts, tool loop, guardrails", "meet it in: coding assistants".
  - **Box 4 (aqua `#199e70`, fill `rgba(25,158,112,0.08)`):** header "training harness — in ML infra"; lines "wraps: the training loop", "job: feed data, checkpoint, scale out", "meet it in: model training teams".
- **Annotation (bold 12px orange `#d95926`, centered at y=276):** "every sense is the rig around the workhorse — never the workhorse itself".
- **Caption (11px `#444`, bottom right, y=294):** "one-line summaries, simplified".

## Same Model, Different Score

**Tags:** `common mistake` (red), `model vs harness` (orange)

- **The claim** — "model X fixes 80% of bugs" quietly means "inside one particular harness"
- **The experiment** — same model, ten bugs: chat-only solves 2, a basic tool loop 5, a full harness 8
- **Check it** — nothing about the model changed between the three bars; only the wrapper did
- **Not a framework** — a framework is a library you build with; a harness is the program that runs
- **The mistake** — comparing two models tested in two different harnesses and crediting the model

*Example (italic):* A team switched harnesses, kept the model, and their bug-fix rate jumped — the announcement still said "the new model is smarter."

**Common mistake:** Reading an agent benchmark as a pure model score — the harness can move the number more than a model upgrade can.

### Visualization (canvas `c4`, 720×300)

Bar chart: the same model attempting the same ten bugs inside three different wrappers, with the solve count rising as the harness gets more capable.

- **Title (bold 15px, `#1a5276`, top center):** "Same Model, Ten Bugs, Three Wrappers".
- **Axes:** origin x=70, baseline y=245, plot width 590, plot top y=60; y = bugs solved 0 to 10 with light `#e5e9ef` gridlines at 2, 4, 6, 8, 10 and 12px `#444` tick labels.
- **Bars (90px wide, centered x = `[200, 390, 580]`):** "chat only" value 2 in gray `#6b7280`; "basic tool loop" value 5 in blue `#2a78d6`; "full harness" value 8 in green `#008300`; bold 13px value label "2 / 10", "5 / 10", "8 / 10" above each bar in the bar's color.
- **X labels:** 12px `#444` main label at y=263 ("chat only", "basic tool loop", "full harness") and an 11px `#6b7280` descriptor at y=279 ("no tools", "tools, no retries", "tools + context + retries").
- **Annotation (bold 13px orange `#d95926`, centered at (390, 85)):** "the model never changed — only the wrapper did".
- **Caption (12px `#444`, bottom right):** "solve counts illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all box positions, trace labels, and bar values are the hardcoded arrays above (no randomness); the trace must show exactly 6 top boxes and 5 bottom boxes to match "6 model calls, 5 tool runs", and the bars must read 2, 5, 8 out of 10 to match the text.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
