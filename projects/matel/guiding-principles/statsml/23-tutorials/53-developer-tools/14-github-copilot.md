# GitHub Copilot

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** GitHub Copilot

**Subtitle:** Copilot suggests the next lines as gray ghost text while you type — Tab accepts, typing on dismisses — and that near-zero friction, not just the model, is why it stuck

## The Gray Text That Finishes Your Line

**Tags:** `core idea` (blue), `ghost text` (green), `2021 launch` (orange)

- **The launch** — in 2021 Copilot became the first mainstream AI pair programmer, built into the editor
- **The ghost** — as you type, a gray continuation of your line appears in place, ahead of your cursor
- **The choice** — press Tab to accept it, or just keep typing and it silently disappears
- **No conversation** — no chat window, no prompt to write, no mode switch out of the code
- **The source** — suggestions come from a model trained on large amounts of public code

*Example (italic):* You type `def total_price(items):` and gray text proposes the return line; Tab takes it in ~200 ms, typing anything else costs you nothing.

**Key point:** Copilot defined the SUGGEST level of coding assistance: suggestions are ambient and zero-cost to ignore, so the friction is measured in milliseconds — which is why it stuck where clunkier tools failed.

### Visualization (canvas `c1`, 720×300)

Editor-mock flow diagram: a code line with a gray ghost-text continuation, and the two possible outcomes (Tab accepts, keep typing dismisses) as labeled arrows to outcome boxes.

- **Title (bold 15px, `#1a5276`, top center):** "Suggestion, Not Conversation: Accept in One Keystroke, Ignore for Free".
- **Editor box:** rounded rect x=40, y=55, width 390, height 150, fill `#f8f9fa`, 1px `#e0e0e0` border, 6px radius; 13px monospace lines at x=60: `def total_price(items):` in `#2c3e50` at y=95; at y=125 typed part `    return sum(` in `#2c3e50` followed by ghost text `item.price for item in items)` in italic `#9aa4af`; 2px `#2c3e50` cursor bar just before the ghost text.
- **Accept path:** 3px green `#008300` arrow from the ghost text to a green rounded box (fill `rgba(0,131,0,0.12)`, 190px wide, 44px tall) at x=490, y=78, 12px `#2c3e50` text "Tab — line accepted (~200 ms)".
- **Dismiss path:** 3px mute `#6b7280` arrow to a gray rounded box (fill `rgba(107,114,128,0.12)`, same size) at x=490, y=160, 12px text "keep typing — it vanishes (0 ms)".
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=260):** "no mode switch, no prompt — the friction is milliseconds".
- **Caption (12px `#444`, bottom right):** "timings illustrative".

## An Afternoon of 100 Suggestions

**Tags:** `worked example` (blue), `sweet spot` (green)

- **The session** — one developer, one afternoon, 100 ghost-text suggestions shown (illustrative tally)
- **Boilerplate** — 18 of 25 accepted (72%): imports, config blocks, struct definitions
- **Tests and docstrings** — tests 14 of 22 (64%), docstrings 8 of 14 (57%): repetitive, pattern-shaped
- **Next-obvious-line** — 12 of 21 accepted (57%): the loop body any reader would predict
- **Novel logic** — 2 of 18 accepted (11%): new algorithms and design decisions are not its game

*Example (italic):* Of 100 suggestions, 54 were accepted — and 52 of those 54 were boilerplate, tests, docstrings, or the next obvious line, not new ideas.

**Key point:** Copilot's documented sweet spot is autocomplete-on-steroids — boilerplate, tests, repetitive patterns, translating comments to code — not inventing the part of the program you haven't designed yet.

### Visualization (canvas `c2`, 720×300)

Horizontal bar chart: acceptance rate by task type for the afternoon's 100 suggestions, one bar per category with accepted/shown counts at bar ends.

- **Title (bold 15px, `#1a5276`, top center):** "One Afternoon, 100 Suggestions: What Gets Accepted".
- **Axis:** vertical 2px `#999` baseline at x=185, bars extend right, full scale 100% = 460px; vertical gridlines `#e5e9ef` at 25/50/75% with 11px `#6b7280` labels at y=250.
- **Rows (bars 16px tall, centered at y = 70, 110, 150, 190, 230), each with a right-aligned 12px `#444` label ending at x=175:**
  - "boilerplate": green `#008300` bar width 331 (72%), 12px label at bar end "18 of 25 — 72%"
  - "tests": aqua `#199e70` bar width 294 (64%), label "14 of 22 — 64%"
  - "docstrings": blue `#2a78d6` bar width 262 (57%), label "8 of 14 — 57%"
  - "next-obvious-line": blue `#2a78d6` bar width 262 (57%), label "12 of 21 — 57%"
  - "novel logic": red `#e74c3c` bar width 51 (11%), label "2 of 18 — 11%"
- **Annotation (bold 13px magenta `#d55181`, near x=320, y=252):** "the sweet spot is autocomplete-on-steroids, not invention".
- **Caption (12px `#444`, bottom right):** "counts illustrative".

## What the Studies Actually Measured

**Tags:** `where it's used` (blue), `published research` (green)

- **Vendor RCT** — GitHub's 2022 study: 95 developers building an HTTP server; Copilot group finished 55% faster
- **The numbers** — median 71 minutes with Copilot vs 161 minutes without, on that scaffolding-heavy task
- **Independent results** — speed gains vary by task type; several studies report mixed effects on code quality
- **Churn signal** — some independent analyses found more code churn (lines rewritten soon after) post-adoption
- **Up the ladder** — the product later added chat and agentic modes, following the industry up the autonomy ladder

*Example (italic):* The 55%-faster headline came from one boilerplate-shaped task; on novel design work, published gains shrink and quality results are mixed.

**Key point:** Read the studies as ranges, not hype: strong, replicated speed gains on scaffolding tasks; mixed and still-debated results on code quality and long-term maintainability.

### Visualization (canvas `c3`, 720×300)

Two-panel chart: left, vertical bars for the vendor RCT completion times; right, a summary panel of independent findings shown as labeled range rows.

- **Title (bold 15px, `#1a5276`, top center):** "Published Studies: Fast on Scaffolding, Mixed on Quality".
- **Left panel (x=60 to 350):** baseline 2px `#999` at y=245, plot height 175, y scale 0–180 min with gridlines `#e5e9ef` at 60/120/180 and 11px `#6b7280` labels; bar 1 blue `#2a78d6`, 70px wide at x=100, height 156px for 161 min, 12px label above "no Copilot — 2h 41m"; bar 2 green `#008300`, 70px wide at x=230, height 69px for 71 min, label "Copilot — 1h 11m"; bold 13px green annotation between bars at y=100: "55% faster"; 11px `#444` caption under baseline: "vendor RCT, n=95, HTTP-server task".
- **Right panel (rounded box x=400, y=60, width 290, height 185, fill `#f8f9fa`, 1px `#e0e0e0`):** 13px bold `#1a5276` header "independent studies" at y=85; three 12px `#444` rows at y=120/155/190, each led by a bold colored dash: green "speed: gains, size varies by task", orange `#d95926` "quality: mixed results reported", red `#e74c3c` "churn: more rewrites in some data".
- **Annotation (bold 12px violet `#4a3aa7`, bottom center y=280):** "one task type ≠ all of programming — cite ranges, not headlines".

## Plausible Is Not the Same as Correct

**Tags:** `common mistake` (red), `automation bias` (orange)

- **The trap** — suggestions are plausible, not verified: fluent-looking code invites uncritical acceptance
- **Hallucinated APIs** — the model happily completes calls to functions and parameters that do not exist
- **Compiles, still wrong** — subtly wrong logic (off-by-one, flipped condition) passes the compiler and the glance
- **Narrow context** — historically it saw nearby code and open tabs, not whole-repo understanding
- **The controversy** — training on public code drew documented litigation and license debates; code-similarity and attribution filters were added in response

*Example (italic):* A suggested date-range check compiles cleanly but excludes the last day; the reviewer, lulled by fluency, Tabs it through — the publicly studied automation-bias pattern.

**Common mistake:** Trusting fluency as a proxy for correctness. Ghost text is a draft from a model that optimizes for plausible continuations — review and test every accepted line as if a stranger wrote it.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: the Tab-reflex path (plausible suggestion accepted, subtle bug ships) vs the review path (same suggestion read and tested, bug caught).

- **Title (bold 15px, `#1a5276`, top center):** "The Automation-Bias Trap: Fluent Code That Compiles Can Still Be Wrong".
- **Row 1 (y=95), label 12px `#444` at x=20:** "Tab reflex"; blue `#2a78d6` rounded box at x=150 labeled "suggestion: idiomatic, compiles" (12px), 3px arrow to a red `#e74c3c` box at x=440 labeled "off-by-one ships to prod" with bold 12px red "✗ looked right".
- **Row 2 (y=205), label:** "read first"; blue box "suggestion: idiomatic, compiles", 3px arrow to a green `#008300` box at x=380 labeled "read + run the tests", then arrow to a green box at x=580 labeled "bug caught" with bold 12px green "✓".
- **Box style:** 150–180px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(231,76,60,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px orange `#d95926`, centered near y=270):** "the interaction design made accepting cheap — reviewing still costs what it always did".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. The c1 editor mock uses a 13px monospace font (`ui-monospace, Menlo, monospace`) for the code lines only. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded values above (no randomness); the afternoon tally in c2 (18/25, 14/22, 8/14, 12/21, 2/18) and the c1 timings are invented and labeled illustrative; the c3 RCT figures (n=95, 71 min vs 161 min, 55% faster) are GitHub's published 2022 study numbers, and the independent-findings rows are summaries of published ranges, not measurements.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
