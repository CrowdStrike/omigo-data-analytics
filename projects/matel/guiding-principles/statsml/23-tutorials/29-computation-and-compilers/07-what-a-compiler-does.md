# What a Compiler Does

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** What a Compiler Does

**Subtitle:** A compiler is a translator that reads the code you wrote as plain text and turns it, in five checkable stages, into instructions the machine can actually run

## One Payroll Line, Five Stops

**Tags:** `core idea` (blue), `pipeline` (green), `translation` (orange)

- **The line** — a coffee shop's payroll program has one line: `pay = hours * rate + 50` (a $50 weekend bonus)
- **The problem** — the machine cannot read that text; it only runs tiny numbered instructions
- **The compiler** — a translator that carries the line through five stops, checking it at each one
- **The five stops** — chop into words, build a grammar tree, check it makes sense, simplify, write instructions
- **One-way trip** — text goes in once, 4 machine instructions come out, and those can run millions of times

*Example (italic):* The single text line `pay = hours * rate + 50` enters the pipeline and exits as 4 machine instructions the till's chip can execute.

**Key point:** A compiler is a five-stage translator: your text is chopped, structured, checked, simplified, and finally rewritten as machine instructions.

### Visualization (canvas `c1`, 720×300)

Horizontal pipeline diagram: five stage boxes connected by arrows, with the artifact each stage produces written beneath it, showing one text line becoming 4 instructions.

- **Title (bold 15px, `#1a5276`, top center):** "Five Stops from Text to Machine Instructions".
- **Source strip:** rounded rect at x=40, y=55, width 640, height 30, fill `rgba(42,120,214,0.15)`, 1px blue `#2a78d6` border; centered 13px monospace `#2c3e50` text: `pay = hours * rate + 50`; 11px `#6b7280` label "source text (what you typed)" above at y=48.
- **Stage boxes (five, each 116×64, y=130, at x = 40, 172, 304, 436, 568):** fills in order `rgba(42,120,214,0.15)` blue, `rgba(0,131,0,0.12)` green, `rgba(217,89,38,0.15)` orange, `rgba(74,58,167,0.12)` violet, `rgba(213,81,129,0.12)` magenta; matching 1.5px borders `#2a78d6`, `#008300`, `#d95926`, `#4a3aa7`, `#d55181`; two-line centered bold 12px labels: "1. Lexing" / "chop into words", "2. Parsing" / "build the tree", "3. Checking" / "does it make sense", "4. Optimizing" / "simplify", "5. Code gen" / "write instructions".
- **Arrows:** 2px `#6b7280` arrows with small filled heads between neighboring boxes (y=162), and one arrow from the source strip down to box 1.
- **Artifact labels (11px `#6b7280`, centered under each box at y=215):** "7 tokens", "1 tree", "checked tree", "smaller tree", "4 instructions".
- **Annotation (bold 13px `#008300`, centered at y=255):** "one line of text in — 4 machine instructions out".
- **Caption (12px `#444`, bottom right):** "stage layout illustrative — real compilers add more stops".

## Chopping and Stacking `hours * rate + 50`

**Tags:** `worked example` (blue), `step by step` (green)

- **Stage 1, chop** — the text becomes 7 tokens: `pay`, `=`, `hours`, `*`, `rate`, `+`, `50`
- **Stage 2, stack** — grammar says `*` binds before `+`, so the tree reads (hours × rate) + 50
- **Stage 5, write** — the tree flattens to 4 instructions: LOAD hours, MUL rate, ADD 50, STORE pay
- **Hand-check** — with hours = 38 and rate = 20: LOAD 38, MUL → 760, ADD 50 → 810, STORE pay
- **Same answer** — 38 × 20 + 50 = 810 by hand, and 810 by the 4 instructions; the translation is faithful

*Example (italic):* Run the 4 instructions with 38 hours at $20: the running value goes 38 → 760 → 810, and 810 lands in `pay` — exactly the by-hand answer.

**Key point:** Every stage is checkable by hand: 7 tokens, one tree shaped by "multiply before add", 4 instructions that compute 38 × 20 + 50 = 810.

### Visualization (canvas `c2`, 720×300)

Three-panel walk-through: the 7 tokens as chips (left), the parse tree (middle), and the 4 instructions with a running-value column (right) — the same line at stages 1, 2, and 5.

- **Title (bold 15px, `#1a5276`, top center):** "One Line at Stage 1, Stage 2, and Stage 5".
- **Panel headers (bold 12px `#1a5276`, at y=52):** "1. tokens (7)" at x=30, "2. tree" at x=280, "5. instructions (4)" at x=490.
- **Token chips (left panel, x=30–230):** seven rounded chips (26px tall, 11px monospace centered text) stacked in two rows at y=70 and y=104: `pay` `=` `hours` `*` in row one, `rate` `+` `50` in row two; name chips (`pay`, `hours`, `rate`) fill `rgba(42,120,214,0.15)` with blue `#2a78d6` border, operator chips (`=`, `*`, `+`) fill `rgba(217,89,38,0.15)` with orange `#d95926` border, the number chip (`50`) fill `rgba(0,131,0,0.12)` with green `#008300` border; 11px `#6b7280` legend at y=150: "blue = names, orange = operators, green = number".
- **Parse tree (middle panel, centered x=330):** nodes as 30px circles with 12px bold labels — `=` at (330, 85), children `pay` at (280, 140) and `+` at (380, 140); `+` has children `*` at (330, 195) and `50` at (430, 195); `*` has children `hours` at (280, 250) and `rate` at (380, 250); 1.5px `#6b7280` connecting lines; operator nodes stroked orange `#d95926`, names blue `#2a78d6`, number green `#008300`; 11px `#6b7280` note under the tree at y=285: "* sits deeper, so it runs first".
- **Instruction list (right panel, x=490–690):** four rows at y = 80, 125, 170, 215, each a rounded rect (fill `rgba(74,58,167,0.12)`, 1px violet `#4a3aa7` border) with 12px monospace text "LOAD hours", "MUL rate", "ADD 50", "STORE pay"; to the right of each row a bold 12px green `#008300` running value: "38", "760", "810", "pay=810".
- **Annotation (bold 12px green `#008300`, right panel bottom at y=255):** "38 × 20 + 50 = 810 — matches the hand answer".
- **Caption (12px `#444`, bottom right):** "hours=38, rate=20 — illustrative payroll numbers".

## Why the Stops Matter: Catch Early, Run Lean

**Tags:** `where it's used` (blue), `error catching` (green), `optimization` (orange)

- **Catch before payday** — type `ratee` by mistake and stage 3 rejects it before any paycheck is wrong
- **A running program can't** — without the checking stop, the typo surfaces mid-run, on somebody's pay
- **The optimizer earns its keep** — written naively as `hours * rate + 25 * 2`, it needs about 6 instructions
- **Fold the constants** — stage 4 computes 25 × 2 = 50 once, at translation time, leaving 4 instructions
- **Small savings scale** — 6 vs 4 instructions is 2 saved per employee; over 1,000,000 payroll rows, 2,000,000 steps

*Example (italic):* The chain's nightly payroll run touches 1,000,000 rows; the folded version does 4,000,000 instructions instead of 6,000,000 — same answer, one third less work.

**Key point:** The middle stages are the payoff: checking catches mistakes before the program ever runs, and optimizing makes the translated code do strictly less work.

### Visualization (canvas `c3`, 720×300)

Grouped bar chart: instructions per payroll row before and after the optimizer folds 25 × 2 into 50, with the identical answer (810) noted on both bars.

- **Title (bold 15px, `#1a5276`, top center):** "Stage 4 at Work: Fold 25 × 2 Once, Save Forever".
- **Axes:** origin x=90, baseline y=245, plot width 480, plot height 180; y axis = instructions per row, 0 to 6, 12px `#444` tick labels at 0, 2, 4, 6 with light `#e5e9ef` gridlines.
- **Bars (each 120px wide):** naive translation at x=160, height for value 6, fill `rgba(217,89,38,0.55)`, 1.5px orange `#d95926` border, bold 13px orange value label "6" above; optimized at x=380, height for value 4, fill `rgba(0,131,0,0.45)`, 1.5px green `#008300` border, bold 13px green value label "4" above.
- **Bar labels (12px `#444`, centered under each bar at y=265):** "naive: ... + 25 * 2" and "optimized: ... + 50".
- **Equality note (12px `#6b7280`, centered between the bars at y=95):** "both compute pay = 810".
- **Side panel (x=590–710):** 12px `#444` two-line note at y=120: "per row: 2 saved" / "1,000,000 rows:"; bold 13px green `#008300` line at y=160: "2,000,000 fewer steps".
- **Annotation (bold 13px green `#008300`, near x=300, y=70):** "same answer, one third fewer instructions".
- **Caption (12px `#444`, bottom right):** "instruction counts illustrative — real chips vary".

## "Isn't That Just an Interpreter?"

**Tags:** `common mistake` (red), `compiler vs interpreter` (orange)

- **Two translators** — a compiler translates the whole program once up front; an interpreter re-reads the text every run
- **The tourist test** — a compiler is a translated phrasebook you print once; an interpreter is a human repeating each phrase
- **Toy costs** — say translating costs 5 units once, then 1 unit per row; interpreting costs 3 units per row, every row
- **Break-even fast** — at 1,000 payroll rows the compiled path costs 5 + 1,000 = 1,005 units; interpreted costs 3,000
- **The mistake** — thinking "compiled" means a different language; it's the same line, translated once instead of re-read forever

*Example (italic):* For a one-off 2-row test the interpreter wins (6 vs 7 units), but the nightly 1,000-row run pays the translation cost back 3 times over.

**Common mistake:** Treating compiling and interpreting as different kinds of programs. They run the same source line — the difference is paying one up-front translation cost versus re-reading the text on every single run.

### Visualization (canvas `c4`, 720×300)

Two cumulative-cost lines over payroll rows processed: the interpreter's line starts at zero but climbs steeply; the compiler's line starts at its one-time translation cost and climbs gently, crossing below early.

- **Title (bold 15px, `#1a5276`, top center):** "Pay Once to Translate, or Pay on Every Row".
- **Axes:** origin x=70, baseline y=245, plot width 560, plot height 180; x axis = payroll rows processed 0 to 1,000, 12px `#444` tick labels "0", "200", "400", "600", "800", "1,000"; y axis = total work units 0 to 3,000, 12px `#444` tick labels "0", "1,000", "2,000", "3,000" with light `#e5e9ef` gridlines.
- **Shared x grid for both lines:** rows = `[0, 100, 200, 300, 400, 500, 600, 700, 800, 900, 1000]`.
- **Interpreter line:** orange `#d95926` 3px line, cumulative cost = `[0, 300, 600, 900, 1200, 1500, 1800, 2100, 2400, 2700, 3000]` (3 units per row); bold 12px orange label "interpreter: 3,000" near its right end.
- **Compiler line:** blue `#2a78d6` 3px line, cumulative cost = `[5, 105, 205, 305, 405, 505, 605, 705, 805, 905, 1005]` (5 up front, then 1 per row); bold 12px blue label "compiler: 1,005" near its right end, staggered below the orange label.
- **Crossover marker:** 6px `#4a3aa7` violet dot at rows=2.5, cost=12.5 (visually at the far left near the origin); 11px `#6b7280` label with a short pointer line: "even by row 3".
- **Annotation (bold 13px blue `#2a78d6`, near x=330, y=90):** "translate once — every later row costs a third as much".
- **Caption (12px `#444`, bottom right):** "toy unit costs, illustrative — real ratios vary widely".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all token lists, tree node positions, instruction rows, bar heights, and line points are the hardcoded literal values above (no randomness); the payroll numbers (38 hours, $20 rate, $50 bonus, pay 810), instruction counts (7 tokens, 4 vs 6 instructions), and unit costs (5 up front, 1 vs 3 per row, break-even by row 3, totals 1,005 vs 3,000) must match between text and charts exactly.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
