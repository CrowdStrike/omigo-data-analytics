# Turing Machines

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Turing Machines

**Subtitle:** A strip of paper, a pencil, and a short list of rules make the simplest possible computer — and anything any computer can compute, this one can too

## A Clerk, a Paper Tape, and a Tiny Rulebook

**Tags:** `core idea` (blue), `the machine` (green), `three parts` (orange)

- **The clerk** — imagine a patient clerk working along a paper tape divided into square cells
- **The tape** — each cell holds one symbol (here a 1, a 0, or a blank) and the tape never runs out
- **The head** — the clerk looks at exactly one cell at a time; that spot is called the head
- **The state** — the clerk keeps one word in mind, like "CARRY", written on a sticky note
- **The rulebook** — each rule says: in this state, seeing this symbol, write X, move, change state
- **That's all** — tape + head + state + rulebook is the entire machine; nothing else is hidden

*Example (italic):* The clerk stands at a cell showing 1, the sticky note says CARRY, and rule 1 says "write 0, step left, stay in CARRY" — one tick of the world's simplest computer.

**Key point:** A Turing machine is just a tape, one reading head, one remembered state, and a fixed rulebook — every computer ever built is dressing on top of this.

### Visualization (canvas `c1`, 720×300)

Single-panel diagram of the clerk's workspace: the tape as a row of cells, the read head pointing at one cell, the state sticky note, and the three-rule rulebook printed underneath.

- **Title (bold 15px, `#1a5276`, top center):** "The Whole Computer: Tape + Head + State + Rules".
- **Tape:** 8 cells, each 58×58px with 2px `#1a5276` borders and white fill, in a row starting at x=70, top edge y=110; cell symbols centered bold 20px `#2c3e50`: `["·", "1", "0", "1", "1", "·", "·", "·"]` (the `·` blanks drawn in mute `#6b7280`).
- **Head marker:** solid orange `#d95926` downward triangle (18px wide, 14px tall) centered above cell 5 (the rightmost "1", cell center x≈331), tip touching the cell's top edge; bold 13px orange label "read head" centered above the triangle at y=80.
- **State note:** rounded rect (110×34px, 6px radius) at x=70, y=38, fill `rgba(42,120,214,0.15)`, 2px blue `#2a78d6` border; bold 13px blue text "state: CARRY" centered inside.
- **Rulebook strip:** three lines of 12px `#2c3e50` text left-aligned at x=70, y=205/225/245, each starting with a bold ink `#1a5276` rule number: "1. CARRY, see 1 → write 0, move left, stay CARRY" / "2. CARRY, see 0 → write 1, halt" / "3. CARRY, see blank → write 1, halt".
- **Annotation (bold 13px violet `#4a3aa7`, right side near x=470, y=55):** two lines: "nothing hidden —" / "these four parts are the entire machine".
- **Caption (12px `#444`, bottom right):** "tape shown as 8 cells; the real tape is endless".

## Adding 1 to 1011, One Cell at a Time

**Tags:** `worked example` (blue), `step by step` (green)

- **The job** — the tape holds 1011, the binary number eleven; the clerk must add 1 to make twelve
- **Start** — head on the rightmost cell, sticky note says CARRY, and the rulebook takes over
- **Step 1** — sees 1: rule 1 says write 0, move left; the tape now reads 1010
- **Step 2** — sees 1 again: write 0, move left; the tape now reads 1000
- **Step 3** — sees 0: rule 2 says write 1 and halt; the tape reads 1100, which is twelve
- **Check it** — eleven plus one is twelve, and 1100 in binary is twelve: three steps, done

*Example (italic):* Trace it on paper: 1011 → 1010 → 1000 → 1100 — each arrow is one rule from the three-line rulebook, and you can verify every write by hand.

**Key point:** The machine turned 1011 (eleven) into 1100 (twelve) in 3 steps using 3 rules — no arithmetic circuits, just look, write, move.

### Visualization (canvas `c2`, 720×300)

Four-row trace of the tape after each step: each row shows the 4-cell tape with the head's cell outlined, plus the rule that fired, ending in the green halted result.

- **Title (bold 15px, `#1a5276`, top center):** "Trace: 1011 + 1 = 1100 in Three Steps".
- **Rows (top edges at y = 60, 115, 170, 225), each:** a 12px `#444` left label at x=30, then 4 cells of 44×44px (2px `#1a5276` borders, white fill) starting at x=185, symbols centered bold 18px `#2c3e50`, then a 12px `#444` note at x=420.
  - Row 1 — label "start", cells `["1", "0", "1", "1"]`, head outline on cell 4, note "state CARRY, head at right".
  - Row 2 — label "step 1", cells `["1", "0", "1", "0"]`, head outline on cell 3, note "saw 1 → wrote 0, moved left".
  - Row 3 — label "step 2", cells `["1", "0", "0", "0"]`, head outline on cell 2, note "saw 1 → wrote 0, moved left".
  - Row 4 — label "step 3", cells `["1", "1", "0", "0"]`, no head outline, note "saw 0 → wrote 1, halted".
- **Head outline:** the head's cell gets a 3px orange `#d95926` border instead of ink, with a small solid orange triangle (12px wide) above it.
- **Final row styling:** row 4 cells filled `rgba(0,131,0,0.12)` with 2px green `#008300` borders; bold 13px green label "= twelve" right of the cells at x=372.
- **Annotation (bold 13px green `#008300`, bottom center at y=285):** "3 steps, 3 rules — redo every write by hand".

## Why One Tape Is Enough

**Tags:** `where it's used` (blue), `universality` (green), `big idea` (orange)

- **The claim** — anything a laptop, phone, or supercomputer can compute, the tape clerk can too
- **Church–Turing** — every serious definition of "computable" ever proposed lands on the same set
- **Universal machine** — one special rulebook can read any other rulebook off the tape and run it
- **Stored programs** — that trick, program-as-data on the tape, is the blueprint of every computer
- **The yardstick** — "can a Turing machine do it?" is how computer science defines possible at all
- **Hard limits** — some questions (like "will this program ever halt?") no machine can ever answer

*Example (italic):* A phone, a laptop, and a datacenter can all be simulated, step for step, by one clerk with the right rulebook — none of them can compute a single problem the tape cannot.

**Key point:** More hardware buys speed, never new reach — the set of solvable problems is fixed by the tape machine, which is why it is the ruler everything is measured against.

### Visualization (canvas `c3`, 720×300)

Convergence diagram: four modern devices on the left, each with an arrow into one tape-machine box on the right, showing that all of them reduce to the same simple machine.

- **Title (bold 15px, `#1a5276`, top center):** "Every Computer Reduces to the Same Tape".
- **Device boxes (left column, 175×38px rounded rects at x=60, top edges y = 62, 112, 162, 212):** 2px borders, bold 13px centered labels — "pocket phone" (blue `#2a78d6`), "laptop" (green `#008300`), "supercomputer" (orange `#d95926`), "cloud datacenter" (violet `#4a3aa7`); each border and label in its own color, fill white.
- **Arrows:** 2px `#6b7280` lines with small arrowheads from each box's right edge (x=235) converging to the tape box's left edge (x=430), meeting at y=156.
- **Tape-machine box:** 230×90px rounded rect at x=430, top edge y=111, fill `rgba(26,82,118,0.08)`, 3px ink `#1a5276` border; inside, a mini 5-cell tape (5 cells of 34×34px, 1px ink borders, symbols bold 14px: `["1", "0", "1", "1", "·"]`) with a small orange triangle head above cell 4, and bold 13px ink label "one clerk, one tape" centered below the mini tape.
- **Annotation (bold 13px magenta `#d55181`, centered at y=270):** "no new problems — only new speed".
- **Caption (12px `#444`, bottom right):** "illustrative — any device on the left can be simulated on the right".

## Slower Is Not Weaker

**Tags:** `common mistake` (red), `speed vs power` (orange)

- **The mistake** — hearing "a tape machine can do anything" and picturing it doing it quickly
- **Same list** — sorting 1,000 names gives the identical sorted list on every machine below
- **Laptop** — finishes the sort in about 0.001 seconds; the phone takes about 0.003 seconds
- **Tape clerk** — shuffling cells one at a time needs roughly 3 hours for the same 1,000 names
- **Two questions** — "can it be computed?" and "can it be computed fast?" are different fields
- **Why it matters** — complexity theory (P, NP, big-O) starts exactly where this page stops

*Example (italic):* All three machines hand back the same 1,000 sorted names — 0.001 seconds, 0.003 seconds, and 3 hours are differences of speed, not of ability.

**Common mistake:** Treating slow as unable. The Turing machine answers "what is computable at all?"; how long it takes is a separate question with its own theory.

### Visualization (canvas `c4`, 720×300)

Three-row bar chart of time to sort the same 1,000 names on a laptop, a phone, and the tape clerk, with a matching "same result" check on every row.

- **Title (bold 15px, `#1a5276`, top center):** "Same 1,000 Names Sorted — Only the Clock Differs".
- **Rows (bar center lines at y = 90, 150, 210), each:** a 12px `#444` machine label at x=30, a rounded horizontal bar (16px tall) starting at x=170, a bold 13px time label just right of the bar, and a green `#008300` bold 13px "✓ same list" at x=645.
  - Row 1 — "laptop", blue `#2a78d6` bar 60px long, label "0.001 s".
  - Row 2 — "phone", aqua `#199e70` bar 90px long, label "0.003 s".
  - Row 3 — "tape clerk", orange `#d95926` bar 400px long with a white 8px zigzag break drawn at x≈420, label "≈ 3 hrs".
- **Break note:** 11px `#6b7280` label under the zigzag at y=232: "bar not to scale".
- **Annotation (bold 13px ink `#1a5276`, centered at y=262):** "same answer every time — speed is the only difference".
- **Caption (12px `#444`, bottom right):** "illustrative timings".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all tape symbols, trace rows, box labels, and bar lengths are the hardcoded literal values above (no randomness); the c2 trace is the exact 3-step binary-increment run 1011 → 1010 → 1000 → 1100 and must match the section text; c4 timings are invented and keep their "illustrative" caption.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
