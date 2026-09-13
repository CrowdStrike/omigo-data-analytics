# Interpreters vs Compilers

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Interpreters vs Compilers

**Subtitle:** An interpreter translates your program line by line while it runs; a compiler translates the whole thing once up front — and a JIT translates mid-run, but only the parts that repeat

## A French Cookbook and Two Ways to Cook From It

**Tags:** `core idea` (blue), `running example` (green), `translation` (orange)

- **The cookbook** — you want to cook a sauce from a French cookbook, but you only read English
- **The interpreter** — a bilingual friend stands beside you and translates each line right as you cook
- **The compiler** — a translator rewrites the whole book into English once; later you cook alone
- **The trade-off** — the friend starts instantly but repeats work; the full translation costs time up front
- **Programs too** — source code is the French; the CPU only "reads English" (machine instructions)

*Example (italic):* Python's default runner is the patient friend at your elbow; a C compiler is the translator who hands you a finished English copy before you ever turn on the stove.

**Key point:** An interpreter translates and executes as it goes; a compiler translates everything first and executes later — both end at the same dish, they just pick a different moment to translate.

### Visualization (canvas `c1`, 720×300)

Two-lane flow diagram: the interpreter lane loops through read → translate → do for every line, while the compiler lane translates the whole book once and then cooks with no translator in sight.

- **Title (bold 15px, `#1a5276`, top center):** "Two Ways to Cook From a French Cookbook".
- **Interpreter lane label (bold 12px `#2a78d6`, x=40, y=68):** "interpreter — translate as you cook".
- **Interpreter lane boxes (y=82, each 170×44, 6px rounded corners, 2px `#2a78d6` border, fill `rgba(42,120,214,0.12)`, centered 12px `#2c3e50` text):** "read French line" at x=60, "translate in head" at x=270, "whisk / do the step" at x=480; 2px `#2a78d6` arrows with arrowheads between consecutive boxes.
- **Loop-back arrow:** 2px dashed (dash 5/4) `#6b7280` arrow from the top of the third box up to y=52 and back left into the top of the first box; 11px `#6b7280` label centered above it: "next line — the translating repeats every pass".
- **Compiler lane label (bold 12px `#008300`, x=40, y=188):** "compiler — translate once, cook later".
- **Compiler lane boxes (y=202, 44px tall, 6px rounded corners, 2px `#008300` border, fill `rgba(0,131,0,0.10)`, centered 12px `#2c3e50` text):** "translate the whole book → English copy" at x=60 width 300, then a 2px `#008300` arrow to "cook from English — no translator around" at x=420 width 260.
- **Annotation (bold 12px orange `#d95926`, x=400, y=175):** "same recipe — different moment of translation".
- **Caption (12px `#444`, bottom right):** "illustrative — the timings appear in the next chart".

## Whisking the Sauce 100 Times

**Tags:** `worked example` (blue), `tree-walking` (green), `bytecode` (orange), `JIT` (red)

- **The step** — "whisk the sauce" repeats 100 times; translating the line takes 3s, whisking takes 1s
- **Tree-walking** — the friend re-translates the raw French line on every pass: 100 × (3+1) = 400s
- **Bytecode** — translate once to a shorthand card (3s); each pass is a 1s read + 1s whisk: 3 + 100×2 = 203s
- **JIT** — the friend spots the repeat, drills it into muscle memory (10s), then 100 × 1 = 110s
- **Same dish** — all three produce the identical sauce; only the moment of translation moved

*Example (italic):* 400s vs 203s vs 110s for the exact same 100 whisks — the recipe never changed, only when the translating happened.

**Key point:** Tree-walking pays the 3s translation on every single pass; bytecode pays it once and keeps a cheap shorthand; JIT pays a bigger 10s once and then runs at full speed.

### Visualization (canvas `c2`, 720×300)

Horizontal stacked bar chart: three strategies on one time axis, each bar split into a one-time translation segment and a per-pass work segment, totals printed at the bar ends.

- **Title (bold 15px, `#1a5276`, top center):** "100 Whisks of the Same Step: 400s vs 203s vs 110s".
- **Axis:** horizontal 2px `#999` line at y=252 from x=210 to x=670 (width 460 = 0 to 400 seconds); 12px `#444` tick labels "0s", "100s", "200s", "300s", "400s" every 100s below the line; light `#e5e9ef` vertical gridlines at each tick up to y=70.
- **Rows (bars 26px tall, left edge x=210, at y = 95, 155, 215), each with a right-aligned 12px `#444` label ending at x=200:** "tree-walking — retranslate each pass", "bytecode — shorthand card", "JIT — muscle memory".
- **Segments (scale 1.15 px per second):** one-time translation in solid orange `#d95926`, per-pass work in `rgba(42,120,214,0.35)` with a 2px `#2a78d6` border. Values: tree-walking = 0s one-time + 400s per-pass; bytecode = 3s one-time (a thin orange sliver) + 200s per-pass; JIT = 10s one-time + 100s per-pass.
- **Totals:** bold 13px `#1a5276` labels just right of each bar end: "400s", "203s", "110s".
- **Legend (12px, x=210, y=70):** orange swatch "one-time translation", blue swatch "per-pass work".
- **Annotation (bold 13px green `#008300`, near x=420, y=228):** "3.6× faster — same 100 whisks".
- **Caption (12px `#444`, bottom right):** "illustrative timings: translate 3s, whisk 1s, JIT drill 10s".

## Where Python, Java, and numpy Land

**Tags:** `where it's used` (blue), `warm-up` (orange), `spectrum` (green)

- **Python** — CPython compiles source to bytecode, then interprets it; that is why raw for-loops feel slow
- **numpy trick** — `arr.sum()` hands the loop to ahead-of-time compiled C, skipping the interpreter entirely
- **Java & JS** — start from bytecode, then a JIT compiles the hot loops to machine code mid-run
- **Warm-up** — JIT programs start slow and speed up; benchmarks that skip warm-up passes mislead
- **A dial, not a wall** — real runtimes mix strategies; "interpreted vs compiled" is a spectrum

*Example (italic):* Summing a million numbers, a plain Python loop re-interprets the add instruction a million times, while numpy translates once and runs compiled code — same math, wildly different clock.

**Key point:** When code feels slow, ask when it gets translated — moving work out of the per-pass translation loop is the whole speed game.

### Visualization (canvas `c3`, 720×300)

Horizontal spectrum: one axis from "translates at the last moment" to "translates fully up front", with four labeled dots placing tree-walking, bytecode, bytecode+JIT, and ahead-of-time runners.

- **Title (bold 15px, `#1a5276`, top center):** "The Translation Spectrum: When Does Your Code Become Machine Instructions?".
- **Axis:** horizontal 2px `#999` line at y=170 from x=80 to x=660, small arrowheads at both ends; 12px `#6b7280` end labels below: "translates at the last moment" (left, x=80) and "translates fully up front" (right-aligned, x=660).
- **Dots (8px radius on the axis line), with a bold 12px name and an 11px `#6b7280` example line, staggered above/below to avoid overlap:**
  - x=110, magenta `#d55181`: name "tree-walking interpreter" above (y=135), examples "config parsers, shells" (y=150)
  - x=270, blue `#2a78d6`: name "bytecode interpreter" below (y=200), examples "CPython, Ruby" (y=215)
  - x=440, violet `#4a3aa7`: name "bytecode + JIT" above (y=135), examples "Java, JavaScript, PyPy" (y=150)
  - x=610, green `#008300`: name "ahead-of-time compiler" below (y=200), examples "C, Rust, Go" (y=215)
- **Annotation (bold 12px orange `#d95926`, centered near x=340, y=75):** "numpy = a door from the Python dot straight to the compiled end"; thin 1px dashed `#d95926` curve from near the CPython dot to near the ahead-of-time dot, arrowhead at the right end.
- **Caption (12px `#444`, bottom right):** "positions are qualitative — illustrative".

## The "Compiled Language" Confusion

**Tags:** `common mistake` (red), `break-even` (orange)

- **The myth** — "Python is an interpreted language" describes CPython the runner, not the language
- **Counterexample** — PyPy runs Python with a JIT, and C interpreters exist; the runner picks the strategy
- **JIT** — a compiler that runs during the program, spending upfront time only on lines that repeat
- **Break-even** — the JIT's 10s drill costs more than pass one, but it wins from the 4th whisk onward
- **Startup vs steady** — interpreters win short scripts; compilers win long-running loops

*Example (italic):* A one-line script can finish before a JIT even warms up — the patient friend wins every race that ends at the front door.

**Common mistake:** Treating "compiled" as a property of the language. It is a property of the runner, chosen per implementation — and modern runners switch strategy in the middle of the program.

### Visualization (canvas `c4`, 720×300)

Cumulative-time line chart: total seconds spent vs number of whisk passes for the three strategies from the worked example, showing the JIT's head-start cost and early break-even against tree-walking.

- **Title (bold 15px, `#1a5276`, top center):** "Total Time vs Number of Passes: the Upfront Cost Pays Off Fast".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 190; x = passes 0 to 100 with 12px `#444` tick labels "0", "20", "40", "60", "80", "100" every 20 and an axis caption "whisk passes"; y = 0 to 400 seconds, light `#e5e9ef` gridlines at 100/200/300/400 with 12px `#444` labels "100s"–"400s".
- **Shared x grid for all three lines:** passes `[0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100]`.
- **Tree-walking (4s per pass):** blue `#2a78d6` 3px line, cumulative seconds `[0, 40, 80, 120, 160, 200, 240, 280, 320, 360, 400]`; 12px blue label "tree-walking" near its right end (x≈88 passes), value 400s.
- **Bytecode (3s once, then 2s per pass):** orange `#d95926` 2px dashed (dash 6/4) line, values `[3, 23, 43, 63, 83, 103, 123, 143, 163, 183, 203]`; 12px orange label "bytecode" near x≈90 passes.
- **JIT (10s once, then 1s per pass):** green `#008300` 3px line, values `[10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110]`; 12px green label "JIT" near x≈92 passes; note it starts at 10s at pass 0 — the drill happens before the first whisk.
- **Break-even marker:** vertical dashed `#6b7280` (dash 4/3) line at pass 4 (where 10 + n first beats 4n) from baseline up to y=80; 7px green dot on the JIT line at that pass.
- **Annotation (bold 12px green `#008300`, near x=12 passes, y=95):** two lines: "JIT pays for itself" / "before the 4th whisk".
- **Caption (12px `#444`, bottom right):** "illustrative — same 3s/1s/10s timings as the bar chart".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all timings, bar lengths, and line points are the hardcoded literal values above (no randomness); the 3s-translate / 1s-whisk / 10s-drill numbers must match across the text, the c2 bars, and the c4 lines; invented timings keep their "illustrative" captions.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
