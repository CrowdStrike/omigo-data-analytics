# Debuggers

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Debuggers

**Subtitle:** A debugger freezes a running program at any line so you can inspect every variable and the full call stack — breakpoints, the stack inspected live

## Pausing at Line 41 and Looking Around

**Tags:** `core idea` (blue), `breakpoints` (green), `live inspection` (orange)

- **The bug** — a pricing pipeline outputs one negative total among a million orders
- **The breakpoint** — mark line 41; the program runs at full speed, then freezes right there
- **Look around** — every variable is inspectable at the pause: price=19.99, qty=3, discount=-2.50
- **The call stack** — the frames show HOW execution got here: main → load_orders → price_order
- **Ask questions** — evaluate any expression in the paused context, e.g. price*qty-discount
- **Under the hood** — the OS traps execution; debug symbols map machine code back to source lines

*Example (italic):* The run pauses at line 41 with discount = -2.50 in plain view — the bug is found before the crash line ever executes.

**Key point:** A breakpoint turns a running program into a frozen scene you can walk through — every variable, every stack frame, live — instead of guessing from its output.

### Visualization (canvas `c1`, 720×300)

Panel diagram of the paused program: a source panel with the breakpoint line highlighted, a variables panel, and a call-stack panel, mimicking a debugger UI.

- **Title (bold 15px, `#1a5276`, top center):** "Paused at Line 41: the Whole Program State, Live".
- **Source panel:** rounded box x=20, y=50, w=300, h=215, fill `rgba(42,120,214,0.08)`, 1px `#2a78d6` border; seven 12px monospace `#2c3e50` lines at y=78 spaced 28px, left-numbered 38–44: `38 for order in orders:`, `39   price = order.price`, `40   qty = order.qty`, `41   total = price*qty - discount`, `42   emit(order.id, total)`, `43`, `44 def apply_discount(o):`; line 41 gets a full-width highlight bar `rgba(201,133,0,0.18)`, a red `#e74c3c` filled circle (r=5) in the gutter, and a bold `#d95926` "▶" arrow at its left edge.
- **Variables panel:** box x=345, y=50, w=170, h=135, fill `rgba(0,131,0,0.06)`, 1px `#008300` border; bold 12px `#1a5276` header "Variables" at top; 12px monospace rows: `price = 19.99`, `qty = 3`, `discount = -2.50` (bold red `#e74c3c`), `total = <not yet>`.
- **Call-stack panel:** box x=540, y=50, w=160, h=135, fill `rgba(74,58,167,0.06)`, 1px `#4a3aa7` border; bold 12px `#1a5276` header "Call stack"; 12px rows top-down: `price_order :41` (bold, topmost frame), `load_orders :88`, `main :12`.
- **Annotation (bold 13px red `#e74c3c`, at x=345, y=225):** "discount = -2.50 — seen before the crash".
- **Caption (12px `#444`, bottom right):** "values illustrative".

## Catching Order 4711 Without Pressing Continue a Million Times

**Tags:** `worked example` (blue), `stepping` (green), `conditional breakpoints` (orange)

- **Step over** — execute line 41 as one unit and stop at line 42; total updates before your eyes
- **Step into** — descend into apply_discount() and walk its internals one statement at a time
- **Step out** — finish the current function and pause back in the caller's frame
- **The condition** — `break at 41 if orderId == 4711` skips the other 999,999 pauses entirely
- **The watchpoint** — pause when discount changes VALUE; it flips to -2.50 in a stale coupon routine

*Example (italic):* The conditional breakpoint fires exactly once, at iteration 831,204 of 1,000,000 — and the watchpoint names the mystery mutator on the same pause.

**Key point:** Stepping walks the program one statement at a time; conditions and watchpoints make the pause land exactly where the mystery is instead of everywhere.

### Visualization (canvas `c2`, 720×300)

Three-row timeline over one million loop iterations showing where each breakpoint style fires: plain (everywhere), conditional (once), watchpoint (on the two writes).

- **Title (bold 15px, `#1a5276`, top center):** "One Million Iterations: Where Each Breakpoint Fires".
- **Axis:** horizontal 2px `#999` baseline at y=250 from x=60 to x=660 (600px = 1,000,000 iterations); 12px `#444` tick labels below at x=60/210/360/510/660: "0", "250k", "500k", "750k", "1M".
- **Row 1 (band centered y=90), 12px `#444` label at x=20 (above the band):** "plain break, line 41"; orange band `rgba(217,89,38,0.35)` from x=60 to x=660, 16px tall, with bold 12px `#d95926` label to its upper right: "fires 1,000,000 times — you press continue all day".
- **Row 2 (y=155), label:** "if orderId == 4711"; single green `#008300` marker (4px wide, 22px tall) at x=559 (iteration 831,204), bold 12px `#008300` label "fires once — iteration 831,204".
- **Row 3 (y=220), label:** "watch discount"; two violet `#4a3aa7` markers (4px wide, 22px tall) at x=60 (set at load) and x=559 (the overwrite), bold 12px `#4a3aa7` label "second write = the mystery mutator".
- **Caption (12px `#444`, bottom right):** "iteration numbers illustrative".

## The Economics: One Pause vs Six Reruns

**Tags:** `where it's used` (blue), `pipelines` (green)

- **The trade** — a print answers one guess per run; a pause answers every question you think of
- **The math** — six hypotheses as prints: 6 reruns × 20 min = 120 min; one debug pause: 30 min
- **Pipelines** — stepping through the failing stage beats sprinkling prints in a 20-minute job
- **Post-mortem** — a core dump snapshots the crash; the debugger inspects the corpse's stack later
- **The skill** — debugger fluency is a force multiplier most people never spend one afternoon on

*Example (italic):* The same six questions cost 120 minutes as print rounds and 30 minutes as one paused inspection of the failing pipeline stage.

**Key point:** Prints are fine for cheap hypotheses, but every round is an edit-rerun cycle; a debugger amortizes one run across many answers — the gap widens with run time.

### Visualization (canvas `c3`, 720×300)

Horizontal segmented bar chart comparing total wall-clock time: six print-debugging rounds vs one debugger session, on a shared minutes scale.

- **Title (bold 15px, `#1a5276`, top center):** "Six Hypotheses: Print Rounds vs One Debug Pause".
- **Scale:** bars extend right from x=170, 4px per minute (max 480px = 120 min); light gridlines `#e5e9ef` at 30/60/90/120 min (x=290/410/530/650) with 11px `#6b7280` labels at y=265.
- **Row 1 (bar top y=80, 22px tall), 12px `#444` label at x=20:** "prints: 6 edit-rerun rounds"; six adjacent segments each 80px wide (20-min rerun), fill `rgba(42,120,214,0.30)` with 1px `#2a78d6` separators; bold 12px `#2a78d6` label at bar end: "6 × 20 min = 120 min".
- **Row 2 (bar top y=160, 22px tall), label:** "debugger: one pause"; green segment 80px `rgba(0,131,0,0.25)` (the 20-min run to the breakpoint) + aqua `#199e70` solid segment 40px (10 min inspecting at the pause); bold 12px `#008300` label at bar end: "20 min run + 10 min at the pause = 30 min".
- **Annotation (bold 13px magenta `#d55181`, near x=300, y=235):** "the pause answers all six questions in one run".
- **Caption (12px `#444`, bottom right):** "minutes illustrative".

## When Prints Actually Win

**Tags:** `common mistake` (red), `heisenbugs` (orange)

- **Heisenbugs** — pausing changes timing; the race condition vanishes while you watch for it
- **Production** — attaching a debugger freezes a live service; logs observe without stopping it
- **Distributed** — pause one node and its peers time out, retry, and create brand-new failures
- **Timing-sensitive** — where microseconds matter, a print perturbs far less than a full pause
- **The mistake** — treating the debugger as strictly better; it is one tool, priced by context

*Example (italic):* A node paused for 30 seconds trips its peer's 5-second timeout — the failure you now see belongs to the debugger, not to your code.

**Common mistake:** Reaching for the debugger where a pause changes behavior — for races, live production, and distributed systems, logs and prints are the honest instrument.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: pausing one node of a distributed system (peer timeout, false failure) vs logging (timing preserved), shown as boxes with arrows.

- **Title (bold 15px, `#1a5276`, top center):** "Why a Pause Can Create the Bug: the Heisenbug Trap".
- **Row 1 (boxes centered y=95), 12px `#444` label at x=20:** "debugger"; blue `#2a78d6` rounded box at x=140 labeled "node A paused at breakpoint" (12px), 3px arrow to a red `#e74c3c` box at x=420 labeled "peer times out at 5s, retries" with bold 12px red "✗ new failure, not your bug" beneath it.
- **Row 2 (y=205), label:** "logs / prints"; blue box at x=140 "node A runs at full speed", 3px arrow to a green `#008300` box at x=380 labeled "timing preserved", then arrow to a green box at x=560 labeled "read logs after" with bold 12px green "✓".
- **Box style:** 150–170px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(231,76,60,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px orange `#d95926`, centered near y=270):** "observe without stopping when stopping changes the answer".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all values are the hardcoded literals above (no randomness); order values (19.99 / 3 / -2.50), the iteration count 831,204 of 1,000,000, and the timing figures (6 × 20 min = 120 min vs 20 + 10 = 30 min) are invented and labeled illustrative; text numbers and chart numbers must stay identical.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
