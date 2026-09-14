# Java Threads & ExecutorService

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Java Threads & ExecutorService

**Subtitle:** A thread is a worker you hire and manage yourself; an ExecutorService is a fixed crew with a ticket rail — you submit the task and the crew handles who runs it and when

## A Pizza Shop That Hires a Cook per Order

**Tags:** `core idea` (blue), `threads` (green), `thread pool` (orange)

- **The shop** — a pizza shop gets 12 orders in a rush; someone has to cook each one
- **A thread** — in Java, a thread is one cook: `new Thread(order).start()` hires a fresh cook per order
- **The problem** — 12 orders means 12 hires; hiring is slow, and 12 cooks jam a 3-burner kitchen
- **The crew** — an `ExecutorService` is 3 cooks hired once, plus a ticket rail where orders wait
- **Submit, don't hire** — `pool.submit(order)` pins a ticket on the rail; a free cook grabs the next one
- **The rail** — tickets that no cook is free for simply wait in line — nothing is lost, nothing crashes

*Example (italic):* Order #7 arrives while all 3 cooks are busy — it hangs on the rail for a few minutes, then cook B finishes order #4 and picks it up.

**Key point:** A thread is the worker; a task is the work order. ExecutorService keeps a fixed crew of workers and a queue of tasks, so you stop managing cooks and start submitting tickets.

### Visualization (canvas `c1`, 720×300)

Two-panel side-by-side diagram: left panel shows thread-per-task (12 cook boxes crowding one kitchen), right panel shows a pool (3 cook boxes plus a ticket rail holding waiting orders).

- **Title (bold 15px, `#1a5276`, top center):** "One Cook per Order vs a Crew with a Ticket Rail".
- **Panels:** left panel x=20 to x=350, right panel x=380 to x=700; both from y=50 to y=270; 1px `#e5e9ef` border, panel labels bold 13px `#2c3e50` at each panel's top left: "new Thread() per order — 12 cooks" and "ExecutorService — 3 cooks + rail".
- **Left panel:** 12 small rounded boxes (44×34) in a 4×3 grid starting at x=45, y=95, gaps 30px horizontal / 22px vertical; fill `rgba(217,89,38,0.18)`, 2px `#d95926` border, centered 12px `#d95926` labels "C1"–"C12"; below the grid a bold 12px `#d95926` line at y=250 centered: "12 hires, one 3-burner kitchen".
- **Right panel:** 3 cook boxes (60×40) stacked at x=410, y = 95, 150, 205; fill `rgba(0,131,0,0.18)`, 2px `#008300` border, centered 12px `#008300` labels "cook A", "cook B", "cook C". Ticket rail: horizontal 3px `#6b7280` line at y=75 from x=520 to x=690, with 5 small ticket boxes (26×32) hanging below it at x = `[530, 562, 594, 626, 658]`, fill `rgba(42,120,214,0.18)`, 1.5px `#2a78d6` border, 11px `#2a78d6` labels "#8"–"#12"; 12px `#6b7280` label "tickets waiting, minutes in" under the rail at y=130. Three 2px `#2a78d6` arrows from the rail's left end to each cook box.
- **Annotation (bold 12px violet `#4a3aa7`, right panel bottom, two lines near x=530, y=240):** "you submit tickets —" / "the crew stays fixed".
- **Caption (12px `#444`, bottom right):** "illustrative — cooks are threads, tickets are tasks".

## Three Cooks, Twelve Orders: Check the Timeline by Hand

**Tags:** `worked example` (blue), `fixed pool` (green)

- **Setup** — every pizza takes 4 minutes; the shop makes the crew with `Executors.newFixedThreadPool(3)`
- **The rush** — all 12 orders are submitted at minute 0; the rail instantly holds tickets #4–#12
- **Round one** — cooks A, B, C take orders #1, #2, #3 and finish together at minute 4
- **Keep pulling** — each free cook grabs the next ticket: 4 rounds of 3 pizzas each
- **Finish line** — 12 orders ÷ 3 cooks = 4 rounds, and 4 rounds × 4 min = 16 minutes total
- **One cook** — the same 12 orders with a single cook would take 12 × 4 = 48 minutes

*Example (italic):* Cook A alone handles orders #1, #4, #7, #10 — finishing them at minutes 4, 8, 12, and 16 — and each of the other two cooks does the same with their four.

**Key point:** With a fixed pool, total time is just arithmetic: 12 tasks ÷ 3 workers × 4 min each = 16 minutes — and you can trace which cook ran which order by hand.

### Visualization (canvas `c2`, 720×300)

Gantt-style timeline: three cook rows, each holding four 4-minute order blocks laid end to end from minute 0 to 16, showing every order's start and finish.

- **Title (bold 15px, `#1a5276`, top center):** "12 Orders, 3 Cooks, 4 Minutes Each → Done at Minute 16".
- **Axes:** time axis 2px `#999` at y=245 from x=110 to x=670 (width 560), minutes 0 to 16; 12px `#444` tick labels "0", "4", "8", "12", "16" at minutes `[0, 4, 8, 12, 16]`; light `#e5e9ef` vertical gridlines at those minutes from y=60 to the axis.
- **Rows:** three lanes at y = 85, 140, 195 (bar height 34), left-aligned 12px `#444` labels at x=20: "cook A", "cook B", "cook C".
- **Order blocks:** each block spans 4 minutes (140px); cook A holds orders `[1, 4, 7, 10]`, cook B `[2, 5, 8, 11]`, cook C `[3, 6, 9, 12]`, at start minutes `[0, 4, 8, 12]`; fills cycle by round: `rgba(42,120,214,0.30)`, `rgba(0,131,0,0.30)`, `rgba(217,89,38,0.30)`, `rgba(74,58,167,0.30)` with 1.5px borders in `#2a78d6`, `#008300`, `#d95926`, `#4a3aa7`; centered bold 12px labels "#1"–"#12" in the matching border color.
- **Finish marker:** vertical dashed `#6b7280` (dash 4/3) line at minute 16 from y=60 to the axis, bold 13px `#008300` label at its top: "all done: 16 min".
- **Annotation (bold 12px ink `#1a5276`, near x=130, y=68):** "12 ÷ 3 cooks × 4 min = 16 min (one cook: 48 min)".
- **Caption (12px `#444`, bottom right):** "illustrative — every pizza takes exactly 4 minutes".

## Why Java Programmers Reach for the Pool

**Tags:** `where it's used` (blue), `cost of threads` (orange), `right-sizing` (green)

- **Hiring is expensive** — starting a thread costs real time and ~1 MB of reserved stack space each
- **Too many cooks** — 10,000 orders as 10,000 threads reserves ~10 GB of stacks and jams the CPU
- **Reuse** — a pool hires its cooks once and reuses them for every ticket that ever arrives
- **Diminishing returns** — for our 12 orders: 1 cook = 48 min, 3 = 16, 6 = 8, 12 = 4 — but burners run out
- **Receipts** — `submit` returns a `Future`; `future.get()` waits for that order and hands you the result
- **Closing time** — call `pool.shutdown()` or the crew stays on the clock and the program never exits

*Example (italic):* Going from 1 cook to 3 saves 32 minutes; going from 6 to 12 saves only 4 — and past the number of burners (CPU cores), extra cooks mostly stand around.

**Key point:** Threads are expensive to create and to keep; a pool caps the cost at a fixed crew size while the queue absorbs any burst of work.

### Visualization (canvas `c3`, 720×300)

Bar chart: total finish time for the same 12 four-minute orders as the crew grows, showing the big early wins and the flat tail.

- **Title (bold 15px, `#1a5276`, top center):** "Same 12 Orders — Finish Time by Crew Size".
- **Axes:** origin x=90, baseline y=245, plot width 570, plot height 180; y axis = minutes 0 to 50 with 12px `#444` labels "0", "10", "20", "30", "40", "50" and light `#e5e9ef` gridlines; x axis = crew size with 12px `#444` labels under each bar.
- **Bars:** six bars, crew sizes `[1, 2, 3, 4, 6, 12]`, finish minutes `[48, 24, 16, 12, 8, 4]`; bar width 58, evenly spaced starting x=115 with 36px gaps; the crew-size-3 bar fill `rgba(0,131,0,0.45)` with 2px `#008300` border (the pool from the worked example), all other bars fill `rgba(42,120,214,0.30)` with 1.5px `#2a78d6` border; bold 12px value labels on top of each bar ("48", "24", "16", "12", "8", "4") in the bar's border color.
- **Highlight:** 12px `#008300` label "our pool" directly under the crew-size-3 axis label.
- **Annotation (bold 12px orange `#d95926`, near x=420, y=100, two lines):** "1→3 cooks saves 32 min;" / "6→12 saves only 4".
- **Caption (12px `#444`, bottom right):** "illustrative — assumes 4 min per order, no waiting on burners".

## Submit the Ticket, Don't Hire the Cook

**Tags:** `common mistake` (red), `submit vs new Thread` (orange)

- **The habit** — people learn `new Thread(task).start()` first and keep writing it inside loops
- **The tell** — a `new Thread` inside a loop means one hire per order: the 12-cook kitchen again
- **The fix** — build the crew once, then only `pool.submit(task)` inside the loop
- **Task, not thread** — the thing you submit is a `Runnable` or `Callable` (the ticket), never a thread
- **Wrapped, not run** — `pool.submit(new Thread(task))` compiles but runs the thread as a plain ticket
- **Rule of thumb** — code says `new Thread` at most once or twice per program, often zero times

*Example (italic):* A batch job that wrote `new Thread(row).start()` per row ran fine on 100 test rows, then hit 2 million rows in production and died creating threads.

**Common mistake:** Submitting threads instead of tasks. `ExecutorService` already owns the threads — hand it the work (`Runnable`/`Callable`), and let the fixed crew run it.

### Visualization (canvas `c4`, 720×300)

Side-by-side code-card diagram: the loop that hires a cook per order on the left (flagged), the build-crew-once-then-submit loop on the right (approved), with one arrow each showing where the work goes.

- **Title (bold 15px, `#1a5276`, top center):** "Inside the Loop: Hire a Cook, or Pin a Ticket?".
- **Left card:** rounded box x=25 to x=345, y=55 to y=195, fill `rgba(231,76,60,0.08)`, 2px `#e74c3c` border; header bold 13px `#e74c3c` at top left inside: "per order — don't"; three 12px monospace `#2c3e50` code lines at x=45, y = 105, 130, 155: "for (Order o : orders) {", "  new Thread(o).start();", "}"; bold 12px `#e74c3c` line centered under the card at y=225: "2,000,000 orders → 2,000,000 threads".
- **Right card:** rounded box x=375 to x=695, y=55 to y=195, fill `rgba(0,131,0,0.08)`, 2px `#008300` border; header bold 13px `#008300`: "crew once, then submit"; four 12px monospace `#2c3e50` code lines at x=395, y = 100, 125, 150, 175: "var pool = Executors.newFixedThreadPool(3);", "for (Order o : orders) {", "  pool.submit(o);", "}"; bold 12px `#008300` line centered under the card at y=225: "2,000,000 orders → still 3 threads".
- **Arrows:** 2px `#e74c3c` arrow from the left card's `new Thread` line down to its red caption; 2px `#008300` arrow from `pool.submit(o)` down to its green caption.
- **Annotation (bold 13px magenta `#d55181`, centered at y=270):** "submit tasks, not threads — the pool already owns the cooks".
- **Caption (12px `#444`, bottom right, at y=290):** "illustrative — line counts trimmed for the card".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all bar values, block placements, and box coordinates are the hardcoded literals above (no randomness); the finish-time bars are exact arithmetic (12 orders × 4 min ÷ crew size, rounded to whole minutes), and text numbers must match chart numbers (16 min, 48 min, crew of 3).
- Code lines inside the c4 cards are drawn as canvas monospace text, not real `<pre>` blocks.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
