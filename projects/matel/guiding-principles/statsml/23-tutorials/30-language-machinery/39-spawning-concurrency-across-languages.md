# Spawning Concurrency Across Languages

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Spawning Concurrency Across Languages

**Subtitle:** Every language has a verb for "go do this while I keep working" — goroutines, executors, tasks, callbacks, threads — and the real difference is who the extra worker is and who schedules them

## Six Orders, One Cook

**Tags:** `core idea` (blue), `spawning` (green), `running example` (orange)

- **The shop** — a sandwich shop at noon: six orders land at once, each takes 3 minutes of work
- **One cook** — making them strictly one at a time finishes the last plate at 6 × 3 = 18 minutes
- **The spawn** — shouting "go make order #2" hands a job to another worker while you keep cooking
- **Three cooks** — split the six orders two each and the last plate lands at 6 minutes, not 18
- **Every language** — each has this "go" verb; they differ only in who the extra worker really is

*Example (italic):* The head cook keeps orders #1–#2, shouts "go" for the rest, and two helpers take two orders each — same six sandwiches, done three times sooner.

**Key point:** Spawning concurrency is one idea everywhere: hand a job to another worker and keep going — languages differ in who that worker is, not in the idea.

### Visualization (canvas `c1`, 720×300)

Two-scenario Gantt chart on a shared minutes axis: one cook doing six 3-minute orders back-to-back on the top row, then three spawned cooks doing two orders each on three lower rows, with finish lines at 18 and 6 minutes.

- **Title (bold 15px, `#1a5276`, top center):** "'Go' = Hand the Order to Another Pair of Hands".
- **Axis:** horizontal 2px `#999` line at y=262 from x=200 to x=680 (width 480), minutes 0 to 18 (26.67 px/min); 12px `#444` tick labels "0", "3", "6", "9", "12", "15", "18" every 3 minutes; 12px `#6b7280` axis caption "minutes" below the right end.
- **Row 1 (bar center y=88), 12px `#444` label at x=20:** "one cook, one at a time"; six 18px-tall bars covering minutes `[0–3, 3–6, 6–9, 9–12, 12–15, 15–18]`, fills alternating `rgba(42,120,214,0.60)` and `rgba(42,120,214,0.35)`; 11px white/`#1a5276` labels "#1".."#6" centered in each bar.
- **Rows 2–4 (bar centers y=170, 200, 230), labels:** "cook A (spawned)", "cook B (spawned)", "cook C (spawned)"; each row two green `rgba(0,131,0,0.45)` 18px bars — A: `[0–3, 3–6]` orders #1/#4, B: `[0–3, 3–6]` orders #2/#5, C: `[0–3, 3–6]` orders #3/#6; 11px labels "#1".."#6" in the bars.
- **Finish markers:** vertical dashed (dash 4/3) blue `#2a78d6` line at minute 18 from y=70 to the axis, bold 13px blue label "done at 18 min" at its top; vertical dashed green `#008300` line at minute 6 from y=150 to the axis, bold 13px green label "done at 6 min" at its top.
- **Annotation (bold 12px orange `#d95926`, two lines near x=390, y=120):** "spawn two helpers:" / "same six orders, 6 min not 18".
- **Caption (12px `#444`, bottom right):** "illustrative — a 3-minute sandwich is invented for easy arithmetic".

## Six Sandwiches, Three Plans

**Tags:** `worked example` (blue), `concurrency vs parallelism` (green)

- **The split** — look closer: each sandwich is 1 minute of hands-on work plus 2 minutes in the toaster
- **Plan A** — one cook, one at a time: 6 × (1 + 2) = 18 minutes, hands idle during every toast
- **Plan B** — one cook starts the next sandwich while toasts run: 6 × 1 hands + final 2-min toast = 8
- **Plan C** — spawn: three cooks take two orders each, 2 × 3 = 6 minutes per cook, done at 6
- **Two tricks** — plan B reuses waiting time (concurrency); plan C adds hands (parallelism)

*Example (italic):* With zero extra staff the clever cook drops 18 minutes to 8 just by filling toaster waits; hiring two helpers drops it to 6.

**Key point:** 18 → 8 came from reusing waits with the same one worker; 18 → 6 came from more workers — the two speedups are different tools, and every language picks a side.

### Visualization (canvas `c2`, 720×300)

Horizontal bar chart of the three plans' total minutes, each bar labeled with the arithmetic the reader can redo by hand.

- **Title (bold 15px, `#1a5276`, top center):** "Six Sandwiches, Three Plans: 18 vs 8 vs 6 Minutes".
- **Axis:** horizontal 2px `#999` line at y=258 from x=230 to x=680 (width 450), minutes 0 to 18 (25 px/min); 12px `#444` tick labels "0", "3", "6", "9", "12", "15", "18"; light `#e5e9ef` vertical gridlines at each tick from y=70 to the axis.
- **Rows (bar centers at y = 100, 160, 220), 26px-tall bars from x=230, left-aligned 12px `#444` two-line row labels at x=20:**
  - "one cook, one at a time": blue `#2a78d6` bar, length 18 min (450px); bold 13px blue label at bar end "18 min = 6 × 3".
  - "one cook, reusing toaster waits": orange `#d95926` bar, length 8 min (200px); bold 13px orange label "8 min = 6 × 1 hands + last toast 2".
  - "three cooks, two orders each": green `#008300` bar, length 6 min (150px); bold 13px green label "6 min = 2 × 3 per cook".
- **Annotation (bold 12px violet `#4a3aa7`, two lines near x=430, y=185):** "waiting time is free speed —" / "extra cooks cost extra hands".
- **Caption (12px `#444`, bottom right):** "illustrative — 1 min hands-on + 2 min toasting per sandwich".

## Every Language's "Go" Verb

**Tags:** `where it's used` (blue), `decision map` (green), `six languages` (orange)

- **Go** — `go makeSandwich(4)`: one keyword hands the job to a goroutine the Go runtime schedules
- **Java** — `executor.submit(task)`: drop the job into a pool of pre-hired OS-thread cooks
- **Python** — `asyncio.create_task(...)` queues it on one loop; `threading.Thread` hires an OS thread
- **JavaScript** — no spawn in the core language: callbacks/Promises on one loop; parallelism needs Workers
- **Rust** — `tokio::spawn(task)`: a light task on a runtime that juggles thousands per thread
- **C** — `pthread_create(&t, ...)`: ask the operating system itself for a whole new thread

*Example (italic):* The same sentence — "go make sandwich #4" — is one goroutine in Go, a pool submission in Java, a queued task in Python or JS, and a brand-new OS thread in C.

**Key point:** Pick by what the worker is: event-loop tasks are shared-cook turn-taking, runtime tasks are cheap hires by the thousands, OS threads are real but expensive staff.

### Visualization (canvas `c3`, 720×300)

Decision-map scatter: seven labeled dots placed on two qualitative axes — who runs the worker (left to right: one shared loop, runtime scheduler, OS threads) and how heavy each extra worker is (bottom light, top heavy).

- **Title (bold 15px, `#1a5276`, top center):** "Who Is the Extra Worker? A Map of the 'Go' Verbs".
- **Frame:** plot area x=60 to x=680, y=70 to y=245; x baseline 2px `#999` at y=245; three zones split by light dashed `#e5e9ef` vertical lines at x=265 and x=470; 12px `#6b7280` zone labels centered below the baseline at y=265: "one shared loop", "runtime-scheduled tasks", "OS threads".
- **Y axis:** 12px `#6b7280` rotated label "cost per extra worker" along x=30; 11px `#6b7280` guide words "light" at (60, 240) and "heavy" at (60, 80).
- **Dots (7px radius) with bold 12px same-color labels beside each, hardcoded positions:**
  - "JS callback / Promise" — blue `#2a78d6` at (150, 232)
  - "Python asyncio.create_task" — aqua `#199e70` at (195, 215)
  - "Go go f()" — green `#008300` at (330, 208)
  - "Rust tokio::spawn" — orange `#d95926` at (405, 198)
  - "Java executor.submit" — violet `#4a3aa7` at (530, 150)
  - "Python threading.Thread" — yellow `#c98500` at (565, 118)
  - "C pthread_create" — magenta `#d55181` at (625, 92)
- **Annotation (bold 12px ink `#1a5276`, two lines near x=90, y=100):** "further right = a real OS thread;" / "higher = heavier to spawn".
- **Caption (12px `#444`, bottom right):** "illustrative — positions show qualitative ordering, not measurements".

## Async Is Not More Cooks

**Tags:** `common mistake` (red), `cpu vs waiting` (orange)

- **The trap** — hearing "async" and expecting pure computation to speed up; async adds no hands
- **No toaster** — six chopping-only orders take one cook 18 minutes, async code or not: still 18
- **One pair of hands** — JS and asyncio run one cook who is brilliant at waiting, never at chopping two things
- **Real hires** — goroutines, tokio tasks, executors, and pthreads can add hands, so chopping time falls too
- **The rule** — waiting-heavy work suits the event loop; hands-on-heavy work needs real extra workers

*Example (italic):* A team rewrote a number-crunching job with async/await and measured the same 18-minute wall clock — there was no waiting to reuse, only chopping.

**Common mistake:** Treating concurrency and parallelism as the same thing. Async reuses waiting time inside one worker; only spawning real workers shortens pure hands-on work.

### Visualization (canvas `c4`, 720×300)

Horizontal bar chart of chopping-only totals (zero toaster time) for the same three plans, showing the async plan collapsing back to the sequential 18 minutes.

- **Title (bold 15px, `#1a5276`, top center):** "No Toaster, No Trick: Pure Chopping Ignores Async".
- **Axis:** horizontal 2px `#999` line at y=258 from x=230 to x=680 (width 450), minutes 0 to 18 (25 px/min); 12px `#444` tick labels "0", "3", "6", "9", "12", "15", "18"; light `#e5e9ef` gridlines at each tick from y=70.
- **Rows (bar centers at y = 100, 160, 220), 26px-tall bars from x=230, 12px `#444` row labels at x=20:**
  - "one cook, one at a time": blue `#2a78d6` bar, length 18 min (450px); bold 13px blue label "18 min".
  - "one cook, async style": orange `#d95926` bar, length 18 min (450px); bold 13px orange label "18 min — nothing to overlap".
  - "three cooks, two orders each": green `#008300` bar, length 6 min (150px); bold 13px green label "6 min".
- **Annotation (bold 13px red `#e74c3c`, two lines near x=300, y=130):** "async reuses waiting —" / "with zero waiting it saves zero minutes".
- **Caption (12px `#444`, bottom right):** "illustrative — six 3-minute all-hands-on orders, no waiting anywhere".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all bar lengths, Gantt spans, and scatter positions are the hardcoded values above (no randomness); minute totals in text and charts must stay in lockstep (18 / 8 / 6, and 18 / 18 / 6). Language comparisons remain qualitative — the c3 map encodes ordering only, and its caption says so.
- **Code snippets:** the language verbs (`go f()`, `executor.submit`, `asyncio.create_task`, `tokio::spawn`, `pthread_create`) appear only inside bullets as inline `<code>`; no multi-line code blocks on the page.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
