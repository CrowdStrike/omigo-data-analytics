# Pthreads

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Pthreads

**Subtitle:** When a program hires helpers that all work inside the same memory, the hiring paperwork is pthreads — the small C library that, on Unix-like systems, nearly every language's threads are a thin wrapper around

## Four Cooks, One Kitchen

**Tags:** `core idea` (blue), `shared memory` (green), `threads` (orange)

- **The rush** — a sandwich shop gets slammed at lunch; one cook can't keep up, so the owner hires three more
- **Same kitchen** — all four cooks work in one kitchen: same fridge, same order board, same tip jar
- **A thread** — each cook is a thread: own hands and own to-do list (a stack), everything else shared
- **pthread_create** — in C, hiring is one call: `pthread_create` starts a new cook on a function you name
- **pthread_join** — at closing the owner waits at the door for each cook to finish: that wait is `pthread_join`
- **Not a new shop** — a second process would be a whole second shop with its own fridge; threads stay in one

*Example (italic):* The owner (the main thread) calls pthread_create three times, four cooks share one kitchen all lunch, and at 3pm the owner joins each one before locking up.

**Key point:** A pthread is a worker inside your process: it gets its own stack, but the heap and globals — the fridge and the tip jar — are shared by everyone.

### Visualization (canvas `c1`, 720×300)

Box diagram: one large process box containing a shared-memory band and four thread stacks, next to a smaller separate process box, showing what threads share and what they don't.

- **Title (bold 15px, `#1a5276`, top center):** "One Process, Four Threads — Shared Kitchen, Private Stacks".
- **Process box:** rounded rect from (40, 55) to (490, 275), 2px ink `#1a5276` border, fill `rgba(42,120,214,0.06)`; bold 13px ink label inside top-left: "the shop (one process)".
- **Shared band:** rect from (60, 90) to (470, 135), fill `rgba(0,131,0,0.12)`, 2px green `#008300` border; centered bold 12px green text: "shared: fridge, order board, tip jar (heap & globals)".
- **Thread stacks:** four rects 90×95 at x = 60, 165, 270, 375, all y = 155–250, fill `rgba(42,120,214,0.18)`, 2px blue `#2a78d6` border; bold 12px blue labels centered near each top: "main", "cook 2", "cook 3", "cook 4"; 11px `#6b7280` "own stack" under each label; a thin blue arrow from each rect's top edge up to the shared band.
- **Other process:** rounded rect from (530, 90) to (690, 250), 2px dashed `#6b7280` border, fill `rgba(107,114,128,0.08)`; 12px `#6b7280` centered text, three lines: "another process" / "= another shop" / "own fridge, no sharing".
- **Annotation (bold 13px orange `#d95926`, centered near x=265, y=268):** "threads share everything except their stacks".
- **Caption (11px `#444`, bottom right):** "illustrative — the pthread memory picture".

## The Tip Jar Loses Two Dollars

**Tags:** `worked example` (blue), `race condition` (red), `mutex` (orange)

- **The jar** — the tip jar starts at $0; cook A and cook B each drop in three $1 tips during the rush
- **Hidden steps** — "drop a tip" is really three steps: read the jar, add 1 in your head, write it back
- **The overlap** — A reads 0, B reads 0, A writes 1, B writes 1 — B just erased A's tip; jar shows $1, not $2
- **Run it out** — with two such overlaps in 12 steps, the jar ends at $4 instead of the true $6
- **The key** — `pthread_mutex_lock` is one jar key: only the key-holder may touch the jar, then hands it back
- **Fixed** — with the key, each read-add-write finishes whole, and the jar always ends at exactly $6

*Example (italic):* Trace it by hand: A r0, B r0, A w1, B w1, A r1, A w2, B r2, B w3, A r3, B r3, A w4, B w4 — six tips in, $4 in the jar.

**Key point:** Two overlapping read-add-write triples silently ate $2; a pthread mutex makes each triple atomic, so 3 + 3 tips always equals $6.

### Visualization (canvas `c2`, 720×300)

Two-lane timeline: cook A's and cook B's read/write steps laid out left to right in the unlucky order above, with the jar's running value along the bottom and the two lost updates flagged in red.

- **Title (bold 15px, `#1a5276`, top center):** "Six $1 Tips, Two Cooks, No Lock — the Jar Ends at $4".
- **Lanes:** two horizontal 2px `#999` lines at y=115 (cook A, blue) and y=175 (cook B, orange), from x=95 to x=660; bold 12px labels at x=20: blue `#2a78d6` "cook A", orange `#d95926` "cook B".
- **Steps:** 12 step slots at x = `[110, 158, 206, 254, 302, 350, 398, 446, 494, 542, 590, 638]`; step sequence (lane, action) = `[A r0, B r0, A w1, B w1, A r1, A w2, B r2, B w3, A r3, B r3, A w4, B w4]`; each step is a 9px dot on its cook's lane with a bold 12px label above it ("r0", "w1", ...), blue on A's lane, orange on B's.
- **Lost updates:** steps 4 ("B w1") and 12 ("B w4") get a red `#e74c3c` dot, red label, and an 11px red tag "lost!" just below the lane.
- **Jar row:** 12px `#444` label "jar:" at x=20, y=235; running jar value after each step, 12px `#2c3e50` at y=235 under each slot: `[0, 0, 1, 1, 1, 2, 2, 3, 3, 3, 4, 4]`; the final "4" bold 13px red `#e74c3c`.
- **Annotation (bold 13px red `#e74c3c`, near x=430, y=70):** "$6 expected, $4 in the jar — two tips overwritten".
- **Caption (11px `#444`, bottom right):** "one unlucky but legal interleaving — illustrative".

## The C Floor Under Every Language's Threads

**Tags:** `where it's used` (blue), `wrappers` (green), `runtimes` (orange)

- **The floor** — on Linux and macOS, Python's `threading.Thread`, Java's `Thread`, and C++'s `std::thread` all bottom out in `pthread_create`
- **NumPy too** — BLAS math libraries keep a pthread worker pool; `OMP_NUM_THREADS` sets its head-count
- **Same bugs** — the tip-jar race lives in every wrapper, because the sharing model IS the pthreads model
- **The GIL** — CPython threads are real pthreads; the GIL is one giant mutex the interpreter passes between them
- **Reading crashes** — stack traces mention `pthread_mutex_lock` and friends; knowing the floor makes them readable

*Example (italic):* A data scientist sets OMP_NUM_THREADS=8 and never types "pthread", yet eight pthread_create calls fire under the hood — while sklearn's n_jobs instead spawns workers that are often separate processes.

**Key point:** Pthreads is the shared C layer under nearly every thread you'll ever use — so its rules (shared memory, mutexes, joins) are the real rules of your language's threads.

### Visualization (canvas `c3`, 720×300)

Layer diagram: four language-level thread APIs as boxes on top, arrows funneling into one wide pthreads band, which rests on the OS kernel band — the wrapping made visible.

- **Title (bold 15px, `#1a5276`, top center):** "Different Languages, One C Floor".
- **Top row (four boxes, 150×52 at y=60–112, x = 45, 210, 375, 540):** "Python / threading" (blue `#2a78d6` border, fill `rgba(42,120,214,0.12)`), "Java / Thread" (orange `#d95926` border, fill `rgba(217,89,38,0.10)`), "C++ / std::thread" (green `#008300` border, fill `rgba(0,131,0,0.10)`), "OpenMP / BLAS pool" (violet `#4a3aa7` border, fill `rgba(74,58,167,0.10)`); centered bold 12px labels in the border color.
- **Arrows:** 2px `#6b7280` arrow from each box's bottom center down to the pthreads band.
- **Pthreads band:** rect from (45, 160) to (690, 212), fill `rgba(26,82,118,0.14)`, 2px ink `#1a5276` border; centered bold 14px ink text: "pthreads — the C threading library"; 11px `#6b7280` right-aligned inside: "pthread_create · pthread_join · pthread_mutex".
- **Kernel band:** rect from (45, 235) to (690, 275), fill `rgba(107,114,128,0.12)`, 2px `#6b7280` border; centered 12px `#6b7280` text: "operating-system kernel threads"; one 2px `#6b7280` arrow from the pthreads band down to it.
- **Annotation (bold 13px magenta `#d55181`, near x=555, y=140):** "four wrappers, one floor".

## More Cooks Is Not Always Faster

**Tags:** `common mistake` (red), `oversubscription` (orange)

- **Stations** — the kitchen has 4 stations (a 4-core machine); only 4 cooks can actually chop at once
- **Sweet spot** — going 1 → 2 → 4 cooks cuts the rush from 60 to 31 to 16 minutes, near-perfect halving
- **Past the cores** — 8 cooks take 18 minutes and 16 cooks take 23: they queue for stations and bump elbows
- **Context switches** — every swap is a cook laying down a knife and picking up someone's half-made order
- **The mistake** — treating thread count as a speed dial; past the core count it mostly buys coordination cost

*Example (italic):* The owner quadruples staff from 4 cooks to 16 hoping to crush the rush, and lunch takes 23 minutes instead of 16 — worse than doing nothing.

**Common mistake:** Setting thread counts far above the core count. On this 4-core kitchen, 16 threads finished slower (23 min) than 4 threads (16 min) — measure, don't assume.

### Visualization (canvas `c4`, 720×300)

Bar chart: minutes to clear the lunch rush versus number of cooks (threads) on a 4-station (4-core) kitchen, with the time falling to a sweet spot at 4 and rising again beyond it.

- **Title (bold 15px, `#1a5276`, top center):** "Rush-Clearing Time vs Cooks on a 4-Station Kitchen".
- **Axes:** origin x=70, baseline y=245, plot width 580, plot height 180; y = minutes 0 to 70 with light `#e5e9ef` gridlines and 12px `#444` labels at 0, 20, 40, 60; x = five bars labeled 13px `#444` "1", "2", "4", "8", "16" with 12px `#6b7280` axis caption "cooks (threads)" centered below.
- **Bars:** centers at x = `[145, 260, 375, 490, 605]`, width 70; minutes = `[60, 31, 16, 18, 23]`; fills — blue `rgba(42,120,214,0.55)` with 2px `#2a78d6` border for 1, 2, and 4 cooks; orange `rgba(217,89,38,0.45)` with 2px `#d95926` border for 8 and 16 cooks; bold 13px value label above each bar ("60", "31", "16", "18", "23"), blue over blue bars, orange over orange.
- **Core marker:** vertical dashed `#6b7280` (dash 4/3) line between the 4- and 8-cook bars (x=432) from y=65 to the baseline; 11px `#6b7280` label at its top: "4 stations (cores)".
- **Annotation (bold 13px orange `#d95926`, near x=520, y=100):** two lines: "past 4 cooks," / "minutes go back up".
- **Caption (11px `#444`, bottom right):** "illustrative — a CPU-bound job on a 4-core machine".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`. Red reserved for the genuine error states (lost updates, the final $4).
- **Data:** all step sequences, jar values, box coordinates, and bar heights are the hardcoded literals above (no randomness); the c2 jar trace `[0,0,1,1,1,2,2,3,3,3,4,4]` must match the bullet-text interleaving exactly, and the c4 minutes `[60,31,16,18,23]` must match the numbers in the text.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
