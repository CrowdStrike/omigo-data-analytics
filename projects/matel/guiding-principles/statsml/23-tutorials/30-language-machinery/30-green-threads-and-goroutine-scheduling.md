# Green Threads & Goroutine Scheduling

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Green Threads & Goroutine Scheduling

**Subtitle:** A green thread (goroutine) is a task the program schedules by itself onto a handful of OS threads — so a million of them can be mid-flight while the operating system only ever sees a few

## Forty Tables, Two Waiters

**Tags:** `core idea` (blue), `M:N scheduling` (green), `runtime` (orange)

- **The restaurant** — 40 tables are mid-meal tonight, but only 2 waiters are on the floor
- **Mostly waiting** — a table needs a waiter for brief moments; the rest is cooking and chewing
- **The trick** — a waiter never stands at a waiting table; he parks it and serves whoever is ready
- **Green threads** — goroutines are the tables: the program juggles thousands on a few OS threads
- **Invisible to the OS** — the OS schedules only the 2 waiters; the 40 meals live inside the program

*Example (italic):* At any instant at most 2 tables are actually being served, yet all 40 meals move forward — nobody would say the restaurant "has only 2 meals going".

**Key point:** A goroutine is a task the program's own runtime schedules onto a few OS threads — the OS sees the waiters, never the tables.

### Visualization (canvas `c1`, 720×300)

Three-layer mapping diagram: a top row of 8 goroutine "tables" in three states (being served, waiting on the kitchen, ready and queued), a middle row of 2 OS-thread "waiters", and arrows showing which table each waiter is at right now.

- **Title (bold 15px, `#1a5276`, top center):** "8 Tables, 2 Waiters: the OS Only Sees the Waiters".
- **Top row (goroutines):** eight 64×34 rounded boxes centered at x = 70, 155, 240, 325, 410, 495, 580, 665, all at y=90; bold 12px labels "T1"…"T8" inside. T2 and T5: fill `rgba(42,120,214,0.35)`, 2px `#2a78d6` border (being served). T1, T3, T4, T6: 2px dashed `#6b7280` border, no fill (waiting on kitchen). T7, T8: 2px `#199e70` border, fill `rgba(25,158,112,0.15)` (ready, queued).
- **State legend (11px, y=125, left-aligned from x=60):** blue square + "being served", grey dashed square + "waiting", aqua square + "ready in queue".
- **Middle row (OS threads):** two 150×40 rounded boxes, 2px `#1a5276` border, fill `rgba(26,82,118,0.10)`, centered at (240, 190) and (480, 190); bold 12px `#1a5276` labels "waiter 1 — OS thread" / "waiter 2 — OS thread".
- **Arrows:** 3px `#2a78d6` arrows with arrowheads from T2 (155, 107) down to waiter 1 (240, 170) and from T5 (410, 107) down to waiter 2 (455, 170).
- **OS boundary:** horizontal dashed `#6b7280` (dash 6/4) line at y=232 from x=60 to x=680; 12px `#6b7280` label "what the operating system can see" just below it at y=248, left at x=60.
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=275):** "6 tables are parked for free — no waiter stands still".
- **Caption (11px `#444`, bottom right):** "illustrative snapshot of one moment".

## The Arithmetic: a Million Tables

**Tags:** `worked example` (blue), `stack sizes` (green), `do the math` (orange)

- **Two price tags** — a new goroutine starts with a 2 KB stack; an OS thread reserves about 8 MB
- **A thousand tasks** — 1,000 × 2 KB = 2 MB of goroutine stacks vs 1,000 × 8 MB = 8 GB of thread stacks
- **A million tasks** — 1,000,000 × 2 KB = 2 GB vs 1,000,000 × 8 MB = 8 TB; only one of these fits
- **The ratio** — 8 MB ÷ 2 KB = 4,096: one OS thread costs as much stack as about 4,000 goroutines
- **Cheap handoffs** — parking a goroutine is a function call inside the program, not a trip into the OS

*Example (italic):* On a 16 GB laptop, 1,000,000 goroutines need about 2 GB and leave room to spare; 1,000,000 OS threads would need 8 TB — five hundred such laptops.

**Key point:** 1,000,000 goroutines ≈ 2 GB while 1,000,000 OS threads ≈ 8 TB — the 4,096× stack gap is why "one thread per task" only works for green threads.

### Visualization (canvas `c2`, 720×300)

Log-scale horizontal bar chart: two task counts (1,000 and 1,000,000), each with a goroutine bar and an OS-thread bar, showing the memory gap growing from megabytes to terabytes.

- **Title (bold 15px, `#1a5276`, top center):** "Stack Memory for N Tasks: 2 KB Each vs 8 MB Each".
- **Axis:** horizontal 2px `#999` line at y=250 from x=200 to x=690; log scale in MB, one decade = 70 px; 12px `#444` tick labels below at x=200 "1 MB", x=340 "100 MB", x=480 "10 GB", x=620 "1 TB"; light `#e5e9ef` vertical gridlines at each tick from y=60 to y=250.
- **Group labels (bold 12px `#2c3e50`, left at x=20):** "1,000 tasks" at y=100, "1,000,000 tasks" at y=195.
- **Bars (16px tall, rounded right end, starting at x=200):** length = log10(value in MB) × 70.
  - 1,000 tasks, goroutines: blue `#2a78d6` bar at y=92 ending x=221 (2 MB); bold 12px blue label "2 MB" right of the bar end.
  - 1,000 tasks, OS threads: orange `#d95926` bar at y=116 ending x=474 (8 GB = 8,192 MB); bold 12px orange label "8 GB".
  - 1,000,000 tasks, goroutines: blue bar at y=187 ending x=432 (2 GB = 2,048 MB); bold 12px blue label "2 GB".
  - 1,000,000 tasks, OS threads: orange bar at y=211 ending x=685 (8 TB = 8,388,608 MB); bold 12px orange label "8 TB" placed just above the bar end to avoid the edge.
- **Legend (12px, top right near x=560, y=70):** blue square + "goroutines (2 KB each)", orange square + "OS threads (8 MB each)".
- **Annotation (bold 13px orange `#d95926`, near x=480, y=160):** "×4,096 memory per task, at every scale".
- **Caption (12px `#444`, bottom right):** "log scale; typical default stack sizes, illustrative".

## Why Chat Servers Bet on This

**Tags:** `where it's used` (blue), `servers` (green), `blocking is fine` (orange)

- **Connections are tables** — a chat server keeps one open connection per user, nearly all idle at once
- **Thread per connection** — near 10,000 OS threads (80 GB of stack reservations) the box starts choking
- **Goroutine per connection** — 1,000,000 connections cost about 2 GB of stacks on the same box
- **Blocking reads are free** — a goroutine stuck on a quiet socket is parked; the waiter serves others
- **The same trick everywhere** — Go goroutines, Erlang processes, Java virtual threads, Kotlin coroutines

*Example (italic):* A one-box chat service holds 1,000,000 mostly-idle connections on about 2 GB of goroutine stacks — the thread-per-connection version stalls near 10,000.

**Key point:** Green threads let each connection be written as simple blocking code, and the runtime turns every wait into a free handoff instead of a stalled OS thread.

### Visualization (canvas `c3`, 720×300)

Log-scale vertical bar chart: idle connections one identical box can hold, thread-per-connection vs goroutine-per-connection.

- **Title (bold 15px, `#1a5276`, top center):** "Idle Chat Connections One Box Can Hold".
- **Axes:** baseline 2px `#999` at y=245 from x=140 to x=660; y is log scale from 1,000 to 1,000,000, one decade = 60 px; 12px `#444` tick labels at left x=130 (right-aligned): "1,000" (y=245), "10,000" (y=185), "100,000" (y=125), "1,000,000" (y=65); light `#e5e9ef` horizontal gridlines at each tick.
- **Bar 1 (thread per connection):** orange `#d95926`, fill `rgba(217,89,38,0.35)` with 2px orange border, 120px wide centered at x=280, from baseline up to y=185 (10,000); bold 13px orange value label "10,000" above the bar top; 12px `#444` label "one OS thread per connection" below the baseline at y=268, centered.
- **Bar 2 (goroutine per connection):** blue `#2a78d6`, fill `rgba(42,120,214,0.35)` with 2px blue border, 120px wide centered at x=520, from baseline up to y=65 (1,000,000); bold 13px blue value label "1,000,000" above the bar top; 12px `#444` label "one goroutine per connection" at y=268, centered.
- **Annotation (bold 13px green `#008300`, near x=400, y=105):** "×100 connections on the same hardware".
- **Caption (12px `#444`, bottom right):** "illustrative capacities; log scale".

## A Million Tables Is Not a Million Cooks

**Tags:** `common mistake` (red), `concurrency vs parallelism` (orange)

- **Waiters, not cooks** — goroutines overlap waiting; they do not add CPU cores to the machine
- **The hard cap** — on 4 cores, at most 4 goroutines are executing in any instant, however many exist
- **Do the math** — 8 jobs of 1 s pure computation on 4 cores take 8 s ÷ 4 = 2 s wall time, never 1 s
- **More is not faster** — spawning 1,000,000 goroutines for that CPU work adds only scheduling overhead
- **Where they shine** — tasks that wait on networks, disks, and timers; cores still cap the crunching

*Example (italic):* 8 image-resize jobs of 1 s CPU each on a 4-core box finish in 2 s as 8 goroutines — the same 8 s of work split by 4 cores, not by 8 goroutines.

**Common mistake:** Treating goroutines as extra horsepower. They multiply how much waiting you can overlap, not how much computing you can do — the core count sets that limit.

### Visualization (canvas `c4`, 720×300)

Gantt-style timeline: four core lanes over a 0–2 s time axis, each running two of the eight 1-second CPU jobs back to back, showing the wall time land at 2 s.

- **Title (bold 15px, `#1a5276`, top center):** "8 Jobs × 1 s of CPU on 4 Cores = 2 s, No Matter How Many Goroutines".
- **Axis:** horizontal 2px `#999` line at y=252 from x=140 to x=660 (1 s = 260 px); 12px `#444` tick labels "0 s" (x=140), "1 s" (x=400), "2 s" (x=660) below at y=272; light `#e5e9ef` vertical gridline at x=400 from y=60 to y=252.
- **Lane labels (12px `#444`, left at x=30):** "core 1" (y=95), "core 2" (y=138), "core 3" (y=181), "core 4" (y=224).
- **Segments (30px tall rounded boxes, bold 12px white centered labels):** each lane has one segment from x=140 to x=400 and one from x=400 to x=660.
  - core 1: "G1" blue `#2a78d6`, then "G5" aqua `#199e70`
  - core 2: "G2" orange `#d95926`, then "G6" violet `#4a3aa7`
  - core 3: "G3" green `#008300`, then "G7" magenta `#d55181`
  - core 4: "G4" yellow `#c98500`, then "G8" ink `#1a5276`
- **Midpoint marker:** vertical dashed `#6b7280` (dash 4/3) line at x=400 from y=60 to y=252; 12px `#6b7280` label "1 s: 4 jobs done, 4 to go" just right of it at y=70.
- **Annotation (bold 13px red `#e74c3c`, near x=530, y=52):** "goroutines added zero speed here".
- **Caption (12px `#444`, bottom right):** "illustrative — pure CPU work, no waiting to overlap".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates.
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all positions, bar lengths, and values are the hardcoded literals above (no randomness); log-scale bar ends in c2/c3 come from log10(value) times the stated pixels-per-decade; every invented capacity or size carries an "illustrative" caption.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
