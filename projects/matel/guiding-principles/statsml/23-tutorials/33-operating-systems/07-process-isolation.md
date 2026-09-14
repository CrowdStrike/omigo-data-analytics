# Process Isolation

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Process Isolation

**Subtitle:** The operating system gives every program its own walled-off memory, so one crashing program cannot take down the others — one crash doesn't take the machine

## The Photo Editor Crashes, the Music Keeps Playing

**Tags:** `core idea` (blue), `private memory` (green), `the OS wall` (orange)

- **The laptop** — at 3pm it runs three programs: a spreadsheet, a music player, and a photo editor
- **The bug** — the photo editor follows a bad pointer and writes to memory it doesn't own
- **The wall** — each program runs as a process with its own private memory; none can touch another's
- **The kill** — the hardware traps the bad write, the OS ends the editor, and only the editor
- **The survivors** — the spreadsheet's unsaved edits and the song's playback position are untouched

*Example (italic):* The editor dies at 3:04pm; the spreadsheet's 45 unsaved edits and the music both carry on as if nothing happened.

**Key point:** Process isolation means each program lives in its own memory sandbox — a crash is contained to the process that caused it, never the whole machine.

### Visualization (canvas `c1`, 720×300)

Three-lane timeline showing each program's alive/dead status from 3:00 to 3:10; the editor's lane cuts off at 3:04 while the other two lanes run flat and unbroken.

- **Title (bold 15px, `#1a5276`, top center):** "3:04pm: One Process Dies, Two Never Notice".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = time "3:00" to "3:10" with 12px `#444` tick labels every 2 minutes; light `#e5e9ef` vertical gridlines at each tick.
- **Lanes (horizontal 4px status lines, left labels 12px `#444` at x=8):** "spreadsheet" at y=95 — blue `#2a78d6` line from minute 0 to 10; "music player" at y=155 — aqua `#199e70` line from minute 0 to 10; "photo editor" at y=215 — orange `#d95926` line from minute 0 to 4 only, ending in a bold 16px red `#e74c3c` "✗" at minute 4.
- **Crash marker:** vertical dashed `#6b7280` (dash 4/3) line at minute 4, 12px `#6b7280` label "bad write → OS kills editor" at its top.
- **Annotation (bold 13px green `#008300`, near minute 7, y=120):** "the other two processes never notice".
- **Caption (12px `#444`, bottom right):** "timeline illustrative".

## Same Address, Two Different Bytes

**Tags:** `worked example` (blue), `page tables` (green)

- **The trick** — every process sees its own private map of addresses; the OS translates them per process
- **The pages** — memory is split into 4096-byte pages; a page table maps each page to a physical frame
- **Hand-check A** — the editor's address 4096 is page 1; its table says page 1 → frame 7 → 7 × 4096 = 28672
- **Hand-check B** — the music player's address 4096 is also page 1, but its table says frame 12 → 49152
- **The trap** — the editor's wild write to 9000 lands on page 2 (9000 − 8192 = offset 808), which has no entry
- **The fault** — no entry means a page fault; the OS kills the editor before the write touches anyone

*Example (italic):* Both programs use address 4096, yet the editor reads physical byte 28672 and the music player reads 49152 — same number, two different bytes.

**Key point:** The same virtual address in two processes points at two different physical locations — so no process can even name, let alone corrupt, another's memory.

### Visualization (canvas `c2`, 720×300)

Mapping diagram: two process boxes on the left, physical memory frames on the right, arrows showing the same virtual address 4096 translating to different frames, plus a red dead-end arrow for the wild write.

- **Title (bold 15px, `#1a5276`, top center):** "Address 4096 in Two Processes → Two Different Physical Frames".
- **Left column (process boxes, 190px wide, 38px tall, 8px radius, 12px `#2c3e50` text):** blue `#2a78d6` border, fill `rgba(42,120,214,0.15)` box "editor — virtual 4096 (page 1)" at (x=30, y=80); aqua `#199e70` border, fill `rgba(25,158,112,0.12)` box "music — virtual 4096 (page 1)" at (x=30, y=160); orange `#d95926` border, fill `rgba(217,89,38,0.12)` box "editor — virtual 9000 (page 2)" at (x=30, y=240).
- **Right column (frame boxes, 200px wide, 38px tall, at x=470):** box "frame 7 → physical 28672" at y=80 (blue border), box "frame 12 → physical 49152" at y=160 (aqua border).
- **Arrows:** 3px blue `#2a78d6` arrow from editor box to frame 7 box, labeled 12px `#444` "editor's page table"; 3px aqua `#199e70` arrow from music box to frame 12 box, labeled "music's page table"; 3px dashed red `#e74c3c` arrow from the virtual-9000 box ending mid-canvas at bold 12px red text "no entry → page fault → killed".
- **Annotation (bold 13px violet `#4a3aa7`, top right area, y=55):** "physical = frame × 4096 + offset".
- **Caption (12px `#444`, bottom right):** "frame numbers illustrative; 4096-byte pages exact".

## One Bad Row Doesn't Kill the Pipeline

**Tags:** `where it's used` (blue), `data pipelines` (green), `crash containment` (orange)

- **The pool** — a data job splits 8,000 rows across 8 worker processes, 1,000 rows each
- **The bad row** — worker 5 hits a corrupt record at row 400 of its chunk and segfaults
- **The containment** — the OS kills worker 5; the other 7 workers finish their 1,000 rows untouched
- **The recovery** — the scheduler re-queues only worker 5's 600 unfinished rows, not all 8,000
- **The notebook** — the same wall is why a crashed notebook kernel never takes down the laptop
- **The browser** — one tab per process is the same idea: a bad page kills one tab, not the browser

*Example (italic):* Of 8,000 rows, 7,400 are done when worker 5 dies — only its remaining 600 rows need a re-run.

**Key point:** Multiprocessing buys crash containment: a data scientist's pipeline loses one worker's unfinished chunk to a bad row, not the whole job.

### Visualization (canvas `c3`, 720×300)

Vertical bar chart of rows completed per worker: seven full bars at 1,000 and one short bar at 400 for the crashed worker, with the re-queued remainder shown as a hatched cap.

- **Title (bold 15px, `#1a5276`, top center):** "8 Workers × 1,000 Rows: One Segfault Costs 600 Rows, Not 8,000".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; y = rows completed 0 to 1000, gridlines `#e5e9ef` at 250/500/750 with 12px `#444` labels; x = workers "w1"–"w8", 12px `#444` labels under each bar.
- **Bars (8 bars, 48px wide, evenly spaced):** rows completed `[1000, 1000, 1000, 1000, 400, 1000, 1000, 1000]`; workers 1–4 and 6–8 fill `rgba(42,120,214,0.35)` with 2px `#2a78d6` border; worker 5 fill `rgba(217,89,38,0.30)` with 2px `#d95926` border and a bold 12px red `#e74c3c` "✗ segfault at row 400" label above it.
- **Re-queue cap:** on worker 5's bar, a dashed 2px `#6b7280` outline box from 400 up to 1000 (no fill), 11px `#6b7280` label "600 re-queued".
- **Value labels:** 11px `#444` row counts at each bar top.
- **Annotation (bold 13px green `#008300`, upper right, y=70):** "7,400 of 8,000 rows survive the crash".
- **Caption (12px `#444`, bottom right):** "row counts illustrative".

## Threads Are Not Processes

**Tags:** `common mistake` (red), `threads vs processes` (orange)

- **The confusion** — threads live inside one process and share its memory; the wall is between processes
- **Shared fate** — one thread's bad write can corrupt every other thread in the same process
- **One bullet** — a segfault in any of 4 threads kills the whole process, all 4 workers at once
- **No catch** — a try/except cannot catch a segfault; the OS kills the process before code sees it
- **The price** — isolation isn't free: processes can't share variables and must copy data between them
- **The choice** — threads for speed and shared state, processes when a crash must stay contained

*Example (italic):* The same bad row that cost 600 rows in the process pool kills all 4 threads of a threaded worker — 4,000 rows (4 threads × 1,000) lost in one shot.

**Common mistake:** Assuming threads give the same crash safety as processes. Threads share one memory sandbox — one thread's segfault, or one silent corrupt write, takes down or poisons all of them.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram comparing the blast radius of one bad write: four threads inside one process box (all die) vs four separate process boxes (one dies, three survive).

- **Title (bold 15px, `#1a5276`, top center):** "One Bad Write: Blast Radius of Threads vs Processes".
- **Row 1 (y=95), label 12px `#444` at x=15:** "4 threads"; one large rounded box (x=130, 300px wide, 60px tall, 8px radius, fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border) containing four 12px `#2c3e50` labels "t1 t2 t3 t4" with t3 marked bold red `#e74c3c` "t3 ✗"; 3px red arrow to a red-bordered box (x=490, fill `rgba(231,76,60,0.12)`) labeled "process dies" with bold 12px red "✗ all 4 gone".
- **Row 2 (y=205), label:** "4 processes"; four small boxes (60px wide, 40px tall, starting x=130, 20px gaps) labeled "p1"–"p4", p1/p2/p4 green `#008300` borders fill `rgba(0,131,0,0.12)`, p3 red `#e74c3c` border fill `rgba(231,76,60,0.12)` with "✗"; 3px green arrow to a green box (x=490) labeled "3 keep running" with bold 12px green "✓".
- **Box text:** 12px `#2c3e50` except the colored ✗/✓ marks.
- **Annotation (bold 13px orange `#d95926`, centered near y=275):** "threads share the sandbox; processes each get their own".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); the crash timeline, frame numbers (7 and 12), and worker row counts `[1000, 1000, 1000, 1000, 400, 1000, 1000, 1000]` are invented and labeled illustrative; the page-table arithmetic is exact (4096-byte pages, 7 × 4096 = 28672, 12 × 4096 = 49152, 9000 − 8192 = offset 808).
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
