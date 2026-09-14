# The ActorSystem

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** The ActorSystem

**Subtitle:** One heavyweight object owns the threads, the scheduler, and the config — so the thousands of actors inside it can stay featherweight

## One Engine Room for the Whole Order App

**Tags:** `core idea` (blue), `one per app` (green), `heavyweight host` (orange)

- **The app** — a coffee-ordering backend handles every online order for a chain of shops
- **The actors** — each live order is one tiny actor: a mailbox, a bit of state, some behavior
- **The host** — all of them live inside a single ActorSystem created when the app boots
- **What it owns** — the actor factory, the dispatcher thread pools, the timer scheduler, the config
- **The split** — the system is deliberately heavy so every actor inside it can be nearly free

*Example (italic):* At the lunch rush the one ActorSystem hosts 40,000 live order actors, all sharing its 16 dispatcher threads.

**Key point:** The ActorSystem is the one heavyweight object in the design — it pays for threads and config once, and rents them out to thousands of featherweight actors.

### Visualization (canvas `c1`, 720×300)

Container diagram: one large ActorSystem box holding the config, scheduler, and dispatcher machinery on the left, and a dense field of tiny actor dots on the right.

- **Title (bold 15px, `#1a5276`, top center):** "One ActorSystem, Thousands of Featherweight Actors".
- **Outer box:** rounded rect x=40, y=55, width 640, height 210, 2px ink `#1a5276` border, fill `rgba(26,82,118,0.05)`, bold 13px ink label "ActorSystem — created once at startup" at its top-left inside edge.
- **Machinery column (left, x=60–250):** three rounded boxes 180px wide, 40px tall, stacked at y = 95, 150, 205; fills `rgba(42,120,214,0.15)` with 12px `#2c3e50` labels: "config (loaded once)", "scheduler (timers)", "dispatcher — 16 threads"; inside the dispatcher box draw 16 small 4px blue `#2a78d6` circles in two rows of 8.
- **Actor field (right, x=300–650):** grid of 8 rows × 15 columns = 120 dots, 5px radius, spacing 24px horizontal / 22px vertical starting at (310, 95); colors cycle through blue `#2a78d6`, aqua `#199e70`, violet `#4a3aa7`; 12px mute `#6b7280` label under the grid: "…40,000 order actors (120 drawn)".
- **Arrow:** 3px green `#008300` arrow from the dispatcher box to the actor field, bold 12px green label "threads are shared, never owned" above it.
- **Annotation (bold 13px violet `#4a3aa7`, near x=330, y=272):** "actors borrow the system's threads — none has its own".
- **Caption (12px `#444`, bottom right):** "actor and thread counts illustrative".

## 30 MB of Actors vs 100 GB of Threads

**Tags:** `worked example` (blue), `memory math` (green)

- **An actor's cost** — a mailbox plus a little state is roughly 300 bytes of heap
- **A thread's cost** — an OS thread reserves about 1 MB just for its call stack
- **Hand-check actors** — 100,000 actors × 300 bytes = 30,000,000 bytes = 30 MB
- **Hand-check threads** — 100,000 threads × 1 MB = 100,000 MB = 100 GB
- **The ratio** — the actor version is about 3,300× smaller; it fits in any laptop's spare RAM
- **The dispatcher** — those 100,000 actors run fine on the system's 16 shared threads

*Example (italic):* The lunch-rush order book at 100,000 actors costs 30 MB — the same headcount as raw threads would demand 100 GB, more than six 16 GB laptops.

**Key point:** One-actor-per-order only works because actors are ~300 bytes; the ActorSystem's shared thread pool is what lets 100,000 of them run on 16 threads.

### Visualization (canvas `c2`, 720×300)

Horizontal bar chart comparing memory for 100,000 concurrent orders: actors vs threads, with a 16 GB laptop as the yardstick between them.

- **Title (bold 15px, `#1a5276`, top center):** "Memory for 100,000 Concurrent Orders: Actors vs Threads".
- **Axis:** horizontal 2px `#999` baseline at x=250, bars extend right, max width 420; log-feel achieved by hardcoded pixel widths, not a real log axis.
- **Rows (top to bottom at y = 85, 150, 215), each with a left-aligned 12px `#444` label at x=20:**
  - "100,000 actors × 300 B = 30 MB": green `#008300` bar width 60, 11px green value label "30 MB" at bar end
  - "one 16 GB laptop (yardstick)": blue `#2a78d6` bar width 240, fill `rgba(42,120,214,0.30)`, 11px blue label "16 GB"
  - "100,000 threads × 1 MB = 100 GB": red `#e74c3c` bar width 420, 11px bold red label "100 GB — 6× a whole laptop"
- **Bar style:** 22px tall, 3px corner radius, solid fills except the yardstick row.
- **Annotation (bold 13px green `#008300`, near x=330, y=110):** "3,300× smaller — actors fit where threads cannot".
- **Caption (12px `#444`, bottom right):** "300 B/actor and 1 MB/thread are typical figures, illustrative; bar widths schematic, not to scale".

## Create It Once, Kill It Once

**Tags:** `where it's used` (blue), `lifecycle` (green), `rule of thumb` (orange)

- **At startup** — the app builds exactly one ActorSystem in `main` and hands it to everything else
- **Shared everywhere** — every module that needs an actor asks this one system to spawn it
- **The owner** — the system's lifecycle owns every actor, timer, and thread created through it
- **At shutdown** — one `terminate()` call stops the actors, cancels timers, and releases threads
- **The payoff** — no orphaned thread pools, no leaked timers, one clean exit point for the JVM

*Example (italic):* At deploy time one `system.terminate()` stops all 40,000 order actors, cancels 500 pending timers, and frees the 16 threads — nothing is left behind.

**Key point:** Because the one system owns everything it created, startup is one constructor call and shutdown is one terminate call — the whole app's concurrency has a single on/off switch.

### Visualization (canvas `c3`, 720×300)

Left-to-right cascade diagram: a single terminate() call flowing through four stages until the process can exit cleanly.

- **Title (bold 15px, `#1a5276`, top center):** "One terminate() Call Shuts Down the Whole System".
- **Trigger box:** rounded rect at x=30, y=125, 120px wide, 50px tall, fill `rgba(74,58,167,0.15)`, 2px violet `#4a3aa7` border, bold 12px violet label "system.terminate()".
- **Cascade boxes (four, left to right at x = 190, 330, 470, 610 — centers), each 120px wide, 50px tall at y=125, 8px radius, fill `rgba(42,120,214,0.15)`, 12px `#2c3e50` two-line labels:** "stop 40,000 actors", "cancel 500 timers", "shut dispatchers (16 threads)", "JVM exits clean".
- **Arrows:** 3px green `#008300` arrows connecting the five boxes in order, drawn at y=150.
- **Sub-labels (11px mute `#6b7280`, under each cascade box at y=200):** "mailboxes drained", "no stray wakeups", "pools released", "no leaks".
- **Annotation (bold 13px green `#008300`, centered near y=245):** "everything the system created, the system destroys".
- **Caption (12px `#444`, bottom right):** "actor and timer counts illustrative".

## The Classic Misuse: a New ActorSystem Per Request

**Tags:** `common mistake` (red), `heavyweight` (orange)

- **The mistake** — code spawns a fresh ActorSystem inside a request handler "to run one actor"
- **What that buys** — every request now pays for new thread pools, a scheduler, and a config load
- **The cost** — each system holds ~20 threads; 500 un-terminated systems is ~10,016 live threads
- **The symptom** — memory and thread counts climb per request until the process falls over
- **The fix** — create one system at startup, share it, and spawn per-request actors inside it

*Example (italic):* After 500 orders the per-request version holds ~10,016 threads and is dying; the shared-system version still sits at its original 16.

**Common mistake:** Treating the ActorSystem like the actors it hosts. Actors are cheap and disposable; the system is the heavyweight — build it once, share it, terminate it once.

### Visualization (canvas `c4`, 720×300)

Line chart of live threads as requests arrive: a per-request-system line climbing toward collapse vs a shared-system line flat at 16.

- **Title (bold 15px, `#1a5276`, top center):** "Live Threads as Orders Arrive: New System Per Request vs One Shared System".
- **Axes:** origin x=70, baseline y=245, plot width 590, plot height 180; x = orders handled 0 to 500 with 12px `#444` tick labels every 100; y = live threads 0 to 10,000, gridlines `#e5e9ef` at 2,500 / 5,000 / 7,500 with 12px `#444` labels "2.5k" / "5k" / "7.5k".
- **Per-request line:** red `#e74c3c` 3px line through orders `[0, 100, 200, 300, 400, 500]`, threads `[16, 2016, 4016, 6016, 8016, 10016]` — ~20 new threads per un-terminated system.
- **Shared line:** green `#008300` 3px line through the same order grid, threads `[16, 16, 16, 16, 16, 16]` — flat along the baseline.
- **Labels:** bold 12px red "new ActorSystem each order" near (x≈300 orders, above the red line); bold 12px green "one shared ActorSystem — 16 threads" near (x≈300 orders, y≈235 just above the green line).
- **Annotation (bold 13px red `#e74c3c`, near x=430 orders, y=70):** "10,016 threads — the process is about to die".
- **Caption (12px `#444`, bottom right):** "20 threads per system, illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); actor counts (40,000 / 100,000), timer count (500), and threads-per-system (20) are invented and labeled illustrative; the memory math is exact given the stated unit costs — 100,000 × 300 B = 30 MB, 100,000 × 1 MB = 100 GB, ratio ≈ 3,300×; the c4 thread series is 16 + 20 × orders.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
