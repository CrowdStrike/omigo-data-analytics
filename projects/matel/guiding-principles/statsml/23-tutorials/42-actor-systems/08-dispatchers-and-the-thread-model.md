# Dispatchers & the Thread Model

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Dispatchers & the Thread Model

**Subtitle:** An actor system runs a thousand actors on a handful of real threads — which is why one blocking call inside an actor can freeze all of them at once

## A Thousand Actors, Four Real Threads

**Tags:** `core idea` (blue), `dispatcher` (green), `thread pool` (orange)

- **The shop** — an online order service runs 1,000 actors: one per open order, plus a few helpers
- **The illusion** — each actor behaves like it has its own thread, mailbox in, replies out
- **The reality** — the dispatcher multiplexes all 1,000 actors onto a pool of just 4 OS threads
- **The turn** — a free thread picks an actor with mail, runs a few messages, then puts it back
- **The throughput knob** — `throughput = 5` means an actor handles at most 5 messages per turn

*Example (italic):* At any instant only 4 of the 1,000 order actors are actually running; the other 996 are just mailboxes waiting for a thread to pick them up.

**Key point:** A dispatcher is the scheduler between actors and threads — actors are cheap paper roles, and the 4 pooled threads are the only real workers taking turns playing them.

### Visualization (canvas `c1`, 720×300)

Flow diagram: a tall stack of actor mailboxes on the left funneling through a dispatcher box in the middle onto 4 thread lanes on the right.

- **Title (bold 15px, `#1a5276`, top center):** "One Dispatcher Multiplexes 1,000 Actors onto 4 Threads".
- **Actor stack (left):** 8 rounded boxes 120×22px at x=30, y = 60 to 235 step 25, fill `rgba(42,120,214,0.15)`, 1px `#2a78d6` border, 11px `#2c3e50` labels "order-1", "order-2", "order-3", "…", "order-998", "order-999", "order-1000", "helper"; bold 12px `#2a78d6` label "1,000 actors (mailboxes)" above the stack at y=48.
- **Dispatcher box (middle):** rounded box 130×70px centered at (330, 150), fill `rgba(26,82,118,0.12)`, 2px `#1a5276` border, bold 13px `#1a5276` two-line label "dispatcher\nthroughput = 5".
- **Arrows:** three 2px `#6b7280` arrows from the stack's right edge (y = 80, 150, 220) converging into the dispatcher's left edge; four 3px `#1a5276` arrows fanning from its right edge to the four thread lanes.
- **Thread lanes (right):** 4 rounded boxes 200×34px at x=470, y = 60, 116, 172, 228, fill `rgba(0,131,0,0.12)`, 1px `#008300` border, 12px labels "thread 1 — running order-17", "thread 2 — running order-402", "thread 3 — running order-655", "thread 4 — running helper".
- **Annotation (bold 13px green `#008300`, below lanes near y=282, right-aligned):** "only 4 actors run at any instant".
- **Caption (12px `#444`, bottom left):** "actor and thread counts illustrative".

## Two Seconds of JDBC Freezes the Pool

**Tags:** `worked example` (blue), `blocking call` (red)

- **Normal pace** — one message takes 1 ms, so 4 threads clear about 4,000 messages per second
- **The slow call** — 4 payment actors each run a blocking JDBC query that takes 2 seconds
- **The pin** — a blocked thread cannot be reused; each query pins one thread for its full 2 s
- **The freeze** — with all 4 threads pinned, the other 996 actors process exactly 0 messages
- **Hand-check** — 4,000 msg/s of arrivals × 2 s of freeze = 8,000 messages piled up in mailboxes

*Example (italic):* Four 2-second queries starting at t = 1 s stop a 4,000 msg/s system dead until t = 3 s, leaving 8,000 unread messages behind.

**Key point:** Blocking does not pause one actor — it confiscates a pool thread. Four blocking calls on a 4-thread dispatcher is a full outage for every actor sharing it.

### Visualization (canvas `c2`, 720×300)

Gantt-style timeline of the 4 threads over 4 seconds: dense green work slices, then a solid red 2-second blocked bar on every lane.

- **Title (bold 15px, `#1a5276`, top center):** "Four 2-Second JDBC Calls Pin All Four Threads".
- **Axes:** origin x=110, baseline y=250, plot width 560, plot height 190; x = time 0 s to 4 s, 12px `#444` tick labels every 1 s; 4 horizontal lanes at y = 75, 120, 165, 210 with left 12px `#444` labels "thread 1"–"thread 4" at x=25.
- **Busy slices:** on each lane, green `#008300` bars 16px tall, fill `rgba(0,131,0,0.35)`, drawn at seconds `[0.0, 0.2, 0.4, 0.6, 0.8]` and `[3.0, 3.2, 3.4, 3.6, 3.8]`, each 0.15 s wide (21px) — short 1 ms-scale turns shown schematically.
- **Blocked bars:** on each lane, one solid red `#e74c3c` bar 16px tall from second 1.0 to 3.0 (x pixels 250 to 530), 11px white centered label "blocked on JDBC — 2 s".
- **Freeze band:** vertical dashed `#6b7280` (dash 4/3) lines at seconds 1.0 and 3.0 spanning the plot, bold 13px red `#e74c3c` label between them at y=52: "0 msg/s for everyone".
- **Annotation (bold 12px violet `#4a3aa7`, below baseline at y=272, centered):** "8,000 messages queue while the pool is pinned (4,000 msg/s × 2 s)".
- **Caption (12px `#444`, bottom right):** "slice widths schematic, durations illustrative".

## Why Actors That Never Touch the Database Stall

**Tags:** `why it matters` (blue), `starvation` (orange)

- **Shared fate** — cart updates, status pages, and timers all ride the same 4-thread dispatcher
- **Innocent victims** — an actor that only does arithmetic still starves; it just never gets a thread
- **Hidden blockers** — `Thread.sleep`, JDBC, file reads, and sync HTTP clients all pin threads
- **Deceptive tests** — with 10 test users the queue drains fast; at 4,000 msg/s it never catches up
- **The symptom** — latency graphs show every feature spiking together, pointing at no single actor

*Example (italic):* During the 2-second freeze, a status-check actor that needs 1 ms of CPU waits behind 8,000 queued messages it had nothing to do with.

**Key point:** Starvation is collective punishment — the dispatcher cannot tell a blocked thread from a busy one, so every actor sharing the pool pays for one actor's blocking call.

### Visualization (canvas `c3`, 720×300)

Line chart of system throughput over 6 seconds: flat at 4,000 msg/s, a cliff to 0 during the freeze, then recovery back to the 4,000 msg/s cap.

- **Title (bold 15px, `#1a5276`, top center):** "Throughput During the Freeze: Everyone Drops to Zero Together".
- **Axes:** origin x=70, baseline y=245, plot width 590, plot height 180; x = time 0 s to 6 s, 12px `#444` tick labels every 1 s; y = messages/s 0 to 6,000, gridlines `#e5e9ef` at 2,000 and 4,000 with 12px `#444` labels "2,000" and "4,000".
- **Throughput line:** blue `#2a78d6` 3px line through seconds `[0, 0.5, 1.0, 1.05, 2.0, 3.0, 3.05, 3.5, 4.5, 5.0, 6.0]`, msg/s `[4000, 4000, 4000, 0, 0, 0, 4000, 4000, 4000, 4000, 4000]` — cliff at 1 s, zero until 3 s, back to the 4,000 msg/s cap.
- **Freeze shading:** light red fill `rgba(231,76,60,0.10)` between seconds 1 and 3 over the full plot height, bold 13px red `#e74c3c` label "all 4 threads pinned" centered in it at y=70.
- **Backlog marker:** bold 12px violet `#4a3aa7` label "back at the 4,000 cap — the 8,000-message backlog never drains" near second 4 at y=95 with a short 2px violet arrow down to the recovered line.
- **Annotation (bold 13px orange `#d95926`, near second 5.2, y=180):** "even non-database actors flatlined".
- **Caption (12px `#444`, bottom right):** "rates illustrative".

## The Fix Is a Second Dispatcher, Not a Bigger Pool

**Tags:** `common mistake` (red), `bulkhead` (green)

- **The reflex** — raising the default pool to 40 threads only delays the freeze until 40 calls block
- **The fix** — declare a separate `blocking-io` dispatcher and assign the 4 payment actors to it
- **The split** — default pool: 4 threads, CPU work only; blocking pool: 16 threads sized for JDBC
- **The bulkhead** — payment queries now pin blocking-pool threads; order actors never notice
- **The rule** — anything that can wait on the outside world gets its own dispatcher, always

*Example (italic):* After the split, the same four 2-second queries pin 4 of the 16 blocking threads while the default pool keeps clearing 4,000 msg/s untouched.

**Common mistake:** Fixing starvation by enlarging the shared pool. A bigger pool is a bigger fuse, not a bulkhead — isolation comes from giving blocking work a dispatcher of its own.

### Visualization (canvas `c4`, 720×300)

Two-row before/after diagram: one shared pool with blocking mixed in (frozen) vs a default pool plus a dedicated blocking-io pool (healthy).

- **Title (bold 15px, `#1a5276`, top center):** "Bulkhead: Move Blocking Work onto Its Own Dispatcher".
- **Row 1 (centered y=105), label bold 12px `#444` at x=20:** "before"; one rounded box 300×58px at x=90, fill `rgba(231,76,60,0.12)`, 2px `#e74c3c` border, 12px two-line label "default dispatcher — 4 threads\norders + payments (JDBC) mixed"; 3px red arrow to a red-bordered box 200×58px at x=470 labeled "frozen 2 s — 0 msg/s" with bold 12px red "✗ everyone starves".
- **Row 2 (centered y=225), label:** "after"; two stacked rounded boxes at x=90 — green-bordered 300×34px at y=196, fill `rgba(0,131,0,0.12)`, 12px label "default — 4 threads, CPU only", and orange-bordered 300×34px at y=238, fill `rgba(230,126,34,0.12)`, 12px label "blocking-io — 16 threads, JDBC"; 3px green arrow from each to a green box 200×58px at x=470 labeled "4,000 msg/s sustained" with bold 12px green "✓ orders unaffected".
- **Box style:** 8px radius, 12px `#2c3e50` text, 2px borders in the colors above.
- **Annotation (bold 13px magenta `#d55181`, centered near y=285):** "size the blocking pool for waiting, keep the default pool for thinking".
- **Caption (12px `#444`, bottom right):** "thread counts illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); actor counts, message rates, query durations, and pool sizes are invented and labeled illustrative; the backlog arithmetic (4,000 msg/s × 2 s = 8,000 messages) must stay consistent between text and charts.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
