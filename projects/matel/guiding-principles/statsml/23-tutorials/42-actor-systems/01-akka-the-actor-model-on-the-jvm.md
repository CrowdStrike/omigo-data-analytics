# Akka — The Actor Model on the JVM

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Akka — The Actor Model on the JVM

**Subtitle:** An actor is a tiny worker with a private notebook and a mailbox — it handles one message at a time, so nothing it owns ever needs a lock

## One Order at a Time, No Locks Anywhere

**Tags:** `core idea` (blue), `mailbox` (green), `private state` (orange)

- **The shop** — a coffee shop has 3 cashiers all firing orders at one order-processing actor
- **The mailbox** — orders #47, #48 and #49 queue up in the actor's mailbox, in arrival order
- **One at a time** — the actor pulls exactly one order, updates its notebook, then takes the next
- **Private state** — its running count (handled = 46) lives inside the actor; nobody else can touch it
- **No locks** — since only the actor reads or writes its own state, there is nothing to synchronize

*Example (italic):* Cashier B's order #48 waits in the mailbox while #47 is processed; the count goes 46 → 47 → 48 with no race and no `synchronized` anywhere.

**Key point:** An actor = private state + a mailbox + a behavior, processing one message at a time — serial processing per actor is what replaces locks.

### Visualization (canvas `c1`, 720×300)

Flow diagram: three cashier boxes send messages into a mailbox queue, which feeds a single actor box holding private state.

- **Title (bold 15px, `#1a5276`, top center):** "Three Senders, One Mailbox, One Actor: Serial by Design".
- **Cashier boxes:** three rounded boxes at x=30, y = 70 / 135 / 200, each 110×36, fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border, 12px `#2c3e50` labels "Cashier A" / "Cashier B" / "Cashier C"; 2px `#6b7280` arrows from each to the mailbox.
- **Mailbox queue:** three side-by-side boxes at y=135 starting x=220, each 70×36, fill `rgba(201,133,0,0.15)`, 2px `#c98500` border, 12px labels "#49" / "#48" / "#47" (front of queue on the right); 12px `#6b7280` caption "mailbox (FIFO)" beneath.
- **Actor box:** rounded box at x=490, y=110, 190×80, fill `rgba(0,131,0,0.12)`, 2px `#008300` border, bold 13px `#1a5276` line "Order actor" and 12px `#2c3e50` line "state: handled = 46"; single 3px `#008300` arrow from the queue front into the box.
- **Annotation (bold 13px green `#008300`, centered near y=265):** "one message at a time — the state needs no lock".
- **Caption (12px `#444`, bottom right):** "order numbers illustrative".

## A Greeter, a Crash, and a Supervisor

**Tags:** `worked example` (blue), `supervision` (green), `hello world` (orange)

- **Define** — a Greeter actor's behavior: on `Greet(name)`, print "Hello, name" and wait for the next message
- **Spawn** — the actor system creates one Greeter and hands back an address (an `ActorRef`), not the object
- **Tell** — send `Greet("Ada")` then `Greet("Bob")`: two lines printed, messages 1 and 2 done
- **Crash** — message 3 is `Greet("")`; the behavior throws on the empty name and the actor dies mid-run
- **Restart** — the parent supervisor's strategy says restart: a fresh Greeter takes over the same mailbox
- **Resume** — messages 4 and 5 (`"Cleo"`, `"Dev"`) are greeted by the new incarnation; 4 greetings, 1 crash

*Example (italic):* Of 5 messages sent, the Greeter prints 4 greetings; the crash on message 3 costs only that one message — the supervisor restarts it before message 4 arrives.

**Key point:** You never call an actor — you `tell` its address. Failure handling lives in the parent: supervisors restart crashed children, so error recovery is a tree, not a try/catch.

### Visualization (canvas `c2`, 720×300)

Sequence diagram: five message boxes along a Greeter lifeline; message 3 crashes, a supervisor above restarts the actor, messages 4–5 succeed.

- **Title (bold 15px, `#1a5276`, top center):** "Five Messages, One Crash: the Supervisor Restarts the Child".
- **Supervisor box:** rounded box top center at x=300, y=45, 160×34, fill `rgba(74,58,167,0.12)`, 2px `#4a3aa7` border, 12px label "Supervisor (parent)".
- **Lifeline:** horizontal 2px `#999` line at y=210 from x=50 to x=670, 12px `#6b7280` label "Greeter" at x=20.
- **Message boxes (five, 100×36, centered at x = 110, 225, 340, 455, 570, sitting on the lifeline):** messages 1–2 green fill `rgba(0,131,0,0.12)` / 2px `#008300` labeled `Greet("Ada")`, `Greet("Bob")`; message 3 red fill `rgba(231,76,60,0.12)` / 2px `#e74c3c` labeled `Greet("") ✗`; messages 4–5 green, labeled `Greet("Cleo")`, `Greet("Dev")`.
- **Escalation:** dashed 2px `#e74c3c` (dash 4/3) arrow from message 3 up to the supervisor box, 12px red label "crash"; solid 2px `#008300` arrow from the supervisor back down to between messages 3 and 4, 12px green label "restart".
- **Annotation (bold 13px violet `#4a3aa7`, near x=470, y=108):** "messages 4 and 5 greet from the fresh incarnation".
- **Caption (12px `#444`, bottom right):** "message timing illustrative".

## Erlang's Big Idea, Ported to the JVM

**Tags:** `where it's used` (blue), `scale` (green)

- **The lineage** — Akka (2009) brought Erlang's actor-and-supervisor model to Java and Scala
- **Cheap workers** — an actor costs ~300 bytes of overhead; an OS thread costs ~1 MB of stack
- **The count** — one 8 GB box runs ≈4,000 comfortable OS threads but ≈2,500,000 actors
- **No shared state** — actors exchange messages instead of sharing memory, so data races vanish by construction
- **Where it runs** — Akka-style systems power streaming pipelines and IoT backends: one actor per device or session

*Example (italic):* An IoT platform gives each of 2 million thermostats its own actor — one box, one mailbox per device, no lock contention anywhere.

**Key point:** Actors make concurrency a modeling tool: because they are ~3,000× cheaper than threads, you can give every entity in the domain its own serial, lock-free worker.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart: how many concurrent workers fit on one 8 GB machine — OS threads vs actors.

- **Title (bold 15px, `#1a5276`, top center):** "Workers per 8 GB Box: Threads vs Actors".
- **Axis:** vertical 2px `#999` baseline at x=230, bars extend right, max width 440; log-feel achieved by hardcoded pixel widths, not a real log axis.
- **Rows (top to bottom at y = 105, 185), each with a left-aligned 12px `#444` label at x=20:**
  - "OS threads (~1 MB stack each)": blue `#2a78d6` bar width 70, 12px `#2c3e50` end label "≈ 4,000"
  - "Akka actors (~300 bytes each)": green `#008300` bar width 440, bold 12px `#008300` end label "≈ 2,500,000"
- **Bar style:** 26px tall, thread bar fill `rgba(42,120,214,0.30)` with 2px `#2a78d6` edge, actor bar fill `rgba(0,131,0,0.25)` with 2px `#008300` edge.
- **Annotation (bold 13px magenta `#d55181`, centered near y=255):** "625× more workers — each ~3,000× cheaper (300 B vs 1 MB)".
- **Caption (12px `#444`, bottom right):** "counts illustrative; pixel widths schematic, not to log scale".

## Telling Is Not Calling

**Tags:** `common mistake` (red), `async` (orange), `dispatcher` (blue)

- **The confusion** — `greeter.tell(msg)` looks like a method call but returns instantly with no result
- **Fire-and-forget** — `tell` only enqueues the message; the reply, if any, arrives as another message
- **Ask** — when you need an answer, `ask` hands back a future — still no waiting on the caller's thread
- **Shared threads** — a dispatcher runs many actors on a small pool; here 4 threads push 400 msgs/s
- **The mistake** — a blocking database call inside an actor pins one pool thread for its full duration
- **The cliff** — each blocked actor removes ~100 msgs/s; at 4 blocked actors the whole system is frozen

*Example (italic):* Four actors each start a 10-second blocking DB call; the 4-thread dispatcher drops from 400 msgs/s to 0 — every other actor on the pool starves too.

**Common mistake:** Treating a message send like a method call. It is asynchronous fire-and-forget — and blocking inside an actor starves the shared dispatcher; hand blocking work to a separate pool or an async client instead.

### Visualization (canvas `c4`, 720×300)

Step chart of dispatcher throughput over 10 seconds as blocking calls pile up, vs a flat line for the async-handoff version.

- **Title (bold 15px, `#1a5276`, top center):** "Blocking Inside Actors Starves the 4-Thread Dispatcher".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = seconds 0 to 10 with 12px `#444` tick labels every 2s; y = messages/s 0 to 400, gridlines `#e5e9ef` at 100/200/300.
- **Blocking line:** red `#e74c3c` 3px step line through points at seconds `[0, 2, 2, 4, 4, 6, 6, 8, 8, 10]`, throughput `[400, 400, 300, 300, 200, 200, 100, 100, 0, 0]` — one step down per blocked actor.
- **Block markers:** vertical dashed `#6b7280` (dash 4/3) lines at seconds 2, 4, 6, 8, each with an 11px `#6b7280` label "block #1" … "block #4" at its top.
- **Async line:** green `#008300` 3px line, flat at 400 across seconds `[0, 10]`, bold 12px green label "async handoff — all 4 threads stay free" near x=5s above the line.
- **Annotation (bold 13px red `#e74c3c`, near x=8s, y=200):** "4 blocked actors = every actor on the pool frozen".
- **Caption (12px `#444`, bottom right):** "throughput numbers illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); order numbers (#47–#49, handled = 46), the 5-message greeter run (crash on message 3, 4 greetings), worker counts (≈4,000 threads vs ≈2,500,000 actors per 8 GB) and dispatcher throughput steps (400 → 300 → 200 → 100 → 0 msgs/s at seconds 2/4/6/8) are invented and labeled illustrative.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
