# Dining Philosophers

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Dining Philosophers

**Subtitle:** Five identical diners each grab their left fork at the same moment and all starve — deadlock caused not by a bug, but by perfect symmetry

## Five Philosophers, Five Forks, One Frozen Table

**Tags:** `core idea` (blue), `deadlock` (red), `shared resources` (orange)

- **The table** — five philosophers sit in a circle with exactly five forks, one between each pair
- **The rule** — a philosopher needs BOTH neighboring forks to eat; one fork is never enough
- **The habit** — each alternates forever between thinking and eating, grabbing forks when hungry
- **The identical plan** — every philosopher follows the same rule: pick up the left fork, then the right
- **The freeze** — if all five get hungry at once, each holds a left fork and waits on the right, forever
- **The twist** — nobody made an error; five correct programs combine into one stuck system

*Example (italic):* At noon all five reach left in the same instant — five forks are held, zero are free, and every philosopher waits on a neighbor who is waiting too.

**Key point:** Deadlock here needs no faulty code — five threads running the same correct steps against a ring of shared resources lock each other permanently.

### Visualization (canvas `c1`, 720×300)

Circular table diagram of the deadlocked state: five philosopher circles around a table, each holding one fork (solid green tick) and waiting on the next fork around the ring (dashed orange arrow), forming a closed wait cycle.

- **Title (bold 15px, `#1a5276`, top center):** "The Deadlock Ring: Everyone Holds One Fork, Waits for Another".
- **Layout:** table center at (360, 170); five philosopher circles (radius 22, fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border) at angles `[-90, -18, 54, 126, 198]` degrees on a radius-105 ring, labeled bold 12px `#1a5276` "P1".."P5" (P1 at top, clockwise); five fork marks (14px-long 3px `#6b7280` line segments) at the midpoint angles `[-54, 18, 90, 162, 234]` degrees on a radius-105 ring, labeled 11px `#6b7280` "f1".."f5" so each philosopher's left fork carries their own number (f1 between P5 and P1, f2 between P1 and P2, ...).
- **Held edges:** solid 3px green `#008300` arrow from each philosopher to the fork counter-clockwise of them (P1→f1, P2→f2, P3→f3, P4→f4, P5→f5), tiny 11px green "holds" label on the P1→f1 edge only.
- **Wait edges:** dashed (dash 5/4) 2px orange `#d95926` arrow from each philosopher to the fork clockwise of them (P1→f2, P2→f3, P3→f4, P4→f5, P5→f1), tiny 11px orange "waits" label on the P1→f2 edge only.
- **Center label (12px `#2c3e50`, at table center):** "5 held / 0 free".
- **Annotation (bold 13px red `#e74c3c`, right side near x=560, y=70):** "the wait arrows form a closed loop — no one can move first".
- **Caption (12px `#444`, bottom right):** "positions schematic; the cycle structure is exact".

## The Trace: Five Grabs, Zero Meals

**Tags:** `worked example` (blue), `hand-checkable` (green)

- **The setup** — 5 philosophers, 5 forks, each meal needs 2 forks, so at most 2 can eat at once (5 ÷ 2)
- **The symmetric run** — at minute 0 all five grab left: 5 forks held, 0 free, every right-hand grab blocks
- **The check** — 5 philosophers × 1 fork each = 5 held; each needs 1 more; 0 remain — stuck by arithmetic
- **The fixed run** — number the forks 1–5 and always grab the lower number first; P5 now reaches right first
- **The payoff** — with ordering, 2 philosophers eat at a time; each meal takes 2 minutes, so 2 meals finish every 2 minutes
- **The tally** — by minute 6 the ordered table has served 6 meals; the symmetric table has served 0

*Example (italic):* Ordered table at minute 2: two meals done; minute 4: four; minute 6: six — the symmetric table sits at zero the whole time.

**Key point:** One asymmetry — a global fork numbering — makes a closed wait loop arithmetically impossible, because the highest-numbered fork is only ever grabbed second.

### Visualization (canvas `c2`, 720×300)

Line chart of cumulative meals served over the first six minutes: the ordered table climbs steadily, the symmetric (deadlocked) table flatlines at zero.

- **Title (bold 15px, `#1a5276`, top center):** "Meals Served: Ordered Forks vs the Symmetric Grab".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = minutes 0 to 6 with 12px `#444` tick labels every minute; y = cumulative meals 0 to 6, gridlines `#e5e9ef` at 2 and 4.
- **Ordered line:** green `#008300` 3px line through minutes `[0, 2, 4, 6]`, meals `[0, 2, 4, 6]`, with 4px green dots at each point and 11px green value labels "2", "4", "6" above the last three points.
- **Deadlocked line:** red `#e74c3c` 3px line through minutes `[0, 2, 4, 6]`, meals `[0, 0, 0, 0]`, 12px red label "symmetric: deadlocked at minute 0" just above the line near x=3.5 min.
- **Legend (12px, top left inside plot):** green swatch "lower-numbered fork first", red swatch "everyone grabs left first".
- **Annotation (bold 13px green `#008300`, near x=4.5 min, y=95):** "2 eaters at a time — 2 meals every 2 minutes".
- **Caption (12px `#444`, bottom right):** "2-minute meals illustrative; the 0-meal deadlock line is exact".

## Two Jobs, Two Table Locks, Same Trap

**Tags:** `where it's used` (blue), `databases` (green), `pipelines` (orange)

- **The disguise** — swap forks for locks and philosophers for threads; the ring is everywhere in software
- **The ETL pair** — a nightly job locks `orders` then `customers`; a backfill locks `customers` then `orders`
- **The collision** — each job takes its first lock, then blocks on the other's — a two-philosopher table
- **The classic fixes** — global lock ordering, a single arbiter (waiter) granting seats, or admitting at most 4 to a 5-seat table
- **The detector** — databases build the same wait-for graph as the diagram and kill one job when a cycle appears
- **The symptom** — deadlocks hide in testing because symmetry needs an unlucky same-instant start to bite

*Example (italic):* The 2am backfill and the 2am nightly load each hold one table lock and wait 40 minutes on the other, until the database picks a victim and rolls it back.

**Key point:** Every fix breaks the symmetry somewhere — in ordering, in an arbiter, or in the count of contenders — because symmetric contenders on a ring of resources can always re-create the cycle.

### Visualization (canvas `c3`, 720×300)

Two-row flow diagram: two jobs acquiring the same two table locks in opposite order (deadlock) vs both following one global order (clean handoff).

- **Title (bold 15px, `#1a5276`, top center):** "Opposite Lock Order Is a Two-Philosopher Table".
- **Row 1 (y=95), label 12px `#444` at x=20:** "opposite order"; blue `#2a78d6` rounded box at x=150 labeled "Job A: lock orders" (12px), dashed orange `#d95926` 2px arrow to a red `#e74c3c` box at x=390 labeled "wants customers — blocked"; beneath the arrow a 11px `#6b7280` note "Job B holds it, wants orders"; bold 12px red "✗ cycle: A waits on B waits on A" at x=390, y=60.
- **Row 2 (y=205), label:** "one global order"; blue box at x=150 "Job A: lock orders, then customers", solid 3px green `#008300` arrow to a green box at x=420 labeled "Job B runs after — no cycle" with bold 12px green "✓".
- **Box style:** 160–200px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(231,76,60,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=270):** "same locks, same jobs — only the acquisition order changed".
- **Caption (12px `#444`, bottom right):** "schematic; job and table names generic".

## Timeouts Don't Break the Symmetry

**Tags:** `common mistake` (red), `livelock` (orange)

- **The tempting fix** — "if the right fork isn't free in 2 seconds, put the left one down and retry"
- **The catch** — five identical philosophers time out together, drop together, and grab together again
- **The result** — forks oscillate between all-held and all-free while meals served stays at zero
- **The name** — this is livelock: everyone is busily moving, yet the system makes no progress
- **The tell** — random retry delays help in practice but only hide the symmetry; ordering removes it
- **The lesson** — a fix must make the contenders different, not just make them retry harder

*Example (italic):* Every 2 seconds all five philosophers drop their left forks in unison and re-grab in unison — an hour of furious activity, zero meals.

**Common mistake:** Treating deadlock as a stuck state you can escape by backing off and retrying. Identical back-off keeps the contenders identical — the loop just becomes a spinning loop.

### Visualization (canvas `c4`, 720×300)

Step chart of the livelock: forks held oscillates 5 → 0 → 5 on a 2-second timeout cycle while cumulative meals stays flat at zero.

- **Title (bold 15px, `#1a5276`, top center):** "Livelock: Everyone Retries Together, Nobody Eats".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = seconds 0 to 12 with 12px `#444` tick labels every 2 seconds; y = count 0 to 5, gridlines `#e5e9ef` at 1 through 4.
- **Forks-held step line:** orange `#d95926` 3px square-step line through seconds `[0, 2, 2, 4, 4, 6, 6, 8, 8, 10, 10, 12]`, forks held `[5, 5, 0, 0, 5, 5, 0, 0, 5, 5, 0, 0]` — a square wave: all grab at even seconds 0/4/8, all time out and drop 2 seconds later.
- **Meals line:** red `#e74c3c` 3px flat line through seconds `[0, 12]`, meals `[0, 0]`, 12px red label "meals served: still 0" just above it near x=8s.
- **Legend (12px, top right inside plot):** orange swatch "forks held", red swatch "meals served".
- **Annotation (bold 13px orange `#d95926`, near x=5s, y=80):** "synchronized retries re-create the deadlock every cycle".
- **Caption (12px `#444`, bottom right):** "2-second timeout illustrative; the zero-meal outcome is the point".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); the structural facts are exact (5 philosophers, 5 forks, 2 forks per meal, at most ⌊5/2⌋ = 2 concurrent eaters, 0 meals under deadlock and livelock); the 2-minute meal time, cumulative meals `[0, 2, 4, 6]` at minutes `[0, 2, 4, 6]`, and the 2-second timeout square wave `[5, 5, 0, 0, 5, 5, 0, 0, 5, 5, 0, 0]` are invented and labeled illustrative.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
