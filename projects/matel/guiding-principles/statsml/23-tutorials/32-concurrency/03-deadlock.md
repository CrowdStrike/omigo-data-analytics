# Deadlock

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Deadlock

**Subtitle:** Two threads each hold one lock and wait for the other's — like two baristas each gripping one machine, both waiting forever

## Two Baristas, One Steamer, One Grinder

**Tags:** `core idea` (blue), `two locks` (green), `two threads` (orange)

- **The shop** — a coffee shop has exactly one milk steamer and one coffee grinder
- **Ana's latte** — she starts the steamer, and keeps it running while she goes for the grinder
- **Ben's mocha** — he starts the grinder, and keeps it running while he goes for the steamer
- **The freeze** — Ana waits for Ben's grinder; Ben waits for Ana's steamer; neither lets go
- **The mapping** — each barista is a thread, each machine is a lock; two threads, two locks
- **No noise** — nothing crashes and no error prints; the shop just quietly stops making drinks

*Example (italic):* Ana stands holding the steamer, Ben stands holding the grinder, and both are still standing there at closing time.

**Key point:** A deadlock is two or more threads each holding a resource the other needs, so every one of them waits forever — that is the definition, and the baristas just acted it out.

### Visualization (canvas `c1`, 720×300)

Resource-allocation cycle diagram: two barista boxes and two machine boxes arranged in a rectangle, with "held by" and "wants" arrows closing a loop.

- **Title (bold 15px, `#1a5276`, top center):** "Two Holds + Two Wants = One Loop That Never Opens".
- **Boxes (150×44, 8px radius, 12px `#2c3e50` labels):** "Ana (thread 1)" blue `rgba(42,120,214,0.15)` with 2px `#2a78d6` border at (100, 70); "steamer (lock A)" yellow `rgba(201,133,0,0.15)` with 2px `#c98500` border at (470, 70); "Ben (thread 2)" aqua `rgba(25,158,112,0.15)` with 2px `#199e70` border at (470, 190); "grinder (lock B)" yellow style at (100, 190).
- **Solid arrows (3px `#1a5276`, 12px `#1a5276` labels):** steamer → Ana labeled "held by"; grinder → Ben labeled "held by".
- **Dashed arrows (3px `#d95926`, dash 6/4, 12px `#d95926` labels):** Ana → grinder labeled "wants"; Ben → steamer labeled "wants" — the four arrows run clockwise around the rectangle.
- **Annotation (bold 13px magenta `#d55181`, centered near (360, 155)):** "the arrows close a loop — that loop is the deadlock".
- **Caption (12px `#444`, bottom right):** "schematic; machines and baristas illustrative".

## Twelve Seconds to a Total Freeze

**Tags:** `worked example` (blue), `timeline` (green)

- **0s** — Ana switches on the steamer for her latte's milk (thread 1 takes lock A)
- **5s** — Ben switches on the grinder for his mocha's beans (thread 2 takes lock B)
- **10s** — Ana reaches for the grinder; Ben has it, so she waits — still holding the steamer
- **12s** — Ben reaches for the steamer; Ana has it, so he waits — still holding the grinder
- **Hand-check** — Ana moves only if Ben releases, Ben moves only if Ana releases: no one ever does
- **Forever** — from second 12 onward, zero drinks are made no matter how long you watch

*Example (italic):* By second 12 both baristas are waiting; at second 60, second 600, and second 6,000 the picture is exactly the same.

**Key point:** The freeze needs no bad luck beyond ordering — Ana grabbed steamer-then-grinder while Ben grabbed grinder-then-steamer, and that opposite order alone is fatal.

### Visualization (canvas `c2`, 720×300)

Gantt-style timeline with two lanes (Ana, Ben) over 0–60 seconds: short "holding" bars followed by long "waiting" bars that never end.

- **Title (bold 15px, `#1a5276`, top center):** "The Timeline: Two Short Holds, Two Infinite Waits".
- **Axes:** x maps seconds 0–60 to pixels x = 90 + s×10 (plot 90→690); baseline 2px `#999` at y=245; tick labels "0s"–"60s" every 10s, 12px `#444`; lane labels "Ana" and "Ben" 12px `#444` at x=20, lanes at y=110 and y=180, bars 26px tall.
- **Ana lane:** blue `#2a78d6` solid bar seconds 0–10 labeled "holds steamer" (11px white, inside); then orange bar `rgba(217,89,38,0.25)` with 2px `#d95926` border, seconds 10–60, with a thin 7px blue `#2a78d6` strip along its bottom edge (the lock is still held), labeled "waiting for grinder — still holding steamer" (12px `#d95926`, above bar).
- **Ben lane:** aqua `#199e70` solid bar seconds 5–12 labeled "holds grinder"; then orange waiting bar (same style) seconds 12–60 with a thin 7px aqua `#199e70` bottom strip, labeled "waiting for steamer — still holding grinder".
- **Freeze marker:** vertical dashed `#6b7280` (dash 4/3) line at second 12 from y=70 to y=245, 12px `#6b7280` label "frozen from here" at its top.
- **Annotation (bold 13px `#d95926`, near (420, 75)):** "each wait points at the other — neither ever ends".
- **Caption (12px `#444`, bottom right):** "timestamps illustrative".

## Four Conditions — Break Any One

**Tags:** `rule of thumb` (blue), `lock ordering` (green), `where it's used` (orange)

- **Mutual exclusion** — one barista per machine; locks exist precisely to enforce this
- **Hold and wait** — Ana keeps the steamer running while she reaches for the grinder
- **No preemption** — nobody yanks a machine out of a busy barista's hands
- **Circular wait** — Ana waits on Ben, Ben waits on Ana: the arrows form a ring
- **The lever** — all four must hold at once; remove any single one and deadlock is impossible
- **Where you meet it** — two database transactions updating the same two rows in opposite order

*Example (italic):* The shop posts one house rule — "always take the grinder before the steamer" — and the freeze can never happen again.

**Key point:** The cheapest fix breaks circular wait: give every lock a fixed global order and make all threads acquire in that order, so the wants can never form a ring.

### Visualization (canvas `c3`, 720×300)

Four condition cards in a row, with the fourth highlighted, and a green "house rule" fix box beneath it connected by an arrow.

- **Title (bold 15px, `#1a5276`, top center):** "The Four Deadlock Conditions (and the One Everyone Breaks)".
- **Cards (155×90, 8px radius, at x = 40, 215, 390, 565, y=70):** "1. Mutual exclusion / one barista per machine" blue `rgba(42,120,214,0.12)` with 2px `#2a78d6` border; "2. Hold and wait / keep one, reach for next" violet `rgba(74,58,167,0.12)` with 2px `#4a3aa7` border; "3. No preemption / no yanking machines" yellow `rgba(201,133,0,0.12)` with 2px `#c98500` border; "4. Circular wait / each waits on the other" magenta `rgba(213,81,129,0.12)` with 3px `#d55181` border (highlighted). Card titles bold 12px `#1a5276`, coffee lines 11px `#2c3e50`.
- **Fix box:** green `rgba(0,131,0,0.12)` rounded box, 300×46, 2px `#008300` border, centered at (490, 210), bold 12px `#008300` text "house rule: grinder first, then steamer"; 3px `#008300` arrow from card 4 down to the box.
- **Annotation (bold 13px green `#008300`, near (60, 235), left-aligned):** "break just one condition and deadlock becomes impossible".
- **Caption (12px `#444`, bottom right):** "the classic four conditions; coffee wording illustrative".

## Stuck Is Not the Same as Slow

**Tags:** `common mistake` (red), `stuck vs slow` (orange)

- **The confusion** — a deadlocked program looks like a slow one: no crash, no error, just no output
- **The test** — slow work shows progress if you wait; deadlocked work shows none, ever
- **The restart trap** — rebooting frees both locks, then the same two orders freeze it again
- **The worked numbers** — after a 3-minute hang and a restart, one drink is made before it refreezes
- **The honest fix** — restarts and timeouts hide the bug; only lock ordering removes it

*Example (italic):* On a merely slow day the grinder drags but all 6 drinks are done by minute 5; on the deadlock day the counter still reads 0.

**Common mistake:** Treating a deadlock as a performance problem. No amount of waiting, faster hardware, or extra threads finishes deadlocked work — more threads just means more ways to form the ring.

### Visualization (canvas `c4`, 720×300)

Line chart of drinks finished over 5 minutes: a slow-but-working day climbs to 6; the deadlock day stays flat at 0, gets one drink from a restart, then flattens again.

- **Title (bold 15px, `#1a5276`, top center):** "Slow Finishes Late — Deadlocked Never Finishes".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = minutes 0 to 5, tick labels "0m"–"5m" every 1m (12px `#444`); y = drinks finished 0 to 6, gridlines `#e5e9ef` at 2 and 4 with 12px `#444` labels.
- **Slow-day line:** green `#008300` 3px line through minutes `[0, 1, 2, 3, 4, 5]`, drinks `[0, 1, 2, 3, 5, 6]`, 12px green label "slow grinder — all 6 done" near its end.
- **Deadlock-day line:** orange `#d95926` 3px line through minutes `[0, 0.2, 3, 3.5, 5]`, drinks `[0, 0, 0, 1, 1]` — flat at 0, a single step to 1 after the restart, flat again.
- **Restart marker:** vertical dashed `#6b7280` (dash 4/3) line at minute 3, 12px `#6b7280` label "restart" at its top.
- **Annotation (bold 13px magenta `#d55181`, near (300, 80)):** "slow work finishes eventually; deadlocked work never does".
- **Caption (12px `#444`, bottom right):** "drink counts illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); the barista timestamps (0s / 5s / 10s / 12s), the c2 hold/wait bar spans (0–10, 10–60, 5–12, 12–60), and the c4 lines (slow day minutes `[0,1,2,3,4,5]` drinks `[0,1,2,3,5,6]`; deadlock day minutes `[0,0.2,3,3.5,5]` drinks `[0,0,0,1,1]`) are invented and labeled illustrative; the four conditions in c3 (mutual exclusion, hold and wait, no preemption, circular wait) are the standard Coffman conditions.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
