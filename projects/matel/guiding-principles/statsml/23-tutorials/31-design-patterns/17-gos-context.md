# Go's Context

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Go's Context

**Subtitle:** Go passes cancellation around as an ordinary value — one "stop" signal handed down the call chain tells every helper below to quit at once

## The Order Ticket That Says "Stop"

**Tags:** `core idea` (blue), `cancellation` (orange), `Go` (green)

- **The order** — a coffee shop order fans out: one barista grinds beans, one steams milk, one warms a cup
- **The walkout** — the customer leaves before pickup; all three helpers are still working for nobody
- **The ticket** — Go's fix: every helper gets a copy of the same order ticket, the `context`
- **The stamp** — cancelling stamps the ticket once; every holder sees the stamp and stops their step
- **The value** — the ticket is just a value passed as the first argument, not a hidden global switch

*Example (italic):* The customer walks out at 9:02am; the ticket is stamped, and the grinder, steamer, and cup-warmer all stop within a second instead of finishing a drink no one will drink.

**Key point:** A context is a value passed down every call; cancelling it flips one shared signal, so the whole tree of work below can notice and stop together.

### Visualization (canvas `c1`, 720×300)

Tree diagram: one handler box at the top passing a `ctx` value down to three worker boxes; a red cancel stamp at the top propagates along all three arrows.

- **Title (bold 15px, `#1a5276`, top center):** "One Cancel at the Top Reaches Every Worker Below".
- **Handler box:** rounded box centered at x=360, y=80, 190px wide, 42px tall, fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border, 13px `#2c3e50` label "order handler (ctx)".
- **Worker boxes (y=210, 160px wide, 42px tall each):** at x=110 "grind beans" (fill `rgba(0,131,0,0.12)`, 2px `#008300`), at x=360 "steam milk" (fill `rgba(25,158,112,0.12)`, 2px `#199e70`), at x=610 "warm cup" (fill `rgba(74,58,167,0.10)`, 2px `#4a3aa7`), all box centers, 13px `#2c3e50` labels.
- **Arrows:** three 3px `#6b7280` arrows from the handler box bottom to each worker box top; 12px `#6b7280` label "ctx" beside each arrow midpoint.
- **Cancel stamp:** bold 13px red `#e74c3c` label "cancel() at 9:02" above the handler box (y=38); a small red 8px-radius dot on each arrow midpoint showing the signal travelling down.
- **Stop labels:** bold 12px red `#e74c3c` "stopped" under each worker box (y=248).
- **Annotation (bold 13px `#2a78d6`, right side near x=560, y=130):** "one stamp, three stops".
- **Caption (12px `#444`, bottom right):** "shop scenario illustrative".

## A Two-Second Deadline for Three Lookups

**Tags:** `worked example` (blue), `timeout` (orange)

- **The setup** — a web request creates a context with a 2000 ms deadline, then starts three lookups
- **Fast ones** — the cache lookup finishes in 120 ms and the database query finishes in 900 ms
- **The slow one** — the recommendation call would take 3500 ms, far past the deadline
- **The cut** — at 2000 ms the context expires; the recommendation call sees it and returns early
- **Hand-check** — 3500 − 2000 = 1500 ms of work is never done; the user waits 2000 ms, not 3500

*Example (italic):* The page renders at the 2000 ms mark with cache and database results filled in and a "recommendations unavailable" slot — not at 3500 ms with everything.

**Key point:** A deadline set once at the top bounds every call underneath it — the slowest branch is cut at 2000 ms without any per-call timer code.

### Visualization (canvas `c2`, 720×300)

Horizontal timeline (Gantt-style) of the three lookups against a shared millisecond axis, with a dashed deadline line at 2000 ms cutting the slow bar.

- **Title (bold 15px, `#1a5276`, top center):** "One 2000 ms Deadline Bounds All Three Lookups".
- **Axes:** origin x=170, baseline y=245, plot width 480, plot height 170; x = time 0 to 4000 ms, 12px `#444` tick labels at `[0, 1000, 2000, 3000, 4000]`, vertical gridlines `#e5e9ef` at each tick.
- **Rows (bar height 26px, left-aligned 12px `#444` labels at x=20; row centers at y = 100, 150, 200):**
  - "cache lookup — 120 ms": green `#008300` bar from 0 to 120 ms, 11px label "120 ms" at bar end
  - "database query — 900 ms": blue `#2a78d6` bar from 0 to 900 ms, 11px label "900 ms" at bar end
  - "recommendations — cut at 2000": aqua `#199e70` bar from 0 to 2000 ms, then a hatched/lighter red `rgba(231,76,60,0.25)` segment from 2000 to 3500 ms with 2px dashed `#e74c3c` border, 11px red label "1500 ms never run"
- **Deadline line:** vertical dashed `#e74c3c` (dash 5/4) line at x = 2000 ms from y=70 to y=245, bold 12px `#e74c3c` label "deadline 2000 ms" at its top.
- **Annotation (bold 13px `#2a78d6`, near x=2600 ms, y=95):** "user waits 2000 ms, not 3500".
- **Caption (12px `#444`, bottom right):** "latencies illustrative".

## Why Servers Leak Without It

**Tags:** `where it's used` (blue), `goroutine leak` (red)

- **The fan-out** — every web request in Go spawns helper goroutines: queries, RPCs, feature fetches
- **The abandon** — users close tabs and clients time out; the request above the helpers is gone
- **The leak** — without a context, each abandoned request leaves 1 helper goroutine stuck forever
- **The math** — at 50 abandoned requests per minute, one hour of traffic strands 3000 goroutines
- **Where you meet it** — model-serving and feature-store APIs fan out per request exactly this way

*Example (italic):* A feature service without contexts climbs to 3000 stuck goroutines after 60 minutes at 50 abandons/min; the same service with contexts stays flat at 0.

**Key point:** Contexts tie the lifetime of every helper to the request that spawned it — when the request dies, its whole subtree of work is reclaimed instead of accumulating.

### Visualization (canvas `c3`, 720×300)

Line chart of stuck goroutines over one hour: without context (red, climbing) vs with context (green, flat at zero).

- **Title (bold 15px, `#1a5276`, top center):** "Stuck Goroutines Over One Hour at 50 Abandons/min".
- **Axes:** origin x=80, baseline y=245, plot width 580, plot height 180; x = minutes 0 to 60, 12px `#444` tick labels at `[0, 15, 30, 45, 60]`; y = stuck goroutines 0 to 3000, horizontal gridlines `#e5e9ef` at 750/1500/2250/3000 with 12px `#444` labels.
- **Without-context line:** red `#e74c3c` 3px line through minutes `[0, 15, 30, 45, 60]`, goroutines `[0, 750, 1500, 2250, 3000]` — straight climb at 50/min.
- **With-context line:** green `#008300` 3px line through the same minutes, goroutines `[0, 0, 0, 0, 0]` — flat on the baseline.
- **Line labels:** bold 12px red "no context" near (40 min, 2200); bold 12px green "with context" near (40 min, y just above baseline).
- **Annotation (bold 13px red `#e74c3c`, near x=20 min, y=85):** "every abandoned request leaks one worker".
- **Caption (12px `#444`, bottom right):** "rates illustrative — leak grows without bound".

## Cancel Is a Request, Not a Kill Switch

**Tags:** `common mistake` (red), `cooperative` (orange)

- **The confusion** — calling `cancel()` does not stop anything by itself; it only flips the signal
- **The check** — a worker must look at `ctx.Done()` inside its loop to actually notice and return
- **The deaf worker** — a loop that never checks runs its full 8 seconds after a cancel at second 1
- **The listening worker** — a loop that checks each pass stops at second 1, right after the cancel
- **The habit** — pass ctx to every blocking call you make; most library calls check it for you

*Example (italic):* Two copies of the same 8-second job get cancelled at second 1 — the one ignoring `ctx.Done()` burns 7 more seconds; the one checking it exits immediately.

**Common mistake:** Believing cancel kills the goroutine. Cancellation is cooperative — the context only carries the news, and code that never checks `ctx.Done()` never hears it.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: a cancel at second 1 hitting a worker that never checks the context (runs to second 8) vs one that checks each loop pass (stops at second 1).

- **Title (bold 15px, `#1a5276`, top center):** "cancel() Only Flips a Flag — the Worker Must Check It".
- **Row 1 (y=105), label 12px `#444` at x=20:** "never checks"; blue `#2a78d6` rounded box at x=185 labeled "cancel() at 1s" (12px), 3px arrow to a red `#e74c3c` box at x=445 labeled "loop ignores ctx.Done()" with bold 12px red "✗ runs all 8s — 7s wasted" beneath.
- **Row 2 (y=215), label:** "checks each pass"; blue box "cancel() at 1s" at x=185, 3px arrow to a green `#008300` box at x=445 labeled "select on ctx.Done()" with bold 12px green "✓ returns at 1s" beneath.
- **Box style:** 160–190px wide, 42px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(231,76,60,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px orange `#d95926`, centered near y=275):** "the context carries the news; the code must listen".
- **Caption (12px `#444`, bottom right):** "8-second job illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); lookup latencies (120 / 900 / 3500 ms) and the 2000 ms deadline, the leak counts (`[0, 750, 1500, 2250, 3000]` at minutes `[0, 15, 30, 45, 60]`, i.e. 50 abandons/min), and the 8-second/1-second cancel timings are invented and labeled illustrative; arithmetic (3500 − 2000 = 1500 ms saved; 50 × 60 = 3000 leaked) is exact.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
