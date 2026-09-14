# Lock-Free Thinking

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Lock-Free Thinking

**Subtitle:** Compare-and-swap updates a shared number in one uninterruptible step — "write the new value only if it still holds the one I read" — so threads coordinate without ever taking a lock

## Two Baristas, One Tally Counter

**Tags:** `core idea` (blue), `atomic operation` (green), `no locks` (orange)

- **The whiteboard** — two baristas share one "cups sold today" tally taped to the espresso machine
- **The race** — both glance at 40, both add their sale, both write 41: one sale silently vanishes
- **A lock** — passing one marker around fixes it, but the second barista just stands there waiting
- **Compare-and-swap** — write 41 only if the tally still reads 40; if it moved, look again and retry
- **One CPU step** — hardware runs the check-and-write as a single instruction nothing can interrupt

*Example (italic):* At the 2pm rush, barista B's write is rejected because the tally already moved to 41 — she re-reads and writes 42.

**Key point:** Lock-free code never blocks a thread behind a lock; every update is conditional — "apply my change only if nothing changed under me" — and simply retries when that check fails.

### Visualization (canvas `c1`, 720×300)

Two-row flow diagram: a blind read-then-write losing a sale (top) vs compare-and-swap catching the collision and retrying (bottom).

- **Title (bold 15px, `#1a5276`, top center):** "Two Sales at Once: Blind Write Loses One, CAS Catches It".
- **Row 1 (y=95), label 12px `#444` at x=20:** "blind write"; blue `#2a78d6` rounded box at x=150 labeled "A reads 40" (12px), blue box at x=320 labeled "B reads 40", 3px arrows into a red `#e74c3c` box at x=490 labeled "both write 41" with bold 12px red "✗ one sale lost" beneath it.
- **Row 2 (y=205), label:** "compare-and-swap"; green `#008300` box at x=150 labeled "A: CAS 40→41 ✓", orange `#d95926` box at x=320 labeled "B: CAS 40→41 rejected", 3px arrow to a green box at x=510 labeled "B retries: 41→42 ✓" with bold 12px green "✓ tally = 42".
- **Box style:** 130–160px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(231,76,60,0.12)` / `rgba(0,131,0,0.12)` / `rgba(217,89,38,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=270):** "the check and the write happen as one uninterruptible step".
- **Caption (12px `#444`, bottom right):** "tally values from the worked example".

## One Collision, One Retry: 40 → 42

**Tags:** `worked example` (blue), `retry loop` (green)

- **Start** — the tally reads 40 when both baristas ring a sale in the same second
- **Step 1** — A runs CAS(expect 40, write 41); the tally is still 40, so it succeeds
- **Step 2** — B runs CAS(expect 40, write 41); the tally is now 41, so it is rejected
- **Step 3** — B re-reads 41 and runs CAS(expect 41, write 42); it succeeds
- **Hand-check** — 2 sales, 3 CAS attempts, 1 retry, final tally 42: nothing lost

*Example (italic):* The whole collision costs B one extra read and one extra attempt — a nanosecond-scale detour, not a queue.

**Key point:** Correctness lives in the expect value — a CAS can only succeed against the exact tally it read, so every lost-update interleaving becomes a visible rejection followed by a retry.

### Visualization (canvas `c2`, 720×300)

Step chart of the tally's true value across the four moments of the collision, with success and rejection markers.

- **Title (bold 15px, `#1a5276`, top center):** "The Tally's True Timeline: 40 → 41 → (rejected) → 42".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = four ticks at steps `[0, 1, 2, 3]` labeled "start", "A ✓", "B ✗", "B retry ✓" (12px `#444`); y = tally 39 to 43, gridlines `#e5e9ef` at 40/41/42.
- **Step line:** ink `#1a5276` 3px stepped line through steps `[0, 1, 2, 3]`, tally values `[40, 41, 41, 42]` — flat segment at step 2 because the rejected CAS changes nothing.
- **Markers:** green `#008300` 6px filled dots at steps 1 and 3 (the successful CAS writes); bold 14px red `#e74c3c` "✗" at step 2, y of value 41, with 12px red label "expect 40, saw 41".
- **Annotation (bold 13px green `#008300`, near step 3, y=90):** "3 attempts, 2 sales, 0 lost — final 42".
- **Caption (12px `#444`, bottom right):** "timing illustrative; tally values exact".

## Where Counters Meet Contention

**Tags:** `where it's used` (blue), `contention` (orange)

- **Metrics counters** — a request counter takes millions of hits per second; a lock would serialize them
- **Databases** — optimistic concurrency is CAS on a row: "update where version still equals 7"
- **ML training** — Hogwild-style SGD lets workers update shared weights lock-free, tolerating rare collisions
- **Queues** — lock-free queues CAS the head pointer so threads hand off work without blocking
- **The scaling win** — under contention a locked counter's throughput sags; the CAS counter degrades far less

*Example (italic):* An illustrative 8-thread benchmark: the locked counter manages 5M increments/s, the CAS counter 15M.

**Key point:** Reach for lock-free when many threads hammer one small value — counters, versions, queue heads — and blocking would turn that hot spot into the whole system's speed limit.

### Visualization (canvas `c3`, 720×300)

Grouped bar chart: increments per second for a lock-guarded counter vs a CAS atomic counter as thread count grows.

- **Title (bold 15px, `#1a5276`, top center):** "One Shared Counter: Lock Sags, CAS Holds Up (illustrative)".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = four groups labeled "1 thread", "2 threads", "4 threads", "8 threads" (12px `#444`); y = M increments/s 0 to 20, gridlines `#e5e9ef` at 5/10/15.
- **Lock bars:** blue `#2a78d6` fill `rgba(42,120,214,0.35)` with 2px blue edge, 34px wide, heights from values `[10, 9, 7, 5]`.
- **CAS bars:** green `#008300` fill `rgba(0,131,0,0.30)` with 2px green edge, 34px wide, beside each lock bar, heights from values `[10, 14, 15, 15]`.
- **Value labels:** 11px `#444` above each bar ("10", "9", "7", "5" and "10", "14", "15", "15").
- **Legend (12px, top right of plot):** blue swatch "lock", green swatch "compare-and-swap".
- **Annotation (bold 13px green `#008300`, near the 8-thread group, y=75):** "at 8 threads: 15M vs 5M — nobody stands in line".
- **Caption (12px `#444`, bottom right):** "throughput numbers illustrative".

## Lock-Free Is Not Wait-Free

**Tags:** `common mistake` (red), `ABA problem` (orange)

- **The myth** — "no locks" does not mean "no waiting": every failed CAS loops back and tries again
- **Retry storms** — with many threads on one value, most attempts fail and the CPU burns cycles retrying
- **The ABA trap** — the tally went 40 → 39 → 40; CAS sees 40 and passes, missing that it ever changed
- **Version stamps** — pairing the value with a counter, (40, v7), makes an ABA round-trip visible
- **When a lock wins** — for long or multi-step updates, a short lock beats a giant retry loop

*Example (italic):* At 64 threads on one counter, an illustrative run needs ~8 failed attempts per success — the "lock-free" counter spends most of its time retrying.

**Common mistake:** Reading "lock-free" as "free of waiting". CAS guarantees the system as a whole makes progress, not that your thread does — any one thread may retry many times, and under heavy contention a plain lock can win.

### Visualization (canvas `c4`, 720×300)

Line chart of failed CAS attempts per successful increment as thread count grows, showing the retry cost of contention.

- **Title (bold 15px, `#1a5276`, top center):** "Failed CAS Attempts per Successful Increment (illustrative)".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = four evenly spaced ticks labeled "1", "4", "16", "64" threads (12px `#444`, axis label "threads on one counter"); y = failed attempts per success 0 to 9, gridlines `#e5e9ef` at 2/4/6/8.
- **Retry line:** orange `#d95926` 3px line with 5px filled dots through the four tick positions, values `[0.0, 0.6, 2.4, 7.9]`, 11px `#444` value labels beside each dot.
- **Comfort band:** faint green fill `rgba(0,131,0,0.08)` across the region y = 0 to 1, 11px green `#008300` label "cheap retries" at its left edge.
- **Annotation (bold 13px red `#e74c3c`, near the 64-thread point, y=80):** "~8 retries per success — contention, not locks, is the enemy".
- **Caption (12px `#444`, bottom right):** "retry counts illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); the worked-example tally values (40 → 41 → rejected → 42, step values `[40, 41, 41, 42]`) are exact and must match the text; the throughput bars (lock `[10, 9, 7, 5]`, CAS `[10, 14, 15, 15]` M increments/s) and the retry curve (`[0.0, 0.6, 2.4, 7.9]` failed attempts per success at 1/4/16/64 threads) are invented and labeled illustrative.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
