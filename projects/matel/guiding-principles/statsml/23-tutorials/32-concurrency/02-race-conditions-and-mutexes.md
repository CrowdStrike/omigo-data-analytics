# Race Conditions & Mutexes

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Race Conditions & Mutexes

**Subtitle:** When two threads update one shared number at the same time, one update can silently vanish — a mutex makes them take turns

## Two Registers, One Cup Tally

**Tags:** `core idea` (blue), `shared state` (green), `lost update` (orange)

- **The tally** — a coffee shop tracks cups sold today as one shared number, currently 100
- **Two registers** — cashiers A and B each ring up a cup at the same instant
- **Three steps** — "add one" is really read the tally, add 1 in your head, write it back
- **The collision** — both read 100, both compute 101, both write 101 — one sale is gone
- **The race** — the final tally depends on who writes last, not on how many cups were sold

*Example (italic):* Two cups leave the counter at 2:03pm, but the tally goes from 100 to 101 — register B's write lands on top of A's and erases it.

**Key point:** The classic race condition is two threads doing read-modify-write on the same value with overlapping steps; the interleaving decides the result, so the answer changes run to run.

### Visualization (canvas `c1`, 720×300)

Two-lane timeline showing the interleaved read/add/write steps of registers A and B against the shared tally, ending at 101 instead of 102.

- **Title (bold 15px, `#1a5276`, top center):** "Both Read 100, Both Write 101: One Sale Vanishes".
- **Lanes:** two horizontal lanes labeled 12px `#444` at x=20 — "Register A" at y=95, "Register B" at y=185; a shared-tally strip at y=250 with 12px `#6b7280` values "100" (x=120), "101" (x=430), "101" (x=590).
- **Step boxes:** rounded boxes 110px wide, 34px tall, 8px radius, 12px `#2c3e50` text; lane A: blue `#2a78d6` fill `rgba(42,120,214,0.15)` boxes "read 100" (x=140), "add 1 → 101" (x=290), "write 101" (x=440); lane B (shifted right one slot): aqua `#199e70` fill `rgba(25,158,112,0.15)` boxes "read 100" (x=215), "add 1 → 101" (x=365), "write 101" (x=515).
- **Arrows:** 2px `#6b7280` arrows from each "read 100" box down to the tally value "100", and from each "write 101" box down to the tally strip; B's write arrow lands after A's.
- **Annotation (bold 13px red `#e74c3c`, near x=560, y=60):** "expected 102, got 101 — A's sale erased".
- **Caption (12px `#444`, bottom right):** "tally values illustrative".

## Ringing Up 1,000 Cups on Each Register

**Tags:** `worked example` (blue), `mutex` (green)

- **The run** — each register increments the shared tally 1,000 times; the true total is 2,000
- **No lock** — one unlocked run ends at 1,438: the two registers overwrote each other 562 times
- **Every run differs** — eight unlocked runs land anywhere from 1,367 to 1,602, never 2,000
- **The mutex** — a lock only one thread can hold; take it, do read-add-write, release it
- **Locked runs** — with the mutex around all three steps, all eight runs end at exactly 2,000

*Example (italic):* Unlocked run 3 finishes at 1,367 — 633 of 2,000 sales lost — while every mutex-protected run hand-checks to 1,000 + 1,000 = 2,000.

**Key point:** The mutex forces the two registers to take turns on the whole read-modify-write, so no write ever lands on a stale read — the count becomes deterministic.

### Visualization (canvas `c2`, 720×300)

Bar chart of the final tally across eight unlocked runs vs the mutex result, with a dashed line at the expected 2,000.

- **Title (bold 15px, `#1a5276`, top center):** "1,000 + 1,000 Increments: Unlocked Runs Never Reach 2,000".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; y = final tally 0 to 2000, gridlines `#e5e9ef` at 500/1000/1500/2000 with 12px `#444` labels; x = nine bars labeled "run 1"–"run 8" and "mutex" (12px `#444`).
- **Unlocked bars:** eight bars 48px wide, fill `rgba(42,120,214,0.35)`, 2px `#2a78d6` border, heights scaled to values `[1438, 1521, 1367, 1489, 1602, 1414, 1553, 1476]`, 11px `#2c3e50` value labels on top.
- **Mutex bar:** ninth bar solid green `#008300`, value `2000`, bold 11px green value label.
- **Expected line:** dashed `#6b7280` (dash 4/3) horizontal line at y for 2000, 12px `#6b7280` label "expected 2,000" at its left end.
- **Annotation (bold 13px red `#e74c3c`, near run 3, y=80):** "worst run lost 633 updates".
- **Caption (12px `#444`, bottom right):** "run totals illustrative — real losses vary with timing".

## Where a Data Scientist Hits This

**Tags:** `where it's used` (blue), `pipelines` (green), `silent corruption` (orange)

- **Parallel pipelines** — worker processes bumping a shared rows-processed or error counter
- **Feature stores** — two jobs read-modify-write the same aggregate row without a transaction
- **Dashboards** — a metric that is quietly 5% low looks plausible, so nobody investigates
- **It scales badly** — more parallel workers means more overlapping writes and more lost updates
- **The fix family** — mutexes, atomic increments, and database transactions are the same idea

*Example (italic):* A 4-worker ingest job reports 3,880 of 4,000 rows processed — the missing 120 are lost counter updates, not lost rows, but the on-call engineer can't tell.

**Key point:** Races rarely crash anything — they corrupt numbers silently, and a slightly-wrong metric is far more dangerous to an analysis than a loud exception.

### Visualization (canvas `c3`, 720×300)

Line chart of lost updates as parallelism grows: percent of counter increments lost vs number of workers, unlocked vs mutex.

- **Title (bold 15px, `#1a5276`, top center):** "More Workers, More Collisions: Lost Updates Grow with Parallelism".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = workers with 12px `#444` tick labels at `[2, 4, 8, 16]`; y = updates lost 0% to 35%, gridlines `#e5e9ef` at 10/20/30 with 12px `#444` labels.
- **Unlocked line:** orange `#d95926` 3px line with 5px dots through workers `[2, 4, 8, 16]`, lost percent `[4, 11, 19, 28]`, 11px orange value labels above each point.
- **Mutex line:** green `#008300` 3px line through the same worker grid, lost percent `[0, 0, 0, 0]`, flat on the baseline, bold 12px green label "with mutex: 0% lost" above its right end.
- **Annotation (bold 13px violet `#4a3aa7`, near x=8 workers, y=75):** "scaling up makes the corruption worse, not rarer".
- **Caption (12px `#444`, bottom right):** "loss percentages illustrative".

## Locking Only the Write Doesn't Fix It

**Tags:** `common mistake` (red), `lock scope` (orange)

- **The trap** — putting the mutex around the write alone; the stale read already happened
- **Replay** — A reads 100, B reads 100, then each politely takes the lock to write 101: still 101
- **The rule** — the lock must cover the whole read-modify-write, not just the last step
- **Same trap, dressed up** — check-then-act ("if seat free, book it") outside the lock races too
- **Looks fixed** — narrow locks pass small tests because collisions are rare, then fail under load

*Example (italic):* With a write-only lock, the two registers still both read 100 and the tally still ends at 101 — the mutex guarded the wrong span.

**Common mistake:** Treating the mutex as protecting a line of code. It has to protect the invariant — every step from reading the value to writing its replacement stays inside one lock.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram comparing lock scopes: write-only lock (still loses the update) vs lock around the full read-modify-write (correct).

- **Title (bold 15px, `#1a5276`, top center):** "Lock Scope Decides Everything: Guard the Whole Read-Modify-Write".
- **Row 1 (y=95), label 12px `#444` at x=20:** "lock write only"; blue `#2a78d6` rounded box at x=150 labeled "A & B both read 100" (12px), 3px arrow to a yellow `#c98500` box at x=350 labeled "lock · write 101 · unlock", 3px arrow to a red `#e74c3c` box at x=555 labeled "tally 101" with bold 12px red "✗ update lost".
- **Row 2 (y=205), label:** "lock all three steps"; yellow `#c98500` box at x=150 labeled "A: lock · read 100 · write 101 · unlock", arrow to a yellow box at x=390 labeled "B: lock · read 101 · write 102 · unlock", arrow to a green `#008300` box at x=600 labeled "tally 102" with bold 12px green "✓".
- **Box style:** 130–210px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(201,133,0,0.15)` / `rgba(231,76,60,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px orange `#d95926`, centered near y=270):** "a mutex in the wrong place gives the confidence without the safety".
- **Caption (12px `#444`, bottom right):** "tally values illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); the c1/c4 tally values (100 → 101 vs 102), the c2 unlocked run totals `[1438, 1521, 1367, 1489, 1602, 1414, 1553, 1476]` vs mutex `2000`, and the c3 lost-update percents `[4, 11, 19, 28]` at workers `[2, 4, 8, 16]` are invented and labeled illustrative; the 1,000 + 1,000 = 2,000 expected total is exact arithmetic.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
