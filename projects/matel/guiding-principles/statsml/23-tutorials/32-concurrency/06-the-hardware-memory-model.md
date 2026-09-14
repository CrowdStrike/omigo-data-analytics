# The Hardware Memory Model

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** The Hardware Memory Model

**Subtitle:** Each CPU core drafts its writes in a private buffer before other cores can see them — so two threads can watch the same memory and see writes land in different orders

## Two Baristas and a Slow Shared Whiteboard

**Tags:** `core idea` (blue), `visibility` (green), `store buffers` (orange)

- **The shop** — baristas Ana and Ben share one whiteboard listing who has claimed which machine
- **The pads** — each jots a claim on a private sticky pad first; a helper posts it to the board later
- **The lag** — Ana writes "espresso: Ana" on her pad at 9:00:01, but it hits the board at 9:00:04
- **The miss** — Ben checks the board at 9:00:02, sees no claim, and pads "espresso: Ben" himself
- **The clash** — Ana checks at 9:00:03, also sees nothing; both pads now honestly say "mine"
- **The definition** — a hardware memory model is the rulebook for when one core's writes become visible to other cores, and in what order

*Example (italic):* Neither barista lied and neither skipped a step — the shared board was simply behind both private pads for three seconds.

**Key point:** CPU cores work like the baristas: writes land in a private store buffer first, so another core can read memory and miss a write that has already "happened" — the memory model states exactly which such misses are allowed.

### Visualization (canvas `c1`, 720×300)

Two-lane event timeline: Ana's lane on top, Ben's below, showing pad writes, board checks, and the delayed board postings on a shared seconds axis.

- **Title (bold 15px, `#1a5276`, top center):** "9:00 AM: Both Baristas Check During the Posting Lag".
- **Axes:** time axis 2px `#999` at y=245 from x=60 to x=660; seconds 0 to 6 mapped x = 60 + s×100, 12px `#444` tick labels "9:00:00" to "9:00:06" every second; lane guide lines `#e5e9ef` at y=100 (Ana) and y=190 (Ben), lane labels bold 12px `#1a5276` at x=20.
- **Ana events (on y=100):** blue `#2a78d6` 6px dot at s=1 with 12px label "pad: espresso→Ana"; violet `#4a3aa7` 6px dot at s=3 with label "checks board: empty"; orange `#d95926` 6px dot at s=4 with label "note hits board".
- **Ben events (on y=190):** violet dot at s=2 "checks board: empty"; blue dot at s=2.5 "pad: espresso→Ben"; orange dot at s=5 "note hits board".
- **Lag arrows:** dashed `#6b7280` (dash 4/3) horizontal arrows along each lane from pad-write dot to board-post dot (Ana s=1→4, Ben s=2.5→5), 11px `#6b7280` label "posting lag" above each.
- **Annotation (bold 13px magenta `#d55181`, centered near y=145):** "both checks land inside the lag — each sees an empty board".
- **Caption (12px `#444`, bottom right):** "timings illustrative".

## The Four-Line Test That Prints the Impossible

**Tags:** `worked example` (blue), `litmus test` (green)

- **The test** — thread A runs "X=1 then print Y"; thread B runs "Y=1 then print X"; both start at 0
- **The count** — the four steps interleave in exactly 6 legal orders (4! / (2!·2!) = 6); list them by hand
- **The menu** — those 6 orders print only (0,1), (1,0), or (1,1); no order prints (0,0)
- **The run** — 1,000,000 trials on a real laptop: 852,400 × (1,1), 91,300 × (0,1), 56,100 × (1,0)
- **The ghost** — plus 200 × (0,0): each write sat in a store buffer while the other core read a stale 0

*Example (italic):* In 200 of a million runs both threads print 0 — an outcome you can prove impossible if the four steps ran one at a time in any order.

**Key point:** (0,0) is the hardware's fingerprint: each core's read overtook its own buffered write — a store→load reordering the memory model of most desktop chips explicitly permits.

### Visualization (canvas `c2`, 720×300)

Horizontal bar chart of the four printed outcomes over 1,000,000 trials, with the "impossible" (0,0) bar highlighted in red.

- **Title (bold 15px, `#1a5276`, top center):** "1,000,000 Runs of the Store-Buffering Test".
- **Axis:** vertical 2px `#999` baseline at x=230, bars extend right, max width 440; log-feel achieved by hardcoded pixel widths, not a real log axis.
- **Rows (top to bottom at y = 80, 125, 170, 215), each with a left-aligned 12px `#444` label at x=20:**
  - "(1,1) — both saw the other": blue `#2a78d6` bar width 440, 11px end label "852,400"
  - "(0,1) — A finished first": aqua `#199e70` bar width 300, end label "91,300"
  - "(1,0) — B finished first": violet `#4a3aa7` bar width 280, end label "56,100"
  - "(0,0) — 'impossible'": red `#e74c3c` bar width 60, bold 11px red end label "200"
- **Bar style:** 16px tall, top three bars filled at 0.75 alpha of their color, (0,0) bar solid red.
- **Annotation (bold 13px red `#e74c3c`, right side near y=245):** "200 runs printed (0,0) — no interleaving of the 6 allows it".
- **Caption (12px `#444`, bottom right):** "counts illustrative; the 6-order combinatorics exact; pixel widths schematic (log feel)".

## Where a Stale Read Bites Real Code

**Tags:** `where it's used` (blue), `lock-free` (green), `portability` (orange)

- **The flag trick** — a worker fills a results array, then sets done=true; a reader polls done, then reads
- **The trap** — on a weakly ordered chip, the write to done can become visible before the array writes
- **The spectrum** — sequential consistency reorders 0 of 4 pair types; x86-style 1 of 4; ARM/POWER-style all 4
- **Where you meet it** — lock-free queues, shared counters, a C extension filling a buffer another thread reads
- **The tools** — locks, atomics, and memory fences insert the ordering the hardware will not give for free

*Example (italic):* A worker sets done=true after writing its results; the reader sees the flag yet averages half-written values, and the bug only appears on the ARM server.

**Key point:** Whenever two threads share data without a lock, the memory model — not your source-code order — decides what the reader sees; portable code must state the ordering it needs.

### Visualization (canvas `c3`, 720×300)

Permission matrix: three memory models (rows) versus the four reordering pair types (columns), marking which reorderings each model may perform.

- **Title (bold 15px, `#1a5276`, top center):** "What Each Memory Model May Reorder".
- **Layout:** column headers 12px `#444` at y=70 over x = 230, 340, 450, 560: "store→load", "store→store", "load→load", "load→store"; row labels 12px `#444` at x=20 for rows at y = 110, 165, 220: "sequential consistency (ideal)", "x86-style (TSO)", "ARM/POWER-style (weak)"; row guide lines `#e5e9ef`.
- **Cells:** at each (row, column) center draw either a green `#008300` 12px check "✓" meaning "kept in order" or a solid orange `#d95926` 9px-radius disc meaning "may reorder".
  - Row 1 (sequential consistency): check, check, check, check
  - Row 2 (x86-style TSO): disc, check, check, check
  - Row 3 (ARM/POWER-style weak): disc, disc, disc, disc
- **Tally column:** bold 12px `#1a5276` at x=650 per row: "0 / 4", "1 / 4", "4 / 4".
- **Annotation (bold 13px orange `#d95926`, centered near y=265):** "your x86 laptop reorders 1 pair type; a weak-model chip may reorder all 4".
- **Caption (12px `#444`, bottom right):** "simplified summary of published architecture rules".

## A Plain Flag Is Not a Fence

**Tags:** `common mistake` (red), `fences` (orange)

- **The mistake** — "it passed 10,000 test runs" — a reordering that strikes 200 times per million hides easily
- **The plain flag** — an ordinary boolean write is just another store; nothing ties it to the data before it
- **The fix** — write the flag with a release store and read it with an acquire load, or take a lock
- **The pairing** — release publishes everything written before it; acquire guarantees the reader sees it all
- **The portability trap** — code clean for months on an x86 laptop can corrupt data in its first hour on ARM

*Example (italic):* A flag-guarded queue runs a month on the developer's x86 laptop, then serves garbage within an hour of deploying to an ARM server that reorders more.

**Common mistake:** Treating a passing test as an ordering proof. Absence of a crash is a statistics problem — 200 in 1,000,000 — not a guarantee; only fences, atomics, or locks make the ordering part of the contract.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: publishing data behind a plain flag (reader gets stale data) vs behind a release/acquire pair (reader guaranteed fresh data).

- **Title (bold 15px, `#1a5276`, top center):** "Publishing Data: Plain Flag vs Release/Acquire".
- **Row 1 (y=95), label 12px `#444` at x=20:** "plain flag"; blue `#2a78d6` rounded box at x=180 labeled "data written (still in buffer)" (12px), 3px arrow to a red `#e74c3c` box at x=430 labeled "reader: done=true, data stale" with bold 12px red "✗ reads garbage".
- **Row 2 (y=205), label:** "release/acquire"; blue box "data written", 3px arrow to a green `#008300` box at x=360 labeled "release store: done=true", then arrow to a green box at x=560 labeled "acquire load sees all data" with bold 12px green "✓".
- **Box style:** 150–170px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(231,76,60,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=270):** "the flag flip is free; the ordering guarantee is what you must ask for".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded values above (no randomness); barista timings and the trial counts (852,400 / 91,300 / 56,100 / 200, summing to 1,000,000) are invented and labeled illustrative; the 6-interleaving count and its (0,1)/(1,0)/(1,1)-only outcome set are exact combinatorics; the reorder matrix (0/4, 1/4, 4/4) is a simplified summary of published sequential-consistency, x86-TSO, and weak-model rules.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
