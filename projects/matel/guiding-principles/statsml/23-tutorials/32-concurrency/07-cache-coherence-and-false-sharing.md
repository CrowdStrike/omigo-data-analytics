# Cache Coherence & False Sharing

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Cache Coherence & False Sharing

**Subtitle:** CPU cores copy memory in cache lines (typically 64 bytes) and only one core may write a line at a time — so two cores updating different variables on the same line still fight over it, like two baristas sharing one tally card

## Two Baristas, One Laminated Tally Card

**Tags:** `core idea` (blue), `cache lines` (green), `one writer at a time` (orange)

- **The counters** — two baristas keep separate tallies: Ana marks espressos, Ben marks lattes
- **The card** — both tallies live on one laminated card, and only one barista may hold the card to write
- **The grab** — every mark Ana makes means snatching the card back from Ben, and vice versa
- **The cores** — CPUs do the same: memory moves in cache lines (usually 64 B), one writer per line
- **The trap** — Ana and Ben never touch each other's tally, yet they still fight over the card all day

*Example (italic):* Ana and Ben each mark 10 drinks in alternating order; the card changes hands 19 times even though no single mark is ever shared.

**Key point:** Cache coherence is the hardware rule that only one core may hold a cache line for writing at a time; false sharing is when two cores write different variables that happen to sit on the same line, so the line bounces between them anyway.

### Visualization (canvas `c1`, 720×300)

Ownership timeline: two horizontal lanes (Ana / core A, Ben / core B) over 20 alternating marks, showing the tally card ping-ponging between lanes with a hand-off arrow at every mark.

- **Title (bold 15px, `#1a5276`, top center):** "20 Marks, 19 Hand-Offs: the Card Never Rests".
- **Lanes:** lane labels 12px `#444` at x=20 — "Ana (core A)" at y=115, "Ben (core B)" at y=195; light gridline `#e5e9ef` across each lane; x = marks 1 to 20 mapped from x=130 to x=690 (28px per mark), 12px `#444` tick labels at marks 1/5/10/15/20 along a 2px `#999` baseline at y=245.
- **Ownership segments:** for each mark i in `[1..20]`, a filled rounded box 22px wide, 20px tall centered on that mark's x — odd marks (Ana's) blue `#2a78d6` fill `rgba(42,120,214,0.30)` on the Ana lane, even marks (Ben's) orange `#d95926` fill `rgba(217,89,38,0.25)` on the Ben lane.
- **Hand-off arrows:** dashed `#6b7280` (dash 4/3) vertical connectors between consecutive boxes (19 total), alternating down/up.
- **Annotation (bold 13px magenta `#d55181`, near x=430, y=60):** "different tallies, same card — every mark forces a hand-off".
- **Caption (12px `#444`, bottom right):** "mark sequence illustrative".

## Counting a Million Orders: 1 ms vs 40 ms

**Tags:** `worked example` (blue), `padding fix` (green)

- **The setup** — thread A bumps counter `a` one million times; thread B bumps counter `b` one million times
- **The layout** — `a` and `b` are 8-byte numbers at bytes 0–7 and 8–15: the same 64-byte line
- **Fast case** — a write to a line the core already owns costs about 1 ns: 1,000,000 × 1 ns = 1 ms
- **Slow case** — a write to a line the other core just stole costs about 40 ns: 1,000,000 × 40 ns = 40 ms
- **The fix** — pad 56 bytes after `a` so `b` starts its own line: both threads drop back to about 1 ms

*Example (italic):* One line of padding turns a 40 ms loop into a 1 ms loop — a 40× speedup without changing a single line of counting logic.

**Key point:** You can hand-check the whole effect from two costs: an owned-line write is ~1 ns, a stolen-line write is ~40 ns — so a million false-shared increments take 40 ms instead of 1 ms.

### Visualization (canvas `c2`, 720×300)

Vertical bar chart of total time for one million increments per thread under three layouts: one thread alone, two threads on the same line, two threads padded onto separate lines.

- **Title (bold 15px, `#1a5276`, top center):** "One Million Increments: Same Line Is 40× Slower".
- **Axes:** origin x=70, baseline y=245, plot width 590, plot height 180; y = milliseconds 0 to 40, gridlines `#e5e9ef` at 10/20/30 with 12px `#444` labels.
- **Bars (90px wide, centered at x = 180, 380, 580), 12px `#444` category labels below the baseline:**
  - "1 thread, alone": green `#008300` fill `rgba(0,131,0,0.30)`, height for 1 ms (about 5px), bold 12px value label "1 ms" above
  - "2 threads, same line": red `#e74c3c` fill `rgba(231,76,60,0.25)`, full height for 40 ms (180px), bold 12px red value label "40 ms" above
  - "2 threads, padded": blue `#2a78d6` fill `rgba(42,120,214,0.30)`, height for 1 ms, bold 12px value label "1 ms" above
- **Annotation (bold 13px green `#008300`, near x=520, y=90):** "56 bytes of padding buys a 40× speedup".
- **Caption (12px `#444`, bottom right):** "1 ns owned / 40 ns stolen per write — costs illustrative, arithmetic exact".

## More Threads, Slower Job

**Tags:** `where it's used` (blue), `parallel counting` (green), `scaling` (orange)

- **The pattern** — 8 workers tally rows into `counts[0..7]`; eight 8-byte slots fill exactly one 64-byte line
- **The symptom** — adding threads makes the job slower while every core shows 100% busy
- **The numbers** — padded counters reach 7.6 M updates/ms at 8 threads; the shared line drops to 0.3
- **The smell** — "the parallel version is slower than serial" is the classic false-sharing signature
- **The fix** — one counter per cache line, or let each thread total privately and merge at the end

*Example (italic):* The same tally job at 8 threads runs 25× faster padded than false-shared — 7.6 versus 0.3 million updates per millisecond.

**Key point:** Any per-thread accumulator packed into one array — histogram bins, metric counters, gradient partials — is a false-sharing trap, and the giveaway is throughput that falls as cores are added.

### Visualization (canvas `c3`, 720×300)

Line chart of throughput versus thread count: padded counters scale up, counters sharing one line scale down.

- **Title (bold 15px, `#1a5276`, top center):** "Throughput vs Threads: One Layout Scales, the Other Collapses".
- **Axes:** origin x=70, baseline y=245, plot width 580, plot height 180; x = threads with tick labels "1", "2", "4", "8" at even spacing (12px `#444`); y = M updates/ms 0 to 8, gridlines `#e5e9ef` at 2/4/6 with 12px `#444` labels.
- **Padded line:** green `#008300` 3px line with 4px dots through threads `[1, 2, 4, 8]`, throughput `[1.0, 2.0, 3.9, 7.6]`; bold 12px green label "one counter per line" near the last point.
- **Shared line:** red `#e74c3c` 3px line with 4px dots through the same thread grid, throughput `[1.0, 0.55, 0.4, 0.3]`; bold 12px red label "eight counters, one line" below its last point.
- **Divergence marker:** vertical dashed `#6b7280` (dash 4/3) line at threads = 2, 12px `#6b7280` label "second core joins" at its top.
- **Annotation (bold 13px violet `#4a3aa7`, near x-center, y=60):** "same code, same math — 25× apart at 8 threads".
- **Caption (12px `#444`, bottom right):** "throughput numbers illustrative".

## It Is Not a Race Condition

**Tags:** `common mistake` (red), `correct but slow` (orange)

- **The confusion** — a data race is two threads writing the same variable; false sharing is different variables, same line
- **Correct but slow** — false sharing never corrupts a result: the answers are right, only the clock is wrong
- **Locks don't help** — a mutex serializes the code, but the line still ping-pongs and you now also wait for the lock
- **Padding helps** — alignment or padding gives each hot variable its own 64-byte line; the hand-offs vanish
- **Finding it** — hardware counters for cache-line transfers expose it; the source code alone looks innocent

*Example (italic):* A team "fixes" a slow parallel tally by wrapping each counter in its own mutex — the output was already correct, and the job gets slower still.

**Common mistake:** Ruling out sharing problems because the output is correct. False sharing hides precisely because nothing is ever wrong except the run time — the cure is layout (padding), not locking.

### Visualization (canvas `c4`, 720×300)

Two-row memory layout diagram: counters `a` and `b` packed into one 64-byte line (line ping-pongs) versus padded onto two lines (each core keeps its own).

- **Title (bold 15px, `#1a5276`, top center):** "Fix the Layout, Not the Locking".
- **Row 1 (y=95), label 12px `#444` at x=20:** "packed"; one 64-byte line drawn as a 460px-wide, 40px-tall box at x=140 outlined 2px red `#e74c3c`, divided into 8 slots of 8 bytes (thin `#e5e9ef` dividers): slot 1 fill `rgba(42,120,214,0.30)` labeled "a" (12px `#2c3e50`), slot 2 fill `rgba(217,89,38,0.25)` labeled "b", slots 3–8 unfilled; bold 12px red label "one line, two writers — ping-pong" at x=615.
- **Row 2 (y=205), label:** "padded"; two 64-byte line boxes 220px wide, 40px tall at x=140 and x=380, each outlined 2px green `#008300`: first holds "a" (blue slot) plus a gray `rgba(107,114,128,0.15)` region labeled "56 B pad" (12px `#6b7280`); second holds "b" (orange slot); bold 12px green label "one writer per line" at x=615.
- **Box style:** 4px corner radius, slot labels 12px `#2c3e50`, byte ruler "0…63" in 11px `#6b7280` under each line box.
- **Annotation (bold 13px orange `#d95926`, centered near y=270):** "the variables were never shared — only the line was".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); the 1 ns owned-write / 40 ns stolen-write costs and the throughput series (padded `[1.0, 2.0, 3.9, 7.6]`, shared `[1.0, 0.55, 0.4, 0.3]` M updates/ms at threads `[1, 2, 4, 8]`) are invented and labeled illustrative; the 1 ms / 40 ms bar values follow exactly from 1,000,000 × 1 ns and 1,000,000 × 40 ns; the 64-byte line size and 8-byte counter offsets (bytes 0–7 and 8–15) are real hardware layout facts.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
