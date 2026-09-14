# Memory Leaks & GC Pauses

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Memory Leaks & GC Pauses

**Subtitle:** A garbage collector can only free objects nobody references — a cache that never evicts keeps everything reachable, so the heap ratchets up for days until the process dies at 3am

## The Session Cache That Never Evicts

**Tags:** `core idea` (blue), `slow leak` (red), `garbage collection` (orange)

- **The service** — a web service keeps a static in-memory map: session ID → session object, ~8 KB each
- **The bug** — sessions are added on login but never removed; there is no eviction and no expiry
- **Still reachable** — the GC only frees unreachable objects; every map entry is still referenced
- **The ratchet** — each GC cycle frees the day's short-lived garbage but none of the cache entries
- **The definition** — a leak in a GC language is memory you still reference but will never use again

*Example (italic):* After every GC run the heap drops — but never back to where it started; the post-GC floor creeps up ~0.1 GB every 6 hours while the code "works fine".

**Key point:** Garbage collection is not magic — objects held by a forgotten cache, an unremoved listener, or a growing static map are reachable, so the collector must keep them forever.

### Visualization (canvas `c1`, 720×300)

Sawtooth heap-usage chart over 72 hours: heap climbs with request garbage, each GC drops it back — but to a floor that rises with the leak.

- **Title (bold 15px, `#1a5276`, top center):** "The Sawtooth With a Rising Floor: Heap Over 3 Days".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = hours 0 to 72 with 12px `#444` tick labels every 12h ("0h"–"72h"); y = heap GB 0 to 3.5, gridlines `#e5e9ef` at 1.0/2.0/3.0.
- **Sawtooth line:** blue `#2a78d6` 2px polyline through hour/GB pairs (vertical cliffs use duplicated x): `(0,0.8) (6,1.8) (6,0.9) (12,1.9) (12,1.0) (18,2.0) (18,1.1) (24,2.1) (24,1.2) (30,2.2) (30,1.3) (36,2.3) (36,1.4) (42,2.4) (42,1.5) (48,2.5) (48,1.6) (54,2.6) (54,1.7) (60,2.7) (60,1.8) (66,2.8) (66,1.9) (72,2.9)` — GC every 6h, floor climbs 0.1 GB per cycle.
- **Floor line:** red `#e74c3c` 3px dashed (dash 6/4) line through the post-GC troughs `(0,0.8)` to `(72,2.0)`.
- **Labels:** 12px blue `#2a78d6` "each drop = one GC run" near (10h, 2.3 GB); bold 13px red `#e74c3c` "the floor never comes back down — that's the leak" near (40h, 0.55 GB).
- **Caption (12px `#444`, bottom right):** "heap sizes illustrative".

## Four Hundred Megabytes a Day, Every Day

**Tags:** `worked example` (blue), `hand-check` (green)

- **The arithmetic** — 50,000 logins/day × 8 KB per session entry = 400 MB of un-evictable heap per day
- **The floor** — post-GC floor starts at 0.8 GB (real working set) and gains 0.4 GB every day
- **The ceiling** — the process runs with a 4 GB heap limit
- **Hand-check** — floor after n days = 0.8 + 0.4·n GB; it hits the 4 GB limit at n = 8 days
- **The 3am OOM** — on day 8 an allocation finds no free heap even after a full GC, and the process dies

*Example (italic):* Day 7's floor is 0.8 + 0.4×7 = 3.6 GB — the cache alone holds 350,000 entries × 8 KB = 2.8 GB; one more night of logins crosses 4 GB and the pager fires at 3am.

**Key point:** A slow leak is a countdown you can compute: (heap limit − baseline) ÷ leak rate = days to OOM — here (4 − 0.8) ÷ 0.4 = 8 days, which is why it always dies "about a week after each deploy".

### Visualization (canvas `c2`, 720×300)

Line chart of the post-GC floor by day marching in a straight line toward the 4 GB heap limit, with the OOM crossing marked.

- **Title (bold 15px, `#1a5276`, top center):** "Floor 0.8 GB + 0.4 GB/day: OOM on Day 8".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = days 0 to 8, 12px `#444` tick labels every day; y = heap GB 0 to 4.5, gridlines `#e5e9ef` at 1/2/3/4.
- **Limit line:** red `#e74c3c` 2px dashed (dash 6/4) horizontal line at 4.0 GB, 12px red label "4 GB heap limit" above its left end.
- **Floor line:** blue `#2a78d6` 3px line with 4px dots through days `[0,1,2,3,4,5,6,7,8]`, GB `[0.8, 1.2, 1.6, 2.0, 2.4, 2.8, 3.2, 3.6, 4.0]`.
- **Healthy line:** green `#008300` 2px flat line at 0.8 GB across all 8 days, 12px green label "healthy service: floor stays put" near (day 4.5, 0.95 GB).
- **OOM marker:** red `#e74c3c` filled 6px dot at (8, 4.0) with bold 13px red callout "OOM, 3am day 8" beside it.
- **Caption (12px `#444`, bottom right):** "0.4 GB/day = 50,000 sessions × 8 KB, illustrative".

## As the Heap Fills, the Pauses Grow

**Tags:** `where it's used` (blue), `tail latency` (orange), `stop-the-world` (red)

- **Less headroom** — as the floor rises, each GC cycle has less free space to reclaim, so GC runs more often
- **Stop-the-world** — during a full collection every request thread is frozen mid-flight
- **Growing pauses** — day 1 pauses ~30 ms; by day 7 full collections take ~520 ms and fire every few seconds
- **Tail latency** — a request that lands on a pause waits it out, so p99 latency tracks the pause length
- **The smell** — "the service gets slower every day and a restart fixes it" is the classic leak signature

*Example (italic):* p99 request latency climbs from 80 ms on day 1 to 570 ms on day 7 with zero code or traffic change — the extra ~500 ms is requests parked behind stop-the-world pauses.

**Key point:** A leak hurts long before the OOM: the death spiral is rising floor → more frequent GC → longer stop-the-world pauses → p99 latency ruined for days before the 3am crash.

### Visualization (canvas `c3`, 720×300)

Combo chart by day: bars for worst GC pause, a line for p99 request latency riding just above the bars.

- **Title (bold 15px, `#1a5276`, top center):** "GC Pauses Eat the p99: Days 1–7 of the Leak".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = days 1 to 7, one bar per day, 12px `#444` day labels under bars; y = milliseconds 0 to 600, gridlines `#e5e9ef` at 150/300/450.
- **Pause bars:** 40px wide, fill `rgba(42,120,214,0.35)` with 2px `#2a78d6` outline, heights from GC pause ms `[30, 45, 70, 110, 180, 300, 520]`, 11px `#444` value labels on top of each bar.
- **p99 line:** orange `#d95926` 3px line with 4px dots through the same days, latency ms `[80, 95, 120, 160, 230, 350, 570]`.
- **Legend (12px, top left inside plot):** blue swatch "worst GC pause", orange swatch "p99 request latency".
- **Annotation (bold 13px red `#e74c3c`, near day 6, y=70):** "day 7: a 520 ms freeze, every few seconds".
- **Caption (12px `#444`, bottom right):** "pause and latency numbers illustrative".

## Restarting Nightly Is Not a Fix

**Tags:** `common mistake` (red), `heap dump` (orange)

- **The mistake** — cron a 4am restart, or hand the GC more heap: both just reschedule the OOM
- **Peaks lie** — heap peaks look scary but are normal; the diagnostic signal is the post-GC floor trend
- **The dump** — a heap dump snapshots every live object and who references whom
- **Retained size** — an object's retained size is everything that would become free if it were released
- **The culprit** — sort the dump by retained size: one map holding 2.8 GB of a 3.6 GB floor names the bug

*Example (italic):* The day-7 heap dump shows `SessionCache.map` retaining 2.8 GB — 78% of the 3.6 GB floor — across 350,000 entries; the fix is a 10-line eviction policy, not a bigger heap.

**Common mistake:** Treating the symptom — restarts, larger heaps, GC tuning flags — instead of dumping the heap. A leak is a reference-graph bug, and retained-size analysis points at the exact object holding the memory hostage.

### Visualization (canvas `c4`, 720×300)

Horizontal bar chart of the day-7 heap dump: top objects by retained size, the session cache dwarfing everything else.

- **Title (bold 15px, `#1a5276`, top center):** "Day-7 Heap Dump by Retained Size: One Map Holds 78%".
- **Axis:** vertical 2px `#999` baseline at x=230, bars extend right, max width 440 scaled linearly to 2.8 GB; left-aligned 12px `#444` row labels at x=20.
- **Rows (top to bottom at y = 70, 115, 160, 205, 250):**
  - "SessionCache.map (350k entries)": red `#e74c3c` bar width 440 (2.8 GB), bold 12px red end label "2.8 GB — the leak"
  - "framework buffers": blue `#2a78d6` bar width 47 (0.3 GB), 11px `#444` label "0.3 GB"
  - "connection pool": blue bar width 31 (0.2 GB), label "0.2 GB"
  - "string pool": blue bar width 24 (0.15 GB), label "0.15 GB"
  - "everything else": blue bar width 24 (0.15 GB), label "0.15 GB"
- **Bar style:** 22px tall, blue bars fill `rgba(42,120,214,0.30)` with 2px `#2a78d6` outline, red bar fill `rgba(231,76,60,0.25)` with 2px `#e74c3c` outline.
- **Annotation (bold 13px green `#008300`, bottom center near y=285):** "sums to the 3.6 GB floor — evict sessions and the sawtooth flattens".
- **Caption (12px `#444`, bottom right):** "retained sizes illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); session counts, heap sizes, pause and latency numbers are invented and labeled illustrative, but they are internally consistent: 50,000 sessions/day × 8 KB = 0.4 GB/day, floor = 0.8 + 0.4·n GB, day-7 cache = 350,000 × 8 KB = 2.8 GB of a 3.6 GB floor (78%), OOM at day 8 on a 4 GB limit.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
