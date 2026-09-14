# Concurrent Hash Maps

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Concurrent Hash Maps

**Subtitle:** Many threads updating one shared map either corrupt it or queue behind one big lock — concurrent hash maps split the lock per bucket so writers only wait when they truly collide

## Four Clerks, One Warehouse

**Tags:** `core idea` (blue), `shared state` (green), `lock striping` (orange)

- **The warehouse** — 16 numbered aisles hold stock counts; the aisle for an item is picked by hashing its name
- **The clerks** — 4 clerks update counts at the same time: that is 4 threads writing one shared hash map
- **No lock** — two clerks adjusting the same count simultaneously lose one of the updates silently
- **One big padlock** — locking the whole warehouse per update is safe, but clerks queue single-file
- **The insight** — clerks in DIFFERENT aisles never interfere, so why lock the whole building?
- **Striped locks** — one padlock per aisle: safe where it matters, parallel everywhere else

*Example (italic):* Clerk 1 restocks coffee in aisle 12 while clerk 3 counts sugar in aisle 2 — with per-aisle padlocks neither waits; with one big padlock one always does.

**Key point:** A concurrent hash map keeps the safety of locking but shrinks each lock's territory to one bucket — threads only wait when they hash to the same place.

### Visualization (canvas `c1`, 720×300)

Two warehouse diagrams side by side: one big padlock gating all 16 aisles versus a padlock on each aisle.

- **Title (bold 15px, `#1a5276`, top center):** "One Padlock on the Building vs One per Aisle".
- **Left warehouse:** 4×4 grid of aisle cells (each 42×34, 1.5px `#6b7280` stroke, 11px `#6b7280` aisle numbers 1–16) with top-left cell at (60, 90); a bold 22px padlock glyph "🔒" (or drawn: a 20×16 rounded rect + arc shackle in 2.5px `#d95926`) centered above the grid at (150, 70); grid outlined 2.5px `#d95926`; below at (150, 262) bold 12px `#d95926`: "everyone queues here".
- **Right warehouse:** identical 4×4 grid with top-left cell at (420, 90), grid outlined 1.5px `#6b7280`; a small 10×8 padlock drawn in 1.5px `#008300` in the top-right corner of EVERY cell; below at (510, 262) bold 12px `#008300`: "one lock per aisle".
- **Clerks:** 4 labeled dots ("clerk 1"–"clerk 4", 7px filled circles in `#2a78d6`, `#199e70`, `#c98500`, `#4a3aa7` with 11px labels) queued vertically left of the left warehouse at x=25, y = 110/145/180/215; and spread around the right warehouse pointing at cells 12, 2, 7, 15 with short 1.5px arrows in their colors.
- **Annotation (bold 13px `#1a5276`, centered x=360, y=45):** "same warehouse, same clerks — different waiting".
- **Caption (12px `#444`, bottom right):** "aisle = hash bucket, clerk = thread".

## Four Updates in One Time Slot (Usually)

**Tags:** `worked example` (blue), `hand-checkable` (green)

- **The updates** — clerks 1–4 want aisles 12, 2, 7, and 7: two of them hash to the same aisle
- **Big padlock** — one clerk at a time: the 4 updates take 4 time slots, always, collisions or not
- **Striped locks** — clerks 1, 2, 3 work at once in slot 1; clerk 4 waits only for aisle 7 to free
- **The count** — striped finishes in 2 slots (and in 1 whenever all four aisles differ)
- **Scale it** — with 16 aisles and 4 clerks, most rounds have no collision at all; waits are the exception
- **The rule** — waiting time tracks actual collisions, not the total number of clerks

*Example (italic):* Count it by hand: slots (big padlock) = 4, one per clerk; slots (striped) = 2, because only aisle 7 was wanted twice.

**Key point:** With one lock the wait grows with every added thread; with striped locks it grows only with hash collisions — 4 updates, 4 slots versus 2.

### Visualization (canvas `c2`, 720×300)

Gantt-style time-slot chart: the four clerks' updates under the global lock (4 serial slots) versus striped locks (2 slots with one wait).

- **Title (bold 15px, `#1a5276`, top center):** "The Same 4 Updates: 4 Time Slots vs 2".
- **Clerk colors:** clerk 1 `#2a78d6`, clerk 2 `#199e70`, clerk 3 `#c98500`, clerk 4 `#4a3aa7`.
- **Left panel ("one big padlock", bold 13px `#d95926` header at (185, 62)):** slot columns at x = 90+k·58 (4 slots, 11px `#444` labels "slot 1"–"slot 4" at y=250); four bars 50×30 stacked diagonally — clerk 1 in slot 1 (y=85), clerk 2 in slot 2 (y=125), clerk 3 in slot 3 (y=165), clerk 4 in slot 4 (y=205); each bar filled with its clerk color at 0.35 alpha, 2px solid stroke, bold 11px label "c1 → a12", "c2 → a2", "c3 → a7", "c4 → a7" inside.
- **Right panel ("striped locks", bold 13px `#008300` header at (530, 62)):** two slot columns at x=440 and x=560 (labels "slot 1", "slot 2" at y=250); slot 1 holds three bars stacked at y=85, 125, 165 for clerks 1, 2, 3; slot 2 holds one bar at y=85 for clerk 4 ("c4 → a7"); a small 11px `#d95926` note "waited for aisle 7" under clerk 4's bar at y=130.
- **Divider:** 1px `#e5e9ef` vertical line at x=390 from y=55 to y=260.
- **Annotation (bold 13px `#008300`, near x=530, y=225):** "only the true collision waited".
- **Caption (12px `#444`, bottom right):** "slots idealized — equal-length updates".

## Counters, Caches, and Cores That Actually Help

**Tags:** `where it's used` (blue), `throughput` (green), `CAS` (orange)

- **Metrics counters** — request counts bumped by every worker thread land in one shared map
- **In-memory caches** — many readers and writers share cached rows, sessions, and features
- **The classic** — Java's ConcurrentHashMap popularized striping; most languages ship an equivalent
- **Lock-free updates** — hot counters go further: compare-and-swap retries instead of locks at all
- **The payoff** — throughput rises with added cores instead of flatlining behind one lock
- **The rent** — a little memory per stripe and subtler code paths; worth it under real contention

*Example (italic):* A metrics service on 8 cores kept a global lock and got 1× throughput at any core count — striping let it scale to nearly 7×.

**Key point:** Whenever many threads hammer one map — counters, caches, feature stores — striped or lock-free designs turn added cores into added throughput.

### Visualization (canvas `c3`, 720×300)

Throughput-versus-threads line chart: the global-lock line flatlines near 1× while the striped line rises with core count.

- **Title (bold 15px, `#1a5276`, top center):** "Updates per Second as Threads Are Added".
- **Axes:** origin x=80, baseline y=240, plot width 560; x = threads, ticks at x = `[150, 260, 370, 480, 590]` labeled "1", "2", "4", "8", "16" (12px `#444`); y = relative throughput 0–8×, light `#e5e9ef` gridlines at 2, 4, 6, 8 with 12px `#444` left labels "2×", "4×", "6×", "8×".
- **Global-lock line:** orange `#d95926` 3px through `[1.0, 1.1, 1.1, 1.0, 0.9]` at the ticks; 5px dots; bold 12px orange label "one big lock — flat, then sagging" near (330, 233), below the line.
- **Striped line:** green `#008300` 3px through `[1.0, 1.9, 3.6, 6.5, 7.4]`; 5px dots with 12px green value labels "1×", "1.9×", "3.6×", "6.5×", "7.4×" above; bold 12px green label "striped locks" near (500, 70), above the curve.
- **Annotation (bold 13px `#1a5276`, near x=250, y=60):** "the lock, not the CPU, was the ceiling".
- **Caption (12px `#444`, bottom right):** "relative throughput — illustrative benchmark shape".

## Thread-Safe Map, Unsafe Recipe

**Tags:** `common mistake` (red), `check-then-act` (orange)

- **The trap** — "the map is thread-safe, so my code is" — safety covers each CALL, not your recipe
- **The recipe** — read count (40), add 1, write 41: three separate calls with gaps between them
- **The race** — two clerks both read 40, both write 41 — one delivery vanished with no error
- **The fix** — ask the map for ONE atomic operation: increment(key) or compute-if-present
- **The rule** — any check-then-act (read, decide, write) needs a single atomic step, not three safe ones

*Example (italic):* Both clerks read "aisle 7: 40 boxes", both wrote 41 — the map never corrupted, yet the count is wrong by one forever.

**Common mistake:** Treating per-call safety as recipe safety. A thread-safe map guarantees each operation lands intact — it cannot know that your three operations were meant to be one.

### Visualization (canvas `c4`, 720×300)

Interleaving diagram of two threads doing read-modify-write on the same counter and losing an update.

- **Title (bold 15px, `#1a5276`, top center):** "The Lost Update: Two Safe Reads, Two Safe Writes, Wrong Answer".
- **Lanes:** two horizontal thread lanes marked by 12px bold labels "clerk A" (`#2a78d6`) and "clerk B" (`#4a3aa7`) at x=55, y=100 and y=180; a center "aisle 7 count" ribbon at y=140 drawn as a 2px `#6b7280` line from x=110 to x=660 with value tags.
- **Steps (rounded boxes 92×30, 2px stroke in the thread color, bold 11px text):** clerk A "read: 40" at x=130 (lane A), clerk B "read: 40" at x=250 (lane B), clerk A "write: 41" at x=390 (lane A), clerk B "write: 41" at x=520 (lane B); thin arrows from each box to the center ribbon.
- **Ribbon value tags (bold 12px `#2c3e50` on the center line):** "40" at x=110, "41" at x=440, "41" at x=575; the final tag circled 2px `#e74c3c` with bold 12px `#e74c3c` label "should be 42" at (575, 116).
- **Fix strip:** bold 12px `#008300` centered at (360, 250): "fix: one atomic increment(key) — read+add+write in a single step"; beneath at (360, 270) an 11px `#6b7280` line: "check-then-act must be one operation".
- **Caption (12px `#444`, bottom right):** "each call was thread-safe — the recipe was not".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the displayed CSS width × `devicePixelRatio` (sharp-rendering pattern) and scales the context; chart functions are pushed into a `__charts` array, run once, and re-run on window resize debounced 150 ms. Draw padlocks as shapes (rect + arc), not emoji, for cross-platform rendering.
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** the 4-clerk / 16-aisle scenario, the aisle assignments (12, 2, 7, 7), the 4-slots-vs-2 counts, the 40/41/should-be-42 lost update, and the throughput arrays `[1.0, 1.1, 1.1, 1.0, 0.9]` vs `[1.0, 1.9, 3.6, 6.5, 7.4]` are the hardcoded literals above (throughput labeled illustrative); text and charts must agree.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
