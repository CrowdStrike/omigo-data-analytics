# Redis

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Redis

**Subtitle:** Redis keeps whole data structures — lists, sets, sorted sets — in memory, so asking "who's in the top 3?" is one command, not a query plan

## The Leaderboard That Sorts Itself

**Tags:** `core idea` (blue), `in-memory` (green), `data structures` (orange)

- **The game** — a mobile game shows a live leaderboard; every match finish updates a player's score
- **The naive way** — a SQL table of scores needs `ORDER BY score DESC` on every leaderboard view
- **The Redis way** — a sorted set keeps members ordered by score at all times; reads never sort
- **Not just strings** — Redis values are structures: strings, lists, sets, sorted sets, hashes, streams
- **In memory** — everything lives in RAM, so a read or write takes microseconds, not milliseconds

*Example (italic):* `ZADD leaderboard 5800 "mei"` files mei into her sorted position on the way in — the top-3 query afterward is just "read the first 3".

**Key point:** Redis is a data-structure server: instead of rows you store live structures, and the structure itself (a sorted set) does the work a SQL query would redo on every read.

### Visualization (canvas `c1`, 720×300)

Horizontal bar chart of the leaderboard sorted set: five players drawn in score order with rank labels, showing the structure is already sorted at rest.

- **Title (bold 15px, `#1a5276`, top center):** "A Sorted Set Is Already in Order — No Query Ever Sorts It".
- **Axis:** vertical 2px `#999` baseline at x=170, bars extend right, max width 460 for the top score 5800; x scale linear (pixels = score / 5800 × 460).
- **Rows (top to bottom at y = 70, 110, 150, 190, 230), each with a right-aligned 12px `#444` label at x=160:** "#1 mei — 5800", "#2 sam — 4900", "#3 ada — 4200", "#4 raj — 3100", "#5 leo — 2500".
- **Bars:** 22px tall, fill `rgba(42,120,214,0.30)` with 2px `#2a78d6` edge; top-3 bars get a solid green `#008300` 4px left cap; 11px `#444` score labels at bar ends.
- **Annotation (bold 13px green `#008300`, right side near y=70):** "top 3 = read the first 3 members".
- **Caption (12px `#444`, bottom right):** "scores illustrative".

## Bumping a Score by Hand

**Tags:** `worked example` (blue), `ZADD / ZINCRBY` (green)

- **The setup** — `ZADD leaderboard 4200 ada 3100 raj 5800 mei 2500 leo 4900 sam` loads five players
- **The bump** — raj wins a match worth 900 points: `ZINCRBY leaderboard 900 raj` returns 4000
- **Hand-check** — 3100 + 900 = 4000 (exact); raj passes no one, since ada holds 4200
- **The rank** — `ZREVRANK leaderboard raj` returns 3: three players (mei, sam, ada) score higher
- **The read** — `ZREVRANGE leaderboard 0 2` returns mei, sam, ada — still the podium
- **Atomic** — Redis runs commands one at a time, so two simultaneous bumps can never lose an update

*Example (italic):* Raj's 3100 becomes 4000 in one atomic command, and his 0-based rank comes back as 3 without anyone sorting anything.

**Key point:** Score updates and rank lookups are single commands (`ZINCRBY`, `ZREVRANK`); because Redis executes commands on a single thread, each one is atomic with no locks to manage.

### Visualization (canvas `c2`, 720×300)

Two-panel grouped bar chart: the five scores before and after raj's `ZINCRBY`, with raj's bar highlighted and his rank labeled in both panels.

- **Title (bold 15px, `#1a5276`, top center):** "ZINCRBY leaderboard 900 raj: 3100 → 4000, Rank Stays #4".
- **Panels:** left panel plot area x=60–340, right panel x=400–680; both baselines at y=245, plot height 170; 13px bold `#1a5276` panel labels "before" (x≈180) and "after" (x≈520) at y=60; y scale 0 to 6000, gridlines `#e5e9ef` at 2000/4000.
- **Left bars (score order mei, sam, ada, raj, leo), 40px wide, centers at x = 90, 145, 200, 255, 310:** heights from scores `[5800, 4900, 4200, 3100, 2500]`; fill `rgba(42,120,214,0.30)`, 2px `#2a78d6` edge; raj's bar in orange `#d95926` fill `rgba(217,89,38,0.30)`; 11px `#444` name + score labels under/over each bar.
- **Right bars (same order and x offsets +340):** heights from scores `[5800, 4900, 4200, 4000, 2500]`; raj's bar green `#008300` fill `rgba(0,131,0,0.25)` with bold 12px green "+900" above it.
- **Rank labels:** bold 12px `#4a3aa7` "#4" above raj's bar in both panels.
- **Annotation (bold 12px violet `#4a3aa7`, centered near y=285):** "one atomic command — no read-modify-write race".
- **Caption (12px `#444`, bottom right):** "scores illustrative; 3100 + 900 = 4000 exact".

## Where the Microseconds Go to Work

**Tags:** `where it's used` (blue), `latency` (green)

- **Cache** — the classic use: park a slow database answer in Redis and serve repeats from RAM
- **Session store** — a hash per logged-in user holds cart and profile bits across web servers
- **Queue** — a list with `LPUSH`/`BRPOP` hands jobs to workers; streams add consumer groups
- **Rate limiter** — `INCR` a per-user counter with a 60s expiry: cheap, atomic, self-cleaning
- **The math** — at ~0.1 ms per Redis read vs ~120 ms for a 1M-row SQL sort, the gap is ~1200×

*Example (italic):* A leaderboard page hitting SQL's `ORDER BY` costs ~120 ms per view; the same page on `ZREVRANGE` costs ~0.1 ms — the sort was paid once, at write time.

**Key point:** Redis shows up wherever many requests need the same small hot data fast — cache, sessions, queues, counters — because RAM plus ready-made structures beats recomputing.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart comparing the cost of a top-3 read: SQL sort-per-read vs Redis sorted-set read, plus two everyday Redis ops for scale.

- **Title (bold 15px, `#1a5276`, top center):** "Cost of One Top-3 Read: Sort Every Time vs Read a Sorted Structure".
- **Axis:** vertical 2px `#999` baseline at x=250, bars extend right, max width 420; log-feel achieved by hardcoded pixel widths, not a real log axis.
- **Rows (top to bottom at y = 80, 130, 180, 230), each with a left-aligned 12px `#444` label at x=20:**
  - "SQL ORDER BY, 1M rows — ~120 ms": red `#e74c3c` bar width 420
  - "Redis ZREVRANGE 0 2 — ~0.1 ms": green `#008300` bar width 42
  - "Redis GET (cache hit) — ~0.08 ms": blue `#2a78d6` bar width 36
  - "Redis INCR (rate limit) — ~0.08 ms": aqua `#199e70` bar width 36
- **Bar style:** 18px tall, solid fills at 0.85 alpha, 11px `#444` time labels at bar ends.
- **Annotation (bold 13px magenta `#d55181`, right side near y=255):** "~1200× — the sort was paid once, at write time".
- **Caption (12px `#444`, bottom right):** "timings illustrative; pixel widths schematic".

## Fast Because It Forgets — Unless You Tell It Not To

**Tags:** `common mistake` (red), `persistence` (orange)

- **The confusion** — treating Redis as a durable database because writes "succeed" instantly
- **The truth** — data lives in RAM; a crash without persistence configured loses everything
- **RDB** — snapshots dump the whole dataset to disk on a schedule; a crash loses the gap since the last one
- **AOF** — an append-only file logs every write; with `fsync` every second, a crash loses at most ~1s
- **The mistake** — keeping the only copy of orders or payments in a cache-configured Redis

*Example (italic):* With snapshots every 5 minutes, a crash at 2:07pm rolls the leaderboard back to its 2:05pm snapshot — every score bump in those 2 minutes is gone.

**Common mistake:** Assuming an in-memory store is durable by default. Redis offers RDB snapshots and AOF logging, but you must choose them — and each trades some speed or some window of loss.

### Visualization (canvas `c4`, 720×300)

Two-row timeline diagram: a crash at 2:07pm under RDB snapshots (2-minute loss window) vs AOF everysec (~1-second loss window).

- **Title (bold 15px, `#1a5276`, top center):** "Crash at 2:07pm: What Each Persistence Mode Loses".
- **Shared time axis:** 2px `#999` horizontal line per row, x=110 to x=650 spanning 2:00pm–2:08pm, 12px `#444` tick labels at 2:00 / 2:05 / 2:07; vertical dashed `#e74c3c` (dash 4/3) crash line at 2:07 (x≈583) crossing both rows, bold 12px red "crash" label at top.
- **Row 1 (axis at y=110), label 12px `#444` at x=20:** "RDB, snapshot every 5 min"; blue `#2a78d6` camera-tick markers at 2:00 and 2:05; red band `rgba(231,76,60,0.15)` from 2:05 to 2:07 with bold 12px red `#e74c3c` label "2 min of writes lost" above it.
- **Row 2 (axis at y=210), label:** "AOF, fsync every 1s"; green `#008300` tick marks every ~8px along the axis (writes logged continuously); thin red band `rgba(231,76,60,0.15)` covering only the final ~12px before the crash with bold 12px green `#008300` label "≤ 1s lost" above it.
- **Annotation (bold 13px orange `#d95926`, centered near y=275):** "durability is a setting, not a default — pick your loss window".
- **Caption (12px `#444`, bottom right):** "timeline illustrative; loss windows follow each mode's schedule".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); player names, scores, and timings are invented and labeled illustrative; the arithmetic 3100 + 900 = 4000 and raj's 0-based `ZREVRANK` of 3 are exact given those scores.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
