# Running Out of File Descriptors

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Running Out of File Descriptors

**Subtitle:** Every open file, socket, and pipe costs one descriptor from a per-process budget — and the classic default budget is only 1,024

## One Leaked Socket per Failed Request

**Tags:** `core idea` (blue), `the 1,024 default` (orange), `EMFILE` (red)

- **The service** — an order-lookup API holds ~280 descriptors at 10am: client sockets, log files, a DB pool
- **The bug** — when a database call times out, the handler returns a 500 but never closes the DB socket
- **The leak** — each failed request strands one socket; a database slowdown makes ~250 failures per hour
- **The ceiling** — the process's soft limit is the classic default: 1,024 open descriptors (`ulimit -n`)
- **The crash** — three hours into the slowdown the table is full and every new `accept()` fails with EMFILE

*Example (italic):* The database slowdown starts at 10am; at 1pm the service is still running but cannot accept a single new connection — the log fills with "too many open files".

**Key point:** A file descriptor is the process's handle to any open file, socket, or pipe. The kernel gives each process a fixed budget of them, and a leak spends that budget one request at a time.

### Visualization (canvas `c1`, 720×300)

Line chart of the process's open-descriptor count over four hours: flat baseline, then a steady climb after the database slowdown until it hits the 1,024 ceiling.

- **Title (bold 15px, `#1a5276`, top center):** "One Socket per Failed Request: 279 to 1,024 in Three Hours".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = wall-clock hours 0 to 4 with 12px `#444` tick labels "9am"–"1pm" every hour; y = open descriptors 0 to 1,100, gridlines `#e5e9ef` at 250/500/750/1000.
- **Limit line:** red `#e74c3c` dashed (dash 6/4) horizontal line at y for 1,024, bold 12px red label "soft limit 1,024" above its left end.
- **Descriptor line:** blue `#2a78d6` 3px line through hours `[0, 0.5, 1, 1.5, 2, 2.5, 3, 3.5, 4]`, descriptors `[279, 281, 280, 405, 529, 654, 779, 904, 1024]` — flat until hour 1, then a straight climb of 250/hour that flattens against the limit line at hour 4.
- **Slowdown marker:** vertical dashed `#6b7280` (dash 4/3) line at hour 1, 12px `#6b7280` label "DB slowdown begins" at its top.
- **Annotation (bold 13px red `#e74c3c`, near hour 3.4, above the line):** "EMFILE: every new accept() fails".
- **Caption (12px `#444`, bottom right):** "descriptor counts illustrative".

## Counting the Descriptor Budget by Hand

**Tags:** `worked example` (blue), `budget arithmetic` (green)

- **The budget** — soft limit 1,024; the healthy process uses 279: 3 std streams + 2 logs + 1 listener + 20 DB pool + 253 client sockets
- **The leak rate** — ~250 stranded sockets per hour once the database starts timing out
- **Hand-check at +2h** — 279 + 500 leaked = 779 open, leaving only 245 descriptors of headroom
- **Hand-check at +3h** — 279 + 745 leaked = 1,024: the table is full; every `open()` and `accept()` now fails
- **Nothing frees itself** — the stranded sockets sit in CLOSE_WAIT until the process closes them or dies

*Example (italic):* Two hours into the slowdown the process holds 779 descriptors — 500 of them are dead sockets that one missing close() call left behind.

**Key point:** Exhaustion is plain arithmetic: baseline usage plus leak rate times hours. With a 1,024 budget, 279 in normal use, and 250 leaks per hour, the service has almost exactly three hours to live.

### Visualization (canvas `c2`, 720×300)

Horizontal stacked bar chart: the 1,024-descriptor budget at three moments — healthy, two hours into the slowdown, and three hours in (full) — split into fixed use, live clients, leaked sockets, and free headroom.

- **Title (bold 15px, `#1a5276`, top center):** "The 1,024 Budget Filling Up: Leaked Sockets Eat the Headroom".
- **Bar geometry:** bars start at x=230 and span up to 440px (440px = 1,024 descriptors, so 0.43px per descriptor); bar height 26px; rows at y = 80, 150, 220; left-aligned 12px `#444` row labels at x=20: "healthy (10am)", "+2 hours (779 open)", "+3 hours (1,024 — full)".
- **Segments (left to right in each row):** fixed use 26 (violet `#4a3aa7` fill `rgba(74,58,167,0.35)`, width 11px), live clients 253 (blue `#2a78d6` fill `rgba(42,120,214,0.30)`, width 109px), leaked CLOSE_WAIT (red `#e74c3c` fill `rgba(231,76,60,0.35)`, widths 0 / 215 / 320px for 0 / 500 / 745 leaked), free (no fill, 1px `#e5e9ef` outline, widths 320 / 105 / 0px for 745 / 245 / 0 free).
- **Limit marker:** vertical 2px `#e74c3c` line at x=670 spanning all rows, bold 12px red label "1,024" above it.
- **Legend (12px, below title):** violet "fixed (26)", blue "live clients (253)", red "leaked", gray outline "free".
- **Annotation (bold 13px red `#e74c3c`, right of the +3h row):** "745 leaked — nothing left to accept with".
- **Caption (12px `#444`, bottom right):** "segment sizes illustrative".

## Soft Limits, Hard Limits, and lsof

**Tags:** `where it's used` (blue), `ulimit` (green), `lsof` (orange)

- **Soft limit** — the cap actually enforced on the process; `ulimit -n` prints it; 1,024 is the classic default
- **Hard limit** — the ceiling on the soft limit; a process may raise soft up to hard, only root raises hard
- **Raising it** — `ulimit -n 65536` in the shell, `LimitNOFILE=65536` in a systemd unit; busy servers set tens of thousands
- **Finding the leak** — `lsof -p <pid>` lists every open descriptor; group the lines by type and TCP state
- **The tell** — hundreds of sockets stuck in CLOSE_WAIT means the peer hung up and your code never called close()

*Example (italic):* Two hours into the slowdown, `lsof -p 4117` prints 779 lines — 500 of them are CLOSE_WAIT sockets all pointing at the database's port, naming the leaky code path.

**Key point:** Raising the limit is the mitigation and lsof is the diagnosis — a leak will fill 65,536 descriptors just as surely as 1,024, only later.

### Visualization (canvas `c3`, 720×300)

Vertical bar chart of the lsof output two hours into the slowdown, grouped by descriptor type, with the CLOSE_WAIT pile towering over everything else.

- **Title (bold 15px, `#1a5276`, top center):** "lsof -p 4117 at +2 Hours: 500 Sockets Stuck in CLOSE_WAIT".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; y = descriptor count 0 to 550, gridlines `#e5e9ef` at 125/250/375/500 with 12px `#444` labels.
- **Bars (90px wide, centered at x = 140, 280, 420, 560), each with a 12px `#444` category label below and a bold 13px count label above:**
  - "files & pipes": violet `#4a3aa7` fill `rgba(74,58,167,0.35)`, count 26 (height 9px)
  - "sockets ESTABLISHED": blue `#2a78d6` fill `rgba(42,120,214,0.30)`, count 253 (height 83px)
  - "sockets CLOSE_WAIT": red `#e74c3c` fill `rgba(231,76,60,0.35)`, count 500 (height 164px)
  - "free headroom": no fill, 1px dashed `#6b7280` outline, count 245 (height 80px)
- **Annotation (bold 13px red `#e74c3c`, above the CLOSE_WAIT bar):** "the smoking gun: half-closed sockets never closed".
- **Caption (12px `#444`, bottom right):** "counts illustrative; total open = 779 of 1,024".

## The Error Shows Up Far From the Leak

**Tags:** `common mistake` (red), `debugging` (orange)

- **The distance** — the leak lives in the DB-timeout handler; the error surfaces in `accept()` and even log rotation
- **Whoever's next** — EMFILE hits whatever code needs the *next* descriptor, not the code hoarding 745 of them
- **Shared symptom** — at the ceiling, TLS handshakes, DNS lookups, and opening a config file all fail the same way
- **The wrong fix** — raising the limit to 65,536 without closing the leak moves the same crash out ~11 days (261 hours at 250/hour)
- **The habit** — close descriptors on every path: error branches, timeouts, early returns (or use with/defer/try-with-resources)

*Example (italic):* The on-call engineer restarts the "broken" web layer at 1pm; it runs clean for three hours and dies again — the real fix was one close() in the timeout branch.

**Common mistake:** Debugging where EMFILE is thrown. The error names the victim, not the culprit — count who is actually holding the descriptors with lsof before touching the code that failed.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram separating where the bug lives from where the error appears, shown as boxes and arrows on a shared descriptor-table backdrop.

- **Title (bold 15px, `#1a5276`, top center):** "The Leak and the Error Are Three Hours and Three Modules Apart".
- **Row 1 (y=95), label 12px `#444` at x=20:** "the bug (10am–1pm)"; blue `#2a78d6` rounded box at x=170 labeled "DB call times out" (12px), 3px arrow to an orange `#d95926` box at x=370 labeled "handler skips close()", 3px arrow to a red `#e74c3c` box at x=560 labeled "+1 CLOSE_WAIT each time".
- **Row 2 (y=205), label:** "the crash (1pm)"; red `#e74c3c` rounded box at x=170 labeled "table full: 1,024 open", 3px arrow to a red box at x=370 labeled "accept() → EMFILE", 3px arrow to a gray `#6b7280` box at x=560 labeled "on-call blames the web layer" with bold 12px red "✗ wrong module".
- **Box style:** 150–170px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(217,89,38,0.12)` / `rgba(231,76,60,0.12)` / `rgba(107,114,128,0.12)`, 12px `#2c3e50` text.
- **Connector:** vertical dashed `#6b7280` (dash 4/3) arrow from the row-1 rightmost box down to the row-2 leftmost box, 12px `#6b7280` label "3 hours later" beside it.
- **Annotation (bold 13px orange `#d95926`, centered near y=270):** "EMFILE names the victim, not the culprit".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); descriptor counts, leak rates, and lsof breakdowns are invented and labeled illustrative; the 1,024 soft-limit default, the soft/hard limit mechanics, EMFILE, and CLOSE_WAIT semantics are real; internal arithmetic is exact (279 + 500 = 779, 279 + 745 = 1,024, (65,536 − 279) / 250 ≈ 261 hours ≈ 11 days).
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
