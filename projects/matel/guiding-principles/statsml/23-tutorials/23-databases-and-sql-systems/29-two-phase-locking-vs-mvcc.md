# Two-Phase Locking vs MVCC

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Two-Phase Locking vs MVCC

**Subtitle:** Two ways a database lets readers and writers share the same row — make one of them wait, or keep an old copy of the row for the reader

## The Report That Blocks the Checkout

**Tags:** `core idea` (blue), `readers vs writers` (green), `concurrency` (orange)

- **The scan** — at 9:00pm a bookstore's nightly report starts a 40-second scan of the orders table
- **The write** — at 9:00:05 a customer checks out, updating book row 118's stock from 7 to 6
- **2PL's answer** — the report holds a read lock on row 118, so the checkout must wait 35 seconds
- **MVCC's answer** — the checkout writes a new copy of row 118; the report reads the old copy
- **The trade** — 2PL queues for one agreed truth; MVCC answers instantly from different moments

*Example (italic):* Under 2PL the customer stares at a spinner for 35 seconds; under MVCC the checkout finishes in 20 milliseconds while the report is mid-scan.

**Key point:** Two-phase locking makes readers and writers take turns on a row; MVCC lets them run at once by keeping multiple versions of the row alive.

### Visualization (canvas `c1`, 720×300)

Two-lane Gantt timeline: the same 40-second report and 9:00:05 checkout, once under 2PL (checkout blocked) and once under MVCC (checkout instant).

- **Title (bold 15px, `#1a5276`, top center):** "Same Report, Same Checkout: Who Waits?".
- **Axes:** shared time axis, origin x=60, baseline y=255, plot width 600; x = seconds 0 to 45 mapped linearly, 12px `#444` tick labels at 0/10/20/30/40s; two lanes with 12px `#444` labels at x=15: "2PL" centered at y=110, "MVCC" centered at y=210.
- **2PL lane (bars 22px tall at y=99):** blue `rgba(42,120,214,0.35)` report bar from 0s to 40s with 12px `#2a78d6` label "report scan (40s)"; orange `rgba(217,89,38,0.35)` bar with 2px `#d95926` border from 5s to 40s labeled bold 12px `#d95926` "checkout blocked 35s"; solid green `#008300` tick bar from 40s to 41s.
- **MVCC lane (bars 22px tall at y=199):** identical blue report bar 0s–40s; solid green `#008300` tick at 5s (3px wide) with bold 12px `#008300` label "checkout done in 20ms".
- **Arrival marker:** vertical dashed `#6b7280` (dash 4/3) line at 5s across both lanes, 12px `#6b7280` label "checkout arrives 9:00:05" at its top.
- **Annotation (bold 13px green `#008300`, near x=420, y=185):** "MVCC: the write never waits for the read".
- **Caption (12px `#444`, bottom right):** "durations illustrative".

## Lock Phases vs a Version Chain, By Hand

**Tags:** `worked example` (blue), `transaction ids` (green)

- **2PL growing phase** — the report (T1) locks rows 87, 118, 205 as it scans, releasing none early
- **2PL shrinking phase** — T1 releases all 3 locks only at commit — those are the two phases
- **The block** — checkout T2 needs a write lock on row 118, blocked by T1's read lock until commit
- **MVCC's chain** — row 118: (stock 7, created 88, deleted 91) → (stock 6, created 91)
- **The snapshot rule** — the report is snapshot 90: sees versions created ≤ 90 and still live at 90
- **Hand-check** — stock 7: created 88 ≤ 90, deleted 91 > 90 → visible; stock 6: 91 > 90 → hidden

*Example (italic):* Report snapshot 90 reads stock 7 even though txn 91 already committed stock 6 — both answers are real, from two moments in time.

**Key point:** 2PL orders transactions with a lock-acquire phase then a release phase; MVCC orders them with transaction ids stamped on row versions and a snapshot rule for visibility.

### Visualization (canvas `c2`, 720×300)

Two-panel diagram: left, the 2PL lock-count staircase for the report; right, row 118's version chain with the snapshot-90 reader picking the old version.

- **Title (bold 15px, `#1a5276`, top center):** "2PL: a Lock Staircase — MVCC: a Version Chain".
- **Left panel (x=50 to x=330):** 12px `#444` panel label "T1's locks held" at (60, 60); axes origin x=60, baseline y=245, panel plot width 250, height 150; x = scan progress with tick labels "start"/"commit" (12px `#444`); y = locks held 0 to 3, gridlines `#e5e9ef` at 1/2/3.
- **Staircase line:** blue `#2a78d6` 3px step line through progress fractions `[0, 0.2, 0.5, 0.8, 1.0]`, locks `[0, 1, 2, 3, 3]` (steps up at rows 87, 118, 205), then a red `#e74c3c` 3px vertical drop from 3 to 0 at commit with bold 12px red label "release all at commit".
- **Right panel (x=380 to x=700):** 12px `#444` panel label "row 118's versions" at (390, 60); two rounded boxes 140px wide, 52px tall, 8px radius, 12px `#2c3e50` text: box A at (400, 110) fill `rgba(42,120,214,0.15)` with 2px `#2a78d6` border, lines "stock 7" / "created 88, deleted 91"; box B at (400, 195) fill `rgba(0,131,0,0.12)` with 2px `#008300` border, lines "stock 6" / "created 91"; 2px `#6b7280` arrow from box A's bottom to box B's top labeled 11px `#6b7280` "newer".
- **Reader arrow:** bold 3px violet `#4a3aa7` arrow from a 12px violet label "report, snapshot 90" at (585, 95) pointing to box A.
- **Annotation (bold 12px violet `#4a3aa7`, near x=560, y=250):** "88 ≤ 90 < 91 → reader sees stock 7".
- **Caption (12px `#444`, bottom right):** "row and txn ids illustrative".

## Why Long Reports Stopped Freezing Writes

**Tags:** `where it's used` (blue), `vacuum` (orange), `history` (green)

- **The old pain** — on lock-based engines, a long report froze checkouts table-wide; shops ran reports at 3am to hide it
- **The MVCC shift** — snapshot-reading engines let analytics run at noon: readers never block writers, writers never block readers
- **The bill arrives** — every update leaves a dead old version behind; a vacuum/garbage sweep must reclaim them
- **Steady state** — with a sweep every 20 minutes, dead versions climb to ~240k then drop to ~20k, a sawtooth
- **The pinning trap** — one report transaction left open for an hour pins every old version it might still need: ~720k dead rows the sweep cannot touch

*Example (italic):* An analyst's forgotten open transaction bloats the orders table to 720k dead versions in an hour — queries slow down for everyone.

**Key point:** MVCC trades blocking for garbage — reads get cheap, but old versions pile up and a vacuum process (plus disciplined short transactions) becomes part of running the database.

### Visualization (canvas `c3`, 720×300)

Line chart of dead row versions over one hour: a healthy vacuum sawtooth vs an hour-long open report pinning versions.

- **Title (bold 15px, `#1a5276`, top center):** "MVCC's Rent: Dead Versions Pile Up Until Vacuum Runs".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = minutes 0 to 60, 12px `#444` tick labels every 15 min; y = dead versions 0 to 750 (thousands), gridlines `#e5e9ef` at 250/500, 12px `#444` labels "250k"/"500k".
- **Sawtooth line (healthy):** green `#008300` 3px line through minutes `[0, 10, 20, 20, 30, 40, 40, 50, 60]`, dead versions (thousands) `[0, 120, 240, 20, 140, 260, 25, 145, 265]` — vertical cliffs at the minute-20 and minute-40 vacuums.
- **Pinned line:** orange `#d95926` 3px line through minutes `[0, 10, 20, 30, 40, 50, 60]`, dead versions (thousands) `[0, 120, 240, 360, 480, 600, 720]` — vacuum runs but reclaims nothing.
- **Vacuum markers:** vertical dashed `#6b7280` (dash 4/3) lines at minutes 20 and 40, 11px `#6b7280` label "vacuum" at the top of each.
- **Line labels:** bold 12px green "vacuum reclaims ~220k" near (x=22 min, y=200); bold 12px orange "open 1-hour report pins everything" near (x=38 min, y=95).
- **Annotation (bold 13px magenta `#d55181`, near x=48 min, y=45):** "long transactions are MVCC's silent bloat".
- **Caption (12px `#444`, bottom right):** "version counts illustrative".

## MVCC Doesn't Mean No Locks

**Tags:** `common mistake` (red), `write conflicts` (orange)

- **The confusion** — hearing "readers don't block", people conclude MVCC databases never make anyone wait
- **Writers still queue** — two checkouts updating the same row 118 conflict; the second waits for the first to commit
- **What MVCC removes** — only the reader-writer and writer-reader waits; writer-writer waits stay in both worlds
- **Snapshots go stale** — an MVCC reader sees the database as of its snapshot, so a just-committed change is invisible to it
- **The subtle bug** — two transactions each read old versions, each write different rows, and jointly break a rule neither saw (write skew)

*Example (italic):* Two customers buy the last copy of book 118 at once — MVCC lets both read stock 1, then makes the second update wait and re-check, not skip the wait.

**Common mistake:** Treating MVCC as lock-free. It deletes two of the three kinds of waiting, and swaps blocking for staleness — writers on the same row still queue, and snapshot readers can act on out-of-date data.

### Visualization (canvas `c4`, 720×300)

Who-waits matrix: three conflict types (read-write, write-read, write-write) as columns, 2PL and MVCC as rows, each cell a "waits" or "no wait" chip.

- **Title (bold 15px, `#1a5276`, top center):** "Who Waits for Whom: 2PL vs MVCC".
- **Grid:** column headers bold 12px `#2c3e50` at y=80 over x=250/420/590: "reader meets writer", "writer meets reader", "writer meets writer"; row labels bold 13px `#1a5276` at x=60: "2PL" at y=140, "MVCC" at y=215; thin `#e5e9ef` separator lines between rows and columns.
- **Cells:** rounded chips 120px wide, 38px tall, 8px radius, centered under each header; 2PL row: three red chips fill `rgba(231,76,60,0.12)`, 2px `#e74c3c` border, bold 12px `#e74c3c` text "waits"; MVCC row: green chip `rgba(0,131,0,0.12)` / 2px `#008300` border / bold 12px "no wait", green chip "no wait", red chip "waits".
- **Cell subtext (11px `#6b7280`, under each MVCC chip):** "reads old version", "write proceeds", "second writer queues".
- **Annotation (bold 13px green `#008300`, centered near y=272):** "MVCC removes 2 of 3 waits — writers on the same row still queue".
- **Caption (12px `#444`, bottom right):** "behavior schematic, engine details vary".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); the 40s scan, 35s block, 20ms write, row ids 87/118/205, txn ids 88/90/91, stock 7→6, and the dead-version series (sawtooth `[0,120,240,20,140,260,25,145,265]`k, pinned `[0,120,240,360,480,600,720]`k) are invented and labeled illustrative or schematic.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
