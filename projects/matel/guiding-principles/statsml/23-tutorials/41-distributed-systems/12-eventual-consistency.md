# Eventual Consistency

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Eventual Consistency

**Subtitle:** Copies of the same record may answer with different values for a moment — but once writes stop, every copy converges to the same number

## One Photo, Three Different Like Counts

**Tags:** `core idea` (blue), `replication` (green), `stale reads` (orange)

- **The photo** — a photo's like count is stored on three server copies, called replicas A, B, and C
- **The reads** — your phone asks replica A, your laptop asks B, your tablet asks C, all at once
- **The answers** — phone says 41 likes, laptop says 42, tablet says 42 — same photo, same instant
- **The lag** — like #42 has reached B and C, but the update is still in flight to replica A
- **The refresh** — one second later the phone also shows 42; the disagreement quietly heals itself

*Example (italic):* At the 3-second mark the three devices read 41, 42, 42; by the 4-second mark all three read 42.

**Key point:** Eventual consistency means replicas may briefly disagree after a write, but if writes stop, every replica eventually returns the same final value.

### Visualization (canvas `c1`, 720×300)

Grouped bar chart: the like count returned by each of the three replicas at two read moments — during the lag and after convergence.

- **Title (bold 15px, `#1a5276`, top center):** "Same Photo, Same Question: 41 or 42 Likes?".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; y = like count 38 to 43, gridlines `#e5e9ef` at 39/40/41/42/43, 12px `#444` y-tick labels; note 11px `#6b7280` under y-axis: "y starts at 38 to make the 1-like gap visible".
- **Groups:** two groups centered at x=210 ("read at 3s") and x=510 ("read at 4s"), 12px `#444` group labels below baseline; three 52px-wide bars per group with 14px gaps: phone→A blue `#2a78d6`, laptop→B green `#008300`, tablet→C aqua `#199e70`.
- **Bar values:** group "read at 3s" = `[41, 42, 42]`; group "read at 4s" = `[42, 42, 42]`; bold 13px `#2c3e50` value label on top of each bar; 11px `#6b7280` replica labels "A" / "B" / "C" inside bar bases.
- **Annotation (bold 12px red `#e74c3c`, arrowed to the 41 bar):** "replica A hasn't heard about like #42 yet".
- **Annotation (bold 12px green `#008300`, above the second group):** "one second later, all three agree".
- **Caption (12px `#444`, bottom right):** "like counts illustrative".

## Watching a Like Ripple Through Three Replicas

**Tags:** `worked example` (blue), `propagation` (green), `convergence` (orange)

- **The start** — at t=0 all three replicas hold 40 likes; two new likes are about to arrive
- **Write #41** — lands on replica B at t=0s; B now reads 41 while A and C still read 40
- **Write #42** — lands on B at t=1s; replication carries #41 to C at 1s and #42 to C at 2s
- **The slow copy** — replica A gets #41 at t=2s and #42 only at t=4s; it lags the whole time
- **Convergence** — the last write was at t=1s; by t=4s every replica reads 42 and stays there
- **The promise** — "eventual" only says: stop writing and the copies will agree — no deadline given

*Example (italic):* Read all three replicas at t=3s and you get A=41, B=42, C=42 — the exact mismatch your three devices saw.

**Key point:** Convergence is defined by writes stopping, not by a clock — the guarantee is "no new writes, then agreement", never "agreement within N seconds".

### Visualization (canvas `c2`, 720×300)

Step-line chart: the value stored on each replica over the 5 seconds after the writes, three lines converging to 42.

- **Title (bold 15px, `#1a5276`, top center):** "Two Likes Ripple Out: Every Replica Ends at 42".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = seconds 0 to 5, 12px `#444` tick labels every 1s; y = stored like count 40 to 42, gridlines `#e5e9ef` at 40/41/42.
- **Replica B step line (green `#008300`, 3px):** seconds `[0, 1, 2, 3, 4, 5]`, values `[41, 42, 42, 42, 42, 42]`; 12px green label "B" at the line's right end.
- **Replica C step line (aqua `#199e70`, 3px):** same second grid, values `[40, 41, 42, 42, 42, 42]`; 12px aqua label "C".
- **Replica A step line (blue `#2a78d6`, 3px):** same second grid, values `[40, 40, 41, 41, 42, 42]`; 12px blue label "A".
- **Write markers:** bold 12px `#c98500` dots on B at (0, 41) and (1, 42) labeled "like #41 lands" and "like #42 lands".
- **Last-write marker:** vertical dashed `#6b7280` (dash 4/3) line at t=1s, 12px `#6b7280` label "last write" at its top.
- **Annotation (bold 13px green `#008300`, near t=4s, y=80):** "writes stopped at 1s — all agree by 4s".
- **Caption (12px `#444`, bottom right):** "replication delays illustrative".

## The Trade: Answer Fast Now, Agree Later

**Tags:** `where it's used` (blue), `availability` (green), `AP systems` (orange)

- **The choice** — when replicas can't all be reached, a system either waits (or errors) or answers stale
- **AP systems** — availability-first stores answer from the nearest replica and reconcile afterwards
- **The speed** — a nearest-replica read returns in ~12ms; waiting on a cross-region quorum costs ~180ms
- **Tolerates it** — like counts, view counters, follower feeds: a briefly stale number harms nobody
- **Does not** — account balances and inventory: two replicas both selling the last seat is real money
- **Per field** — one app can mix both: eventual for the feed, strongly consistent for the checkout

*Example (italic):* A like count that reads 41 instead of 42 for three seconds costs nothing; an inventory count that does the same can sell one seat twice.

**Key point:** Eventual consistency is a deliberate trade — you accept briefly stale reads to keep answering fast during lag and partitions; choose it per field by asking what a stale value can break.

### Visualization (canvas `c3`, 720×300)

Two-panel chart: left, read-latency bars for eventual vs strongly consistent reads; right, a checklist of which data tolerates staleness.

- **Title (bold 15px, `#1a5276`, top center):** "What You Buy, and What You Risk".
- **Left panel (x 40–360):** two horizontal bars, 26px tall, starting at x=150, max width 190; 12px `#444` row labels at x=40. Row y=110 "nearest replica (eventual)": green `#008300` bar width 13 (12ms), 12px green value label "12 ms" at bar end. Row y=170 "cross-region quorum (strong)": orange `#d95926` bar width 190 (180ms), 12px orange label "180 ms". Sub-caption 11px `#6b7280` at y=215: "read latency, illustrative".
- **Right panel (x 400–700):** four rows at y = 95, 135, 175, 215, each a 13px `#2c3e50` item with a bold 14px verdict mark at x=410: "✓" green `#008300` before "like counts", "✓" green before "news feeds", "✗" red `#e74c3c` before "account balances", "✗" red before "inventory counts"; panel header bold 12px `#1a5276` "can this field be stale for seconds?" at y=65.
- **Divider:** vertical 1px `#e5e9ef` line at x=380 from y=55 to y=250.
- **Annotation (bold 13px magenta `#d55181`, centered near y=272):** "pick consistency per field, not per app".

## Eventual Is Not Soon — and the Count Can Go Backwards

**Tags:** `common mistake` (red), `no deadline` (orange), `ordering` (blue)

- **The confusion** — "eventual" reads as "in a second or two"; the contract names no time at all
- **The tail** — lag is usually milliseconds, but a partition or backlog can stretch it to minutes
- **No ordering** — plain eventual consistency doesn't promise your reads see values in write order
- **Backwards** — refreshing can bounce you to a laggier replica, so the count drops from 42 to 40
- **Your own like** — you tap like, refresh, and it's gone: read-your-writes is a separate guarantee
- **The fixes** — session guarantees (sticky replica, monotonic reads) are add-ons, not the default

*Example (italic):* Six refreshes over four seconds return 42, 40, 42, 41, 42, 42 as reads bounce between replicas B, A, C, A, B, A — each answer honest for its replica.

**Common mistake:** Treating "eventual" as "soon and in order". The bare guarantee is convergence only — no deadline, no monotonic reads, no read-your-writes; those need extra session or ordering guarantees.

### Visualization (canvas `c4`, 720×300)

Read-sequence chart: one user's six refreshes over 5 seconds, each read answered by a different replica, showing the count visibly going backwards.

- **Title (bold 15px, `#1a5276`, top center):** "Six Refreshes, One User: the Count Goes 42 → 40".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = seconds 0 to 5, 12px `#444` tick labels every 1s; y = value returned 39 to 43, gridlines `#e5e9ef` at 40/41/42.
- **Read points:** 7px dots connected by a 2px dashed `#6b7280` line, at seconds `[1, 1.5, 2, 2.5, 3, 4.5]`, values `[42, 40, 42, 41, 42, 42]`; 11px `#6b7280` replica label under each dot: `["B", "A", "C", "A", "B", "A"]` (values match the replica step lines in `c2`).
- **Dot colors:** reads that go backwards from the previous read — the 40 at t=1.5s and the 41 at t=2.5s — red `#e74c3c`; all other dots blue `#2a78d6`.
- **Annotation (bold 13px red `#e74c3c`, near t=1.5s, y=90, arrowed to the 40 dot):** "refresh dropped the count by 2 — no monotonic-reads guarantee".
- **Annotation (bold 12px green `#008300`, near t=4.5s):** "converged: every replica now says 42".
- **Caption (12px `#444`, bottom right):** "read timings illustrative; each answer is correct for its replica".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); the like counts (40 → 42), the replica step values in `c2` (B `[41,42,42,42,42,42]`, C `[40,41,42,42,42,42]`, A `[40,40,41,41,42,42]`), the 3s snapshot `[41,42,42]` / 4s snapshot `[42,42,42]` in `c1`, the read latencies (12 ms / 180 ms) in `c3`, and the refresh sequence `[42,40,42,41,42,42]` from replicas `[B,A,C,A,B,A]` in `c4` are all invented and labeled illustrative; the `c4` read values must stay consistent with the `c2` step lines at those timestamps.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
