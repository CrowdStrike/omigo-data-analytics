# Sync vs Async Replication

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Sync vs Async Replication

**Subtitle:** A database can say "saved" after its copy confirms (slow but safe) or say it immediately and copy later (fast but a crash can eat acknowledged writes)

## One Order, Two Ways to Say "Saved"

**Tags:** `core idea` (blue), `durability vs latency` (green), `replication` (orange)

- **The shop** — a web shop writes every order to a primary database with a replica across town
- **Sync** — the primary tells the customer "order placed" only after the replica confirms its copy
- **Async** — the primary says "order placed" the instant its own disk write lands, copies later
- **The crash** — at 12:00:00 the primary's power supply dies, moments after taking orders
- **Sync survives** — every acknowledged order already sits on the replica; nothing is lost
- **Async loses** — orders acknowledged but not yet copied exist nowhere; the "saved" was a lie

*Example (italic):* Order #4712 is acknowledged at 11:59:58; under sync it is on the replica at 11:59:58, under async it may still be in transit when the primary dies at 12:00:00.

**Key point:** Synchronous replication makes the acknowledgment wait for the copy; asynchronous replication makes the copy wait for spare time — and a crash in that gap deletes acknowledged writes.

### Visualization (canvas `c1`, 720×300)

Two-row flow diagram on a shared left-to-right time arrow: the sync acknowledgment path (write → replica confirms → ack) vs the async path (write → ack immediately, replica catches up later), with the crash moment marked after both acks.

- **Title (bold 15px, `#1a5276`, top center):** "Where the 'Saved' Happens: Before or After the Copy".
- **Rows:** row labels 12px `#444` at x=20 — "sync" at y=100, "async" at y=210; each row is a chain of rounded boxes joined by 3px arrows.
- **Sync row (y=100):** blue `#2a78d6` box at x=110 "primary writes (2 ms)", aqua `#199e70` box at x=290 "replica confirms (18 ms)", green `#008300` box at x=480 "ack customer at 20 ms" with bold 12px green "✓ copy exists first".
- **Async row (y=210):** blue box at x=110 "primary writes (2 ms)", green box at x=290 "ack customer at 2 ms", magenta `#d55181` dashed-border box at x=480 "replica copies... later" with bold 12px magenta "copy still pending".
- **Crash marker:** vertical dashed red `#e74c3c` (dash 4/3) line at x=640 spanning both rows, bold 12px red label "primary dies" at its top.
- **Box style:** 150–170px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(25,158,112,0.15)` / `rgba(0,131,0,0.12)` / `rgba(213,81,129,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px ink `#1a5276`, centered near y=270):** "sync: ack means copied — async: ack means promised".
- **Caption (12px `#444`, bottom right):** "timings illustrative".

## The 20-Millisecond Price of a Guarantee

**Tags:** `worked example` (blue), `latency math` (green)

- **Local write** — the primary's own disk write costs 2 ms; async commits are done right there
- **Same-city sync** — add one 18 ms network round trip to the replica: 2 + 18 = 20 ms per commit
- **Cross-region sync** — a 120 ms round trip to a far region makes it 2 + 120 = 122 ms per commit
- **The ratio** — same-city sync is 10× the async commit time; cross-region sync is 61×
- **The loss window** — an async replica running 5 s behind at 40 orders/s puts 200 orders at risk
- **Hand-check** — 5 s of lag × 40 orders/s = 200 acknowledged orders that exist only on the primary

*Example (italic):* Every checkout waits 20 ms under same-city sync and 2 ms under async — but a crash with 5 s of async lag erases 200 orders that customers were told went through.

**Key point:** The sync tax is one network round trip on every single commit; the async risk is lag × write rate — 5 seconds behind at 40 orders/s means 200 acknowledged orders can vanish.

### Visualization (canvas `c2`, 720×300)

Horizontal bar chart of commit latency per acknowledged order across the three setups, with the async bar carrying the data-at-risk annotation.

- **Title (bold 15px, `#1a5276`, top center):** "Commit Latency: 2 ms Async vs 20 ms Sync vs 122 ms Cross-Region Sync".
- **Axis:** vertical 2px `#999` baseline at x=250, bars extend right, max width 420; x is linear in milliseconds (420 px = 122 ms), light `#e5e9ef` gridlines at 30/60/90/120 ms with 11px `#6b7280` tick labels below y=250.
- **Rows (bar centers at y = 80, 145, 210), each with a left-aligned 12px `#444` label at x=20:**
  - "async — local write only": green `#008300` bar width 7 (2 ms), 12px label "2 ms" at bar end
  - "sync — same-city replica": blue `#2a78d6` bar width 69 (20 ms), 12px label "20 ms = 2 + 18" at bar end
  - "sync — cross-region replica": orange `#d95926` bar width 420 (122 ms), 12px label "122 ms = 2 + 120" at bar end
- **Bar style:** 26px tall, fills `rgba(0,131,0,0.30)` / `rgba(42,120,214,0.30)` / `rgba(217,89,38,0.30)` with 2px solid edges in the same hues.
- **Annotation (bold 13px magenta `#d55181`, under the async row near y=110):** "async's hidden price: 5 s lag × 40 orders/s = 200 orders at risk".
- **Caption (12px `#444`, bottom right):** "latencies and rates illustrative".

## RPO, Semi-Sync, and the Cross-Region Bill

**Tags:** `where it's used` (blue), `RPO/RTO` (green), `semi-sync` (orange)

- **RPO** — recovery point objective: how many seconds of acknowledged data you may lose in a crash
- **RTO** — recovery time objective: how long until service is back; replication mode sets the RPO
- **Sync RPO** — zero: every acknowledged write is already on a replica when disaster strikes
- **Async RPO** — the replication lag: 5 s behind means an RPO of 5 s, 200 orders at 40 orders/s
- **Semi-sync** — wait for ONE nearby replica (20 ms), ship to far regions async: zero RPO for one failure
- **The economics** — cross-region sync taxes every commit 122 ms to protect against rare region loss

*Example (italic):* The shop picks semi-sync: 20 ms commits with a same-city replica confirming, plus an async cross-region copy — a region fire costs seconds of data, a single machine failure costs none.

**Key point:** Replication mode is the RPO dial — sync buys RPO zero with every commit's latency, async buys speed with a lag-sized loss window, and semi-sync pays the cheap round trip to cover the common failure.

### Visualization (canvas `c3`, 720×300)

Trade-off scatter: commit latency on x, orders at risk in a primary crash on y, one labeled dot per setup, showing semi-sync in the corner where both are small.

- **Title (bold 15px, `#1a5276`, top center):** "The Trade-Off: Commit Latency vs Orders at Risk".
- **Axes:** origin x=70, baseline y=240, plot width 580, plot height 170; x = commit latency 0–130 ms, 12px `#444` tick labels at 0/25/50/75/100/125; y = orders at risk 0–250, gridlines `#e5e9ef` at 50/100/150/200 with 11px `#6b7280` labels; axis titles 12px `#444` "commit latency (ms)" below and "orders at risk" rotated left.
- **Dots (10 px radius, 2px white ring), with bold 12px labels beside each:**
  - async: magenta `#d55181` dot at (2 ms, 200 orders), label "async — 2 ms, 200 at risk"
  - semi-sync: green `#008300` dot at (20 ms, 0 orders), label "semi-sync — 20 ms, 0 at risk (node failure)"
  - full cross-region sync: orange `#d95926` dot at (122 ms, 0 orders), label "cross-region sync — 122 ms, 0 at risk"
- **Corner shading:** light green `rgba(0,131,0,0.07)` rectangle over x ≤ 35 ms and y ≤ 50 orders, 11px `#008300` label "the corner you want".
- **Annotation (bold 13px ink `#1a5276`, near x=60 ms, y=60):** "semi-sync: pay the 18 ms trip once, skip the 120 ms one".
- **Caption (12px `#444`, bottom right):** "positions from the worked example, illustrative".

## The Failover That Quietly Ate 200 Orders

**Tags:** `common mistake` (red), `failover` (orange)

- **The mistake** — promoting a lagging async replica to primary and calling the failover "clean"
- **The gap** — the replica never received the last 5 s of writes; promotion cannot conjure them back
- **Silent loss** — no error fires: the new primary simply starts from an older version of history
- **The victims** — 200 customers hold confirmation emails for orders no database remembers
- **The tell** — order IDs jump backwards after failover; audits find payments without orders
- **The fix** — measure lag before promoting, or use semi-sync so an up-to-date replica always exists

*Example (italic):* The primary dies at 12:00:00 holding 400 orders from the last 10 s; the promoted replica has only 200 of them — the 200 acknowledged after 11:59:55 are gone without a single error message.

**Common mistake:** Believing a successful failover means no data loss. Async promotion silently rewinds history by the lag; the writes the old primary acknowledged after the replica's last sync are unrecoverable.

### Visualization (canvas `c4`, 720×300)

Line chart of the last 10 seconds before the crash: cumulative acknowledged orders on the primary vs orders present on the 5-second-lagged replica, with the gap at crash time shaded as lost data.

- **Title (bold 15px, `#1a5276`, top center):** "Crash at 12:00:00: the Replica Is Missing 200 Acknowledged Orders".
- **Axes:** origin x=70, baseline y=245, plot width 580, plot height 180; x = seconds before crash, tick labels "11:59:50" to "12:00:00" every 2 s (12px `#444`); y = cumulative orders 0 to 400, gridlines `#e5e9ef` at 100/200/300 with 11px `#6b7280` labels.
- **Primary line:** blue `#2a78d6` 3px line through seconds `[0, 2, 4, 6, 8, 10]`, cumulative orders `[0, 80, 160, 240, 320, 400]` — 40 orders/s, ending at 400 at the crash.
- **Replica line:** aqua `#199e70` 3px line through the same seconds, cumulative orders `[0, 0, 0, 40, 120, 200]` — flat for the first 5 s of lag, then climbing 40/s to 200 at the crash (always 5 s behind the primary).
- **Loss band:** red fill `rgba(231,76,60,0.15)` between the two lines from second 5 to 10, bold 13px red `#e74c3c` label inside near second 8: "200 acknowledged orders lost".
- **Crash marker:** vertical dashed `#6b7280` (dash 4/3) line at second 10, 12px `#6b7280` label "primary dies, replica promoted" at its top.
- **Annotation (bold 12px violet `#4a3aa7`, near second 3, y=80):** "the new primary starts from the aqua line, not the blue one".
- **Caption (12px `#444`, bottom right):** "order counts illustrative — 40 orders/s, 5 s lag".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); latencies (2 ms local write, 18 ms same-city round trip, 120 ms cross-region round trip → 20 ms and 122 ms sync commits), the 40 orders/s rate, the 5 s lag, and the resulting 200 orders at risk (with 400 vs 200 cumulative at the crash) are invented and labeled illustrative; text numbers and chart numbers must stay identical.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
