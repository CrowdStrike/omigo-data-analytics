# Replication Lag & Read Guarantees

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Replication Lag & Read Guarantees

**Subtitle:** Your comment can vanish on refresh because the copy you read from is a moment behind the original — three simple promises hide that gap

## The Comment That Vanished on Refresh

**Tags:** `core idea` (blue), `replication` (green), `lag` (orange)

- **The post** — at 12:00:00 a user posts comment #341 on a recipe; the leader database stores it
- **The copies** — two replicas copy every write from the leader, but each copy takes time to arrive
- **The refresh** — 300 ms later the user refreshes; the read is routed to a replica for speed
- **The gap** — that replica is 800 ms behind, so comment #341 has not arrived — the page shows nothing
- **The name** — the delay is replication lag; a read guarantee is the promise that hides it

*Example (italic):* The comment reappears on the next refresh a second later — the replica caught up, but the user has already hit "post" twice.

**Key point:** A replica is a copy that is always slightly behind the original; replication lag is that delay, and without a read guarantee any read can land inside the gap.

### Visualization (canvas `c1`, 720×300)

Two-lane timeline: the leader lane gets the write at 0 ms, the replica lane applies it at 800 ms, and a refresh read at 300 ms lands in the stale window.

- **Title (bold 15px, `#1a5276`, top center):** "One Comment, Two Copies: an 800 ms Window of Disagreement".
- **Lanes:** horizontal 2px `#999` lines at y=110 (leader) and y=200 (replica), from x=80 to x=660; bold 12px `#1a5276` labels "leader" / "replica" at x=20.
- **Time axis:** x maps 0–1000 ms to 80–660 (x = 80 + ms×0.58); 12px `#444` tick labels "0 ms" / "250" / "500" / "750" / "1000 ms" along y=245.
- **Write marker:** blue `#2a78d6` filled dot (r=6) at (80, 110), bold 12px blue label "comment #341 written (0 ms)" above it.
- **Copy arrow:** dashed `#6b7280` (dash 4/3) arrow from (80,110) to (544,200), 12px `#6b7280` label "copy in flight — lag 800 ms" at its midpoint.
- **Apply marker:** green `#008300` dot (r=6) at (544, 200), 12px green label "applied (800 ms)" below it.
- **Read marker:** red `#e74c3c` dot (r=6) at (254, 200) — 300 ms on the replica lane — bold 12px red label "refresh read (300 ms) ✗ not found".
- **Stale window:** `rgba(231,76,60,0.08)` rectangle from x=80 to x=544 spanning y=185 to y=215 on the replica lane.
- **Annotation (bold 13px violet `#4a3aa7`, top right near y=55):** "any read in the shaded window sees a page without the comment".
- **Caption (12px `#444`, bottom right):** "timings illustrative".

## Three Promises, One Anomaly Each

**Tags:** `worked example` (blue), `read-your-writes` (green), `monotonic reads` (orange)

- **Read-your-writes** — route the writer's own reads to the leader; you always see your comment
- **Its anomaly** — post at 0 ms, refresh at 300 ms, replica 800 ms behind: your comment invisible
- **Monotonic reads** — pin each session to one replica; a refresh never shows less than the last
- **Its anomaly** — refresh 1 hits a 100 ms-lag replica (seen), refresh 2 a 900 ms one (gone again)
- **Consistent prefix** — apply writes in order everywhere; an answer never precedes its question
- **Its anomaly** — a replica applies the 200 ms reply before the 0 ms question: a reply to nothing

*Example (italic):* With all three promises the poster sees comment #341 on every refresh, no refresh ever shows less than the one before, and the reply never appears without its question.

**Key point:** Each guarantee is a routing or ordering rule, not stronger hardware — read-your-writes routes the writer, monotonic reads pins the session, consistent prefix preserves write order.

### Visualization (canvas `c2`, 720×300)

Three-row diagram: each row names a guarantee, shows its anomaly on a two-replica timeline as a red event, and the fix as a green box.

- **Title (bold 15px, `#1a5276`, top center):** "Three Guarantees, Three Anomalies They Kill".
- **Rows at y = 85, 160, 235; left-aligned bold 12px `#1a5276` labels at x=20:** "read-your-writes", "monotonic reads", "consistent prefix"; a faint 1px `#e5e9ef` horizontal guide line per row from x=170 to x=660.
- **Row 1:** blue `#2a78d6` dot at x=190 labeled "write 0 ms" (11px); red `#e74c3c` box at x=300 labeled "replica read 300 ms ✗ missing"; 3px arrow to a green `#008300` box at x=520 labeled "route writer to leader ✓ seen".
- **Row 2:** green `#008300` dot at x=190 labeled "refresh 1: replica A, lag 100 ms ✓"; red `#e74c3c` box at x=350 labeled "refresh 2: replica B, lag 900 ms ✗ gone"; green box at x=545 labeled "pin session to A".
- **Row 3:** blue dot at x=190 labeled "Q at 0 ms, reply at 200 ms"; red box at x=350 labeled "replica applies reply first ✗"; green box at x=545 labeled "apply in write order".
- **Box style:** 130–160px wide, 34px tall, 8px radius; red fill `rgba(231,76,60,0.12)`, green fill `rgba(0,131,0,0.12)`, 11px `#2c3e50` text.
- **Annotation (bold 13px magenta `#d55181`, centered near y=280):** "same lag, three different symptoms — each promise removes exactly one".
- **Caption (12px `#444`, bottom right):** "lag values illustrative".

## Who Reads From Where — and Watching the Lag

**Tags:** `where it's used` (blue), `routing` (green), `monitoring` (orange)

- **Read routing** — a proxy sends a user's reads to the leader for 1 s after their own write
- **Session stickiness** — hashing the session id to one replica gives monotonic reads for free
- **Lag monitoring** — each replica reports how far behind it is; the router drops the laggards
- **The threshold** — with a 500 ms cutoff, a replica that falls 8.2 s behind stops serving reads
- **The trade** — reading only from the leader is always safe but gives up the read scaling replicas exist for

*Example (italic):* During the 2pm batch import, lag jumps from 60 ms to 8.2 s; the router pulls that replica until it is back under the 500 ms cutoff.

**Key point:** Read guarantees live in the routing layer — measure each replica's lag, route by who wrote what, and only lean on replicas that are provably close.

### Visualization (canvas `c3`, 720×300)

Line chart of one replica's lag across the day: flat tens of milliseconds, a 2pm batch-job spike to 8.2 s crossing the 500 ms router cutoff, then recovery.

- **Title (bold 15px, `#1a5276`, top center):** "Replica Lag Through the Day: the 2pm Batch Blows the Budget".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = hours "10:00" to "18:00", 12px `#444` tick labels each hour; y is log-feel via hardcoded pixel heights, not a real scale.
- **Data:** hours `[10, 11, 12, 13, 14, 15, 16, 17, 18]`, lag ms `[45, 60, 55, 60, 8200, 2400, 480, 90, 60]`, pixel heights above baseline `[10, 12, 11, 12, 170, 120, 55, 15, 12]`.
- **Line:** blue `#2a78d6` 3px through the nine points; 5px dots at each point — blue below the cutoff, red `#e74c3c` at 14:00 and 15:00 (above it).
- **Cutoff:** dashed magenta `#d55181` (dash 4/3) horizontal line at y=189 (≈500 ms), 12px magenta label "router cutoff: 500 ms" at its left end.
- **Value labels (11px `#444`)** above the points at 13:00 "60 ms", 14:00 "8.2 s", 15:00 "2.4 s", 16:00 "480 ms".
- **Annotation (bold 13px orange `#d95926`, near 14:00, y=50):** "lag 8.2 s — replica pulled from read rotation".
- **Caption (12px `#444`, bottom right):** "y-axis log-feel, lag values labeled; illustrative".

## A Replica Is Not a Mirror

**Tags:** `common mistake` (red), `lag tail` (orange)

- **The assumption** — "the replica has the same data" is only true after lag catches up, never during
- **Usually invisible** — 62% of reads land behind by under 10 ms, so a quiet test system shows nothing
- **The tail bites** — 2% of reads land over a second behind: thousands of vanished comments at scale
- **Not a database bug** — asynchronous replication is working as designed; the guarantee is the missing piece
- **Eventual is not soon** — "eventually consistent" promises no bound; under load the gap grows to minutes

*Example (italic):* The feature passes every test on a laptop with zero lag, then ships — and users report ghost comments within an hour.

**Common mistake:** Treating replication as instant because it usually is. Guarantees exist for the tail: design for the 1-in-50 read that lands in the gap, not the 49 that don't.

### Visualization (canvas `c4`, 720×300)

Histogram of how far behind the leader a replica read lands: overwhelmingly fresh, with a small over-one-second tail where the anomalies live.

- **Title (bold 15px, `#1a5276`, top center):** "How Stale Is a Read? Most Are Fresh — the Tail Is the Problem".
- **Axes:** 2px `#999` baseline at y=245; five bars 70px wide centered at x = 130, 250, 370, 490, 610; 12px `#444` bucket labels below the baseline: "< 10 ms", "10–100 ms", "100 ms–1 s", "1–10 s", "> 10 s".
- **Data:** percents `[62, 30, 6, 1.8, 0.2]`, bar heights px `[180, 87, 17, 6, 4]` (last two floored at a visible minimum).
- **Bar colors:** first two `rgba(42,120,214,0.35)` with 2px `#2a78d6` edge; third yellow `#c98500`; fourth orange `#d95926`; fifth red `#e74c3c`.
- **Percent labels:** bold 12px `#2c3e50` above each bar: "62%", "30%", "6%", "1.8%", "0.2%".
- **Annotation (bold 13px red `#e74c3c`, over the last two bars near y=90):** "2% land over 1 s behind — the vanished-comment zone".
- **Caption (12px `#444`, bottom right):** "distribution illustrative; smallest bars drawn at a minimum height".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); the 800 ms lag, 300 ms refresh, 100 ms / 900 ms replica lags, the daily lag series `[45, 60, 55, 60, 8200, 2400, 480, 90, 60]` ms with its 500 ms cutoff, and the staleness percents `[62, 30, 6, 1.8, 0.2]` are all invented and labeled illustrative; text numbers must stay in sync with these chart numbers.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
