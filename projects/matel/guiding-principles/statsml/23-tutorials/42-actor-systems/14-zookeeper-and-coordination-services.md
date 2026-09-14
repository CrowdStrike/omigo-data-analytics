# ZooKeeper & Coordination Services

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** ZooKeeper & Coordination Services

**Subtitle:** Distributed systems outsource their hardest problems — who is leader, who is alive, what is the config — to one small store that never disagrees with itself

## Five Workers, One Leader, Zero Arguments

**Tags:** `core idea` (blue), `consistent kernel` (green), `ZooKeeper` (orange)

- **The fleet** — five payment workers process a shared queue; exactly one must be the leader
- **The trap** — workers voting among themselves during a network hiccup can elect two leaders
- **The outsource** — instead, all five talk to a coordination service: a tiny replicated store
- **The tree** — the store holds znodes, a file-system-like tree: `/config`, `/workers`, `/election`
- **The kernel** — the service solves consensus once, internally; clients just read and write znodes

*Example (italic):* Each worker creates a znode under `/election`; the service hands out strictly ordered names, and whoever holds the lowest number is the leader — no worker-to-worker voting at all.

**Key point:** A coordination service is a small, strongly-consistent znode store that distributed systems delegate leader election, membership, locks, and config to — so only one system in the stack has to get consensus right.

### Visualization (canvas `c1`, 720×300)

Znode tree diagram: root at the left, three branches, with the `/election` branch expanded to show the five workers' sequential ephemeral nodes.

- **Title (bold 15px, `#1a5276`, top center):** "The Znode Tree: One Small Store Everyone Trusts".
- **Root box:** rounded box 60×34 at (30, 133) labeled "/" (bold 13px `#2c3e50`), fill `rgba(26,82,118,0.12)`.
- **Branch boxes (130px wide, 34px tall, 8px radius, fill `rgba(42,120,214,0.15)`, 12px `#2c3e50` text), stacked at x=150:** "/config" at y=55, "/workers" at y=133, "/election" at y=211; 2px `#6b7280` elbow lines from root to each.
- **Election children (five boxes 155×26, x=340, y = 60, 105, 150, 195, 240, fill `rgba(0,131,0,0.12)`, 12px text):** "n_0000000001 (A)", "n_0000000002 (B)", "n_0000000003 (C)", "n_0000000004 (D)", "n_0000000005 (E)"; 2px `#6b7280` elbow lines from "/election" to each.
- **Leader marker:** 3px `#008300` border on the "n_0000000001 (A)" box, bold 12px green `#008300` label "lowest number = leader" at (520, 73).
- **Ephemeral note (12px `#6b7280`, at (520, 160)):** "ephemeral: vanishes if its\nowner's session dies".
- **Annotation (bold 13px violet `#4a3aa7`, bottom center y=288):** "clients read and write this tree — the service keeps every copy in agreement".

## The Election, Node by Node

**Tags:** `worked example` (blue), `ephemeral nodes` (green), `watches` (orange)

- **Join** — each worker creates an ephemeral sequential znode; the service appends a counter: A gets 1, B gets 2, ... E gets 5
- **Decide** — every worker lists `/election`; A holds the lowest number (1), so A is leader
- **Watch** — B sets a watch on A's znode (its predecessor); C watches B, D watches C, E watches D
- **Crash** — at t=40s worker A dies; its session stops heartbeating, and at the 10s session timeout its znode is deleted
- **Fail over** — at t=50s the deletion fires B's watch; B re-lists, sees 2 is now lowest, and takes over as leader

*Example (italic):* A leads from t=0 to t=40s; the fleet is leaderless for the 10-second session timeout; B leads from t=50s on — no message ever passed between workers directly.

**Key point:** Ephemeral znodes tie liveness to a session — a dead worker's node disappears on its own — and watches turn that deletion into a push notification, so failover needs no polling.

### Visualization (canvas `c2`, 720×300)

Step-line timeline of who is leader over 90 seconds: A's reign, the session-timeout gap, then B's reign; watch events marked.

- **Title (bold 15px, `#1a5276`, top center):** "Failover by Ephemeral Node: A Dies at 40s, B Leads at 50s".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = seconds 0 to 90 with 12px `#444` tick labels every 15s; y = two lanes, "A" centered at y=110 and "B" at y=180, 12px `#444` lane labels at x=30.
- **A's leadership bar:** green `#008300` filled bar 14px tall in lane A from t=0 to t=40 (x=60 to x=327); at t=40 a red `#e74c3c` ✗ marker (bold 16px) and 12px red label "A crashes" above it.
- **Session-timeout gap:** hatched-look gray band `rgba(107,114,128,0.20)` across both lanes from t=40 to t=50 (x=327 to x=393), bold 12px `#6b7280` vertical-ish label "10s session timeout — leaderless" above the band at y=60.
- **Znode deletion marker:** vertical dashed `#6b7280` (dash 4/3) line at t=50, 12px `#6b7280` label "znode n_...01 deleted, B's watch fires" at its top.
- **B's leadership bar:** green `#008300` filled bar 14px tall in lane B from t=50 to t=90 (x=393 to x=660), bold 12px green label "B is leader" above it at t≈65.
- **Annotation (bold 13px violet `#4a3aa7`, near x=15s, y=265):** "failover time ≈ the session timeout, not a human's pager".
- **Caption (12px `#444`, bottom right):** "timings illustrative; 10s is a typical session timeout".

## Why Systems Outsource This

**Tags:** `where it's used` (blue), `quorum` (green)

- **Quorum writes** — a 5-server ensemble commits a write once 3 of 5 servers ack it, so a majority always knows the truth
- **Fault budget** — a 3-server ensemble survives 1 dead server, 5 survives 2, 7 survives 3 — always a minority
- **No split brain** — two halves of a partitioned ensemble can't both reach a majority, so at most one side serves writes
- **Who uses it** — Kafka (older versions), HBase, and Hadoop HDFS HA all delegated election and metadata to ZooKeeper
- **The division of labor** — the big system stays fast and loose; the tiny ensemble stays small and strict

*Example (italic):* With 5 ensemble servers, a write to `/election` commits when 3 servers ack — even if 2 machines are down, leadership decisions keep flowing.

**Key point:** Consensus is expensive, so you run it on a tiny 3-to-7-node ensemble holding kilobytes of critical state — and let the thousand-node system read the answers instead of solving the problem itself.

### Visualization (canvas `c3`, 720×300)

Grouped horizontal bar chart: for ensemble sizes 3, 5, 7 — total servers, quorum needed, and failures tolerated.

- **Title (bold 15px, `#1a5276`, top center):** "Quorum Math: Majority Commits, Minority Can Fail".
- **Layout:** three row groups at y = 75, 145, 215, left-aligned bold 13px `#2c3e50` group labels at x=20: "ensemble of 3", "ensemble of 5", "ensemble of 7"; bars start at x=180, scale 60px per server (max value 7 → 420px).
- **Bars per group (12px tall, 4px gap, value labels 12px at bar ends):**
  - ensemble of 3: blue `rgba(42,120,214,0.30)` bar "servers: 3" width 180; green `#008300` bar "quorum: 2" width 120; orange `#d95926` bar "can lose: 1" width 60
  - ensemble of 5: blue bar "servers: 5" width 300; green bar "quorum: 3" width 180; orange bar "can lose: 2" width 120
  - ensemble of 7: blue bar "servers: 7" width 420; green bar "quorum: 4" width 240; orange bar "can lose: 3" width 180
- **Annotation (bold 13px magenta `#d55181`, right side near y=260):** "two partitions can't both hold a majority — split brain is arithmetic-proof".
- **Caption (12px `#444`, bottom right):** "quorum = floor(n/2)+1, exact".

## The Herd Effect

**Tags:** `common mistake` (red), `watches` (orange)

- **The naive setup** — all five workers set their watch on the leader's znode instead of their predecessor's
- **The stampede** — the leader dies, one deletion fires four watches, and four workers re-list `/election` at once
- **At scale** — with 1,000 clients that's 999 simultaneous wake-ups and re-reads hammering the ensemble
- **The recipe** — ZooKeeper's documented election recipe: watch only the znode one number below yours
- **The result** — one deletion wakes exactly one client; failover cost stays flat as the fleet grows

*Example (italic):* Watch-the-leader wakes 4 workers for one crash in our fleet of five; watch-your-predecessor wakes exactly 1 — worker B, the next in line — and nobody else notices.

**Common mistake:** Pointing every client's watch at the same hot znode. A watch fires once per event per watcher, so N watchers means an N-client stampede — the chain-of-predecessors recipe exists precisely to avoid it.

### Visualization (canvas `c4`, 720×300)

Two-row diagram: watch-the-leader (one deletion fires four watches) vs watch-your-predecessor (one deletion fires one watch).

- **Title (bold 15px, `#1a5276`, top center):** "One Crash: Herd of Watches vs Chain of Watches".
- **Row 1 (centered y=100), label 12px `#444` at x=20:** "all watch leader"; red-bordered `#e74c3c` box 70×30 at x=140 labeled "A ✗" (fill `rgba(231,76,60,0.12)`); four blue `rgba(42,120,214,0.15)` boxes 55×30 labeled "B", "C", "D", "E" at x = 300, 400, 500, 600; 2px red `#e74c3c` arrows from A's box to all four; bold 12px red label "4 watches fire at once" at (330, 60).
- **Row 2 (centered y=215), label:** "watch predecessor"; same red "A ✗" box at x=140; same four blue boxes at x = 300, 400, 500, 600; 2px green `#008300` arrow only from A to B, thin 1.5px `#6b7280` chain arrows B←C, C←D, D←E (each watching the one before it); bold 12px green label "only B wakes — becomes leader" at (330, 175).
- **Divider:** 1px `#e5e9ef` horizontal line at y=155.
- **Annotation (bold 13px orange `#d95926`, bottom center y=285):** "wake-ups per crash: N−1 in the herd, exactly 1 in the chain".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all positions and values are the hardcoded numbers above (no randomness); the 10s session timeout, 40s/50s crash timings, and worker counts are invented and labeled illustrative; quorum arithmetic (3-of-5, floor(n/2)+1, failures tolerated 1/2/3) is exact ZooKeeper-documented behavior, as are ephemeral/sequential znodes, watches, and the watch-your-predecessor election recipe.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
