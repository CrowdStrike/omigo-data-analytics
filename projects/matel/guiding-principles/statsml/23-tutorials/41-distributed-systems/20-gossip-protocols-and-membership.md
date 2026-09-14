# Gossip Protocols & Membership

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Gossip Protocols & Membership

**Subtitle:** Each node repeats what it heard to two random peers per round — like a rumor, the news reaches a 64-node cluster in about log(N) rounds, with no announcer in charge

## The Rumor That Sweeps a 64-Node Cluster

**Tags:** `core idea` (blue), `epidemic spread` (green), `random peers` (orange)

- **The cluster** — 64 storage nodes, no leader; each node knows only a random handful of peers
- **The news** — node 12 notices node 7 stopped answering and wants the whole cluster to know
- **The tell** — each round, every node that knows the news repeats it to 2 peers picked at random
- **The spread** — knowers go 1, 3, 9, 27, then all 64 — four rounds, like a rumor through a school
- **The name** — this repeat-to-random-peers pattern is called a gossip (or epidemic) protocol

*Example (italic):* Node 12 tells nodes 30 and 51 in round 1; by round 4 every node in the cluster has heard that node 7 is down.

**Key point:** Gossip spreads news the way an epidemic spreads a cold: each carrier infects a few random others, so the whole cluster learns in about log(N) rounds with no announcer in charge.

### Visualization (canvas `c1`, 720×300)

Line chart of nodes-in-the-know per gossip round: the epidemic growth curve 1 → 3 → 9 → 27 → 64.

- **Title (bold 15px, `#1a5276`, top center):** "One Rumor, Two Tells per Node: All 64 Nodes Know by Round 4".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = rounds 0 to 4 with 12px `#444` tick labels "round 0"–"round 4"; y = nodes that know 0 to 64, gridlines `#e5e9ef` at 16/32/48.
- **Spread line:** blue `#2a78d6` 3px line through rounds `[0, 1, 2, 3, 4]`, nodes `[1, 3, 9, 27, 64]`.
- **Points:** green `#008300` filled dots (r=5) at each round with bold 12px `#008300` count labels "1", "3", "9", "27", "64" above each dot.
- **Ceiling line:** horizontal dashed `#6b7280` (dash 4/3) line at y for 64 nodes, 12px `#6b7280` label "all 64 nodes" at its left.
- **Annotation (bold 13px green `#008300`, near round 1.5, y=100):** "4 rounds ≈ log₃(64) — epidemic, not linear".
- **Caption (12px `#444`, bottom right):** "round counts idealized (no duplicate tells) — illustrative".

## Tripling by Hand, and Heartbeats That Freeze

**Tags:** `worked example` (blue), `heartbeat counters` (green), `failure detection` (orange)

- **The multiplier** — each knower tells 2 fresh peers, so the count of knowers triples every round
- **Hand-check** — 1×3=3, 3×3=9, 9×3=27, 27×3=81, capped at the 64 nodes that actually exist
- **The duplicates** — real tells sometimes hit nodes that already know, costing a round of slack
- **The heartbeat** — every node gossips a counter it bumps each second, e.g. "node 7: 45"
- **The freeze** — node 7's counter sticks at 45 while node 3's climbs 40, 44, 48, 52, 56, 60
- **The verdict** — a counter frozen for 10 seconds marks node 7 suspect, without ever pinging it

*Example (italic):* At t=14s node 7's gossiped counter has read 45 for 10 straight seconds, so its neighbors mark it suspect — no direct ping ever happened.

**Key point:** The same tripling that spreads a rumor also spreads liveness: nodes gossip heartbeat counters, and a counter that stops climbing is itself the failure signal.

### Visualization (canvas `c2`, 720×300)

Line chart of three gossiped heartbeat counters over 20 seconds: two keep climbing, node 7's freezes at t=4s and gets marked suspect 10 seconds later.

- **Title (bold 15px, `#1a5276`, top center):** "Gossiped Heartbeat Counters: Node 7 Goes Quiet at t=4s".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = seconds 0 to 20, 12px `#444` tick labels every 4s; y = counter value 30 to 65, gridlines `#e5e9ef` at 40/50/60.
- **Node 3 line:** green `#008300` 3px line through seconds `[0, 4, 8, 12, 16, 20]`, counters `[40, 44, 48, 52, 56, 60]`, 12px green label "node 3" at its right end.
- **Node 21 line:** aqua `#199e70` 3px line through the same seconds, counters `[38, 42, 46, 50, 54, 58]`, 12px aqua label "node 21" at its right end.
- **Node 7 line:** orange `#d95926` 3px line through the same seconds, counters `[41, 45, 45, 45, 45, 45]` — flat after t=4, 12px orange label "node 7 (frozen at 45)" above the flat stretch.
- **Suspect marker:** vertical dashed red `#e74c3c` (dash 4/3) line at t=14, bold 12px red label "10s frozen → suspect" at its top.
- **Annotation (bold 13px orange `#d95926`, near t=8, y=90):** "no pings needed — the frozen counter travels by gossip".
- **Caption (12px `#444`, bottom right):** "counter values illustrative".

## Membership Without a Boss

**Tags:** `where it's used` (blue), `no single point of failure` (green), `scales to thousands` (orange)

- **Membership** — Cassandra- and Serf-style clusters use gossip to track who is in and who is alive
- **No boss** — there is no central registry to crash; any node can seed news, every node hears it
- **Cheap** — each node sends only 2 small messages per round, whatever the cluster size
- **Log scaling** — 64 nodes need 4 rounds; 32,768 nodes need only 10 — size grows 512×, rounds add 6
- **Self-healing** — a lost message barely matters; the news arrives again from another random peer

*Example (italic):* A 4,096-node cluster spreads a membership change in about 8 gossip rounds — a second or two of wall-clock time at typical round intervals.

**Key point:** Gossip gives cluster membership with no single point of failure and constant per-node cost — that is why it scales calmly to clusters of thousands of nodes.

### Visualization (canvas `c3`, 720×300)

Vertical bar chart: gossip rounds needed to reach every node, across cluster sizes from 64 to 32,768.

- **Title (bold 15px, `#1a5276`, top center):** "512× the Cluster, Only 6 More Rounds".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; y = rounds 0 to 10, gridlines `#e5e9ef` at 2/4/6/8; no x tick marks, bars carry their own labels.
- **Bars:** 4 bars, 90px wide, centered at x = 140, 290, 440, 590; cluster sizes `[64, 512, 4096, 32768]`, rounds `[4, 6, 8, 10]`, pixel heights `[72, 108, 144, 180]` (18px per round); fill `rgba(42,120,214,0.35)`, 2px `#2a78d6` border.
- **Labels:** bold 12px `#1a5276` round counts "4", "6", "8", "10" above each bar; 12px `#444` size labels "64", "512", "4,096", "32,768" under the baseline with an 11px `#6b7280` "nodes" sub-label.
- **Annotation (bold 13px violet `#4a3aa7`, upper left near x=90, y=70):** "rounds grow with log(size) — thousands of nodes, ~10 rounds".
- **Caption (12px `#444`, bottom right):** "rounds = ⌈log₃ N⌉ at fanout 2; exact, not simulated".

## Everyone Agrees — Just Not at the Same Instant

**Tags:** `common mistake` (red), `eventually consistent` (orange), `probabilistic` (blue)

- **The confusion** — people expect one crisp instant when "the cluster" decides node 7 is dead
- **The reality** — at round 2 only 9 nodes say down while 55 still say alive; both views are correct
- **Probabilistic** — random peer picks mean the round a given node hears the news is chance
- **Eventually consistent** — every node converges to the same member list, just not simultaneously
- **The mistake** — treating the brief disagreement window as a bug and bolting on a coordinator

*Example (italic):* A dashboard polling node 44 shows node 7 alive one second after node 12's dashboard shows it dead — both are reading a healthy gossip cluster mid-spread.

**Common mistake:** Expecting gossip to give an instant, cluster-wide truth. It promises convergence within ~log(N) rounds — the short window where nodes disagree is normal operation, not a fault.

### Visualization (canvas `c4`, 720×300)

Banded area chart of the disagreement window: nodes convinced node 7 is down (lower band, growing) vs nodes still saying alive (upper band, shrinking), total constant at 64.

- **Title (bold 15px, `#1a5276`, top center):** "Rounds 1–3: The Cluster Honestly Disagrees About Node 7".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = rounds 0 to 4, 12px `#444` tick labels "round 0"–"round 4"; y = nodes 0 to 64, gridlines `#e5e9ef` at 16/32/48.
- **"Say down" band (bottom):** green fill `rgba(0,131,0,0.12)` under a 2px `#008300` line through rounds `[0, 1, 2, 3, 4]`, nodes `[1, 3, 9, 27, 64]`.
- **"Say alive" band (top):** magenta fill `rgba(213,81,129,0.15)` between the green line and the constant total 64, 2px `#d55181` upper edge along y for 64.
- **Labels:** bold 12px magenta "still say alive" at (round≈1, high in the upper band); bold 12px green "say down" at (round≈3.4, low in the lower band).
- **Annotation (bold 13px magenta `#d55181`, near round 2, y=70):** "at round 2: 9 say down, 55 say alive — both are behaving correctly".
- **Caption (12px `#444`, bottom right):** "counts idealized, illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); the spread counts `[1, 3, 9, 27, 64]` are the ideal tripling capped at 64 and labeled illustrative; heartbeat counters (node 3 `[40,44,48,52,56,60]`, node 21 `[38,42,46,50,54,58]`, node 7 `[41,45,45,45,45,45]`) and the round-2 split (9 down / 55 alive) are invented and labeled illustrative; the rounds-vs-size bars (64→4, 512→6, 4,096→8, 32,768→10) are the exact values of ⌈log₃ N⌉ at fanout 2.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
