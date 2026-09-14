# Consensus & Paxos/Raft

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Consensus & Paxos/Raft

**Subtitle:** How five machines agree on one answer even while any of them can crash — a value counts once a majority stores it, and majorities always overlap

## Five Copies of the Orders Table

**Tags:** `core idea` (blue), `agreement` (green), `fault tolerance` (orange)

- **The setup** — a coffee chain keeps its orders table on 5 servers so no single crash loses data
- **The problem** — a new order must land on every copy, but any server can crash mid-write
- **The vote** — instead of waiting for all 5, the servers treat 3 matching copies as the truth
- **The overlap** — any two groups of 3 out of 5 share a server, so two majorities can never disagree
- **The survival** — with 3 of 5 alive the cluster keeps taking orders; 2 crashes are survivable

*Example (italic):* Order #4812 (2 lattes) arrives; servers S4 and S5 crash mid-vote, but S1, S2, and S3 all store it — 3 of 5, so the order is safe.

**Key point:** Consensus makes N machines act as one reliable machine: a value is decided once a majority stores it, and because any two majorities overlap, the cluster can never split into two truths.

### Visualization (canvas `c1`, 720×300)

Fan-out diagram: one client order arriving at five replica servers, three storing it (majority) and two crashed mid-vote.

- **Title (bold 15px, `#1a5276`, top center):** "Order #4812: 2 Servers Crash Mid-Vote, 3 of 5 Still Decide".
- **Client box:** rounded box at x=40, y=130, 110×44, fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border, 12px `#2c3e50` label "order #4812\n2 lattes".
- **Server boxes:** five rounded boxes at x=330, width 170, height 34, at y = 45, 90, 135, 180, 225; 2px arrows from the client box's right edge fanning to each box's left edge.
- **Alive servers (S1, S2, S3 — rows at y=45/90/135):** fill `rgba(0,131,0,0.12)`, 2px `#008300` border, 12px labels "S1 — stored ✓", "S2 — stored ✓", "S3 — stored ✓"; their arrows solid `#2a78d6`.
- **Crashed servers (S4, S5 — rows at y=180/225):** fill `rgba(231,76,60,0.12)`, 2px `#e74c3c` border, 12px labels "S4 — crashed ✗", "S5 — crashed ✗"; their arrows dashed (dash 4/3) `#6b7280`.
- **Majority bracket:** dashed `#4a3aa7` (dash 5/4) rounded rectangle around the three alive boxes (x≈320 to 515, y≈35 to 180), 12px violet `#4a3aa7` label "majority: 3 of 5" at its top-right.
- **Annotation (bold 13px green `#008300`, near x=540, y=110):** "3 matching copies = decided".
- **Caption (12px `#444`, bottom right):** "one order, five replicas — crash pattern illustrative".

## Term 2: Electing S3, Then Committing Entry #7

**Tags:** `worked example` (blue), `leader election` (green), `majority acks` (orange)

- **The timeout** — old leader S5 crashes; S3's election timer fires first, at t=0ms
- **The term** — S3 bumps the term counter to 2, votes for itself, and asks the others for votes
- **The votes** — S1 votes at 4ms and S2 at 6ms; with its own vote S3 has 3 of 5 and is leader
- **The entry** — S3 appends entry #7 (order #4812) to its log at 20ms and sends it to all followers
- **The acks** — S1 acks at 24ms, S2 at 26ms; leader + 2 acks = 3 copies, entry #7 is committed
- **The straggler** — S4 acks late at 29ms and S5 never answers; neither one delays the commit

*Example (italic):* Entry #7 commits at 26ms with S3, S1, and S2 holding it — S4 catches up at 29ms, and dead S5 is simply ignored.

**Key point:** Raft splits consensus into two majority votes — one to elect a leader for the term, one per log entry to commit it. Both need only 3 of 5, so one dead server blocks neither.

### Visualization (canvas `c2`, 720×300)

Timeline chart, one row per server, showing vote messages then replication acks on a shared 0–35ms axis, with dashed markers where each majority is reached.

- **Title (bold 15px, `#1a5276`, top center):** "One Failure, Two Majorities: Leader at 6ms, Commit at 26ms".
- **Axes:** row labels "S1".."S5" 12px `#444` at x=30; plot from x=90 to x=670 mapped to t = 0–35ms; server rows at y = 60, 100, 140, 180, 220 (S1..S5); light 1px `#e5e9ef` horizontal guide line per row; x tick labels "0ms", "10ms", "20ms", "30ms" (12px `#444`) below y=250.
- **Vote dots (blue `#2a78d6`, radius 6):** at (t=0, S3 row) labeled "self-vote" (11px), (t=4, S1 row) and (t=6, S2 row) labeled "vote" (11px mute `#6b7280`).
- **Leader marker:** vertical dashed `#2a78d6` (dash 4/3) line at t=6 from y=40 to y=245, bold 12px blue label "3/5 votes — S3 is leader (term 2)" at its top.
- **Append marker:** small blue square (10×10) at (t=20, S3 row) labeled "entry #7 sent" (11px `#6b7280`).
- **Ack dots (green `#008300`, radius 6):** at (t=24, S1 row), (t=26, S2 row), (t=29, S4 row) — the S4 dot outlined only (2px stroke, no fill) with 11px mute label "late ack".
- **Commit marker:** vertical dashed `#008300` (dash 4/3) line at t=26 from y=40 to y=245, bold 12px green label "3/5 copies — #7 committed" at its top.
- **Crashed row:** S5 row drawn with 12px `#e74c3c` label "crashed — never answers" at t≈12, its guide line dashed `#e5e9ef`.
- **Annotation (bold 13px violet `#4a3aa7`, near t=31, y=170):** "stragglers never delay the majority".
- **Caption (12px `#444`, bottom right):** "message timings illustrative".

## The Quiet Layer Under Kubernetes and Kafka

**Tags:** `where it's used` (blue), `coordination` (green), `2f+1` (orange)

- **Config stores** — etcd and ZooKeeper are consensus clusters; Kubernetes keeps its cluster state in etcd
- **Controllers** — Kafka elects its controller through the same machinery, so brokers agree on who leads
- **Locks and leases** — a distributed lock is just consensus on "who holds this lock right now"
- **The formula** — tolerating f crashed servers takes 2f+1 total: 3 survive 1, 5 survive 2, 7 survive 3
- **The cost curve** — each extra failure tolerated costs two more servers and a slightly bigger vote

*Example (italic):* A 5-node etcd cluster keeps a Kubernetes control plane running through 2 simultaneous machine failures.

**Key point:** Any system that must agree on one small fact — who is leader, what the config says, who holds the lock — sits on a consensus cluster sized by the 2f+1 rule.

### Visualization (canvas `c3`, 720×300)

Icon-row diagram of the 2f+1 rule: three cluster sizes drawn as rows of server squares, with the maximum tolerable crashes marked red and the surviving majority green.

- **Title (bold 15px, `#1a5276`, top center):** "2f+1: How Many Servers Buy How Many Failures".
- **Rows (top to bottom at y = 75, 145, 215), each with a left-aligned 12px `#444` label at x=20:**
  - "3 nodes — tolerates 1": 3 squares starting at x=220, of which 1 red
  - "5 nodes — tolerates 2": 5 squares starting at x=220, of which 2 red
  - "7 nodes — tolerates 3": 7 squares starting at x=220, of which 3 red
- **Square style:** 44×44, 14px gap, 6px radius; surviving squares fill `rgba(0,131,0,0.15)` with 2px `#008300` border and 13px green "✓"; crashed squares (rightmost in each row) fill `rgba(231,76,60,0.12)` with 2px `#e74c3c` border and 13px red "✗".
- **Majority counts:** 12px `#008300` label at each row's right end: "2 alive", "3 alive", "4 alive".
- **Annotation (bold 13px aqua `#199e70`, near x=460, y=270):** "the majority survives in every row — f can die out of 2f+1".
- **Caption (12px `#444`, bottom right):** "crash counts exact for the 2f+1 rule".

## Six Servers Are No Safer Than Five

**Tags:** `common mistake` (red), `even clusters` (orange), `latency` (blue)

- **The instinct** — "more servers, more safety", so someone grows the cluster from 5 to 6 nodes
- **The math** — 6 nodes need a majority of 4, so 6 still tolerates only 2 crashes — same as 5
- **The downside** — the 6th server adds cost and a bigger voting round for zero extra tolerance
- **The tie risk** — even clusters can split 3–3 in an election, forcing timeout-and-retry rounds
- **Not fast** — every commit waits a network round trip to a majority; consensus buys safety, not speed
- **Right-sizing** — real clusters run 3, 5, or rarely 7 nodes; keep the fast path out of the quorum

*Example (italic):* A team grows etcd from 5 to 6 nodes for "extra safety" and gains nothing — the majority rises from 3 to 4, and 3 simultaneous crashes still kill both clusters.

**Common mistake:** Adding an even node to a consensus cluster. Fault tolerance only steps up at odd sizes (2f+1): 4 nodes buy what 3 do, 6 buy what 5 do — an extra machine, a bigger quorum, no extra safety.

### Visualization (canvas `c4`, 720×300)

Bar chart of failures tolerated by cluster size 3–7, with even sizes colored as wasted steps on a staircase that only rises at odd sizes.

- **Title (bold 15px, `#1a5276`, top center):** "Failures Tolerated by Cluster Size: the Staircase Rises Only at Odd Sizes".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; y = failures tolerated 0 to 3, gridlines `#e5e9ef` at 1/2/3 with 12px `#444` tick labels; x categories "3 nodes".."7 nodes", 12px `#444` labels under the bars.
- **Bars:** width 70, centered at x = 130, 250, 370, 490, 610; cluster sizes `[3, 4, 5, 6, 7]`, failures tolerated `[1, 1, 2, 2, 3]`; odd sizes (3, 5, 7) fill `rgba(42,120,214,0.30)` with 2px `#2a78d6` border; even sizes (4, 6) fill `rgba(217,89,38,0.25)` with 2px `#d95926` border.
- **Bar-top labels:** bold 12px in the bar's border color: "1", "1 — same as 3", "2", "2 — same as 5", "3" (the "same as" labels sit above the orange bars).
- **Quorum labels:** 11px `#6b7280` inside each bar's base: "needs 2", "needs 3", "needs 3", "needs 4", "needs 4".
- **Annotation (bold 13px magenta `#d55181`, near x=200, y=70):** "an even node = more cost, bigger quorum, zero extra safety".
- **Caption (12px `#444`, bottom right):** "tolerances exact from the 2f+1 rule; every commit still costs a round trip to a majority".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); the worked-example timings (votes at 0/4/6ms, leader at 6ms, append at 20ms, acks at 24/26/29ms, commit at 26ms) and the crash pattern in c1 are invented and labeled illustrative; the 2f+1 numbers (sizes `[3,4,5,6,7]`, failures tolerated `[1,1,2,2,3]`, quorums `[2,3,3,4,4]`) are exact consequences of majority arithmetic.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
