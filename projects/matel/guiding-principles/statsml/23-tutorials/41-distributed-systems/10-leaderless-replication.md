# Leaderless Replication

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Leaderless Replication

**Subtitle:** Instead of one leader node taking every write, any replica accepts them — overlapping read and write quorums replace the leader

## The Orders Table With No Boss Node

**Tags:** `core idea` (blue), `no single leader` (green), `Dynamo` (orange)

- **The setup** — a coffee chain's orders table is copied onto three nodes: A, B, and C
- **No leader** — no node is in charge; a client can send its write to any of the three
- **The write** — order #4127 (one latte) is sent to all three and succeeds once 2 of 3 confirm
- **The miss** — node C is slow and never applies the write; A and B hold it, C does not
- **The read** — a later read asks 2 of 3 nodes and keeps the answer with the newest version

*Example (italic):* Order #4127 lands on A and B at version 9; C stays at version 8, yet the write already counts as a success.

**Key point:** Leaderless replication has no failover to wait for — every node accepts reads and writes, and overlap between the two quorums does the leader's job.

### Visualization (canvas `c1`, 720×300)

Flow diagram: one client write fanning out to three replica nodes; two acknowledge, one misses, and the write still succeeds.

- **Title (bold 15px, `#1a5276`, top center):** "One Write, Three Replicas: Success at 2 of 3 Acks".
- **Client box:** rounded box at x=60, y=125, 150×50, fill `rgba(42,120,214,0.15)`, 12px `#2c3e50` text "client — write order #4127 (latte, v9)".
- **Node boxes (x=430, 210×46 each):** node A at y=55 and node B at y=125, both green `#008300` border, fill `rgba(0,131,0,0.12)`, labels "node A — ack, now v9" / "node B — ack, now v9" with bold 12px green "✓"; node C at y=195, mute `#6b7280` dashed border, fill `rgba(107,114,128,0.10)`, label "node C — no ack, still v8".
- **Arrows:** 3px `#2a78d6` arrows from the client box to A and B; 2px dashed `#6b7280` (dash 4/3) arrow to C.
- **Annotation (bold 13px green `#008300`, near x=430, y=270):** "2 acks ≥ W=2 — the write succeeds without C".
- **Caption (12px `#444`, bottom right):** "order and version numbers illustrative".

## N=3, W=2, R=2: Why the Sets Must Touch

**Tags:** `worked example` (blue), `quorum math` (green), `read repair` (orange)

- **The rule** — with N=3 copies, write to W=2 and read from R=2; since 2+2 > 3, the sets overlap
- **Hand-check** — the possible read pairs are {A,B}, {A,C}, {B,C}; every pair contains A or B
- **The stale node** — C still answers with version 8; version numbers let the client spot it
- **The pick** — a read of {A,C} returns v9 and v8; the client keeps v9, the newest
- **Read repair** — the reader writes v9 back to C on the spot, healing the stale copy
- **Hinted handoff** — if C is down at write time, neighbor D holds the write and delivers it later

*Example (italic):* Reading {A, C} yields v9 from A and v8 from C — the client answers with v9 and quietly upgrades C to v9.

**Key point:** W + R > N forces every read quorum to include at least one node that saw the latest write; read repair and hinted handoff clean up the stragglers.

### Visualization (canvas `c2`, 720×300)

Overlap diagram: three node boxes with their versions, a write-set bracket over {A,B}, a read-set bracket under {A,C}, and a read-repair arrow fixing C.

- **Title (bold 15px, `#1a5276`, top center):** "W+R > N: Every Read Pair Hits a Fresh Copy".
- **Node boxes (y=125, 120×50 each, 8px radius, 13px `#2c3e50` text):** "A — v9" at x=140 and "B — v9" at x=320, fill `rgba(0,131,0,0.12)` with 2px `#008300` border; "C — v8" at x=500, fill `rgba(107,114,128,0.10)` with 2px `#6b7280` border.
- **Write bracket (green `#008300`, 3px):** horizontal bracket at y=95 spanning x=140 to x=440 (over A and B), bold 12px green label "write set W=2 → wrote v9" centered above at y=80.
- **Read bracket (blue `#2a78d6`, 3px):** horizontal bracket at y=205 spanning x=140 to x=260 and a dashed 2px blue connector continuing to x=620 (under A and C, skipping B), bold 12px blue label "read set R=2 → sees v9 and v8" centered at y=228.
- **Overlap highlight:** 3px `#4a3aa7` outline around box A, bold 12px violet `#4a3aa7` label "in both sets" above-left of A at y=60.
- **Read-repair arrow:** 2px dashed `#d95926` (dash 4/3) curved arrow from box A to box C along y≈188, 12px `#d95926` label "read repair: push v9 to C".
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=270):** "2 + 2 > 3 — the read cannot dodge every fresh node".
- **Caption (12px `#444`, bottom right):** "versions illustrative; quorum arithmetic exact".

## Built for Staying Up: Dynamo, Cassandra, Riak

**Tags:** `where it's used` (blue), `availability` (green)

- **The paper** — Amazon's 2007 Dynamo paper described this design for its shopping-cart store
- **The descendants** — Apache Cassandra and Riak built open-source stores on the same ideas
- **Leader-based cost** — when a leader dies, writes stall until a new leader is elected
- **Leaderless win** — with one of three nodes down, W=2 is still reachable; writes never pause
- **The trade** — you give up a single ordered history in exchange for staying writable

*Example (italic):* When node B dies at minute 3, a leader-based store rejects writes for a ~30-second failover; the leaderless store never blinks.

**Key point:** Leaderless replication trades strict ordering for availability — the Dynamo lineage chose staying writable through failures over keeping one authoritative copy.

### Visualization (canvas `c3`, 720×300)

Timeline chart comparing accepted writes per second during a node failure: leader-based (stalls during failover) vs leaderless (stays level), on a shared time axis.

- **Title (bold 15px, `#1a5276`, top center):** "Node Failure at Minute 3: Failover Stall vs No Stall".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = time 0 to 10 minutes with 12px `#444` tick labels every 2 minutes; y = accepted writes/sec 0 to 600, gridlines `#e5e9ef` at 150/300/450.
- **Leader-based line:** red `#e74c3c` 3px line through minutes `[0, 2, 3, 3.1, 3.5, 3.6, 4, 6, 8, 10]`, writes/sec `[500, 505, 498, 0, 0, 490, 496, 502, 499, 503]` — vertical cliff to 0 at minute 3, flat at 0 through the ~30s election, recovery at 3.6.
- **Leaderless line:** green `#008300` 3px line through the same minute grid, writes/sec `[500, 505, 498, 496, 494, 497, 501, 499, 502, 503]` — flat.
- **Failure marker:** vertical dashed `#6b7280` (dash 4/3) line at minute 3, 12px `#6b7280` label "node B fails" at its top.
- **Legend (12px, upper right):** red swatch "leader-based", green swatch "leaderless (N=3, W=2)".
- **Annotation (bold 13px green `#008300`, near minute 6, y=90):** "W=2 still reachable — writes never stop".
- **Caption (12px `#444`, bottom right):** "write rates illustrative; failover ~30s".

## Overlap Isn't Consensus: Conflicts Still Happen

**Tags:** `common mistake` (red), `write conflicts` (orange)

- **The trap** — believing W+R > N alone makes the store behave like one single machine
- **Sloppy quorum** — during an outage, writes may land on stand-in nodes the read set never asks
- **Concurrent writes** — two clients can update order #4127 on different nodes at the same instant
- **Last-write-wins** — resolving by timestamp silently throws away one client's change
- **Siblings** — Dynamo-style stores keep both versions and make the application merge them

*Example (italic):* Client 1 adds "oat milk" via node A while client 2 adds "extra shot" via node C; last-write-wins keeps one edit, a merge keeps both.

**Common mistake:** Treating a quorum as a lock. Quorums guarantee a read sees some latest write — they do not order concurrent writes, so conflict handling (merges, vector clocks) is still your job.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: the same concurrent edits resolved by last-write-wins (one edit lost) vs sibling merge (both edits kept).

- **Title (bold 15px, `#1a5276`, top center):** "Two Clients, Same Order, Same Instant: Who Wins?".
- **Row 1 (y=95), label 12px `#444` at x=20:** "last-write-wins"; two blue `#2a78d6` rounded boxes stacked at x=170 (y=72 and y=118, 170×36) labeled "client 1: +oat milk (via A)" and "client 2: +extra shot (via C)", 3px arrows converging to a red `#e74c3c` box at x=450 (170×40) labeled "kept: extra shot" with bold 12px red "✗ oat milk lost".
- **Row 2 (y=205), label:** "sibling merge"; the same two blue boxes at x=170 (y=182 and y=228), arrows converging to a green `#008300` box at x=450 (200×40) labeled "kept: oat milk + extra shot" with bold 12px green "✓ both edits survive".
- **Box style:** 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(231,76,60,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px orange `#d95926`, centered near y=280):** "the quorum delivered both writes — merging them is the app's job".
- **Caption (12px `#444`, bottom right):** "order edits illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); the order number (#4127), version numbers (v8/v9), and writes/sec series are invented and labeled illustrative; the quorum arithmetic (N=3, W=2, R=2, W+R=4 > 3, read pairs {A,B}/{A,C}/{B,C}) is exact, and the Dynamo paper (Amazon, 2007) with Cassandra/Riak descendants is published fact.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
