# Total Order Broadcast

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Total Order Broadcast

**Subtitle:** Deliver every message to every replica in the exact same order — a primitive so strong it is provably equivalent to consensus

## Three Bank Replicas, Two Transfers, Two Different Answers

**Tags:** `core idea` (blue), `replication` (green), `ordering` (orange)

- **The setup** — a bank keeps account A on three replicas; A starts at $100, B and C start at $0
- **Two transfers** — T1 moves $80 from A to B; T2 moves $50 from A to C; overdrafts are rejected
- **The race** — T1 and T2 arrive over the network, and each replica applies them in arrival order
- **The split** — replicas 1 and 3 get T1 first (A=$20, T2 rejected); replica 2 gets T2 first (A=$50)
- **The fix** — total order broadcast makes all three agree on one sequence before applying anything
- **The result** — with the agreed order T1 then T2, every replica lands on A=$20, B=$80, C=$0

*Example (italic):* Replica 2 swears A holds $50 and paid C, while replicas 1 and 3 swear A holds $20 and paid B — the same bank, three machines, two irreconcilable histories.

**Key point:** Replicas diverge not because messages were lost but because they were applied in different orders — total order broadcast removes exactly that freedom.

### Visualization (canvas `c1`, 720×300)

Two-panel grouped bar chart of account A's final balance on each replica: left panel "arrival order" (bars diverge), right panel "agreed order" (bars identical).

- **Title (bold 15px, `#1a5276`, top center):** "Same Two Transfers, Different Orders: Account A Ends at $20 or $50".
- **Panels:** left panel x=60–340 labeled "each replica applies in arrival order" (12px `#444` under title), right panel x=400–680 labeled "total order broadcast: agreed T1 → T2"; shared baseline y=245, plot height 170; y = dollars 0 to 100, gridlines `#e5e9ef` at 25/50/75 with 12px `#6b7280` labels on the left edge.
- **Left bars (replicas R1, R2, R3 at x = 100, 185, 270, width 55):** values `[20, 50, 20]`; R1 and R3 blue `#2a78d6`, R2 orange `#d95926`; bold 13px value labels "$20" / "$50" / "$20" above each bar; 12px `#444` labels "R1", "R2", "R3" below the baseline.
- **Right bars (same x offsets at 440, 525, 610, width 55):** values `[20, 20, 20]`, all green `#008300`, value labels "$20" above each; same replica labels below.
- **Divider:** vertical dashed `#6b7280` (dash 4/3) line at x=370 from y=60 to y=245.
- **Annotation (bold 13px orange `#d95926`, over the left panel near y=70):** "R2 disagrees by $30 — replicas have diverged".
- **Caption (12px `#444`, bottom right):** "balances illustrative".

## Two Guarantees, One Deterministic Machine

**Tags:** `worked example` (blue), `state machine replication` (green)

- **Guarantee 1: reliable delivery** — if any replica delivers a message, every replica delivers it
- **Guarantee 2: total order** — all replicas deliver the messages in exactly the same sequence
- **The log** — the agreed sequence is a numbered log: slot 1 holds T1, slot 2 holds T2
- **Slot 1 replay** — T1 moves $80: A goes 100 → 20, B goes 0 → 80, on every replica
- **Slot 2 replay** — T2 asks A for $50 but A holds $20, so all three replicas reject it identically
- **The theorem** — same start state + same deterministic ops + same order = same end state, always

*Example (italic):* Any replica that starts at A=$100, B=$0, C=$0 and replays slot 1 then slot 2 computes A=$20, B=$80, C=$0 — no coordination needed beyond the order itself.

**Key point:** This is state-machine replication: once the order is fixed, replicas never compare answers — determinism guarantees they all derive the same state from the same log.

### Visualization (canvas `c2`, 720×300)

Shared-log diagram: two log slots at the top feed three replica lanes below, each lane showing the identical balance trajectory for account A.

- **Title (bold 15px, `#1a5276`, top center):** "One Agreed Log, Three Identical Replays".
- **Log slots (top, y=60):** two rounded boxes 150px wide, 34px tall, 8px radius at x=180 and x=390; slot 1 fill `rgba(42,120,214,0.15)` with 12px `#2c3e50` text "slot 1: T1  A→B $80"; slot 2 fill `rgba(201,133,0,0.15)` with text "slot 2: T2  A→C $50 (rejected)"; 11px `#6b7280` label "agreed log" at x=60, y=80.
- **Replica lanes (y = 140, 190, 240):** 12px `#444` labels "R1", "R2", "R3" at x=30; each lane a horizontal 2px `#e5e9ef` rule from x=60 to x=680.
- **Balance steps per lane (identical for all three):** three bold 13px markers on each lane at x = 150, 360, 570 reading "A=$100" (mute `#6b7280`), "A=$20" (blue `#2a78d6`), "A=$20" (blue `#2a78d6`), joined by 2px `#2a78d6` arrows; the last marker gets an 11px `#c98500` note "T2 rejected" beneath it on lane R1 only.
- **Feed arrows:** thin dashed `#6b7280` arrows from each log slot down to the x=360 and x=570 marker columns.
- **Annotation (bold 13px green `#008300`, right side near y=165):** "three replays, one answer: A=$20, B=$80".
- **Caption (12px `#444`, bottom right):** "balances illustrative; replay logic exact".

## The Log Inside Kafka, Raft, and Every Replicated Database

**Tags:** `where it's used` (blue), `consensus` (orange), `replicated log` (green)

- **Replicated log** — append-only log where all nodes see the same entries in the same slots is TOB by another name
- **Kafka partition** — a single partition hands every consumer the same total order, so all derive the same state
- **Consensus per slot** — deciding which message occupies slot 1 is exactly one consensus decision
- **The reduction** — a consensus box can build TOB (one decision per slot); TOB can solve consensus (deliver, adopt the first)
- **Raft and ZAB** — Raft's log and ZooKeeper's atomic broadcast are production total order broadcast
- **The price** — since TOB equals consensus, it inherits consensus's cost: no async guarantee, leader bottlenecks

*Example (italic):* Two consumers replaying the same Kafka partition from offset 0 build byte-identical tables — the partition's total order is doing the state-machine-replication work for them.

**Key point:** Total order broadcast and consensus are reducible to each other — anything that gives you an agreed log has already paid the full price of consensus, and delivers its full power.

### Visualization (canvas `c3`, 720×300)

Equivalence diagram: "Total order broadcast" and "Consensus" boxes joined by two labeled arrows, above a log strip where each slot is one consensus decision.

- **Title (bold 15px, `#1a5276`, top center):** "Two Names for the Same Problem".
- **Top boxes (y=70):** rounded box 210px wide, 46px tall, 8px radius at x=90 labeled "Total order broadcast" (bold 13px `#1a5276`, fill `rgba(42,120,214,0.15)`); matching box at x=420 labeled "Consensus" (fill `rgba(0,131,0,0.12)`).
- **Arrows between boxes:** 3px `#008300` arrow left-to-right along y=80 labeled 11px `#008300` "deliver messages, adopt the first delivered"; 3px `#2a78d6` arrow right-to-left along y=105 labeled 11px `#2a78d6` "run one consensus decision per log slot".
- **Log strip (y=185):** five adjacent boxes 100px wide, 40px tall starting at x=110, fills alternating `rgba(42,120,214,0.15)` / `rgba(74,58,167,0.12)`, 12px `#2c3e50` labels "slot 1: T1", "slot 2: T2", "slot 3: T3", "slot 4: T4", "slot 5: ?"; 11px `#6b7280` caption "each slot = one consensus decision" centered beneath at y=245.
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=270):** "solve one and you have solved the other — for the same cost".
- **Caption (12px `#444`, bottom right):** "diagram schematic".

## FIFO per Sender Is Not a Total Order

**Tags:** `common mistake` (red), `ordering` (orange)

- **The confusion** — "TCP already delivers my messages in order" only orders one sender's own messages
- **FIFO order** — teller X's messages arrive in X's send order; it says nothing about X versus Y
- **The interleave** — X sends T1, Y sends T2; replica R1 sees T1 then T2, replica R2 sees T2 then T1
- **Both legal** — each replica respected every per-sender FIFO rule, yet they applied different orders
- **Causal order too** — causal delivery also leaves concurrent, unrelated messages free to interleave
- **What TOB adds** — it forces even concurrent messages from different senders into one shared sequence

*Example (italic):* With per-sender FIFO alone, R1 computes A=$20 (T1 won) while R2 computes A=$50 (T2 won) — the divergence from the opening example, reproduced with zero reordering per sender.

**Common mistake:** Assuming per-connection ordering (TCP, FIFO queues) gives replicas a shared order. The hard part of total order broadcast is ordering across senders — and that is precisely the consensus-hard part.

### Visualization (canvas `c4`, 720×300)

Message-flow diagram: two senders at the top, two replicas below, delivery arrows crossing so each replica receives T1 and T2 in a different interleaving — both FIFO-legal.

- **Title (bold 15px, `#1a5276`, top center):** "FIFO-Legal at Every Sender, Divergent at Every Replica".
- **Sender boxes (y=60):** rounded box 160px wide, 36px tall at x=140 labeled "teller X sends T1" (fill `rgba(42,120,214,0.15)`, 12px `#2c3e50`); box at x=420 labeled "teller Y sends T2" (fill `rgba(201,133,0,0.15)`).
- **Replica boxes (y=190):** box 190px wide, 44px tall at x=110 labeled "R1 delivers: T1, T2" with second line bold 12px blue `#2a78d6` "A=$20"; box at x=420 labeled "R2 delivers: T2, T1" with second line bold 12px orange `#d95926` "A=$50".
- **Arrows:** 3px `#2a78d6` arrows from teller X box to both replica boxes; 3px `#c98500` arrows from teller Y box to both replica boxes — the X→R2 and Y→R1 arrows cross in the middle.
- **Check marks:** 12px `#008300` "FIFO respected ✓" beside each replica box (x≈310, y=190 and x≈620, y=190).
- **Annotation (bold 13px red `#e74c3c`, centered near y=270):** "no per-sender rule was broken, yet R1 and R2 disagree by $30".
- **Caption (12px `#444`, bottom right):** "balances illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all values are the hardcoded numbers above (no randomness); starting balances A=$100, B=$0, C=$0, transfers T1=$80 A→B and T2=$50 A→C with overdraft rejection are invented and labeled illustrative; the divergent outcomes (`[20, 50, 20]` in arrival order vs `[20, 20, 20]` under the agreed order T1→T2, and the R1=$20 / R2=$50 split in c4) follow exactly from those rules; the TOB↔consensus reduction in c3 is standard theory, not data.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
