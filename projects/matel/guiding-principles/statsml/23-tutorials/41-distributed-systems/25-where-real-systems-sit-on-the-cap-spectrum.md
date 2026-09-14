# Where Real Systems Sit on the CAP Spectrum

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column `table.layout`, text left 50% / canvas right 50%)
**HTML title tag:** Where Real Systems Sit on the CAP Spectrum

**Subtitle:** CAP is not "pick 2 of 3" — a partition is weather, not a design option, so every system only really chooses what to do when the network splits, and what to pay when it does not

## The Only Choice a Partition Gives You

**Tags:** `core idea` (blue), `not pick 2 of 3` (red), `PACELC` (orange)

- **The setup** — one value lives on 5 servers; a switch fails and cuts them into a group of 3 and a group of 2
- **No third door** — a client talking to the small side asks to write; the system answers or it refuses
- **Refuse the write (CP)** — the small side says "no leader, try later": consistent, but this client is down
- **Accept the write (AP)** — the small side stores it anyway: available, but two sides now disagree
- **Why "CA" is empty** — dropping P means "we stop when the wire breaks", which is just choosing C
- **The bigger half of the bill (PACELC)** — partitions are rare, so the everyday trade is latency vs consistency
- **Read a system twice** — once for "P → A or C", once for "Else → L or C"; most labels only state the first

*Example (italic):* A 5-node cluster split 3 | 2 has no way to let the 2-node side accept a write and still promise one global value.

**Key point:** You never choose P. You choose what happens on the losing side of a split — and, far more often, whether every normal read pays for a round trip to the leader.

### Visualization (canvas `c1`, 720×300)

A split cluster on the left, the two possible answers on the right.

- **Title (bold 15px `#1a5276`, centered at y=24):** "A Partition Offers Two Answers, Never Three".
- **Split cluster (left block, x 40–330):** five circles radius 20, white fill, 2px stroke. Majority side `#2a78d6` at (95,110), (160,90), (128,165) labeled A, B, C; minority side `#d95926` at (270,110), (295,170) labeled D, E.
- **Partition line:** 3px dashed (dash 8/6) `#e74c3c` vertical zigzag from (212,60) to (212,250); bold 12px red label "network split" rotated horizontally above at (212,50), centered.
- **Side labels (bold 12px, centered):** "majority — 3 nodes" in `#2a78d6` at (128,215); "minority — 2 nodes" in `#d95926` at (283,215).
- **Client arrow:** gray `#6b7280` 2px arrow from (283,255) up to (283,200) with 11px label "write?" at (283,272), centered.
- **Two answer boxes (right block, x 380–690):** rounded rects 300×74, radius 8, 2px stroke, 1px matching tinted fill.
  - Upper box at (380,80): stroke `#008300`, fill `rgba(0,131,0,0.08)`; bold 13px green title "CP — refuse the write" at (395,105) left-aligned; 12px `#2c3e50` lines "one value stays true everywhere" at (395,127) and "this client sees an error" at (395,145).
  - Lower box at (380,180): stroke `#c98500`, fill `rgba(201,133,0,0.10)`; bold 13px `#c98500` title "AP — accept the write" at (395,205); 12px lines "every client is served" at (395,227) and "the two sides now disagree" at (395,245).
- **Struck-out third option:** 12px `#6b7280` text "CA — keep both, ignore the split" centered at (535,285) with a 1.5px `#e74c3c` line drawn through it.

## Coordination Services: Consistency Is the Product

**Tags:** `ZooKeeper & etcd` (blue), `CP` (green), `stale local reads` (red)

- **What they store** — tiny critical facts: who is the leader, which shard is where, is the lock held
- **Writes need a majority** — a write commits only once ⌊N/2⌋+1 of the nodes log it (ZAB in one, Raft in the other)
- **The minority stops** — on a 3 | 2 split the 2-node side has no majority and rejects every write
- **Why that is correct** — a lock service that hands the same lock to both sides of a split is worse than one that pauses
- **The read footnote** — ZooKeeper reads answer from whatever node you connected to, so they can be stale
- **Paying for a fresh read** — `sync()` first in ZooKeeper, or a leader-confirmed read in etcd, buys recency for a round trip
- **The everyday cost** — small quorum, small data, but each write is a majority round trip, so throughput is modest

*Example (italic):* A node cut off from the majority still answers "the leader is A" long after the majority elected B — the answer is old, not wrong-for-its-time.

**Key point:** These systems are CP on purpose: they exist so that everything else can assume one answer. The subtlety is that a fast local read is not automatically a current one.

### Visualization (canvas `c2`, 720×300)

Two lanes showing the same write request hitting the majority side and the minority side of a 3 | 2 split.

- **Title (bold 15px `#1a5276`, centered at y=24):** "Majority Commits, Minority Refuses".
- **Majority lane (y=100):** three node circles radius 22, 2px `#2a78d6` stroke, white fill, at x=140, 215, 290, labeled A(leader), B, C — leader marked with a filled `#2a78d6` ring 26px and white bold letter. Green `#008300` check at (350,100) radius 12 with bold 13px green label "committed" at (400,105), left-aligned.
- **Lane label (bold 12px `#2a78d6`, left-aligned at (40,105)):** "3 of 5".
- **Minority lane (y=210):** two node circles radius 22, 2px `#d95926` stroke at x=140, 215, labeled D, E. Red `#e74c3c` X at (350,210) size 12 with bold 13px red label "rejected — no majority" at (400,215), left-aligned.
- **Lane label (bold 12px `#d95926`, left-aligned at (40,215)):** "2 of 5".
- **Quorum bar (right, x 400–690, y 140–165):** background bar `#e5e9ef` height 16 representing 5 nodes as 5 equal cells with 2px white gaps; first 3 cells filled `#2a78d6`, last 2 filled `#d95926`; bold 12px `#1a5276` label above at (545,132) centered: "quorum line at 3 of 5".
- **Annotation (bold 12px `#d95926`, centered at (545,255)):** "a local read on D still returns the old leader".
- **Caption (12px `#444`, bottom right at (690,288), right-aligned):** "writes: majority round trip · reads: local unless you ask".

## Global SQL: Buy Consistency With Time

**Tags:** `Spanner` (blue), `CP + external consistency` (green), `clock uncertainty` (orange)

- **The claim** — a globally distributed SQL database whose transactions look like one machine executed them in order
- **How** — Paxos per shard, two-phase commit across shards, and timestamps from a clock service with known error bars
- **The clock trick** — `TrueTime` never returns a point, it returns an interval that is guaranteed to contain the true time
- **Commit wait** — a writer picks a timestamp, then waits until that instant is certainly past before releasing the commit
- **What that buys** — any transaction starting later gets a larger timestamp, so ordering is real, not merely plausible
- **What it costs** — every commit pays the clock uncertainty as latency; that is the "EC" half of PACELC, always on
- **Still CP** — a shard whose replicas lose their majority stops accepting writes; high availability comes from rare partitions, not from choosing A

*Example (italic):* If the clock interval is 6 ms wide, the commit waits out those 6 ms before acknowledging — the price of an order nobody can dispute.

**Key point:** Spanner does not beat CAP. It stays CP and makes partitions rare enough that CP feels available, paying for strictness in per-commit latency.

### Visualization (canvas `c3`, 720×300)

A timeline of one commit showing the clock-uncertainty band and the commit-wait gap. Illustrative widths.

- **Title (bold 15px `#1a5276`, centered at y=24):** "Commit Wait: Pay Clock Uncertainty as Latency (illustrative)".
- **Time axis:** 2px `#1a5276` line at y=215 from x=90 to x=680 with arrowhead right; 12px `#6b7280` label "time" at (680,238), right-aligned; tick marks every 98px with 11px labels "0", "2 ms", "4 ms", "6 ms", "8 ms", "10 ms", "12 ms".
- **Uncertainty band:** rect from x=188 to x=482, y 100–150, fill `rgba(201,133,0,0.16)`, 1.5px `#c98500` stroke; bold 12px `#c98500` centered label "TrueTime interval — true now is somewhere in here" at (335,90).
- **Chosen timestamp:** 2.5px `#4a3aa7` vertical line at x=482 from y=95 to y=225; bold 12px violet label "chosen commit timestamp" at (482,75), centered.
- **Write arrival:** 2.5px `#2a78d6` vertical line at x=188 from y=95 to y=225; bold 12px blue label "write ready" at (188,75), centered.
- **Commit-wait bracket:** horizontal 2px `#d95926` line at y=175 from x=188 to x=482 with 6px end ticks; bold 13px `#d95926` centered label "commit wait — 6 ms of doing nothing" at (335,168) placed just above the line.
- **Acknowledgement:** green `#008300` filled dot radius 6 at (482,215) with bold 12px green label "ack released" at (520,205), left-aligned.
- **Later transaction:** 2px dashed `#008300` vertical line at x=580 with 12px green label "any later txn gets a larger timestamp" at (580,262), centered.
- **Caption (12px `#444`, at (90,288), left-aligned):** "interval width is the configured clock error bound, not a measurement".

## Object Storage: Strong per Key, Eventual per Region

**Tags:** `S3` (blue), `read-after-write` (green), `async replication` (orange)

- **The old behaviour** — for years a `PUT` overwriting an existing key could be followed by a `GET` returning the old bytes
- **What changed in 2020** — reads of a single object became strongly consistent: after a successful `PUT`, every `GET` sees it
- **Listings too** — a bucket listing now reflects the writes that already succeeded, which it previously might not
- **What did not change** — there is still no transaction across two objects, so a pair of keys can be seen half-updated
- **Cross-region is still eventual** — replication to another region is asynchronous, so the copy lags by seconds or more
- **A read is not a lock** — two writers overwriting the same key still race; the service picks one, and the loser vanishes silently
- **Where pipelines break** — a manifest file listing parts written a moment ago is safe; a manifest replicated to another region is not

*Example (italic):* Write `part-001` then `manifest.json`; a reader in the same region always sees both, a reader in the mirrored region may see the manifest before the part.

**Key point:** "Strongly consistent" here means per object, per region. Cross-object and cross-region are both still eventual, and that is where the surprises live.

### Visualization (canvas `c4`, 720×300)

Three lanes: the old eventual window, the current strong read, and the still-eventual cross-region copy.

- **Title (bold 15px `#1a5276`, centered at y=24):** "Same Key, Three Different Promises".
- **Lanes:** three horizontal `#e5e9ef` 2px baselines at y=95, 165, 235, from x=210 to x=680; right-aligned 12px `#2c3e50` labels at x=200: "then: overwrite GET", "now: same-region GET", "always: other-region GET".
- **Write marker:** 2.5px `#1a5276` vertical line at x=250 spanning y=70–255; bold 12px `#1a5276` label "PUT succeeds" at (250,58), centered.
- **Lane 1 (old):** 4px `#6b7280` segment from x=250 to x=420 then 4px `#008300` segment to x=680; 5px `#008300` dot at x=420; bold 12px `#6b7280` label "may return old bytes" at (335,84), centered.
- **Lane 2 (now):** 4px `#008300` segment from x=250 to x=680; bold 12px `#008300` label "new bytes immediately" at (400,154), centered; small 5px green dot at x=250.
- **Lane 3 (cross-region):** 4px `#6b7280` segment from x=250 to x=590 then 4px `#c98500` segment to x=680; 5px `#c98500` dot at x=590; bold 12px `#c98500` label "async replication lag" at (420,224), centered.
- **Shaded window:** rect x=250–590, y=210–255, fill `rgba(201,133,0,0.10)`, no stroke, drawn behind lane 3.
- **Annotation (bold 12px `#d95926`, centered at (450,278)):** "one object is safe — two objects are never atomic".
- **Caption (12px `#444`, at (40,288), left-aligned):** "lag shown illustratively".

## Tunable Reads: Pay More for Fresher

**Tags:** `DynamoDB` (blue), `read-your-choice` (green), `global tables` (orange)

- **Three copies** — every item is stored on three replicas in three failure zones of one region
- **The cheap read** — the default read answers from any replica: about half the price, and possibly one write behind
- **The strict read** — a strongly consistent read is served by the item's leader replica: current, dearer, and slower
- **Writes go to the leader** — a write is acknowledged after the leader plus enough peers have durably stored it
- **The knob is per call** — the same table can serve a stale-tolerant dashboard and a strict balance check
- **Global tables flip the model** — multi-region tables replicate asynchronously and settle conflicts by last writer wins
- **The lost update** — two regions writing the same item within the replication window leave exactly one survivor, no error

*Example (italic):* A read of a counter can legally return 41 while the leader holds 42 — unless you asked for, and paid for, the strong read.

**Key point:** Consistency here is a per-request purchase inside a region, and an unavoidable eventual model across regions.

### Visualization (canvas `c5`, 720×300)

One item on three replicas with two labelled read paths and a price/staleness contrast.

- **Title (bold 15px `#1a5276`, centered at y=24):** "Two Read Paths to the Same Item".
- **Replica circles:** radius 30, white fill, at (215,105) leader (3px `#2a78d6` stroke), (150,215) and (290,215) followers (2px `#6b7280` stroke); bold 13px labels "leader" `#2a78d6` and "replica" `#6b7280` centered inside at y offset −2, with a bold 14px version line under each: "v42" `#2a78d6` at (215,128), "v41" `#6b7280` at (150,238), "v42" `#6b7280` at (290,238).
- **Write arrow:** 2px `#1a5276` arrow from (215,55) to (215,72) with 12px `#1a5276` label "write v42" at (215,45), centered.
- **Replication arrows:** two 1.5px dashed `#6b7280` arrows from the leader to each follower.
- **Strong read path:** 2.5px `#008300` arrow from (430,120) to (255,110); bold 13px green label "strong read" at (440,112), left-aligned; 12px `#2c3e50` sub-line "leader only · full price · v42" at (440,133), left-aligned.
- **Eventual read path:** 2.5px `#c98500` arrow from (430,225) to (325,220); bold 13px `#c98500` label "default read" at (440,215), left-aligned; 12px sub-line "any replica · half price · v41 or v42" at (440,236), left-aligned.
- **Annotation (bold 12px `#d95926`, centered at (360,282)):** "across regions there is no strong option — last writer wins".
- **Caption (12px `#444`, at (40,58), left-aligned):** "3 replicas, one region".

## Wide-Column AP: The Dial Goes Both Ways

**Tags:** `Cassandra` (blue), `AP by default` (orange), `R + W > N` (green)

- **No leader** — any replica takes a write, so a partition cannot remove the node that writes must go through
- **Consistency level per query** — `ONE`, `QUORUM`, `ALL` chosen per statement, on both reads and writes
- **The strong corner** — `QUORUM` reads with `QUORUM` writes on 3 replicas gives 2 + 2 > 3, so reads see the newest write
- **Still not linearizable** — quorum overlap fixes staleness, not two conflicting writes racing at the same instant
- **Conflicts settle by timestamp** — last writer wins per column, so a clock skew silently picks the wrong winner
- **The stronger tool** — lightweight transactions run a Paxos round per partition key for compare-and-set, at real cost
- **Availability by design** — hinted handoff lets a stand-in hold a write for an unreachable node and deliver it later

*Example (italic):* On 3 replicas, `ONE` reads are fast and may be stale; switching both reads and writes to `QUORUM` closes the gap and roughly doubles the coordination.

**Key point:** An AP system with a consistency dial can be turned toward C per query — but the dial changes staleness and latency, not the underlying last-writer-wins conflict rule.

### Visualization (canvas `c6`, 720×300)

Grouped bars comparing consistency-level combinations on staleness risk and coordination cost, with the overlap rule marked.

- **Title (bold 15px `#1a5276`, centered at y=24):** "Turning the Dial on 3 Replicas (illustrative)".
- **Axes:** y axis 2px `#1a5276` from (110,60) to (110,235); x axis to (660,235); 12px `#6b7280` y-label rotated −90° "relative cost / risk" centered at (78,150); horizontal `#e5e9ef` gridlines at 25%, 50%, 75%, 100% of the 175px height with 11px right-aligned labels "low", "med", "high" at 25/55/85 percent heights.
- **Four groups** at x centres 175, 300, 425, 550, each two bars 34px wide with a 6px gap, drawn from the axis upward: coordination cost `#2a78d6`, staleness risk `#c98500`.
  - "W=ONE R=ONE": cost 0.18, staleness 0.92
  - "W=QUORUM R=ONE": cost 0.48, staleness 0.55
  - "W=QUORUM R=QUORUM": cost 0.66, staleness 0.10
  - "W=ALL R=ONE": cost 0.95, staleness 0.10
- **Group labels (12px `#2c3e50`, centered at y=252, two lines each):** the four labels above split across "W=…" / "R=…" lines.
- **Legend (top right, 12px):** 12×12 `#2a78d6` swatch + "coordination cost" at (470,50); 12×12 `#c98500` swatch + "staleness risk" at (470,68) — placed inside the plot's empty top-left region if clearer.
- **Overlap annotation:** bold 12px `#008300` label "2 + 2 > 3 — overlap reached" at (425,278), centered, with a 1.5px green bracket under the third group.
- **Caption (12px `#444`, at (110,278), left-aligned):** "bars are relative, not measured".

## In-Memory Replication: Neither Letter, Honestly

**Tags:** `Redis` (blue), `async failover` (red), `write loss window` (orange)

- **One writable node per shard** — clients write to a primary; replicas follow it asynchronously
- **The ack is local** — the primary answers "OK" before any replica has the write, which is why it is fast
- **The failover** — a supervisor promotes a replica when the primary looks dead, and the promoted node may be behind
- **Writes evaporate** — anything acknowledged but not yet replicated is gone, with no error ever surfaced to the writer
- **Not available either** — the detection-plus-promotion gap is a window where the shard accepts nothing
- **The partial fix** — a wait-for-replicas command blocks until N replicas confirm, trading latency for durability
- **The rule of thumb** — treat it as a cache whose contents may regress, not as a source of record

*Example (italic):* Increment a counter to 500, the primary dies a millisecond later, the promoted replica knows only 497 — three acknowledged increments never happened.

**Key point:** Asynchronous replication plus automatic failover is neither C nor A: it can drop acknowledged writes and still be unreachable during the switch.

### Visualization (canvas `c7`, 720×300)

A failover timeline with an acknowledged-but-lost write region and an unavailability gap.

- **Title (bold 15px `#1a5276`, centered at y=24):** "Acknowledged, Replicated, Lost".
- **Two lanes:** "primary" at y=105, "replica → new primary" at y=200, baselines 2px `#e5e9ef` from x=190 to x=680; right-aligned 12px `#2c3e50` labels at x=180.
- **Primary lane:** 4px `#2a78d6` segment from x=190 to x=430; five 5px `#2a78d6` dots at x=230, 270, 310, 350, 390 with a bold 12px blue label "acks 496…500" at (310,86), centered; red `#e74c3c` X size 12 at (430,105) with bold 12px red label "crash" at (430,80), centered.
- **Replica lane:** 4px `#6b7280` segment from x=190 to x=430; three 5px `#6b7280` dots at x=230, 270, 310 with 12px `#6b7280` label "replicated to 497" at (270,225), centered; then 4px `#008300` segment from x=520 to x=680 with a 6px green dot at x=520 and bold 12px green label "promoted, serving from 497" at (600,182), centered.
- **Lost-write band:** rect x=310–430, y=70–145, fill `rgba(231,76,60,0.12)`, 1.5px dashed `#e74c3c` stroke; bold 12px red label "3 acked writes lost" at (370,60), centered.
- **Unavailable band:** rect x=430–520, y=160–240, fill `rgba(201,133,0,0.14)`, 1.5px `#c98500` stroke; bold 12px `#c98500` rotated-free label "no writes accepted" placed at (475,255), centered, with a thin leader line to the band.
- **Time arrow:** 1.5px `#6b7280` arrow at y=275 from x=190 to x=680 with 12px centered label "time".

## Replicated Logs: Choose C, and Say So Loudly

**Tags:** `Kafka` (blue), `ISR` (green), `configured trade` (orange)

- **The unit** — a partition is an ordered log with one leader and a set of replicas that are caught up, the ISR
- **The durable ack** — with `acks=all` a produce request succeeds only after every in-sync replica has the record
- **The floor** — `min.insync.replicas=2` on 3 replicas means the leader refuses writes once the ISR drops to itself
- **That refusal is the C choice** — the producer sees an error instead of a write only one machine knows about
- **Consumers see less** — reads stop at the high watermark, the offset every in-sync replica already has
- **The AP switch** — allowing an out-of-sync replica to become leader restores writes and truncates committed records
- **So the letters are yours** — the same cluster is CP or AP depending on three settings nobody reads twice

*Example (italic):* Replication factor 3 with `min.insync.replicas=2` survives losing one replica; losing two stops producers rather than accepting a single-copy write.

**Key point:** Kafka does not have a CAP position — its configuration does. The default-looking settings and the safe settings are not the same settings.

### Visualization (canvas `c8`, 720×300)

A three-state strip showing ISR size against producer outcome, plus the unclean-election branch.

- **Title (bold 15px `#1a5276`, centered at y=24):** "ISR Size Decides Whether the Producer Is Served".
- **Three panels** side by side, each a rounded rect 200×140 at y=70, x=45, 260, 475, radius 8, 1.5px stroke, tinted fill:
  - Panel 1 (`#008300`, fill `rgba(0,131,0,0.07)`): three replica circles radius 16 at y=125 inside, all 2px green with letters L, R1, R2; bold 13px green header "ISR = 3" at panel top +22, centered; 12px `#2c3e50` line "produce succeeds" near the panel bottom.
  - Panel 2 (`#008300`, fill `rgba(0,131,0,0.07)`): L and R1 green, R2 drawn 2px dashed `#6b7280` and greyed; header "ISR = 2" green; bottom line "still succeeds — floor met".
  - Panel 3 (`#e74c3c`, fill `rgba(231,76,60,0.09)`): L green, R1 and R2 dashed grey; bold 13px red header "ISR = 1"; bottom line in red "produce fails — below floor".
- **Floor marker:** 2px dashed `#1a5276` horizontal line at y=228 across x=45–675 with bold 12px `#1a5276` label "min.insync.replicas = 2" at (45,246), left-aligned.
- **Unclean-election branch:** bold 12px `#d95926` text "unclean leader election = true → writes resume, committed records truncated" centered at (360,272).
- **Caption (12px `#444`, at (675,246), right-aligned):** "replication factor 3, acks=all".

## Single-Primary SQL: "CA" Means It Stops

**Tags:** `PostgreSQL` (blue), `no partition to tolerate` (red), `sync levels` (green)

- **One node, no theorem** — a single database has nothing to partition, so calling it CA says nothing useful
- **Add a standby** — streaming replication ships the write-ahead log to a replica that replays it
- **Asynchronous** — the primary acks first and ships after: fast, but a promotion after a crash can lose the tail
- **`remote_write`** — the standby confirms receipt; a standby crash right after can still lose it
- **`remote_apply`** — the standby confirms the change is visible there, so a reader on the standby cannot go backwards
- **The availability bite** — synchronous with one named standby means losing that standby freezes commits on the primary
- **Quorum synchronous** — require any 2 of 3 standbys and a single standby failure stops mattering
- **Reads on replicas lag** — an async standby answers with data older than the primary, which breaks read-your-writes

*Example (italic):* With one synchronous standby, taking that standby down for patching stops every commit on the primary until it returns or is de-listed.

**Key point:** The classic relational setup is not exempt from CAP; it faces the same choice, expressed as a replication-mode setting and a failover policy.

### Visualization (canvas `c9`, 720×300)

Three configurations compared on when the commit is acknowledged and what a failure costs.

- **Title (bold 15px `#1a5276`, centered at y=24):** "Where the Commit Is Acknowledged".
- **Three rows** at y=90, 160, 230; right-aligned 12px `#2c3e50` labels at x=175: "async", "sync: remote_write", "sync: remote_apply".
- **Each row:** a 2px `#e5e9ef` baseline from x=190 to x=600; a blue `#2a78d6` bar height 14 from x=190 to the ack point; a 6px `#008300` dot with bold 11px "ack" label above at the ack point.
  - async: ack at x=250
  - remote_write: ack at x=390
  - remote_apply: ack at x=520
- **Stage ticks:** 1.5px `#6b7280` vertical ticks with 11px `#6b7280` labels above the title row at y=62: "local WAL flush" at x=250, "standby received" at x=390, "standby applied" at x=520.
- **Cost column (right of x=610, 11px, centered per row):** "may lose tail" in `#e74c3c` at (650,90); "small loss window" in `#c98500` at (650,160); "no loss" in `#008300` at (650,230).
- **Availability annotation (bold 12px `#d95926`, centered at (400,272)):** "one named synchronous standby down = commits stop; quorum of 2 of 3 does not".
- **Caption (12px `#444`, at (190,272), left-aligned):** "bar length = wait before ack".

## In-Process Analytics: No Network, No Theorem

**Tags:** `DuckDB` (blue), `off the map` (red), `borrowed consistency` (orange)

- **Not a server** — it is a library linked into your process; a query and its data share one address space
- **Nothing to partition** — CAP needs two machines that can stop hearing each other, and there is only one here
- **Its own guarantees are local** — ACID transactions on its file, with a single writer process holding the lock
- **The multi-process rule** — one writer at a time; other processes attach read-only or wait, they do not coordinate
- **Where distribution sneaks in** — querying Parquet on object storage makes the network part of the query plan
- **Inherited, not owned** — reading remote files, your consistency is exactly the object store's, never better
- **The stale-cache trap** — a cached file handle or listing can serve last hour's partition with no error at all
- **The honest label** — a local engine reading a remote eventually-consistent surface, not a CP or AP system

*Example (italic):* Two DuckDB processes scanning the same S3 prefix a second apart can return different row counts — the engine is consistent, the prefix is not.

**Key point:** DuckDB has no CAP position because it has no partition to suffer. Point it at remote storage and it silently adopts that storage's position instead.

### Visualization (canvas `c10`, 720×300)

Two panels: the in-process case with no network to cut, and the remote-file case where the object store's consistency becomes the query's.

- **Title (bold 15px `#1a5276`, centered at y=24):** "The Network Only Appears When the Data Is Remote".
- **Left panel:** rounded rect 300×180 at (40,60), radius 8, 1.5px `#008300`, fill `rgba(0,131,0,0.06)`; bold 13px green header "local file — one process" at (190,86), centered. Inside, one rounded rect 200×54 at (90,105) labelled bold 12px `#2a78d6` "your process: app + DuckDB" over 11px `#6b7280` "one address space", and one 200×40 rect at (90,180) labelled bold 12px `#1a5276` "data.duckdb on local disk"; a 2px `#2a78d6` double-headed arrow between them at x=190. Bold 12px green label "no wire to cut — CAP does not apply" at (190,255), centered.
- **Right panel:** rounded rect 300×180 at (380,60), radius 8, 1.5px `#c98500`, fill `rgba(201,133,0,0.08)`; bold 13px `#c98500` header "remote Parquet — network in the plan" at (530,86), centered. Inside, a 200×54 rect at (430,105) labelled bold 12px `#2a78d6` "your process: app + DuckDB", and a 200×40 rect at (430,180) labelled bold 12px `#1a5276` "object store prefix".
- **Cut marker:** 3px dashed `#e74c3c` horizontal zigzag across the right panel at y=160 between the two inner boxes, with bold 12px red label "the wire" at (620,153), right-aligned; a 2px `#c98500` double-headed arrow at x=530 crossing it.
- **Inherited-position label (bold 12px `#d95926`, centered at (530,255)):** "consistency is now the store's, not the engine's".
- **Caption (12px `#444`, at (40,288), left-aligned):** "the engine is unchanged in both panels — only the data's location differs".

## Reading a System's Real Position

**Tags:** `PACELC map` (blue), `two questions` (green), `where it's used` (orange)

- **First question** — when the network splits, does the losing side error out or accept the write anyway
- **Second question** — with no split at all, does a normal read pay a round trip for recency or answer locally
- **Most labels stop early** — "eventually consistent" tells you the P answer and hides the everyday latency answer
- **Coordination and global SQL** — refuse on partition, pay latency always: the strict corner of the map
- **Object storage and cheap reads** — strong for one key in one region, eventual as soon as you cross either boundary
- **Dials, not positions** — wide-column stores, log brokers, and relational replication all move on the map by config
- **Some things are off the map** — an in-process engine has no partition, so it inherits whatever it reads from
- **What to ask of a data source** — not "is it consistent" but "which of these two bills does my query pay"

*Example (italic):* Two systems both described as "highly available" can differ completely: one serves stale reads locally, the other errors out rather than serve them.

**Key point:** Place a system with two answers, not one letter — behaviour during a partition, and the latency it charges when everything is healthy.

### Visualization (canvas `c11`, 720×300)

A 2×2 PACELC map placing the systems discussed, illustrative positions.

- **Title (bold 15px `#1a5276`, centered at y=24):** "Two Questions, Four Quadrants (illustrative placement)".
- **Frame:** plot rect x 150–650, y 55–230, 1.5px `#1a5276`; a 1.5px `#1a5276` vertical divider at x=400 and horizontal divider at y=142.5.
- **Axis labels:** bold 12px `#1a5276` "partition → refuse (PC)" centered at (275,248) and "partition → accept (PA)" centered at (525,248); rotated −90° bold 12px "no split → local read (EL)" centered at (135,190) and "no split → strict read (EC)" centered at (135,100).
- **Quadrant tints:** upper-left `rgba(0,131,0,0.06)`, upper-right `rgba(74,58,167,0.05)`, lower-left `rgba(42,120,214,0.05)`, lower-right `rgba(201,133,0,0.07)`.
- **Placed labels (bold 12px, each with a 4px dot of the same colour to its left):**
  - PC/EC quadrant: "ZooKeeper / etcd" `#008300` at (200,80); "Spanner" `#008300` at (200,105); "Kafka (acks=all)" `#008300` at (200,130).
  - PC/EL quadrant: "DynamoDB, strong read" `#2a78d6` at (200,170); "S3, one key one region" `#2a78d6` at (200,196); "PostgreSQL, sync standby" `#2a78d6` at (200,218).
  - PA/EL quadrant: "Cassandra (ONE)" `#c98500` at (430,170); "DynamoDB global tables" `#c98500` at (430,196); "Redis async" `#c98500` at (430,218).
  - PA/EC quadrant: 12px `#6b7280` note "rare — pay latency, then diverge anyway" at (430,105), left-aligned.
- **Off-map marker:** a `#6b7280` dashed rounded rect 232×30 at (458,264) — outside and below the plot frame — holding bold 12px `#6b7280` "DuckDB — no partition to place" centered, so it reads as detached from every quadrant.
- **Annotation (bold 12px `#d95926`, left-aligned at (30,282)):** "several of these move between quadrants by configuration".


## Regeneration instructions

- **Template:** tutorials topic page (see `14-quorums.html` for the exact skeleton, CSS, tag pills, and canvas `setup()` helper).
- **Layout:** h1 (no index number), `.subtitle`, then one `.card-section` per section above, each containing an h2 and a `table.layout` with `td.text-col` at **50%** and `td.viz-col` at **50%**.
- **Text column:** `.tags` pill row, then a `<ul>` of one-line bullets each opening with `<b>bold term</b>`, then one `p.example` italic line, then one `.key-point` callout.
- **Canvases:** 720×300 logical, `width:100%` CSS with `max-width:720px`, backing store sized to the displayed width × `devicePixelRatio`, redrawn on resize.
- **Palette:** `blue #2a78d6`, `green #008300`, `magenta #d55181`, `yellow #c98500`, `aqua #199e70`, `orange #d95926`, `violet #4a3aa7`, `ink #1a5276`, `text #2c3e50`, `mute #6b7280`, `grid #e5e9ef`; red `#e74c3c` only for genuine error states.
- **Data integrity:** no `Math.random()`; every number in a chart is a hardcoded literal and matches the text. Latency, lag, and cost figures are marked illustrative in the chart caption.
- **Hard rules:** no nav bar, no back/home links, no cross-page links, no `.nav` CSS.
