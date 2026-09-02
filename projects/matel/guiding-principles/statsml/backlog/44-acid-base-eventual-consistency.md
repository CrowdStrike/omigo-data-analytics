# ACID, BASE & Eventual Consistency

**Page type:** detail page (backlog-style 2-col text/viz layout: h2 sections with text left ~45%, canvas right ~55%; two full-width tables and a closing key-point)
**HTML title tag:** ACID, BASE & Eventual Consistency — Discussion Backlog

**Subtitle:** Consistency models and what they mean for data pipelines, profiling correctness, and analytical reproducibility.

## Intro callout

**Core tension:** ACID gives correctness guarantees but limits throughput and distribution. BASE gives availability and partition tolerance but means a pipeline may read stale, partial, or out-of-order data. A profiling pipeline must know which model its source lives under — because "correct" means different things in each. The chemistry pun is intentional: acids and bases are opposites on the pH scale; ACID and BASE are opposites on the CAP spectrum.

## 1. ACID — The Strong-Consistency Contract

**A**tomicity, **C**onsistency, **I**solation, **D**urability — the transactional model of relational databases. Writes inside a transaction are invisible until commit; then they appear all at once.

Table (`.data`, columns Property / What breaks without it):

| Property | What breaks without it |
|----------|------------------------|
| **Atomicity** | Half-written records, orphaned foreign keys |
| **Consistency** | Violated invariants: negative balances, duplicate keys |
| **Isolation** | Dirty reads, phantom rows, non-repeatable reads |
| **Durability** | Acknowledged writes vanish after a crash |

Key-point callout: **Reader guarantee:** No intermediate state is ever visible. A reader sees the old state or the new state — never a mixture. Aborted work leaves no trace.

Example (italic): Example: PostgreSQL, MySQL, Oracle, SQL Server — and the reason bank transfers debit and credit as one unit.

### Visualization (canvas `c1`, 720×300)

Timeline diagram: atomic commit vs abort, two paired lanes.

- **Title (bold 14px `#1a5276`, top center):** "Atomic Commit: Readers See Old State or New State — Never a Mixture"
- **Lanes:** horizontal `#ccc` lines from x=130 to x=660, right-aligned 11px `#555` labels to their left. Four lanes: "T1 (internal)" at y=65, "readers' view" at y=105, "T2 (internal)" at y=190, "readers' view" at y=230.
- **Committed transaction (T1):** three orange `#e67e22` 4px dots at x=200/270/340 labeled "w1" "w2" "w3" above; blue "BEGIN" label at x=160 below the lane; green `#27ae60` 5px dot at x=420 with bold "COMMIT" label. Readers' view is a blue `#1a5276` step function (width 2.5): flat at yR+8 up to x=420, stepping up to yR−8 through x=660; labels "old state" (gray, left) and "new state — all writes appear at once" (green, right of the step). Green dashed vertical line at the commit x connecting the two lanes.
- **Aborted transaction (T2):** same three orange write dots w1–w3 and "BEGIN"; at x=420 a red `#e74c3c` X mark with bold red "ABORT" label. Readers' view is a flat blue line the full width, labeled "old state — aborted work leaves no trace".
- **Time arrow:** gray `#999` arrow at y=270 from x=130 to x=660 with arrowhead, centered label "time".

## 2. BASE — Availability First, Convergence Later

**B**asically **A**vailable, **S**oft state, **E**ventually consistent — the availability-first model of distributed NoSQL systems. A write lands on one replica and propagates asynchronously to the rest.

- **Basically Available:** always answers — possibly with stale data
- **Soft state:** state changes without new input as replicas converge
- **Eventually consistent:** replicas agree — given enough time with no new writes

Key-point callout: **The unbounded window:** "Eventually" has no deadline. Between the write and full convergence, two reads of the same key can return different values — both served successfully, both "correct" under the model.

Example (italic): Example: Cassandra, DynamoDB, CouchDB, Riak — and why a just-posted comment shows on your phone but not yet on your laptop.

### Visualization (canvas `c2`, 720×300)

Timeline diagram: three replica lanes flipping from v1 to v2 at different times.

- **Title (bold 14px `#1a5276`, top center):** "Eventual Consistency: Replicas Converge at Different Times"
- **Lanes:** "Replica A" / "Replica B" / "Replica C" at y=80/140/200, x from 130 to 660; flip times at x=210, 360, 510 respectively. Each lane: red `#e74c3c` 3px segment (v1) up to its flip point, then green `#27ae60` segment (v2) to the end, with a 5px green dot at the flip.
- **Inconsistency window:** shaded rectangle `rgba(230,126,34,0.12)` from x=210 to x=510, y=50–230; orange 11px centered labels above and below: "inconsistency window — reads return v1 or v2" / "depending on which replica answers".
- **Write annotation:** bold blue 10px "write v2 lands here" above Replica A's flip point (x=210).
- **Segment labels:** red "v1 (stale)" near the left of Replica C's lane; green "v2 (converged)" right of its flip.
- **Time arrow:** gray `#999` arrow at y=272 with centered label: "time — \"eventually\" has no deadline".

## 3. The CAP Trade-off

CAP theorem: when a network partition happens, a distributed system must choose between **C**onsistency (refuse to answer rather than answer wrong) and **A**vailability (answer, possibly stale). Partition tolerance is not optional in a distributed system.

Table (`.data`, columns Dimension / ACID / CP / BASE / AP):

| Dimension | ACID / CP | BASE / AP |
|-----------|-----------|-----------|
| Priority | Correctness | Availability |
| Under partition | Rejects writes it can't guarantee | Accepts writes, reconciles later |
| Read guarantee | Committed, consistent state | Possibly stale or partially converged |
| Scale model | Vertical, coordinated | Horizontal, partition-tolerant |

Key-point callout: **Not a ranking:** Neither side is universally better. The choice encodes which failure mode the system can tolerate — being wrong or being down.

### Visualization (canvas `c3`, 720×300)

CAP triangle diagram with highlighted CP and AP edges.

- **Title (bold 14px `#1a5276`, top center):** "CAP: Under a Partition, Pick Consistency or Availability"
- **Triangle:** vertices C at (200,70), A at (520,70), P at (360,250); outline `#bbb` 1.5px. Each vertex is a white 16px-radius circle with blue `#1a5276` stroke and bold blue letter; side labels in 11px `#555`: "Consistency" (left of C), "Availability" (right of A), "Partition tolerance" (below P).
- **Edge highlights:** C–P edge in blue `#1a5276` 4px; A–P edge in orange `#e67e22` 4px.
- **CP edge labels** (right-aligned at edge midpoint): bold blue 11px "CP — ACID side"; gray 10px "PostgreSQL, Spanner, ZooKeeper" and "refuses to answer rather than answer wrong".
- **AP edge labels** (left-aligned at edge midpoint): bold orange 11px "AP — BASE side"; gray 10px "Cassandra, DynamoDB, CouchDB" and "answers, possibly stale; reconciles later".
- **CA note** (gray `#999`, centered above the top edge): "CA edge: only meaningful without partitions (single node)"

## 4. What the Profiler Sees: Phantom Bimodality

Profile an eventually-consistent source mid-convergence and two versions of truth coexist in the same scan: rows already updated cluster at the new value, rows not yet converged cluster at the old one. The histogram is genuinely bimodal — but the bimodality is an artifact of *when* you read, not a property of the data.

- Shape detection fires on a gap/valley that doesn't exist in the converged data
- The same scan an hour later is unimodal — nothing changed except replication catching up
- No amount of sample size fixes it: more rows just sharpens both phantom modes

Key-point callout: **Precondition, not statistics:** This is a case where the fix is upstream of any statistical test — verify the read is a consistent snapshot before trusting the shape.

### Visualization (canvas `c4`, 720×300)

Side-by-side histogram comparison: bimodal mid-convergence vs unimodal after convergence.

- **Title (bold 14px `#1a5276`, top center):** "Same Column, Same Data — Read at Two Different Moments"
- **Left panel** (label "profiled mid-convergence", 11px `#555` centered): bars 16px wide, gap 3, baseline y=240, starting x=60. Three groups: old-value mode heights `[30, 75, 110, 80, 32]` in `rgba(231,76,60,0.45)`; valley heights `[6, 10, 8, 6, 5]` at x offset +100 in `rgba(26,82,118,0.2)`; new-value mode heights `[28, 70, 100, 72, 30]` at x offset +195 in `rgba(26,82,118,0.35)`. Labels below baseline: red "stale rows (v1)", blue "converged rows (v2)", orange "phantom valley — shape detector fires here".
- **Divider:** vertical `#ddd` line at x=390 from y=45 to y=260.
- **Right panel** (label "profiled after convergence"): single histogram starting at x=470, heights `[15, 45, 95, 140, 150, 120, 70, 35, 14]` in `rgba(39,174,96,0.4)`. Labels below: green "one mode — the true shape"; gray "nothing changed except replication catching up".

## 5. Non-Deterministic Profiles Without Snapshot Isolation

If successive profiling runs hit different replicas — or the same replica at different convergence states — row counts, null rates, and cardinality estimates flicker between runs with no underlying data change.

- Run-over-run diffs alarm on replication lag, not real drift
- Row count is not a completeness check under eventual consistency
- Joins across systems with different consistency models silently create states that never existed in either system

Key-point callout: **Reproducibility requirement:** A profile is only comparable to a previous profile if both read from a pinned snapshot (or behind a watermark/fence). Otherwise the diff measures the storage system, not the data.

Example (italic): Example: nightly profile alerts "row count dropped 2%" — the replica was 10 minutes behind; the rows arrive by morning.

### Visualization (canvas `c5`, 720×300)

Line chart: row count across 10 repeated profiling runs, flat vs jittering series.

- **Title (bold 14px `#1a5276`, top center):** "Row Count Across Repeated Profiling Runs — No Data Change"
- **Axes:** gray `#999` L-shaped axes, x from 100 to 660, baseline y=250, top y=60; x labels "run 1" … "run 10" evenly spaced; rotated y-axis label "row count" at x=60.
- **Snapshot-isolated series:** flat green `#27ae60` line (width 2.5) at y=120 with 4px dots at each run; 11px label left-aligned above: "snapshot-isolated read — reproducible".
- **Eventually-consistent series:** orange `#e67e22` line (width 2) through fixed jitter offsets below the flat line: y = 120 + `[28, 62, 40, 75, 33, 58, 82, 45, 68, 36]` per run, with 4px orange dots; label: "eventually-consistent read — flickers with replica lag". (Offsets are fixed, not random — deterministic render.)
- **False alarm annotation:** dashed red `#e74c3c` 12px-radius circle around run 7's orange point (jitter 82); red 10px centered two-line text below: "\"2% drop!\" — false alarm," / "replica was behind".

## Anti-Patterns

Full-width `.data` table (no canvas):

| Anti-pattern | Why it fails |
|--------------|--------------|
| Assuming ACID when reading a distributed cache or event stream | Reads are stale/partial by design; the pipeline treats them as committed truth |
| Profiling during active ingestion without a watermark/fence | The scan mixes converged and in-flight rows — phantom modes, moving counts |
| Comparing two runs that read different replica states | The diff measures replication lag, not data drift |
| Using row count as a completeness check | Under eventual consistency, the count is a lower bound with unknown lag |

## Questions to Resolve

Full-width `.data` table (no canvas):

| Question | What's at stake |
|----------|-----------------|
| Pipeline assumptions | Does the profiling pipeline assume ACID reads? What breaks if it doesn't get them? |
| Staleness tolerance | How stale can source data be before profile results are meaningless? |
| Conflict resolution | Last-write-wins vs vector clocks vs CRDTs — which matters for analytics? |
| Snapshot isolation | Does the pipeline need a full consistent snapshot, or is read-your-writes enough? |
| Failure semantics | At-least-once vs exactly-once vs at-most-once delivery into the pipeline |

## Closing key-point

**The analytical insight:** Consistency models are a data-quality precondition, not an infrastructure detail. ACID sources can produce a wrong profile only if the data is wrong; BASE sources can produce a wrong profile from correct data, purely because of when and where the read landed. Before interpreting any shape, gap, or count from a distributed source, the pipeline must establish what read guarantee it actually had.

## Regeneration instructions

- **Template:** backlog detail-page layout — h1 with 2px `#2980b9` bottom border, `.subtitle`, `.intro-callout` (background `#f8f9fa`, left border `3px solid #2980b9`, 10px 14px padding, 0.93rem), then one `.card-section` per section: h2 (1.3rem `#1a5276`, 2px `#2980b9` bottom border) followed by a `table.layout` row with `td.text-col` (45%) and `td.viz-col` (55%) holding the canvas. The Anti-Patterns and Questions to Resolve sections are `.card-section`s containing only a full-width `table.data` (no canvas). The closing key-point is a standalone `.key-point` div with 30px top margin.
- **Callout styles:** `.key-point` — background `#f8f9fa`, left border `3px solid #e74c3c`, 8px 12px padding, 0.9rem. `.example` — italic, `#555`, 0.9rem.
- **Table style:** `table.data` — collapsed borders, full width; th/td `1px solid #ddd`, 6px 10px padding, left-aligned, 0.9rem; th background `#f8fafb`, weight 600.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `ul` 0.92rem; canvases `width: 100%` with `1px solid #e0e0e0` border, 4px radius.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`; grays `#555`/`#999`/`#bbb`/`#ccc`.
- **Canvas:** intrinsic size 720×300 per chart; a shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- No nav bar, no back/home links. In regenerated HTML, any card links use `.html` extensions.
