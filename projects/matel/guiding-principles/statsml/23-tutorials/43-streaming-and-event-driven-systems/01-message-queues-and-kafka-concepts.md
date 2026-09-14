# Message Queues & Kafka Concepts

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Message Queues & Kafka Concepts

**Subtitle:** A Kafka topic is a named append-only log split into partitions — consumers read at their own pace and can rewind, because reading never deletes anything

## One Orders Topic, Three Partitions

**Tags:** `core idea` (blue), `append-only log` (green), `partitions` (orange)

- **The shop** — an online store writes every order as one record to a topic named `orders`
- **The log** — `orders` is an append-only file: new records go at the end, nothing is edited
- **The split** — the topic is divided into 3 partitions (p0, p1, p2) so writes and reads spread out
- **The key** — the producer hashes the customer id; the same key always lands on the same partition
- **The payoff** — customer `cust-42` always maps to p1, so their orders stay in arrival order

*Example (italic):* Three orders from `cust-42` — placed, paid, shipped — all hash to p1 and sit there in exactly that sequence.

**Key point:** A topic is not a mailbox; it is a durable log split into partitions, and keyed writes guarantee that all records for one key line up in order on one partition.

### Visualization (canvas `c1`, 720×300)

Flow diagram: one producer box on the left, a hash-by-key router, and three horizontal partition logs on the right, each drawn as a row of appended record cells.

- **Title (bold 15px, `#1a5276`, top center):** "Topic `orders`: One Log Split Into 3 Partitions, Routed by Key".
- **Producer box:** rounded rect at x=20, y=125, 120×50, fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border, 12px `#2c3e50` label "producer" with sub-label "key = customer id".
- **Router:** violet `#4a3aa7` diamond-ish rounded box at x=185, y=125, 110×50, 12px label "hash(key) % 3"; three 2px `#6b7280` arrows fanning from it to the partition rows.
- **Partition rows (y = 70, 150, 230), each:** left 12px bold `#1a5276` label ("p0", "p1", "p2") at x=330; then record cells 44px wide, 34px tall, 1px `#e5e9ef` gaps, starting at x=365 — p0 holds 5 cells (offsets 0–4), p1 holds 6 cells (offsets 0–5), p2 holds 4 cells (offsets 0–3); cell fill `rgba(26,82,118,0.10)`, 11px `#444` offset numbers inside.
- **Key highlight:** the cells at p1 offsets 2, 4, 5 filled green `rgba(0,131,0,0.25)` with bold 11px `#008300` labels "42" — the three `cust-42` orders in arrival order.
- **Annotation (bold 13px green `#008300`, near x=430, y=205):** "same key → same partition → same order".
- **Caption (12px `#444`, bottom right):** "record counts illustrative".

## Two Groups, Two Bookmarks in the Same Log

**Tags:** `worked example` (blue), `consumer groups` (green), `offsets` (orange)

- **Two readers** — a billing service and an analytics service both consume the `orders` topic
- **Groups** — each service is its own consumer group; within a group, each partition has one owner
- **Offsets** — every record has a position number; a group commits how far it has read per partition
- **The state** — on p1 with 8 records (offsets 0–7), billing has committed offset 5, analytics offset 2
- **Hand-check** — billing still owes offsets 5, 6, 7 (lag 3); analytics owes 2 through 7 (lag 6)

*Example (italic):* Billing and analytics read the exact same 8 records on p1, yet billing is 3 behind the end and analytics is 6 behind — neither reader affects the other.

**Key point:** Consuming does not remove records; each group just advances its own committed offset per partition, so many independent readers share one log without interfering.

### Visualization (canvas `c2`, 720×300)

Single partition p1 drawn as a horizontal strip of 8 offset cells, with two labeled bookmark arrows (billing at 5, analytics at 2) and lag braces.

- **Title (bold 15px, `#1a5276`, top center):** "Partition p1: 8 Records, Two Groups, Two Independent Offsets".
- **Log strip:** 8 cells, each 70px wide and 46px tall, starting at x=70, y=130; fill `rgba(26,82,118,0.10)`, 1px `#e5e9ef` borders, bold 13px `#444` offset numbers 0–7 centered; 12px `#6b7280` label "partition p1 →" above the strip at x=70, y=118.
- **Analytics bookmark:** magenta `#d55181` 3px vertical arrow from y=220 up to the left edge of cell 2 (x=210), bold 12px magenta label "analytics: committed offset 2" at y=238; magenta bracket under cells 2–7 with 12px label "lag 6".
- **Billing bookmark:** blue `#2a78d6` 3px vertical arrow from y=95 down to the left edge of cell 5 (x=420), bold 12px blue label "billing: committed offset 5" at y=80; blue bracket over cells 5–7 with 12px label "lag 3".
- **End marker:** dashed `#6b7280` (dash 4/3) vertical line at the right edge of cell 7 (x=630), 12px `#6b7280` label "log end (8)" beside it.
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=270):** "reading moves a bookmark — it never deletes a record".
- **Caption (12px `#444`, bottom right):** "offsets illustrative".

## Retention and Replay: Why the Log Wins

**Tags:** `where it's used` (blue), `replay` (green)

- **Classic queue** — a traditional message queue deletes each message once a consumer acks it
- **The log** — Kafka keeps records for a configured retention window (say 7 days), read or not
- **The bug** — Wednesday, analytics discovers Monday's revenue numbers were computed wrong
- **The rewind** — analytics resets its offset back 2 days and reprocesses ~2,400 orders from the log
- **The queue's answer** — in a delete-on-ack queue those 2,400 messages are gone; only re-sending helps

*Example (italic):* With 7-day retention holding ~8,400 orders, the 2-day rewind replays 2,400 of them — billing's offsets never move and billing never notices.

**Key point:** Because the log deletes by age rather than by ack, an offset is just a position you can set backwards — replay after a bug is a one-line offset reset, not a data-recovery project.

### Visualization (canvas `c3`, 720×300)

Two-row comparison: a delete-on-ack queue timeline (consumed messages vanish) vs the retained log timeline (7-day window intact, replay arrow jumping back 2 days).

- **Title (bold 15px, `#1a5276`, top center):** "Delete-on-Ack vs Retained Log: What a Wednesday Rewind Can Reach".
- **Shared x-axis:** days Mon–Sun left to right, plot from x=110 to x=670, 12px `#444` day labels along y=265; 12px `#6b7280` row labels at x=15: "classic queue" (y=105) and "Kafka log" (y=195).
- **Queue row (y=85, 40px tall):** Mon–Tue span drawn as dashed 1px `#e74c3c` empty outline with 12px red `#e74c3c` label "acked = deleted"; Wed onward a small solid `rgba(42,120,214,0.30)` block labeled "only new messages" (11px `#444`).
- **Log row (y=175, 40px tall):** full Mon–Sun bar fill `rgba(0,131,0,0.20)` with 2px `#008300` border, 12px `#008300` label "7-day retention — ~8,400 orders" centered inside.
- **Replay arrow:** bold 3px orange `#d95926` arrow curving from Wed (x≈360) back to Mon (x≈140) above the log row at y≈150, bold 12px orange label "reset offset −2 days: replay 2,400 orders".
- **Failure mark:** bold 13px red `#e74c3c` "✗ nothing to replay" over the queue row's Mon–Tue gap.
- **Caption (12px `#444`, bottom right):** "order counts illustrative, retention window as configured".

## Ordered — But Only Within a Partition

**Tags:** `common mistake` (red), `ordering` (orange)

- **The claim** — "Kafka delivers our orders in order" is only true one partition at a time
- **Within p1** — `cust-42`'s records at offsets 2, 4, 5 are always read in that sequence
- **Across partitions** — a consumer reading p0, p1, p2 sees the three logs interleaved unpredictably
- **The other trap** — a group with 4 consumers on 3 partitions leaves one consumer idle, owning nothing
- **The fix** — put everything that must stay ordered under one key; scale partitions before consumers

*Example (italic):* An order placed on p0 at 9:00 can be read after an order placed on p2 at 9:05 — each partition is a separate line at the checkout.

**Common mistake:** Expecting global order from a partitioned topic. Kafka guarantees order per partition only; cross-partition sequence depends on read timing, and extra consumers beyond the partition count sit idle.

### Visualization (canvas `c4`, 720×300)

Two-row diagram: same-key records flowing in guaranteed order on one partition (top), vs three partitions merging into one consumer with interleaved arrival (bottom).

- **Title (bold 15px, `#1a5276`, top center):** "Order Holds Inside a Partition, Not Across Them".
- **Row 1 (y=80), label 12px `#444` at x=15:** "one key, one partition"; three green `rgba(0,131,0,0.15)` rounded boxes (110×36, 8px radius, 2px `#008300` border) at x=170/310/450 labeled "42: placed", "42: paid", "42: shipped" (12px `#2c3e50`), joined by 3px `#008300` arrows; bold 12px green "✓ always this order" at x=590.
- **Row 2 (y=190), label:** "three partitions, one reader"; three small blue `rgba(42,120,214,0.15)` boxes (70×30) stacked at x=170 (y=160/195/230) labeled "p0", "p1", "p2" (12px), three 2px `#6b7280` arrows converging to a violet `rgba(74,58,167,0.12)` consumer box (120×40, 2px `#4a3aa7` border) at x=330, y=185 labeled "consumer"; to its right a 12px `#2c3e50` read-sequence strip "p1:2  p0:3  p2:1  p1:3  p0:4" at x=480 with bold 12px red `#e74c3c` "✗ interleaving varies run to run" below at y=245.
- **Annotation (bold 13px orange `#d95926`, centered near y=280):** "need total order? use one key — or accept per-key order only".
- **Caption (12px `#444`, bottom right):** "record labels illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all positions, offsets, and counts are the hardcoded values above (no randomness); partition record counts, committed offsets (billing 5, analytics 2, log end 8), and order volumes (8,400 retained / 2,400 replayed) are invented and labeled illustrative; the described mechanics (keyed partitioning, per-group offsets, retention-based deletion, per-partition ordering, idle consumers beyond partition count) are documented Kafka behavior.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
