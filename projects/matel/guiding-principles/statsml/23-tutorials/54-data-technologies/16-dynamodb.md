# DynamoDB

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** DynamoDB

**Subtitle:** AWS's fully managed key-value store answers one question — "give me this customer's orders" — in single-digit milliseconds no matter how big the table gets, as long as you ask by key

## An Orders Table Filed by Customer, Sorted by Date

**Tags:** `core idea` (blue), `key-value` (green), `Dynamo paper` (orange)

- **The table** — an online shop keeps every order in one DynamoDB table called `orders`
- **Partition key** — each item's `customerId` is hashed to pick which storage partition holds it
- **Sort key** — within a customer, items are stored sorted by `orderDate`, so ranges are cheap
- **One question** — "orders for customer 412 in July" is a single Query straight to one partition
- **The lineage** — Amazon's 2007 Dynamo paper became the managed DynamoDB product in 2012
- **No server** — AWS runs the machines; you see only a table, keys, and a request API

*Example (italic):* `Query(customerId=412, orderDate between 2026-07-01 and 2026-07-31)` lands on one partition and walks a sorted run of items — no other customer's data is touched.

**Key point:** DynamoDB is a giant hash-of-sorted-lists: the partition key picks the bucket, the sort key orders items inside it — and every fast query must follow that shape.

### Visualization (canvas `c1`, 720×300)

Diagram of the hash-and-sort layout: a key box on the left, a hash arrow, three partition boxes on the right with customer 412's orders sorted by date inside one of them.

- **Title (bold 15px, `#1a5276`, top center):** "Partition Key Picks the Bucket, Sort Key Orders the Items".
- **Key box:** rounded box at x=25, y=115, 165×46, fill `rgba(42,120,214,0.15)`, 12px `#2c3e50` text "PK: customer#412", 8px radius.
- **Hash arrow:** 3px `#6b7280` arrow from (190,138) to (275,138) with 12px `#6b7280` label "hash(PK)" above it.
- **Partition boxes (three, each 130×205 at y=55, 8px radius, 1.5px `#1a5276` border):** "Partition A" at x=285, "Partition B" at x=430, "Partition C" at x=575; 12px bold `#1a5276` name at each box top.
- **Partition B contents:** four 12px `#2c3e50` item rows at y = 110, 140, 170, 200: "412 | 07-03", "412 | 07-11", "412 | 07-19", "412 | 07-28"; each row backed by a 118px-wide strip fill `rgba(0,131,0,0.12)`.
- **Partitions A and C contents:** two muted 12px `#6b7280` rows each ("088 | ...", "731 | ...") to show other customers live elsewhere.
- **Annotation (bold 13px green `#008300`, under Partition B near y=280):** "sorted by orderDate — July is one contiguous run".
- **Caption (12px `#444`, bottom left):** "customer ids and dates illustrative".

## Counting Reads and Writes in Capacity Units

**Tags:** `worked example` (blue), `capacity units` (green)

- **The units** — 1 RCU buys one strongly consistent read/sec of up to 4 KB; 1 WCU one 1 KB write/sec
- **The discount** — an eventually consistent read costs half: 0.5 RCU per 4 KB
- **The query** — customer 412 has 20 July orders at ~1 KB each, so the Query returns 20 KB
- **Hand-check** — 20 KB ÷ 4 KB = 5 RCU strongly consistent, or 2.5 RCU eventually consistent
- **The write side** — inserting one 1 KB order costs exactly 1 WCU
- **Two billing modes** — provisioned reserves RCU/WCU per second; on-demand bills per request

*Example (italic):* Reading all 20 of customer 412's July orders costs 5 RCU (strong) or 2.5 RCU (eventual); writing one new order costs 1 WCU.

**Key point:** You pay in capacity units, not CPU time — the cost of a request is a simple function of bytes touched, which is why well-keyed queries stay cheap and predictable.

### Visualization (canvas `c2`, 720×300)

Horizontal bar chart of capacity cost per operation on the orders table: single read, 20-order query, single write — strong vs eventual where it applies.

- **Title (bold 15px, `#1a5276`, top center):** "What Each Operation Costs in Capacity Units".
- **Axis:** vertical 2px `#999` baseline at x=250, bars extend right, scale 60 px per unit, max width 440; x tick labels "1", "2.5", "5" (12px `#444`) below y=255 at x = 310, 400, 550, light `#e5e9ef` gridlines at those x positions.
- **Rows (bar height 18px, left-aligned 12px `#444` labels at x=20):**
  - y=80: "GetItem 1 order (1 KB), strong" — blue `#2a78d6` bar width 60, 11px label "1 RCU" at bar end
  - y=115: "GetItem 1 order, eventual" — aqua `#199e70` bar width 30, label "0.5 RCU"
  - y=150: "Query 20 orders (20 KB), strong" — blue bar width 300, label "5 RCU"
  - y=185: "Query 20 orders, eventual" — aqua bar width 150, label "2.5 RCU"
  - y=220: "PutItem 1 order (1 KB)" — orange `#d95926` bar width 60, label "1 WCU"
- **Annotation (bold 13px violet `#4a3aa7`, near x=360, y=55):** "strong read cost = bytes ÷ 4 KB; eventual = half".
- **Caption (12px `#444`, bottom right):** "order sizes illustrative; RCU/WCU formulas exact".

## Flat Latency at Any Size — If You Ask by Key

**Tags:** `where it's used` (blue), `predictable latency` (green)

- **The promise** — a keyed Query answers in single-digit milliseconds at 1 GB or at 10 TB
- **Why flat** — the hash sends the request to one partition; table growth adds partitions, not work
- **The contrast** — a relational ad-hoc join re-plans over the whole dataset and slows as it grows
- **Who needs it** — carts, sessions, profiles, order lookups: hot paths that must never get slower
- **Single-table school** — practitioners pack many entity types into one table keyed for every access
- **The price** — you give up ad-hoc joins and aggregations; those move to exports or other engines

*Example (italic):* The same customer-412 Query returns in ~4 ms when the table holds 1 GB and ~5 ms at 10 TB — while an illustrative SQL join drifts from 12 ms to well past 100 ms.

**Key point:** DynamoDB trades query flexibility for a flat latency curve — it stays fast at any scale precisely because it refuses to answer questions that would require scanning everything.

### Visualization (canvas `c3`, 720×300)

Line chart of read latency vs table size: DynamoDB keyed Query flat, relational ad-hoc join climbing, on a shared log-feel size axis.

- **Title (bold 15px, `#1a5276`, top center):** "Keyed Query Latency Stays Flat as the Table Grows 10,000×".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = table size with 12px `#444` tick labels "1 GB", "10 GB", "100 GB", "1 TB", "10 TB" at x = 60, 210, 360, 510, 660 (log-feel via equal spacing, not a real log axis); y = latency 0 to 120 ms, gridlines `#e5e9ef` at 30/60/90, 12px `#444` labels "30 ms", "60 ms", "90 ms".
- **DynamoDB line:** green `#008300` 3px line through the five x ticks at latencies `[4, 4, 5, 4, 5]` ms — flat along the bottom.
- **SQL join line:** magenta `#d55181` 3px line through the same x ticks at latencies `[12, 25, 48, 85, 115]` ms — steady climb, small open arrowhead past the last point hinting it keeps rising.
- **Line labels:** bold 12px green "DynamoDB Query by key" near (x=380, y=215); bold 12px magenta "relational ad-hoc join" near (x=330, y=105).
- **Annotation (bold 13px green `#008300`, near x=480, y=170):** "still single-digit ms at 10 TB".
- **Caption (12px `#444`, bottom right):** "latencies illustrative; flat-vs-growing shape is the point".

## Designing the Table Before Knowing the Queries

**Tags:** `common mistake` (red), `access patterns` (orange)

- **The habit** — SQL lets you model tables first and invent queries later; DynamoDB does not
- **The surprise** — "orders where status = shipped" has no key to use, so it becomes a Scan
- **What Scan does** — it reads every item in the table and filters afterward, billing for all of it
- **Hand-check** — 1,000,000 orders at 1 KB is ~1 GB scanned: ~125,000 RCU eventual vs 2.5 for the Query
- **The fix** — list every access pattern first, then choose keys (and secondary indexes) to serve each
- **The escape hatch** — a global secondary index on `status` re-keys the data so shipped is a Query

*Example (italic):* The shipped-orders dashboard scans 1,000,000 items to return 8,000 matches — 50,000× the capacity of the customer-412 Query — until a status index turns it back into a keyed read.

**Common mistake:** Bringing relational habits to a key-value store. In DynamoDB the queries design the table; any question you didn't key for degenerates into a full-table Scan.

### Visualization (canvas `c4`, 720×300)

Two-row comparison of items read: a keyed Query touching 20 items vs a Scan touching all 1,000,000, drawn with log-feel pixel widths.

- **Title (bold 15px, `#1a5276`, top center):** "Query Reads What You Asked For — Scan Reads Everything".
- **Layout:** vertical 2px `#999` baseline at x=230, bars extend right, max width 440, bar height 26px; left-aligned 12px `#444` two-line row labels at x=20.
- **Row 1 (y=95):** "Query: customer 412, July / (keyed)" — green `#008300` bar width 24, bold 12px green label "20 items — 2.5 RCU" at the bar end.
- **Row 2 (y=185):** "Scan: status = shipped / (no key)" — red `#e74c3c` bar width 440, fill `rgba(231,76,60,0.25)` with 2px `#e74c3c` border; inside it a thin darker `#e74c3c` strip of width 4 at the left edge labeled 11px "8,000 matches"; bold 12px red label "1,000,000 items — ~125,000 RCU" above the bar.
- **Bridge annotation (bold 13px orange `#d95926`, centered near x=360, y=145):** "50,000× the read cost for the same-sized answer".
- **Fix note (12px `#444`, bottom center near y=270):** "fix: a global secondary index on status makes this a Query again".
- **Caption (12px `#444`, bottom right):** "item counts illustrative; pixel widths schematic, not to scale".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); order counts, item sizes, latencies, and scan totals are invented and labeled illustrative; the capacity-unit formulas (1 RCU = one strongly consistent 4 KB read/sec, 0.5 RCU eventual, 1 WCU = one 1 KB write/sec) and the derived 5 / 2.5 / 1 unit costs are exact per AWS's documented pricing model.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
