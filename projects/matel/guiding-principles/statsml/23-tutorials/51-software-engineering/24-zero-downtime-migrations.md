# Zero-Downtime Migrations

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Zero-Downtime Migrations

**Subtitle:** Changing a live database schema without stopping the world — expand, dual-write, backfill, contract: five boring deploys instead of one scary one

## One Database, Two Code Versions

**Tags:** `core idea` (blue), `rolling deploys` (green), `schema changes` (orange)

- **The setup** — an orders service on 8 servers, all reading and writing one `orders` table
- **The rolling deploy** — servers update one at a time over 20 minutes; old and new code overlap
- **The naive rename** — `ALTER TABLE orders RENAME COLUMN cust_nm TO customer_name` in one step
- **The break** — every server still on old code fails instantly: "column cust_nm does not exist"
- **The count** — 8 old servers × 30 writes/s at minute 0, shrinking to 0 only when the rollout ends

*Example (italic):* The rename lands at minute 0; over the 20-minute rollout roughly 162,000 writes fail before the last old server is replaced.

**Key point:** During any deploy, old and new code run simultaneously against the same database — so every schema change must be compatible with both versions at once.

### Visualization (canvas `c1`, 720×300)

Step-line chart of failing writes per second during a 20-minute rolling deploy after a naive one-step rename at minute 0.

- **Title (bold 15px, `#1a5276`, top center):** "Naive Rename at Minute 0: Old Servers Fail Every Write Until the Rollout Ends".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = rollout minutes 0 to 20 with 12px `#444` tick labels every 5 minutes; y = failing writes/s 0 to 240, gridlines `#e5e9ef` at 60/120/180.
- **Failing-writes step line:** red `#e74c3c` 3px step line through minutes `[0, 2.5, 5, 7.5, 10, 12.5, 15, 17.5, 20]`, fails/s `[240, 210, 180, 150, 120, 90, 60, 30, 0]` — one 30 writes/s step down each time a server picks up new code.
- **Step labels (11px `#6b7280`):** "8 old servers" above the first step, "4" near minute 10, "0" at minute 20.
- **Fill:** red area `rgba(231,76,60,0.12)` under the step line down to the baseline.
- **Annotation (bold 13px red `#e74c3c`, near minute 9, y=80):** "≈162,000 failed writes in one rollout".
- **Caption (12px `#444`, bottom right):** "server and write counts illustrative".

## Renaming a Column in Five Boring Deploys

**Tags:** `worked example` (blue), `expand–contract` (green)

- **Expand (day 1)** — add `customer_name` as a nullable column beside `cust_nm`; both code versions ignore it
- **Dual-write (day 2)** — deploy code that writes both columns but still reads `cust_nm`
- **Backfill (days 3–4)** — copy 12M historical rows old→new in 2,400 batches of 5,000, pausing between
- **Switch reads (day 5)** — read `customer_name`, keep writing both; rollback is flipping the read back
- **Contract (day 8+)** — stop writing `cust_nm`, and on day 15 drop the column for good

*Example (italic):* By day 5 the app reads only customer_name, yet any surprise can still be rolled back in one deploy because cust_nm is still being written.

**Key point:** Each step is a separate, additive deploy that both code versions survive — and each is reversible by going back exactly one step, never by restoring a backup.

### Visualization (canvas `c2`, 720×300)

Gantt-style timeline of the five expand–contract steps for the rename, on a shared day axis 1 to 15, with the read/write state annotated per step.

- **Title (bold 15px, `#1a5276`, top center):** "The Expand–Contract Timeline: Each Step Ships Alone".
- **Axis:** horizontal 2px `#999` baseline at y=255, x from 170 to 670 mapping days 1 to 15; 12px `#444` tick labels at days 1/5/10/15.
- **Rows (top to bottom at y = 70, 110, 150, 190, 230), each with a left-aligned 12px `#444` label at x=20:**
  - "1 EXPAND": blue `#2a78d6` bar day 1 only, 11px note at bar end "add customer_name (nullable)"
  - "2 DUAL-WRITE": aqua `#199e70` bar days 2–8, note "write both, read old"
  - "3 BACKFILL": orange `#d95926` bar days 3–4, note "2,400 batches × 5,000 rows"
  - "4 SWITCH READS": green `#008300` bar days 5–15, note "read new, still write both"
  - "5 CONTRACT": violet `#4a3aa7` bar days 8–15, note "stop old writes; drop day 15"
- **Bar style:** 16px tall, 4px radius, fills at 0.30 alpha with a solid 2px edge in the row color; notes 11px `#6b7280` right of each bar.
- **Marker:** vertical dashed `#6b7280` (dash 4/3) line at day 15, 11px label "cust_nm dropped".
- **Annotation (bold 13px `#1a5276`, centered near y=280):** "rollback at any step = go back exactly one step".
- **Caption (12px `#444`, bottom right):** "day spans illustrative".

## The Same Shape Moves Whole Tables

**Tags:** `where it's used` (blue), `reversible steps` (green)

- **Not just renames** — type changes, table splits, and store migrations all reuse the same five steps
- **The bigint rescue** — an int primary key overflows near 2.1 billion rows; the fix is expand–contract on the id
- **The store move** — migrating orders to a new database is dual-write and backfill at store scale
- **Rollback shape** — at every step, undo means redeploying the previous step, never restoring a backup
- **The habit** — teams that know the shape stop fearing schema changes and ship them routinely

*Example (italic):* The same playbook that renamed one column later moved the entire orders table to a new database — only the batch sizes changed.

**Key point:** Expand–contract is a shape, not a rename trick — any live data migration decomposes into the same five independently deployable, reversible steps.

### Visualization (canvas `c3`, 720×300)

Matrix diagram: three different migrations as rows, the five expand–contract steps as columns, each cell a small labeled box showing what the step means for that migration.

- **Title (bold 15px, `#1a5276`, top center):** "Same Five Steps, Three Different Migrations".
- **Column headers (bold 12px `#1a5276`, centered at y=58, x centers = 205, 310, 415, 520, 625):** "EXPAND", "DUAL-WRITE", "BACKFILL", "SWITCH", "CONTRACT".
- **Row labels (12px `#444`, left-aligned at x=20, vertically centered per row):** "rename column", "int → bigint id", "move to new store"; rows centered at y = 105, 165, 225.
- **Cells:** boxes 96px wide × 44px tall, 6px radius, centered under each header; per-column fill/edge: blue `rgba(42,120,214,0.15)`/`#2a78d6`, aqua `rgba(25,158,112,0.15)`/`#199e70`, orange `rgba(217,89,38,0.15)`/`#d95926`, green `rgba(0,131,0,0.12)`/`#008300`, violet `rgba(74,58,167,0.12)`/`#4a3aa7`; 11px `#2c3e50` text, two lines max:
  - Row 1: "add customer_name" / "write both cols" / "copy 12M rows" / "read new col" / "drop cust_nm"
  - Row 2: "add id_big bigint" / "write both ids" / "copy every id" / "read id_big" / "drop old id"
  - Row 3: "stand up new store" / "write both stores" / "copy history" / "read new store" / "retire old store"
- **Arrows:** 2px `#6b7280` arrows between adjacent cells in each row.
- **Annotation (bold 13px magenta `#d55181`, centered near y=278):** "learn the shape once — reuse it for every live migration".

## The Unthrottled Backfill and the Zombie Dual-Write

**Tags:** `common mistake` (red), `table locks` (orange)

- **The giant UPDATE** — one statement touching all 12M rows holds locks for ~9 minutes; every write waits
- **The throttled version** — 2,400 batches of 5,000 rows with a pause between barely moves p99 (12 → ~19ms)
- **The locking ALTER** — adding NOT NULL or an index can scan the whole table; know your DB's online-DDL rules
- **The zombie dual-write** — teams skip contract; six months later both columns are written and drift apart
- **The rule** — a migration is not done until the contract lands and the old column is actually gone

*Example (italic):* One `UPDATE orders SET customer_name = cust_nm` with no WHERE clause locks the table for 9 minutes — the classic self-inflicted outage.

**Common mistake:** The backfill is the dangerous step and the contract is the forgotten one — throttle the first into small batches, and put a calendar date on the second.

### Visualization (canvas `c4`, 720×300)

Line chart of p99 write latency over a 20-minute window: one giant unthrottled UPDATE (locks the table) vs the same backfill in throttled batches.

- **Title (bold 15px, `#1a5276`, top center):** "Backfilling 12M Rows: One Giant UPDATE vs 2,400 Throttled Batches".
- **Axes:** origin x=70, baseline y=245, plot width 590, plot height 180; x = minutes 0 to 20 with 12px `#444` tick labels every 5; y = p99 write latency in ms, 0 to 40, gridlines `#e5e9ef` at 10/20/30; the lock plateau is drawn pinned above the scale (see below), not on a real axis.
- **Throttled line:** green `#008300` 3px line through minutes `[0, 2, 4, 6, 8, 10, 12, 13, 14, 16, 18, 20]`, p99 ms `[12, 12, 18, 19, 18, 19, 18, 18, 12, 12, 12, 12]` — a small ripple during the backfill window (minutes 4–13).
- **Unthrottled line:** red `#e74c3c` 3px line at 12ms for minutes 0–4, then a vertical jump to a flat plateau drawn at y=65 (pinned off-scale) from minute 4 to minute 13, then back down to 12ms; plateau labeled bold 12px red "table locked — p99 = 30,000ms timeouts" with a small axis-break zigzag on the y axis at y=75.
- **Backfill window marker:** vertical dashed `#6b7280` (dash 4/3) lines at minutes 4 and 13, 11px `#6b7280` label "backfill running (9 min)".
- **Annotation (bold 13px green `#008300`, near minute 16, y=150):** "throttled batches: 12 → ~19ms, nobody notices".
- **Caption (12px `#444`, bottom right):** "latencies illustrative; lock plateau drawn off-scale".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); server counts, write rates, row counts, day spans, and latencies are invented and labeled illustrative; derived figures are exact from the illustrative inputs (8 servers × 30 writes/s = 240 fails/s; step-down sum 150s × (240+210+…+30) fails/s = 162,000 failed writes; 12M rows ÷ 5,000 per batch = 2,400 batches; lock window minutes 4–13 = 9 minutes).
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
