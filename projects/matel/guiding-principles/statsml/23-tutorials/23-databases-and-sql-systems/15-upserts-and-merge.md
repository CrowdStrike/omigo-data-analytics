# Upserts & MERGE

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Upserts & MERGE

**Subtitle:** One statement that inserts the row if it's new and updates it if it already exists — insert-or-update atomically, with no gap in between

## The Nightly Customer Feed

**Tags:** `core idea` (blue), `insert or update` (green), `atomic` (orange)

- **The feed** — every night a loyalty program receives a file of 5 customer rows from the stores
- **The mix** — some rows are brand-new customers, others are existing customers with a changed email
- **The naive plan** — for each row, SELECT to check if the id exists, then INSERT or UPDATE accordingly
- **The gap** — between the check and the insert, another loader can slip the same id in first
- **The upsert** — one statement does both: insert if the key is new, update if it already exists
- **No gap** — the database checks the key and acts inside a single atomic operation

*Example (italic):* Row `c103, dana@mail.com` arrives; one upsert statement inserts it tonight and would update it tomorrow — same statement, both cases.

**Key point:** An upsert folds check-then-write into one atomic statement, so "does this row exist?" and "write it" can never be separated by another writer.

### Visualization (canvas `c1`, 720×300)

Two-row flow diagram: check-then-insert as two boxes with a labeled gap between them, vs a single atomic upsert box, both acting on the same feed row.

- **Title (bold 15px, `#1a5276`, top center):** "Two Statements Leave a Gap; One Statement Doesn't".
- **Row 1 (y=95), label 12px `#444` at x=20:** "check, then insert"; blue `#2a78d6` rounded box at x=150 labeled "SELECT c103 — not found" (12px), 3px arrow to an orange `#d95926` dashed box at x=360 labeled "gap: anyone can write", then arrow to a red `#e74c3c` box at x=550 labeled "INSERT c103 — may collide".
- **Row 2 (y=205), label:** "one upsert"; green `#008300` rounded box at x=250, width 260, labeled "INSERT c103 ON CONFLICT DO UPDATE" with bold 12px green "✓ check + write, atomic" at x=560.
- **Box style:** 150–260px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(217,89,38,0.12)` / `rgba(231,76,60,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px orange `#d95926`, centered near y=150):** "the gap is the bug — the upsert removes it".
- **Caption (12px `#444`, bottom right):** "flow schematic, illustrative".

## Walking Five Rows Through ON CONFLICT and MERGE

**Tags:** `worked example` (blue), `ON CONFLICT` (green), `MERGE` (orange)

- **The table** — `customers` already holds 3 rows: c101, c102, c105, keyed by customer id
- **The feed** — tonight's 5 rows: c101 (new email), c102 (unchanged), c103, c104 (new), c105 (new city)
- **Postgres form** — `INSERT ... ON CONFLICT (id) DO UPDATE SET email = EXCLUDED.email`
- **MERGE form** — `MERGE ... WHEN MATCHED THEN UPDATE WHEN NOT MATCHED THEN INSERT`
- **The result** — c103 and c104 insert (2 rows), c101, c102, c105 update (3 rows); table now has 5 rows
- **Run it again** — same file re-run: 0 inserts, 5 updates, table still 5 rows — same final state

*Example (italic):* c101 exists so its email updates; c103 doesn't so it inserts — one statement, and re-running the file changes nothing.

**Key point:** Both forms make the same per-row decision — matched keys update, unmatched keys insert — so the load lands the same final table no matter how many times it runs.

### Visualization (canvas `c2`, 720×300)

Mapping diagram: the 5 feed rows as boxes on the left, arrows to the customers table on the right, each arrow colored and labeled by the action taken (INSERT vs UPDATE).

- **Title (bold 15px, `#1a5276`, top center):** "Tonight's 5 Rows: 2 Insert, 3 Update".
- **Feed boxes (left, x=40, width 200, height 30):** at y = `[70, 113, 156, 199, 242]`, labels `["c101 — new email", "c102 — unchanged", "c103 — new customer", "c104 — new customer", "c105 — new city"]` (12px `#2c3e50`), fill `rgba(42,120,214,0.12)`, 1px `#2a78d6` border.
- **Target box (right):** rounded rect at x=520, y=70, width 170, height 202, fill `rgba(26,82,118,0.06)`, 1px `#1a5276` border, bold 12px `#1a5276` header "customers (key: id)" and 12px `#2c3e50` lines "c101 · c102 · c105" and "+ c103 + c104".
- **Arrows (3px, from x=240 to x=520 at each row's mid-height):** rows 1, 2, 5 in blue `#2a78d6` with 12px blue label "UPDATE"; rows 3, 4 in green `#008300` with bold 12px green label "INSERT".
- **Annotation (bold 12px violet `#4a3aa7`, centered near y=290):** "re-run: 0 inserts, 5 updates — same table".
- **Caption (12px `#444`, bottom right):** "customer ids and counts illustrative".

## Loads You Can Safely Run Twice

**Tags:** `where it's used` (blue), `idempotent` (green), `CDC` (orange)

- **Idempotent loads** — a pipeline that upserts can crash mid-run and simply be restarted from the top
- **Append breaks** — a plain INSERT load re-run after a crash duplicates every row it already wrote
- **Dedupe on natural keys** — upserting on (store, date) keeps one row per key however often data arrives
- **CDC apply** — change-data-capture streams replay inserts and updates; upsert absorbs both kinds
- **The habit** — data engineers reach for upsert by default so retries are always safe

*Example (italic):* The 5-row feed loads three nights in a row by mistake; append leaves 18 rows with duplicates, upsert leaves the correct 5.

**Key point:** Upserts make loads idempotent — running the same feed once or five times converges to the same table, which is what makes retries and replays safe.

### Visualization (canvas `c3`, 720×300)

Grouped bar chart: table row count after re-running the same 5-row feed 1, 2, and 3 times, append-INSERT vs upsert (table starts at 3 rows).

- **Title (bold 15px, `#1a5276`, top center):** "Re-Running the Same Feed: Append Grows, Upsert Converges".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = three groups labeled "run 1", "run 2", "run 3" centered at x = `[180, 380, 580]` (12px `#444`); y = table rows 0 to 20, gridlines `#e5e9ef` at 5/10/15, 12px `#444` tick labels.
- **Append bars (red `#e74c3c`, 60px wide, left of each group center):** heights for row counts `[8, 13, 18]`, 12px red value labels on top.
- **Upsert bars (green `#008300`, 60px wide, right of each group center):** heights for row counts `[5, 5, 5]`, 12px green value labels on top.
- **Reference line:** dashed `#6b7280` (dash 4/3) horizontal line at 5 rows with 11px `#6b7280` label "correct: 5 rows" at the left end.
- **Annotation (bold 13px green `#008300`, near x=430, y=80):** "upsert lands on 5 every time".
- **Caption (12px `#444`, bottom right):** "row counts illustrative; table starts at 3 rows".

## The Check-Then-Insert Race

**Tags:** `common mistake` (red), `concurrency` (orange), `duplicate keys` (green)

- **The race** — two workers both SELECT c103, both see "not found", both INSERT: duplicate-key error
- **Why it happens** — the check and the write are separate statements; another writer fits between them
- **Not fixed by speed** — shrinking the gap makes the race rarer, not impossible; only atomicity fixes it
- **MERGE's own trap** — if the source feed holds c103 twice, MERGE hits one target row with two updates
- **The standard says** — a multi-match MERGE raises a cardinality error; dedupe the source first
- **The fix** — upsert for the race; `GROUP BY` or `ROW_NUMBER` to one row per key before any MERGE

*Example (italic):* Two loaders race on c103 and one crashes with a duplicate-key error; the same feeds using one upsert statement both succeed.

**Common mistake:** Believing a SELECT-then-INSERT you wrote yourself is "basically an upsert." Under concurrency it is a coin flip; and MERGE is only safe once the source has exactly one row per key.

### Visualization (canvas `c4`, 720×300)

Timeline diagram: two concurrent workers running check-then-insert on the same new key, their check→insert windows overlapping, ending in a duplicate-key error; an upsert lane below stays clean.

- **Title (bold 15px, `#1a5276`, top center):** "Two Workers, One New Key: the Gap Bites".
- **Axis:** horizontal 2px `#999` time line at y=250 from x=60 to x=660, 12px `#444` tick labels "t=0" at x=60, "t=5ms" at x=360, "t=10ms" at x=660 (30px per ms).
- **Worker A lane (y=85):** 12px `#444` label "worker A" at x=20; blue `#2a78d6` box from t=0 to t=2 (x=60–120) labeled "SELECT c103: none" (11px), dashed `#6b7280` gap segment to t=6, blue `#2a78d6` box from t=6 to t=8 (x=240–300) labeled "INSERT c103 ✓".
- **Worker B lane (y=150):** label "worker B"; blue box from t=1 to t=3 (x=90–150) labeled "SELECT c103: none", dashed gap to t=7, red box from t=7 to t=9 (x=270–330) labeled "INSERT c103 ✗ dup key" with bold 12px red "error" at its right.
- **Upsert lane (y=215):** label "upsert"; green `#008300` boxes: t=0 to t=3 (x=60–150) "upsert c103 → insert ✓" and t=3 to t=6 (x=150–240) "upsert c103 → update ✓".
- **Box style:** 26px tall, 6px radius, fills `rgba(42,120,214,0.15)` / `rgba(231,76,60,0.12)` / `rgba(0,131,0,0.12)`, 11px `#2c3e50` text.
- **Annotation (bold 13px magenta `#d55181`, near x=430, y=120):** "both checks passed before either insert ran".
- **Caption (12px `#444`, bottom right):** "timings in ms, illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); the 5 feed rows (c101–c105), the 2-insert/3-update split, the re-run counts (0 inserts / 5 updates), the append-vs-upsert row counts (`[8, 13, 18]` vs `[5, 5, 5]` from a 3-row start), and the millisecond race timings are invented and labeled illustrative.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
