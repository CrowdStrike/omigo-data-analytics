# ETL Basics

**Page type:** detail page (tutorial layout: h1 + subtitle, then one `.card-section` per concept, each an h2 + two-column `table.layout` with text left 50% / canvas right 50%)
**HTML title tag:** ETL Basics

**Subtitle:** Extract, Transform, Load — copy raw data out, clean it up, store it where analysts work

## One Night with the Orders Table

**Tags:** `core idea` (blue), `running example` (green)

- **The setup** — the app writes orders all day; analysts want them in the warehouse by morning
- **Extract** — at 2:00am a job copies yesterday's 12,480 order rows out of the app database
- **Transform** — the copy is cleaned (test orders dropped) and joined with the customers table
- **Load** — the finished 12,448 rows are written into the warehouse table **orders_daily**
- **Same recipe nightly** — E, then T, then L: one day of data, once per day

*Example:* Monday 9am: an analyst queries the warehouse and sees Sunday's 12,448 cleaned orders — the app database never felt a thing.

**Key point:** ETL is just move, clean, store. Extract copies raw data out, Transform reshapes it, Load writes the result where analysis happens.

### Visualization (canvas `c1`, 720×300)

Flow diagram: the nightly E → T → L pipeline from source database to warehouse, with row counts and timestamps.

- **Title (bold 16px, `#1a5276`, top center):** "One Night: 12,480 Raw Rows In, 12,448 Clean Rows Out".
- **Source:** database cylinder at left (x=20, y=95, 90×90) filled `rgba(42,120,214,0.12)`, stroked blue `#2a78d6`, labeled bold 12px "app database" / 12px "orders".
- **Stage boxes (130×76, centered on y=140, `#f8f9fa` fill, 2px colored stroke, bold 14px label + two 12px lines):**
  - at x=150 green `#008300`: "EXTRACT" / "2:00am" / "copy 12,480 rows"
  - at x=315 orange `#d95926`: "TRANSFORM" / "drop 30 test rows" / "join customers"
  - at x=480 violet `#4a3aa7`: "LOAD" / "2:31am" / "write 12,448 rows"
- **Arrows:** gray `#6b7280` filled-head arrows connecting cylinder → EXTRACT → TRANSFORM → LOAD → warehouse.
- **Destination:** cylinder at right (x=634, y=95, 76×90) filled `rgba(0,131,0,0.10)`, stroked green `#008300`, labeled bold 12px "warehouse" / 12px "orders_daily".
- **Flow counts (12px `#6b7280`):** "12,480 raw" above the first arrow, "12,448 clean" above the last arrow.
- **Callouts (centered):** bold 13px magenta `#d55181` at y=260: "32 rows removed on the way: 30 test orders + 2 sent to quarantine"; 12px `#6b7280` at y=282: "the same three steps run every night at 2:00am (illustrative counts)".

## Where 32 Rows Went: The Transform by Hand

**Tags:** `worked example` (green), `core idea` (blue)

- **Start** — the extract lands 12,480 raw rows at 2:14am
- **Drop test orders** — 30 rows from QA's fake accounts are removed: 12,450 left
- **Join customers** — each order looks up its customer's name and city
- **Quarantine** — 2 orders point to customer ids that don't exist; they go to a review table
- **Load** — the remaining 12,448 rows are written to the warehouse at 2:31am

*Example:* Order 88412 carried customer_id 99340 — no such customer — so it waits in quarantine instead of polluting the morning report.

**Key point:** every transform is countable — rows in, rows dropped, rows out — and the counts must add up: 12,480 − 30 − 2 = 12,448.

### Visualization (canvas `c2`, 720×300)

Row-count waterfall: four bars stepping down through the transform, with dashed drop connectors.

- **Title (bold 15px, `#1a5276`, top center):** "Row Counts Must Add Up: 12,480 − 30 − 2 = 12,448".
- **Axes:** padding top 56, bottom 62, left 78, right 24; y scale from 12,400 to 12,500 with tick labels 12,400 / 12,440 / 12,480 (12px `#6b7280`) and gridlines `#e5e9ef`; axis lines `#999`.
- **Bars (108px wide, evenly spaced; fill at 35% alpha of the stroke color, 2px stroke; value bold 12px above, label 12px `#6b7280` below):**
  - "extracted 2:14am" — 12,480, blue `#2a78d6`
  - "drop 30 test rows" — 12,450, orange `#d95926`
  - "quarantine 2 rows" — 12,448, orange `#d95926`
  - "loaded 2:31am" — 12,448, green `#008300`
- **Connectors:** dashed (4/3) magenta `#d55181` step lines between consecutive bar tops, with bold 12px magenta drop labels "−30" and "−2" where the count falls.
- **Callout (bold 13px magenta `#d55181`, centered below x labels):** "if the counts don’t reconcile, the transform is losing rows silently".

## Why the Warehouse Copy Exists at All

**Tags:** `where it's used` (blue), `rule of thumb` (green)

- **Heavy queries** — a year-of-revenue scan would slow the live database customers use
- **Cleaned once** — every analyst gets the same de-tested, customer-joined rows
- **History kept** — the app updates and deletes rows; the warehouse keeps each night's record
- **Joined ahead of time** — orders already carry customer city, so reports skip the join
- **One place to fix** — a cleaning bug is fixed in the pipeline once, not in fifty notebooks

*Example:* The "revenue by city" dashboard is a one-table query because the T step already joined city on.

**Key point:** a data scientist rarely touches the raw source — the warehouse table IS the dataset, so knowing how E, T, L built it tells you what you can trust.

### Visualization (canvas `c3`, 720×300)

Split diagram: heavy query blocked on the app database (left) vs one warehouse table feeding three consumers (right).

- **Title (bold 15px, `#1a5276`, top center):** "Clean Once, Use Everywhere — and Keep Heavy Queries Off the App".
- **Divider:** vertical dashed (4/3) line `#bdc3c7` at x=300.
- **Left half:** database cylinder (x=90, y=70, 110×92) filled `rgba(42,120,214,0.12)`, stroked blue `#2a78d6`, labeled bold 12px "app database" / 12px "serving customers"; below it a dashed magenta line ending in a bold magenta X mark, with magenta bold 12px annotations: "\"scan a year of revenue\"" and "not here — it slows checkouts".
- **Right half:** box (x=330, y=110, 150×62) `#f8f9fa` fill, green `#008300` 2px stroke, labeled bold 13px green "orders_daily" and 12px "12,448 rows/night"; gray arrows fan out to three consumer boxes (140×40 at x=560, `#f8f9fa` fill, 2px colored stroke, bold 12px label): "city dashboard" violet `#4a3aa7` (y≈70), "analyst notebook" aqua `#199e70` (y≈141), "churn model" yellow `#c98500` (y≈212).
- **Callout (bold 13px green `#008300`, centered under the right half at y=274):** "one cleaned table, three consumers — all seeing the same rows".

## ETL or ELT? Same Letters, Different Order

**Tags:** `common confusion` (red), `variant` (orange)

- **ELT in one line** — load the raw rows into the warehouse first, then clean them there with SQL
- **Same three steps** — only the location of the cleaning changes, not the work
- **Modern default** — cheap warehouse storage made "load raw, clean inside" common
- **Raw layer bonus** — ELT keeps the untouched 12,480 rows, so a bad transform can be redone
- **The confusion** — people debate ETL vs ELT like rival tools; it's just where T runs

*Example:* In ELT, the same 30 test orders are dropped by a SQL job inside the warehouse instead of a script outside it.

**Rule of thumb:** don't memorize the acronym — ask "where does the cleaning happen?" Before the load: ETL. After: ELT.

### Visualization (canvas `c4`, 720×300)

Two-row flow comparison: ETL pipeline on top, ELT pipeline below, each ending in a dashed warehouse box.

- **Title (bold 15px, `#1a5276`, top center):** "Same Work, Different Place: Where Does T Happen?".
- **ETL row (y=62, row label "ETL" bold 14px blue `#2a78d6` at left):** boxes (`#f8f9fa` fill, 2px colored stroke, bold 12px title + 12px sub) connected by gray arrows — "E: extract / 12,480 rows" green `#008300`, "T: clean outside / − 32 rows" orange `#d95926`, "L: load / 12,448 rows" violet `#4a3aa7`; dashed gray box at right (180×64) labeled 12px `#6b7280`: "warehouse holds" / "clean 12,448 only".
- **ELT row (y=168, row label "ELT"):** "E: extract / 12,480 rows" green, "L: load raw / 12,480 rows" violet, "T: SQL inside / − 32 rows" orange; dashed gray box at right (168×64) labeled: "warehouse holds raw" / "12,480 + clean 12,448".
- **Callouts (centered):** bold 13px aqua `#199e70` at y=262: "ELT keeps the raw 12,480 rows — a bad transform can simply be rerun"; 12px `#6b7280` at y=284: "the cleaning work is identical in both; only its address changes".

## Regeneration instructions

- **Template:** tutorials topic-page layout. `<h1>` (no index number) with 2px `#2980b9` bottom border, `.subtitle` line, then four `.card-section` blocks. Each section: `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one `<tr>`: left `<td class="text-col">` (50%) and right `<td class="viz-col">` (50%) holding one canvas.
- **Text column structure:** `.tags` pill row first, then a `<ul>` of one-line bullets each opening with `<b>bold term</b>` (bold terms colored `#1a5276`), one italic `.example` line, one `.key-point` callout (background `#f8f9fa`, left border 3px solid `#e74c3c`, padding 8px 12px, 0.9rem).
- **Tag pills:** 0.72rem, weight 600, padding 2px 10px, radius 10px; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; ul 0.92rem; canvases `width:100%` with 1px `#e0e0e0` border, 4px radius.
- **Canvas:** intrinsic 720×300 attributes; a shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates; shared `arrow()` and `cylinder()` drawing helpers. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette:** shared `P` object — blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`; site palette `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- No nav bar, no back/home links. In regenerated HTML, any card links use `.html` extensions.
