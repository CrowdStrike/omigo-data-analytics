# NULL Behavior in SQL

**Page type:** detail page (tutorial: 4 `.card-section` blocks, each an h2 + two-column `table.layout` — text left 50%, canvas right 50%)
**HTML title tag:** NULL Behavior in SQL

**Subtitle:** NULL means "unknown", not zero — and that quietly changes how comparisons, counts, and joins behave

## Section 1: Two Holes Appear in the Orders Table

**Tags:** `core idea` (blue), `running example` (green)

- **Order 1009** — Farid skipped the city field at checkout: city is NULL
- **Order 1010** — Chloe's payment is still processing: amount is NULL
- **NULL is not 0** — 1010's amount isn't zero dollars; it simply isn't known yet
- **NULL is not ""** — Farid's city isn't a blank name; there is no value at all
- **Read it as "unknown"** — every strange NULL rule below follows from that one word

*Example (italic):* A paper form with an empty box: the customer has a city, the shop just doesn't know which one.

**Key point:** NULL is a missing value, not a value. The moment you read it as "unknown", its odd behavior becomes logical.

### Visualization (canvas `c1`, 720×300)

Rendered data table with two highlighted NULL cells and callout labels.

- **Title (bold 15px `#1a5276`, top center):** "The Orders Table with Two Unknowns".
- **Table** drawn at (60,40), row height 23, columns `order_id / customer / city / amount` (widths 64/74/70/62), header row filled `#1a5276` with white bold 12px text; alternating body rows `#fff` / `#f4f6f8`. Rows:
  - 1001 Asha Pune $8; 1002 Ben Delhi $15; 1003 Asha Pune $5; 1004 Chloe Mumbai $8; 1005 Ben Delhi $12; 1006 Dev Pune $15; 1007 Asha Pune $12; 1008 Ella Delhi $6
  - 1009 Farid **NULL** $9 and 1010 Chloe Mumbai **NULL** — these two rows get background `rgba(231,76,60,0.08)`; every literal "NULL" cell rendered bold red `#e74c3c`. Outer border `#cfd8e0`.
- **Callouts (bold 13px red, left-aligned at x=380, connected by 1.5px red leader lines from the row edge):** "city unknown — form field skipped" (row 1009), "amount unknown — payment pending" (row 1010).
- **Annotation (bold 13px orange `#d95926`, two lines at x=380):** "NULL is a hole in the table," / "not a zero and not an empty string". Below in gray `#6b7280` 12px: "8 complete rows + 2 rows with one" / "missing fact each = 10 rows total".

## Section 2: Delhi, Not-Delhi... and Neither

**Tags:** `worked example` (green), `count traps` (red)

- **city = 'Delhi'** — returns 3 rows: orders 1002, 1005, 1008
- **city <> 'Delhi'** — returns 6 rows: the 4 Pune and 2 Mumbai orders
- **3 + 6 = 9, not 10** — Farid's order 1009 is in neither result
- **Why** — NULL = 'Delhi' is UNKNOWN, and NULL <> 'Delhi' is UNKNOWN too
- **WHERE keeps only TRUE** — UNKNOWN rows are silently filtered on both sides

SQL block (`.sql`, monospace, left border `#1a5276`):

```sql
WHERE city = 'Delhi'    -- 3 rows
WHERE city <> 'Delhi'   -- 6 rows  (not 7!)
WHERE city IS NULL      -- 1 row: order 1009
```

*Example (italic):* Is an unknown city Delhi? Maybe. Is it not Delhi? Also maybe — so SQL answers "unknown" to both questions.

**Key point:** SQL logic has three answers — TRUE, FALSE, UNKNOWN — and WHERE only keeps TRUE. Test for missing values with IS NULL, never = NULL.

### Visualization (canvas `c2`, 720×300)

Three-bar chart splitting the 10 rows.

- **Title (bold 15px `#1a5276`, top center):** "WHERE city = 'Delhi' vs city <> 'Delhi': 3 + 6 = 9 Rows, Not 10".
- **Bars** (width 140, baseline y=220, chart height 140, y-scale max 7, thin `#999` baseline from x=40 to 680):
  - "city = 'Delhi'" at x=60: 3 rows, green `#008300`, ids "1002, 1005, 1008"
  - "city <> 'Delhi'" at x=270: 6 rows, blue `#2a78d6`, ids "1001, 1003, 1004, 1006, 1007, 1010"
  - "neither!" at x=520: 1 row, red `#e74c3c` (globalAlpha 0.85), ids "1009 (city NULL)"
  - Bold 14px count label ("3 rows" etc.) above each bar in `#2c3e50`; bold 12px group label in the bar color and 11px gray id list below the baseline.
- **Annotation (bold 13px red, centered at x=590, three lines):** "order 1009 fails BOTH filters:" / "NULL = Delhi → UNKNOWN" / "NULL <> Delhi → UNKNOWN".
- **Caption (gray 12px, bottom center):** "WHERE keeps TRUE only — UNKNOWN is dropped on both sides; only IS NULL finds row 1009".

## Section 3: COUNT and AVG Quietly Skip the Holes

**Tags:** `worked example` (green), `count traps` (red)

- **COUNT(*) = 10** — counts rows, holes and all
- **COUNT(amount) = 9** — counts values, so 1010's NULL amount is skipped
- **SUM(amount) = $90** — the nine known amounts: $81 from 1001-1008 plus Farid's $9
- **AVG(amount) = $10** — $90 / 9 known values, not $90 / 10 rows
- **If NULL meant zero** — the average would be $9; treating unknown as $0 shifts every stat

SQL block:

```sql
SELECT COUNT(*),        -- 10
       COUNT(amount),   -- 9
       AVG(amount)      -- 90 / 9 = 10
FROM   orders;
```

*Example (italic):* Two "order counts" from one table, 10 and 9 — both correct, answering different questions.

**Key point:** Aggregates ignore NULLs. COUNT(*) vs COUNT(column) differing is your cheapest missing-data detector — and COALESCE(amount, 0) is how you opt in to "treat as zero".

### Visualization (canvas `c3`, 720×300)

Two paired bar charts split by a vertical dashed divider `#bdc3c7` (dash 4/3) at x=340.

- **Title (bold 15px `#1a5276`, top center):** "Aggregates Skip NULLs: 10 Rows, 9 Values".
- **Left pair** (baseline y=225, chart height 145, y-scale max 11, bars 100 wide at x=55 and 190, `#999` baseline):
  - "COUNT(*)" = 10, blue `#2a78d6`, sub-label "rows"
  - "COUNT(amount)" = 9, violet `#4a3aa7`, sub-label "known values"
  - Bold 14px value above each bar; annotation bold 12px red centered between them: "the gap = order 1010" / "(amount NULL)".
- **Right pair** (bars 110 wide at x=390 and 550, same scale/baseline):
  - "AVG(amount)" = $10, green `#008300`, sub-label "$90 / 9 — NULL skipped"
  - "if NULL were $0" = $9, orange `#d95926`, sub-label "$90 / 10 — wrong default"
  - Annotation bold 12px green centered above: "\"unknown\" and \"$0\" are different claims —" / "$1 apart on average, here".

## Section 4: NULL Never Equals Anything — Not Even NULL

**Tags:** `common mistake` (red), `why it matters` (orange)

- **NULL = NULL is UNKNOWN** — two unknown cities aren't known to match
- **Joins drop them** — joining on city silently discards Farid's NULL-city row
- **NOT IN bites hardest** — one NULL in the list makes NOT IN return zero rows
- **Arithmetic infects** — amount + NULL is NULL; one hole blanks the whole expression
- **Where it hurts** — every real dataset has NULLs, so every filter, join, and metric hits this

*Example (italic):* A revenue dashboard "lost" orders for weeks: a join on a nullable region column was silently dropping them.

**Rule of thumb:** Whenever a filter, join key, or NOT IN list can contain NULL, decide explicitly: IS NULL branch, COALESCE default, or accept the dropped rows on purpose.

### Visualization (canvas `c4`, 720×300)

Two-panel diagram split by a vertical dashed divider `#bdc3c7` at x=360.

- **Title (bold 15px `#1a5276`, top center):** "Two Famous NULL Ambushes".
- **Left panel — join drop.** Heading bold 13px blue `#2a78d6` centered at x=185: "JOIN cities ON orders.city = cities.city". Two mini tables (same rendered-table style as c1, row height 26): orders table at (50,66) with columns order_id/city and rows `1004 Mumbai`, `1006 Pune`, `1009 NULL` (NULL bold red); cities table at (235,66) with columns city/zone and rows `Mumbai W`, `Pune W`, `Delhi N`. Solid green `#008300` match lines connect Mumbai→Mumbai and Pune→Pune; a dashed red 2px line from row 1009 stops short of the cities table. Annotation bold 12px red: "NULL matches no city row —" / "order 1009 silently leaves the report"; gray 11px: "NULL = NULL is UNKNOWN, so even two" / "NULL keys refuse to match each other".
- **Right panel — NOT IN trap.** Heading bold 13px violet `#4a3aa7` centered at x=540: "city NOT IN ('Delhi', NULL)". Four evaluation lines (monospace left column at x=400, bold colored verdict at x=575, 34px apart):
  - `city <> 'Delhi'` — "TRUE for Pune" (green)
  - `AND city <> NULL` — "UNKNOWN — always" (red)
  - `= TRUE AND UNKNOWN` — "UNKNOWN" (red)
  - `WHERE keeps TRUE only` — "row dropped" (red)
  - Annotation bold 13px red: "one NULL in the list → NOT IN" / "returns 0 rows, for every city"; gray 11px: "fix: NOT EXISTS, or filter NULLs out of the list".

## Regeneration instructions

- **Layout:** tutorial detail page. h1 (2rem `#1a5276`, 2px solid `#2980b9` bottom border) + `.subtitle` (`#666`, 0.95rem), then four `.card-section` blocks. Each section: `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `.text-col` (50%) holding `.tags`, a `<ul>` of bullets, optional `<pre class="sql">`, `.example` italic line, and `.key-point` callout; `.viz-col` (50%) holding the canvas.
- **Text styles:** body system-ui sans, white background, text `#2c3e50`, padding 40px, line-height 1.6; bullets 0.92rem with bold lead terms `<b>` in `#1a5276`; `.example` italic `#555` 0.9rem; `.key-point` background `#f8f9fa`, left border `3px solid #e74c3c`, 0.9rem; `.sql` background `#f8f9fa`, left border `3px solid #1a5276`, monospace 0.8rem.
- **Tag pills:** `.tag` inline pill, 0.72rem bold, radius 10px — blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Canvas:** 720×300 intrinsic, CSS `width:100%`, 1px `#e0e0e0` border, 4px radius; scaled by `window.devicePixelRatio` via a shared `setup(id)` helper. A shared `drawTable` helper renders header-and-rows mini tables (header fill `#1a5276`, alternating row fills, literal "NULL" cells bold red `#e74c3c`, border `#cfd8e0`); it exposes row-center coordinates for leader lines.
- **Chart palette (JS `P` object):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- No nav bar, no back/home links, no cross-page links. In regenerated HTML any card links would use `.html` extensions.
