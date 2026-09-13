# Indexes

**Page type:** detail page (tutorial card-sections: h2 per section, two-column `table.layout` with text left 50% / canvas right 50%)
**HTML title tag:** Indexes

**Subtitle:** A sorted side-list that lets the database jump to a row like a phone book — reads get fast, writes pick up the bill

## Finding Chloe Among a Million Customers

**Tags:** `core idea` (blue), `running example` (green)

- **The shop grew** — customers now holds 1,000,000 rows; one query wants Chloe's row
- **No index** — the database reads every row and checks the name: a full table scan
- **The phone book** — nobody reads it page by page; it is sorted, so you jump
- **An index** — a sorted copy of one column, each entry pointing back to its row
- **Same answer** — an index changes the speed, never the result

SQL block (verbatim):

```sql
SELECT * FROM customers WHERE name = 'Chloe';

CREATE INDEX idx_name ON customers(name);
```

*Example:* Same query before and after the CREATE INDEX line — one crawls through a million rows, the other jumps.

**Key point:** An index is a sorted side-list for one column. Sorted means you can halve your way to any entry instead of reading everything.

### Visualization (canvas `c1`, 720×300)

Side-by-side comparison of full scan vs sorted-index binary search, split by a vertical dashed `#bdc3c7` divider at x=360.

- **Title (bold 15px `#1a5276`, top center):** "Two Ways to Find One Name"
- **Left — heading "full scan: check every row":** 8 unsorted row boxes (120×20) `[Ravi, Ella, Mo, Zoe, Ben, Chloe, Dev, Asha]`; every row fill `rgba(231,76,60,0.10)` with an 11px red "read" tag beside it, except the Chloe hit: fill `rgba(39,174,96,0.25)`, green `#008300` stroke, bold green text. Muted caption "… 999,992 more rows to read"; bold 13px red `#e74c3c` total: "1,000,000 reads".
- **Right — heading "index: sorted, so jump like a phone book":** 8 sorted row boxes `[Asha, Ben, Chloe, Dev, Ella, Mo, Ravi, Zoe]`; binary-search hop rows (Dev, Ben, Chloe) highlighted `rgba(42,120,214,0.15)` with blue `#2a78d6` stroke, others plain `#f7f8fa`/grid stroke; Chloe hit in green. Hop labels bold 11px: blue "1: Dev — too far" (beside Dev), blue "2: Ben, too early" (beside Ben), green "3: Chloe — found" (beside Chloe). Muted caption "each hop halves what is left"; bold 13px green total: "~20 reads at a million rows".

## 20 Checks Instead of 1,000,000

**Tags:** `worked example` (green), `rule of thumb` (blue)

- **Halving** — in a sorted list, one comparison throws away half of what's left
- **By hand** — 1,000,000 → 500,000 → 250,000 → … → 1: about 20 halvings
- **The rule** — 2²⁰ ≈ 1,000,000, so a million rows cost about 20 checks
- **1,000x bigger table** — the scan does 1,000x the work; the index adds ~10 checks
- **Doubling the table** — adds exactly one more check for the index

*Example:* This is why the customer-lookup page answers in milliseconds even though the table has a million rows.

**Key point:** Scans grow with the table; index lookups barely grow at all. The gap widens every day the table does.

### Visualization (canvas `c2`, 720×300)

Grouped bar chart on a log-height scale: rows checked, full scan vs index lookup, at three table sizes.

- **Title (bold 15px `#1a5276`, top center):** "Rows Checked to Find One Customer (bar height on log scale)"
- **Data (bar height = log10(value)/6 of the 168px chart height; baseline y=236; bars 62px wide, scan/index pair per group):**
  - "1,000 rows": scan 1,000 vs index 10
  - "100,000 rows": scan 100,000 vs index 17
  - "1,000,000 rows": scan 1,000,000 vs index 20
- **Colors:** full scan bars red `#e74c3c`; index bars green `#008300`. Bold 12px value labels above each bar (scan values comma-formatted); muted group labels below baseline.
- **Legend (top left):** red swatch "full scan", green swatch "index lookup".
- **Annotation (bold 13px green, bottom center):** "the table grew 1,000x — the index lookup grew by 10 checks"

## The Bill Arrives at Night: Writes Pay for Reads

**Tags:** `trade-off` (orange), `why it matters` (blue)

- **Every INSERT** — must also file the new row into every index, in sorted position
- **The mystery** — SELECTs became instant, yet the nightly data load got slower
- **The cause** — 8 indexes on orders: every loaded row is filed 8 extra times
- **Space too** — each index is a partial copy of the table sitting on disk
- **The deal** — pay a little on every write to save a lot on every read

*Example:* The load ran 12 minutes with no indexes and 58 minutes with eight — same rows, eight extra filings per row (illustrative timings).

**Trade-off:** Index the columns your queries actually search. Indexing "everything, just in case" makes every insert pay for lookups nobody runs.

### Visualization (canvas `c3`, 720×300)

Bar chart: nightly load time vs number of indexes.

- **Title (bold 15px `#1a5276`, top center):** "Nightly Load Time vs Indexes on the Table (illustrative)"
- **Data (bars 80px wide, baseline y=232, chart height 158px, y max 65 min):**
  - "0 indexes" = 12 min, green `#008300`
  - "1 index" = 16 min, aqua `#199e70`
  - "3 indexes" = 25 min, yellow `#c98500`
  - "8 indexes" = 58 min, red `#e74c3c`
- **Y axis:** muted labels "0 min / 20 min / 40 min / 60 min" with gridlines `#e5e9ef`; bold 13px "N min" value labels above bars; bar names below baseline.
- **Annotations:** bold 13px red near the top: "every loaded row is re-filed once per index"; muted 12px bottom center: "same rows loaded each night — only the number of indexes changed"

## An Index Answers Only the Question It Sorted For

**Tags:** `common mistake` (red)

- **Sorted by name** — a phone book finds "Chloe" fast, not "everyone in Pune"
- **Wrong column** — WHERE city = 'Pune' ignores the name index and scans anyway
- **Wrapped column** — UPPER(name) = 'CHLOE' hides the sorted value: scan again
- **Leading edge** — an index on (city, name) helps city searches, not name-only ones
- **Check, don't guess** — EXPLAIN shows whether the database jumped or scanned

*Example:* The team indexed name, then spent a week asking why the city report was still slow.

**Common mistake:** Saying "the table is indexed." Tables aren't indexed — columns are, and only queries that search those columns get the speedup.

### Visualization (canvas `c4`, 720×300)

Three stacked query cards, each marked pass or fail for a single index on name.

- **Title (bold 15px `#1a5276`, top center):** "One Index on name — Three Queries, Three Fates"
- **Cards (580×58, stacked; query in bold 13px monospace `#2c3e50`, note in muted 12px; ✓/✕ mark bold 18px):**
  - ✓ green `#008300` (fill `rgba(39,174,96,0.08)`): `WHERE name = 'Chloe'` — "jumps via the index: ~20 checks"
  - ✕ red `#e74c3c` (fill `rgba(231,76,60,0.06)`): `WHERE city = 'Pune'` — "index is sorted by name, not city: full scan"
  - ✕ red: `WHERE UPPER(name) = 'CHLOE'` — "UPPER() hides the sorted value: full scan"
- **Annotation (bold 13px orange `#d95926`, bottom center):** "same table, same index — only the first query gets the speedup"

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`, social-graph reference skeleton). Body: `<h1>` (no index number), `.subtitle`, then four `.card-section` blocks, each `<h2>` + `table.layout` with one `<tr>`: left `<td class="text-col">` (50%) and right `<td class="viz-col">` (50%) holding one 720×300 canvas.
- **Left column structure:** `.tags` pill row first, then a `<ul>` of bullets each opening with `<b>bold term</b>` (bold terms render `#1a5276`), optional `<pre class="sql">` block (monospace 0.8rem, background `#f8f9fa`, left border `3px solid #1a5276`), one italic `.example` paragraph, one `.key-point` callout (background `#f8f9fa`, left border `3px solid #e74c3c`, 0.9rem). Section 1 contains the SQL block shown above.
- **Tag pill styles:** inline-block, 0.72rem bold, padding 2px 10px, radius 10px; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` bottom border; section h2 1.3rem `#1a5276` with 2px `#2980b9` bottom border; subtitle `#666` 0.95rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** `setup(id)` reads each canvas's declared width/height attributes, sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`), and calls `ctx.scale` so drawing stays in logical coordinates. All data arrays are hardcoded literals — no `Math.random()`; invented timings labeled "illustrative" in the chart title.
- **Chart palette (`P` object):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`; alarm red `#e74c3c`. Project palette anchors: #1a5276 primary blue, #27ae60 green, #e74c3c red, #e67e22 orange.
- In regenerated HTML, any card links use `.html` extensions (this page has none).
