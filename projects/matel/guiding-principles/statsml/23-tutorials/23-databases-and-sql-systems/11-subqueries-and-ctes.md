# Subqueries & CTEs

**Page type:** detail page (tutorial: 4 `.card-section` blocks, each an h2 + two-column `table.layout` — text left 50%, canvas right 50%)
**HTML title tag:** Subqueries & CTEs

**Subtitle:** Breaking one big query into small named steps — each step is a mini-table you can read, test, and reuse

## Section 1: A Question That Won't Fit in One Step

**Tags:** `core idea` (blue), `running example` (green)

- **The ask** — "which cities have an average order above the shop's overall average?"
- **Two answers needed** — each city's average AND the overall average, then a comparison
- **Step 1** — average per city from the 8 orders: Pune $10, Delhi $11, Mumbai $8
- **Step 2** — overall average: $81 total / 8 orders = $10.13
- **Step 3** — keep cities beating $10.13: only Delhi ($11) qualifies

*Example (italic):* You would naturally do this on paper as three little tables — subqueries and CTEs let SQL do the same.

**Key point:** A subquery or CTE is just a query whose result is used as a table by the next query — steps, not magic.

### Visualization (canvas `c1`, 720×300)

Flow diagram: pipeline of three named steps feeding one another.

- **Title (bold 15px, `#1a5276`, top center):** "One Question, Three Small Steps".
- **Box "orders"** at (30,100), 130×70, gray stroke `#6b7280`, fill `#f4f6f8`; bold label "orders" in `#1a5276`, sub-label "8 rows" in `#6b7280`.
- **Box "step 1: city_stats"** at (230,52), 175×92, blue stroke `#2a78d6`; bold blue title, then three left-aligned data lines in `#2c3e50` 12px: "Pune      $10", "Delhi     $11", "Mumbai  $8".
- **Box "step 2: overall"** at (230,176), 175×62, violet stroke `#4a3aa7`; bold violet title, data line "all_avg = $10.13".
- **Box "step 3: compare"** at (490,105), 190×78, green stroke `#008300` with fill `rgba(0,131,0,0.08)`; bold green title "step 3: compare", line "city_avg > all_avg?" in `#2c3e50`, bold green result "→ Delhi".
- **Arrows** (2px lines with filled triangular heads): orders→step 1 (blue), orders→step 2 (violet), step 1→step 3 (blue), step 2→step 3 (violet).
- **Caption (bold 13px orange `#d95926`, bottom center):** "each box is a mini-table the next box treats as input".

## Section 2: The Same Numbers, Checked by Hand

**Tags:** `worked example` (green), `core idea` (blue)

- **Pune** — $8 + $5 + $15 + $12 = $40 over 4 orders: average $10
- **Delhi** — $15 + $12 + $6 = $33 over 3 orders: average $11
- **Mumbai** — one $8 order: average $8
- **Overall** — $40 + $33 + $8 = $81 over 8 orders: average $10.13
- **The verdict** — $11 > $10.13 yes; $10 and $8 no — Delhi is the answer

SQL block (`.sql`, monospace, left border `#1a5276`):

```sql
SELECT city, AVG(amount) AS city_avg
FROM   orders
GROUP  BY city
HAVING AVG(amount) >
       (SELECT AVG(amount) FROM orders);
```

*Example (italic):* The bracketed query runs first and becomes a single number, $10.13, that the outer query compares against.

**Key point:** The inner query's answer ($10.13) is computed once and slotted into the outer query like a hand-written constant.

### Visualization (canvas `c2`, 720×300)

Bar chart: city averages vs a dashed overall-average reference line.

- **Title (bold 15px `#1a5276`, top center):** "City Averages vs the Overall Average ($10.13)".
- **Data:** cities `['Pune', 'Delhi', 'Mumbai']`, averages `[10, 11, 8]`, sub-labels `['$40 / 4 orders', '$33 / 3 orders', '$8 / 1 order']`, bar colors `[#2a78d6, #008300, #d55181]`.
- **Geometry:** plot x from 120, width 460; baseline y=235, chart height 165, y-scale max 13; bar width 100. Thin gray `#999` baseline.
- **Reference line:** horizontal dashed orange (`#d95926`, dash 7/5, width 2.5) at value 10.13, labeled to the right in bold orange on two lines: "overall" / "$10.13".
- **Bars:** filled in their color if the city average beats 10.13 (only Delhi); losing bars filled gray `rgba(107,114,128,0.35)`. Bold value label "$10"/"$11"/"$8" above each bar in `#2c3e50`; city name and gray `#6b7280` sub-label below the baseline.
- **Annotations:** bold green `#008300` "only Delhi clears the line" above the Delhi bar (y≈58); bottom caption in gray 12px: "the inner query computes the dashed line; the outer query keeps bars above it".

## Section 3: WITH: Give Each Step a Name

**Tags:** `core idea` (blue), `why it matters` (orange)

- **Nested pain** — queries inside queries read inside-out; three levels deep is a puzzle
- **WITH city_stats AS (...)** — names step 1; the next step uses it like a real table
- **Reads top-down** — the CTE version reads in the order you would explain it aloud
- **Debug one step** — run SELECT * FROM city_stats alone and eyeball the 3 rows
- **Real pipelines** — production analytics queries are often 5-10 named steps chained this way

SQL block:

```sql
WITH city_stats AS (
  SELECT city, AVG(amount) AS city_avg
  FROM orders GROUP BY city
), overall AS (
  SELECT AVG(amount) AS all_avg FROM orders
)
SELECT city FROM city_stats, overall
WHERE  city_avg > all_avg;   -- Delhi
```

*Example (italic):* A teammate can read city_stats, then overall, then the final SELECT — never holding more than one step in their head.

**Key point:** Same result as the nested version — the win is that every intermediate table now has a name you can read and test.

### Visualization (canvas `c3`, 720×300)

Side-by-side diagram: nested subquery boxes (left) vs flat CTE pipeline (right), split by a vertical dashed divider `#bdc3c7` (dash 4/3) at x=360.

- **Title (bold 15px `#1a5276`, top center):** "Nested Reads Inside-Out — CTEs Read Top-Down".
- **Left half** (heading bold 13px red `#e74c3c`, centered at x=185): "nested subqueries". Three concentric boxes: outer gray `#6b7280` box (45,66) 280×190 labeled "SELECT ... FROM (", middle violet `#4a3aa7` box (75,96) 220×120 labeled "SELECT ... FROM (", inner blue `#2a78d6` box (105,126) 160×58 labeled "SELECT ..." / "FROM orders"; closing ")" characters in violet and gray. Bottom caption bold red: "reading starts at the innermost box".
- **Right half** (heading bold 13px green `#008300`, centered at x=540): "CTE pipeline". Three stacked 240×40 boxes at x=420, y=70/136/202, connected by downward gray arrows: "WITH city_stats AS (...)" (blue `#2a78d6`), "WITH overall AS (...)" (violet `#4a3aa7`), "SELECT ... final compare" (green `#008300`). Bottom caption bold green: "reads in the order you would explain it".

## Section 4: A CTE Is a Name, Not a Saved Table

**Tags:** `common mistake` (red), `why it matters` (orange)

- **Not stored** — city_stats vanishes when the query finishes; nothing lands on disk
- **Not automatically faster** — naming a step does not make the database do less work
- **One query's scope** — the next query in your notebook cannot see city_stats
- **Multi-row trap** — a subquery used where one value is expected errors if it returns 3 rows
- **The payoff is human** — fewer bugs, reviewable steps, testable pieces — not speed

*Example (italic):* Comparing amount = (subquery returning Pune, Delhi, Mumbai) fails — use IN when the step returns a list.

**Rule of thumb:** Reach for a CTE when a query stops fitting in your head; reach for a real table (CREATE TABLE AS) when other queries need the result too.

### Visualization (canvas `c4`, 720×300)

Side-by-side diagram: CTE lifetime (left) vs stored table (right), vertical dashed divider `#bdc3c7` at x=360.

- **Title (bold 15px `#1a5276`, top center):** "What a CTE Is — and Is Not".
- **Column headings (bold 13px, centered):** left blue `#2a78d6` "CTE: a name inside one query"; right aqua `#199e70` "CREATE TABLE AS: saved for everyone".
- **Left:** blue box (60,76) 250×120 labeled bold "query runs..."; inside it a violet `#4a3aa7` box (95,108) 180×40 with fill `rgba(74,58,167,0.08)` labeled "city_stats (3 rows)"; gray text "...query finishes"; then bold red `#e74c3c` "city_stats is gone" and gray 12px "next query: \"table not found\"".
- **Right:** an aqua-stroked database cylinder centered at (540,120), 140 wide, fill `rgba(25,158,112,0.10)`, labeled bold aqua "city_stats on disk". Three consumers labeled "query A", "query B", "dashboard" at y=232 (x = 430/540/650) with gray arrows pointing up to the cylinder. Bold aqua caption: "reusable — but you must refresh it yourself".
- **Bottom caption (bold 12px orange `#d95926`, centered):** "choose by audience: one query → CTE, many queries → real table".

## Regeneration instructions

- **Layout:** tutorial detail page. h1 (2rem `#1a5276`, 2px solid `#2980b9` bottom border) + `.subtitle` (`#666`, 0.95rem), then four `.card-section` blocks. Each section: `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `.text-col` (50%) holding `.tags`, a `<ul>` of bullets, optional `<pre class="sql">`, `.example` italic line, and `.key-point` callout; `.viz-col` (50%) holding the canvas.
- **Text styles:** body system-ui sans, white background, text `#2c3e50`, padding 40px, line-height 1.6; bullets 0.92rem with bold lead terms `<b>` in `#1a5276`; `.example` italic `#555` 0.9rem; `.key-point` background `#f8f9fa`, left border `3px solid #e74c3c`, 0.9rem; `.sql` background `#f8f9fa`, left border `3px solid #1a5276`, monospace 0.8rem.
- **Tag pills:** `.tag` inline pill, 0.72rem bold, radius 10px — blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Canvas:** 720×300 intrinsic, CSS `width:100%`, 1px `#e0e0e0` border, 4px radius; scaled by `window.devicePixelRatio` via a shared `setup(id)` helper (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates). Shared helpers draw stroked boxes and arrows with triangular heads.
- **Chart palette (JS `P` object):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- No nav bar, no back/home links, no cross-page links. In regenerated HTML any card links would use `.html` extensions.
