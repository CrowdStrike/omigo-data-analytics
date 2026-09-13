# GROUP BY & Aggregation

**Page type:** detail page (tutorial page: h2 card-sections, each a two-column table.layout — text left 50%, canvas right 50%)
**HTML title tag:** GROUP BY & Aggregation

**Subtitle:** Collapsing many rows into one summary row per group — counts, sums, and averages instead of the raw detail

## From 8 Orders to 3 City Rows

Tags: `core idea` (blue), `running example` (green)

- **The question** — the shop asks "how much did each city buy?", not "show me every order"
- **By hand** — deal the 8 orders into piles by city: Pune gets 4, Delhi 3, Mumbai 1
- **GROUP BY city** — tells the database to make exactly those piles for you
- **One row per pile** — 8 order rows go in, 3 city rows come out
- **The detail is gone** — you can no longer see order 1004 in the output, only its pile

*Example (italic):* Sorting receipts into three trays labeled Pune, Delhi, Mumbai — then reporting one line per tray. (The orphaned order 1008 from the joins page has since been fixed: it was Ella's, $6, Delhi.)

**Key point:** GROUP BY collapses rows into piles. The output has one row per distinct value of the grouping column — never one row per order.

### Visualization (canvas `c1`, 720×300)

Fan-out diagram: an 8-row orders table on the left with colored lines gathering rows into three city pile boxes on the right.

- **Title (bold 15px `#1a5276`, top center):** "GROUP BY city: 8 Rows In, 3 Piles Out".
- **Orders data** (order_id, customer, city, amount): 1001/Asha/Pune/$8, 1002/Ben/Delhi/$15, 1003/Asha/Pune/$5, 1004/Chloe/Mumbai/$8, 1005/Ben/Delhi/$12, 1006/Dev/Pune/$15, 1007/Asha/Pune/$12, 1008/Ella/Delhi/$6.
- **City colors:** Pune blue `#2a78d6` (bg `rgba(42,120,214,0.10)`), Delhi green `#008300` (bg `rgba(0,131,0,0.10)`), Mumbai magenta `#d55181` (bg `rgba(213,81,129,0.12)`).
- **Left table** at (30, 44), columns order_id/customer/city/amount (widths 62/72/66/60, row height 26), header `#1a5276` white bold; each row tinted and text-colored by its city.
- **Pile boxes** on the right (190×34 boxes at x=474, converge point x=470): "Pune pile: 4 orders" at y=90, "Delhi pile: 3 orders" at y=160, "Mumbai pile: 1 order" at y=230 — each box filled with the city background tint, stroked 2px in the city color, bold 13px city-colored label.
- **Connector lines:** 1.5px city-colored lines from the right edge of each order row's center to its pile box.
- **Annotation (bold 13px orange `#d95926`, left-aligned at x=484, y=276):** "one output row per pile".

## COUNT, SUM, AVG: The Numbers on Each Pile

Tags: `worked example` (green), `core idea` (blue)

- **Pune pile** — orders 1001, 1003, 1006, 1007: count 4, sum $8+$5+$15+$12 = $40
- **Delhi pile** — orders 1002, 1005, 1008: count 3, sum $15+$12+$6 = $33
- **Mumbai pile** — only order 1004: count 1, sum $8
- **AVG is sum ÷ count** — Pune $40/4 = $10, Delhi $33/3 = $11, Mumbai $8/1 = $8
- **One number per pile** — each aggregate squeezes a whole pile into a single value

SQL block:

```sql
SELECT city, COUNT(*), SUM(amount), AVG(amount)
FROM   orders
GROUP  BY city;   -- 3 rows
```

*Example (italic):* Check Delhi yourself: 15 + 12 + 6 = 33, and 33 divided by 3 orders is 11.

**Key point:** An aggregate function (COUNT, SUM, AVG, MIN, MAX) answers one question per pile — that is why it pairs with GROUP BY.

### Visualization (canvas `c2`, 720×300)

Result table plus SUM bar chart.

- **Title (bold 15px `#1a5276`, top center):** "The 3-Row Result — Every Number Checkable by Hand".
- **Left result table** at (40, 60), columns city/COUNT(*)/SUM/AVG (widths 70/72/55/55, row height 28): Pune/4/$40/$10 (bg `rgba(42,120,214,0.10)`), Delhi/3/$33/$11 (bg `rgba(0,131,0,0.10)`), Mumbai/1/$8/$8 (bg `rgba(213,81,129,0.12)`).
- **Bar chart** (plot x=360, width 320, baseline y=232, chart height 150, y-scale max 45, bar width 74): SUM bars Pune $40 (blue `#2a78d6`), Delhi $33 (green `#008300`), Mumbai $8 (magenta `#d55181`). Value labels bold 13px `#2c3e50` above bars ("$40", "$33", "$8"); city names 12px below baseline; muted sublabels "4 orders, avg $10" / "3 orders, avg $11" / "1 orders, avg $8" below those. Gray `#999` baseline.
- **Chart headings (centered over bars):** bold 13px blue "Pune: 8+5+15+12 = $40" (y=58); 12px muted "SUM(amount) per city" (y=78).
- **Annotation (bold 12px orange `#d95926`, left-aligned at x=60):** two lines "the 8 orders are gone —" (y=210) / "only their summaries remain" (y=228).

## HAVING: Filtering Piles, Not Orders

Tags: `core idea` (blue), `why it matters` (orange)

- **New question** — "which cities bought at least $30 in total?" filters the piles themselves
- **WHERE can't do it** — WHERE sees one order at a time; a pile's total does not exist yet
- **HAVING SUM >= 30** — runs after the piles are built: Pune $40 and Delhi $33 pass
- **Mumbai dropped** — its $8 pile fails the test, so the whole city row disappears
- **Everywhere in practice** — revenue per region, events per user, error counts per server: all GROUP BY

SQL block:

```sql
SELECT city, SUM(amount) AS total
FROM   orders
GROUP  BY city
HAVING SUM(amount) >= 30;   -- Pune, Delhi
```

*Example (italic):* "Users with more than 100 events" and "cities above $30" are the same shape: build piles, then test each pile.

**Key point:** WHERE filters rows before the piles exist; HAVING filters the finished piles. Nearly every dashboard metric is a GROUP BY with one of these.

### Visualization (canvas `c3`, 720×300)

Stage-pipeline boxes plus pile bars against a threshold line.

- **Title (bold 15px `#1a5276`, top center):** "HAVING SUM(amount) >= 30: Tests Whole Piles".
- **Stage boxes** (150×44 at y=54, fill `#f4f6f8`, 2px colored stroke, bold 14px colored label + 12px muted sublabel, muted arrows between them):
  - x=40: "8 orders" / "raw rows" (muted `#6b7280`).
  - x=230: "3 piles" / "GROUP BY city" (blue `#2a78d6`).
  - x=470: "2 piles" / "HAVING >= $30" (green `#008300`).
- **Pile bars** (plot x=120, width 440, baseline y=262, chart height 118, y-scale max 45, bar width 92): Pune $40 (blue), Delhi $33 (green), Mumbai $8 — Mumbai fails the threshold so it is drawn in faded `rgba(213,81,129,0.30)` with bold 12px red label "whole pile dropped" above its value. Value labels bold 13px above bars; city names 12px below baseline.
- **Threshold line:** dashed red `#e74c3c` (dash 6/4, 2px) horizontal at the $30 level, labeled "$30 bar" in bold 12px red to the right.
- **Annotation (bold 13px green `#008300`, left-aligned at x=130, y=130):** "the test compares pile totals — no single order is ever $30".

## The Average of Averages Is Not the Average

Tags: `common mistake` (red), `why it matters` (orange)

- **The trap** — averaging the 3 city averages: (10 + 11 + 8) / 3 = $9.67 per order
- **The truth** — the real average is total $81 over all 8 orders = $10.13
- **Why they differ** — Mumbai's single $8 order counts as much as Pune's whole pile of 4
- **Small piles shout** — the fewer orders a group has, the more its average distorts the mix
- **The fix** — go back to the raw rows: AVG(amount) with no grouping, or SUM/SUM across groups

*Example (italic):* A dashboard averaged per-country averages and a 3-order country moved the global number more than a 3-million-order one.

**Rule of thumb:** Never average a column that is itself an average unless every group has the same number of rows — weight by count instead.

### Visualization (canvas `c4`, 720×300)

Two-bar comparison of the wrong and right average.

- **Title (bold 15px `#1a5276`, top center):** 'Two "Average Order Values" from the Same 8 Orders'.
- **Bars** (plot x=100, baseline y=226, chart height 150, y-scale max 12, bar width 150, gap 130; gray `#999` baseline):
  - Left: $9.67, red `#e74c3c`; label "average of the 3 city averages", sublabel "(10 + 11 + 8) / 3".
  - Right: $10.13, green `#008300`; label "true average over all 8 orders", sublabel "$81 / 8".
  - Value labels bold 14px `#2c3e50` above bars; labels 12px below baseline; sublabels 12px muted below those.
- **Annotations (centered):** bold 13px red "Mumbai (1 order) pulls as hard as Pune (4 orders)" (y=62); 12px muted "the gap grows with how unequal the pile sizes are" (y=82); 12px muted bottom caption "weight each group by its COUNT, or aggregate the raw rows directly" (y=284).

## Regeneration instructions

- **Layout:** tutorial detail page. `<h1>` + `.subtitle`, then four `.card-section` blocks. Each section: `<h2>` (1.3rem `#1a5276`, 2px solid `#2980b9` bottom border), then `table.layout` with one `<tr>`: left `td.text-col` (50%) holding `.tags` pills, a `<ul>` of bullets (each opening with `<b>` term in `#1a5276`), an optional `.sql` `<pre>` block, an italic `.example` paragraph, and a `.key-point` callout; right `td.viz-col` (50%) holding one `<canvas>` 720×300 with `width:100%`, 1px `#e0e0e0` border, 4px radius.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem; `ul` 0.92rem; `.example` italic `#555` 0.9rem; `.key-point` background `#f8f9fa`, left border 3px solid `#e74c3c`, 0.9rem; `.sql` background `#f8f9fa`, left border 3px solid `#1a5276`, ui-monospace 0.8rem.
- **Tag pills:** `.tag` inline-block 0.72rem bold, padding 2px 10px, radius 10px; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Palette:** primary blue `#1a5276` (ink), green `#27ae60`, red `#e74c3c`, orange `#e67e22`; chart palette object P: blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`.
- **Canvas:** intrinsic 720×300 attributes; scale by `window.devicePixelRatio` via a shared `setup(id)` helper (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates). A shared `drawTable(ctx,x,y,cols,widths,rows,opt)` helper draws data tables with `#1a5276` header (white bold 12px text), alternating white/`#f4f6f8` rows, optional per-row background/color/bold overrides, `#cfd8e0` 1px outer border, and a `rowCenter(r)` accessor for connector lines. Shared ORDERS array (8 rows) plus CITY_COLOR / CITY_BG maps. No nav bar, no back/home links. In regenerated HTML, any card links use .html extensions.
