# Window Functions

**Page type:** detail page (tutorial page: h2 card-sections, each a two-column table.layout — text left 50%, canvas right 50%)
**HTML title tag:** Window Functions

**Subtitle:** Rankings and running totals computed by looking at neighboring rows — while keeping every row instead of collapsing them

## Keep All 8 Rows, Add a Column

Tags: `core idea` (blue), `running example` (green)

- **New question** — "for each order, is it the biggest in its city?" needs order AND city context
- **GROUP BY can't** — it collapses the 8 orders into 3 city rows; the orders vanish
- **A window** — each row peeks at its neighbors (its city's other orders) without merging
- **Same row count** — 8 rows in, 8 rows out, plus one new computed column
- **The keyword** — OVER (...) is what turns SUM or RANK into a window function

*Example (italic):* Every student keeps their own exam sheet, but a "rank in class" is written in the corner of each one.

**Key point:** GROUP BY answers "one number per group". A window function answers "one number per row, computed from its group".

### Visualization (canvas `c1`, 720×300)

Side-by-side comparison: the collapsed GROUP BY result vs the full window-function result.

- **Title (bold 15px `#1a5276`, top center):** "Same Question Family, Two Shapes of Answer".
- **Divider:** vertical dashed line (`#bdc3c7`, dash 4/3) at x=300 from y=35 to y=285.
- **Left panel — heading (bold 13px violet `#4a3aa7`, centered x=150, y=52):** "GROUP BY: collapses". Table at (55, 64), columns city/MAX (widths 80/55, row height 24): Pune/$15, Delhi/$15, Mumbai/$8. Captions below: bold 12px violet "3 rows — orders gone" (y=190); 12px muted "which order was the $15?" (y=210) and "impossible to say" (y=227).
- **Right panel — heading (bold 13px green `#008300`, centered x=505, y=52):** "window: keeps rows, adds max_in_city". Table at (340, 64), columns order_id/city/amount/max_in_city (widths 62/62/60/86, row height 24), 8 rows: 1001/Pune/$8/$15, 1002/Delhi/$15/$15, 1003/Pune/$5/$15, 1004/Mumbai/$8/$8, 1005/Delhi/$12/$15, 1006/Pune/$15/$15, 1007/Pune/$12/$15, 1008/Delhi/$6/$15 — rows 1002, 1004, 1006 highlighted `rgba(0,131,0,0.12)`.
- **Right annotation (bold 12px green, left-aligned at x=630):** three lines "8 rows kept —" (y=120), "new column" (y=138), "per row" (y=156).

## RANK Inside Each City: PARTITION BY

Tags: `worked example` (green), `core idea` (blue)

- **PARTITION BY city** — draw invisible fences: Pune rows, Delhi rows, Mumbai rows
- **ORDER BY amount DESC** — inside each fence, sort biggest first and number them
- **Pune by hand** — $15 (1006) is #1, $12 (1007) #2, $8 (1001) #3, $5 (1003) #4
- **Delhi by hand** — $15 (1002) #1, $12 (1005) #2, $6 (1008) #3
- **Ranks restart** — Mumbai's lone $8 order is #1 in its own fence

SQL block:

```sql
SELECT order_id, city, amount,
       RANK() OVER (PARTITION BY city
                    ORDER BY amount DESC) AS rk
FROM   orders;   -- still 8 rows
```

*Example (italic):* Order 1001's $8 ranks 3rd in Pune, while the same $8 in Mumbai (1004) ranks 1st — rank is relative to the fence.

**Key point:** PARTITION BY decides who your neighbors are; ORDER BY decides how they line up. The rank counter resets at every fence.

### Visualization (canvas `c2`, 720×300)

Three partitioned mini-tables separated by dashed fence lines.

- **Title (bold 15px `#1a5276`, top center):** "RANK() OVER (PARTITION BY city ORDER BY amount DESC)".
- **Three partitions**, each a mini-table (columns order_id/amount/rk, widths 62/62/36, row height 26) at y=66 with a bold 13px city-colored heading above (y=56):
  - "Pune fence" at x=30, blue `#2a78d6` header, rows tinted `rgba(42,120,214,0.10)`: 1006/$15/1, 1007/$12/2, 1001/$8/3, 1003/$5/4 (rank-1 row bold).
  - "Delhi fence" at x=275, green `#008300` header, rows tinted `rgba(0,131,0,0.10)`: 1002/$15/1, 1005/$12/2, 1008/$6/3.
  - "Mumbai fence" at x=520, magenta `#d55181` header, row tinted `rgba(213,81,129,0.12)`: 1004/$8/1.
- **Fence lines:** orange `#d95926` dashed vertical lines (dash 6/5, 2px) between adjacent partitions, from y=48 to y=220.
- **Captions (centered):** bold 13px orange "the rank counter restarts at every fence — three separate #1s" (y=254); 12px muted "same $8: rank 3 of 4 in Pune (order 1001), rank 1 of 1 in Mumbai (order 1004)" (y=276).

## Running Total: Each Row Sums Everything Up to Itself

Tags: `worked example` (green), `core idea` (blue)

- **The question** — "after each order, how much has the shop earned so far?"
- **SUM ... OVER (ORDER BY order_id)** — each row adds itself to all earlier rows
- **By hand** — 8, then 8+15 = 23, then 23+5 = 28, 36, 48, 63, 75, 81
- **Last row = grand total** — order 1008 shows $81, the sum of all 8 amounts
- **Still 8 rows** — every order keeps its own line with its own so-far number

SQL block:

```sql
SELECT order_id, amount,
       SUM(amount) OVER (ORDER BY order_id)
         AS running_total
FROM   orders;
```

*Example (italic):* Check the middle: after order 1005, the shop has earned 8+15+5+8+12 = $48.

**Key point:** ORDER BY inside OVER makes the window grow row by row — that growing window is exactly a running total.

### Visualization (canvas `c3`, 720×300)

Stacked staircase bar chart: cumulative total per order, with each bar split into "everything before" and "this row's amount".

- **Title (bold 15px `#1a5276`, top center):** "Running Total After Each Order".
- **Data:** order ids `[1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008]`; amounts `[8, 15, 5, 8, 12, 15, 12, 6]`; running totals `[8, 23, 28, 36, 48, 63, 75, 81]`.
- **Bars** (plot x=70, width 590, baseline y=240, chart height 175, y-scale max 90, bar width 48; gray `#999` baseline): each bar's lower segment (previous total) filled `rgba(42,120,214,0.30)`, upper segment (this order's amount) filled solid orange `#d95926`. Bold 12px `#2c3e50` cumulative label above each bar ("$8", "$23", … "$81"); 11px muted order id below the baseline and "+amount" (e.g. "+8") beneath it.
- **Legend annotations (bold 13px, left-aligned at x=76):** orange "orange = this row" (y=60); blue `#2a78d6` "blue = everything before it" (y=80). Bold 13px green `#008300`, right-aligned at the plot's right edge (y=60): "last row = grand total $81".
- **Bottom caption (12px muted, centered, y=285):** "ORDER BY order_id inside OVER( ) — the window grows one row at a time".

## Where You'll Need It: Top-1 per Group

Tags: `why it matters` (orange), `common mistake` (red)

- **The classic ask** — "the biggest order in each city" needs the whole row, not just the max
- **GROUP BY trap** — MAX(amount) gives $15 per city but cannot tell you which order it was
- **Window fix** — rank within city, then keep rank 1: orders 1006, 1002, 1004
- **Same trick everywhere** — latest event per user, best model per experiment, dedup by ROW_NUMBER
- **Mind the tie** — RANK gives two #1s on a tie; ROW_NUMBER always picks exactly one

SQL block:

```sql
SELECT * FROM (
  SELECT o.*, ROW_NUMBER() OVER (
    PARTITION BY city ORDER BY amount DESC) rn
  FROM orders o) t
WHERE rn = 1;   -- 1006, 1002, 1004
```

*Example (italic):* "Each customer's most recent order" is the same query with ORDER BY order date instead of amount.

**Rule of thumb:** The moment a question contains "per group" AND "which row", reach for a window function, not GROUP BY.

### Visualization (canvas `c4`, 720×300)

Ranked table with arrows from the rn=1 rows to an answer box.

- **Title (bold 15px `#1a5276`, top center):** "Biggest Order per City: Rank, Then Keep rn = 1".
- **Left table** at (40, 44), columns order_id/customer/city/amount/rn (widths 62/70/64/58/32, row height 25), rows in partition order: 1006/Dev/Pune/$15/1, 1007/Asha/Pune/$12/2, 1001/Asha/Pune/$8/3, 1003/Asha/Pune/$5/4, 1002/Ben/Delhi/$15/1, 1005/Ben/Delhi/$12/2, 1008/Ella/Delhi/$6/3, 1004/Chloe/Mumbai/$8/1. rn=1 rows highlighted `rgba(0,131,0,0.14)` and bold; other rows tinted `rgba(107,114,128,0.08)`.
- **Answer box** at (420, 90), 250×110: fill `rgba(0,131,0,0.08)`, stroke green `#008300` 2px; bold 13px green heading "WHERE rn = 1  →  3 rows"; three 12px `#2c3e50` result lines: "1006  Dev    Pune     $15", "1002  Ben    Delhi    $15", "1004  Chloe  Mumbai   $8".
- **Arrows:** 1.6px green lines from the right edge of each rn=1 table row to the corresponding line in the answer box.
- **Annotations (left-aligned at x=420):** bold 12px orange `#d95926` two lines "the whole row survives — customer," (y=230) / "order_id and all; MAX per city loses them" (y=248); 12px muted two lines "if one city had two $15 orders, RANK" (y=274) / "would keep both as #1; ROW_NUMBER picks one" (y=290).

## Regeneration instructions

- **Layout:** tutorial detail page. `<h1>` + `.subtitle`, then four `.card-section` blocks. Each section: `<h2>` (1.3rem `#1a5276`, 2px solid `#2980b9` bottom border), then `table.layout` with one `<tr>`: left `td.text-col` (50%) holding `.tags` pills, a `<ul>` of bullets (each opening with `<b>` term in `#1a5276`), an optional `.sql` `<pre>` block, an italic `.example` paragraph, and a `.key-point` callout; right `td.viz-col` (50%) holding one `<canvas>` 720×300 with `width:100%`, 1px `#e0e0e0` border, 4px radius.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem; `ul` 0.92rem; `.example` italic `#555` 0.9rem; `.key-point` background `#f8f9fa`, left border 3px solid `#e74c3c`, 0.9rem; `.sql` background `#f8f9fa`, left border 3px solid `#1a5276`, ui-monospace 0.8rem.
- **Tag pills:** `.tag` inline-block 0.72rem bold, padding 2px 10px, radius 10px; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Palette:** primary blue `#1a5276` (ink), green `#27ae60`, red `#e74c3c`, orange `#e67e22`; chart palette object P: blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`.
- **Canvas:** intrinsic 720×300 attributes; scale by `window.devicePixelRatio` via a shared `setup(id)` helper (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates). A shared `drawTable(ctx,x,y,cols,widths,rows,opt)` helper draws data tables with `#1a5276` header by default (overridable via `headBg`, white bold 12px text), alternating white/`#f4f6f8` rows, optional per-row background/bold overrides, `#cfd8e0` 1px outer border, and a `rowCenter(r)` accessor for connector lines. No nav bar, no back/home links. In regenerated HTML, any card links use .html extensions.
