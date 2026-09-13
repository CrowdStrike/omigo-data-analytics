# SQL Execution Order

**Page type:** detail page (tutorial page: h2 card-sections, each a two-column table.layout — text left 50%, canvas right 50%)
**HTML title tag:** SQL Execution Order

**Subtitle:** The database runs your query in a different order than you write it — and that one fact explains most confusing SQL errors

## You Write SELECT First — It Runs Almost Last

Tags: `core idea` (blue), `running example` (green)

- **Written order** — SELECT, FROM, WHERE, GROUP BY, HAVING, ORDER BY, LIMIT
- **Run order** — FROM first: the database must fetch the orders table before anything else
- **Then filter** — WHERE throws rows out, GROUP BY piles them, HAVING tests the piles
- **SELECT is late** — picking output columns happens after all that filtering and piling
- **Sort and cut last** — ORDER BY and LIMIT touch only the rows that survived

*Example (italic):* Reading a recipe aloud top-to-bottom while the cook actually starts in the pantry, not at the plating step.

**Key point:** SQL is written for humans and run for machines: FROM → WHERE → GROUP BY → HAVING → SELECT → ORDER BY → LIMIT.

### Visualization (canvas `c1`, 720×300)

Two-column mapping diagram: written order on the left, run order on the right, with crossing colored connector lines.

- **Title (bold 15px `#1a5276`, top center):** "Written Order vs Run Order".
- **Column headings (bold 13px muted `#6b7280`):** "you write" over the left column, "it runs" over the right column.
- **Left column** (boxes 110×24 starting at x=90, y=52, 32px row pitch), written order top to bottom: "1. SELECT", "2. FROM", "3. WHERE", "4. GROUP BY", "5. HAVING", "6. ORDER BY", "7. LIMIT".
- **Right column** (x=520), run order top to bottom: "1. FROM", "2. WHERE", "3. GROUP BY", "4. HAVING", "5. SELECT", "6. ORDER BY", "7. LIMIT".
- **Keyword colors** (box stroke and label): SELECT orange `#d95926`, FROM blue `#2a78d6`, WHERE green `#008300`, GROUP BY violet `#4a3aa7`, HAVING aqua `#199e70`, ORDER BY yellow `#c98500`, LIMIT magenta `#d55181`. Boxes filled `#f4f6f8`, bold 12px labels.
- **Connectors:** a line in each keyword's color joins its left box to its right box (1.2px; the SELECT line is 2.5px to emphasize the big jump).
- **Annotation (bold 13px orange, centered at x=355):** one line "SELECT: written 1st, run 5th of 7" (y=290, below the crossing lines).

## One Query on the Orders Table, Step by Step

Tags: `worked example` (green), `core idea` (blue)

- **FROM orders** — start with all 8 rows of the shop's orders table
- **WHERE amount >= 8** — drops 1003 ($5) and 1008 ($6): 6 rows left
- **GROUP BY city** — 3 piles: Pune $8+$15+$12 = $35, Delhi $15+$12 = $27, Mumbai $8
- **HAVING SUM > 20** — Mumbai's $8 pile fails: 2 piles left
- **ORDER BY, LIMIT 1** — sort $35 above $27, keep the top row: Pune

SQL block:

```sql
SELECT city, SUM(amount) AS total
FROM   orders
WHERE  amount >= 8
GROUP  BY city
HAVING SUM(amount) > 20
ORDER  BY total DESC
LIMIT  1;               -- Pune | 35
```

*Example (italic):* Trace it yourself: 8 rows, then 6, then 3 piles, then 2, then 1 answer — Pune at $35.

**Key point:** Every clause is a stage in a pipeline, and each stage only sees what the previous stage let through.

### Visualization (canvas `c2`, 720×300)

Vertical funnel diagram: five stage bars narrowing from 8 rows to 1, with stage labels and notes on the right.

- **Title (bold 15px `#1a5276`, top center):** "The Pipeline: 8 Rows → 6 → 3 Piles → 2 → 1 Answer".
- **Funnel bars** centered at x=250, starting y=46, 48px row pitch, 30px tall; width proportional to count (max width 340 for 8): fill at 25% alpha of the stage color, 2px colored stroke, bold 13px colored count label centered in the bar. Muted downward arrows between stages.
- **Stages** (count label, then right-side bold 12px ink label at x=450 with 12px muted note below):
  1. "8 rows" blue `#2a78d6` — "FROM orders" / "all orders".
  2. "6 rows" green `#008300` — "WHERE amount >= 8" / "drops 1003 ($5), 1008 ($6)".
  3. "3 piles" violet `#4a3aa7` — "GROUP BY city" / "Pune $35, Delhi $27, Mumbai $8".
  4. "2 piles" aqua `#199e70` — "HAVING SUM > 20" / "Mumbai $8 dropped".
  5. "1 row" orange `#d95926` — "ORDER BY + LIMIT 1" / "Pune | $35".
- **Annotation (bold 13px orange, left-aligned at x=450, y=285):** "each stage only sees what survived the one above".

## WHERE and HAVING Ask Different Questions

Tags: `why it matters` (orange), `worked example` (green)

- **WHERE runs early** — it filters single orders before any pile exists
- **HAVING runs late** — it filters finished piles using their totals
- **Query A** — WHERE amount >= 8, then sum: Pune $35 (only the big orders count)
- **Query B** — sum everything, then HAVING >= 30: Pune $40 (all orders count)
- **Same table, different answers** — because the filter ran at a different stage

*Example (italic):* "Revenue from orders above $8" and "revenue of cities above $30" sound alike but are different pipelines.

**Key point:** Putting a filter in the wrong stage does not error — it silently answers a different question. Execution order is what tells you which.

### Visualization (canvas `c3`, 720×300)

Side-by-side bar panels comparing the two queries' city totals.

- **Title (bold 15px `#1a5276`, top center):** "Same Table, Two Filters, Two Different Pune Totals".
- **Divider:** vertical dashed line (`#bdc3c7`, dash 4/3) at x=360 from y=35 to y=285.
- **Each panel:** bold 13px ink title centered at top; 3 bars (Pune blue `#2a78d6`, Delhi green `#008300`, Mumbai magenta `#d55181`), bar width 66, baseline y=235, chart height 130, y-scale max 45, gray `#999` baseline; bold 13px value labels above bars, 12px city names below; bold 12px colored note centered at y=275.
  - Left panel (x0=20): "A: WHERE amount >= 8, then SUM" — bars $35 / $27 / $8, none dropped; note (green): "small orders excluded before piling".
  - Right panel (x0=390): "B: SUM all, then HAVING >= 30" — bars $40 / $33 / $8; Mumbai drawn faded `rgba(213,81,129,0.25)` with a red `#e74c3c` X (two crossed 1.5px diagonals) over it; note (violet `#4a3aa7`): "full sums; Mumbai pile dropped after".
- **Bottom caption (bold 13px orange `#d95926`, centered, y=296):** "Pune: $35 vs $40 — the stage where the filter runs changes the number".

## "Unknown Column total" — The Alias Isn't Born Yet

Tags: `common mistake` (red), `core idea` (blue)

- **The error** — WHERE total > 30 fails: "column total does not exist"
- **The reason** — the alias total is created by SELECT, which runs after WHERE
- **At WHERE time** — only the raw columns exist: order_id, customer, city, amount
- **ORDER BY total works** — sorting runs after SELECT, so the alias exists by then
- **The fix** — repeat the expression in WHERE/HAVING, or wrap the query as a named step

SQL block:

```sql
SELECT city, SUM(amount) AS total
FROM   orders
WHERE  total > 30      -- ERROR: not born yet
GROUP  BY city
ORDER  BY total DESC;  -- fine: born by now
```

*Example (italic):* The same word "total" is illegal on line 3 and legal on line 5 — only the run order explains it.

**Rule of thumb:** An alias can be used only in clauses that run after SELECT. If SQL says a column you clearly typed "doesn't exist", check the execution order first.

### Visualization (canvas `c4`, 720×300)

Alias-lifetime timeline across the seven execution stages.

- **Title (bold 15px `#1a5276`, top center):** 'When Does the Alias "total" Exist?'.
- **Timeline:** horizontal muted `#6b7280` 2px line at y=130 from x=60 across the width; seven 9px-radius dots for the stages in run order — FROM, WHERE, GROUP BY, HAVING, SELECT, ORDER BY, LIMIT — evenly spaced; stage names bold 12px `#2c3e50` alternating above/below the line.
- **Dot colors:** red `#e74c3c` for FROM through HAVING (alias not born), green `#008300` for SELECT, ORDER BY, LIMIT.
- **Existence band:** `rgba(0,131,0,0.12)` rectangle (84px tall, centered on the line) spanning from the SELECT dot to the end.
- **Birth marker:** vertical dashed green line (dash 5/4, 2px) at the SELECT position, with bold 12px green label below the timeline: '"AS total" born here'.
- **Annotations (bold 13px, centered):** red pair over the early stages: "alias does not exist yet" (y=70) / "WHERE total > 30 → ERROR" (y=90); green pair over the band: "alias alive from SELECT on" (y=70) / "ORDER BY total → fine" (y=90).
- **Bottom captions (12px muted, centered):** "fix: repeat the expression (HAVING SUM(amount) > 30) or name the step in a WITH clause" (y=250); "same query, same word, legal or illegal purely by stage" (y=272).

## Regeneration instructions

- **Layout:** tutorial detail page. `<h1>` + `.subtitle`, then four `.card-section` blocks. Each section: `<h2>` (1.3rem `#1a5276`, 2px solid `#2980b9` bottom border), then `table.layout` with one `<tr>`: left `td.text-col` (50%) holding `.tags` pills, a `<ul>` of bullets (each opening with `<b>` term in `#1a5276`), an optional `.sql` `<pre>` block, an italic `.example` paragraph, and a `.key-point` callout; right `td.viz-col` (50%) holding one `<canvas>` 720×300 with `width:100%`, 1px `#e0e0e0` border, 4px radius.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem; `ul` 0.92rem; `.example` italic `#555` 0.9rem; `.key-point` background `#f8f9fa`, left border 3px solid `#e74c3c`, 0.9rem; `.sql` background `#f8f9fa`, left border 3px solid `#1a5276`, ui-monospace 0.8rem.
- **Tag pills:** `.tag` inline-block 0.72rem bold, padding 2px 10px, radius 10px; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Palette:** primary blue `#1a5276` (ink), green `#27ae60`, red `#e74c3c`, orange `#e67e22`; chart palette object P: blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`.
- **Canvas:** intrinsic 720×300 attributes; scale by `window.devicePixelRatio` via a shared `setup(id)` helper (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates). No nav bar, no back/home links. In regenerated HTML, any card links use .html extensions.
