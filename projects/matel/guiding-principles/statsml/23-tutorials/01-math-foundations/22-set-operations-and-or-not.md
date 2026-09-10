# Set Operations: AND, OR, NOT

**Page type:** detail page (tutorial card-sections: one `<h2>` per section, two-column `table.layout` — text left 50%, canvas right 50%)
**HTML title tag:** Set Operations: AND, OR, NOT

**Subtitle:** Two customer lists and three little words — AND, OR, NOT carve the lists into groups, and every data filter is one of them

## Two Customer Lists, One Venn Diagram

**Tags:** `core idea` (blue), `running example` (green)

- **The setup** — a cafe with 100 customers this week: 60 bought coffee, 45 bought pastry
- **The overlap** — 25 people bought both, so the two circles overlap
- **AND (intersection)** — bought coffee AND pastry: the 25 in the middle
- **OR (union)** — bought coffee OR pastry or both: 35 + 25 + 20 = 80 people
- **NOT (difference)** — coffee but NOT pastry: 60 − 25 = 35 on the left

*Example (italic):* 20 of the 100 customers bought neither — they sit outside both circles.

**Key point:** Three words split two lists into four groups that add back to everyone: 35 + 25 + 20 + 20 = 100.

### Visualization (canvas `c1`, 720×300)

Venn diagram of the 100-customer cafe inside a universe box.

- **Title (bold 15px, `#1a5276`, top center):** "100 Customers: Coffee 60, Pastry 45, Both 25".
- **Universe box:** light gray rectangle (`#b9c2cc`, 1px) at (80,48) size 560×216, labeled 12px mute gray (`#6b7280`) top-left "all 100 customers".
- **Venn circles:** two overlapping circles at cy=160 — Coffee at x=280 radius 88, Pastry at x=405 radius 78; fills at 0.22 alpha (blue `#2a78d6`, magenta `#d55181`), 2px strokes in same colors.
- **Circle labels (bold 13px):** blue "Coffee 60" at (235,92); magenta "Pastry 45" at (455,98).
- **Region counts (bold 16px at y=168) with 12px `#444` labels at y=188:** blue "35" / "coffee only" at x=237; violet (`#4a3aa7`) "25" / "both" at x=348; magenta "20" / "pastry only" at x=442. Outside circles at x=580: mute bold 14px "20" with 12px "neither".
- **Bottom annotation (bold violet 13px, center, y=288):** "35 + 25 + 20 + 20 = 100 — the four groups cover everyone once".

## Ten Customers You Can Check by Hand

**Tags:** `worked example` (green)

- **The customers** — ten people, named A through J
- **Coffee list** — {A, B, C, D, E, F}: six coffee buyers
- **Pastry list** — {D, E, F, G, H}: five pastry buyers
- **AND** — names on both lists: {D, E, F} = 3 people
- **OR** — names on either list: A through H = 8 people, not 6 + 5 = 11
- **NOT** — coffee NOT pastry: {A, B, C} = 3; neither list: {I, J} = 2

*Example (italic):* 6 + 5 = 11 memberships but only 8 distinct names — D, E, F sit on both lists.

**Key point:** Union size = 6 + 5 − 3 = 8. Add the lists, then subtract the overlap once, because the sum counted it twice.

### Visualization (canvas `c2`, 720×300)

Venn diagram with ten named customer dots placed in their regions.

- **Title (bold 15px, `#1a5276`, top center):** "Ten Customers, Every Name in Exactly One Region".
- **Universe box:** gray rectangle (`#b9c2cc`) at (80,46) size 560×214.
- **Venn circles:** Coffee at x=300 radius 82, Pastry at x=410 radius 76, cy=155; same fill/stroke style as c1 (blue and magenta, 0.22 alpha fills).
- **Circle labels (bold 12px):** blue "coffee {A..F}" at (255,62); magenta "pastry {D..H}" at (462,66).
- **Person dots:** filled circles radius 13 with bold white 12px letter inside:
  - Coffee only (blue `#2a78d6`): A (252,122), B (238,160), C (255,198).
  - Both (violet `#4a3aa7`): D (356,118), E (356,158), F (356,198).
  - Pastry only (magenta `#d55181`): G (448,132), H (452,185).
  - Neither (gray `#8b95a1`): I (135,105), J (135,205).
- **Caption (12px `#444` at (570,240)):** "region counts: 3 + 3 + 2 + 2 = 10".
- **Bottom annotation (bold violet 13px, center, y=286):** "AND = {D,E,F} = 3 · OR = {A..H} = 8 · coffee NOT pastry = {A,B,C} = 3".

## The Same Three Words in SQL and pandas

**Tags:** `where it's used` (blue), `worked example` (green)

- **AND** — `WHERE coffee = 1 AND pastry = 1` returns the 25 both-buyers
- **OR** — `WHERE coffee = 1 OR pastry = 1` returns the 80 either-buyers
- **NOT** — `WHERE coffee = 1 AND pastry = 0` returns the 35 coffee-only
- **pandas** — `df[(df.coffee) & (df.pastry)]`, with `|` for OR and `~` for NOT
- **Parentheses** — in pandas, wrap each condition: `&` and `|` bind tighter than comparisons

*Example (italic):* The Venn region and the filter are the same object — every filter selects a set of rows.

**Key point:** Every WHERE clause is a set operation, so the Venn counts predict the row counts before the query runs — a free sanity check.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart: four SQL filters and the row counts they return.

- **Title (bold 15px, `#1a5276`, top center):** "Each Filter Returns One Venn Region (rows out of 100)".
- **Rows (monospace 12px `#333` SQL label at left x=30, 11px mute sub-label, then a 24px-tall horizontal bar starting at x=300, scale 0–100 over 340px, 0.7 alpha, bold 13px value at bar end):**
  1. `WHERE coffee = 1 AND pastry = 1` — "both" — 25, violet `#4a3aa7`
  2. `WHERE coffee = 1 OR  pastry = 1` — "either" — 80, blue `#2a78d6`
  3. `WHERE coffee = 1 AND pastry = 0` — "coffee only" — 35, green `#008300`
  4. `WHERE NOT (coffee = 1 OR pastry = 1)` — "neither" — 20, yellow `#c98500`
- **Gridlines:** vertical light gray (`#ccc`) at 0, 25, 50, 75, 100 with mute 12px tick labels; x-axis caption 12px `#444` "customers returned".
- **Bottom annotation (bold orange `#d95926` 13px, centered under bars, y=290):** "count the Venn first — it predicts every row count".

## "Coffee and Pastry" Means Two Different Sizes

**Tags:** `common mistake` (red), `watch out` (orange)

- **Plain English** — "customers who bought coffee and pastry" often means the 80 either-buyers
- **The filter** — AND in code returns only the 25 who bought both
- **3x gap** — 25 vs 80: the same sentence, wildly different segment sizes
- **Adding fails too** — 60 + 45 = 105 people out of 100: the overlap got counted twice
- **Say it precisely** — "both" for AND, "either" for OR, "only" for NOT

*Example (italic):* A stakeholder asking for "coffee and pastry buyers" probably wants either — ask which before filtering.

**Common mistake:** OR is not addition. The union is 60 + 45 − 25 = 80; plain adding invents 25 phantom customers.

### Visualization (canvas `c4`, 720×300)

Three-bar chart contrasting the AND count, the OR count, and the impossible naive sum.

- **Title (bold 15px, `#1a5276`, top center):** "\"Bought Coffee and Pastry\": 25? 80? Never 105".
- **Bars (110px wide, baseline gray line at y=235, chart height 165px, y scale max 115, plot x=110 width 470, evenly gapped):**
  1. 25, violet `#4a3aa7`, 0.75 alpha — labels "both" (12px `#333`) / "AND filter" (11px mute).
  2. 80, blue `#2a78d6`, 0.75 alpha — "either" / "OR (60 + 45 − 25)".
  3. 105, red `#e74c3c`, 0.55 alpha — "60 + 45 added" / "overlap counted twice".
- **Value labels:** bold 14px in bar color above each bar (25, 80, 105).
- **Ceiling line:** horizontal dashed mute line (`#6b7280`, dash 6/4, 1.5px) at the 100-customer level, labeled bold 12px "all 100 customers" to the right.
- **Annotations:** bold red 13px above the third bar: "105 > 100: 25 phantom customers"; bottom center bold orange (`#d95926`) 13px: "same sentence, 3x apart — ask \"both or either?\" before you filter".

## Regeneration instructions

- **Template:** tutorials topic-page layout (see `tutorials/CLAUDE.md`). h1 (no index number) + `.subtitle`, then four `.card-section` blocks, each with an `<h2>` and a `table.layout` (`td.text-col` 50%, `td.viz-col` 50%, cells padded 12px, top-aligned).
- **Left column per section:** `.tags` pill row, `<ul>` of one-line bullets opening with `<b>bold term</b>` (bold in `#1a5276`), one italic `.example` line, one `.key-point` callout. Inline `code` uses ui-monospace/Menlo 0.9em on `#f4f6f8` background, 1px 4px padding, 3px radius.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6. h1 2rem `#1a5276` with 2px `#2980b9` bottom border; section h2 1.3rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem. Canvas `width:100%`, 1px `#e0e0e0` border, 4px radius. `.key-point` background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem. `.example` italic `#555` 0.9rem.
- **Tag pills:** `.tag` inline-block 0.72rem bold, padding 2px 10px, radius 10px; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Chart palette object `P`:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette anchors: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- **Canvas:** all canvases 720×300 intrinsic, scaled with `window.devicePixelRatio` via a shared `setup(id)` helper (backing store sized to rendered width × `window.devicePixelRatio` (display capped via `style.maxWidth`), `ctx.scale` back to logical coordinates); a shared `venn(ctx, c1x, c2x, cy, r1, r2)` helper draws the two-circle diagrams; data hardcoded as literals, no `Math.random()`.
- No nav bar, no back/home links, no cross-page links. In regenerated HTML any card links use `.html` extensions.
