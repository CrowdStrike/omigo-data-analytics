# Slowly Changing Data

**Page type:** detail page (tutorial page: card-sections, each with a two-column layout table — text left 45% with tag pills / bullets / example / key-point, canvas right 55%; section 3 uses a 3-column layout with two canvases)
**HTML title tag:** Slowly Changing Data

**Subtitle:** Customers move, prices change, categories get renamed — if you overwrite the old value, every old report quietly changes with it. Keeping dated history lets each report show what was true back then.

## Maya Moves to Denver — and Her Spring Orders Move With Her

Tags: `core idea` (blue), `running example` (green), `common mistake` (red)

- **The customer** — Maya lives in Austin and places two orders there: $40 in March, $60 in May
- **The move** — on July 1 she relocates to Denver, then orders $50 in August and $30 in November
- **The overwrite** — the customer table has one row per person; city is updated to "Denver"
- **The rewrite** — every report joining orders to that row now puts all four orders in Denver
- **The name** — attributes that change occasionally like this are "slowly changing" data

*Example:* The Austin store manager's spring numbers dropped $100 overnight — months after spring ended.

**Key point:** Overwriting an attribute doesn't just change the future — it silently rewrites the past of every report that joins to it.

### Visualization (canvas `c1`, 720×300)

Two order timelines for 2025: reality vs after a Type 1 overwrite.

- **Title (bold 16px, `#1a5276`, top center):** "Maya's 2025 Orders: What Happened vs What the Overwrite Says".
- **Axis:** months Jan–Dec mapped to x=90..655; month tick labels "Jan", "Apr", "Jul", "Oct", "Dec" (12px `#6b7280`) at y=232.
- **Orders (both rows):** four 8px dots at ~mid-March ($40), late May ($60), early Aug ($50), early Nov ($30); amount labels bold 11px above each dot.
- **Row "reality"** (8px `#e5e9ef` track at y=105): March and May dots blue `#2a78d6` (Austin), Aug and Nov dots aqua `#199e70` (Denver).
- **Row "after Type 1"** (track at y=195): all four dots aqua (everything counted as Denver).
- **Move marker:** vertical dashed orange (`#d95926`, dash 5/4) line at Jul 1, labeled bold 12px "Jul 1: moves to Denver".
- **Legend:** blue dot "counted as Austin", aqua dot "counted as Denver" (12px `#2c3e50`).
- **Bottom annotation (bold 13px `#e74c3c`, centered, y=286):** "overwrite repaints March and May: Austin $100 → $0, Denver $80 → $180".

## Two Rows, Two Date Ranges: Joining Each Order to Its Own Era

Tags: `worked example` (green), `effective dates` (blue), `history table` (orange)

- **The fix** — instead of updating the row, close it and add a new one (this is "SCD Type 2")
- **Row 1** — city Austin, effective 2023-01-10 to 2025-06-30, current = N
- **Row 2** — city Denver, effective 2025-07-01 to 9999-12-31, current = Y
- **The join** — match each order where order_date falls BETWEEN effective_from AND effective_to
- **Check it** — Mar 15 and May 20 land in row 1 (Austin $100); Aug 9 and Nov 2 in row 2 (Denver $80)

*Example:* Four orders, two dim rows, one BETWEEN condition — you can redo the whole join on paper.

**Key point:** The date range on each row is what lets one customer be two different facts at two different times.

### Visualization (canvas `c2`, 720×300)

Rendered SCD Type 2 dimension table with as-of join arrows from two order boxes.

- **Title (bold 16px, `#1a5276`, top center):** "dim_customer, SCD Type 2: One Person, Two Dated Rows".
- **Table** (at x=175, width 480, rows 34px): header cells fill `#eef3f8` with `#1a5276` border and bold 12px labels "city" (100px), "effective_from" (135px), "effective_to" (135px), "current" (110px). Data rows (white fill, 1.5px border in row color, city name bold in row color):
  - Row 1 (blue `#2a78d6`): Austin | 2023-01-10 | 2025-06-30 | N
  - Row 2 (aqua `#199e70`): Denver | 2025-07-01 | 9999-12-31 | Y
- **Order boxes** (180×30, fill `#f8f9fa`, 1.5px stroke in matching row color, bold 12px text, at left): "order  Mar 15  $40" (y≈200, blue) and "order  Aug 9   $50" (y≈250, aqua); each with a curved colored arrow to its matching dimension row.
- **Annotations:** bold 13px orange (`#d95926`) centered at y=216: "join: order_date BETWEEN effective_from AND effective_to"; bold 12px `#1a5276` centered at y=286: "result: Austin gets $40 + $60 = $100, Denver gets $50 + $30 = $80 — forever".

## Why the June Report No Longer Matches the December Re-Run

Tags: `where it's used` (blue), `what goes wrong` (red)

- **June** — the H1 report is run: Austin shows Maya's $100, and it is correct
- **December** — finance re-runs the same query for an audit; Austin now shows $0 for her
- **No bug** — the query is identical; the overwritten city row changed underneath it
- **Everywhere** — sales regions, price lists, product categories, plan tiers all change this way
- **With history** — a Type 2 table gives the same $100 answer in June, December, or 2030

*Example:* The auditor asked why two runs of one query disagreed; the answer was a customer's moving van.

**Key point:** If a re-run of an old report can change, you don't have history — you have only the present, retroactively applied.

### Visualization (canvas `c3a`, 420×300)

Two-bar chart: the same query drifting under a Type 1 store.

- **Title (bold 15px, `#1a5276`, top center):** "Type 1 store: the report drifts"; sub-line 12px `#6b7280`: "Maya's H1 spend attributed to Austin".
- **Bars:** "run in June" = $100 (blue `#2a78d6`) and "re-run in Dec" = $0 (red `#e74c3c`, drawn as a 2px stub); 90px wide, fill at 55% alpha with 2px stroke; value labels "$100" / "$0" bold 13px in bar color; run labels 12px `#2c3e50` below; scale max 120, L-shaped `#6b7280` axis.
- **Annotations:** bold 13px red centered at y=272: "same query, $100 vanished"; italic 11px `#6b7280` at y=290: "illustrative amounts from the running example".

### Visualization (canvas `c3b`, 400×300)

Two-bar chart: the same query holding steady under a Type 2 store.

- **Title (bold 15px, `#1a5276`, top center):** "Type 2 store: the report holds"; sub-line 12px `#6b7280`: "Maya's H1 spend attributed to Austin".
- **Bars:** "run in June" = $100 and "re-run in Dec" = $100, both green `#008300`, 90px wide, fill at 50% alpha with 2px stroke; value labels "$100" bold 13px green; scale max 120.
- **Annotations:** bold 13px green centered at y=272: "history rows make old answers stable"; italic 11px `#6b7280` at y=290: "the December run reads the closed Austin row".

## Type 1, 2, 3 — and Which Changes Deserve History

Tags: `SCD types` (orange), `rule of thumb` (orange), `common confusion` (red)

- **Type 1** — overwrite in place; the past is repainted; keeps the table small and simple
- **Type 2** — add a dated row per change; full history; the join picks the row by date
- **Type 3** — one "previous value" column; remembers exactly one change, then forgets
- **The confusion** — Type 1 is not "wrong": fixing a typo SHOULD rewrite the past
- **The rule** — corrections get Type 1; real-world changes you may report on get Type 2

*Example:* "Austn" → "Austin" is a correction; Austin → Denver is an event — same UPDATE, opposite handling.

**Key point:** Ask one question per attribute: "would an old report ever need the old value?" Yes means Type 2; no means Type 1 is fine.

### Visualization (canvas `c4`, 720×300)

Three side-by-side panels comparing SCD Types 1, 2, 3.

- **Title (bold 16px, `#1a5276`, top center):** "Three Ways to Store \"Austin → Denver\"".
- **Panels** (196px wide, at x = 30, 262, 494; panel title bold 13px in panel color; row boxes 36px tall, fill `#f8f9fa`, 2px stroke in panel color, 12px `#2c3e50` text; note1 bold 12px in panel color at y=208, note2 12px `#6b7280` at y=228):
  - "Type 1: overwrite" (red `#e74c3c`): one row "city: Denver"; notes "past repainted" / "use for typo fixes"
  - "Type 2: add a row" (green `#008300`): rows "Austin · to 2025-06-30" and "Denver · from 2025-07-01"; notes "full history" / "use for real changes"
  - "Type 3: extra column" (yellow `#c98500`): one row "city: Denver · prev: Austin"; notes "one step of memory" / "forgets the 2nd move"
- **Dividers:** dashed vertical `#e5e9ef` lines between panels.
- **Annotations:** bold 13px orange (`#d95926`) centered at y=270: "one question decides: would an old report ever need the old value?"; 12px `#6b7280` at y=290: "yes → Type 2 · no → Type 1 · \"just the last one\" → Type 3".

## Regeneration instructions

- **Layout:** tutorial detail page. `<h1>` + `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px solid `#2980b9` bottom border) + a layout table. Sections 1, 2, 4 use `table.layout` (`td.text-col` 45% / `td.viz-col` 55%); section 3 uses `table.layout3` (text 38%, two viz cells 31% each holding canvases `c3a` 420×300 and `c3b` 400×300). Text cell order: `.tags` pill row, `<ul>` bullets (each starting with `<b>bold term</b>` in `#1a5276`), italic `.example`, `.key-point` callout.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius.
- **Tag pills:** `.tag` inline-block 0.72rem bold, padding 2px 10px, radius 10px; blue = bg `rgba(26,82,118,0.12)` / `#1a5276`, green = `rgba(39,174,96,0.15)` / `#27ae60`, red = `rgba(231,76,60,0.12)` / `#e74c3c`, orange = `rgba(230,126,34,0.15)` / `#e67e22`.
- **Key-point callout:** background `#f8f9fa`, left border 3px solid `#e74c3c`, padding 8px 12px, 0.9rem.
- **Canvas:** intrinsic width/height read from attributes; shared `setup(id)` helper scales by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates). Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette (JS object P):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`, red `#e74c3c`. Doc palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- No nav bar, no back/home links. In regenerated HTML any card links would use `.html` extensions (this page has none).
