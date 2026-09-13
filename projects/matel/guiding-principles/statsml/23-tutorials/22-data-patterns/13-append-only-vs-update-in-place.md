# Append-Only vs Update-in-Place

**Page type:** detail page (tutorial card-sections: h2 per section, two-column `table.layout` with text left 50% / canvas right 50%)
**HTML title tag:** Append-Only vs Update-in-Place

**Subtitle:** Add a new row for every change and keep the old ones, or overwrite the existing row — keeping history versus keeping it simple

## A Customer Moves: Overwrite the Row or Add One?

**Tags:** `core idea` (blue), `running example` (green)

- **The row** — customer 42 in a customers table, with a shipping address column
- **Update-in-place** — she moves, so UPDATE her one row; "12 Oak St" is gone forever
- **Append-only** — she moves, so INSERT a new row; the old row stays, marked no-longer-current
- **Two moves later** — overwrite still has 1 row; append-only has 3 rows, dated
- **The trade** — one is small and simple; the other remembers every version

*Example:* Customer 42's addresses: "12 Oak St" (Jan 5), "88 Pine Ave" (Mar 20), "3 Lake Rd" (Jul 2).

**Key point:** Update-in-place stores only the latest truth; append-only stores every truth that ever held, each with its dates.

### Visualization (canvas `c1`, 720×300)

Side-by-side comparison: update-in-place table (one surviving row plus ghost rows) vs append-only table (three kept rows). Vertical dashed divider `#bdc3c7` at x=320.

- **Title (bold 15px `#1a5276`, top center):** "Customer 42 After Two Moves: One Row vs Three Rows"
- **Left half — heading bold 13px blue `#2a78d6`:** "UPDATE-IN-PLACE — 1 row"
  - Two ghost rows at 35% opacity, dashed muted `#6b7280` boxes with red `#e74c3c` strikethrough lines: "42 | 12 Oak St" and "42 | 88 Pine Ave".
  - Current row box (blue `#2a78d6` stroke, fill `rgba(42,120,214,0.08)`): bold "42 | 3 Lake Rd".
  - Bold red caption: "old values destroyed on write"; muted: "(crossed rows no longer exist anywhere)".
- **Right half — heading bold 13px green `#008300`:** "APPEND-ONLY — 3 rows, all kept"
  - Three green-stroked boxes (330×38) in monospace, current row bolder with fill `rgba(0,131,0,0.10)` vs `rgba(0,131,0,0.03)`:
    - "42 | 12 Oak St   | Jan 5  | current: no"
    - "42 | 88 Pine Ave | Mar 20 | current: no"
    - "42 | 3 Lake Rd   | Jul 2  | current: YES" (bold, dark green `#0a5a0a`)
  - Bold green caption: "a move = one INSERT; nothing is ever edited"
- **Annotation (bold 13px orange `#d95926`, bottom center):** "same customer, same moves — only the right side remembers them"

## Where Did Order 507 Ship on April 10?

**Tags:** `worked example` (green), `core idea` (blue)

- **The question** — order 507 shipped Apr 10; which address was correct back then?
- **Read the versions** — row 1 valid Jan 5 – Mar 20, row 2 valid Mar 20 – Jul 2, row 3 valid Jul 2 – now
- **Find the interval** — Apr 10 falls inside Mar 20 – Jul 2
- **Answer** — "88 Pine Ave", read straight off row 2 of the append-only table
- **Overwrite table** — can only say "3 Lake Rd", today's address — wrong for April

*Example:* The lookup is just "which row's valid_from ≤ Apr 10 < valid_to" — a date comparison you can do by eye.

**Key point:** Append-only lets you ask "what was true on date X?" for any X. Update-in-place can only ever answer for today.

### Visualization (canvas `c2`, 720×300)

Gantt-style validity timeline: three horizontal version bars over a Jan–Aug month axis, with a vertical as-of lookup line.

- **Title (bold 15px `#1a5276`, top center):** "Each Version Owns a Slice of Time — Apr 10 Falls in the Middle One"
- **Bars (30px tall, 35%-alpha fill + 2px solid stroke in same color; left labels bold 12px in bar color; validity caption centered inside in `#555`):**
  - "12 Oak St", aqua `#199e70`, spans months 0.13–2.63, caption "Jan 5 – Mar 20", y=78.
  - "88 Pine Ave", violet `#4a3aa7`, spans 2.63–6.05, caption "Mar 20 – Jul 2", y=128.
  - "3 Lake Rd", magenta `#d55181`, spans 6.05–8, caption "Jul 2 – now", y=178.
- **Month axis:** horizontal `#999` line at y=226 with labels Jan–Aug centered per month; left padding 130, right 40.
- **Lookup line:** vertical dashed (6/4) orange `#d95926` 2.5px line at Apr 10 (month ≈ 3.3) from y=48 to the axis; bold orange label above: "order 507 ships: Apr 10"; orange filled dot (r=6) where it crosses the middle bar; bold orange label to the right: "→ answer: 88 Pine Ave".
- **Annotations:** bold 13px red `#e74c3c`, centered: 'the overwrite table would answer "3 Lake Rd" — today\'s truth, not April\'s'; muted 12px: "as-of lookup: find the row whose valid_from ≤ date < valid_to"

## Why Overwritten History Breaks Training Data

**Tags:** `where it's used` (blue), `leakage` (red)

- **The task** — train a delivery-time model on last year's orders, with "customer region" as a feature
- **The join** — each old order is joined to the customers table to fetch the region
- **The bug** — an overwrite table serves TODAY's region for orders shipped months ago
- **Order 507 again** — shipped to Pine Ave in April, but the join labels it with Lake Rd's region
- **Audit and debug** — "who changed this, when, from what?" only exists if old rows survive

*Example:* Training saw the customer where she lives now, not where the parcel actually went — a quiet time-travel error.

**Key point:** Joining past events to an overwrite table leaks the future into training features. Append-only (with an as-of-date join) is the fix.

### Visualization (canvas `c3`, 720×300)

Two-path join-flow diagram: one past order joined either to the overwrite table (wrong) or the append-only table as-of April (right).

- **Title (bold 15px `#1a5276`, top center):** "Building Training Data: Which Table Does the Join Hit?"
- **Source box (yellow `#c98500` stroke, fill `rgba(201,133,0,0.10)`, 170×76 at left):** bold "order 507 (April)", "label: delivered in 4 days", "feature: region = ?".
- **Top path (all red `#e74c3c`, arrows red):** box "overwrite table (today)" containing "42 | 3 Lake Rd" → result box "region from Lake Rd" / "LEAKED FUTURE" (fills `rgba(231,76,60,0.08)` and `rgba(231,76,60,0.10)`).
- **Bottom path (all green `#008300`, arrows green):** box "append-only, as of Apr 10" containing "42 | 88 Pine Ave | Mar–Jul" → result box "region from Pine Ave" / "true at ship time" (fills `rgba(0,131,0,0.07)` and `rgba(0,131,0,0.10)`).
- **Annotations (bottom center):** bold 13px orange `#d95926` "the model must only see what was knowable when the order shipped"; muted 12px "overwrite tables make the leaky join the EASY one to write"

## The Confusion: Append-Only Rows Are Versions, Not Customers

**Tags:** `common mistake` (red), `rule of thumb` (blue)

- **The naive query** — "how many customers?" as a plain row count over the append-only table
- **Wrong answer** — customer 42 counts 3 times, once per address version
- **Right answer** — count distinct customer_id, or keep only rows where is_current = true
- **Same trap in joins** — joining orders to all 3 versions duplicates every one of her orders
- **Rule of thumb** — every query on an append-only table must say WHICH version it wants

*Example:* The dashboard reported 5 customers when there were 3 — movers were counted once per address.

**Key point:** In an append-only table a row means "a version of a thing", not "a thing" — forget the current-version filter and every count and join inflates.

### Visualization (canvas `c4`, 720×300)

Row-list plus two query-result boxes: naive count vs current-filter count.

- **Title (bold 15px `#1a5276`, top center):** '"How Many Customers?" — 5 Rows, 3 Customers'
- **Left: 5 monospace row boxes (320×30):** current rows in green `#008300` stroke / fill `rgba(0,131,0,0.09)` / bold dark green `#0a5a0a` text; old versions in muted `#6b7280` stroke / fill `rgba(107,114,128,0.06)` / gray `#777` text:
  - "41 | 7 Elm St     | current: YES" (green)
  - "42 | 12 Oak St    | current: no" (gray)
  - "42 | 88 Pine Ave  | current: no" (gray)
  - "42 | 3 Lake Rd    | current: YES" (green)
  - "43 | 5 Mill Ln    | current: YES" (green)
  - Muted caption below: "grey rows = old versions of customer 42"
- **Right: two result boxes (270×74):**
  - Red `#e74c3c` box: bold "naive: COUNT(*)", large bold 20px "5  ✗", fill `rgba(231,76,60,0.08)`.
  - Green `#008300` box: bold "WHERE is_current = true", large bold 20px "3  ✓", fill `rgba(0,131,0,0.08)`.
- **Annotations (bottom center):** bold 13px magenta `#d55181` "joins have the same trap: 3 versions × her orders = every order tripled"; muted 12px "always state which version you want: current, or as-of a date"

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`, social-graph reference skeleton). Body: `<h1>` (no index number), `.subtitle`, then four `.card-section` blocks, each `<h2>` + `table.layout` with one `<tr>`: left `<td class="text-col">` (50%) and right `<td class="viz-col">` (50%) holding one canvas.
- **Left column structure:** `.tags` pill row first, then a `<ul>` of bullets each opening with `<b>bold term</b>` (bold terms render `#1a5276`), one italic `.example` paragraph, one `.key-point` callout (background `#f8f9fa`, left border `3px solid #e74c3c`, 0.9rem).
- **Tag pill styles:** inline-block, 0.72rem bold, padding 2px 10px, radius 10px; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` bottom border; section h2 1.3rem `#1a5276` with 2px `#2980b9` bottom border; subtitle `#666` 0.95rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** each canvas declared 720×300; a shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Shared helpers `boxAt` (filled/stroked, optionally dashed 6/4 rectangle) and `arrowTo` (line with filled arrowhead) draw the diagrams. All data arrays are hardcoded literals — no `Math.random()`. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette (`P` object):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`; alarm red `#e74c3c`. Project palette anchors: #1a5276 primary blue, #27ae60 green, #e74c3c red, #e67e22 orange.
- In regenerated HTML, any card links use `.html` extensions (this page has none).
