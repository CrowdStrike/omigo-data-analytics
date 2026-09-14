# Case Classes & Companion Objects

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Case Classes & Companion Objects

**Subtitle:** Scala folds the value-object, factory, and builder patterns into two keywords — a coffee order record plus its factory in four lines instead of fifty

## Fifty Lines of Boilerplate Become One

**Tags:** `core idea` (blue), `value object` (green), `Scala` (orange)

- **The record** — a coffee shop tracks every sale as an order: item, size, price
- **The old way** — classic Java needs a constructor, getters, equals, hashCode, toString, copy
- **The count** — hand-written, those pieces run about 47 lines for a three-field record
- **The keyword** — `case class Order(item, size, price)` generates every one of them automatically
- **The companion** — an `object Order` beside it holds the would-be static factory methods
- **The hook** — the patterns didn't disappear; they dissolved into syntax the compiler writes

*Example (italic):* The shop's whole order record — value semantics, printing, copying, and a factory — fits in 4 lines: one `case class` line and a 3-line companion object.

**Key point:** A case class is the value-object pattern as a keyword: the compiler writes the boilerplate, so the source shows only the three fields that actually matter.

### Visualization (canvas `c1`, 720×300)

Horizontal bar chart: lines of hand-written code per boilerplate piece in a classic value class, versus the 4-line case class + companion that replaces them all.

- **Title (bold 15px, `#1a5276`, top center):** "One Order Record: 47 Hand-Written Lines vs 4 Lines of Case Class".
- **Axis:** vertical 2px `#999` baseline at x=230, bars extend right, scale 9px per line of code, max width 440.
- **Rows (top to bottom at y = 58, 80, 102, 124, 146, 168, 190), each with a left-aligned 12px `#444` label at x=20, blue `#2a78d6` bars, fill `rgba(42,120,214,0.30)`, 14px tall, 11px `#444` line-count labels at bar ends:**
  - "constructor": width 45 (5 lines)
  - "getters": width 54 (6 lines)
  - "equals": width 108 (12 lines)
  - "hashCode": width 36 (4 lines)
  - "toString": width 45 (5 lines)
  - "copy": width 72 (8 lines)
  - "static factory": width 63 (7 lines)
- **Bottom row (y=222), label "case class + companion" in bold 12px `#008300`:** solid green `#008300` bar width 36 (4 lines), bold 11px green label "4 lines" at the bar end.
- **Annotation (bold 13px green `#008300`, right side near y=120):** "47 lines → 4: the pattern became syntax".
- **Caption (12px `#444`, bottom right):** "line counts typical for a 3-field record, illustrative".

## The Companion Object Prices the Order

**Tags:** `worked example` (blue), `factory` (green)

- **The menu** — base prices live in the companion: latte 4.50, espresso 3.00, mocha 5.00
- **The multipliers** — the companion's `apply` scales by size: S ×0.9, M ×1.0, L ×1.2
- **The call** — writing `Order("latte", "L")` runs the factory; no `new`, no separate builder class
- **Hand-check** — a large latte is 4.50 × 1.2 = 5.40; a small espresso is 3.00 × 0.9 = 2.70
- **Value equality** — two separate `Order("latte", "L")` calls compare equal because the fields match
- **Pattern match** — the same machinery lets you take orders apart: `case Order(item, "L", p)` binds item and price

*Example (italic):* `Order("latte", "L")` returns `Order("latte", "L", 5.40)`, and a second identical call produces an object that `==` says is the same order.

**Key point:** The companion object is the factory pattern in its natural home — construction logic lives beside the class, and `apply` makes the factory call look like plain construction.

### Visualization (canvas `c2`, 720×300)

Grouped bar chart: the 3×3 price grid the companion's factory produces — three items, three size bars each, all computable by hand from base price × multiplier.

- **Title (bold 15px, `#1a5276`, top center):** "Every Price the Factory Can Return: base × size multiplier".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; y = price 0 to 6.50, gridlines `#e5e9ef` at 2.00/4.00/6.00 with 12px `#444` labels; x = three item groups centered at x=170, 370, 570 with 12px `#444` labels "latte (4.50)", "espresso (3.00)", "mocha (5.00)".
- **Bars:** three per group, 34px wide, 6px gaps; S = blue `#2a78d6`, M = aqua `#199e70`, L = violet `#4a3aa7`; heights from prices `[4.05, 4.50, 5.40]` (latte), `[2.70, 3.00, 3.60]` (espresso), `[4.50, 5.00, 6.00]` (mocha); bold 11px price labels in the bar's color above each bar; 11px `#6b7280` "S/M/L" letters below the baseline under each bar.
- **Annotation (bold 13px violet `#4a3aa7`, above the latte L bar, y≈60):** "Order(\"latte\",\"L\") → 4.50 × 1.2 = 5.40".
- **Caption (12px `#444`, bottom right):** "menu prices illustrative; multipliers S ×0.9, M ×1.0, L ×1.2".

## Where Typed Records Save a Data Pipeline

**Tags:** `where it's used` (blue), `data pipelines` (green)

- **Spark** — a case class is a Dataset schema: `spark.read.csv(...).as[Order]` gives typed rows
- **The typo** — an untyped job reading `row("pricee")` compiles fine and dies mid-run on the cluster
- **The clock** — typed, that typo is a compile error in ~0.2 minutes; untyped, it surfaces 180 minutes into the job
- **Events** — ETL code pattern-matches on case classes: `case Refund(id, amt)` vs `case Sale(id, amt)`
- **Immutability** — case class fields are `val`s, so parallel workers can share records without locks

*Example (italic):* The nightly sales job crashes at 3am on `pricee`; the typed rewrite of the same job refuses to compile until the field name is fixed.

**Key point:** Data scientists meet case classes as schemas — the compiler checks field names and types before the job runs, moving whole classes of pipeline failures from 3am to compile time.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart: how long the same field-name typo survives before being caught, under three ways of running the pipeline.

- **Title (bold 15px, `#1a5276`, top center):** "Same Typo, Three Discovery Times: Minutes Until 'pricee' Is Caught".
- **Axis:** vertical 2px `#999` baseline at x=250, bars extend right, max width 420; log-feel achieved by hardcoded pixel widths, not a real log axis.
- **Rows (top to bottom at y = 80, 140, 200), each with a left-aligned 12px `#444` label at x=20, bars 16px tall, 11px width labels at bar ends:**
  - "typed Dataset[Order] — compile": green `#008300` bar width 12, label "0.2 min"
  - "unit test on a sample": yellow `#c98500` bar width 90, label "5 min"
  - "untyped job on the cluster": red `#e74c3c` bar width 420, label "180 min"
- **Annotation (bold 13px green `#008300`, near x=300, y=105):** "the schema is a contract the compiler enforces".
- **Caption (12px `#444`, bottom right):** "bar widths compressed, not to scale — 180 min is 900× the 0.2-min compile; minutes illustrative".

## Copy Makes a New Order, It Never Edits Yours

**Tags:** `common mistake` (red), `immutability` (orange)

- **The confusion** — `order.copy(size = "L")` looks like an edit, but it builds a brand-new order
- **The original** — after the copy, `order` is still the medium 4.50 latte it always was
- **The bug** — upgrading a customer's cup, then handing them the old `order` variable: still an M
- **The fix** — capture the result: `val larger = order.copy(size = "L", price = 5.40)`
- **Name clash** — `Order` (capital) is the companion object, one per program; `order` is one cup

*Example (italic):* The barista runs the upgrade but rings up `order` instead of `larger` — the receipt says M 4.50 while the customer holds an L worth 5.40.

**Common mistake:** Expecting `copy` to mutate in place. Case classes are immutable by design — every "change" is a new value, and the old one is untouched until you rebind the name.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: the mistaken read of `copy` as an in-place edit (wrong receipt) vs capturing the returned value (correct receipt).

- **Title (bold 15px, `#1a5276`, top center):** "copy Returns a New Value — the Old Order Never Changes".
- **Row 1 (y=95), label 12px `#444` at x=20:** "result dropped"; blue `#2a78d6` rounded box at x=170 labeled "order = (latte, M, 4.50)" (12px), 3px arrow labeled 11px `#6b7280` "copy(size=L)" to a red `#e74c3c` box at x=430 labeled "ring up order → M 4.50" with bold 12px red "✗ upgrade lost".
- **Row 2 (y=205), label:** "result captured"; blue box "order = (latte, M, 4.50)", 3px arrow "val larger = copy(...)" to a green `#008300` box at x=380 labeled "larger = (latte, L, 5.40)", then arrow to a green box at x=580 labeled "ring up larger ✓".
- **Box style:** 150–180px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(231,76,60,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px orange `#d95926`, centered near y=270):** "immutable means the change lives in the return value, not the variable".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); boilerplate line counts (5/6/12/4/5/8/7 = 47 vs 4) are typical-but-illustrative; the price grid follows exactly from bases 4.50/3.00/5.00 and multipliers 0.9/1.0/1.2; typo-discovery minutes (0.2 / 5 / 180) are invented and labeled illustrative.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
