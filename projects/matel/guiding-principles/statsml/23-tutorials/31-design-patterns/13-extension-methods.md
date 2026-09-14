# Extension Methods

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Extension Methods

**Subtitle:** A way to bolt new methods onto a type you don't own — the class file never changes, but `order.withTip(18)` suddenly works

## Teaching the Vendor's Order New Tricks

**Tags:** `core idea` (blue), `types you don't own` (green), `syntax sugar` (orange)

- **The vendor type** — a coffee shop's point-of-sale library ships an `Order` class the shop cannot edit
- **The wish** — the shop wants `order.withTip(18)` to read as naturally as the vendor's own methods
- **The trick** — write a plain static helper in the shop's code and declare it an extension of `Order`
- **The rewrite** — the compiler turns `order.withTip(18)` into `OrderExt.withTip(order, 18)` for you
- **Nothing changes** — the vendor's class file is untouched; the new method lives entirely in shop code

*Example (italic):* The shop adds `withTip`, `roundUp`, and `receiptLine` to `Order` in one afternoon without asking the vendor for anything.

**Key point:** An extension method is a static function the language lets you call with dot syntax on a type you don't own — the type itself is never modified.

### Visualization (canvas `c1`, 720×300)

Flow diagram: a locked vendor class on the left, the shop's extension file on the right, and the compiler rewrite shown as an arrow between the sugared call and the real static call.

- **Title (bold 15px, `#1a5276`, top center):** "order.withTip(18) Is Really a Static Call in Disguise".
- **Vendor box:** rounded box at x=50, y=70, 250×60, fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border, 12px `#2c3e50` two-line text "vendor's Order class" / "(read-only — cannot edit)", small 12px `#6b7280` padlock glyph "🔒" at its top-right corner.
- **Shop box:** rounded box at x=420, y=70, 250×60, fill `rgba(0,131,0,0.12)`, 2px `#008300` border, text "shop's OrderExt file" / "withTip(order, pct)".
- **Call row (y=200):** 13px monospace `#2c3e50` text "order.withTip(18)" at x=80; 3px `#4a3aa7` arrow from x=250 to x=420 with bold 12px violet `#4a3aa7` label "compiler rewrites" above it; 13px monospace `#2c3e50` text "OrderExt.withTip(order, 18)" at x=430.
- **Dashed link:** vertical dashed `#6b7280` (dash 4/3) line from the shop box bottom (x=545, y=130) down to the rewritten call (y=190).
- **Annotation (bold 13px green `#008300`, centered near y=265):** "the vendor's class file never changes".
- **Caption (12px `#444`, bottom right):** "class and file names illustrative".

## Tipping Three Orders by Hand

**Tags:** `worked example` (blue), `hand-checkable` (green)

- **Three orders** — morning totals are $8.00, $12.50, and $21.00 on the vendor's plain `Order` objects
- **The call** — `order.withTip(18)` adds an 18% tip: the tips are $1.44, $2.25, and $3.78
- **Hand-check** — 12.50 × 0.18 = 2.25, so the middle order becomes 12.50 + 2.25 = $14.75
- **The results** — the three tipped totals come out to $9.44, $14.75, and $24.78
- **Chaining** — `order.withTip(18).receiptLine()` reads left to right because each call returns a value

*Example (italic):* The $21.00 order: 21.00 × 0.18 = 3.78 tip, so `withTip(18)` returns $24.78 — the same math a plain function would do, with nicer spelling.

**Key point:** The extension does nothing a plain function couldn't — `withTip(order, 18)` and `order.withTip(18)` run the identical code and produce identical numbers.

### Visualization (canvas `c2`, 720×300)

Stacked bar chart of the three orders: base total in blue with the 18% tip stacked in green, tipped totals labeled on top.

- **Title (bold 15px, `#1a5276`, top center):** "order.withTip(18): Three Orders, Tips Checked by Hand".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; y = dollars 0 to 28, gridlines `#e5e9ef` at 7/14/21 with 12px `#444` labels "$7"/"$14"/"$21"; x = three bars centered at x=170, 350, 530, each 90px wide, 12px `#444` labels "order A" / "order B" / "order C" below the baseline.
- **Base segments (blue fill `rgba(42,120,214,0.30)`, 2px `#2a78d6` border):** heights scaled at 180/28 px per dollar for base totals `[8.00, 12.50, 21.00]`.
- **Tip segments (green fill `rgba(0,131,0,0.30)`, 2px `#008300` border):** stacked on the bases for tips `[1.44, 2.25, 3.78]`.
- **Total labels (bold 12px `#1a5276`, centered above each bar):** "$9.44", "$14.75", "$24.78".
- **Annotation (bold 13px violet `#4a3aa7`, upper left near x=80, y=70):** "12.50 × 0.18 = 2.25 — redo every bar by hand".
- **Caption (12px `#444`, bottom right):** "order amounts illustrative, tip arithmetic exact".

## The Pattern Behind LINQ and Pandas Accessors

**Tags:** `where it's used` (blue), `call chains` (green)

- **LINQ** — C#'s `.Where`, `.Select`, and `.Sum` are all extension methods bolted onto plain collections
- **Kotlin & Swift** — both make extensions a headline feature for the same prize: readable call chains
- **Pandas accessors** — a registered accessor gives `df.shop.clean()` on DataFrames you don't own
- **Rust traits** — an extension trait adds `.with_tip()` to a library struct without forking the crate
- **Pipelines** — filter → transform → aggregate chains stay readable because each step hangs off the dot

*Example (italic):* `orders.Where(total > 10).Select(withTip 18%).Sum()` keeps the $12.50 and $21.00 orders and returns $39.53.

**Key point:** Whenever a data scientist chains dot-calls on a type the library authors never met, extension methods — or a cousin of them — are doing the work.

### Visualization (canvas `c3`, 720×300)

Pipeline diagram: three orders flow through Where, Select, and Sum boxes, with the surviving dollar amounts written under each stage.

- **Title (bold 15px, `#1a5276`, top center):** "A Chain of Extension Methods: Filter, Tip, Sum".
- **Stage boxes (rounded, 40px tall, centered at y=120, 12px `#2c3e50` monospace labels):** "orders" at x=40 width 100 fill `rgba(42,120,214,0.15)` border `#2a78d6`; ".Where(t > 10)" at x=190 width 130 fill `rgba(217,89,38,0.12)` border `#d95926`; ".Select(withTip 18%)" at x=370 width 160 fill `rgba(0,131,0,0.12)` border `#008300`; ".Sum()" at x=580 width 100 fill `rgba(74,58,167,0.12)` border `#4a3aa7`.
- **Arrows:** 3px `#6b7280` arrows between consecutive boxes at y=140.
- **Data rows under each stage (12px `#444`, starting y=185, one amount per line):** under "orders": `[8.00, 12.50, 21.00]`; under Where: `[12.50, 21.00]` plus 12px `#d95926` note "$8.00 dropped"; under Select: `[14.75, 24.78]`; under Sum: bold 13px `#4a3aa7` "$39.53".
- **Annotation (bold 13px green `#008300`, centered near y=270):** "every dot in the chain is a static function in disguise".
- **Caption (12px `#444`, bottom right):** "amounts illustrative — same orders as the tipping example".

## Sugar Over a Static Function, Not Monkey Patching

**Tags:** `common mistake` (red), `dispatch rules` (orange)

- **Not a patch** — the vendor's class is never edited; Ruby-style monkey patching really rewrites the class
- **No private access** — an extension sees only the public surface; `order._discount` stays out of reach
- **Instance wins** — if the vendor later ships a real `withTip`, calls quietly switch to the vendor's version
- **Static dispatch** — extensions are resolved at compile time, so a subclass cannot override them
- **The import** — forget the `using`/`import` line and the method vanishes from autocomplete entirely

*Example (italic):* The vendor's v2 ships its own `withTip` that tips pre-tax; on upgrade day the shop's 18% math is silently bypassed with no error anywhere.

**Common mistake:** Expecting an extension method to behave like a real member — to override, touch private state, or dispatch polymorphically. It is compile-time sugar over a static call, nothing more.

### Visualization (canvas `c4`, 720×300)

Two-row diagram of the surprises: an extension reaching for a private field (compile error) vs a name clash where the vendor's new instance method silently wins.

- **Title (bold 15px, `#1a5276`, top center):** "Two Ways Extensions Surprise You".
- **Row 1 (y=95), label 12px `#444` at x=20:** "private field"; green `#008300` rounded box at x=150 width 190 labeled "extension reads order._discount" (12px), 3px arrow to a red `#e74c3c` box at x=440 width 220 labeled "compile error — public surface only" with bold 12px red "✗" at its left edge.
- **Row 2 (y=205), label:** "name clash"; blue `#2a78d6` rounded box at x=150 width 190 labeled "call: order.withTip(18)", 3px arrow to an orange `#d95926` box at x=440 width 220 labeled "vendor's new instance method runs" with bold 12px orange "shop extension silently ignored" beneath the box.
- **Box style:** 40px tall, 8px radius, fills `rgba(0,131,0,0.12)` / `rgba(231,76,60,0.12)` / `rgba(42,120,214,0.15)` / `rgba(217,89,38,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px magenta `#d55181`, centered near y=270):** "no crash, no warning — the wrong tip just ships".
- **Caption (12px `#444`, bottom right):** "dispatch rules as in C#/Kotlin; scenario illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all amounts are the hardcoded arrays above (no randomness); order bases `[8.00, 12.50, 21.00]`, 18% tips `[1.44, 2.25, 3.78]`, tipped totals `[9.44, 14.75, 24.78]`, and the pipeline sum `$39.53` (14.75 + 24.78) are invented and labeled illustrative, but the tip arithmetic is exact and hand-checkable.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
