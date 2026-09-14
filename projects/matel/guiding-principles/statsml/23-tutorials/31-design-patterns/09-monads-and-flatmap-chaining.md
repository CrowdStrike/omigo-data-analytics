# Monads & flatMap Chaining

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Monads & flatMap Chaining

**Subtitle:** A monad is a wrapped value plus a chaining rule — put a value in a box, and flatMap snaps box-returning steps into one pipeline

## The Tracking Code Behind Three Maybe-Empty Lookups

**Tags:** `core idea` (blue), `wrapped value` (green), `chaining rule` (orange)

- **The shop** — a coffee shop's online store; a support agent needs a customer's tracking code
- **The chain** — email → customer → last order → tracking code: three lookups, each can come up empty
- **The box** — each lookup returns a box: either it holds a value or it is marked empty
- **The rule** — flatMap opens a full box, runs the next lookup, and hands back that lookup's box
- **The skip** — flatMap on an empty box does nothing and passes the empty box straight along

*Example (italic):* The agent writes one line — findCustomer(email).flatMap(lastOrder).flatMap(trackingCode) — and gets exactly one box back.

**Key point:** A monad is just this pair: a wrapped value (the box) plus a chaining rule (flatMap) that lets box-returning steps snap together into one pipeline.

### Visualization (canvas `c1`, 720×300)

Two-row flow diagram: a chain where every lookup succeeds (full boxes flow through) vs a chain that hits an empty box (the empty slides through the remaining steps untouched).

- **Title (bold 15px, `#1a5276`, top center):** "Each Step Returns a Box; flatMap Connects the Boxes".
- **Row 1 (y=100), label 12px `#444` at x=15:** "full boxes"; blue `#2a78d6` rounded box at x=105 labeled "customer #71" (12px), 3px arrow labeled "flatMap(lastOrder)" (11px `#6b7280`) to a blue box at x=305 labeled "order #204", 3px arrow labeled "flatMap(trackingCode)" to a green `#008300` box at x=520 labeled "code TRK-88" with bold 12px green "✓".
- **Row 2 (y=210), label:** "empty box"; blue box at x=105 labeled "customer #85", 3px arrow to a mute `#6b7280` dashed-border box at x=305 labeled "empty — no order", then a dashed 2px `#6b7280` (dash 4/3) arrow to a mute dashed box at x=520 labeled "empty — step skipped".
- **Box style:** 150–165px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(0,131,0,0.12)` / `rgba(107,114,128,0.10)`, 12px `#2c3e50` text.
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=270):** "an empty box slides through the rest of the chain — no crash".
- **Caption (12px `#444`, bottom right):** "customer and order ids illustrative".

## Ten Emails Through the Chain, By Hand

**Tags:** `worked example` (blue), `short-circuit` (green)

- **The batch** — the agent runs the same one-line chain for 10 customer emails from today's tickets
- **Step 1** — findCustomer: 2 emails have typos, so 8 of the 10 boxes come back full
- **Step 2** — lastOrder: 2 of those customers never ordered, so 6 of the 8 boxes stay full
- **Step 3** — trackingCode: 1 order has not shipped yet, so 5 of the 6 boxes stay full
- **Hand-check** — 10 − 2 − 2 − 1 = 5 codes out; the 5 empty boxes carried no crashes with them

*Example (italic):* Email #3 dies at step 1; the two remaining flatMaps see an empty box and skip — no null pointer, no try/catch.

**Key point:** Each empty box short-circuits the rest of its own chain quietly; the code path for the 5 successes and the 5 failures is the same single line.

### Visualization (canvas `c2`, 720×300)

Funnel bar chart of the batch: how many boxes are still full after each lookup step, dropping 10 → 8 → 6 → 5.

- **Title (bold 15px, `#1a5276`, top center):** "10 Emails Enter the Chain, 5 Tracking Codes Come Out".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; y = full boxes 0 to 10 (18px per unit), gridlines `#e5e9ef` at 2/4/6/8 with 12px `#444` tick labels.
- **Bars:** width 90, left edges at x = `[90, 240, 390, 540]`, values `[10, 8, 6, 5]` (heights `[180, 144, 108, 90]`); first three bars fill `rgba(42,120,214,0.30)` with 2px `#2a78d6` border, last bar fill `rgba(0,131,0,0.30)` with 2px `#008300` border; bold 13px `#1a5276` value labels on top; 12px `#444` stage labels under the baseline: "emails in", "customers found", "orders found", "codes found".
- **Drop labels (12px red `#e74c3c`, centered in the gaps between bars at y≈70):** "−2 typos", "−2 never ordered", "−1 not shipped".
- **Annotation (bold 13px green `#008300`, upper right near x=460, y=45):** "5 codes, 0 null checks, 0 crashes".
- **Caption (12px `#444`, bottom right):** "ticket counts illustrative".

## The Same Rule in Lists, Promises, and Pipelines

**Tags:** `where it's used` (blue), `lists & promises` (green), `big data` (orange)

- **Lists** — a list is a box holding many; flatMap(items) turns 3 orders with 2, 3, 1 items into 6 flat rows
- **Big data** — Spark's flatMap is exactly this: one input row becomes 0, 1, or many output rows
- **Promises** — a future is a box holding "a value, later"; .then chains it just like flatMap
- **Null-safe** — the ?. operator in many languages is flatMap for maybe-missing fields, in disguise
- **Pipelines** — feature pipelines chain maybe-failing steps (parse → join → score) the same way

*Example (italic):* flatMapping items over the shop's 3 open orders yields one flat picking list of 6 items for the barista.

**Key point:** The wrapper changes — maybe-missing, many-at-once, later-in-time — but the chaining rule is identical, so one mental model covers Optionals, lists, and promises.

### Visualization (canvas `c3`, 720×300)

Three-row flow diagram: the same box-plus-flatMap pattern wearing three costumes — Optional, List, Promise — each row an input box, a flatMap arrow, an output box.

- **Title (bold 15px, `#1a5276`, top center):** "Three Costumes, One Rule: Optional, List, Promise".
- **Row 1 (y=85), label 12px `#444` at x=15:** "Optional"; blue `#2a78d6` rounded box at x=120 labeled "box(order #204)" (12px), 3px arrow labeled "flatMap(trackingCode)" (11px `#6b7280`) to a green `#008300` box at x=480 labeled "box(TRK-88)".
- **Row 2 (y=165), label:** "List"; blue box at x=120 labeled "3 orders (2, 3, 1 items)", 3px arrow labeled "flatMap(items)" to an aqua `#199e70` box at x=480 labeled "6 items, one flat list".
- **Row 3 (y=245), label:** "Promise"; blue box at x=120 labeled "future(response)", 3px arrow labeled ".then(parseJson)" to a violet `#4a3aa7` box at x=480 labeled "future(json)".
- **Box style:** 175–195px wide, 38px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(0,131,0,0.12)` / `rgba(25,158,112,0.12)` / `rgba(74,58,167,0.10)`, 12px `#2c3e50` text.
- **Annotation (bold 13px magenta `#d55181`, top right near x=470, y=48):** "open the box, run the step, hand back one box".
- **Caption (12px `#444`, bottom right):** "item counts illustrative".

## map Wraps Twice, flatMap Wraps Once

**Tags:** `common mistake` (red), `map vs flatMap` (orange)

- **The slip** — using map with a box-returning step wraps the result again: box(box(code))
- **The symptom** — types like Optional<Optional<String>> or lists of lists needing an extra unwrap
- **The fix** — flatMap does map plus one flatten, keeping the wrapping exactly one layer deep
- **The list twin** — map(items) on the 3 orders gives 3 nested lists; flatMap(items) gives 6 flat rows
- **Rule of thumb** — step returns a plain value: use map; step returns a box: use flatMap

*Example (italic):* order.map(trackingCode) returns box(box("TRK-88")); the agent's equality check against "TRK-88" silently never matches.

**Common mistake:** Reaching for map out of habit. When the step itself returns a box, map double-wraps the result — flatMap exists precisely to remove that one extra layer.

### Visualization (canvas `c4`, 720×300)

Two-row diagram contrasting the same step run through map (a box drawn inside a box — double-wrapped) vs flatMap (one clean box).

- **Title (bold 15px, `#1a5276`, top center):** "Same Step, Two Verbs: map Nests, flatMap Flattens".
- **Row 1 (y=100), label 12px `#444` at x=15:** "map(trackingCode)"; blue `#2a78d6` rounded box at x=200 labeled "box(order #204)" (12px), 3px arrow to a red `#e74c3c` outer box at x=460 (190px wide, 56px tall) containing a smaller inner red-bordered box labeled "box(TRK-88)", outer box labeled above in bold 12px red "box(box(TRK-88)) ✗".
- **Row 2 (y=215), label:** "flatMap(trackingCode)"; blue box at x=200 labeled "box(order #204)", 3px arrow to a single green `#008300` box at x=460 labeled "box(TRK-88)" with bold 12px green "✓ one layer".
- **Box style:** plain boxes 150–170px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(231,76,60,0.10)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text; inner nested box 120px wide, 30px tall.
- **Annotation (bold 13px orange `#d95926`, centered near y=280):** "one layer of wrapping is the whole contract".
- **Caption (12px `#444`, bottom right):** "code value illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); the funnel values `[10, 8, 6, 5]` with drops of 2/2/1 are invented and labeled illustrative, and must match the section-2 text (10 − 2 − 2 − 1 = 5); the list example (3 orders with 2, 3, 1 items → 6 flat rows) reuses the same numbers in sections 3 and 4; customer/order ids and "TRK-88" are illustrative.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
