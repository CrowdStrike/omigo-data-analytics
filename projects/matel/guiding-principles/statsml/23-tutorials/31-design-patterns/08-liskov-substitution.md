# Liskov Substitution

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Liskov Substitution

**Subtitle:** If code works with a parent type, it must keep working when handed any child type — which is why a square, surprisingly, is not a rectangle in code

## The Frame Shop's Square Problem

**Tags:** `core idea` (blue), `substitution` (green), `square vs rectangle` (orange)

- **The shop** — a custom frame shop's software models every photo frame as a rectangle
- **The contract** — a rectangle has `set_width` and `set_height`, and each can change independently
- **The shortcut** — a developer adds `Square` as a child of `Rectangle`: "a square IS a rectangle, right?"
- **The twist** — to stay square, `Square` overrides both setters so width and height always move together
- **The break** — every routine written for rectangles now silently misbehaves when handed a square

*Example (italic):* The order-sizing routine sets a frame to 8 wide and 3 tall and expects area 24 — handed a square frame, it gets area 9 and cuts the wrong glass.

**Key point:** Liskov Substitution says a child type must honor every promise of its parent — if callers of `Rectangle` can break when given a `Square`, then `Square` is not a valid child, no matter what geometry class says.

### Visualization (canvas `c1`, 720×300)

Two-row flow diagram: the same caller code fed a rectangle (works) vs fed a square (breaks), shown as boxes flowing left to right.

- **Title (bold 15px, `#1a5276`, top center):** "Same Caller Code, Two Shapes: One Keeps the Promise, One Breaks It".
- **Caller box (left, spans both rows):** blue `#2a78d6` rounded box at x=30, y=120, labeled "set_width(8); set_height(3); expect area 24" (12px, two lines).
- **Row 1 (y=95), label 12px `#444` at x=250:** "Rectangle"; 3px arrow from the caller box to a green `#008300` box at x=430 labeled "8 × 3 = area 24" with bold 12px green "✓ promise kept" at its right.
- **Row 2 (y=205), label:** "Square"; 3px arrow to a red `#e74c3c` box at x=430 labeled "3 × 3 = area 9" with bold 12px red "✗ caller broken" at its right.
- **Box style:** 150–200px wide, 44px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(0,131,0,0.12)` / `rgba(231,76,60,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px ink `#1a5276`, centered near y=272):** "substitution failed — Square can't stand in for Rectangle".
- **Caption (12px `#444`, bottom right):** "frame sizes illustrative".

## Resizing a 5×5 Frame, Step by Step

**Tags:** `worked example` (blue), `hand-checkable` (green)

- **The start** — both shapes begin as 5 × 5, so both report area 25; so far they agree
- **Step 1** — `set_width(8)`: the rectangle becomes 8 × 5 = 40; the square jumps to 8 × 8 = 64
- **Step 2** — `set_height(3)`: the rectangle becomes 8 × 3 = 24; the square collapses to 3 × 3 = 9
- **The check** — caller's expectation after both steps is width × height = 8 × 3 = 24, exactly once
- **The verdict** — the rectangle hits 24; the square lands on 9, off by 15 with no error raised

*Example (italic):* Redo it on paper: 5×5 → set_width(8) → set_height(3) gives 24 for the rectangle but 64 then 9 for the square — the square never passes through 8 × 3 at all.

**Key point:** The square doesn't fail loudly — it returns a perfectly legal-looking area of 9 instead of 24, which is the worst kind of bug: a kept type signature hiding a broken behavioral promise.

### Visualization (canvas `c2`, 720×300)

Grouped bar chart: area after each of three steps (start, after set_width(8), after set_height(3)), rectangle bars vs square bars side by side.

- **Title (bold 15px, `#1a5276`, top center):** "Area After Each Call: Rectangle Ends at 24, Square Ends at 9".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; y = area 0 to 70, gridlines `#e5e9ef` at 20/40/60 with 12px `#444` labels; x = three groups centered at x=160, 360, 560 with 12px `#444` labels "start 5×5", "set_width(8)", "set_height(3)".
- **Rectangle bars:** blue `#2a78d6` fill `rgba(42,120,214,0.30)` with 2px solid `#2a78d6` border, 48px wide, left of each group center; heights from areas `[25, 40, 24]`.
- **Square bars:** orange `#d95926` fill `rgba(217,89,38,0.25)` with 2px solid `#d95926` border, 48px wide, right of each group center; heights from areas `[25, 64, 9]`.
- **Value labels:** bold 12px in each bar's color, centered above each bar: 25, 40, 24 / 25, 64, 9.
- **Expected marker:** dashed `#6b7280` (dash 4/3) horizontal segment at area 24 across the third group, 12px `#6b7280` label "caller expects 24".
- **Annotation (bold 13px red `#e74c3c`, near the last square bar, y=110):** "9 ≠ 24 — silent wrong answer".
- **Legend (12px, top right):** blue swatch "Rectangle", orange swatch "Square".
- **Caption (12px `#444`, bottom right):** "areas exact for the steps shown, frame sizes illustrative".

## Where a Data Scientist Trips Over This

**Tags:** `where it's used` (blue), `pipelines` (green), `subclassing` (orange)

- **Estimators** — a pipeline calls `fit` then `predict` on anything estimator-shaped; children must obey both
- **The subclass** — a custom model whose `fit` silently drops rows with missing values breaks that contract
- **The symptom** — the pipeline runs green end to end, but metrics are computed on fewer rows than fed in
- **Data loaders** — a loader child that reorders rows breaks any caller relying on the parent's row order
- **The test** — write tests against the parent's promises and run every child through the same suite

*Example (italic):* A loader is promised 10,000 rows in insertion order; a "faster" child returns 10,000 rows sorted by key, and a downstream train/test split by row position quietly leaks future data.

**Key point:** Every plug-in point — estimator, loader, metric, data source — is a Liskov contract: the caller was written against the parent's behavior, so a child that bends that behavior corrupts results without raising a single error.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart: rows surviving each stage of a 10,000-row pipeline, run with the honest loader vs the row-dropping subclass.

- **Title (bold 15px, `#1a5276`, top center):** "Same Pipeline, Two Loaders: Where 10,000 Rows Quietly Become 8,400".
- **Axis:** vertical 2px `#999` baseline at x=210, bars extend right, max width 460 representing 10,000 rows; 12px `#444` row labels left-aligned at x=20.
- **Rows (top to bottom at y = 70, 110, 170, 210):**
  - "honest loader — loaded": blue `#2a78d6` bar width 460 (10,000 rows), 11px label "10,000" at bar end
  - "honest loader — fit on": green `#008300` bar width 460 (10,000 rows), label "10,000"
  - "subclass — loaded": blue bar width 460 (10,000 rows), label "10,000"
  - "subclass — fit on": red `#e74c3c` bar width 386 (8,400 rows), label "8,400"
- **Bar style:** 18px tall, fills `rgba(42,120,214,0.30)` / `rgba(0,131,0,0.25)` / `rgba(231,76,60,0.25)` with 2px solid borders in each bar's color.
- **Gap marker:** dashed `#6b7280` (dash 4/3) vertical line at x=596 (the 8,400 mark) spanning the bottom row pair, bold 12px red label "1,600 rows vanish, no error" beside it.
- **Annotation (bold 13px ink `#1a5276`, bottom center near y=272):** "the pipeline never complained — only the contract test would catch it".
- **Caption (12px `#444`, bottom right):** "row counts illustrative".

## "Is-A" in English Is Not "Is-A" in Code

**Tags:** `common mistake` (red), `behavior contract` (orange)

- **The trap** — inheritance decided by nouns: a square is a rectangle in geometry, so subclass it
- **The truth** — code contracts are about verbs: `Rectangle` promises independent width and height
- **The check** — before subclassing, ask "can every caller of the parent handle the child unchanged?"
- **Fix 1** — make shapes immutable: `resize` returns a new shape, and the promise everyone relies on shrinks
- **Fix 2** — drop the inheritance: both implement a small `Shape` interface that only promises `area()`

*Example (italic):* An immutable `resized(8, 3)` that returns a fresh 8×3 rectangle keeps every caller correct — the square type simply never claims it can resize sides independently.

**Common mistake:** Believing subclassing encodes real-world taxonomy. It encodes behavioral promises — a child may strengthen what it guarantees, never weaken what callers were promised.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: the noun-driven design (square extends rectangle, caller breaks) vs the contract-driven design (both implement a narrow interface, caller safe).

- **Title (bold 15px, `#1a5276`, top center):** "Design by Noun vs Design by Promise".
- **Row 1 (y=95), label 12px `#444` at x=20:** "by noun"; blue `#2a78d6` rounded box at x=140 labeled "Square extends Rectangle" (12px), 3px arrow to a red `#e74c3c` box at x=420 labeled "inherits set_width / set_height" with bold 12px red "✗ can't keep both promises".
- **Row 2 (y=205), label:** "by promise"; blue box at x=140 labeled "Shape interface: area() only", 3px arrow to a green `#008300` box at x=420 labeled "Rectangle and Square both fit" with bold 12px green "✓ every caller safe".
- **Box style:** 190–220px wide, 44px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(231,76,60,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px orange `#d95926`, centered near y=270):** "narrow the promise until every child can keep it".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates.
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); the resize walkthrough areas (rectangle `[25, 40, 24]`, square `[25, 64, 9]`) are exact arithmetic for the 5×5 → set_width(8) → set_height(3) sequence; frame sizes and the pipeline row counts (10,000 loaded / 8,400 fit) are invented and labeled illustrative.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
