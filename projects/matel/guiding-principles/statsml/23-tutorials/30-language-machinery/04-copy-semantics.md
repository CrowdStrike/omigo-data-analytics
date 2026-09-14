# Copy Semantics

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Copy Semantics

**Subtitle:** `=` and a function call copy the sticky note that points at your data, never the data itself — so two names can quietly share one object, and "pass by reference" is really the reference passed by value

## Two Names, One Shopping List

**Tags:** `core idea` (blue), `aliasing` (green), `sticky notes` (orange)

- **The list** — two roommates keep one grocery list: `groceries = ["milk", "eggs", "bread"]`, 3 items
- **The copy that isn't** — `weekend = groceries` writes a second sticky note, not a second list
- **One object** — both notes point at the same list in memory; there is still exactly one list
- **Shared edits** — `weekend.append("cake")` and now `groceries` also shows 4 items
- **Real copies exist** — `list(groceries)` or `groceries.copy()` builds a genuine second list

*Example (italic):* One roommate adds "cake" through the name `weekend`, and the other roommate's `groceries` grows to 4 items — because there was only ever one list.

**Key point:** Assignment copies the reference (the sticky note), never the object — after `b = a` there is one object with two names.

### Visualization (canvas `c1`, 720×300)

Box-and-arrow diagram: two name-tag boxes on the left, both with arrows to a single list box on the right, whose fourth row ("cake") is highlighted as the edit made through the second name.

- **Title (bold 15px, `#1a5276`, top center):** "b = a Copies the Note, Not the List".
- **Name tags:** two rounded boxes 150×40 at x=50, y=85 and x=50, y=185; 1.5px `#1a5276` border, fill `rgba(42,120,214,0.10)`; bold 13px `#1a5276` centered labels `groceries` and `weekend`.
- **List box:** rounded box 230×170 at x=400, y=75; 2px `#2a78d6` border, white fill; bold 12px `#2a78d6` header "the one list" at its top; four 13px `#2c3e50` rows at x=420, y = 125, 155, 185, 215: `"milk"`, `"eggs"`, `"bread"`, `"cake"` — the `"cake"` row in bold green `#008300` with a green `+` prefix.
- **Arrows:** two 2.5px blue `#2a78d6` arrows with small filled arrowheads, from (200, 105) and (200, 205) to the list box's left edge at (400, 130) and (400, 200).
- **Edit label:** 12px green `#008300` text under the lower arrow, near (230, 235): "weekend.append(\"cake\")".
- **Annotation (bold 13px orange `#d95926`, near x=430, y=270, two lines):** "two names, one object —" / "an edit shows under both".
- **Caption (12px `#444`, bottom right):** "illustrative — any mutable object (list, dict, DataFrame) behaves this way".

## Append Sticks, Rebind Doesn't

**Tags:** `worked example` (blue), `mutate vs rebind` (green)

- **Start** — `shopping = ["milk", "eggs", "bread"]`; hand it to two small functions, one after the other
- **Function 1** — `add(lst): lst.append("cake")` edits the shared list; caller now sees 4 items
- **Function 2** — `replace(lst): lst = ["tofu"]` re-points only the function's own note
- **After both** — `shopping` is `["milk", "eggs", "bread", "cake"]`: still 4 items, no tofu anywhere
- **The rule** — mutating through a name changes the shared object; `=` on a name only moves that note

*Example (italic):* Trace it by hand: 3 items, append makes it 4, rebind changes nothing — the caller ends with 4 items and never sees "tofu".

**Key point:** `lst.append(...)` reaches the caller's list; `lst = [...]` inside a function re-points a local note and the caller's 4 items survive untouched.

### Visualization (canvas `c2`, 720×300)

Two side-by-side panels sharing one style: each shows the caller's note and the function's note; in the left panel both arrows land on one list that grows, in the right panel the function's arrow swings away to a new list while the caller's arrow stays put.

- **Title (bold 15px, `#1a5276`, top center):** "Inside a Function: append Reaches Out, = Stays Local".
- **Divider:** 1px `#e5e9ef` vertical line at x=360 from y=45 to y=280.
- **Panel titles (bold 13px):** left, green `#008300` at x=180 centered: "add(lst): lst.append(\"cake\")"; right, orange `#d95926` at x=540 centered: "replace(lst): lst = [\"tofu\"]"; both at y=60.
- **Left panel:** note boxes 110×30 (`shopping` at x=30, y=95; `lst` at x=30, y=160), 1.5px `#1a5276` border, 12px bold `#1a5276` labels; one list box 130×130 at x=210, y=85 with 13px rows `"milk"`, `"eggs"`, `"bread"` in `#2c3e50` and `"cake"` in bold green; 2px blue `#2a78d6` arrows from both notes to the list box; 12px green label "4 items — caller sees it" at x=210, y=245.
- **Right panel:** note boxes `shopping` (x=390, y=95) and `lst` (x=390, y=160), same style; old list box 100×115 at x=550, y=80 with rows `"milk"`, `"eggs"`, `"bread"`, `"cake"` (13px `#2c3e50`); new list box 100×45 at x=550, y=215 with row `"tofu"` (13px `#d95926`); 2px blue arrow `shopping` → old box; 2px dashed (dash 5/4) orange `#d95926` arrow `lst` → new box; 11px `#6b7280` label "old arrow dropped" beside the `lst` note.
- **Annotation (bold 12px orange `#d95926`, near x=390, y=280):** "only the local note moved".
- **Caption (12px `#444`, bottom left):** "illustrative — same picture in Python, Java, JavaScript".

## Where This Bites a Data Scientist

**Tags:** `where it's used` (blue), `shallow copy` (orange), `hidden sharing` (green)

- **Nested data** — `orders = [["milk", 2], ["eggs", 12]]`: a list whose rows are themselves lists
- **Shallow copy** — `backup = orders.copy()` copies the outer list but the two rows stay shared
- **The surprise** — fix a typo with `orders[1][1] = 6` and the "backup" also reads eggs 6, not 12
- **Deep copy** — `copy.deepcopy(orders)` copies rows too; only then does the backup keep eggs 12
- **Same trap elsewhere** — DataFrame views, dicts of lists, default `def f(x=[])` all share this way

*Example (italic):* An analyst "backs up" the order table, cleans eggs from 12 to 6 in the original, and the backup silently reads 6 too — the shallow copy shared every row.

**Key point:** A shallow copy duplicates only the outer container — edit a shared inner row and every "copy" changes with it; deep copy when the insides must be independent.

### Visualization (canvas `c3`, 720×300)

Two side-by-side panels: shallow copy on the left with two outer boxes whose arrows converge on the same two row boxes (one row edited, both names affected), deep copy on the right with fully separate row boxes and the backup keeping the old value.

- **Title (bold 15px, `#1a5276`, top center):** "Shallow Copy Shares the Rows, Deep Copy Doesn't".
- **Divider:** 1px `#e5e9ef` vertical line at x=360 from y=45 to y=280.
- **Panel titles (bold 13px, y=62):** left `#1a5276` centered x=180: "orders.copy() — shallow"; right `#1a5276` centered x=540: "deepcopy(orders) — deep".
- **Left panel:** outer boxes `orders` (x=30, y=85) and `backup` (x=30, y=165), 115×32, 1.5px `#1a5276` border, bold 12px labels; two shared row boxes 140×34 at x=200, y=95 (`["milk", 2]`, 13px `#2c3e50`) and x=200, y=165 (`["eggs", 6]`, bold 13px `#d95926`); four 2px blue `#2a78d6` arrows: each outer box to each row box; 11px `#d95926` label "was 12, edited once" under the eggs row at y=215.
- **Right panel:** outer boxes `orders` (x=390, y=85) and `backup` (x=390, y=165), same style; `orders` arrows to its own rows `["milk", 2]` and `["eggs", 6]` (boxes 130×30 at x=555, y=78 and y=115, eggs value 13px `#d95926`); `backup` arrows to separate rows `["milk", 2]` and `["eggs", 12]` (boxes at x=555, y=170 and y=207, eggs value bold 13px `#008300`); all arrows 2px, orders' in blue `#2a78d6`, backup's in green `#008300`.
- **Annotation (bold 12px magenta `#d55181`, left panel, near x=45, y=255, two lines):** "the 'backup' changed too:" / "both eggs rows read 6".
- **Caption (12px `#444`, bottom right):** "illustrative — quantities invented".

## "Pass by Reference"? Not Quite

**Tags:** `common mistake` (red), `references by value` (orange)

- **The claim** — people say "Python passes lists by reference", then expect `=` inside to reach out
- **What really happens** — the call photocopies the caller's sticky note; both copies point one way
- **Why append works** — either note reaches the same list, so `lst.append("cake")` edits it for both
- **Why rebind fails** — `lst = ["tofu"]` re-points the photocopy; the caller's note never moved
- **The honest name** — references passed by value: the arrow is copied, the object never is

*Example (italic):* True pass-by-reference (C++ `&`) would let `lst = ["tofu"]` replace the caller's list — in Python, Java, and JavaScript the caller still holds 4 items.

**Common mistake:** Hearing "by reference" and expecting assignment inside a function to replace the caller's object. The reference itself travels by value — mutation crosses the call, rebinding never does.

### Visualization (canvas `c4`, 720×300)

Two-row frame diagram on one timeline: the caller's frame on top and the function's frame below, with the call drawn as a photocopy of the arrow, then the rebind swinging only the bottom arrow to a new object while the top arrow never moves.

- **Title (bold 15px, `#1a5276`, top center):** "The Call Copies the Arrow, Not the List".
- **Frame bands:** two full-width rounded rectangles, fill `rgba(42,120,214,0.06)`, 1px `#e5e9ef` border: caller frame x=30, y=55, 660×95; function frame x=30, y=170, 660×95; bold 12px `#6b7280` labels "caller's frame" and "replace()'s frame" at each band's top-left inset (x=42).
- **Caller note:** box 115×32 at x=55, y=95, 1.5px `#1a5276` border, bold 12px `#1a5276` label `shopping`; solid 2.5px blue `#2a78d6` arrow to the list box.
- **List box:** 150×80 at x=300, y=80, 2px `#2a78d6` border; bold 11px `#2a78d6` header "the 4-item list"; 12px `#2c3e50` rows "milk, eggs" and "bread, cake".
- **Function note:** box 115×32 at x=55, y=210, same style, label `lst`; grey 11px `#6b7280` text "photocopied at the call" beneath it at y=252; a light dashed (dash 4/3) 1.5px `#6b7280` vertical connector from the caller note's bottom edge to the function note's top edge.
- **Rebind:** new list box 110×45 at x=470, y=205, 1.5px `#d95926` border, 12px `#d95926` row `"tofu"`; 2.5px dashed (dash 6/4) orange `#d95926` arrow from `lst` to it; a small grey `#6b7280` X (11px stroke pair) on the dropped path from `lst` toward the 4-item list.
- **Annotation (bold 13px green `#008300`, near x=470, y=125, two lines):** "caller's arrow never moved —" / "still 4 items after the call".
- **Caption (12px `#444`, bottom right):** "illustrative — 'call by sharing' is the textbook name".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all list contents, item counts (3 → 4 items, eggs 12 → 6), box positions, and arrow endpoints are the hardcoded literals above (no randomness); the same grocery items and counts must appear in both the text bullets and the diagrams.
- **Diagrams, not plots:** all four canvases are box-and-arrow memory diagrams; draw arrowheads as small filled triangles, use rounded rectangles (radius ~6px) for notes and objects, and keep every font at 11px or larger.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
