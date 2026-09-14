# Value Types vs Reference Types

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Value Types vs Reference Types

**Subtitle:** A value variable is a box that holds the thing itself; a reference variable is a box that holds an arrow to where the thing lives — copy the box and you copy either the thing or just the arrow

## One Sticky Note, One Shared Doc

**Tags:** `core idea` (blue), `copy vs link` (green), `the box` (orange)

- **The sticky note** — Ana writes "5 eggs" on a note and hands Ben a photocopy; each now owns a note
- **Ben's edit** — Ben crosses out 5 and writes 6 on his copy; Ana's note still says 5, untouched
- **The shared doc** — the grocery list lives in one online doc; Ana and Ben each hold a link to it
- **One list, two links** — Ben adds "milk" through his link; Ana opens hers and milk is already there
- **The two kinds** — a value is a photocopy in its own box; a reference is a link pointing at one shared box

*Example (italic):* Copying "5 eggs" gives Ben his own number to scribble on; sending the grocery-doc link gives him the power to change what Ana sees.

**Key point:** A value variable stores the thing itself, so copies are independent; a reference variable stores an arrow to the thing, so copies all point at the same one.

### Visualization (canvas `c1`, 720×300)

Two-panel diagram: left panel shows the photocopied sticky note (two independent boxes), right panel shows two link tags whose arrows converge on one shared doc box.

- **Title (bold 15px, `#1a5276`, top center):** "Photocopy vs Link: Two Notes, One Doc".
- **Panel split:** vertical 1px `#e5e9ef` divider at x=360; left panel header bold 13px blue `#2a78d6` at (180, 55) centered: "VALUE — photocopy"; right panel header bold 13px green `#008300` at (540, 55) centered: "REFERENCE — shared link".
- **Left panel:** Ana's note = rounded rect at (55, 90) size 110×70, fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border, 12px `#444` label "Ana's note" above, bold 16px `#2c3e50` text inside "5 eggs"; Ben's note = same-style rect at (215, 90), label "Ben's copy", inside text "6 eggs" (his edit); a 2px `#6b7280` arrow from Ana's right edge to Ben's left edge, 11px `#6b7280` label "photocopy" above it; 12px `#444` caption under the pair at y=200 centered on x=190: "Ben changed 5 → 6; Ana still has 5".
- **Right panel:** two small tag rects at (400, 85) and (400, 145), each 90×40, fill `rgba(0,131,0,0.12)`, 2px `#008300` border, bold 12px `#2c3e50` texts "Ana's link" / "Ben's link"; one doc rect at (565, 85) size 120×100, fill `#f8f9fa`, 2px `#199e70` border, 12px `#444` header "grocery doc", inside 13px `#2c3e50` lines "eggs" / "milk"; 2px green arrows from each tag's right edge to the doc's left edge with small arrowheads.
- **Annotation (bold 12px orange `#d95926`, near x=470, y=235, two lines):** "Ben added milk —" / "Ana sees it instantly".
- **Caption (11px `#444`, bottom right):** "illustrative — two boxes vs two arrows into one box".

## Tracing It by Hand: 5 Eggs and a Grocery List

**Tags:** `worked example` (blue), `memory boxes` (green)

- **Value trace** — `a = 5`, then `b = a`, then `b = b + 1`: now `a` is 5 and `b` is 6 — two boxes
- **The copy** — `b = a` copied the 5 itself into b's box, so changing b never touches a
- **Python fine print** — `b = a` shares one 5 object, but numbers can't change in place: it acts copied
- **Reference trace** — `xs = ["eggs", "milk"]`, then `ys = xs`, then `ys.append("bread")`
- **The surprise** — print `xs` and it shows `["eggs", "milk", "bread"]` — ys copied the arrow, not the list
- **One list all along** — xs and ys are two names on the same box; append through either, both "change"

*Example (italic):* Redo it on paper: draw a box per variable, put 5 or an arrow inside, and step through the three lines — the value pair ends 5 and 6, the list pair ends as one 3-item list.

**Key point:** `b = a` copies the number, so a stays 5 while b becomes 6; `ys = xs` copies the arrow, so appending "bread" through ys also shows up in xs.

### Visualization (canvas `c2`, 720×300)

Two-row memory-box diagram: the top row traces the integer copy (independent boxes), the bottom row traces the list assignment (two names, two arrows, one list box that grows to 3 items).

- **Title (bold 15px, `#1a5276`, top center):** "The Same Three Lines, Two Different Endings".
- **Row labels (bold 13px `#1a5276`, left at x=20):** "integers" at y=105, "lists" at y=215.
- **Top row:** box `a` = rounded rect at (110, 75) size 80×55, fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border, bold 12px `#444` name "a" above, bold 15px `#2c3e50` "5" inside; box `b` at (220, 75) same style, name "b", inside "6"; 11px `#6b7280` note under b at y=145: "was 5, then +1"; bold 12px blue annotation at (420, 100): "two boxes — a is still 5".
- **Bottom row:** name tags `xs` at (110, 185) and `ys` at (110, 245), each 60×34, fill `rgba(0,131,0,0.12)`, 2px `#008300` border, bold 12px `#2c3e50` labels; one list rect at (280, 190) size 230×70, fill `#f8f9fa`, 2px `#199e70` border, three inner cells (each ~70px wide) with 13px `#2c3e50` texts "eggs", "milk", "bread"; the "bread" cell fill `rgba(217,89,38,0.18)` with 11px `#d95926` label "appended" below it; 2px green arrows from xs and ys to the list rect's left edge.
- **Annotation (bold 12px green `#008300`, near x=560, y=230, two lines):** "one list, two names —" / "xs shows bread too".
- **Caption (11px `#444`, bottom right):** "illustrative — boxes hold values; names hold arrows".

## Where the Shared Arrow Bites a Data Scientist

**Tags:** `where it's used` (blue), `hidden mutation` (red), `functions` (orange)

- **Passing data** — hand a list to a function and the function receives the arrow, not a fresh copy
- **The setup** — `prices = [40, 25, 15]`; call `apply_discount(prices)` which halves each item in place
- **The bite** — after the call, the caller's own `prices` reads `[20.0, 12.5, 7.5]` — the original is gone
- **Why languages do it** — copying an arrow is one cheap step; copying a million-row table is not
- **The fix** — functions that must not change the caller's data should work on an explicit copy

*Example (italic):* A "quick preview" function that drops bad rows in place quietly shrinks the caller's dataset, and every later analysis runs on the mutilated version.

**Key point:** Functions receive arrows to big objects, so an in-place edit inside the function rewrites the caller's data — halve `[40, 25, 15]` inside and the caller holds `[20.0, 12.5, 7.5]`.

### Visualization (canvas `c3`, 720×300)

Flow diagram: the caller's price list box on the left, the function box on the right receiving only an arrow, and a before/after strip showing the caller's list rewritten to the halved values.

- **Title (bold 15px, `#1a5276`, top center):** "The Function Got an Arrow — and Rewrote Your List".
- **Caller box:** rounded rect at (50, 80) size 200×80, fill `rgba(42,120,214,0.12)`, 2px `#2a78d6` border, bold 12px `#1a5276` header "caller: prices"; inside, three 13px `#2c3e50` cells "40", "25", "15" (before state, drawn struck through with a 2px `#e74c3c` line).
- **Function box:** rounded rect at (440, 80) size 230×80, fill `#f8f9fa`, 2px `#d95926` border, bold 12px `#d95926` header "apply_discount(p)"; inside 12px `#444` text "p[i] = p[i] / 2 — in place".
- **Arrow:** 3px `#6b7280` arrow from the caller box's right edge to the function box's left edge, bold 12px `#6b7280` label above it: "arrow passed, no copy made".
- **After strip:** rounded rect at (50, 200) size 200×55, fill `rgba(217,89,38,0.15)`, 2px `#d95926` border, bold 12px `#d95926` header "caller after the call"; inside three 13px `#2c3e50` cells "20.0", "12.5", "7.5".
- **Return arrow:** 2px dashed (dash 5/4) `#d95926` arrow from the function box's bottom edge curving to the after strip, 11px `#d95926` label "same box, new contents".
- **Annotation (bold 13px red `#e74c3c`, near x=470, y=230, two lines):** "the original 40, 25, 15" / "no longer exists anywhere".
- **Caption (11px `#444`, bottom right):** "illustrative — in-place edits travel back through the arrow".

## Equal Is Not the Same Box

**Tags:** `common mistake` (red), `identity vs equality` (orange)

- **Two questions** — "do they hold the same contents?" and "are they the very same box?" are different
- **Same contents** — Ana and Ben each type their own list `["eggs", "milk"]`: equal (`==` is true)
- **Different boxes** — those two lists are separate docs, so identity (`is`) is false; edits stay private
- **Same box** — after `ys = xs`, both `==` and `is` are true: one box wearing two name tags
- **The mistake** — testing `is` when you meant `==`, or assuming equal contents means shared fate

*Example (italic):* Two shoppers with identical handwritten lists still shop independently; two shoppers sharing one doc do not — equality says lookalike, identity says same object.

**Common mistake:** Reading "the lists are equal" as "they are the same list". Equality compares contents; identity compares arrows — only identical arrows make an edit show up in both places.

### Visualization (canvas `c4`, 720×300)

Two-panel comparison: left panel shows two separate list boxes with identical contents (equal, not identical), right panel shows two names arrowed into one box (equal and identical), each with its `==` / `is` verdict.

- **Title (bold 15px, `#1a5276`, top center):** "Lookalike Boxes vs One Shared Box".
- **Panel split:** vertical 1px `#e5e9ef` divider at x=360; left header bold 13px blue `#2a78d6` centered at (180, 55): "equal, NOT the same"; right header bold 13px green `#008300` centered at (540, 55): "equal AND the same".
- **Left panel:** two list rects at (65, 85) and (65, 165), each 190×55, fill `rgba(42,120,214,0.12)`, 2px `#2a78d6` border, 11px `#444` names "Ana's list" / "Ben's list" to the left-top of each, inside 13px `#2c3e50` text "eggs | milk" in both; verdict lines at (180, 250) centered, 13px: `#008300` "== true", then `#e74c3c` bold "is false" beside it.
- **Right panel:** name tags "xs" and "ys" at (400, 90) and (400, 160), each 60×34, fill `rgba(0,131,0,0.12)`, 2px `#008300` border, bold 12px labels; one list rect at (530, 105) size 160×65, fill `#f8f9fa`, 2px `#199e70` border, inside 13px `#2c3e50` "eggs | milk"; 2px green arrows from both tags to the rect's left edge; verdict line at (540, 250) centered, 13px: `#008300` "== true", then `#008300` bold "is true".
- **Annotation (bold 12px violet `#4a3aa7`, near x=180, y=280, centered):** "same words, separate fates".
- **Caption (11px `#444`, bottom right):** "illustrative — == asks about contents, is asks about the box".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all box contents, prices, and list items are the hardcoded literals above (`5`, `6`, `["eggs", "milk", "bread"]`, `[40, 25, 15]`, `[20.0, 12.5, 7.5]`) — no randomness; the charts are box-and-arrow diagrams drawn from fixed pixel coordinates, and the text's numbers must match the chart's numbers exactly.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
