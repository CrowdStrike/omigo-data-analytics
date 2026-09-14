# Templates & Generics

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Templates & Generics

**Subtitle:** A generic is ordinary code with a type-shaped hole in it — write the box once, fill in the type later; the compiler either stamps a tailored copy per filling (monomorphization) or keeps one copy and forgets the label (erasure)

## A Moving Box with a Blank on the Label

**Tags:** `core idea` (blue), `the hole` (green), `one design` (orange)

- **The blank label** — a print shop sells one moving-box design whose label reads "BOX OF ____"
- **Fill the hole** — write Books, Mugs, or Plates in the blank and it becomes a box for that thing only
- **One design** — nobody draws a books-box and a mugs-box separately; a single template covers them all
- **The rule** — whatever fills the blank is the only thing allowed in and the only thing you get out
- **In code** — `Box<T>` is that label: T is the hole, and `Box<Mug>` accepts and returns only mugs

*Example (italic):* A helper who tries to drop a mug into the box labeled "BOX OF BOOKS" is stopped at the flap — the moment the blank was filled, the box's contents were decided.

**Key point:** A generic is normal code with a type-shaped hole; filling the hole once fixes what goes in and what comes out, with no second copy of the code written by hand.

### Visualization (canvas `c1`, 720×300)

Diagram: one template box with a blank label on the left, arrows fanning right to three filled-in boxes, and a rejected mug at the books box showing the label being enforced.

- **Title (bold 15px, `#1a5276`, top center):** "One Label Design, Any Filling: BOX OF ____".
- **Template box:** rounded rect at (60, 95) size 150×90, fill `#f8f9fa`, 2px `#1a5276` border; bold 14px `#2c3e50` text inside "BOX OF ____"; 12px `#6b7280` caption below at y=205 centered on x=135: "the template — T is the blank".
- **Filled boxes (right column at x=430, each 140×50):** y=65 fill `rgba(42,120,214,0.15)` border 2px `#2a78d6` bold 13px text "BOX OF BOOKS"; y=130 fill `rgba(0,131,0,0.12)` border 2px `#008300` text "BOX OF MUGS"; y=195 fill `rgba(217,89,38,0.15)` border 2px `#d95926` text "BOX OF PLATES".
- **Arrows:** 2px `#6b7280` lines with small arrowheads from the template's right edge to each filled box's left edge; 11px `#6b7280` labels above each arrow midpoint: "T = Books", "T = Mugs", "T = Plates".
- **Rejected item:** 13px `#2c3e50` text "a mug" at (620, 78) with a bold 2px `#e74c3c` X drawn over the arrowhead pointing at the BOOKS box; 11px `#e74c3c` label below: "label says Books".
- **Annotation (bold 12px violet `#4a3aa7`, near x=135, y=250, two lines):** "one design, written once —" / "the blank fixes what fits".
- **Caption (11px `#444`, bottom right):** "illustrative — a type-shaped hole filled three ways".

## Four Fillings, Two Compilers: 8 KB or 2 KB

**Tags:** `worked example` (blue), `monomorphization` (green), `erasure` (orange)

- **The template** — compiled on its own, the box code comes out as 2 KB of machine instructions
- **Four fillings** — the program fills the blank with 4 types: books, mugs, plates, and lamps
- **Monomorphize** — stamp one tailored copy per filling: 4 copies × 2 KB = 8 KB of box code shipped
- **Erase** — keep a single 2 KB copy, throw every label away, handle each item as "some object"
- **Check it by hand** — erased stays 2 KB at 1, 2, 3, or 4 types; stamped grows 2, 4, 6, 8 KB in step

*Example (italic):* The same program through two compilers: the stamping one ships 8 KB of box code, the erasing one ships 2 KB — the 6 KB difference bought four tailor-made copies.

**Key point:** Monomorphization ships copies × size (4 × 2 KB = 8 KB); erasure ships one 2 KB copy no matter how many types fill the hole.

### Visualization (canvas `c2`, 720×300)

Grouped bar chart: shipped box-code size (KB) as the number of filling types grows from 1 to 4, one blue bar per group for stamping and one green bar for erasure.

- **Title (bold 15px, `#1a5276`, top center):** "Box Code Shipped: Stamp a Copy per Type vs Keep One".
- **Axes:** origin x=70, baseline y=245, plot width 560, plot height 175; x = types used, group centers at x = 140, 280, 420, 560 with 12px `#444` labels "1 type", "2 types", "3 types", "4 types"; y = KB 0 to 10, light `#e5e9ef` gridlines at 2, 4, 6, 8 with 12px `#444` labels.
- **Bars:** two 40px-wide bars per group, 8px apart; monomorphized blue `#2a78d6` heights from `[2, 4, 6, 8]` KB; erased green `#008300` heights from `[2, 2, 2, 2]` KB; bold 12px value label in the bar's color above each bar ("2", "4", "6", "8" and "2", "2", "2", "2").
- **Legend (12px, top left at x=80, y=60):** blue swatch "stamped (monomorphized)", green swatch "erased (one shared copy)".
- **Annotation (bold 13px orange `#d95926`, near x=470, y=90):** "4 types: 8 KB stamped vs 2 KB erased".
- **Caption (11px `#444`, bottom right):** "illustrative — template compiles to 2 KB".

## The Price Tag: Fast Calls or a Slim Program

**Tags:** `where it's used` (blue), `speed vs size` (orange), `hot loops` (green)

- **Stamped copy** — the mugs copy handles mugs directly, no lookup, no wrapping: about 1 ns per put
- **Erased copy** — one copy for every type must first wrap each plain number in an object: about 3 ns
- **Who stamps** — C++ and Rust stamp a copy per type: fast calls, fatter binaries, slower compiles
- **Who erases** — Java and TypeScript erase; Java wraps primitives at runtime, TypeScript checks then strips
- **Hot loops** — pushing a million numbers through an erased generic list pays the wrapping a million times

*Example (italic):* The same "put a number in the box" loop runs 1 ns per item stamped and 3 ns erased — over a million numbers that is 1 ms vs 3 ms.

**Key point:** Stamping trades binary size and compile time for straight-line speed; erasure trades a per-call wrapping cost for one slim copy of the code.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart: the cost of one "put a number in the box" call, one plain bar for the stamped copy and one segmented bar for the erased copy splitting real work from wrapping.

- **Title (bold 15px, `#1a5276`, top center):** "One 'Put a Number in the Box' Call, Timed".
- **Axis:** horizontal 2px `#999` line at y=235 from x=250 to x=680 (width 430), scale 0 to 4 ns; 12px `#444` tick labels "0", "1 ns", "2 ns", "3 ns", "4 ns" below.
- **Row 1 (bar top y=100, 28px tall), label 12px `#444` at x=20:** "stamped copy (monomorphized)"; solid blue `#2a78d6` bar from 0 to 1 ns; bold 13px blue label "1 ns" just right of the bar end.
- **Row 2 (bar top y=170, 28px tall), label:** "shared copy (erased)"; segmented bar — 0 to 1 ns green `#008300` with 11px white inset label "real work", 1 to 3 ns orange `#d95926` with 11px white inset label "wrapping (boxing)"; bold 13px orange label "3 ns" just right of the bar end.
- **Annotation (bold 12px orange `#d95926`, near x=430, y=75, two lines):** "2 of the 3 ns is wrapping —" / "× 1,000,000 items: 3 ms vs 1 ms".
- **Caption (11px `#444`, bottom right):** "illustrative timings".

## The Label Is Gone, but the Check Already Happened

**Tags:** `common mistake` (red), `type safety` (orange)

- **The worry** — "if the label is erased, can a mug sneak into the books box while the program runs?"
- **Check first** — the compiler checked every put and take before erasing; a wrong item never compiles
- **Then erase** — the label is dropped only after the whole program is proven to respect it
- **What you lose** — a running erased box cannot answer "what type am I?"; the label truly is gone
- **The mistake** — asking a running generic for its T, e.g. Java's `x instanceof List<Mug>` won't compile

*Example (italic):* Java rejects `books.put(mug)` at compile time even though at runtime both boxes look identical — the guard did its job before the label vanished.

**Common mistake:** Believing erasure means unchecked. The type check runs at compile time; erasure only means the runtime can no longer read the label, not that the rule was never enforced.

### Visualization (canvas `c4`, 720×300)

Two-lane pipeline diagram across three stages (source, compile, run): both lanes pass the same compile-time check; the erasure lane arrives at run time with a gray unlabeled box, the stamping lane with four labeled copies.

- **Title (bold 15px, `#1a5276`, top center):** "Where the Check Happens vs Where the Label Lives".
- **Stage headers (bold 13px `#1a5276`, centered at y=58):** "SOURCE" at x=160, "COMPILE" at x=380, "RUN" at x=600.
- **Lane labels (12px `#444`, left at x=20):** "erasure" at y=118, "stamping" at y=218.
- **Erasure lane:** source box = rounded rect at (100, 92) size 120×46, fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border, bold 13px `#2c3e50` text "Box&lt;Mug&gt;"; compile stage = green `#008300` circle (radius 16) at (380, 115) with bold 14px white check mark, 11px `#008300` label below "types checked"; run stage = rect at (540, 92) size 120×46, fill `#f1f3f5`, 2px dashed `#6b7280` border, 13px `#6b7280` text "Box of ?", 11px `#6b7280` label below "label gone".
- **Stamping lane:** source box at (100, 192) same style, text "Box&lt;T&gt; ×4 fills"; compile stage = green circle at (380, 215) with check, 11px `#008300` label "checked, then ×4"; run stage = four stacked rects at x=540, each 120×17 starting y=178 with 3px vertical gaps, borders 2px `#2a78d6` / `#008300` / `#d95926` / `#4a3aa7`, matching pale fills, 11px `#2c3e50` texts "books copy", "mugs copy", "plates copy", "lamps copy".
- **Arrows:** 2px `#6b7280` lines with arrowheads left-to-right between stages in both lanes.
- **Annotation (bold 13px green `#008300`, centered near x=380, y=285):** "the check already ran — erasure deletes the label, not the guard".
- **Caption (11px `#444`, bottom right):** "illustrative — both lanes reject the mug before running".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all bar heights, sizes, and timings are the hardcoded literal values above (no randomness); the KB arrays `[2, 4, 6, 8]` and `[2, 2, 2, 2]` and the 1 ns / 3 ns timings are invented and labeled "illustrative" in each chart's caption; text numbers must match chart numbers exactly. Diagram type names inside canvas text use plain angle brackets drawn with `fillText` (no HTML escaping needed in JS strings).
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
