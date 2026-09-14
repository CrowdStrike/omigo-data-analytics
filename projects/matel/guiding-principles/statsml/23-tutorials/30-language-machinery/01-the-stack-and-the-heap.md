# The Stack and the Heap

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** The Stack and the Heap

**Subtitle:** A running program has two places to get memory: the stack, a tidy pile that grows and shrinks with each function call, and the heap, a big open storeroom where you ask for space and get a tag pointing to it

## The Notepad and the Coat Check

**Tags:** `core idea` (blue), `two memories` (green), `restaurant analogy` (orange)

- **The restaurant** — picture a waiter with a notepad and, by the door, a coat check with numbered tags
- **The notepad is the stack** — each table gets a fresh page on top; when the table pays, tear it off
- **Top page only** — the waiter only ever writes on or tears off the top page, never digs into the middle
- **The coat check is the heap** — hand over a coat of any size, get tag #101; it stays until you claim it
- **The tag is the address** — the waiter's pocket holds the small tag, not the coat; the coat sits elsewhere
- **Programs do both** — function calls use notepad pages (stack); big or long-lived data goes to coat check (heap)

*Example (italic):* A table's scribbled order lives one page deep on the notepad, but a customer's bulky winter coat gets tag #101 and a shelf in the back room.

**Key point:** The stack is scratch paper that cleans itself up in strict last-in-first-out order; the heap is rented shelf space you locate through a tag (an address) and must eventually give back.

### Visualization (canvas `c1`, 720×300)

Side-by-side diagram: left half is the notepad drawn as a neat pile of stacked pages, right half is the coat-check room drawn as scattered tagged boxes with free gaps between them.

- **Title (bold 15px, `#1a5276`, top center):** "Two Ways to Get Memory: the Notepad (stack) and the Coat Check (heap)".
- **Left panel (stack):** three rectangles stacked bottom-up, each 190px wide × 46px tall, left edge x=70, bottoms at y=250, y=202, y=154; fill `rgba(42,120,214,0.20)`, 2px `#2a78d6` border; centered 12px `#1a5276` labels bottom-to-top: "page 1 — table 4's order", "page 2 — table 9's order", "page 3 — drinks round (top)"; bold 13px `#2a78d6` header "STACK — tidy pile" at x=165 (centered), y=56; small blue downward arrow centered over the top page (x=165) pointing at its top edge, with 11px `#2a78d6` label "add / remove here only" above it.
- **Right panel (heap):** four rounded rectangles placed irregularly inside a dashed 1px `#6b7280` region from (400, 105) to (690, 265): box A 110×55 at (415, 120) labeled "coat #101", box B 70×40 at (560, 130) labeled "bag #102", box C 90×50 at (430, 200) labeled "coat #103", box D 60×38 at (585, 205) labeled "hat #104"; fills `rgba(0,131,0,0.18)`, 2px `#008300` borders, 12px `#1a5276` labels; two unfilled gaps left blank with 11px `#6b7280` label "free space" at (630, 165); bold 13px `#008300` header "HEAP — tagged shelves" centered at x=545, y=90.
- **Annotation (bold 12px orange `#d95926`, two lines, centered near x=360, y=55):** "stack: order is automatic" / "heap: the tag is the only way back".
- **Caption (12px `#444`, bottom right):** "illustrative — page and shelf sizes invented".

## One Bill, Three Pages Deep

**Tags:** `worked example` (blue), `call stack` (green)

- **The program** — `main` starts the shift, calls `makeBill` for a table, which calls `addTax` on the total
- **Box counts** — `main` needs 3 boxes of scratch space, `makeBill` needs 2, `addTax` needs 1
- **Growing** — start at 0; `main` runs: 3 boxes; call `makeBill`: 5; call `addTax`: 6 boxes at the peak
- **Shrinking** — `addTax` returns: back to 5; `makeBill` returns: back to 3; shift ends: 0 again
- **Reverse order** — the last page added is always the first torn off; nothing is ever freed out of turn
- **Meanwhile on the heap** — `makeBill` stores a 40-box receipt image at the coat check and keeps tag #101

*Example (italic):* The stack climbs 0 → 3 → 5 → 6 boxes and walks back down 6 → 5 → 3 → 0, while the 40-box receipt image sits on the heap under tag #101 the whole time.

**Key point:** Stack usage is a staircase you can trace by hand — up 3, up 2, up 1, then back down the same steps — while heap data (the 40-box image behind tag #101) ignores the staircase and lives on.

### Visualization (canvas `c2`, 720×300)

Single-panel step chart: boxes of stack space in use across the seven moments of the program, drawn as a filled staircase, with the peak marked and a flat heap line for contrast.

- **Title (bold 15px, `#1a5276`, top center):** "Stack Boxes in Use: up 3, up 2, up 1 — then back down the same stairs".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 185; x = seven moments at equal spacing with 12px `#444` labels below (angled if needed): `["start", "main runs", "call makeBill", "call addTax", "addTax done", "makeBill done", "end"]`; y = boxes 0 to 8 with light `#e5e9ef` gridlines at 2, 4, 6 and 12px `#444` tick labels.
- **Step curve:** blue `#2a78d6` 3px stepped line through hardcoded values `[0, 3, 5, 6, 5, 3, 0]` (horizontal-then-vertical steps); fill under the steps `rgba(42,120,214,0.15)`.
- **Peak marker:** dashed blue (dash 4/3) horizontal line at y-value 6 across the plot; bold 13px `#2a78d6` label above the peak step: "peak: 6 boxes".
- **Heap line:** green `#008300` 2px dashed (dash 6/4) horizontal line at y-value 2 from the "call makeBill" moment to the right edge, 12px `#008300` label at its right end: "heap: tag #101 still held (40 boxes, off this scale)".
- **Annotation (bold 12px orange `#d95926`, near the descending steps, x≈470, y≈100):** "freed in exact reverse order — no cleanup code needed".
- **Caption (12px `#444`, bottom right):** "illustrative box counts".

## Why a Programmer Should Care

**Tags:** `where it's used` (blue), `speed` (green), `lifetimes` (orange)

- **Stack is fast** — getting a page is one move of a bookmark; here, 1 step versus about 25 for the heap
- **Heap is flexible** — the coat check takes any size, and the coat outlives the table that checked it in
- **Stack is small** — the notepad has limited pages; endless self-calls fill it: a stack overflow
- **Heap must be returned** — forget to hand tags back and shelves fill up forever: a memory leak
- **Languages differ** — C makes you return tags yourself; Python and Java send a cleaner (garbage collector)
- **Data work lives on the heap** — a million-row table never fits a notepad page; only its tag rides the stack

*Example (italic):* A data frame with a million rows sits on the heap; the variable name in your function is just a tag on the stack, which is why passing it around costs almost nothing.

**Key point:** Choose by lifetime and size: short-lived, small, function-local things suit the stack; big things or things that must outlive the call belong on the heap — at the price of slower handouts and cleanup duty.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart: two bars comparing the relative work to get one piece of memory from the stack versus the heap, with a second dashed marker row noting who does the cleanup.

- **Title (bold 15px, `#1a5276`, top center):** "Handing Out Memory: Relative Work per Request".
- **Axis:** horizontal 2px `#999` line at y=225 from x=230 to x=680 (width 450), scale 0 to 30 steps; 12px `#444` tick labels "0", "5", "10", "15", "20", "25", "30" below.
- **Bar 1 (y=105, 34px tall), 12px `#444` row label at x=20:** "stack — move one bookmark"; blue `#2a78d6` bar fill `rgba(42,120,214,0.35)` with 2px blue border from 0 to 1 step; bold 13px `#2a78d6` value label "1 step" just right of the bar end.
- **Bar 2 (y=165, 34px tall), row label:** "heap — search shelves for a fit"; green `#008300` bar fill `rgba(0,131,0,0.25)` with 2px green border from 0 to 25 steps; bold 13px `#008300` value label "25 steps" right of the bar end.
- **Cleanup note (12px `#6b7280`, x=20, y=250, two lines):** "cleanup: stack — automatic on return" / "heap — you (or a garbage collector) must free it".
- **Annotation (bold 13px orange `#d95926`, near x=420, y=75):** "about 25× more work per handout — and you still owe the cleanup".
- **Caption (12px `#444`, bottom right):** "illustrative step counts, not benchmarks".

## The Tag That Outlives the Coat

**Tags:** `common mistake` (red), `dangling pointer` (orange)

- **The setup** — a function writes a total on its own notepad page and hands back the page's location
- **The tear-off** — the moment the function returns, that page is torn off and the spot is up for reuse
- **The dangling tag** — the caller still holds a location that now points at a torn-off, reused page
- **Sometimes it "works"** — the old ink may still be readable for a while, which makes the bug sneaky
- **The fix** — hand back the value itself, or check the data into the heap and return its coat-check tag
- **Not the leak** — a leak is the opposite mistake: the coat stays on the shelf after every tag is lost

*Example (italic):* The waiter points you to "page 3 of my notepad" for your total, then tears page 3 off — by the time you look, page 3 is someone else's drink order.

**Common mistake:** Returning the address of a stack variable. The stack page dies with the function call; only heap tags are safe to hand to code that runs later.

### Visualization (canvas `c4`, 720×300)

Two-panel before/after diagram: the left panel shows a caller holding an arrow into a live stack page; the right panel shows the same arrow after the page is torn off, now landing on reused space.

- **Title (bold 15px, `#1a5276`, top center):** "A Dangling Tag: the Page Is Gone but the Arrow Remains".
- **Panel split:** vertical 1px `#e5e9ef` divider at x=360; 13px bold `#444` panel headers "during the call" centered at x=190, y=55 and "after the call returns" centered at x=530, y=55.
- **Left panel:** two stacked rectangles 170×46, left edge x=100, bottoms y=250 and y=202; lower box fill `rgba(42,120,214,0.20)`, 2px `#2a78d6` border, 12px label "caller's page"; upper box same style, 12px label "makeBill's page: total = 42"; a 3px `#1a5276` arrow from a small 12px `#1a5276` note "tag in caller's pocket" at (60, 90) curving to the upper box; bold 12px `#008300` check label "points at live data" near (250, 120).
- **Right panel:** one rectangle 170×46 at left edge x=440, bottom y=250, same blue style, label "caller's page"; above it a grey rectangle 170×46, bottom y=202, fill `rgba(107,114,128,0.18)`, 2px dashed `#6b7280` border, 12px `#6b7280` label "torn off — reused by others"; the same 3px arrow, now red `#e74c3c`, from the note "same tag, kept too long" at (400, 90) to the grey box; bold 12px `#e74c3c` cross label "points at junk" near (600, 120).
- **Annotation (bold 13px magenta `#d55181`, centered near x=360, y=285):** "stack memory dies with the call — return the value, or use a heap tag".
- **Caption (12px `#444`, bottom right):** "illustrative — layout simplified".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all values are the hardcoded literals above (no `Math.random()`): the step array `[0, 3, 5, 6, 5, 3, 0]` in c2, the 1-vs-25 step bars in c3, and the box positions/sizes in c1 and c4; all invented numbers carry an "illustrative" caption.
- **Consistency check:** box counts (3, 2, 1, peak 6), heap size (40 boxes), tag numbers (#101–#104), the 1-vs-25 step comparison, and the total 42 must match between text bullets and their charts.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
