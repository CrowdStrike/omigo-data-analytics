# Scoping & Symbol Tables

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Scoping & Symbol Tables

**Subtitle:** When a program says "x", scoping is the rule for deciding which x it means — and a symbol table is the address book the compiler keeps for each region of the code

## Two Sams in One Office

**Tags:** `core idea` (blue), `name lookup` (green), `nearest wins` (orange)

- **The office** — a company has a Sam in accounting and a Sam on your own five-person team
- **The question** — a teammate says "ask Sam" and nobody is confused: they mean the team's Sam
- **The rule** — you check the smallest group first: your team, then your floor, then the whole company
- **Scoping** — programs use the same rule: a name means the nearest surrounding definition of it
- **Symbol table** — each group keeps a roster of its own names; lookup walks the rosters inside-out
- **A miss** — a name on no roster at all is the compiler's "undefined variable" error

*Example (italic):* "Ask Sam" on your team means teammate Sam; shouted across the whole company, plain "Sam" is ambiguous — you'd have to say which Sam.

**Key point:** A name by itself is ambiguous — scoping resolves it by searching the nearest enclosing roster first, and those rosters are the symbol tables.

### Visualization (canvas `c1`, 720×300)

Nested-boxes diagram: three rounded rectangles nested inside each other (company > floor > team), a "Sam" entry written in both the company box and the team box, and a lookup arrow from a speaker inside the team stopping at the nearer Sam.

- **Title (bold 15px, `#1a5276`, top center):** "'Ask Sam' — the Nearest Roster Wins".
- **Company box:** rounded rectangle x=60, y=50, width 600, height 225, 2px `#6b7280` border, fill `rgba(107,116,128,0.06)`; bold 12px `#6b7280` label "company roster" at its top-left inner corner; 12px `#6b7280` entry "Sam (accounting)" near x=95, y=95.
- **Floor box:** rounded rectangle x=170, y=105, width 460, height 150, 2px `#2a78d6` border, fill `rgba(42,120,214,0.06)`; bold 12px blue `#2a78d6` label "floor 3 roster" top-left; 12px `#6b7280` entry "(no Sam here)" near x=205, y=145.
- **Team box:** rounded rectangle x=300, y=155, width 300, height 80, 2px `#008300` border, fill `rgba(0,131,0,0.08)`; bold 12px green `#008300` label "your team roster" top-left; bold 13px green entry "Sam (teammate)" near x=335, y=205.
- **Speaker:** 7px `#1a5276` dot at x=560, y=205 with 12px `#1a5276` label "you: 'ask Sam'" to its right-above; solid 3px green arrow from the dot to the team's "Sam (teammate)" entry with an arrowhead.
- **Rejected path:** dashed 2px `#6b7280` (dash 4/3) arrow from the dot curving up toward "Sam (accounting)", crossed by a short 2px red `#e74c3c` tick; 11px `#6b7280` label "never reached" beside it.
- **Annotation (bold 12px orange `#d95926`, near x=95, y=260):** "lookup walks inside-out and stops at the first match".
- **Caption (12px `#444`, bottom right):** "illustrative — rosters and names are invented".

## Tracing a Five-Line Program by Hand

**Tags:** `worked example` (blue), `shadowing` (orange)

- **The program** — line 1: `price = 10`; lines 2–4 define `sale()`: `price = 3`, `tax = 1`, `print(price + tax)`
- **Line 5** — after the function runs, the last line is `print(price)`
- **Inside `sale()`** — lookup of `price` checks the function's own table first and finds 3, so it prints 3 + 1 = 4
- **On line 5** — the function's table is gone; lookup finds the outer `price = 10` and prints 10
- **Shadowing** — the inner `price = 3` hid the outer `price = 10` inside the function, nothing more
- **Two tables** — outer table: {price: 10}; `sale()`'s table: {price: 3, tax: 1}; each name lives in exactly one

*Example (italic):* Run it by hand: the program prints 4 first (inner price 3 plus tax 1), then 10 — two different variables that merely share the name `price`.

**Key point:** Every lookup starts in the innermost table and walks outward — that single rule explains why the same word `price` prints 4 inside the function and 10 outside it.

### Visualization (canvas `c2`, 720×300)

Two-panel diagram: the five numbered code lines on the left, the two symbol tables drawn as boxes on the right, with colored arrows from each `print` line to the table entry its lookup lands on.

- **Title (bold 15px, `#1a5276`, top center):** "One Name, Two Tables: price = 10 Outside, price = 3 Inside".
- **Code panel (left):** monospace 13px `#2c3e50` lines left-aligned at x=50, one per row at y = 80, 110, 140, 170, 200: "1  price = 10", "2  def sale():", "3      price = 3;  tax = 1", "4      print(price + tax)   # 4", "5  print(price)              # 10"; lines 3–4 indented and tinted blue `#2a78d6` to mark the function body.
- **Outer table box:** rounded rectangle x=430, y=70, width 230, height 60, 2px `#6b7280` border, fill `rgba(107,116,128,0.06)`; bold 12px `#6b7280` header "outer (global) table"; monospace 13px `#2c3e50` entry "price → 10" inside.
- **Function table box:** rounded rectangle x=430, y=165, width 230, height 80, 2px `#008300` border, fill `rgba(0,131,0,0.08)`; bold 12px green `#008300` header "sale() table"; monospace 13px `#2c3e50` entries "price → 3" and "tax → 1" on two rows.
- **Arrows:** 3px green `#008300` arrow from the end of code line 4 to the "price → 3" entry, bold 12px green label "prints 4" at its midpoint; 3px `#2a78d6` blue arrow from the end of code line 5 to the "price → 10" entry, bold 12px blue label "prints 10" at its midpoint.
- **Annotation (bold 12px orange `#d95926`, near x=430, y=270):** "inner table checked first — outer price is hidden, not changed".
- **Caption (12px `#444`, bottom right):** "toy program — values chosen for hand-tracing".

## Where the Compiler Keeps Its Address Book

**Tags:** `where it's used` (blue), `compiler pass` (green), `common mistake` (red)

- **Every language** — Python, JavaScript, SQL aliases, spreadsheet named ranges all resolve names this way
- **The compiler** — while reading your code it builds one symbol table per scope, before anything runs
- **Instant answers** — "undefined variable" and "duplicate declaration" errors are symbol-table lookups
- **The classic bug** — a function writes `total = 0` locally, thinking it is resetting the outer `total`
- **The damage** — the function adds 25 into its own private `total`; the outer one the report reads stays 0
- **Autocomplete** — your editor's suggestion list is literally the symbol tables of the scopes around your cursor

*Example (italic):* A cart function summed five 5-dollar items into a local `total` of 25, but the receipt printed the untouched outer `total` — 0 — because assignment created a new inner name.

**Common mistake:** Assuming an inner assignment updates the outer variable of the same name — in languages like Python it silently creates a fresh local entry instead, and the outer value never changes.

### Visualization (canvas `c3`, 720×300)

Two-bar comparison chart: the total accumulated inside the function versus the total the outer code actually reads, with the two symbol-table entries written beside the bars to show why they differ.

- **Title (bold 15px, `#1a5276`, top center):** "The Shadowing Bug: 25 Dollars Counted, 0 Dollars Reported".
- **Axes:** origin x=90, baseline y=245, plot width 560, plot height 175; y axis = dollars 0 to 30 with 12px `#444` tick labels "0", "10", "20", "30" and light `#e5e9ef` gridlines at 10 and 20.
- **Bar 1:** green `#008300` fill `rgba(0,131,0,0.35)` with 2px green border, centered at x=240, width 130, height for value 25; bold 13px green value label "25" above; 12px `#444` two-line label below the baseline: "inner total" / "sale() table: total → 25".
- **Bar 2:** red `#e74c3c` fill `rgba(231,76,60,0.20)` with 2px red border, centered at x=500, width 130, height for value 0 drawn as a 3px-tall sliver at the baseline; bold 13px red value label "0" above; 12px `#444` two-line label below: "total the receipt reads" / "outer table: total → 0".
- **Bridge arrow:** dashed 2px `#6b7280` (dash 4/3) arrow from the top of bar 1 toward bar 2, crossed by a short 2px red tick; 11px `#6b7280` label "never copied out" above it.
- **Annotation (bold 12px red `#e74c3c`, near x=380, y=95):** "two entries named total — the function fed the wrong one".
- **Caption (12px `#444`, bottom right):** "illustrative — five 5-dollar items, invented cart".

## Hidden Is Not Deleted

**Tags:** `common mistake` (red), `shadowing vs overwriting` (orange)

- **The worry** — people fear the inner `price = 3` destroyed the outer `price = 10`; it did not
- **Two entries** — shadowing adds a second entry in an inner table; overwriting changes the one entry
- **During the call** — code inside `sale()` sees 3 because lookup stops at the inner table first
- **After the call** — the inner table is thrown away and lookup reaches the outer entry: 10 again
- **The tell** — if the value "comes back" when the function ends, it was shadowed, never overwritten

*Example (italic):* Print `price` before, inside, and after the call and you get 10, then 3, then 10 — the outer value survives untouched behind the temporary inner one.

**Common mistake:** Reading a shadowed name as "the variable changed and changed back" — nothing changed; a second, nearer entry existed for a while and then was discarded.

### Visualization (canvas `c4`, 720×300)

Step chart over three moments in time: the value of `price` that the running code can see, stepping 10 → 3 → 10, with the outer value drawn as a continuous quiet line underneath to show it never moved.

- **Title (bold 15px, `#1a5276`, top center):** "What Code Sees: 10 → 3 → 10 (the Outer 10 Never Moved)".
- **Axes:** origin x=70, baseline y=245, plot width 580, plot height 175; y axis = value 0 to 12 with 12px `#444` tick labels "0", "3", "10" and light `#e5e9ef` gridlines at 3 and 10; x axis three phase labels (12px `#444`, centered under their thirds): "before call", "inside sale()", "after call".
- **Outer value line:** solid 3px `#2a78d6` blue horizontal line at value 10 across the full plot width; 12px blue label "outer price = 10 the whole time" above its right end.
- **Visible-value step line:** solid 4px green `#008300` step path — value 10 over the first third (x=70 to 263), dropping to value 3 over the middle third (x=263 to 457), returning to 10 over the last third (x=457 to 650); vertical connectors dashed 2px green (dash 4/3).
- **Shadow band:** middle third shaded `rgba(217,89,38,0.10)` from top of plot to baseline; 11px orange `#d95926` label "inner table alive" at the band's top.
- **Point labels:** bold 13px green "10", "3", "10" just above the step line at the middle of each third.
- **Annotation (bold 12px orange `#d95926`, near x=280, y=200):** two lines: "shadowed, not overwritten —" / "the inner entry is simply discarded".
- **Caption (12px `#444`, bottom right):** "illustrative timeline of one call to sale()".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`. Red reserved for the genuine bug/error states in c3 and the crossed-out paths.
- **Data:** all values are the hardcoded literals above (no randomness) — the toy program's 10 / 3 / 1 / 4, the cart's 25 vs 0, and the 10 → 3 → 10 timeline; code lines in c2 render as monospace text drawn on the canvas. Invented numbers carry an "illustrative" caption.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
