# Closures & Lexical Scope

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Closures & Lexical Scope

**Subtitle:** A closure is a function that carries a small backpack of the variables from the place it was born — so it still finds them long after that place has finished running

## A Ticket Dispenser That Remembers

**Tags:** `core idea` (blue), `birthplace` (green), `hidden state` (orange)

- **The deli** — a deli owner writes one recipe, `makeDispenser()`, that builds "take-a-number" machines
- **Inside the recipe** — the recipe starts a private `count = 0`, then hands back a `next()` function
- **The backpack** — `next()` was born next to `count`, so it keeps that variable in a backpack forever
- **Lexical scope** — the function looks things up where it was WRITTEN, not where it is later called
- **Two machines** — calling `makeDispenser()` twice builds two machines with two separate counts

*Example (italic):* The recipe finished running long ago, yet every pull of `next()` still finds its own `count` — the function carried its birthplace with it.

**Key point:** A closure = a function plus the variables that surrounded its birth; each run of the maker packs a fresh, private backpack.

### Visualization (canvas `c1`, 720×300)

Factory diagram: one recipe box on the left producing two dispenser functions on the right, each drawn with its own small "backpack" holding a separate `count` variable.

- **Title (bold 15px, `#1a5276`, top center):** "One Recipe, Two Machines — Each Carries Its Own count".
- **Factory box:** rounded rect x=40, y=80, w=200, h=140, 2px `#1a5276` border, fill `rgba(26,82,118,0.06)`; bold 13px ink label "makeDispenser()" at top inside; 12px `#2c3e50` code lines inside: "count = 0" and "return next()".
- **Arrows:** two 3px `#6b7280` arrows with arrowheads from the box's right edge (x=240, y=130 and y=170) to the two machine boxes; 11px `#6b7280` labels "call #1" and "call #2" above each arrow.
- **Machine A:** rounded rect x=430, y=55, w=170, h=80, 2px blue `#2a78d6` border; bold 13px blue label "dispenserA — a next()"; attached backpack: small rounded rect x=610, y=70, w=90, h=50, fill `rgba(42,120,214,0.15)`, 12px blue text "count: 0".
- **Machine B:** rounded rect x=430, y=165, w=170, h=80, 2px green `#008300` border; bold 13px green label "dispenserB — a next()"; attached backpack: small rounded rect x=610, y=180, w=90, h=50, fill `rgba(0,131,0,0.15)`, 12px green text "count: 0".
- **Annotation (bold 12px orange `#d95926`, two lines near x=270, y=262):** "two separate backpacks —" / "each factory call packs a fresh one".
- **Caption (11px `#444`, bottom right):** "illustrative — state just after both machines are built".

## Two Dispensers, Five Pulls

**Tags:** `worked example` (blue), `step by step` (green)

- **Setup** — build two machines: `dispenserA = makeDispenser()` and `dispenserB = makeDispenser()`
- **Each pull** — `next()` adds 1 to its own backpack's `count` and returns the new value
- **The sequence** — pull A, A, B, A, B and read the tickets: 1, 2, 1, 3, 2
- **No crosstalk** — B's first pull says 1 even though A already said 2; the counts never mix
- **Check by hand** — A was pulled 3 times so it sits at 3; B was pulled twice so it sits at 2

*Example (italic):* Pull order A, A, B, A, B prints tickets 1, 2, 1, 3, 2 — B restarts at 1 because its backpack holds a different `count`.

**Key point:** After five pulls A shows 3 and B shows 2 — same recipe, same code, separate backpacks.

### Visualization (canvas `c2`, 720×300)

Bar chart of the five pulls in order: x axis is the pull sequence, bar height is the ticket number returned, colored by which dispenser was pulled.

- **Title (bold 15px, `#1a5276`, top center):** "Five Pulls: A, A, B, A, B → Tickets 1, 2, 1, 3, 2".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 190; y = ticket number 0 to 3 with light `#e5e9ef` gridlines and 12px `#444` labels at 1, 2, 3; x = five slots with 12px `#444` labels "pull 1 (A)", "pull 2 (A)", "pull 3 (B)", "pull 4 (A)", "pull 5 (B)".
- **Bars:** widths 70px, centered in five equal slots; heights from hardcoded array `[1, 2, 1, 3, 2]`; fills — A pulls (slots 1, 2, 4) `rgba(42,120,214,0.35)` with 2px blue `#2a78d6` border, B pulls (slots 3, 5) `rgba(0,131,0,0.30)` with 2px green `#008300` border.
- **Value labels:** bold 13px above each bar, blue "1", "2", "3" over A's bars, green "1", "2" over B's bars.
- **Legend (12px, top right inside plot):** blue swatch "dispenser A", green swatch "dispenser B".
- **Annotation (bold 12px green `#008300`, near x=330, y=85):** "B restarts at 1 — its count is a different variable".

## Functions That Outlive Their Makers

**Tags:** `where it's used` (blue), `callbacks` (green), `function factories` (orange)

- **Alert factory** — `makeAlert(0.8)` returns `check(score)` that fires whenever a score beats 0.8
- **Maker dies, value lives** — `makeAlert` returns at step 1, yet the 0.8 stays alive inside `check`
- **Fires later** — at step 9 a score of 0.93 arrives; `check` still compares it against 0.8 and fires
- **Everyday uses** — callbacks, event handlers, one scorer per metric, Python decorators, partials
- **The alternative** — without closures the 0.8 would need a global variable that anything could edit

*Example (italic):* You build alerts for three metrics with thresholds 0.8, 0.5, and 0.9 from one factory — each handler quietly carries its own number.

**Key point:** Closures let you configure a function now and run it much later — the settings ride along instead of living in globals.

### Visualization (canvas `c3`, 720×300)

Timeline chart: a horizontal time axis with three marked moments — the handler is born with 0.8, its maker returns, and much later an event fires and the handler still reads 0.8.

- **Title (bold 15px, `#1a5276`, top center):** "Born at Step 0, Maker Gone at Step 1, Still Working at Step 9".
- **Time axis:** horizontal 2px `#999` line at y=200 from x=60 to x=680, marked "step 0" to "step 9" with small ticks every 62px and 12px `#444` labels at steps 0, 1, 9.
- **Event 1 (step 0, x=60):** blue `#2a78d6` 8px dot on the axis; box above (x=40, y=75, w=150, h=50, 2px blue border) with 12px text "check(score) born" / "backpack: 0.8"; thin blue connector line from box to dot.
- **Event 2 (step 1, x=122):** `#6b7280` 8px dot; 12px `#6b7280` label above at y=155: "makeAlert returns"; short dashed `#6b7280` (dash 4/3) vertical connector.
- **Faded zone:** light `rgba(107,114,128,0.10)` band from x=122 to x=618 between y=180 and y=220, 11px `#6b7280` label centered in it: "maker's scope is gone";
- **Event 3 (step 9, x=618):** orange `#d95926` 8px dot; box above (x=520, y=75, w=170, h=50, 2px orange border) with 12px text "score 0.93 arrives" / "0.93 > 0.8 → alert!"; thin orange connector.
- **Backpack arrow:** 2px dashed blue (dash 4/3) curved arrow from the step-0 box to the step-9 box, bold 12px blue label at its middle (near x=350, y=68): "the 0.8 rides along in the closure".
- **Annotation (bold 12px orange `#d95926`, near x=430, y=252):** "no globals — the setting travels with the function".
- **Caption (11px `#444`, bottom right):** "illustrative timeline".

## It Remembers the Variable, Not the Number

**Tags:** `common mistake` (red), `loop capture` (orange)

- **The trap** — a loop over i = 0, 1, 2 builds three printers, each meant to print its own i
- **Run them later** — after the loop ends all three print 3, because the loop left i at 3
- **Why** — a closure packs the VARIABLE i itself, not a snapshot of the number it held at birth
- **One shared backpack** — all three printers point at the same single i, so they agree on 3
- **The fix** — give each loop turn its own fresh variable (e.g. `let` per iteration, or a default arg)

*Example (italic):* Three buttons wired in a loop all pop up "item 3" when clicked — the classic sign of three closures sharing one loop variable.

**Common mistake:** Expecting a closure to freeze a value. It captures the living variable; if the variable changes after birth, every closure sharing it sees the change.

### Visualization (canvas `c4`, 720×300)

Grouped bar chart: three printer functions on the x axis, each with an "expected" bar and a "what it prints" bar, showing 0/1/2 expected but 3/3/3 printed under shared capture.

- **Title (bold 15px, `#1a5276`, top center):** "Three Printers from One Loop — All Say 3".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 190; y = printed value 0 to 3, light `#e5e9ef` gridlines and 12px `#444` labels at 1, 2, 3; x = three groups with 12px `#444` labels "printer built at i=0", "printer built at i=1", "printer built at i=2".
- **Bars per group (two, 55px wide, 12px gap):** expected values from hardcoded array `[0, 1, 2]`, fill `rgba(0,131,0,0.30)` with 2px green `#008300` border; printed values from hardcoded array `[3, 3, 3]`, fill `rgba(231,76,60,0.25)` with 2px red `#e74c3c` border; the expected-0 bar drawn as a green 2px baseline tick with its label just above the axis.
- **Value labels:** bold 12px above each bar — green "0", "1", "2" and red "3", "3", "3".
- **Legend (12px, top left inside plot):** green swatch "expected (i at birth)", red swatch "printed (shared i now)".
- **Annotation (bold 12px red `#e74c3c`, two lines near x=400, y=80):** "all three share ONE i —" / "by run time the loop left it at 3".
- **Caption (11px `#444`, bottom right):** "per-iteration variables restore 0, 1, 2".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`. Red only for the genuine bug in c4.
- **Data:** all bar heights, ticket sequences, and timeline positions are the hardcoded literal arrays above (no `Math.random()`); the c2 ticket numbers `[1, 2, 1, 3, 2]` and the c4 arrays `[0, 1, 2]` vs `[3, 3, 3]` must match the text bullets exactly.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
