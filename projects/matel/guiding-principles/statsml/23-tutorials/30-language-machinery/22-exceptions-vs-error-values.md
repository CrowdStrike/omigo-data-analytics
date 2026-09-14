# Exceptions vs Error Values

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Exceptions vs Error Values

**Subtitle:** When something goes wrong deep inside a program, the bad news has to travel back to whoever asked — exceptions shout it up the chain, error values pass it hand to hand, and result types seal it in a box you must open

## The Empty Oat-Milk Carton

**Tags:** `core idea` (blue), `failure travels` (green), `three styles` (orange)

- **The order** — you ask the cashier for an oat-milk latte, the cashier asks the barista, the barista finds the carton empty
- **The question** — the drink can't be made; how does "no oat milk" get back to you, three people away?
- **Exception** — the barista shouts across the shop straight to you, skipping the cashier entirely
- **Error value** — the barista hands the cashier a note, the cashier reads it and passes it to you; every link handles it
- **Result type** — the barista returns a sealed box labeled "drink" or "problem"; nobody can serve it without opening it
- **Same news** — all three deliver "no oat milk"; they differ only in who along the chain must notice

*Example (italic):* One empty carton, three shops: in shop A the barista shouts, in shop B a note travels back desk to desk, in shop C a labeled box does.

**Key point:** Exceptions, error values, and result types are three answers to one question: how does a failure travel from where it happens to where someone can deal with it?

### Visualization (canvas `c1`, 720×300)

Three-column flow diagram: the same You → Cashier → Barista chain drawn three times, with the "no oat milk" news traveling back differently in each column.

- **Title (bold 15px, `#1a5276`, top center):** "One Empty Carton, Three Ways the News Travels Back".
- **Layout:** three columns centered at x = 140, 360, 580; each column has three rounded boxes (110×30, fill `#f8f9fa`, 1.5px `#1a5276` border, 12px `#2c3e50` centered labels) at y = 70 ("You"), y = 135 ("Cashier"), y = 200 ("Barista"); a small 12px label "no oat milk!" in red `#e74c3c` just below each Barista box at y=245.
- **Column headers (bold 13px, above each column at y=52):** "exception" in blue `#2a78d6`, "error value" in orange `#d95926`, "result type" in green `#008300`.
- **Column 1 (exception):** one 3px blue `#2a78d6` curved arrow from the Barista box directly to the You box, bowing 55px left of the column and bypassing the Cashier box; 11px blue label "shout — skips the middle" beside the curve.
- **Column 2 (error value):** two 3px orange `#d95926` straight arrows, Barista→Cashier and Cashier→You, each with a small 14×10 note icon (orange outline rectangle) at its midpoint; 11px orange label "note passed hand to hand" right of the arrows.
- **Column 3 (result type):** two 3px green `#008300` straight arrows, Barista→Cashier and Cashier→You, each with a small 16×12 box icon (green outline square with a lid line) at its midpoint labeled 11px green "sealed"; 11px green label "labeled box — must be opened".
- **Annotation (bold 12px `#1a5276`, bottom center near y=285):** "same bad news, three delivery routes".
- **Caption (12px `#444`, bottom right):** "illustrative — a coffee-shop stand-in for a call chain".

## One Bad Order, Three Round Trips

**Tags:** `worked example` (blue), `count the checks` (green)

- **The chain** — 4 steps deep: you → takeOrder → makeDrink → pourMilk; the failure happens at step 4
- **Exception route** — 1 throw at step 4 plus 1 catch at step 1 = 2 lines of handling; steps 2 and 3 are skipped
- **Error-value route** — 1 return at step 4 plus an "is there an error?" check at steps 3, 2, and 1 = 4 lines
- **Result route** — 3 hand-offs, each a labeled box; the compiler won't let you use the drink unopened
- **The trade** — exceptions touch 2 lines but hide the route; error values touch 4 lines but every step is visible

*Example (italic):* Count by hand: 2 handling lines for the exception, 4 for the error value, 4 for the result type — and only the result type makes forgetting one impossible.

**Key point:** In a 4-step chain with one failure, exceptions cost 2 lines and skip 2 steps; error values and result types cost 4 lines and walk back through every step.

### Visualization (canvas `c2`, 720×300)

Three side-by-side ladders of the same 4 call-stack frames, with the failure's return path drawn on each and the touched lines counted.

- **Title (bold 15px, `#1a5276`, top center):** "The Same 4-Step Chain: 2 Lines vs 4 Lines vs 4 Checked Lines".
- **Layout:** three ladders centered at x = 140, 360, 580; each ladder has four stacked boxes (130×32, fill `#f8f9fa`, 1.5px `#1a5276` border, 12px `#2c3e50` labels) at y = 70 ("you"), 110 ("takeOrder"), 150 ("makeDrink"), 190 ("pourMilk ✗"); the ✗ and the bottom box border in red `#e74c3c` on all three ladders.
- **Ladder headers (bold 13px at y=52):** "exception" blue `#2a78d6`, "error value" orange `#d95926`, "result type" green `#008300`.
- **Ladder 1:** 3px blue arrow curving from the "pourMilk ✗" box straight up to the "you" box, bowing 50px left, passing outside the two middle boxes; small bold 11px blue "throw" at its start and "catch" at its end; middle boxes drawn with 40% opacity to show they are skipped.
- **Ladder 2:** three short 3px orange arrows stepping up box to box (190→150, 150→110, 110→70); a bold 11px orange "if err?" tag beside each of the three upper boxes.
- **Ladder 3:** three short 3px green arrows stepping up box to box; a small green box icon on each arrow and a bold 11px green "must open" tag beside each of the three upper boxes.
- **Counts (bold 13px, centered under each ladder at y=252):** "2 lines touched" in blue, "4 lines touched" in orange, "4 lines, compiler-checked" in green.
- **Annotation (bold 12px `#1a5276`, bottom center near y=285):** "exceptions skip 2 frames; the other two visit every frame".
- **Caption (12px `#444`, bottom right):** "illustrative — line counts from the worked example".

## When You Find Out You Forgot

**Tags:** `why it matters` (blue), `where it's used` (green), `silent failure` (red)

- **The real risk** — the danger is not the failure itself but a programmer forgetting to handle it
- **Result type** — the forgotten check is caught on day 0, at compile time, before the program ever runs
- **Exception** — the miss surfaces on day 30, when a real customer hits it and the program crashes loudly
- **Error value** — an ignored return code may never surface; the program keeps going with a wrong answer
- **In the wild** — Python and Java lean on exceptions, C and Go on error values, Rust on result types
- **Loud beats silent** — a crash on day 30 is painful; a quietly wrong report shipped for months is worse

*Example (italic):* Three teams each forgot one check: the Rust build failed on day 0, the Python service crashed on day 30, and the C job ran wrong for months, silently.

**Key point:** The three styles differ most when a check is forgotten — day 0 and loud, day 30 and loud, or never and silent; silent is the expensive one.

### Visualization (canvas `c3`, 720×300)

Horizontal timeline chart: one row per style showing when a single forgotten failure check gets noticed, from compile time to never.

- **Title (bold 15px, `#1a5276`, top center):** "One Forgotten Check: When Does Anyone Notice?".
- **Axis:** horizontal 2px `#999` line at y=245 from x=200 to x=680 (width 480); tick labels 12px `#444` below: "day 0 (compile)" at x=200, "day 10" at x=340, "day 20" at x=480, "day 30 (production)" at x=620; a 12px `#6b7280` label "never" at x=680 with a short dashed lead-out.
- **Rows (y = 95, 150, 205), each with a left-aligned 12px `#444` label at x=20:**
  - "result type — Rust": green `#008300` 9px dot at x=200 (day 0), bold 12px green label above: "won't compile — day 0".
  - "exception — Python": blue `#2a78d6` 9px dot at x=620 (day 30), bold 12px blue label above: "crash in production — day 30"; thin dashed blue line from x=200 to the dot showing the quiet wait.
  - "error value — C": orange `#d95926` open-circle 9px marker at x=680 past the axis end, bold 12px orange label above: "wrong answer, no noise — never"; thin dashed orange line from x=200 to the marker.
- **Guide line:** vertical dashed `#6b7280` (dash 4/3) line at x=200 from y=75 to the axis, 11px `#6b7280` label "before the program runs" at its top.
- **Annotation (bold 13px red `#e74c3c`, near x=430, y=70):** "silent and never is the worst outcome".
- **Caption (12px `#444`, bottom right):** "illustrative timeline — days invented for the example".

## An Empty Carton Is Not a Bug

**Tags:** `common mistake` (red), `expected vs bug` (orange)

- **Two kinds of wrong** — an empty oat-milk carton is expected life; charging a customer twice is a bug
- **Expected failures** — out of stock, a typo in the order, a card that declines: normal outcomes to handle
- **Bugs** — dividing by zero, reading a missing field: mistakes in the code itself, not in the world
- **The mistake** — throwing a loud exception for every empty carton, or hiding a real bug in a return code
- **The habit** — handle expected failures as values close to where they happen; let true bugs fail loudly

*Example (italic):* A shop that shouts "EMERGENCY" for every empty carton trains everyone to ignore shouting — then the actual fire alarm gets ignored too.

**Common mistake:** Using one mechanism for everything. Expected failures deserve quiet, checked hand-offs; genuine bugs deserve a loud, unmissable stop — mixing them up makes both worse.

### Visualization (canvas `c4`, 720×300)

Two-bucket sorter diagram: everyday failure events sorted into an "expected failures" bucket and a "bugs" bucket, each labeled with the handling style that fits.

- **Title (bold 15px, `#1a5276`, top center):** "Sort First, Then Pick the Mechanism".
- **Buckets:** two rounded rectangles 300×160 at (60, 80) and (390, 80); left bucket fill `rgba(0,131,0,0.08)` with 2px green `#008300` border, bold 14px green header inside at top: "expected failures"; right bucket fill `rgba(231,76,60,0.08)` with 2px red `#e74c3c` border, bold 14px red header: "bugs".
- **Left bucket items (12px `#2c3e50`, one per line, small green dot bullets):** "carton is empty (out of stock)", "customer typo in the order", "card declined", "network timed out".
- **Right bucket items (12px `#2c3e50`, small red dot bullets):** "divided by zero", "read a field that isn't there", "charged the customer twice".
- **Handling labels (bold 12px, centered under each bucket at y=262):** left in green: "handle as a value, close by"; right in red: "fail loudly, stop the program".
- **Annotation (bold 12px `#1a5276`, top right near x=560, y=60):** "the sorting decides the mechanism".
- **Caption (12px `#444`, bottom right):** "illustrative — items borrowed from the coffee-shop example".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`. Red only for genuine failure/bug states (the ✗ frame, the "silent" warning, the bugs bucket).
- **Style-color mapping (keep consistent across all four canvases):** exceptions = blue `#2a78d6`, error values = orange `#d95926`, result types = green `#008300`.
- **Data:** all diagrams use the hardcoded coordinates, labels, and counts above (no randomness); the line counts (2 / 4 / 4) and timeline days (0 / 30 / never) in the charts must match the text bullets exactly.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
