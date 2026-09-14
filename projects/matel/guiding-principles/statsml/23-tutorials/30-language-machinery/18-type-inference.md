# Type Inference

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Type Inference

**Subtitle:** Write `price = 4.50` and the computer works out for itself that price is a number — all the safety of declared types with almost none of the paperwork

## The Till Script That Never Says "Number"

**Tags:** `core idea` (blue), `types deduced` (green), `no ceremony` (orange)

- **The till script** — a coffee shop's register runs a tiny script that prices each order
- **No labels** — the script says `price = 4.50` and never once writes the word "number"
- **The deduction** — the computer sees 4.50 and concludes on its own: price must be a number
- **Text too** — `name = "latte"` gets deduced as text the same way, straight from the quotes
- **Still strict** — the types are real and fixed; they were deduced, not skipped

*Example (italic):* The line `total = price * 3` earns the type "number" automatically — anything you multiply by 3 must have been a number.

**Key point:** Type inference means the computer works out each variable's type from the value you gave it — declared types without the declaring.

### Visualization (canvas `c1`, 720×300)

Single-panel diagram: three code lines in boxes on the left, arrows to the type badges the compiler deduced on the right — no type word appears in any code box.

- **Title (bold 15px, `#1a5276`, top center):** "Three Lines, Zero Type Words — Three Types Deduced".
- **Code boxes:** three 250×44 rounded rects (fill `#f8f9fa`, 1.5px `#6b7280` border), left edge x=70, tops at y = `[70, 135, 200]`; bold 13px monospace `#2c3e50` text centered: `price = 4.50`, `name = "latte"`, `total = price * 3`.
- **Arrows:** 2.5px `#6b7280` arrows with arrowheads from each box's right edge (x=320) to x=430 at each row's vertical center; 11px `#6b7280` label above each arrow midpoint: "from 4.50", "from the quotes", "from the multiply".
- **Type badges:** three 180×44 rounded rects at x=430, same rows; rows 1 and 3 fill `rgba(0,131,0,0.12)`, 2px green `#008300` border, bold 13px green centered text "number"; row 2 fill `rgba(42,120,214,0.12)`, 2px blue `#2a78d6` border, bold 13px blue text "text".
- **Annotation (bold 12px orange `#d95926`, near x=400, y=272):** two lines: "the badges came from the values —" / "nobody wrote 'number' or 'text'".
- **Caption (12px `#444`, bottom right):** "illustrative — a till script's first three lines".

## Tracing the Deduction by Hand

**Tags:** `worked example` (blue), `step by step` (green)

- **Start at the values** — 4.50 and 3 are numbers on sight; "latte" and " x3" are text on sight
- **Step up** — `total = price * qty` multiplies two numbers, so total is a number: 13.50
- **Other branch** — `label = name + " x3"` glues two texts, so label is text: "latte x3"
- **Top of the tree** — the receipt line combines label and total, and every piece checks out
- **Zero annotations** — five variables got five correct types and the script names none of them

*Example (italic):* Trace it yourself: 4.50 × 3 gives total = 13.50 (number); "latte" + " x3" gives label = "latte x3" (text) — the receipt prints "latte x3 — 13.50".

**Key point:** Inference is bookkeeping you could do by hand — types start at the literal values and climb through each expression to the top.

### Visualization (canvas `c2`, 720×300)

Bottom-up deduction tree: four literal leaves at the bottom, two deduced variables in the middle, the finished receipt line at the top, arrows carrying the types upward.

- **Title (bold 15px, `#1a5276`, top center):** "Types Climb the Tree: From 4.50 and \"latte\" to the Receipt Line".
- **Leaf boxes (tops at y=218):** four 140×44 rounded rects (fill `#f8f9fa`) at left edges x = `[55, 210, 390, 545]`; bold 12px centered two-line text: `4.50` / "number" (2px green `#008300` border), `3` / "number" (green border), `"latte"` / "text" (2px blue `#2a78d6` border), `" x3"` / "text" (blue border).
- **Mid boxes (tops at y=132):** left 210×44 at x=95, green border, bold 12px green text: `total = 13.50` / "number"; right 210×44 at x=435, blue border, bold 12px blue text: `label = "latte x3"` / "text".
- **Top box (top at y=48):** 280×44 centered at x=220, 2px ink `#1a5276` border, fill `rgba(26,82,118,0.08)`, bold 13px `#1a5276` text: `receipt: "latte x3 — 13.50"`.
- **Arrows:** 2px `#6b7280` arrows with arrowheads from each leaf's top center to its mid box's bottom edge (4.50 and 3 feed the left box, "latte" and " x3" the right), and from both mid boxes up to the top box.
- **Annotation (bold 12px violet `#4a3aa7`, near x=30, y=85):** two lines: "every type deduced —" / "zero annotations written".
- **Caption (12px `#444`, bottom right):** "illustrative — one receipt line traced by hand".

## Safety Without the Paperwork

**Tags:** `where it's used` (blue), `bugs caught early` (green), `less typing` (orange)

- **The bug** — one day someone writes `total = total + name`, adding text to money
- **Caught cold** — inferred types are still checked; the mistake is flagged before the script runs
- **Dynamic contrast** — with no checking, the same line waits quietly and crashes on a live order
- **Less to write** — a 20-variable annotated script needs 20 type labels; inference needs about 2
- **Where you meet it** — Rust, Kotlin, Swift, TypeScript and C++ `auto` all lean on inference daily

*Example (italic):* Same 20-variable till script three ways: fully annotated writes 20 labels, inferred writes 2 (only at function boundaries), dynamic writes 0 but catches nothing early.

**Key point:** Inference keeps the compile-time safety net of static typing while cutting the annotations you write from 20 to about 2.

### Visualization (canvas `c3`, 720×300)

Bar chart of type labels written under three styles for the same 20-variable script, with a caught-before-running verdict row under each bar.

- **Title (bold 15px, `#1a5276`, top center):** "Type Labels You Write — and Whether the Bug Is Caught Early".
- **Axes:** origin x=70, baseline y=210, plot width 560, plot height 140; y = labels written 0 to 20, light `#e5e9ef` gridlines at 5, 10, 15, 20 with 12px `#6b7280` tick labels.
- **Bars (110px wide, centered at x = `[190, 370, 550]`), values `[20, 2, 0]`:** "fully annotated" fill `rgba(42,120,214,0.35)` with 2px blue `#2a78d6` border; "inferred" fill `rgba(0,131,0,0.35)` with 2px green `#008300` border; "dynamic" drawn as a 2px flat `#6b7280` dash on the baseline. Bold 13px value labels above each: "20 labels" (blue), "2 labels" (green), "0 labels" (`#6b7280`).
- **Category labels:** 12px `#444` centered under the baseline at y=230: "fully annotated", "inferred", "dynamic".
- **Verdict row (y=258):** 12px `#444` label "bug caught before running?" at x=20; under each bar bold 13px verdicts: green `#008300` "yes" (annotated), green "yes" (inferred), red `#e74c3c` "no" (dynamic).
- **Annotation (bold 12px green `#008300`, near x=300, y=85):** "inferred: annotated safety at a tenth of the typing".
- **Caption (12px `#444`, bottom right):** "illustrative counts — one 20-variable script".

## Inference Is Not Dynamic Typing

**Tags:** `common mistake` (red), `fixed at compile time` (orange)

- **Looks alike** — `price = 4.50` reads the same in Python and in Rust; the machinery differs
- **Inferred = fixed** — once price is deduced as a number, assigning it "latte" later is an error
- **Dynamic = whatever** — a dynamic language lets price hold text at 3pm and a number at 4pm
- **When bugs surface** — inferred: seconds after you hit save; dynamic: Friday rush, customer #212
- **The tell** — inference does its work before running; dynamic typing decides while running

*Example (italic):* The buggy `total = total + name` is rejected at compile time under inference, but under dynamic typing it crashes mid-order at customer #212.

**Common mistake:** Assuming "no type annotations" means "no static types". Inferred types are just as fixed as declared ones — only the writing is skipped.

### Visualization (canvas `c4`, 720×300)

Two-row timeline on a shared time axis: the identical buggy line under inference (caught at the compile check) and under dynamic typing (riding along until it crashes on a live customer).

- **Title (bold 15px, `#1a5276`, top center):** "The Same Bug, Two Fates: total = total + name".
- **Time axis:** horizontal 2px `#999` line at y=245 from x=170 to x=680; 12px `#444` tick labels below at x=200 "you hit save", x=380 "compile check", x=600 "Friday rush, customer #212"; small 2px tick marks at each.
- **Guide line:** vertical dashed `#6b7280` (dash 4/3) line at x=380 from y=68 to the axis; 11px `#6b7280` label "before the script ever runs" at its top.
- **Row 1 (y=115), 12px `#444` label at x=20:** "inferred (static)"; 3px green `#008300` line from x=200 to x=380 ending in a 9px green dot at x=380; bold 13px green label above the dot: "error flagged — fixed in seconds"; nothing drawn past x=380 (the bug never ships).
- **Row 2 (y=185), label at x=20:** "dynamic"; 3px dashed (dash 6/4) `#6b7280` line from x=200 to x=600 with 11px `#6b7280` label "bug rides along quietly" at its midpoint (y=170); 9px red `#e74c3c` dot at x=600 with bold 13px red label above: "crash mid-order".
- **Annotation (bold 13px magenta `#d55181`, centered near y=285):** "same line of code — caught before running vs on a live customer".
- **Caption (12px `#444`, bottom right):** "illustrative timeline".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all box texts, bar values, and coordinates are the hardcoded literals above (no randomness); code snippets in charts render in a monospace font; the worked example's numbers (4.50, 3, 13.50, "latte x3", 20/2/0 labels, customer #212) must match between text and charts.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
