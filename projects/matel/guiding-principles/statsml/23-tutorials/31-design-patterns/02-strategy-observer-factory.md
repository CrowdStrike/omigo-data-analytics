# Strategy, Observer, Factory

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Strategy, Observer, Factory

**Subtitle:** Three plug points that keep a coffee shop's checkout simple — a factory builds the drink, a strategy prices it, observers hear it's ready — three classic patterns you actually meet

## One Coffee Order, Three Moving Parts

**Tags:** `core idea` (blue), `plug points` (green), `GoF` (orange)

- **The order** — order #42 comes in at the counter: one latte, one muffin, one tea
- **Factory** — the barista station takes the word "latte" and builds the right drink, steps included
- **Strategy** — the till holds one pricing rule in a slot; today the slot holds the happy-hour rule
- **Observer** — when #42 is ready, the pickup screen, the customer's phone, and the printer all hear it
- **The trick** — the checkout never says "if latte... if happy-hour..."; it hands work to the plugged-in part

*Example (italic):* Order #42 flows through all three: the factory builds a Latte from the word "latte", the happy-hour rule in the slot prices it, and three listeners hear "ready".

**Key point:** Each pattern is a plug point — a factory picks *what to build*, a strategy picks *how to compute*, an observer picks *who to tell* — while the surrounding code stays unchanged.

### Visualization (canvas `c1`, 720×300)

Three-row flow diagram: order #42 passing through the factory, the strategy slot, and the observer fan-out, one pattern per row.

- **Title (bold 15px, `#1a5276`, top center):** "Order #42 Walks Through Three Plug Points".
- **Rows at y = 90, 165, 240; each row has a bold 12px left label at x=20:** "FACTORY" (blue `#2a78d6`), "STRATEGY" (violet `#4a3aa7`), "OBSERVER" (aqua `#199e70`).
- **Row 1 (factory):** grey box at x=110 labeled `"latte"` (12px), 3px `#6b7280` arrow to a blue `#2a78d6` box at x=280 labeled "DrinkFactory.make()", arrow to a green `#008300` box at x=500 labeled "Latte object".
- **Row 2 (strategy):** grey box at x=110 labeled "subtotal $10.00", arrow to a violet `#4a3aa7` box at x=280 labeled "pricing rule slot" with three 11px `#6b7280` pill labels stacked above it reading "regular / happy-hour / loyalty", arrow to a green box at x=500 labeled "total $8.60".
- **Row 3 (observer):** blue box at x=110 labeled "#42 ready", three 3px arrows fanning right to three aqua `#199e70` boxes at x=340 (y offsets −28, 0, +28) labeled "customer text", "pickup screen", "receipt printer".
- **Box style:** 130–170px wide, 34px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(74,58,167,0.12)` / `rgba(25,158,112,0.12)` / `rgba(107,114,128,0.10)`, 12px `#2c3e50` text.
- **Annotation (bold 13px ink `#1a5276`, bottom center near y=285):** "the checkout code never changes — only what's plugged in does".
- **Caption (12px `#444`, bottom right):** "order flow illustrative".

## Pricing the Same Order Three Ways

**Tags:** `worked example` (blue), `swap the rule` (green)

- **The order** — latte $5.00 + muffin $3.00 + tea $2.00 gives a subtotal of $10.00
- **Regular rule** — charges the subtotal as-is: total $10.00
- **Happy-hour rule** — 20% off drinks only: latte $4.00 + tea $1.60 + muffin $3.00 = $8.60
- **Loyalty rule** — a flat $2.00 off the whole subtotal: $10.00 − $2.00 = $8.00
- **The swap** — the till runs `rule.price(order)` all three times; only the rule object in the slot differs

*Example (italic):* Switching from regular to happy-hour at 3pm is one line — put a different rule object in the slot — and the same order rings up $8.60 instead of $10.00.

**Key point:** Three totals from one checkout — $10.00, $8.60, $8.00 — come from swapping the strategy object, never from editing the checkout code itself.

### Visualization (canvas `c2`, 720×300)

Vertical bar chart: the same $10.00 order priced by each of the three interchangeable rules.

- **Title (bold 15px, `#1a5276`, top center):** "Same Order, Three Pricing Strategies".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; y = total in dollars 0 to 10, gridlines `#e5e9ef` at 2.50 / 5.00 / 7.50 with 12px `#444` tick labels.
- **Bars (90px wide, centered at x = 180, 360, 540):** values `[10.00, 8.60, 8.00]`; fills blue `#2a78d6`, aqua `#199e70`, green `#008300` at 0.85 alpha with solid 2px matching borders.
- **Bar labels:** bold 13px `#2c3e50` value ("$10.00", "$8.60", "$8.00") above each bar top; 12px `#444` rule name ("regular", "happy-hour 20% off drinks", "loyalty $2 off") below the baseline under each bar.
- **Annotation (bold 13px violet `#4a3aa7`, near x=470, y=60):** "same checkout code — only the rule object changed".
- **Caption (12px `#444`, bottom right):** "menu prices illustrative; totals hand-checkable".

## The Same Shapes in an ML Training Script

**Tags:** `where it's used` (blue), `ML training` (green), `callbacks` (orange)

- **Factory in ML** — `make_model("xgboost")` turns a config string into a ready estimator object
- **Strategy in ML** — passing `optimizer="adam"` swaps the update rule without touching the training loop
- **Observer in ML** — callbacks (logger, early-stop, checkpoint) subscribe to the epoch-end event
- **Why it matters** — adding a fourth pricing rule or a new optimizer means one new file, zero edited ones
- **The payoff** — code grows by addition, not by editing a giant if/else block that everyone fears

*Example (italic):* A training script with a model factory, a pluggable optimizer, and three epoch-end callbacks uses all three patterns before lunch.

**Key point:** You already meet these patterns daily under other names — estimator configs, pluggable losses, training callbacks — knowing the three shapes lets you name and reuse them.

### Visualization (canvas `c3`, 720×300)

Three-row mapping diagram: each pattern shown as its coffee-shop instance on the left and its ML-training instance on the right.

- **Title (bold 15px, `#1a5276`, top center):** "The Same Three Shapes in a Training Script".
- **Rows at y = 95, 165, 235; each row starts with a colored pill label at x=25:** "FACTORY" (bold 12px blue `#2a78d6`), "STRATEGY" (bold 12px violet `#4a3aa7`), "OBSERVER" (bold 12px aqua `#199e70`).
- **Left boxes (coffee shop) at x=130, 200px wide:** `DrinkFactory.make("latte")`, "pricing rule slot at the till", "order-ready fan-out to 3 listeners" (12px `#2c3e50` text).
- **Right boxes (ML) at x=420, 260px wide:** `make_model("xgboost") builds the estimator`, `optimizer="sgd" / "adam" passed into the loop`, "epoch-end → logger, early-stop, checkpoint".
- **Arrows:** 3px `#6b7280` horizontal arrow from each left box to its right box, 11px `#6b7280` label "same shape" above the middle arrow.
- **Box style:** 34px tall, 8px radius, left fills `rgba(42,120,214,0.12)`, right fills `rgba(25,158,112,0.12)`, 1.5px borders in the row's pill color.
- **Annotation (bold 13px magenta `#d55181`, bottom center near y=280):** "you already use all three — the names came later".
- **Caption (12px `#444`, bottom right):** "library call names generic, illustrative".

## A Pattern With One Variant Is Just Ceremony

**Tags:** `common mistake` (red), `overuse` (orange)

- **The reflex** — reaching for a pattern before there is a second variant that actually needs swapping
- **One strategy** — a strategy interface with a single implementation is a function with extra steps
- **Factory for one** — a factory that only ever builds Latte hides `new Latte()` behind ceremony
- **Observer sprawl** — when everything notifies everything, nobody can trace what one order triggers
- **The test** — add the pattern when the second real variant shows up, not when you imagine it might

*Example (italic):* A shop selling one drink at one price wraps it in `AbstractDrinkFactoryProvider` anyway — now finding the price means opening six files instead of one.

**Common mistake:** Patterns are answers to a *repeated* problem. With one drink, one price rule, and one listener, the pattern adds indirection and removes nothing.

### Visualization (canvas `c4`, 720×300)

Horizontal bar chart: how many files you open to change one price, at three levels of abstraction.

- **Title (bold 15px, `#1a5276`, top center):** "Files You Open to Change One Price".
- **Axis:** vertical 2px `#999` baseline at x=250, bars extend right, max width 400; 20px of pixel width per file (schematic).
- **Rows (bars 16px tall, at y = 85, 150, 215), each with a left-aligned 12px `#444` label at x=20:**
  - "price hardcoded in checkout": green `#008300` bar width 20 (1 file), 11px `#444` label "1 file" at bar end
  - "three strategies, one slot": blue `#2a78d6` bar width 40 (2 files), label "2 files — worth it: 3 rules swap freely"
  - "factory-of-factories, one drink": red `#e74c3c` bar width 120 (6 files), bold 12px red label "6 files, zero variants"
- **Annotation (bold 13px orange `#d95926`, near x=300, y=260):** "patterns pay off at three variants, not at one".
- **Caption (12px `#444`, bottom right):** "file counts illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); the worked-example order is latte $5.00 + muffin $3.00 + tea $2.00 = $10.00 subtotal, priced `[10.00, 8.60, 8.00]` by the regular / happy-hour (20% off drinks) / loyalty ($2 off) rules — totals hand-checkable and must match the c2 bars; file counts in c4 (`[1, 2, 6]`) and all box/arrow flows are invented and labeled illustrative.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
