# Dependency Injection

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Dependency Injection

**Subtitle:** A piece of code should not build the things it works with — it should receive them, the way a register is handed a price list instead of having prices carved into it

## A Register That Builds Its Own Price List

**Tags:** `core idea` (blue), `receive, don't build` (green), `coffee shop` (orange)

- **The register** — a coffee shop register totals each order using a price list and a printer
- **The hard-wired way** — prices are carved inside the register and it wires up its own printer
- **The pain** — raising the latte price or trying a new printer means opening and rebuilding the register
- **The injected way** — each morning the manager hands the register a price list and plugs in a printer
- **The shift** — the register stops constructing its collaborators and just uses whatever it is given

*Example (italic):* The shop switches receipt printers on Tuesday; the injected register doesn't change at all — only the thing plugged into it does.

**Key point:** Dependency injection means a component receives its collaborators from outside (as constructor or function arguments) instead of creating them internally — so the collaborators can be swapped without touching the component.

### Visualization (canvas `c1`, 720×300)

Two-row flow diagram: a register that constructs its own price list and printer inside (hard to change) vs a register that receives both from outside (easy to swap).

- **Title (bold 15px, `#1a5276`, top center):** "Build It Inside vs Receive It From Outside".
- **Row 1 (y=95), label 12px `#444` at x=20:** "builds its own"; one blue `#2a78d6` rounded box at x=180 labeled "register" (12px) containing two small inner boxes "prices carved in" and "printer welded in" (11px `#6b7280`); bold 12px red `#e74c3c` note at x=470: "✗ any change = rebuild the register".
- **Row 2 (y=205), label:** "receives"; two green `#008300` rounded boxes at x=150 labeled "price list" and x=150/y+50 offset labeled "printer", each with a 3px arrow pointing right into a blue rounded box at x=380 labeled "register (uses what it's given)"; bold 12px green note at x=560: "✓ swap either one freely".
- **Box style:** 140–200px wide, 36–64px tall, 8px radius, fills `rgba(42,120,214,0.15)` for register boxes and `rgba(0,131,0,0.12)` for injected pieces, 12px `#2c3e50` text.
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=270):** "the register's code is identical in row 2 — only its inputs differ".
- **Caption (12px `#444`, bottom right):** "schematic — boxes illustrative".

## Swapping the Price List Without Opening the Register

**Tags:** `worked example` (blue), `same code, new list` (green)

- **The order** — 2 lattes, 1 espresso, 1 muffin, rung up on the exact same register both times
- **Regular list** — latte 4.00, espresso 2.50, muffin 3.00; total = 4.00 + 4.00 + 2.50 + 3.00 = 13.50
- **Happy-hour list** — latte 3.00, espresso 2.00, muffin 3.00; total = 3.00 + 3.00 + 2.00 + 3.00 = 11.00
- **The swap** — at 4pm the manager hands the register the happy-hour list; zero register code changes
- **Hand-check** — the drop is 13.50 − 11.00 = 2.50: one dollar off each latte, fifty cents off the espresso

*Example (italic):* The same four-item order totals 13.50 at noon and 11.00 at 4:05pm, because a different price list was injected — not because the register changed.

**Key point:** The register's totaling logic never changes; injecting a different price list is what changes the answer — behavior is configured by what you pass in.

### Visualization (canvas `c2`, 720×300)

Grouped bar chart: the four order items priced under the regular list vs the injected happy-hour list, with both totals annotated.

- **Title (bold 15px, `#1a5276`, top center):** "Same Register, Same Order — Injected List Changes the Total".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; y = dollars 0 to 5, gridlines `#e5e9ef` at 1/2/3/4 with 12px `#444` labels; x = four groups centered at x = 130, 270, 410, 550 labeled "latte #1", "latte #2", "espresso", "muffin" (12px `#444`).
- **Bars:** each group has two 40px-wide bars side by side — regular list blue `#2a78d6` fill `rgba(42,120,214,0.55)` at heights for `[4.00, 4.00, 2.50, 3.00]`, happy-hour aqua `#199e70` fill `rgba(25,158,112,0.55)` at heights for `[3.00, 3.00, 2.00, 3.00]`; 11px value labels ("4.00" etc.) above each bar.
- **Legend (12px, top left inside plot):** blue swatch "regular list — total 13.50", aqua swatch "happy-hour list — total 11.00".
- **Annotation (bold 13px violet `#4a3aa7`, near x=380, y=75):** "13.50 → 11.00 with zero register changes".
- **Caption (12px `#444`, bottom right):** "menu prices illustrative".

## Testing a Pipeline Without Touching the Warehouse

**Tags:** `where it's used` (blue), `testing` (green), `reproducibility` (orange)

- **The pipeline** — a daily revenue job hard-wired to the production warehouse can only be tested against it
- **The fake** — inject a 10-row in-memory orders table instead, and the test checks a total you can add by hand
- **The speed** — 50 tests at 12s per warehouse query take 600s; the same 50 on the fake take 0.05s each, 2.5s total
- **The seed** — injecting a seeded random generator (instead of the global one) makes a training run reproducible
- **The clock** — injecting a fake clock lets you test "end of month" logic on any day of the year

*Example (italic):* The revenue test hands the job a 10-row fake table, asserts the known total, and the whole 50-test suite finishes in 2.5 seconds with no warehouse credentials.

**Key point:** Injection is what makes code testable: any slow, external, or nondeterministic collaborator — a database, a clock, a random generator — can be replaced by a fast fake at the boundary.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart: total time for a 50-test suite when the orders source is hard-wired to the warehouse vs injected as a 10-row fake table.

- **Title (bold 15px, `#1a5276`, top center):** "50 Tests: Hard-Wired Warehouse vs Injected Fake Table".
- **Axis:** vertical 2px `#999` baseline at x=230, bars extend right, max width 440; widths are pixel-schematic, not to scale.
- **Rows (top to bottom at y = 90, 180), each with a left-aligned 12px `#444` label at x=20:**
  - "hard-wired: 50 × 12s": orange `#d95926` bar width 440, 12px `#444` end label "600s (10 min)"
  - "injected fake: 50 × 0.05s": green `#008300` bar width 24, 12px `#444` end label "2.5s"
- **Bar style:** 34px tall, fills `rgba(217,89,38,0.45)` and `rgba(0,131,0,0.45)` with solid 2px borders in the same hues.
- **Annotation (bold 13px green `#008300`, near x=300, y=225):** "240× faster — and no production credentials in tests".
- **Caption (12px `#444`, bottom right):** "timings illustrative; bar widths schematic, not to scale".

## Injection Is an Argument, Not a Framework

**Tags:** `common mistake` (red), `hidden dependency` (orange)

- **The confusion** — people hear "dependency injection" and picture a heavyweight framework or container
- **The core** — at its heart it is just a parameter: `Register(prices, printer)` instead of `Register()`
- **The trap** — reaching into a global lookup from inside the register hides the dependency again
- **The tell** — if you can't list a component's collaborators from its signature, they aren't injected
- **The framework** — containers only automate the wiring of many injections; the pattern needs none

*Example (italic):* A register that quietly calls a global `get_prices()` inside still depends on the price list — the dependency is just invisible from the outside.

**Common mistake:** Treating a global registry or singleton lookup inside the code as injection. If the collaborator doesn't arrive through the signature, callers can't see it and tests can't swap it.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: a register secretly reaching out to a global registry (hidden dependency) vs a register whose dependencies arrive through its signature (visible and swappable).

- **Title (bold 15px, `#1a5276`, top center):** "Hidden Lookup vs Visible Argument".
- **Row 1 (y=95), label 12px `#444` at x=20:** "global lookup"; blue `#2a78d6` rounded box at x=170 labeled "Register()" with a dashed 2px `#6b7280` (dash 4/3) arrow reaching out to a red `#e74c3c` box at x=430 labeled "global get_prices()", bold 12px red note "✗ invisible from the signature".
- **Row 2 (y=205), label:** "injected"; green `#008300` rounded box at x=150 labeled "price list" with a solid 3px arrow into a blue box at x=380 labeled "Register(prices, printer)", bold 12px green note "✓ callers see it, tests swap it".
- **Box style:** 150–200px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(231,76,60,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px orange `#d95926`, centered near y=270):** "the whole pattern fits in a function signature — frameworks are optional".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); menu prices (regular `[4.00, 4.00, 2.50, 3.00]` totaling 13.50, happy-hour `[3.00, 3.00, 2.00, 3.00]` totaling 11.00) and test timings (50 × 12s = 600s vs 50 × 0.05s = 2.5s) are invented and labeled illustrative; the arithmetic within them is exact and must stay consistent between text and charts.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
