# Newtype & Typestate (Rust)

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Newtype & Typestate (Rust)

**Subtitle:** Rust lets you wrap a plain number or state in its own type so the compiler enforces your rules — and the wrapper vanishes at runtime, costing nothing

## Cents Are Not Grams at the Coffee Counter

**Tags:** `core idea` (blue), `newtype` (green), `Rust` (orange)

- **The shop** — a till stores a latte's price (450 cents) and its espresso dose (18 grams)
- **The problem** — both are plain integers, so `charge(18)` compiles and bills the customer 18 cents
- **The newtype** — wrap each number: `struct Cents(u64)` and `struct Grams(u64)` are different types
- **The rule** — `charge(price: Cents)` refuses a `Grams` value; the mix-up dies at compile time
- **Zero cost** — the wrapper is erased when compiled; `Cents(450)` is the bare integer 450 in memory

*Example (italic):* The till code calls `charge(Grams(18))` by accident; the program never builds, and no customer is ever billed 18 cents for a 450-cent latte.

**Key point:** A newtype gives an old value a new name the compiler enforces — same bits at runtime, but wrong-unit code becomes impossible to write.

### Visualization (canvas `c1`, 720×300)

Two-row flow diagram: the same raw integer wrapped two ways flowing into `charge(price: Cents)` — the Cents path compiles, the Grams path is rejected at compile time.

- **Title (bold 15px, `#1a5276`, top center):** "Same Bits, Different Types: the Compiler Sorts Cents from Grams".
- **Row 1 (y=100), label 12px `#444` at x=20:** "right unit"; blue `#2a78d6` rounded box at x=150 labeled "Cents(450)" (12px), 3px arrow to a green `#008300` box at x=400 labeled "charge(price: Cents)" with bold 12px green "✓ compiles — bill 450¢" at its right.
- **Row 2 (y=210), label:** "wrong unit"; blue box at x=150 labeled "Grams(18)", 3px arrow to a red `#e74c3c` box at x=400 labeled "charge(price: Cents)" with bold 12px red "✗ compile error — never runs".
- **Box style:** 150–180px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(0,131,0,0.12)` / `rgba(231,76,60,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=270):** "at runtime both are one machine integer — the fence exists only at compile time".
- **Caption (12px `#444`, bottom right):** "prices and doses illustrative".

## Four Order States, Nine Impossible Moves

**Tags:** `worked example` (blue), `typestate` (green)

- **The states** — an order lives as Draft, then Paid, then Brewed, then PickedUp — four types, not one flag
- **The methods** — `pay()` exists only on Draft, `brew()` only on Paid, `hand_over()` only on Brewed
- **The count** — 4 states give 4 × 3 = 12 possible cross-state moves; only 3 of them are legal
- **Hand-check** — the other 12 − 3 = 9 moves (like brewing an unpaid Draft) simply have no method to call
- **The payoff** — every one of the 9 bad paths is a compile error, not a 2pm bug on the shop floor

*Example (italic):* A new hire writes code that hands over an order straight from Draft; `Draft` has no `hand_over()`, so the build fails before the shop opens.

**Key point:** Typestate makes each stage its own type, so of the 12 conceivable transitions only the 3 legal ones can even be written — 9 bugs are deleted before the program runs.

### Visualization (canvas `c2`, 720×300)

State-machine diagram: four boxes in a row with the 3 legal transitions as solid green arrows and 2 sample illegal moves as red dashed arcs marked with an ✗.

- **Title (bold 15px, `#1a5276`, top center):** "Only 3 of 12 Moves Exist: pay → brew → hand_over".
- **State boxes (y=140, 120px wide, 44px tall, 8px radius, fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border, 13px `#2c3e50` text), centers at x = `[110, 280, 450, 620]`:** labeled "Draft", "Paid", "Brewed", "PickedUp".
- **Legal arrows:** three 3px green `#008300` horizontal arrows between adjacent boxes, each with a bold 12px green method label above: "pay()", "brew()", "hand_over()".
- **Illegal arcs:** red `#e74c3c` 2px dashed (dash 5/4) arcs: one above from Draft (x=110) to Brewed (x=450) peaking at y=70, one below from PickedUp (x=620) back to Draft (x=110) peaking at y=230; each midpoint carries a bold 12px red "✗ no such method".
- **Annotation (bold 13px violet `#4a3aa7`, near x=360, y=272):** "9 of the 12 cross-state moves fail to compile".
- **Caption (12px `#444`, bottom right):** "transition count exact: 4×3 = 12 pairs, 3 legal".

## Where Mixed-Up Numbers Bite an Engineer

**Tags:** `where it's used` (blue), `zero cost` (green)

- **Unit bugs** — cents vs dollars, grams vs milliliters: silent factor-of-100 errors in billing and dosing code
- **ID swaps** — `user_id` and `order_id` are both integers; joining on the wrong one corrupts a whole report
- **Tainted data** — a `RawInput` vs `Sanitized` newtype stops unchecked text reaching a query
- **The cost test** — adding two raw u64s takes 1.0 ns; adding two `Cents` also takes 1.0 ns — identical code
- **The alternative** — checking units with an if-statement at runtime costs 1.6 ns on every single call, forever

*Example (italic):* A data scientist wraps ids as `UserId` and `OrderId`; a join written on the wrong column stops compiling instead of quietly matching user 450 to order 450.

**Key point:** The checks run in the compiler, not the program — the safety of a runtime guard at the price of none, which is why the pattern is called zero-cost.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart: time per addition for a raw integer, a newtype-wrapped integer, and a runtime-checked version — the first two bars identical.

- **Title (bold 15px, `#1a5276`, top center):** "Nanoseconds per Add: the Wrapper Compiles Away".
- **Axis:** vertical 2px `#999` baseline at x=230, bars extend right, max width 440; x scale 0–2.0 ns with 12px `#444` tick labels at 0 / 0.5 / 1.0 / 1.5 / 2.0 and gridlines `#e5e9ef`.
- **Rows (top to bottom at y = 90, 150, 210), each with a left-aligned 12px `#444` label at x=20, bars 18px tall:**
  - "raw u64 add — 1.0 ns": blue `#2a78d6` bar width 220
  - "newtype Cents add — 1.0 ns": green `#008300` bar width 220
  - "runtime unit check — 1.6 ns": orange `#d95926` bar width 352
- **Bar style:** solid fills, 11px `#444` value labels ("1.0 ns", "1.0 ns", "1.6 ns") just past each bar end.
- **Annotation (bold 13px green `#008300`, right-aligned at x=w-12, y=120):** "identical bars — the wrapper costs 0 ns at runtime".
- **Caption (12px `#444`, bottom right):** "timings illustrative".

## A Type Alias Is Not a Newtype

**Tags:** `common mistake` (red), `alias vs wrapper` (orange)

- **The confusion** — `type Cents = u64;` looks like the same trick but is only a nickname for u64
- **No fence** — with the alias, `charge(grams)` still compiles; the compiler sees two u64s, not two units
- **The symptom** — the 18-gram dose sails into `charge()` and the latte rings up at 18 cents, not 450
- **The fix** — one line: `struct Cents(u64);` — a real type the compiler refuses to confuse with Grams
- **The tell** — if you can pass the value without unwrapping or converting, you wrote an alias, not a newtype

*Example (italic):* A refactor swaps two alias-typed arguments; every test that only checks "it runs" passes, and the shop undercharges by 432 cents per latte until an audit.

**Common mistake:** Believing the alias gives type safety. An alias renames; a newtype separates — only the wrapper struct makes cents-vs-grams a compile error.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: the same wrong-unit call under an alias (slips through, wrong charge at runtime) vs under a newtype (blocked at compile time).

- **Title (bold 15px, `#1a5276`, top center):** "Alias Lets the Bug Through; Newtype Stops the Build".
- **Row 1 (y=100), label 12px `#444` at x=20:** "type Cents = u64"; blue `#2a78d6` rounded box at x=170 labeled "grams: 18" (12px), 3px arrow to a yellow `#c98500` box at x=380 labeled "charge(18) compiles", then arrow to a red `#e74c3c` box at x=580 labeled "billed 18¢, not 450¢" with bold 12px red "✗ runtime bug".
- **Row 2 (y=210), label:** "struct Cents(u64)"; blue box at x=170 labeled "Grams(18)", 3px arrow to a green `#008300` box at x=380 labeled "compile error" with bold 12px green "✓ bug never ships" at its right.
- **Box style:** 140–170px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(201,133,0,0.15)` / `rgba(231,76,60,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px orange `#d95926`, centered near y=272):** "an alias renames the number; a newtype builds a wall around it".
- **Caption (12px `#444`, bottom right):** "charge amounts illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all values are the hardcoded literals above (no randomness); the latte price 450 cents, espresso dose 18 grams, and add timings 1.0 / 1.0 / 1.6 ns are invented and labeled illustrative; the transition arithmetic (4 states, 4×3 = 12 cross-state pairs, 3 legal, 9 blocked) is exact; box x-positions and bar pixel widths (220 / 220 / 352 on a 0–2.0 ns scale) are as listed.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
