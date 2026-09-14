# Polymorphism & Dynamic Dispatch

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Polymorphism & Dynamic Dispatch

**Subtitle:** One call, many behaviors — the checkout writes `pay(order)` once, and at runtime the program looks at what kind of order it is and runs the matching code

## One Checkout Button, Three Ways to Pay

**Tags:** `core idea` (blue), `one call, many behaviors` (green), `runtime choice` (orange)

- **The shop** — a coffee shop's register accepts cash, card, and gift-card payments at one counter
- **The call** — the checkout code says `pay(order)` and nothing else; it never asks which kind
- **The behaviors** — cash opens the drawer, card contacts the bank, gift card deducts a balance
- **The dispatch** — at the moment of the call, the runtime checks the order's actual type and jumps there
- **The payoff** — the checkout line of code never changes, no matter how many payment types exist

*Example (italic):* The barista rings up three lattes in a row — cash, card, gift card — and the register runs the same `pay(order)` line all three times, doing something different each time.

**Key point:** Polymorphism means one call works on many kinds of thing; dynamic dispatch is the runtime lookup that picks which version of the code actually runs.

### Visualization (canvas `c1`, 720×300)

Fan-out flow diagram: a single `pay(order)` call box on the left, three arrows to three behavior boxes on the right, one per payment type.

- **Title (bold 15px, `#1a5276`, top center):** "One Call Site, Three Behaviors Picked at Runtime".
- **Call box:** blue `#2a78d6` rounded box at x=50, y=135, 170px wide, 44px tall, fill `rgba(42,120,214,0.15)`, 13px `#2c3e50` label "checkout: pay(order)".
- **Dispatch hub:** small violet `#4a3aa7` filled circle at x=300, y=157, 12px `#4a3aa7` label "runtime looks at the order's type" just below it.
- **Behavior boxes (x=430, 210px wide, 40px tall, 8px radius, 12px text), top to bottom at y = 75, 137, 199:** green `#008300` box fill `rgba(0,131,0,0.12)` "CashOrder.pay → open drawer"; violet `#4a3aa7` box fill `rgba(74,58,167,0.12)` "CardOrder.pay → contact bank"; aqua `#199e70` box fill `rgba(25,158,112,0.12)` "GiftCardOrder.pay → deduct balance".
- **Arrows:** one 3px `#6b7280` arrow from the call box to the hub; three 2px arrows from the hub to each behavior box, colored to match each box.
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=270):** "the checkout code is written once — the arrow is chosen per order".
- **Caption (12px `#444`, bottom right):** "schematic — boxes and arrows, no data".

## Charging a $5 Latte Three Different Ways

**Tags:** `worked example` (blue), `same call, different math` (green)

- **The latte** — every order below is the same $5.00 latte; only the payment type differs
- **Cash** — `pay` takes $5.00, fee $0.00, so the shop nets the full $5.00
- **Card** — `pay` charges $5.00 but the processor takes 2% + $0.10 = $0.20, netting $4.80
- **Gift card** — `pay` deducts $5.00 from a $20.00 balance, leaving $15.00; the shop nets $5.00
- **The day** — 200 orders: 90 cash, 70 card, 40 gift card, all through the one `pay(order)` line
- **Hand-check** — only card orders carry fees: 70 orders × $0.20 = $14.00 in fees for the day

*Example (italic):* Three customers buy the same $5.00 latte; the shop nets $5.00, $4.80, and $5.00 because dispatch ran three different `pay` bodies.

**Key point:** The caller passed identical inputs three times — the differing results come entirely from which type's `pay` method the dispatch selected.

### Visualization (canvas `c2`, 720×300)

Horizontal bar chart: one row per payment type showing the shop's net on a $5.00 latte, with the card fee drawn as a red overlay slice, plus the day's order counts as row labels.

- **Title (bold 15px, `#1a5276`, top center):** "Same $5.00 Latte, Three pay() Bodies, Three Nets".
- **Axis:** vertical 2px `#999` baseline at x=230, bars extend right, $5.00 = 440px, gridlines `#e5e9ef` at $1 intervals with 12px `#444` labels "$1"–"$5" along y=260.
- **Rows (bars 22px tall, top to bottom at y = 75, 140, 205), each with a left-aligned 12px `#444` two-line label at x=20 giving type and day count:**
  - "cash — 90 orders/day": green `#008300` bar width 440 (net $5.00), 12px label "$5.00" at bar end
  - "card — 70 orders/day": violet `#4a3aa7` bar width 422 (net $4.80) plus red `#e74c3c` overlay slice width 18 at the bar's right end (fee $0.20), 12px red label "fee $0.20"
  - "gift card — 40 orders/day": aqua `#199e70` bar width 440 (net $5.00), 12px label "$5.00; balance $20.00 → $15.00"
- **Bar fills:** `rgba(0,131,0,0.30)` / `rgba(74,58,167,0.30)` / `rgba(25,158,112,0.30)` with solid 2px borders in the row color; fee overlay solid red.
- **Annotation (bold 13px red `#e74c3c`, right side near y=250):** "70 card orders × $0.20 = $14.00 fees today".
- **Caption (12px `#444`, bottom right):** "prices, counts, and 2% + $0.10 fee illustrative".

## Why One predict() Call Fits Every Model

**Tags:** `where it's used` (blue), `sklearn interface` (green)

- **The interface** — scikit-learn models all answer `fit(X, y)` and `predict(X)`; dispatch does the rest
- **The loop** — one benchmarking loop calls `model.predict(X_test)` on a list of very different models
- **The run** — linear regression scores MAE 12.4, decision tree 9.8, k-nearest-neighbors 11.1
- **The win** — adding a fourth model means appending it to the list; the loop's code never changes
- **Everywhere** — Python's `len()`, the `+` operator, and file-like objects all work the same way

*Example (italic):* The same five-line loop benchmarks all three models and crowns the tree (MAE 9.8) — nobody wrote per-model code.

**Key point:** Shared interfaces plus dynamic dispatch are why one experiment harness runs any model — the loop calls the name, and each object supplies its own behavior.

### Visualization (canvas `c3`, 720×300)

Vertical bar chart: test-set MAE for three models, all produced by the identical `model.predict(X_test)` call, with the shared call shown above the bars.

- **Title (bold 15px, `#1a5276`, top center):** "One Loop, Three Models: model.predict(X_test) Every Time".
- **Axes:** origin x=70, baseline y=245, plot width 580, plot height 170; y = MAE 0 to 15, gridlines `#e5e9ef` at 5/10/15 with 12px `#444` labels.
- **Call banner:** blue `#2a78d6` rounded box centered at y=70, 300px wide, fill `rgba(42,120,214,0.15)`, 12px mono-style `#2c3e50` text "for model in models: model.predict(X_test)"; three thin 2px `#6b7280` arrows from its bottom edge to the three bar tops.
- **Bars (90px wide, centered at x = 170, 360, 550, 12px `#444` name below and bold 13px value above each):** "linear regression" blue `#2a78d6` height for MAE 12.4; "decision tree" green `#008300` height for MAE 9.8; "kNN" orange `#d95926` height for MAE 11.1; fills at 0.30 alpha with solid 2px borders.
- **Annotation (bold 13px green `#008300`, beside the tree bar near y=110):** "best model found — loop code untouched".
- **Caption (12px `#444`, bottom right):** "MAE values illustrative".

## Dispatch Is Not an if-else Chain

**Tags:** `common mistake` (red), `type checks` (orange)

- **The imitation** — a chain of `if isinstance(order, Cash): ... elif isinstance(order, Card): ...` also "works"
- **The rot** — that chain gets copy-pasted; soon 12 call sites each list every payment type by hand
- **The cost** — adding one gift-card-v2 type means editing all 12 chains; missing one charges the wrong fee
- **The polymorphic way** — write one new class with its own `pay`; the existing 12 call sites need 0 edits
- **A near-twin** — overloading picks a version at compile time from argument types; dispatch picks at
  runtime from the object's actual type

*Example (italic):* The team adds a prepaid-app payment type: the if-else codebase edits 12 scattered chains and misses one in refunds; the polymorphic codebase adds 1 class and ships.

**Common mistake:** Reaching for isinstance chains instead of a method — every new type then means hunting down every chain, and the compiler won't tell you which one you missed.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram comparing how a new payment type lands: the if-else codebase (edit every chain, one missed) vs the polymorphic codebase (one new class, zero edits).

- **Title (bold 15px, `#1a5276`, top center):** "Adding a New Payment Type: Edit 12 Chains vs Add 1 Class".
- **Row 1 (y=95), label 12px `#444` at x=20:** "if-else chains"; orange `#d95926` rounded box at x=170 labeled "new type: PrepaidApp" (12px), 3px arrow to a red `#e74c3c` box at x=400 labeled "edit 12 call sites by hand" with bold 12px red "✗ 1 chain missed — wrong fee in refunds" to its right.
- **Row 2 (y=205), label:** "polymorphism"; orange box "new type: PrepaidApp", 3px arrow to a green `#008300` box at x=400 labeled "add 1 class with its own pay()" with bold 12px green "✓ 0 existing lines touched".
- **Box style:** 170–200px wide, 40px tall, 8px radius, fills `rgba(217,89,38,0.12)` / `rgba(231,76,60,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=270):** "dispatch moves the type check into the language — you stop maintaining it".
- **Caption (12px `#444`, bottom right):** "call-site counts illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all numbers are the hardcoded values above (no randomness) and are invented/illustrative: $5.00 latte with nets `[5.00, 4.80, 5.00]`, card fee 2% + $0.10 = $0.20, gift balance $20.00 → $15.00, daily orders `[90, 70, 40]` (70 × $0.20 = $14.00 fees), model MAEs `[12.4, 9.8, 11.1]`, and the 12-call-sites-vs-1-class edit counts.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
