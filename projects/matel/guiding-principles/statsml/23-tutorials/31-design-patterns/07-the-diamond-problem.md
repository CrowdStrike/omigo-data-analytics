# The Diamond Problem

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** The Diamond Problem

**Subtitle:** When a class has two parents that share a grandparent, it inherits two versions of the same method — and the language must decide whose method wins

## One Mocha, Two Parent Recipes

**Tags:** `core idea` (blue), `inheritance` (green), `two parents` (orange)

- **The menu** — a coffee shop's ordering system models every drink as a class with a prepare() method
- **The base** — Drink defines prepare(): pour into a cup and hand it over the counter
- **Two children** — Coffee overrides it to add an espresso shot; HotChocolate overrides it to melt chocolate
- **The mocha** — Mocha inherits from both Coffee and HotChocolate, because it genuinely is both
- **The diamond** — Drink on top, two parents in the middle, Mocha at the bottom: the shape is a diamond
- **The question** — both parents override prepare(); when a Mocha is prepared, whose method runs?

*Example (italic):* A barista rings up a mocha and the system calls prepare() — espresso first, chocolate first, or somehow both?

**Key point:** The diamond problem: a class with two parents that share a grandparent inherits two competing versions of the same method, and something must pick which one runs.

### Visualization (canvas `c1`, 720×300)

Class diagram drawn as a diamond: Drink at the top, Coffee and HotChocolate in the middle row (both marked as overriding prepare()), Mocha at the bottom pointing up to both.

- **Title (bold 15px, `#1a5276`, top center):** "The Diamond: One Grandparent, Two Parents, One Mocha".
- **Boxes:** rounded rects 170×44, 8px radius, 12px `#2c3e50` two-line labels; Drink at (x=275, y=48) fill `rgba(42,120,214,0.15)` labeled "Drink / prepare(): pour into cup"; Coffee at (x=70, y=132) fill `rgba(0,131,0,0.12)` labeled "Coffee / prepare(): add espresso ✓"; HotChocolate at (x=480, y=132) fill `rgba(213,81,129,0.12)` labeled "HotChocolate / prepare(): add chocolate ✓"; Mocha at (x=275, y=216) fill `rgba(74,58,167,0.12)` labeled "Mocha / no prepare() of its own".
- **Arrows:** 2px `#6b7280` lines with small arrowheads: Mocha top corners up to Coffee and HotChocolate; Coffee and HotChocolate up to Drink's bottom corners — closing the diamond.
- **Annotation (bold 13px orange `#d95926`, centered near y=285):** "Mocha inherits two prepare() methods — the language must pick one".
- **Caption (12px `#444`, bottom right):** "class diagram schematic".

## Pricing the Mocha: 5.50 or 4.50?

**Tags:** `worked example` (blue), `linearization` (green)

- **The base price** — Drink.price() returns 3.00: the cup, the steamed milk, the counter
- **Coffee's markup** — Coffee.price() takes the price so far and adds 1.50 for the espresso shot
- **Chocolate's markup** — HotChocolate.price() takes the price so far and adds 1.00 for the chocolate
- **The chain** — Python lines the diamond up as Mocha → Coffee → HotChocolate → Drink and runs each once
- **Hand-check** — 3.00 + 1.00 + 1.50 = 5.50: both surcharges land exactly once
- **The naive path** — if Coffee jumped straight to Drink, the bill is 3.00 + 1.50 = 4.50 and chocolate is free

*Example (italic):* The same mocha rings up 5.50 when the chain visits every class once, but 4.50 when the HotChocolate step gets skipped.

**Key point:** Languages solve the diamond by linearizing it — flattening the diamond into one ordered chain (Python calls it the MRO) so every class runs exactly once, no class twice.

### Visualization (canvas `c2`, 720×300)

Two-row flow diagram of the mocha's bill: the linearized chain accumulating to 5.50 (top) vs the shortcut that skips HotChocolate and stops at 4.50 (bottom).

- **Title (bold 15px, `#1a5276`, top center):** "One Mocha, Two Bills: the Full Chain Charges 5.50, the Shortcut 4.50".
- **Row 1 (boxes centered on y=105), 12px `#444` label "order the charges land (MRO reversed)" at x=20, y=68:** rounded boxes 130×40, 8px radius: blue `rgba(42,120,214,0.15)` box at x=150 labeled "Drink / 3.00"; 3px `#6b7280` arrow to magenta `rgba(213,81,129,0.12)` box at x=320 labeled "HotChocolate / +1.00 → 4.00"; arrow to green `rgba(0,131,0,0.12)` box at x=490 labeled "Coffee / +1.50 → 5.50"; bold 13px green `#008300` "✓ 5.50" at x=645.
- **Row 2 (boxes centered on y=225), label "shortcut: Coffee jumps to Drink" at x=20, y=188:** blue box at x=150 "Drink / 3.00"; arrow to green box at x=320 "Coffee / +1.50 → 4.50"; dashed 2px `#6b7280` outline-only box at x=490 labeled "HotChocolate / skipped" with bold 12px orange `#d95926` "✗ 1.00 never charged" beneath it at y=262.
- **Annotation (bold 13px violet `#4a3aa7`, right side near y=160):** "linearization visits every class exactly once".
- **Caption (12px `#444`, bottom right):** "prices illustrative".

## Where the Diamond Shows Up

**Tags:** `where it's used` (blue), `mixins` (green), `language rules` (orange)

- **Mixins** — stacking ready-made behaviors (logging, caching, validation) onto one class builds diamonds
- **ML code** — estimator classes routinely combine a base class with fit/predict mixins the same way
- **Python** — C3 linearization computes one MRO; the parent listed first (leftmost) gets priority
- **C++** — refuses to guess: an ambiguous call is a compile error until Mocha overrides or picks one
- **Java** — sidesteps it: one class parent only, and clashing interface defaults must be overridden by hand
- **Order matters** — Mocha(Coffee, HotChocolate) and Mocha(HotChocolate, Coffee) resolve differently

*Example (italic):* A model class mixing in both a logging helper and a caching helper that each wrap save() is the mocha all over again.

**Key point:** You meet the diamond whenever you snap two prebuilt behaviors onto one class — knowing your language's rule tells you which method actually runs.

### Visualization (canvas `c3`, 720×300)

Four-row diagram: each row names a language on the left and shows its one-line answer to the diamond in a colored box.

- **Title (bold 15px, `#1a5276`, top center):** "Four Languages, Four Answers to 'Which Parent Wins?'".
- **Rows (boxes at y = 60, 112, 164, 216), 12px bold `#444` language label at x=20 vertically centered on each box:** answer boxes from x=110, width 560, height 38, 8px radius, 12px `#2c3e50` text inset 12px:
  - "Python": fill `rgba(0,131,0,0.12)`, border 2px `#008300`, text "linearizes: C3 builds one MRO, leftmost parent wins ties"
  - "Scala": fill `rgba(25,158,112,0.12)`, border 2px `#199e70`, text "linearizes traits: the rightmost trait's method wins"
  - "C++": fill `rgba(217,89,38,0.12)`, border 2px `#d95926`, text "refuses to guess: ambiguity is a compile error until the child overrides or picks one"
  - "Java": fill `rgba(74,58,167,0.12)`, border 2px `#4a3aa7`, text "bans class diamonds: one superclass; clashing interface defaults must be overridden"
- **Annotation (bold 13px magenta `#d55181`, centered near y=278):** "no language flips a coin — each has a written rule".
- **Caption (12px `#444`, bottom right):** "rules simplified to one line each".

## super() Is Not Your Parent

**Tags:** `common mistake` (red), `super()` (orange)

- **The wrong model** — people read super() as "call my parent" and expect Coffee's super() to be Drink
- **The real rule** — super() means "call the next class in this object's chain", whatever that turns out to be
- **The sibling call** — inside a Mocha, Coffee's super().price() lands on HotChocolate, its sibling
- **Why it works** — that detour is exactly what charges the 1.00 chocolate on the way down to Drink
- **The breakage** — hardcoding Drink.price(self) instead of super() skips the sibling and loses the 1.00
- **The symptom** — code that worked in Coffee alone silently misprices once combined into Mocha

*Example (italic):* A Coffee tested on its own prices fine at 4.50; drop it into Mocha with a hardcoded Drink.price call and the mocha rings up 4.50 instead of 5.50.

**Common mistake:** Treating super() as a fixed pointer to the parent class. It is relative to the object's linearized chain, so the same Coffee code calls a different "next" in every subclass it ends up in.

### Visualization (canvas `c4`, 720×300)

Split diagram: left shows the assumed family-tree jump (Coffee straight to Drink, marked wrong); right shows the actual chain inside a Mocha, where Coffee's super() lands on HotChocolate.

- **Title (bold 15px, `#1a5276`, top center):** "super() Follows the Chain, Not the Family Tree".
- **Divider:** 1px `#e5e9ef` vertical line at x=360 from y=45 to y=285.
- **Left half, 12px bold `#444` label "what people assume" at x=60, y=60:** green `rgba(0,131,0,0.12)` box 150×34 at (x=100, y=120) labeled "Coffee", dashed 2px `#6b7280` arrow up to a blue `rgba(42,120,214,0.15)` box 150×34 at (x=100, y=195) labeled "Drink", 12px `#6b7280` arrow label "super() → parent?" with bold 12px orange `#d95926` "✗ not how it resolves" at (x=100, y=255).
- **Right half, label "what actually happens (inside a Mocha)" at x=400, y=60:** four stacked boxes 180×30, 8px radius, at x=400, tops y=78 / 130 / 182 / 234: violet `rgba(74,58,167,0.12)` "Mocha", green `rgba(0,131,0,0.12)` "Coffee", magenta `rgba(213,81,129,0.12)` "HotChocolate", blue `rgba(42,120,214,0.15)` "Drink"; 2px `#6b7280` arrows connect each box down to the next; the Coffee→HotChocolate arrow is 3px green `#008300` with a bold 12px green two-line label "Coffee's super()" / "= HotChocolate" to its right, left-aligned at x=595, y=162 and y=178.
- **Annotation (bold 13px magenta `#d55181`, left half near x=60, y=285):** "same Coffee code, different next class in every subclass".
- **Caption (12px `#444`, bottom right):** "chain shown for Mocha(Coffee, HotChocolate)".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all boxes, arrows, and figures are the hardcoded layouts and literal numbers above (no randomness); the prices 3.00 base, +1.50 espresso, +1.00 chocolate and the totals 5.50 (full chain) vs 4.50 (shortcut) are invented and labeled illustrative; the four language rules (Python C3 MRO, Scala rightmost-trait, C++ ambiguity-is-an-error, Java single-superclass) are real documented language semantics.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
