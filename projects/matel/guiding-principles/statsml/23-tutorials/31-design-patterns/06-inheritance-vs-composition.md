# Inheritance vs Composition

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Inheritance vs Composition

**Subtitle:** Two ways to build objects from other objects — "a latte is-a drink" extends a class, "a drink has-a milk and a syrup" plugs parts together

## One Menu, Two Ways to Build It

**Tags:** `core idea` (blue), `is-a vs has-a` (green), `object design` (orange)

- **The shop** — a coffee shop's ordering app must model every drink the counter can sell
- **Is-a** — inheritance says a Latte is-a Drink: a subclass that inherits price and size logic
- **Has-a** — composition says a Drink has-a base, has-a milk, has-a syrup: parts plugged together
- **The tree** — inheritance grows a family tree of drink types, one named class per drink
- **The kit** — composition keeps a parts kit; any order is assembled from pieces at runtime

*Example (italic):* The order "iced oat latte" is one leaf class in the tree, but just three parts — espresso + oat milk + ice — pulled from the kit.

**Key point:** Inheritance models "X is a kind of Y" by extending a class; composition models "X is built from Ys" by holding parts as fields — the same menu can be written either way.

### Visualization (canvas `c1`, 720×300)

Two-row diagram of the same drink built both ways: an is-a chain of subclasses on top, a has-a box of parts below.

- **Title (bold 15px, `#1a5276`, top center):** "The Same Iced Oat Latte: One Node in a Tree vs Three Parts in a Kit".
- **Row 1 (y=95), label 12px `#444` at x=20:** "is-a (inheritance)"; four rounded boxes left to right at x = 150, 300, 450, 600 labeled "Drink", "Latte", "OatLatte", "IcedOatLatte" (12px), joined by 3px `#2a78d6` arrows pointing from each child back to its parent, each tagged 11px `#6b7280` "is-a"; first three boxes blue fill `rgba(42,120,214,0.15)`, last box violet `#4a3aa7` border with fill `rgba(74,58,167,0.12)`.
- **Row 2 (y=205), label:** "has-a (composition)"; one wide `#1a5276`-bordered box at x=150 labeled "Drink order" (12px), containing three small part boxes side by side labeled "espresso" (blue `rgba(42,120,214,0.15)`), "oat milk" (green `rgba(0,131,0,0.12)`), "ice" (aqua `rgba(25,158,112,0.15)`) separated by bold 13px `#6b7280` "+" signs.
- **Box style:** chain boxes 120px wide, part boxes 100px wide, 40px tall, 8px radius, 12px `#2c3e50` text.
- **Annotation (bold 13px violet `#4a3aa7`, right side near y=250):** "one class per drink vs one field per part".
- **Caption (12px `#444`, bottom right):** "menu schematic, illustrative".

## Counting the Classes: 48 vs 11

**Tags:** `worked example` (blue), `combinatorial explosion` (red)

- **The menu** — 3 base drinks: espresso, filter coffee, hot cocoa
- **The options** — 4 customer choices, 2 ways each: milk, temperature, size, caffeine
- **Inheritance count** — each option doubles the tree: 3 → 6 → 12 → 24 → 48 subclasses
- **Composition count** — each option adds 2 parts to the kit: 3 → 5 → 7 → 9 → 11 pieces
- **Hand-check** — 3 × 2 × 2 × 2 × 2 = 48 combinations, but only 3 + (4 × 2) = 11 distinct parts

*Example (italic):* Adding a fifth option (syrup: vanilla or caramel) means writing 48 new subclasses in the tree but stocking only 2 new parts in the kit.

**Key point:** Combinations multiply while parts add — an inheritance tree pays the multiplied cost, a composition kit pays the added cost.

### Visualization (canvas `c2`, 720×300)

Line chart of class count vs options added: the inheritance line doubles at every step, the composition line climbs by two.

- **Title (bold 15px, `#1a5276`, top center):** "Each New Option: the Tree Doubles, the Kit Adds Two".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = options added 0 to 4, 12px `#444` tick labels "0"–"4"; y = classes or parts 0 to 50, gridlines `#e5e9ef` at 10/20/30/40 with 12px `#444` labels.
- **Inheritance line:** orange `#d95926` 3px line with 4px dots through options `[0, 1, 2, 3, 4]`, counts `[3, 6, 12, 24, 48]`; bold 12px orange label "subclasses" near the last point.
- **Composition line:** green `#008300` 3px line with 4px dots through the same options, counts `[3, 5, 7, 9, 11]`; bold 12px green label "kit parts" near the last point.
- **Annotation (bold 13px orange `#d95926`, near x=2.2, y=75):** "4 options: 48 subclasses vs 11 parts".
- **Caption (12px `#444`, bottom right):** "menu setup illustrative; counts follow exactly from it".

## Where Pipelines Beat Family Trees

**Tags:** `where it's used` (blue), `ML pipelines` (green)

- **ML pipelines** — a pipeline has-a scaler, has-an encoder, has-a model: composition end to end
- **Swapping** — replacing the model means changing one part, not rewriting a subclass hierarchy
- **Testing** — a part can be tested alone; a deep subclass drags its whole ancestry into every test
- **Frameworks** — dataframes, tokenizer-plus-model stacks, and layered plots are all has-a designs
- **The default** — "favor composition over inheritance" is standard design advice for this reason

*Example (italic):* Switching a churn pipeline from logistic regression to boosted trees swaps one part; the scaler and encoder stay untouched.

**Key point:** Data science tooling leans on composition because analyses change one piece at a time — swap the model, keep the preprocessing, and nothing else needs to know.

### Visualization (canvas `c3`, 720×300)

Two-row flow diagram of a three-part pipeline before and after a model swap: only the last box changes.

- **Title (bold 15px, `#1a5276`, top center):** "A Model Swap in a has-a Pipeline Touches One Box".
- **Row 1 (y=95), label 12px `#444` at x=20:** "before"; three rounded boxes at x = 170, 360, 550 labeled "scaler", "encoder", "logistic model" (12px), joined by 3px `#6b7280` arrows; all fills blue `rgba(42,120,214,0.15)`.
- **Row 2 (y=205), label:** "after"; the same "scaler" and "encoder" boxes unchanged, third box at x=550 labeled "boosted trees" with green `#008300` border and fill `rgba(0,131,0,0.12)`, bold 12px green "✓ swapped" above it.
- **Box style:** 150px wide, 40px tall, 8px radius, 12px `#2c3e50` text.
- **Annotation (bold 13px green `#008300`, centered near y=265):** "2 of 3 parts never change".
- **Caption (12px `#444`, bottom right):** "pipeline schematic, illustrative".

## Inheriting Just to Reuse Code Is the Trap

**Tags:** `common mistake` (red), `fragile base class` (orange)

- **The shortcut** — a class inherits another only to reuse its methods, with no real is-a meaning
- **The test** — say it out loud: "an OrderQueue is-a CustomerList" sounds wrong because it is
- **Fragile base** — one edit to the base class silently changes behavior in every subclass below
- **The ripple** — in the 48-class drink tree, editing Drink's price logic touches all 48 at once
- **The fix** — hold the other class as a field (has-a) and expose only the methods that make sense

*Example (italic):* A DiscountedOrder that inherits Order also gains a public "cancel all items" method it never wanted; holding an Order inside would keep it hidden.

**Common mistake:** Reaching for inheritance because it saves typing. Inherit only when the subclass truly is-a parent everywhere the parent appears; otherwise compose.

### Visualization (canvas `c4`, 720×300)

Horizontal bar chart: how many classes change behavior after one edit to the shared price logic, tree vs kit.

- **Title (bold 15px, `#1a5276`, top center):** "One Edit to the Shared Price Logic: Classes Whose Behavior Changes".
- **Axis:** vertical 2px `#999` baseline at x=230, bars extend right, 10px of width per class.
- **Rows (at y = 110 and y = 190), each with a left-aligned 12px `#444` label at x=20:**
  - "inheritance tree (48 subclasses)": red `#e74c3c` bar width 480, bold 12px red label "48 classes affected" at the bar end
  - "composition kit (price part)": green `#008300` bar width 10, bold 12px green label "1 part affected" at the bar end
- **Bar style:** 22px tall, red fill `rgba(231,76,60,0.25)` with 2px `#e74c3c` border, green solid `#008300`.
- **Annotation (bold 13px magenta `#d55181`, right side near y=250):** "a shared base class is a single point of failure".
- **Caption (12px `#444`, bottom right):** "widths proportional: 10px per class".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); the coffee-shop menu (3 bases, 4 two-way options) is invented and labeled illustrative, but the counts derive exactly from it: subclasses `[3, 6, 12, 24, 48]` = 3 × 2^k, kit parts `[3, 5, 7, 9, 11]` = 3 + 2k, and the c4 bars (48 vs 1) reuse the 48-class tree at 10px per class.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
