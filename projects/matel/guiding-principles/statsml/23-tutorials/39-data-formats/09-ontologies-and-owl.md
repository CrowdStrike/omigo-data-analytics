# Ontologies & OWL

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Ontologies & OWL

**Subtitle:** An ontology is a schema for meaning — it declares what kinds of things exist and how they may relate, so a machine can deduce facts nobody typed

## The Menu That Explains Itself

**Tags:** `core idea` (blue), `classes & relations` (green), `OWL` (orange)

- **The menu** — a coffee shop writes its menu not as a list of names but as a small ontology
- **Classes** — Latte is-a CoffeeDrink, CoffeeDrink is-a Drink; Espresso and Milk are Ingredients
- **A property** — `madeWith` is declared with a domain (CoffeeDrink) and a range (Ingredient)
- **A guardrail** — Ingredient and Furniture are declared disjoint: nothing can be both
- **The payoff** — a reasoner reads these declarations and starts deducing facts on its own

*Example (italic):* Nobody ever typed "Latte is a Drink," yet the machine knows it — Latte is a CoffeeDrink and every CoffeeDrink is a Drink.

**Key point:** An ontology is a data format for meaning itself: it stores the rules of a domain, not just its rows, so software can infer instead of merely look up.

### Visualization (canvas `c1`, 720×300)

Class-hierarchy diagram of the menu ontology: a tree of rounded boxes with is-a arrows, plus one dashed `madeWith` property arrow showing its domain and range.

- **Title (bold 15px, `#1a5276`, top center):** "The Menu as an Ontology: Classes, is-a Links, One Property".
- **Layout:** four levels at y = 70, 125, 180, 240; boxes 110×28, 8px radius, 12px `#2c3e50` labels, fill `rgba(42,120,214,0.12)` with 1.5px `#2a78d6` border for classes.
- **Level 1:** "Thing" centered at x=360.
- **Level 2:** "Drink" at x=180, "Ingredient" at x=400, "Furniture" at x=590.
- **Level 3:** "CoffeeDrink" at x=180, "Espresso" at x=350, "Milk" at x=460, "Chair" at x=590 (Espresso/Milk/Chair fill `rgba(25,158,112,0.12)`, border aqua `#199e70`).
- **Level 4:** "Latte" at x=110, "Mocha" at x=255, both fill `rgba(74,58,167,0.10)`, border violet `#4a3aa7`.
- **is-a arrows:** solid 2px `#6b7280` lines from each child box top to its parent box bottom, small arrowheads; 11px `#6b7280` label "is-a" beside the Latte→CoffeeDrink line.
- **Property arrow:** dashed (dash 5/4) 2px orange `#d95926` arrow from "CoffeeDrink" to "Ingredient", bold 12px orange label "madeWith (domain → range)" above it.
- **Disjoint marker:** 12px red `#e74c3c` label "disjoint" with a short red zigzag between "Ingredient" and "Furniture".
- **Annotation (bold 13px violet `#4a3aa7`, bottom left, y=285):** "10 boxes and 1 property encode the whole menu's logic".
- **Caption (12px `#444`, bottom right):** "toy ontology, illustrative".

## Five Typed Facts, Three Free Deductions

**Tags:** `worked example` (blue), `reasoner` (green)

- **Typed by a human** — 5 facts: Latte is-a CoffeeDrink; Mocha is-a CoffeeDrink; CoffeeDrink is-a Drink; Latte madeWith Espresso; Chair is-a Furniture
- **Deduction 1** — Latte is a Drink (is-a chains compose: Latte → CoffeeDrink → Drink)
- **Deduction 2** — Mocha is a Drink, by the exact same two-step chain
- **Deduction 3** — Espresso is an Ingredient (it appears as the object of `madeWith`, whose range is Ingredient)
- **The flag** — assert "Latte madeWith Chair" and the reasoner rejects it: the range forces Chair to be an Ingredient, but Chair is a Furniture, and the two are disjoint

*Example (italic):* From 5 typed facts the reasoner derives 3 new ones and catches 1 contradiction — a 60% return in free knowledge, checked by hand in seconds.

**Key point:** The reasoner never guesses — every deduction is a mechanical application of the declared hierarchy, domains, ranges, and disjointness, so you can replay each step yourself.

### Visualization (canvas `c2`, 720×300)

Two-column ledger: facts typed by a human (left, blue boxes) feeding a reasoner that emits deduced facts (right, green boxes) and one rejected fact (red box).

- **Title (bold 15px, `#1a5276`, top center):** "5 Facts In, 3 Deductions Out, 1 Contradiction Caught".
- **Left column (x=30, width 240), 12px `#444` header "typed by a human" at y=55:** five rounded boxes (230×26, fill `rgba(42,120,214,0.12)`, border `#2a78d6`, 12px `#2c3e50` text) at y = 70, 102, 134, 166, 198: "Latte is-a CoffeeDrink", "Mocha is-a CoffeeDrink", "CoffeeDrink is-a Drink", "Latte madeWith Espresso", "Chair is-a Furniture".
- **Center:** rounded box 120×50 at x=310, y=125, fill `rgba(74,58,167,0.12)`, border violet `#4a3aa7`, bold 13px violet label "reasoner"; 2px `#6b7280` arrows from each left box into it and out to each right box.
- **Right column (x=460, width 240), 12px `#444` header "deduced — nobody typed these" at y=55:** three green boxes (230×26, fill `rgba(0,131,0,0.12)`, border `#008300`) at y = 70, 102, 134: "Latte is-a Drink", "Mocha is-a Drink", "Espresso is-a Ingredient".
- **Rejected fact:** red box (230×26, fill `rgba(231,76,60,0.12)`, border `#e74c3c`) at x=460, y=186 labeled "Latte madeWith Chair ✗", bold 12px red caption beneath: "range says Ingredient; Chair is Furniture — disjoint".
- **Annotation (bold 13px green `#008300`, centered at y=270):** "the schema does inference, not just validation".
- **Caption (12px `#444`, bottom right):** "fact counts exact for this toy example".

## Where Ontologies Won — and Where They Sank

**Tags:** `where it's used` (blue), `overreach lesson` (red)

- **Biology & medicine** — gene-function and clinical-term ontologies thrive: closed expert communities curate millions of annotations against one shared hierarchy
- **The lightweight win** — schema.org asks web pages for a few typed properties (a Product, a price, a rating), and search engines read them at web scale
- **The overreach** — the 2000s semantic-web vision expected the open web to publish full formal OWL; almost nobody did
- **Why it sank** — full OWL demands agreement, logic training, and maintenance that anonymous, sloppy, adversarial web authors will never supply
- **The lesson** — deep formal ontologies win inside disciplined domains; the open web settled for shallow shared vocabularies

*Example (italic):* A hospital coding system can afford a 300,000-term curated ontology; a recipe blogger will only ever fill in "name, cookTime, calories."

**Key point:** The technology did not fail — the deployment assumption did. Match the formality of the schema to the discipline of whoever must author against it.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart: real-world uptake of three levels of ontology formality, from shallow shared tags to full formal OWL on the open web.

- **Title (bold 15px, `#1a5276`, top center):** "Uptake by Formality: Shallow and Shared Beat Deep and Formal".
- **Axis:** vertical 2px `#999` baseline at x=250, bars extend right, max width 430; widths schematic, not a linear scale.
- **Rows (bar height 22px, top edges at y = 75, 135, 195), each with a left-aligned 12px `#444` two-line label ending at x=240:**
  - "schema.org tags — open web (lightweight)": blue `#2a78d6` bar width 430, 11px `#444` end label "tens of millions of sites"
  - "biomedical ontologies — expert niches (deep)": aqua `#199e70` bar width 260, end label "millions of curated annotations"
  - "full OWL on the open web (deep + open)": orange `#d95926` bar width 22, end label "niche experiments"
- **Bar fills:** `rgba` at 0.30 alpha of each color with a solid 2px border in the full color.
- **Annotation (bold 13px magenta `#d55181`, right side near y=250):** "formality only survives where a community maintains it".
- **Caption (12px `#444`, bottom right):** "bar widths schematic, illustrative".

## Modeling the World Before Selling a Coffee

**Tags:** `common mistake` (red), `big-bang modeling` (orange)

- **The trap** — teams meet ontologies and try to model the entire domain perfectly before shipping anything
- **The symptom** — month three is spent debating whether Syrup is an Ingredient or a Flavoring, while zero facts serve users
- **The cost** — the model grows faster than anyone can validate it, and the first real query arrives after the budget runs out
- **The fix** — ship a 12-class menu ontology in month one, let real queries expose which distinctions actually matter
- **The evidence** — the ontologies that thrive grew for years by accretion; none were born complete

*Example (italic):* The incremental team's ontology answers live menu queries from month one and reaches 50 classes by month twelve; the big-bang team's 400-class model is cancelled at month ten with zero classes in production.

**Common mistake:** Treating an ontology as a one-shot act of philosophy instead of a living schema — model the 12 classes today's queries need, and let the reasoner earn the right to grow.

### Visualization (canvas `c4`, 720×300)

Line chart over 12 months: classes actually serving queries in production, incremental team vs big-bang team.

- **Title (bold 15px, `#1a5276`, top center):** "Classes in Production: Ship 12, Grow to 50 — or Model 400 and Ship 0".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = months 0 to 12 with 12px `#444` tick labels every 2 months; y = classes in production 0 to 60, gridlines `#e5e9ef` at 15/30/45.
- **Incremental line:** green `#008300` 3px line through months `[1, 2, 4, 6, 8, 10, 12]`, classes `[12, 18, 25, 31, 38, 44, 50]`, small filled dots at each point.
- **Big-bang line:** red `#e74c3c` 3px line through months `[0, 2, 4, 6, 8, 10]`, classes `[0, 0, 0, 0, 0, 0]` — flat along the baseline; bold 13px red "✗ cancelled, month 10" at (month 10, y=215).
- **Marker:** vertical dashed `#6b7280` (dash 4/3) line at month 1, 12px `#6b7280` label "first 12 classes live" at its top.
- **Annotation (bold 13px green `#008300`, near month 7, y=90):** "real queries decide which classes to add next".
- **Caption (12px `#444`, bottom right):** "class counts illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); the worked example's counts (5 typed facts, 3 deductions, 1 contradiction) are exact for the toy ontology; the uptake bar widths (430 / 260 / 22) and the month-by-month class counts (`[12, 18, 25, 31, 38, 44, 50]` vs flat 0, cancelled at month 10) are invented and labeled illustrative/schematic.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
