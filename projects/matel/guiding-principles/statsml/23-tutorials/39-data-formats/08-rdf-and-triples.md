# RDF & Triples

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** RDF & Triples

**Subtitle:** RDF stores data as tiny three-part facts — subject, predicate, object — so any two datasets can merge by simply stacking their facts on top of each other

## A Coffee Menu Smashed Into Atomic Facts

**Tags:** `core idea` (blue), `atomic facts` (green), `graph data` (orange)

- **The menu** — a coffee shop's board says a latte is $4.50 and is made from espresso plus steamed milk
- **The smash** — every statement becomes one three-part fact: (Latte, costs, 4.50), (Latte, madeWith, Espresso)
- **The parts** — subject is the thing, predicate is the relationship, object is the value or another thing
- **The count** — three drinks decompose into exactly 7 triples; nothing else is needed
- **The name** — this format is RDF (Resource Description Framework); each fact is called a triple
- **The shape** — connect subjects to objects with predicate arrows and the menu becomes a graph

*Example (italic):* "A cappuccino costs $4.00 and contains espresso and milk foam" is not one record — it is three triples: (Cappuccino, costs, 4.00), (Cappuccino, madeWith, Espresso), (Cappuccino, madeWith, MilkFoam).

**Key point:** A triple is the smallest possible statement about the world — subject, predicate, object — and an RDF dataset is nothing more than a bag of such statements.

### Visualization (canvas `c1`, 720×300)

Node-edge graph of the 7 menu triples: drink nodes on the left, ingredient and price nodes on the right, one labeled arrow per triple.

- **Title (bold 15px, `#1a5276`, top center):** "One Coffee Menu = 7 Triples = One Small Graph".
- **Nodes (rounded boxes 26px tall, 12px `#2c3e50` labels, hardcoded centers):** subjects "Latte" at (130, 105) and "Cappuccino" at (130, 215), both fill `rgba(42,120,214,0.15)` with 2px `#2a78d6` border; resource objects "Espresso" at (400, 160), "SteamedMilk" at (400, 70), "MilkFoam" at (400, 250), fill `rgba(25,158,112,0.15)` with 2px `#199e70` border; literal objects "$4.50" at (620, 105), "$4.00" at (620, 215), "$3.00" at (620, 160), fill `rgba(201,133,0,0.15)` with 2px `#c98500` border.
- **Edges (7 arrows, 2px, arrowheads, 11px predicate label at each midpoint):** blue `#2a78d6` "madeWith" arrows Latte→Espresso, Latte→SteamedMilk, Cappuccino→Espresso, Cappuccino→MilkFoam; yellow `#c98500` "costs" arrows Latte→$4.50, Cappuccino→$4.00, Espresso→$3.00.
- **Annotation (bold 13px violet `#4a3aa7`, near (360, 288)):** "every arrow is one triple: subject → predicate → object".
- **Caption (12px `#444`, bottom right):** "menu prices illustrative".

## The Same Menu as a Table vs a Triple Store

**Tags:** `worked example` (blue), `merge by stacking` (green), `URIs` (orange)

- **The table** — 3 rows × columns price, ingredient1, ingredient2; Espresso's ingredient cells sit empty (NULL)
- **The triples** — the same content is 7 rows of (subject, predicate, object) with no empty cells anywhere
- **A new fact** — recording (Espresso, caffeineMg, 75) forces a new column on every table row, but is just triple #8
- **The merge** — a second shop publishes 5 triples of its own; concatenating 8 + 5 gives a 13-triple store, no schema meeting
- **Global names** — writing `http://menu.example/Espresso` instead of the bare word "Espresso" makes both shops' facts snap onto the same node
- **Hand-check** — after the merge, asking "what is madeWith Espresso?" returns Latte, Cappuccino, and Mocha in one query

*Example (italic):* Shop B's 5 triples — (Mocha, costs, 5.00), (Mocha, madeWith, Espresso), (Mocha, madeWith, Chocolate), (Mocha, calories, 290), (Latte, calories, 190) — stack under Shop A's 8 with zero reformatting.

**Key point:** Tables must agree on columns before they can hold a fact; triples never do — adding or merging data is always just appending more three-part rows.

### Visualization (canvas `c2`, 720×300)

Side-by-side comparison: left half a menu table with NULL holes and a "new column" problem, right half the same facts as a growing triple list.

- **Title (bold 15px, `#1a5276`, top center):** "Table Needs New Columns; Triple Store Just Appends Rows".
- **Left table (x=30 to 330, header row y=70, rows at y=95/120/145, 12px labels):** columns "drink | price | ingr1 | ingr2 | caffeine?"; rows `["Latte","4.50","Espresso","StMilk","—"]`, `["Cappuccino","4.00","Espresso","Foam","—"]`, `["Espresso","3.00","NULL","NULL","75"]`; NULL cells filled `rgba(231,76,60,0.12)` with 12px red `#e74c3c` text; the "caffeine?" header drawn in bold 12px red `#e74c3c` with a red dashed border around that column.
- **Left label (bold 12px red `#e74c3c`, near (90, 185)):** "one new fact → new column, NULLs everywhere".
- **Right list (x=400 to 690, 8 rows starting y=70, 20px spacing, 11px `#2c3e50` mono-style text):** `(Latte, costs, 4.50)`, `(Latte, madeWith, Espresso)`, `(Latte, madeWith, SteamedMilk)`, `(Cappuccino, costs, 4.00)`, `(Cappuccino, madeWith, Espresso)`, `(Cappuccino, madeWith, MilkFoam)`, `(Espresso, costs, 3.00)` in text `#2c3e50`, then `(Espresso, caffeineMg, 75)` in bold green `#008300` with a green "+ just appended" tag at its right.
- **Divider:** vertical 1px `#e5e9ef` line at x=365 from y=55 to y=265.
- **Annotation (bold 13px green `#008300`, near (470, 262)):** "no NULLs, no schema change — row 8 just lands".
- **Caption (12px `#444`, bottom right):** "prices and caffeine mg illustrative".

## Why Data Integration Loves Triples

**Tags:** `where it's used` (blue), `knowledge graphs` (green), `integration` (orange)

- **The pain** — merging N conventional databases pairwise needs N(N-1)/2 schema mappings between them
- **The shortcut** — if each source maps once onto a shared triple vocabulary, N sources need only N mappings
- **The count** — at 10 sources that is 45 pairwise mappings versus 10 vocabulary mappings
- **The lineage** — Wikidata, DBpedia, and commercial knowledge graphs are triple stores at heart
- **The everyday echo** — search-engine info boxes and "people also ask" panels are assembled from such fact graphs
- **The query** — SPARQL asks pattern questions over triples: "?drink madeWith Espresso" finds every match

*Example (italic):* Adding an 11th data source to a pairwise-mapped warehouse means writing 10 new mappings; adding it to a shared-vocabulary triple store means writing 1.

**Key point:** Triples turn integration from an every-pair negotiation into a publish-and-stack operation — which is why knowledge graphs at web scale are built on them.

### Visualization (canvas `c3`, 720×300)

Grouped bar chart: number of mappings needed to integrate N sources, pairwise schema mapping vs shared triple vocabulary.

- **Title (bold 15px, `#1a5276`, top center):** "Mappings Needed to Integrate N Sources: Pairwise vs Shared Vocabulary".
- **Axes:** origin x=70, baseline y=245, plot width 590, plot height 175; x = sources with 12px `#444` group labels `["2", "4", "6", "8", "10"]`; y = mappings 0 to 50, gridlines `#e5e9ef` at 10/20/30/40 with 12px `#444` tick labels.
- **Pairwise bars (orange `#d95926`, 34px wide, left of each group center):** heights for mapping counts `[1, 6, 15, 28, 45]` (the exact values of N(N-1)/2), 11px count labels on top.
- **Vocabulary bars (green `#008300`, 34px wide, right of each group center):** heights for mapping counts `[2, 4, 6, 8, 10]`, 11px count labels on top.
- **Legend (12px, top left inside plot):** orange swatch "pairwise schema mappings", green swatch "map once to shared vocabulary".
- **Annotation (bold 13px orange `#d95926`, near the N=10 group, y=85):** "45 vs 10 — the gap widens with every new source".
- **Caption (12px `#444`, bottom right):** "mapping counts exact: N(N-1)/2 vs N".

## Triples Still Need a Shared Vocabulary

**Tags:** `common mistake` (red), `vocabulary` (orange)

- **The trap** — believing triples remove all agreement: the format is free, but the words are not
- **The mismatch** — Shop A prices drinks with `menu:costs`; Shop B uses `shop:price` for the same idea
- **The silent miss** — a query for `costs` over the merged 6-drink store returns only Shop A's 3 prices
- **No error** — nothing crashes or warns; the other 3 facts are simply invisible to the question asked
- **The fix** — agree on one predicate up front, or add a bridge triple declaring the two predicates equivalent
- **The lesson** — RDF moves schema agreement from table columns to vocabulary choice; it does not delete it

*Example (italic):* After stacking both shops' menus, "list every drink and its cost" comes back with 3 rows instead of 6 — and the dashboard quietly reports half a menu.

**Common mistake:** Treating "no fixed schema" as "no agreement needed." Two datasets merge physically the moment you concatenate them, but they only merge meaningfully when their predicates and URIs line up.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: merging with mismatched predicates (query finds 3 of 6 prices) vs merging after vocabulary alignment (finds 6 of 6).

- **Title (bold 15px, `#1a5276`, top center):** "Same Merge, Different Vocabulary: 3 of 6 Prices vs 6 of 6".
- **Row 1 (y=100), label 12px `#444` at x=20:** "mismatched"; blue `#2a78d6` rounded box at x=140 labeled "Shop A: 3 × menu:costs" (12px), stacked with a violet `#4a3aa7` box labeled "Shop B: 3 × shop:price"; 3px arrow to a `#6b7280` box at x=390 labeled "query: ?drink costs ?p"; arrow to a red `#e74c3c` box at x=590 labeled "3 results" with bold 12px red "✗ half the menu missing".
- **Row 2 (y=215), label:** "aligned"; the same blue and violet source boxes at x=140; 3px arrow to a green `#008300` box at x=390 labeled "bridge: price ≡ costs"; arrow to a green box at x=590 labeled "6 results" with bold 12px green "✓ full menu".
- **Box style:** 140–170px wide, 38px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(74,58,167,0.12)` / `rgba(231,76,60,0.12)` / `rgba(0,131,0,0.12)` / `rgba(107,114,128,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px magenta `#d55181`, centered near y=280):** "stacking merges the files; shared vocabulary merges the meaning".
- **Caption (12px `#444`, bottom right):** "drink counts illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); the 7 menu triples, prices (4.50 / 4.00 / 3.00 / 5.00), caffeine 75 mg, calories 190, and the 3-of-6 query results are invented and labeled illustrative; the mapping counts `[1, 6, 15, 28, 45]` vs `[2, 4, 6, 8, 10]` are the exact values of N(N-1)/2 vs N for N = 2/4/6/8/10.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
