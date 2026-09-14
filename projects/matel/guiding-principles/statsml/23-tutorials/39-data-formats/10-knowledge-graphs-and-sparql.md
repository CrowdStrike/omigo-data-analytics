# Knowledge Graphs & SPARQL

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Knowledge Graphs & SPARQL

**Subtitle:** The semantic web's surviving descendants — store every fact as a tiny subject–predicate–object sentence, and questions become graph patterns you match against the web of facts

## A Retail Chain in Twelve Facts

**Tags:** `core idea` (blue), `triples` (green), `semantic web` (orange)

- **The chain** — three products, three suppliers, and two stores across three cities, all in one dataset
- **A triple** — every fact is one tiny sentence, subject → predicate → object: "EspressoBeans suppliedBy BeanCo"
- **Twelve of them** — 3 suppliedBy + 4 stockedAt + 3 basedIn + 2 locatedIn facts link into one web
- **Shared names** — Portland is one node, so BeanCo and Store #12 connect through it without anyone planning it
- **The heritage** — RDF triples and SPARQL are the early-2000s semantic web pieces that survived into industry

*Example (italic):* All twelve facts fit on a sticky note, yet together they answer questions no single fact contains — like which store depends on which city.

**Key point:** A knowledge graph is just facts stored as subject–predicate–object triples; the "graph" appears on its own once shared names make separate facts touch.

### Visualization (canvas `c1`, 720×300)

Node-edge diagram of the full retail graph: suppliers, products, and stores as three columns of rounded boxes, with suppliedBy and stockedAt arrows between them.

- **Title (bold 15px, `#1a5276`, top center):** "Twelve Facts, One Graph: Products, Suppliers, Stores".
- **Layout:** three columns of rounded boxes (130px wide, 38px tall, 8px radius), centered at x=105 (suppliers), x=330 (products), x=560 (stores); rows at y = 90, 165, 240.
- **Supplier nodes (violet border `#4a3aa7` 2px, fill `rgba(74,58,167,0.10)`), 12px `#2c3e50` name + 11px `#6b7280` city line:** "BeanCo / Portland" (105, 90), "FarmFresh / Salem" (105, 165), "PackRight / Tacoma" (105, 240) — the city line is the basedIn triple.
- **Product nodes (blue border `#2a78d6` 2px, fill `rgba(42,120,214,0.10)`):** "EspressoBeans" (330, 90), "OatMilk" (330, 165), "PaperCups" (330, 240).
- **Store nodes (green border `#008300` 2px, fill `rgba(0,131,0,0.10)`), name + 11px city line:** "Store #12 / Portland" (560, 110), "Store #7 / Salem" (560, 220) — the city line is the locatedIn triple.
- **suppliedBy edges (2px `#6b7280` arrows, product → supplier):** EspressoBeans→BeanCo, OatMilk→FarmFresh, PaperCups→PackRight; one 11px `#6b7280` label "suppliedBy" above the top edge.
- **stockedAt edges (2px `#6b7280` arrows, product → store):** EspressoBeans→Store #12, EspressoBeans→Store #7, OatMilk→Store #12, PaperCups→Store #7; one 11px `#6b7280` label "stockedAt" above the top edge.
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=285):** "any fact can point at anything — no fixed table shape".
- **Caption (12px `#444`, right-aligned near top right):** "retail data invented, illustrative".

## One SPARQL Query, Walked by Hand

**Tags:** `worked example` (blue), `SPARQL` (green), `multi-hop` (orange)

- **The question** — "which stores stock a product whose supplier is based in Portland?"
- **The query** — `SELECT ?store` over three triple patterns joined by shared variables:
  `?p suppliedBy ?s . ?s basedIn Portland . ?p stockedAt ?store`
- **Pattern 1** — `?s basedIn Portland` matches exactly one supplier in the graph: BeanCo
- **Pattern 2** — `?p suppliedBy BeanCo` then matches exactly one product: EspressoBeans
- **Pattern 3** — `EspressoBeans stockedAt ?store` matches two stores, so the result has two rows
- **The SQL pain** — the same question is a three-table join; every extra hop is another JOIN clause

*Example (italic):* The result table has exactly two rows — ?store = Store #12 and ?store = Store #7, both reached via BeanCo's EspressoBeans.

**Key point:** A SPARQL query is a small graph with holes in it; the answers are every way the holes can be filled so the pattern lies flat against the data.

### Visualization (canvas `c2`, 720×300)

The same node-edge graph with everything grayed out except the matched query path, plus a mini results table on the right.

- **Title (bold 15px, `#1a5276`, top center):** "Three Triple Patterns Filled Against the Graph".
- **Layout:** same three-column node layout as c1 but shifted left — suppliers x=90, products x=280, stores x=470; rows at y = 90, 165, 240; node boxes 120px wide, 36px tall.
- **Grayed elements:** FarmFresh, PackRight, OatMilk, PaperCups and their edges drawn in `#e5e9ef` borders with 11px `#6b7280` text — they matched no pattern.
- **Highlighted path:** BeanCo box border 3px violet `#4a3aa7` (its "Portland" city line underlined); edge EspressoBeans→BeanCo 3px orange `#d95926`; EspressoBeans box border 3px blue `#2a78d6`; edges EspressoBeans→Store #12 and EspressoBeans→Store #7 both 3px green `#008300`; both store boxes border 3px green.
- **Binding labels (bold 12px, colored to match):** "?supplier = BeanCo" (violet, under BeanCo), "?product = EspressoBeans" (blue, under EspressoBeans), "?store = #12, #7" (green, right of the stores).
- **Results table (right side, x = 575 to 705):** header "?store" bold 12px `#1a5276` on a 1px `#e5e9ef` ruled box, two 12px `#2c3e50` rows: "Store #12", "Store #7".
- **Annotation (bold 12px green `#008300`, near y=280 under the table):** "two rows out — every way the holes can be filled".
- **Caption (12px `#444`, bottom left):** "grayed facts matched no pattern".

## Where Graph-Shaped Questions Live

**Tags:** `where it's used` (blue), `entity resolution` (green), `data catalogs` (orange)

- **Knowledge panels** — the info box beside a web search result is a knowledge graph read out loud
- **Data catalogs** — "which dashboards depend on this table?" is a multi-hop lineage walk, not a lookup
- **Entity resolution** — merging "Bean Co." and "BeanCo Inc" into one node makes their facts add up
- **Recommendations** — "customers near Salem buy what Salem stores stock" chains three hops of facts
- **The tell** — if your question keeps saying "of the… of the…", it is graph-shaped

*Example (italic):* "Which cities depend on a Tacoma supplier?" is one more triple pattern in SPARQL, but a fourth JOIN and a schema-archaeology session in SQL.

**Key point:** Reach for a knowledge graph when the joins ARE the question — when the path between things matters more than the columns of any one table.

### Visualization (canvas `c3`, 720×300)

Grouped bar chart: lines of query needed as the question grows from 1 to 4 hops, SPARQL vs SQL.

- **Title (bold 15px, `#1a5276`, top center):** "Each Extra Hop: One More Triple Pattern vs One More JOIN".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; y = lines of query 0 to 30, gridlines `#e5e9ef` at 10 and 20 with 12px `#444` labels; x = four question sizes with bar pairs centered at x = 150, 290, 430, 570.
- **Category labels (12px `#444` under baseline):** "1 hop", "2 hops", "3 hops", "4 hops"; second line 11px `#6b7280`: "who supplies X", "stores stocking X", "stores w/ Portland supplier", "cities tied to Tacoma".
- **SPARQL bars (solid green `#008300`, 36px wide, left of each pair):** heights for values `[1, 2, 3, 4]` — one triple pattern per hop.
- **SQL bars (fill `rgba(42,120,214,0.35)`, 2px `#2a78d6` border, 36px wide, right of each pair):** heights for values `[4, 9, 16, 26]` — joins, aliases, and subqueries pile up.
- **Value labels:** bold 12px above each bar in its color.
- **Legend (12px, top right of plot):** green swatch "SPARQL patterns", blue swatch "SQL lines".
- **Annotation (bold 13px magenta `#d55181`, near x=380, y=70):** "the joins were the question all along".
- **Caption (12px `#444`, bottom right):** "line counts illustrative".

## Not Every Graph Database Is a Knowledge Graph

**Tags:** `common mistake` (red), `identifiers` (orange), `vocabulary` (blue)

- **The confusion** — storing nodes and edges is the easy part; a knowledge graph adds shared meaning
- **Identifiers** — "Bean Co." and "BeanCo Inc" as strings are two nodes; one shared URI makes them one
- **Vocabulary** — agreeing that suppliedBy means the same thing in every team's data is the hard part
- **The failure** — merge two datasets without shared IDs and the graph doubles instead of connecting
- **The payoff** — with shared identifiers, every new dataset quietly enriches old answers for free

*Example (italic):* A retailer loads a second product feed; without a shared supplier ID, BeanCo splits into two nodes and the Portland query silently returns half its answers.

**Common mistake:** Calling any node-and-edge store a knowledge graph. The graph shape is trivial — the shared identifiers and agreed vocabulary are what let facts from different sources snap together.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: merging two data feeds without shared identifiers (facts split) vs with a shared URI (facts accumulate).

- **Title (bold 15px, `#1a5276`, top center):** "Same Company, Two Spellings: Identifiers Do the Work".
- **Row 1 (y=95), label 12px `#444` at x=20:** "strings as names"; magenta `#d55181` rounded box at x=200 labeled "feed A: \"Bean Co.\"" (12px), magenta box at x=390 labeled "feed B: \"BeanCo Inc\"", no edge between them; bold 12px red `#e74c3c` text at x=560, two lines: "✗ facts split across" / "two nodes".
- **Row 2 (y=205), label:** "shared URI"; blue `#2a78d6` box at x=200 "feed A: \"Bean Co.\"", blue box at x=390 "feed B: \"BeanCo Inc\"", both with 3px arrows converging into a green `#008300` box at x=555 labeled ":supplier/beanco" with bold 12px green "✓ facts accumulate".
- **Box style:** 140–160px wide, 40px tall, 8px radius, fills `rgba(213,81,129,0.12)` / `rgba(42,120,214,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px orange `#d95926`, centered near y=272):** "the graph shape is easy — shared identifiers are the actual product".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all facts are the hardcoded retail graph above (no randomness) — 12 triples (3 suppliedBy, 4 stockedAt, 3 basedIn, 2 locatedIn), the worked query returns exactly 2 rows (Store #12, Store #7), and the c3 bar values are SPARQL `[1, 2, 3, 4]` vs SQL `[4, 9, 16, 26]`; company/store names and SQL line counts are invented and labeled illustrative.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
