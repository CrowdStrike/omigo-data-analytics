# Abstract Syntax Trees

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Abstract Syntax Trees

**Subtitle:** Before a computer can run a formula or a query, it redraws the text as a tree — operations become branch points, and the tree's shape decides what happens first

## A Receipt Formula Becomes a Tree

**Tags:** `core idea` (blue), `text to tree` (green), `shape is meaning` (orange)

- **The cell** — a coffee cart's spreadsheet computes each order as (price − discount) × quantity
- **The question** — the computer can't run raw text; it must first decide which operation goes first
- **The tree** — it redraws the formula as a tree: × at the top, − below it, the three names as leaves
- **Leaves and branches** — leaves hold values (price, discount, quantity); branch nodes hold operations
- **Shape is meaning** — the parentheses vanish; their whole job is now done by the tree's shape
- **The name** — this picture is an abstract syntax tree (AST): the formula's meaning, punctuation stripped

*Example (italic):* When the owner types (price − discount) × quantity into the cell, the spreadsheet quietly builds this three-level tree before computing anything.

**Key point:** An abstract syntax tree is the formula redrawn as a tree — operations become branch points, and "what runs first" becomes "what sits deeper".

### Visualization (canvas `c1`, 720×300)

Single-panel node-and-edge diagram: the formula text at the top, and below it the same formula as a three-level tree with operator circles and value leaves.

- **Title (bold 15px, `#1a5276`, top center):** "(price − discount) × quantity — Redrawn as a Tree".
- **Formula strip:** 13px `#2c3e50` monospace text "(price − discount) × quantity" centered at (360, 52).
- **Operator nodes (circles, radius 18, fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border, bold 15px `#1a5276` symbol centered):** "×" at (360, 100); "−" at (240, 168).
- **Leaf nodes (rounded rects 96×28, radius 6, fill `rgba(0,131,0,0.12)`, 2px `#008300` border, bold 12px `#008300` word centered):** "quantity" centered at (480, 168); "price" at (150, 238); "discount" at (330, 238).
- **Edges:** 2px `#6b7280` straight lines from each parent circle's bottom edge to each child's top edge (× → −, × → quantity, − → price, − → discount).
- **Order hints:** 11px `#6b7280` italic labels "runs last" to the right of the × node at (400, 96) and "runs first" to the left of the − node at (150, 164).
- **Annotation (bold 12px orange `#d95926`, right side, two lines at (545, 235) and (545, 251)):** "parentheses are gone —" / "the shape does their job now".

## Climbing the Tree: $5, $1, 3 Cups

**Tags:** `worked example` (blue), `bottom-up` (green)

- **The order** — 3 lattes at $5 each with a $1-per-cup loyalty discount: price 5, discount 1, quantity 3
- **Start at the leaves** — replace each name with its value: price → 5, discount → 1, quantity → 3
- **Work upward** — the − node computes 5 − 1 = 4; only after that may the × node run
- **Finish at the root** — the × node computes 4 × 3 = 12; the root's value is the answer: $12
- **One rule** — a node may run only when both children have answers: children first, parent after

*Example (italic):* Reading the text left to right suggests starting with 5, but the tree forces 5 − 1 = 4 before the multiply — the root then gives 4 × 3 = 12.

**Key point:** Evaluation is a climb — leaves first, root last: (5 − 1) × 3 = 12, and the tree's shape is what guaranteed the subtraction ran before the multiplication.

### Visualization (canvas `c2`, 720×300)

The same tree as c1, but leaves now carry their numbers and computed values bubble up the branches, with the two evaluation steps labeled in order.

- **Title (bold 15px, `#1a5276`, top center):** "Leaves First, Root Last: (5 − 1) × 3 = 12".
- **Tree geometry:** identical to c1 — "×" circle at (360, 100), "−" circle at (240, 168), leaves "quantity = 3" at (480, 168), "price = 5" at (150, 238), "discount = 1" at (330, 238); same node styles and 2px `#6b7280` edges (leaf rects widened to 108×28 to fit the "= value").
- **Value badges (bold 13px white on rounded 26×20 pills):** green `#008300` pill labeled "4" just right of the − node at (272, 148); green pill labeled "12" just right of the × node at (392, 80).
- **Step labels (bold 12px aqua `#199e70`):** "step 1: 5 − 1 = 4" at (85, 130); "step 2: 4 × 3 = 12" at (455, 78).
- **Flow arrows:** short 2px `#199e70` arrows alongside the edges pointing upward (leaves toward −, − toward ×) to show values flowing up.
- **Annotation (bold 13px green `#008300`, at (555, 130)):** "root answer: $12".
- **Caption (12px `#444`, bottom right):** "illustrative — 3 lattes at $5 with a $1 loyalty discount".

## Every Query Engine Draws This First

**Tags:** `where it's used` (blue), `query engines` (green), `compilers` (orange)

- **Databases** — a query like "orders where price × quantity > 50" becomes an AST before any row is read
- **Compilers** — nearly every language implementation builds an AST from source code early on
- **Optimizers** — engines speed queries up by rewriting the tree, never by editing your text
- **Tools** — formatters, linters, and autocomplete all walk the AST, not the raw characters
- **Without it** — editing code with find-and-replace on text mangles meaning; the tree keeps it intact

*Example (italic):* Asked for orders over $50, the engine builds a > node with the price × quantity subtree on one side and the number 50 on the other.

**Key point:** The AST is the shared middle step — text is for humans, execution is for machines, and the tree is where the two meet.

### Visualization (canvas `c3`, 720×300)

Horizontal four-stage pipeline: the query text flows through tokens into an AST (drawn as a mini tree) and then to execution, with the tree stage highlighted as the one everything depends on.

- **Title (bold 15px, `#1a5276`, top center):** "What Happens Before a Single Row Is Read".
- **Stage boxes (rounded rects 145×74 at y=90, radius 8, fill `#f8f9fa`, 2px `#6b7280` border), left x = 25, 200, 375, 550:** bold 12px `#1a5276` stage name centered near each box top; 11px `#2c3e50` content below it.
  - Box 1 "TEXT": monospace 11px, two lines: "price × quantity" / "> 50".
  - Box 2 "TOKENS": five small pills (11px `#2c3e50` on `rgba(42,120,214,0.12)`) reading "price", "×", "quantity", ">", "50", wrapped on two rows.
  - Box 3 "AST": mini tree — bold 12px `#1a5276` symbols ">" at (447, 118), "×" at (418, 140), "50" at (478, 140), 11px "price" at (398, 160), "qty" at (442, 160); 1.5px `#6b7280` edges between them.
  - Box 4 "EXECUTE": 11px, two lines: "check each" / "order row".
- **AST highlight:** box 3 border restyled 2.5px blue `#2a78d6` with a dashed (dash 4/3) blue outer ring 6px around it.
- **Arrows:** 2px `#6b7280` arrows with solid arrowheads between consecutive boxes at y=127; 11px `#6b7280` labels under each arrow: "tokenize", "parse", "run".
- **Annotation (bold 12px violet `#4a3aa7`, centered at (360, 215)):** "the tree exists before a single order row is read".
- **Caption (12px `#444`, centered at (360, 250)):** "same pipeline in databases, compilers, spreadsheets — illustrative".

## Drop the Parentheses, Change the Tree

**Tags:** `common mistake` (red), `precedence` (orange)

- **The typo** — the owner retypes the cell as price − discount × quantity, dropping the parentheses
- **New tree** — precedence rules push × deeper than −, so the new tree multiplies first
- **New answer** — 1 × 3 = 3 runs first, then 5 − 3 = 2: the order rings up as $2, not $12
- **Same words** — both formulas use the same five symbols; only the tree's shape differs
- **The confusion** — people think the text is the program; the machine only ever runs the tree

*Example (italic):* One missing pair of parentheses turned a $12 order into a $2 order — the text barely changed, but the tree changed completely.

**Common mistake:** Reading formulas left to right and assuming the machine does too — the machine follows the tree's shape, and when parentheses are absent, precedence rules decide that shape for you.

### Visualization (canvas `c4`, 720×300)

Two trees side by side built from the same five symbols: the parenthesized version (× on top, answer 12) versus the bare version (− on top, answer 2), separated by a dashed divider.

- **Title (bold 15px, `#1a5276`, top center):** "price − discount × quantity: Same Symbols, Two Trees".
- **Divider:** vertical dashed (dash 4/3) `#e5e9ef` 2px line at x=360 from y=48 to y=282.
- **Panel headers (12px `#6b7280`, centered):** "with parentheses" at (190, 62); "parentheses dropped" at (530, 62).
- **Left tree ((5 − 1) × 3):** operator circles (radius 16, fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border, bold 14px `#1a5276`): "×" at (190, 95), "−" at (120, 160); leaf pills (rounded rects 44×24, fill `rgba(0,131,0,0.12)`, 2px `#008300` border, bold 12px `#008300`): "3" at (260, 160), "5" at (75, 225), "1" at (165, 225); 2px `#6b7280` edges.
- **Left result (bold 13px green `#008300`, centered at (190, 268)):** "(5 − 1) × 3 = 12".
- **Right tree (5 − (1 × 3)):** same styles — "−" circle at (530, 95), "×" circle at (600, 160); leaves "5" at (460, 160), "1" at (555, 225), "3" at (645, 225); 2px `#6b7280` edges; the × circle's border restyled orange `#d95926` to flag that it now runs first.
- **Right result (bold 13px orange `#d95926`, centered at (530, 268)):** "5 − (1 × 3) = 2".
- **Annotation (bold 13px magenta `#d55181`, centered between panels, two lines at (360, 120) and (360, 136)):** "same five symbols —" / "$12 vs $2".
- **Caption (12px `#444`, bottom right):** "illustrative — the $2 bill undercharges by $10".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Drawing helpers:** small functions for a labeled circle node, a labeled rounded-rect leaf, an edge line between node centers (trimmed to node borders), and a 2px arrow with a filled triangular head — reused across c1–c4.
- **Data:** every node position, label, and number is the hardcoded literal given above (no randomness anywhere); the worked-example numbers (5, 1, 3, 4, 12, 2, 50) must appear identically in text and charts.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
