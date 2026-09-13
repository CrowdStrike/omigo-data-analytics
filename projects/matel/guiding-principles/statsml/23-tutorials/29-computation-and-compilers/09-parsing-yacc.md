# Parsing (yacc)

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Parsing (yacc)

**Subtitle:** A parser takes a flat list of tokens and, following a grammar's rules, folds them into a tree that shows which pieces belong together — and yacc writes that parser for you from the grammar alone

## A Calculator Reads "3 + 4 × 2"

**Tags:** `core idea` (blue), `tokens to tree` (green), `grammar` (orange)

- **The input** — a pocket calculator receives the keystrokes "3 + 4 × 2" as five tokens: 3, +, 4, ×, 2
- **Flat is blind** — the token list alone never says whether + or × goes first; read left to right it gives 14
- **The grammar** — rules: expr is expr + term, term is term × number, term is number, expr is term
- **The tree** — the rules fold the tokens into a tree: + sits at the top, the 4 × 2 piece hangs below it
- **The answer** — evaluate bottom-up: 4 × 2 = 8, then 3 + 8 = 11 — the grouping was the meaning

*Example (italic):* The same five tokens could mean 14 or 11; the grammar's tree picks 11 by making × a deeper branch than +.

**Key point:** Parsing turns a flat token list into a tree by applying grammar rules — the tree, not the token order, carries the meaning.

### Visualization (canvas `c1`, 720×300)

Two-layer diagram: the flat token strip across the top, connector lines down to the parse tree the grammar builds from it, tree evaluated bottom-up to 11.

- **Title (bold 15px, `#1a5276`, top center):** "Five Flat Tokens Fold into One Tree".
- **Token strip (top row, y=48):** five rounded boxes 58×30 with 13px bold `#2c3e50` centered labels "3", "+", "4", "×", "2" at box left-edges x = `[160, 254, 348, 442, 536]`; fill `rgba(42,120,214,0.12)`, 1.5px `#2a78d6` border; 12px `#6b7280` label "tokens (flat, no grouping yet)" at x=160, y=36.
- **Tree nodes (circles r=17, 2px border, 14px bold centered labels):** root "+" at (360, 150) blue `#2a78d6`; leaf "3" at (255, 210) `#6b7280` border; node "×" at (465, 210) green `#008300`; leaf "4" at (410, 265) `#6b7280`; leaf "2" at (520, 265) `#6b7280`.
- **Edges:** 2px `#94a3b8` lines root→3, root→×, ×→4, ×→2, drawn circle-edge to circle-edge.
- **Dotted connectors:** 1px dashed (dash 3/3) `#c8d2dc` lines from each token box bottom to its matching tree node top.
- **Value tags (11px `#008300`, right of node):** "= 8" beside the × node, "= 11" beside the root +.
- **Annotation (bold 12px orange `#d95926`, two lines, near x=105, y=160):** "× sits deeper," / "so 4 × 2 goes first".
- **Caption (12px `#444`, bottom right):** "one expression, drawn as tokens then as the grammar's tree".

## Folding the Tokens One Rule at a Time

**Tags:** `worked example` (blue), `shift & fold` (green)

- **Two moves** — a yacc parser only ever does two things: shift (take the next token) or fold a rule
- **The stack** — shifted tokens pile onto a stack; a fold replaces the top pieces with the rule's name
- **Steps 1–2** — shift 3, then fold it into E (a finished expression piece): stack = E
- **Steps 3–4** — shift +, then shift 4: stack = E + 4
- **The peek** — the next token is ×, which binds tighter, so the parser shifts × and 2 instead of folding now
- **Steps 7–8** — fold 4 × 2 into T (worth 8), then fold E + T into E (worth 11): one piece left, done

*Example (italic):* Eight small moves — five shifts and three folds — and the whole expression is a single E on the stack, worth 11.

**Key point:** The parser never plans further than one token ahead — shift, shift, fold by a grammar rule — and the tree assembles itself bottom-up.

### Visualization (canvas `c2`, 720×300)

Step-ladder trace: eight rows, one per parser move, each showing the move name, the stack as small boxes, and the not-yet-read input fading out on the right.

- **Title (bold 15px, `#1a5276`, top center):** "Shift & Fold: Eight Moves for 3 + 4 × 2".
- **Column heads (11px `#6b7280`, y=52):** "move" at x=20, "stack" at x=130, "still to read" at x=520.
- **Rows at y = `[78, 104, 130, 156, 182, 208, 234, 260]`, move label 12px at x=20, shifts `#2c3e50`, folds bold green `#008300`:**
  - "1 shift 3" — stack boxes `["3"]`; remaining "+ 4 × 2"
  - "2 fold" — `["E"]`; remaining "+ 4 × 2"
  - "3 shift +" — `["E", "+"]`; remaining "4 × 2"
  - "4 shift 4" — `["E", "+", "4"]`; remaining "× 2"
  - "5 shift ×" — `["E", "+", "4", "×"]`; remaining "2"
  - "6 shift 2" — `["E", "+", "4", "×", "2"]`; remaining "(empty)"
  - "7 fold ×" — `["E", "+", "T"]`; remaining "(empty)"
  - "8 fold +" — `["E"]`; remaining "(empty)"
- **Stack box style:** 40×20 rounded boxes starting at x=130, 46px apart; 12px bold centered labels; token boxes fill `rgba(42,120,214,0.12)` border `#2a78d6`; folded pieces E and T fill `rgba(0,131,0,0.15)` border `#008300`.
- **Remaining-input text:** 12px `#6b7280` at x=520.
- **Value tags (11px `#008300`):** "T = 8" right of row 7's T box; "E = 11" right of row 8's E box.
- **Row highlight:** rows 7 and 8 get a full-width `rgba(0,131,0,0.06)` background band.
- **Annotation (bold 12px green `#008300`, bottom left, x=20, y=290):** "fold = one grammar rule applied".
- **Footnote (12px `#6b7280`, bottom right, right-aligned at w-16, y=290):** "number→term→expr folds compressed for readability".

## Where Parsers Earn Their Keep

**Tags:** `where it's used` (blue), `yacc` (orange), `grammar first` (green)

- **yacc** — you write the grammar rules in a `.y` file; yacc generates the code that does every shift and fold
- **Compilers** — every language you type (Python, SQL, JSON) runs through a lexer-then-parser front end
- **SQL engines** — `SELECT price * qty FROM orders` becomes a tree before the database plans anything
- **Config & logs** — JSON readers and log parsers are the same machine: grammar in, tree out
- **Error messages** — "syntax error near ×" is the parser failing to find any rule that fits its stack
- **Hand-rolled pain** — without a generator, precedence bugs like 14-instead-of-11 creep into hand-written parsers

*Example (italic):* One grammar line saying "× binds tighter than +" fixes every expression the calculator will ever read — no if-chains to patch.

**Key point:** yacc's bargain: describe the language once as a correct, conflict-free grammar, and the generated parser never mis-groups any input in that language.

### Visualization (canvas `c3`, 720×300)

Left-to-right pipeline: raw text enters a lexer, tokens flow to a yacc-built parser holding the grammar, a tree comes out and an evaluator turns it into 11.

- **Title (bold 15px, `#1a5276`, top center):** "The Front End of Every Compiler".
- **Stage boxes (rounded, 2px borders, centered on y=155):** text box `"3 + 4 × 2"` (90×46, `#6b7280` border, 13px `#2c3e50`) at x=25; "LEXER" (86×46, `#2a78d6` border, bold 13px blue) at x=150; token strip of five 24×22 mini boxes labeled "3 + 4 × 2" (11px, fill `rgba(42,120,214,0.12)`) starting x=270, 27px apart; "PARSER (yacc)" (110×46, `#d95926` border, bold 13px orange) at x=430; mini tree (root "+" over leaves "3" and "×(4,2)", r=10 circles, 11px labels, green `#008300` edges) centered x=595, y=150; result circle "11" (r=20, bold 15px, green fill `rgba(0,131,0,0.15)`) at x=680.
- **Arrows:** 2px `#94a3b8` arrows with small solid heads between consecutive stages along y=155.
- **Stage captions (11px `#6b7280`, y=215, centered under each stage):** "raw text", "splits", "tokens", "groups", "tree", "answer".
- **Grammar card:** small rounded box (150×54, dashed `#d95926` border, fill `rgba(217,89,38,0.06)`) at x=410, y=52 listing 11px `#2c3e50` mono lines "E : E + T | T", "T : T × NUM", "T : NUM"; dashed 1px orange connector down to the PARSER box.
- **Annotation (bold 12px violet `#4a3aa7`, two lines, near x=180, y=70):** "the grammar lives here —" / "yacc turns it into code".
- **Caption (12px `#444`, bottom right):** "illustrative — the same pipeline sits inside every compiler and database".

## The Tokens Don't Change — the Tree Does

**Tags:** `common mistake` (red), `grouping` (orange)

- **Same five tokens** — 3, +, 4, ×, 2 in the same order feed both trees below; not one token moved
- **Left-to-right tree** — grouping (3 + 4) first pushes + to the bottom: (3 + 4) × 2 = 14
- **Grammar's tree** — making × bind tighter puts it deeper: 3 + (4 × 2) = 11
- **The mistake** — assuming tokenizing settled the meaning; the lexer only splits, it never groups
- **One-line fix** — yacc precedence declarations (`%left '+'` above `%left '*'`) pick the right tree

*Example (italic):* Two engineers read the same token list and shipped calculators answering 14 and 11 — the difference was one grammar rule, not the input.

**Common mistake:** Treating the token list as the meaning. Tokenizing splits text into pieces; only parsing decides which pieces belong together — and that grouping is the answer.

### Visualization (canvas `c4`, 720×300)

Side-by-side parse trees built from the identical token list: the left-to-right grouping evaluating to 14, the grammar's precedence grouping evaluating to 11.

- **Title (bold 15px, `#1a5276`, top center):** "One Token List, Two Possible Trees".
- **Shared token strip (y=48, centered):** five 44×24 rounded boxes "3", "+", "4", "×", "2" starting x=250, 48px apart; fill `rgba(42,120,214,0.12)`, border `#2a78d6`, 12px bold labels.
- **Left tree (wrong grouping), heading bold 13px red `#e74c3c` at x=185 centered, y=100: "left-to-right → 14":** root "×" circle at (185, 140) red `#e74c3c` border; node "+" at (115, 200) `#6b7280`; leaf "2" at (255, 200); leaves "3" at (75, 258) and "4" at (155, 258); all non-root circles r=15 with `#6b7280` borders, 13px bold labels; 2px `#94a3b8` edges; 11px red tags "= 7" beside the + node and "= 14" beside the root.
- **Right tree (grammar grouping), heading bold 13px green `#008300` at x=535 centered, y=100: "grammar precedence → 11":** root "+" at (535, 140) green `#008300` border; leaf "3" at (465, 200); node "×" at (605, 200) green; leaves "4" at (565, 258) and "2" at (645, 258); same circle and edge style; 11px green tags "= 8" beside the × node and "= 11" beside the root.
- **Divider:** 1px dashed `#e5e9ef` vertical line at x=360 from y=95 to y=280.
- **Annotation (bold 13px magenta `#d55181`, centered at x=360, y=292):** "same tokens, different tree — 14 vs 11".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** every token label, stack state, tree node position, and value tag is the hardcoded literal above (no randomness); the arithmetic is exact (4 × 2 = 8, 3 + 8 = 11, (3 + 4) × 2 = 14) so text and chart numbers must stay in lockstep.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
