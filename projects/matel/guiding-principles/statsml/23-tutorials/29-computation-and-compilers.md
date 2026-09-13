# Computation & Compilers

**Page type:** grid page (tutorials category grid: single flat 4-column nav-grid of cards with topic tags)
**HTML title tag:** Computation & Compilers

**Subtitle:** What computers can and can't compute, how patterns become machines, and how the code you write gets turned into something a machine — or a database — actually runs.

## Cards

Each card links to a topic page under `computation-compilers/`. The card shows a colored uppercase subcategory label (`.card-num`), a numbered title, a one-line description, and 2-4 topic tag pills. All cards sit in one flat `.nav-grid`; the colored labels carry the grouping.

| # | Category | Title | Link | Description | Topic tags |
|---|----------|-------|------|-------------|------------|
| 1 | COMPUTABILITY & COMPLEXITY | Turing Machines | [29-computation-and-compilers/01-turing-machines.md](29-computation-and-compilers/01-turing-machines.md) | A strip of paper, a pencil, and a short list of rules make the simplest possible computer — and it can compute anything any computer can. | tape & rules, simplest computer, universality |
| 2 | COMPUTABILITY & COMPLEXITY | The Halting Problem | [29-computation-and-compilers/02-the-halting-problem.md](29-computation-and-compilers/02-the-halting-problem.md) | No program can look at any other program and always predict whether it will finish or run forever — a ten-line prankster is the whole proof. | undecidability, self-reference, proof by contradiction |
| 3 | COMPUTABILITY & COMPLEXITY | P vs NP | [29-computation-and-compilers/03-p-vs-np.md](29-computation-and-compilers/03-p-vs-np.md) | Some questions are quick to check but, as far as anyone knows, brutally slow to solve — P vs NP asks whether that gap is real. | check vs solve, open problem, hard problems |
| 4 | LANGUAGES & AUTOMATA | Regular Expressions as Machines | [29-computation-and-compilers/04-regular-expressions-as-machines.md](29-computation-and-compilers/04-regular-expressions-as-machines.md) | Every regex is secretly a little machine — a row of states that reads one character at a time and either reaches the finish circle or falls off. | finite automata, states, one pass |
| 5 | LANGUAGES & AUTOMATA | Practical Regular Expressions | [29-computation-and-compilers/05-practical-regular-expressions.md](29-computation-and-compilers/05-practical-regular-expressions.md) | Groups mark the slice to keep, anchors pin where the pattern must sit, and the greedy star grabs more than you expect. | groups, anchors, greedy matching |
| 6 | LANGUAGES & AUTOMATA | Conway's Game of Life | [29-computation-and-compilers/06-conways-game-of-life.md](29-computation-and-compilers/06-conways-game-of-life.md) | Four tiny rules about lit windows and their neighbors produce blinking shapes, gliding spaceships, and even a working computer. | cellular automata, emergence, simple rules |
| 7 | HOW COMPILERS READ CODE | What a Compiler Does | [29-computation-and-compilers/07-what-a-compiler-does.md](29-computation-and-compilers/07-what-a-compiler-does.md) | A translator that reads the code you wrote as plain text and turns it, in five checkable stages, into instructions the machine can run. | five stages, translation, pipeline |
| 8 | HOW COMPILERS READ CODE | Lexing (lex) | [29-computation-and-compilers/08-lexing-lex.md](29-computation-and-compilers/08-lexing-lex.md) | Before a computer can understand anything you type, it chops the stream of raw characters into labeled words called tokens. | tokens, chopping text, lex |
| 9 | HOW COMPILERS READ CODE | Parsing (yacc) | [29-computation-and-compilers/09-parsing-yacc.md](29-computation-and-compilers/09-parsing-yacc.md) | A parser folds a flat list of tokens into a tree showing which pieces belong together — and yacc writes that parser for you from the grammar alone. | grammar rules, parse tree, yacc |
| 10 | HOW COMPILERS READ CODE | Abstract Syntax Trees | [29-computation-and-compilers/10-abstract-syntax-trees.md](29-computation-and-compilers/10-abstract-syntax-trees.md) | A formula or query redrawn as a tree — operations become branch points, and the tree's shape decides what happens first. | tree shape, order of operations, code as data |
| 11 | FROM MEANING TO MACHINE CODE | Interpreters vs Compilers | [29-computation-and-compilers/11-interpreters-vs-compilers.md](29-computation-and-compilers/11-interpreters-vs-compilers.md) | Translate line by line while the program runs, or translate the whole thing once up front — and a JIT translates mid-run, but only the parts that repeat. | line by line, ahead of time, JIT |
| 12 | FROM MEANING TO MACHINE CODE | Scoping & Symbol Tables | [29-computation-and-compilers/12-scoping-and-symbol-tables.md](29-computation-and-compilers/12-scoping-and-symbol-tables.md) | When a program says "x", scoping is the rule for deciding which x it means — and the symbol table is the compiler's address book for each region of code. | which x, address book, shadowing |
| 13 | FROM MEANING TO MACHINE CODE | Classic Optimizations | [29-computation-and-compilers/13-classic-optimizations.md](29-computation-and-compilers/13-classic-optimizations.md) | What -O2 actually does: rewrite your code into a faster version it can prove gives the same answer — fold constants, hoist loop work, delete dead code. | constant folding, loop hoisting, dead code |
| 14 | QUERIES AS PROGRAMS | How SQL Becomes Execution | [29-computation-and-compilers/14-how-sql-becomes-execution.md](29-computation-and-compilers/14-how-sql-becomes-execution.md) | You write what you want, never how — the database parses your sentence, plans the steps, and picks the cheapest way to run them, a compiler in disguise. | declarative, query plan, compiler in disguise |
| 15 | QUERIES AS PROGRAMS | Query Optimizers | [29-computation-and-compilers/15-query-optimizers.md](29-computation-and-compilers/15-query-optimizers.md) | The database's optimizer prices several routes to the same answer and quietly runs the cheapest one — your SQL never says which. | cost model, plan choice, cheapest route |

## Regeneration instructions

To rebuild the HTML from this spec:

- **Template:** tutorials category grid. Single page: h1, `.subtitle` paragraph, then one flat `.nav-grid` of `.nav-card` anchors (no h2 section headings).
- **Layout:** `.nav-grid` is CSS grid, `repeat(4, 1fr)`, 16px gap, 15px top margin; responsive: 3 columns below 1400px, 2 below 1100px, 1 below 600px.
- **Links:** the tables above link to the `.md` versions for markdown navigation; in the regenerated HTML, each card's `href` is the same path with an `.html` extension instead.
- **Card structure:** `<a class="nav-card" href="...">` containing `<div class="card-num">SUBCATEGORY LABEL</div>`, `<h3>N. Topic Title</h3>` (unpadded index number matching the 2-digit zero-padded file index), `<p>description</p>`, then `<div class="topics">` of `<span class="topic-tag">` pills.
- **Category label colors:** applied by a small script mapping `.card-num` text to color — "COMPUTABILITY & COMPLEXITY" `#2980b9`, "LANGUAGES & AUTOMATA" `#27ae60`, "HOW COMPILERS READ CODE" `#8e44ad`, "FROM MEANING TO MACHINE CODE" `#e67e22`, "QUERIES AS PROGRAMS" `#16a085`; the CSS default for `.card-num` is `#2980b9`, 0.75em bold, 4px bottom margin.
- **Card style:** background `#ffffff`, border `1px solid #d8d8d8`, radius 10px, padding 20px, shadow `0 2px 4px rgba(0,0,0,0.05)`, transition on border-color/transform; hover: border `#2980b9`, `translateY(-2px)`. h3 `#1a3a4a` 1em with 6px bottom margin; description `#555` 0.85em. Topic tags: background `#f0f0f0`, border `1px solid #ccc`, radius 4px, padding 2px 6px, 0.7em `#666`, in a flex-wrap row with 4px gap and 8px top margin.
- **Page style:** body system sans-serif, background `#f5f5f0`, text `#2a2a2a`, padding 40px, line-height 1.6; universal `* { margin:0; padding:0; box-sizing:border-box }` reset; h1 1.8em `#2980b9` with 10px bottom margin; subtitle `#666` 1.05em with 30px bottom margin. No nav bar, no back/home links.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22` (page accents here use `#2980b9`, `#27ae60`, `#8e44ad`, `#e67e22`, `#16a085`).
- **Canvases:** none on this page; any canvases elsewhere in this series use `window.devicePixelRatio` scaling.
