# Prefix, Infix & Postfix

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Prefix, Infix & Postfix

**Subtitle:** One expression tree read in three depth-first orders gives three notations — the operator speaks before, between, or after its operands, and all three compute the same answer

## One Tree, Three Reading Orders

**Tags:** `core idea` (blue), `expression tree` (green), `tree walks` (orange)

- **The tree** — (3 + 4) × 5 stored as a tree: × at the root, + below holding 3 and 4, and 5 on the right
- **The walk** — a depth-first walk visits every node once; the only choice is when a node says its symbol
- **In-order** — say the left child, yourself, then the right child: "3 + 4 × 5" — that is infix
- **Pre-order** — say yourself first, then both children: "× + 3 4 5" — that is prefix
- **Post-order** — say both children first, yourself last: "3 4 + 5 ×" — that is postfix
- **One value** — all three strings describe the same tree, and that tree computes 35

*Example (italic):* A parent node can speak before, between, or after its two children — that single choice is the whole difference between prefix, infix, and postfix.

**Key point:** Prefix, infix, and postfix are not three kinds of math — they are three visit orders of the same expression tree.

### Visualization (canvas `c1`, 720×300)

Three copies of the same five-node expression tree side by side, each with numbered visit-order badges for one walk, and the resulting string under each copy.

- **Title (bold 15px, `#1a5276`, top center):** "The Tree of (3 + 4) × 5, Visited Three Ways".
- **Tree shape (shared helper `drawExprTree`):** for a tree centered at `cx` with row ys `[95, 155, 215]` and node radius 14: × at (cx, 95), + at (cx−55, 155), 5 at (cx+55, 155), 3 at (cx−90, 215), 4 at (cx−20, 215); edges ×–+, ×–5, +–3, +–4 in 1.5px `#6b7280`; nodes white fill, 2px `#1a5276` stroke, bold 13px `#1a5276` symbol centered.
- **Visit badges:** filled circle radius 8 at each node's top-right (node x + 17, node y − 15), white bold 11px number inside; badge order arrays given as `[×, +, 3, 4, 5]`.
- **Three copies:** centers cx = 145, 380, 615; badge numbers `[1, 2, 3, 4, 5]` in violet `#4a3aa7` (pre-order), `[4, 2, 1, 3, 5]` in blue `#2a78d6` (in-order), `[5, 3, 1, 2, 4]` in green `#008300` (post-order).
- **String labels (bold 12px, centered at y=262, matching badge colors):** "pre-order: × + 3 4 5" at x=120, "in-order: 3 + 4 × 5" at x=355, "post-order: 3 4 + 5 ×" at x=590.
- **Annotation (bold 12px orange `#d95926`, centered at x=360, y=48):** "same tree — only when each node speaks changes".
- **Caption (12px `#444`, bottom right):** "badges show visit order 1–5".

## Walking the Tree, Then Running the Stack

**Tags:** `worked example` (blue), `stack evaluation` (green)

- **Pre-order by hand** — root ×, then the + subtree as +, 3, 4, then 5: written out, "× + 3 4 5"
- **In-order by hand** — left subtree as 3, +, 4, then ×, then 5: "3 + 4 × 5" — parentheses keep it honest
- **Post-order by hand** — 3, then 4, then +, then 5, then ×: "3 4 + 5 ×"
- **Stack run starts** — read "3 4 + 5 ×" left to right: push 3, push 4; see + — pop 4 and 3, push 7
- **Stack run ends** — push 5; see × — pop 5 and 7, push 35; one number remains: the answer
- **The rule** — numbers get pushed; every operator pops two, computes, and pushes one result back

*Example (italic):* Reading "3 4 + 5 ×" takes five steps and ends with exactly one number on the stack: 35.

**Key point:** To evaluate postfix, push numbers and let each operator pop two and push one — no parentheses and no precedence table, just a stack.

### Visualization (canvas `c2`, 720×300)

Five columns, one per token of "3 4 + 5 ×", each showing the token being read, the stack contents after the step, and the action taken; arrows link the steps left to right.

- **Title (bold 15px, `#1a5276`, top center):** "Evaluating \"3 4 + 5 ×\" With a Stack, Step by Step".
- **Columns:** centers x = `[95, 225, 355, 485, 615]`; tokens `['3', '4', '+', '5', '×']`; operator steps are columns 3 and 5.
- **Token line (bold 16px, centered at y=66):** "read 3", "read 4", "read +", "read 5", "read ×" — blue `#2a78d6` for pushes, orange `#d95926` for operator steps.
- **Stacks after each step (hardcoded):** `['3']`, `['3','4']`, `['7']`, `['7','5']`, `['35']` — drawn bottom-up as boxes 64×30 above a baseline at y=230, fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border, bold 13px `#1a5276` value centered; a 2px `#999` stack-floor line under each column (6px wider than the box on each side).
- **Action labels (11px, centered at y=252):** "push 3", "push 4", "pop 4, 3 → push 7", "push 5", "pop 5, 7 → push 35" — `#6b7280` for pushes, orange `#d95926` for operator steps.
- **Step arrows:** 1.5px `#6b7280` horizontal arrows at y=150 between neighboring columns (from col x+42 to next col x−44) with small solid arrowheads.
- **Annotation (bold 13px green `#008300`, centered at x=615, y=100):** "one number left: 35".
- **Caption (12px `#444`, bottom right):** "numbers push; an operator pops two and pushes one".

## Why Calculators and Compilers Pick Postfix

**Tags:** `where it's used` (blue), `stack machines` (green), `children first` (orange)

- **No parentheses** — postfix (reverse Polish) never needs brackets or precedence: symbol order is the plan
- **Calculators** — RPN calculators and stack machines run postfix directly with the push-pop loop
- **Compilers** — a compiler parses infix "(3 + 4) × 5" into the tree, then emits postfix-style stack code
- **Children first** — post-order is the natural order for children-before-parent jobs like folder sizes
- **Parent first** — pre-order is how you copy or serialize a tree: make the parent, then its children

*Example (italic):* To report a folder's total size you must total its subfolders first — that is a post-order walk of the directory tree.

**Key point:** Postfix is how machines like to compute, pre-order is how you rebuild a tree, and post-order is the shape of every children-before-parent job.

### Visualization (canvas `c3`, 720×300)

A small directory tree with file sizes, each node carrying a green post-order visit badge, showing that every folder's total is computed only after its children.

- **Title (bold 15px, `#1a5276`, top center):** "Folder Sizes Are a Post-Order Walk: Children Before Parent".
- **Nodes (boxes 104×32 centered at the coordinates, label + size inside, bold 11px `#1a5276`):**
  - `project/ 14 MB` at (360, 85), visit 7 — folder
  - `docs/ 5 MB` at (215, 155), visit 3 — folder
  - `img/ 9 MB` at (505, 155), visit 6 — folder
  - `a.txt 2 MB` at (140, 225), visit 1 — file
  - `b.txt 3 MB` at (290, 225), visit 2 — file
  - `photo.jpg 8 MB` at (430, 225), visit 4 — file
  - `logo.png 1 MB` at (580, 225), visit 5 — file
- **Node styles:** folders fill `rgba(42,120,214,0.15)` with 2px `#2a78d6` border; files fill `rgba(25,158,112,0.15)` with 2px `#199e70` border.
- **Edges:** 1.5px `#6b7280` lines project→docs, project→img, docs→a.txt, docs→b.txt, img→photo.jpg, img→logo.png (drawn from box bottom to box top, ±16px vertical offsets).
- **Visit badges:** green `#008300` filled circles radius 8 at each box's top-right corner (box x + 55, box y − 17), white bold 11px visit number.
- **Annotation (bold 12px green `#008300`, left-aligned at x=40, y=66 and y=82, two lines):** "docs/ = 2 + 3, img/ = 8 + 1," / "then project/ = 5 + 9 = 14".
- **Caption (12px `#444`, bottom right):** "green badges: post-order visit 1–7 — sizes illustrative".

## Same Math, Different Word Order

**Tags:** `common mistake` (red), `ambiguity` (orange)

- **The names** — pre, in, and post say where the operator sits: before, between, or after its operands
- **Not new math** — "× + 3 4 5", "(3 + 4) × 5", and "3 4 + 5 ×" all compute the same 35
- **The trap** — infix "3 + 4 × 5" is ambiguous until precedence rules pick a tree: 23, not 35
- **Never ambiguous** — prefix and postfix strings each map to exactly one tree, with no brackets at all
- **Brackets are patches** — parentheses exist only to force infix back into the tree you actually meant

*Example (italic):* The postfix string "3 4 + 5 ×" gives 35 to every reader, but "3 + 4 × 5" gives 23 to any calculator that honors precedence.

**Common mistake:** Thinking the three notations compute different things. Only infix can be misread — that is what parentheses fix; prefix and postfix are unambiguous by construction.

### Visualization (canvas `c4`, 720×300)

Three mini expression trees: the two possible readings of the infix string "3 + 4 × 5" (giving 35 and 23) and the single tree of the postfix string (always 35).

- **Title (bold 15px, `#1a5276`, top center):** "\"3 + 4 × 5\" Has Two Trees — \"3 4 + 5 ×\" Has One".
- **Mini-tree geometry (helper `miniTree`):** rows at y = 100 (root), 155 (middle), 210 (leaves); node radius 13; white fill, 2px stroke in the tree's color, bold 12px `#1a5276` symbols; edges 1.5px `#6b7280`. Left-deep variant "(a op2 b) op1 c": root at cx, sub-op at cx−50, c at cx+50, a at cx−82, b at cx−18. Right-deep variant "a op1 (b op2 c)": root at cx, a at cx−50, sub-op at cx+50, b at cx+18, c at cx+82.
- **Three trees:** left-deep ×/+ tree at cx=135 stroked blue `#2a78d6`; right-deep +/× tree at cx=370 stroked orange `#d95926`; left-deep ×/+ tree at cx=595 stroked green `#008300`. All use symbols a=3, b=4, c=5.
- **Column headers (bold 12px, centered at y=62, matching tree colors):** "what we meant: (3 + 4) × 5" at x=135, "precedence reads: 3 + (4 × 5)" at x=370, "postfix: 3 4 + 5 ×" at x=595.
- **Results (bold 14px, centered at y=248, matching colors):** "= 35" at x=135, "= 23" at x=370, "= 35, always" at x=595.
- **Annotation (bold 12px orange `#d95926`, centered at x=360, y=282):** "one infix string, two possible trees — postfix maps to exactly one".
- **Caption (12px `#444`, right-aligned at y=44):** "parentheses only exist to pick the left tree".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates; all charts pushed into a `__charts` array of functions, drawn once on load and redrawn on a 150ms-debounced window resize.
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all tree shapes, visit-order arrays, stack states, folder sizes, and results are the hardcoded values above (no randomness); the three walk strings, the five stack steps ending in 35, the 23-vs-35 ambiguity, and the 2+3/8+1/5+9=14 folder totals in the text must match the charts exactly.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
