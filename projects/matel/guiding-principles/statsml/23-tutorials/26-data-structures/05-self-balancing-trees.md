# Self-Balancing Trees

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Self-Balancing Trees

**Subtitle:** A search tree fed sorted data collapses into a slow chain — self-balancing trees rotate nodes as they arrive so the height, and every lookup, stays a few steps

## The Contact List That Became a Queue

**Tags:** `core idea` (blue), `worst case` (green), `rotations` (orange)

- **The promise** — a binary search tree halves the search at every node: smaller names left, larger right
- **The catch** — the shape depends on arrival order, and sorted arrivals are the worst order possible
- **The chain** — inserting Ana, Ben, Cara, Dev, Ed, Fay, Gil in order hangs each name off the last one
- **The damage** — that "tree" is a 7-deep chain, and finding Gil now walks all 7 names: a plain scan
- **The fix** — self-balancing trees (AVL, red-black) rotate nodes as they arrive to flatten the lean
- **The payoff** — after rotations those 7 sorted inserts end up 3 levels tall with Dev at the root

*Example (italic):* Alphabetical inserts are exactly what a contact-sync job produces — the naive tree quietly turns into a linked list on the most natural input there is.

**Key point:** A plain BST is only fast if arrivals happen to be shuffled; self-balancing trees make the log-n height a guarantee instead of a hope.

### Visualization (canvas `c1`, 720×300)

Side-by-side node diagrams: the 7-name chain produced by sorted inserts versus the balanced tree a self-balancing insert produces from the same names.

- **Title (bold 15px, `#1a5276`, top center):** "Same 7 Names, Two Shapes: a Chain of 7 or a Tree of Height 3".
- **Node style:** rounded circles radius 15, white fill, 2px stroke, bold 11px name centered inside.
- **Left half (chain):** nodes Ana→Ben→Cara→Dev→Ed→Fay→Gil stroked `#d95926`, drawn as a descending staircase at (95, 70), (125, 100), (155, 130), (185, 160), (215, 190), (245, 220), (275, 250); 1.5px `#6b7280` arrows between consecutive nodes; bold 12px `#d95926` label "height 7 — a list in disguise" at (150, 288).
- **Right half (balanced):** nodes stroked `#008300`: Dev at (530, 85); Ben (445, 155), Fay (615, 155); Ana (405, 225), Cara (485, 225), Ed (575, 225), Gil (655, 225); 1.5px `#6b7280` edges parent→child; bold 12px `#008300` label "height 3 — same names" at (530, 288).
- **Divider:** 1px `#e5e9ef` vertical line at x=350 from y=55 to y=270.
- **Annotation (bold 13px `#1a5276`, near x=360, y=45):** "arrival order decides the left shape — rotations force the right one".
- **Caption:** none (labels carry it).

## Watching One Rotation Fix the Lean

**Tags:** `worked example` (blue), `hand-checkable` (green)

- **Start small** — insert just Ana, then Ben, then Cara: each is larger, so each hangs bottom-right
- **The lean** — after Cara the tree is Ana→Ben→Cara, a right-leaning chain of height 3
- **The trigger** — a balanced tree notices: Ana's right side is 2 levels deep, her left side is 0
- **The rotation** — lift the middle name: Ben becomes the parent, Ana slides to Ben's left, Cara stays right
- **The result** — height drops from 3 to 2, and the search order (Ana < Ben < Cara) still reads true
- **Repeat forever** — every later insert gets the same check, so no chain ever survives past a few nodes

*Example (italic):* Walk it by hand: Ana–Ben–Cara leaning right becomes Ben on top with Ana left and Cara right — three names, one lift, lean gone.

**Key point:** A rotation is a constant-time re-hang of two or three links that preserves sorted order — it is the only trick needed to keep the whole tree short.

### Visualization (canvas `c2`, 720×300)

Before/after diagram of the Ana–Ben–Cara rotation with a curved lift arrow on the middle node.

- **Title (bold 15px, `#1a5276`, top center):** "The Rotation: Lift Ben, Tuck Ana Under His Left".
- **Node style:** circles radius 17, white fill, 2px stroke, bold 12px name inside.
- **Before (left):** Ana at (150, 95) stroked `#d95926`, Ben at (210, 165) stroked `#c98500` (the node about to be lifted), Cara at (270, 235) stroked `#d95926`; 1.5px `#6b7280` arrows Ana→Ben and Ben→Cara; bold 12px `#d95926` label "height 3, all right children" at (195, 282).
- **After (right):** Ben at (530, 95) stroked `#c98500`, Ana at (455, 195) stroked `#008300`, Cara at (605, 195) stroked `#008300`; 1.5px `#6b7280` edges Ben→Ana and Ben→Cara; bold 12px `#008300` label "height 2, order preserved" at (530, 282).
- **Lift arrow:** 2.5px `#c98500` curved arrow (quadratic, control point (365, 60)) from Ben's before-position to Ben's after-position, with filled arrowhead; bold 12px `#c98500` label "lift the middle" at (365, 52).
- **Order check:** 11px `#6b7280` text under the after-tree at (530, 240): "in-order walk still reads Ana, Ben, Cara".
- **Divider:** 1px `#e5e9ef` vertical line at x=355 from y=60 to y=265.
- **Caption (12px `#444`, bottom right):** "one rotation — two links re-hung".

## Why Your Map Type Never Gets Slow

**Tags:** `where it's used` (blue), `guaranteed log n` (green)

- **The guarantee** — self-balancing trees cap height near log₂(n), whatever order the data arrives in
- **The scale** — 1,000,000 sorted inserts: the chain answers lookups in ~500,000 steps, the tree in ~20
- **Ordered maps** — sorted-map/set types are usually red-black trees, so O(log n) is a contract
- **Database indexes** — index structures rebalance on write for the same reason: real data arrives sorted
- **Range scans** — the tree stays a sorted structure, so "every name from D to F" walks it in order

*Example (italic):* A dashboard inserting timestamped events — always-increasing keys — is the sorted-insert worst case; the self-balancing index shrugs it off.

**Key point:** Real-world keys arrive sorted embarrassingly often (ids, timestamps, alphabetized exports) — balancing turns that worst case into just another day.

### Visualization (canvas `c3`, 720×300)

Line chart of lookup steps versus items inserted in sorted order: the chain's line climbs linearly while the balanced tree's line hugs the floor.

- **Title (bold 15px, `#1a5276`, top center):** "Lookup Steps After n Sorted Inserts: Chain vs Balanced Tree".
- **Axes:** origin x=80, baseline y=245, plot width 570, plot height 185; x = items with 5 category ticks at x = `[140, 265, 390, 515, 640]` labeled "16", "64", "256", "1,024", "4,096" (12px `#444`); y = average steps 0 to 2,048, light `#e5e9ef` gridlines at 512, 1,024, 1,536, 2,048 with 12px `#444` left labels "512", "1,024", "1,536", "2,048".
- **Chain line:** orange `#d95926` 3px through steps = `[8, 32, 128, 512, 2048]` at the 5 ticks; 5px dots; 12px orange value labels "8", "32", "128", "512", "2,048" beside each dot; 12px orange label "plain BST fed sorted keys" near (300, 150).
- **Balanced line:** green `#008300` 3px through steps = `[4, 6, 8, 10, 12]`; 5px dots; 12px green value labels "4", "6", "8", "10", "12" above each dot; bold 12px green label "balanced — about log₂(n)" near (480, 205).
- **Annotation (bold 13px `#008300`, near x=250, y=60):** "the guarantee: ~20 steps even at a million".
- **Caption (12px `#444`, bottom right):** "average lookup steps — illustrative".

## Balanced Doesn't Mean Symmetric

**Tags:** `common mistake` (red), `height bound` (orange)

- **The worry** — people expect a balanced tree to look like a perfect pyramid at all times
- **The truth** — "balanced" only bounds the height; lopsided-looking subtrees are perfectly legal
- **AVL's rule** — sibling subtrees may differ by 1 level, no more; strict, so it rotates more often
- **Red-black's rule** — looser coloring rules allow height up to ~2× log₂(n) but need fewer fix-ups
- **The trade** — AVL reads a hair faster (shorter), red-black writes faster (fewer rotations)
- **Both win** — either way the height is capped, and that cap is the entire point

*Example (italic):* A legal red-black tree can have one branch nearly twice as deep as another — and still every lookup finishes within the promised step budget.

**Common mistake:** Judging balance by looks. The invariant is a height bound, not symmetry — a tree can lean noticeably and still honor its O(log n) contract.

### Visualization (canvas `c4`, 720×300)

A legal, visibly-leaning balanced tree with its height budget marked, next to the illegal chain for contrast.

- **Title (bold 15px, `#1a5276`, top center):** "Legal Lean: Height Within Budget Beats Pretty Symmetry".
- **Node style:** circles radius 13, white fill, 2px stroke, bold 10px key inside (keys 1–9).
- **Legal tree (left, stroked `#008300`):** 4 at (200, 80); 2 at (120, 140), 6 at (280, 140); 1 at (80, 200), 3 at (160, 200), 5 at (240, 200), 8 at (330, 200); 7 at (295, 255), 9 at (365, 255); 1.5px `#6b7280` edges; the 8-7-9 subtree makes the right side one level deeper.
- **Height ruler:** dashed 1.5px `#008300` horizontal line (dash 4/3) at y=265 from x=60 to x=395 with 11px green label "height 4 ≤ budget for 9 keys" at (225, 283).
- **Illegal chain (right, stroked `#e74c3c`):** keys 1→2→3→4→5 as a staircase at (520, 80), (550, 122), (580, 164), (610, 206), (640, 248); 1.5px `#6b7280` arrows; bold 12px `#e74c3c` label "height 5 for 5 keys — illegal" at (565, 283).
- **Divider:** 1px `#e5e9ef` vertical line at x=445 from y=55 to y=270.
- **Annotation (bold 12px `#c98500`, near x=200, y=48):** "leans a little — still balanced where it counts".
- **Caption:** none (labels carry it).

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the displayed CSS width × `devicePixelRatio` (sharp-rendering pattern) and scales the context; chart functions are pushed into a `__charts` array, run once, and re-run on window resize debounced 150 ms.
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all node names, positions, and line values are the hardcoded literals above (no randomness); the 7-name chain and height-3 balanced tree in `c1` must match the section-one bullets; the Ana/Ben/Cara rotation in `c2` must match section two; the `[8, 32, 128, 512, 2048]` vs `[4, 6, 8, 10, 12]` step counts in `c3` are illustrative and labeled so.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
