# Apriori & FP-growth

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Apriori & FP-growth

**Subtitle:** Finding every combo of items that keeps showing up across receipts — without counting all 2^n possible combos, by pruning candidates (Apriori) or compressing receipts into a tree (FP-growth)

## Ten Receipts at a Coffee Shop

**Tags:** `core idea` (blue), `support` (green), `market baskets` (orange)

- **The shop** — a coffee shop keeps 10 receipts; items are coffee, muffin, sugar, tea, and cookie
- **Itemset** — any combo of items bought together, like {coffee, sugar}, is called an itemset
- **Support** — support = how many receipts contain the whole combo: {coffee, muffin} is on 5 of 10
- **Frequent** — a combo counts as frequent if support clears a bar we pick: here 4 of 10 receipts
- **The wall** — 5 items already make 31 possible combos; 1,000 store items make more than atoms exist

*Example (italic):* {coffee, muffin, sugar} sits on receipts 1, 5, and 8 — support 3, just under our bar of 4.

**Key point:** Frequent-itemset mining finds every combo that clears a support bar — the whole game is doing it without counting all 2^n combos one by one.

### Visualization (canvas `c1`, 720×300)

Receipt dot matrix (left) showing which of the 5 items is on each of the 10 receipts, plus horizontal per-item support bars (right) with the support bar of 4 marked.

- **Title (bold 15px, `#1a5276`, top center):** "Ten Receipts, Five Items — Which Combos Keep Showing Up?".
- **Data (fixed boolean matrix, columns coffee/muffin/sugar/tea/cookie):** r1 `[1,1,1,0,0]`, r2 `[1,0,1,0,0]`, r3 `[1,1,0,0,0]`, r4 `[0,0,0,1,1]`, r5 `[1,1,1,0,0]`, r6 `[1,0,1,1,0]`, r7 `[0,1,0,0,1]`, r8 `[1,1,1,0,1]`, r9 `[0,0,1,1,0]`, r10 `[1,1,0,0,0]`. Column sums: coffee 7, muffin 6, sugar 6, tea 3, cookie 3.
- **Matrix (left):** column headers "coffee", "muffin", "sugar", "tea", "cookie" 12px `#444` at y=52, column x = 120, 175, 230, 285, 340; row labels "r1"–"r10" 11px `#6b7280` right-aligned at x=92; row y from 72 step 22 (72–270); presence = filled 5px-radius dot, blue `#2a78d6`.
- **Highlight:** rows r1, r5, r8 get a background band `rgba(201,133,0,0.12)` (x=100–360, 18px tall); yellow `#c98500` bold 12px annotation right of the band area: "r1, r5, r8 share {coffee, muffin, sugar}".
- **Support bars (right):** heading bold 12px `#444` "receipts containing each item" centered at x=560, y=58; five horizontal bars from x=470, y = 80, 114, 148, 182, 216, height 18, width = count/7 × 190; item names 12px `#444` right-aligned at x=465; counts bold 12px at bar end; coffee/muffin/sugar fill `rgba(42,120,214,0.55)`, tea/cookie fill `rgba(213,81,129,0.45)`.
- **Threshold:** vertical dashed `#1a5276` line (dash 4/3) at x = 470 + 4/7 × 190 ≈ 579, from y=70 to y=240; bold 12px ink label "bar: 4 of 10" above it.
- **Caption (12px `#444`, bottom):** "the same fixed 10 receipts are used in every chart on this page".

## Apriori: Prune Before You Count

**Tags:** `worked example` (blue), `downward closure` (green)

- **Shrinking rule** — adding an item can only drop support: sugar is on 6 receipts, {muffin, sugar} on 3
- **Level 1** — count the five single items: coffee 7, muffin 6, sugar 6, tea 3, cookie 3
- **Prune** — tea (3) and cookie (3) miss the bar of 4, so every combo containing them is dead too
- **Level 2** — only pair the survivors: {coffee, muffin} 5, {coffee, sugar} 5, {muffin, sugar} 3
- **Level 3** — a triple needs all its pairs frequent; {muffin, sugar} failed, so no triple is counted
- **The win** — Apriori counted 8 candidate combos instead of all 31 and still found every frequent one

*Example (italic):* Of the 31 possible combos, pruning killed 23 without ever counting them; the 8 that were counted give the same answer.

**Key point:** If a combo is rare, every bigger combo containing it is at least as rare — so one failed count kills whole branches of the search before they are ever counted.

### Visualization (canvas `c2`, 720×300)

Three-level candidate funnel: singles, pairs, triples, with survivors in green, pruned candidates in magenta, and never-counted combos as gray notes.

- **Title (bold 15px, `#1a5276`, top center):** "Apriori Counts 8 Candidates Instead of All 31 Combos".
- **Threshold note (bold 12px `#1a5276`, top right):** "frequent = on ≥ 4 of 10 receipts".
- **Level labels (bold 12px `#444`, left, x=14):** "singles" at y=90, "pairs" at y=165, "triples+" at y=240.
- **Level 1 (five rounded rects 96×34, top y=70, x = 70, 196, 322, 448, 574):** texts "coffee 7", "muffin 6", "sugar 6", "tea 3", "cookie 3" centered 12px; survivors (first three) border 2px `#008300`, fill `rgba(0,131,0,0.08)`; pruned (tea, cookie) border 2px `#d55181`, fill `rgba(213,81,129,0.08)`, bold magenta "✗" at box top-right.
- **Level 2 (three rounded rects 132×34, top y=148, x = 90, 250, 410):** "coffee+muffin 5" green style, "coffee+sugar 5" green style, "muffin+sugar 3" magenta style with "✗"; gray note 11px `#6b7280` at x=560, y=160 (two lines): "7 other pairs" / "never counted"; 1px `#e5e9ef` connector lines from the three level-1 survivor boxes down to the level-2 boxes.
- **Level 3 (one dashed rect 200×34, border `#6b7280` dash 4/3, top y=226, x=90):** text "no candidate triples" 12px `#6b7280`; magenta `#d55181` bold 12px note at x=310, y=246: "muffin+sugar failed, so no triple is tried"; gray note 11px `#6b7280` below it: "16 bigger combos skipped".
- **Takeaway (bold 13px `#008300`, centered, y=290):** "counted: 5 singles + 3 pairs = 8 of 31 possible combos".

## FP-growth: Compress First, Count Never

**Tags:** `worked example` (blue), `FP-tree` (orange)

- **Two passes** — FP-growth reads the receipts exactly twice: once to count items, once to build a tree
- **Drop first** — tea and cookie are rare (3 each), so they never enter the tree at all
- **Sort & stack** — each receipt is sorted coffee → muffin → sugar and stacked onto shared branches
- **Compression** — the 10 receipts hold 19 surviving item entries, but the tree needs only 6 nodes
- **Mining** — walk up from sugar: coffee sits above it on paths worth 5, so {coffee, sugar} = 5
- **No candidates** — the tree is read directly; no list of maybe-frequent combos is ever generated

*Example (italic):* Receipts 1, 5, and 8 are identical after sorting and dropping (coffee, muffin, sugar), so all three stack onto one branch.

**Key point:** Apriori prunes the candidate list; FP-growth never makes one — shared prefixes squeeze 10 receipts into one small tree that is mined by walking paths upward.

### Visualization (canvas `c3`, 720×300)

FP-tree diagram (left) built from the 10 receipts, with a build-and-mine walkthrough panel (right).

- **Title (bold 15px, `#1a5276`, top center):** "Ten Receipts Compressed into a Six-Node Tree".
- **Tree nodes (rounded rects 86×26, centers):** "root" gray (fill `#f1f3f6`, border 1px `#9aa3af`) at (170, 68); "coffee 7" at (115, 128); "muffin 5" at (70, 188); "sugar 3" at (70, 248); "sugar 2" at (185, 188); "muffin 1" at (255, 128); "sugar 1" at (335, 128); non-root nodes fill `rgba(42,120,214,0.10)`, border 2px `#2a78d6`, 12px text with the count in bold.
- **Edges (2px `#b8c4d0`):** root→coffee 7, root→muffin 1, root→sugar 1, coffee 7→muffin 5, coffee 7→sugar 2, muffin 5→sugar 3.
- **Right panel (x from 420):** heading bold 12px `#444` "how it was built" at y=58; lines 12px `#2c3e50` (~17px apart): "pass 1: coffee 7, muffin 6, sugar 6", "tea 3 and cookie 3 dropped (below 4)", "pass 2: sort each receipt by item count,", "then stack it onto the tree".
- **Mining box (rounded rect x=420, y=150, 280×84, fill `rgba(0,131,0,0.06)`, border 1px `#008300`):** heading bold 12px `#008300` "mine sugar (walk up from its 3 nodes)"; lines 12px: "paths above sugar: coffee·muffin ×3,", "coffee ×2, root ×1 → coffee totals 5", "5 ≥ 4, so {coffee, sugar} = 5 is frequent".
- **Annotation (bold 13px `#008300`, x=420, y=262):** "19 item entries → 6 tree nodes".
- **Caption (12px `#444`, bottom left):** "receipts 1, 5, 8 all become the coffee → muffin → sugar branch (count 3)".

## Frequent Is Not the Same as Linked

**Tags:** `common mistake` (red), `lift` (green), `confidence` (orange)

- **Confidence** — of the 7 coffee receipts, 5 include a muffin: coffee → muffin has confidence 71%
- **Base rate** — muffins are on 6 of 10 receipts anyway, a 60% base rate, so 71% is a real bump
- **The trap** — muffin → sugar has confidence 50%, but sugar's base rate is 60%: muffins predict less
- **Lift** — divide confidence by base rate: coffee → muffin lifts 1.19; muffin → sugar sinks to 0.83
- **Why it matters** — a recommender built on raw support just pushes popular items at everyone

*Example (italic):* {muffin, sugar} appears on 3 receipts — it looks like a combo, yet the pairing is rarer than chance (3.6 expected) predicts.

**Common mistake:** Reading a frequent pair as a relationship. Always compare the rule's confidence against the right-hand item's base rate (lift) before acting on it.

### Visualization (canvas `c4`, 720×300)

Grouped bar chart comparing rule confidence against base rate for two rules from the same receipts: one with lift above 1, one below.

- **Title (bold 15px, `#1a5276`, top center):** "Two Rules from the Same Receipts: One Real, One Illusion".
- **Data:** rule "coffee → muffin": confidence 71% (5 of 7), muffin base rate 60%; rule "muffin → sugar": confidence 50% (3 of 6), sugar base rate 60%.
- **Axis:** origin x=70, baseline y=240, chart height 180, y scale 0–100%; horizontal gridlines `#e5e9ef` at 25/50/75/100 with 11px `#6b7280` labels at x=62 right-aligned.
- **Bars (70px wide):** group 1 — confidence bar at x=130 fill `rgba(42,120,214,0.6)`, base-rate bar at x=215 fill `rgba(107,114,128,0.35)`; group 2 — confidence bar at x=420 fill `rgba(213,81,129,0.55)`, base-rate bar at x=505 fill `rgba(107,114,128,0.35)`; bold 13px value labels "71%", "60%", "50%", "60%" above each bar.
- **Group labels (bold 12px `#1a5276`, below baseline):** "coffee → muffin (lift 1.19)" centered at x=207, "muffin → sugar (lift 0.83)" centered at x=497.
- **Annotations:** green `#008300` bold 12px above group 1: "coffee buyers beat the 60% base rate"; magenta `#d55181` bold 12px above group 2: "muffin buyers pick sugar LESS than average".
- **Legend (top right, 12px):** solid blue swatch "rule confidence", gray swatch "item base rate".
- **Caption (12px `#444`, bottom):** "confidence = share of the rule's left-side receipts that also contain the right side".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (here it hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
