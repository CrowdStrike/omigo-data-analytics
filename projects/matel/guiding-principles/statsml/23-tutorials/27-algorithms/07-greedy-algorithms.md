# Greedy Algorithms

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Greedy Algorithms

**Subtitle:** A greedy algorithm takes the best-looking step right now and never looks back — brilliantly fast when the problem rewards it, quietly wrong when it doesn't

## Making Change the Lazy Way

**Tags:** `core idea` (blue), `best local step` (green), `no look-back` (orange)

- **The cashier** — a customer is owed 68 cents; the cashier grabs the biggest coin that still fits
- **Step by step** — 25 leaves 43, another 25 leaves 18, a 10 leaves 8, a 5 leaves 3, then three 1s
- **Seven coins** — 25 + 25 + 10 + 5 + 1 + 1 + 1 = 68, handed over in seven grabs with zero planning
- **The greedy rule** — at every step take the choice that looks best right now, then never revisit it
- **It happens to be optimal** — with US coins no cleverer plan beats the biggest-coin-first habit

*Example (italic):* Owed 68 cents, the cashier never asks "what if I skip the quarter?" — biggest coin, repeat, done in seven.

**Key point:** A greedy algorithm commits to the best local step and never backtracks — for 68 cents with US coins that lazy habit is also the fewest coins possible.

### Visualization (canvas `c1`, 720×300)

Single-panel staircase chart: the amount still owed dropping after each greedy grab, one labeled step per coin, from 68 down to 0.

- **Title (bold 15px, `#1a5276`, top center):** "Greedy Change for 68¢: Biggest Coin First, Seven Steps to Zero".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 190; x axis = step 0 to 7 with 12px `#444` tick labels "start", "1", "2", ..., "7"; y axis = cents owed 0 to 70, 12px `#444` labels at 0, 10, ..., 70, light `#e5e9ef` gridlines.
- **Staircase:** blue `#2a78d6` 3px stepped line through the hardcoded remaining amounts `[68, 43, 18, 8, 3, 2, 1, 0]` at steps 0–7; fill under the steps `rgba(42,120,214,0.15)`; a 5px blue dot at each step level.
- **Coin labels:** bold 12px `#d95926` label above each drop naming the coin taken, in order: "25", "25", "10", "5", "1", "1", "1".
- **End marker:** green `#008300` 7px dot at (step 7, 0) with bold 13px green label "0¢ — 7 coins" just above it.
- **Annotation (bold 12px `#1a5276`, near step 1.5, y=90):** two lines: "always grab the biggest coin" / "that still fits — never look back".
- **Caption (12px `#444`, bottom right):** "exact arithmetic — 25+25+10+5+1+1+1 = 68".

## A Vending Machine That Breaks Greedy

**Tags:** `worked example` (blue), `greedy fails` (red)

- **Odd coins** — a vending machine abroad only holds 1-, 3-, and 4-cent coins, and owes 6 cents
- **Greedy's move** — biggest first: a 4 leaves 2, then 1 and 1 — greedy pays 4 + 1 + 1 = 3 coins
- **The better plan** — skip the tempting 4 entirely: 3 + 3 = 6 uses just 2 coins
- **Where it went wrong** — grabbing the 4 was the best single step but poisoned everything after it
- **Same code, new coins** — the algorithm didn't change; the coin set stopped rewarding local moves

*Example (italic):* Owed 6 cents in {1, 3, 4} coins, greedy hands back three coins (4, 1, 1) while two 3s would do — the best first grab was the wrong first grab.

**Key point:** Greedy paid 3 coins where 2 suffice: 4+1+1 versus 3+3. The failure isn't a bug in the code — the problem itself stopped rewarding the best local step.

### Visualization (canvas `c2`, 720×300)

Two-row coin strip: greedy's coins on top, the optimal coins below, drawn as labeled squares whose widths are proportional to their value, both rows summing to the same 6-cent bar length.

- **Title (bold 15px, `#1a5276`, top center):** "Paying 6¢ with {1, 3, 4} Coins: Greedy vs Best".
- **Layout:** shared horizontal scale x=200 to x=680 (480px = 6 cents, so 80px per cent); light `#e5e9ef` vertical gridlines every cent with 12px `#444` labels "1¢" through "6¢" at y=265.
- **Row 1 (y=95, 44px tall), 13px `#444` label at x=20:** "greedy: 4 + 1 + 1"; three rounded rects: width 320 filled `rgba(217,89,38,0.25)` with 2px `#d95926` border labeled bold 14px "4", then two width-80 rects same style labeled "1", "1"; bold 13px `#d95926` count at the row's right end: "3 coins".
- **Row 2 (y=185, 44px tall), label:** "best: 3 + 3"; two rounded rects width 240 each, filled `rgba(0,131,0,0.20)` with 2px `#008300` border, labeled bold 14px "3", "3"; bold 13px `#008300` count at the right end: "2 coins".
- **Annotation (bold 13px `#e74c3c`, centered near x=440, y=55):** "the tempting 4 costs one extra coin".
- **Caption (12px `#444`, bottom right):** "both rows total exactly 6¢".

## Where Data Scientists Meet Greedy

**Tags:** `where it's used` (blue), `hill climbing` (green), `local optimum` (orange)

- **Decision trees** — each split picks the best purity gain right now; the whole tree is greedy stacked
- **Forward feature selection** — add the one feature that helps most, repeat; early picks are never undone
- **Hill climbing** — gradient-style search always steps uphill and stops at the first peak it finds
- **Local vs global** — the first peak (the local optimum) can sit well below the true best answer
- **When greedy is provably safe** — Huffman coding, minimum spanning trees, and US-coin change all have proofs
- **When it isn't** — 0/1 knapsack, nearest-neighbor routing, and {1,3,4} change need something smarter

*Example (italic):* A hill climber starting at position 10 walks uphill to a peak of height 60 and stops — the mountain's true summit, height 88, sits farther right across a dip it refuses to cross.

**Key point:** Half of everyday ML is greedy under the hood — fine when a proof (or a dip-free landscape) backs it, a silent local optimum when not.

### Visualization (canvas `c3`, 720×300)

Single-panel landscape curve with two peaks: a hill climber's uphill path ending on the lower local peak, with the taller global peak marked beyond a valley.

- **Title (bold 15px, `#1a5276`, top center):** "Hill Climbing: Greedy Stops at the First Peak".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 190; x = position 0 to 100 with 12px `#444` tick labels every 20; y = height 0 to 100, light `#e5e9ef` gridlines at 25, 50, 75.
- **Landscape:** `#6b7280` 2px smooth curve through hardcoded points at x = `[0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100]`, height = `[20, 42, 56, 60, 52, 38, 50, 74, 88, 70, 40]`; fill under `rgba(107,114,128,0.10)`.
- **Climber's path:** blue `#2a78d6` 4px line overlaying the curve from x=10 to x=30 (heights 42 → 56 → 60), with three 6px blue dots at x=10, 20, 30 and small blue up-arrows between them.
- **Local peak marker:** orange `#d95926` 8px dot at (30, 60), bold 13px orange label above: "stuck here (60)".
- **Global peak marker:** green `#008300` 8px dot at (80, 88), bold 13px green label above: "true best (88)".
- **Valley note:** 12px `#6b7280` label near (50, 38): "the dip greedy won't cross".
- **Annotation (bold 12px `#1a5276`, near x=55, y=75):** "every step went up — and it still missed the summit".
- **Caption (12px `#444`, bottom right):** "illustrative landscape".

## "It Passed My Tests, So It's Optimal"

**Tags:** `common mistake` (red), `hidden traps` (orange)

- **The trap** — greedy change in {1, 3, 4} coins matches the true best on 10 of the 12 amounts from 1 to 12
- **Spot checks lie** — test amounts 3, 7, 8, or 12 and greedy looks flawless every single time
- **The two traitors** — at 6 greedy pays 3 coins vs 2, and at 10 it pays 4 vs 3; nowhere else below 13
- **The mistake** — concluding "greedy works here" from a handful of passing examples instead of a proof
- **The honest options** — prove the greedy choice is safe, exhaustively check the input range, or don't trust it

*Example (italic):* An engineer tested the {1,3,4} change-maker on five random amounts, shipped it, and only amount 6 in production revealed the extra coin.

**Common mistake:** Believing a greedy rule is optimal because your test cases passed. Failures hide at specific inputs (here, exactly 6 and 10) — a proof or a full sweep is the only real guarantee.

### Visualization (canvas `c4`, 720×300)

Grouped bar chart: for each amount 1 through 12 in the {1, 3, 4} coin system, a blue greedy-coins bar next to a green fewest-coins bar, with the two mismatching amounts highlighted.

- **Title (bold 15px, `#1a5276`, top center):** "Greedy vs Best Coin Count, Amounts 1–12 in {1, 3, 4}".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 190; x = amounts 1–12, one group per amount, 12px `#444` labels centered under each group; y = number of coins 0 to 4, 12px `#444` labels at 1–4, light `#e5e9ef` gridlines.
- **Bars (each ~18px wide, 4px gap within a group):** greedy counts in blue `rgba(42,120,214,0.55)` = `[1, 2, 1, 1, 2, 3, 2, 2, 3, 4, 3, 3]`; best counts in green `rgba(0,131,0,0.45)` = `[1, 2, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3]`.
- **Mismatch highlights:** at amounts 6 and 10, a 2px `#e74c3c` rounded outline around the group and a bold 13px `#e74c3c` "✗" above it; every other group gets a small 12px `#008300` "=" above.
- **Legend (12px, top left inside plot):** blue swatch "greedy", green swatch "fewest possible".
- **Annotation (bold 12px `#e74c3c`, near amount 8, y=75):** two lines: "10 of 12 amounts agree —" / "spot checks would miss 6 and 10".
- **Caption (12px `#444`, bottom right):** "counts are exact for the {1, 3, 4} coin system".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (here it hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all values are the hardcoded arrays above (no randomness); the change-making numbers in c1, c2, and c4 are exact arithmetic for their coin systems; only the c3 landscape curve is invented and carries the "illustrative" caption.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
