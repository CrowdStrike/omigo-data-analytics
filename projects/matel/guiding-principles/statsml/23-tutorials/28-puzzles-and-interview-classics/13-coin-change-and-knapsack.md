# Coin Change & Knapsack

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Coin Change & Knapsack

**Subtitle:** Grabbing the biggest piece first feels obviously right and can quietly cost you — building the best answer up from smaller amounts (dynamic programming) never does

## Six Cents of Postage, Three Odd Stamps

**Tags:** `core idea` (blue), `greedy fails` (red), `puzzle` (orange)

- **The stamp drawer** — a mail clerk has stamps worth 1¢, 3¢, and 4¢ and must hit exact postage
- **The greedy habit** — for 6¢ she grabs the biggest first: a 4¢, then 1¢, then 1¢ — three stamps
- **The better answer** — 3¢ + 3¢ covers 6¢ with just two stamps; greedy walked right past it
- **Why greedy slips** — taking the 4¢ leaves 2¢ behind, which these stamps cover only with two 1¢s
- **The lesson** — the best first move depends on what it leaves behind, and greedy never looks

*Example (italic):* For 6¢ of postage, biggest-first spends 4+1+1 — three stamps — while 3+3 does the same job in two.

**Key point:** Greedy commits to the biggest piece without checking the remainder it creates — and sometimes that remainder is the expensive part.

### Visualization (canvas `c1`, 720×300)

Two horizontal stacked bars on a shared cents axis: the greedy pick (4+1+1) above and the best pick (3+3) below, each segment drawn to scale so the extra stamp is visible at a glance.

- **Title (bold 15px, `#1a5276`, top center):** "Making 6¢ with 1-3-4 Stamps: Greedy vs Best".
- **Axis:** horizontal 2px `#999` line at y=250 from x=160 to x=680 (width 520), cents 0 to 6; tick labels "0¢", "1¢", ..., "6¢" every 1¢ (12px `#444`) below; light `#e5e9ef` vertical gridlines at each tick from y=70 to the axis.
- **Row 1 (bar top y=100, height 36), label 12px `#444` at x=20:** "greedy: biggest first"; segments left to right — 4¢ orange `#d95926` fill `rgba(217,89,38,0.35)` with 2px orange border, then two 1¢ segments same style; bold 13px orange label centered in each segment: "4", "1", "1"; bold 13px orange count at x=690: "3 stamps".
- **Row 2 (bar top y=180, height 36), label:** "best: two 3¢ stamps"; two 3¢ segments green `#008300` fill `rgba(0,131,0,0.30)` with 2px green border, bold 13px green labels "3", "3"; bold 13px green count at x=690: "2 stamps".
- **Annotation (bold 13px `#e74c3c`, centered near x=420, y=60):** "greedy pays one extra stamp — the 4¢ leaves a clumsy 2¢ remainder".

## Building the Answer One Cent at a Time

**Tags:** `worked example` (blue), `dynamic programming` (green)

- **One question** — for each amount a, what is the fewest stamps? call it best[a], with best[0] = 0
- **One rule** — best[a] = 1 + the smallest of best[a−1], best[a−3], best[a−4], skipping negatives
- **Fill upward** — best[1]=1, best[2]=2, best[3]=1, best[4]=1, best[5]=2, and so on, never guessing
- **Check 6¢** — best[5]+1=3, best[3]+1=2, best[2]+1=3; the minimum is 2, matching 3¢+3¢
- **Keep going** — the finished row 0..12 reads 0,1,2,1,1,2,2,2,2,3,3,3,3 — every amount solved once

*Example (italic):* To settle 6¢ the table only compares three earlier cells — best[5]=2, best[3]=1, best[2]=2 — and takes the cheapest plus one stamp: 2.

**Key point:** DP earns its keep by reusing answers — each amount is solved once from smaller amounts, then looked up forever after.

### Visualization (canvas `c2`, 720×300)

A single row of 13 table cells for amounts 0¢..12¢ showing best[a], with three arrows curving from cell 6 back to cells 5, 3, and 2 — the only comparisons needed to settle 6¢.

- **Title (bold 15px, `#1a5276`, top center):** "The best[] Table for Stamps {1, 3, 4}".
- **Cells:** 13 boxes, 44px wide, 46px tall, tops at y=150, left edges at x = 55 + 46·i for i = 0..12; 1px `#999` border, white fill; amount label ("0"–"12", 12px `#6b7280`) centered 14px above each box; value (bold 15px `#2c3e50`) centered inside; values left to right: `[0, 1, 2, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3]`.
- **Highlights:** cell 6 fill `rgba(0,131,0,0.18)` with 2px `#008300` border; cells 5, 3, 2 fill `rgba(42,120,214,0.15)` with 2px `#2a78d6` border.
- **Arrows:** three 2px blue `#2a78d6` quadratic curves from the top of cell 6 to the tops of cells 5, 3, and 2 (control points arcing up to about y=100), each with a small arrowhead at the destination; 11px blue labels "−1¢", "−3¢", "−4¢" near each arc's peak, staggered so none overlap.
- **Winner note:** bold 12px green `#008300` under cell 6 (y≈225): "1 + best[3] = 2".
- **Annotation (bold 13px `#1a5276`, centered near x=360, y=62):** "each cell looks back at only three earlier cells — no guessing, no regret".
- **Caption (12px `#444`, bottom right):** "values are exact minimum stamp counts for {1, 3, 4}".

## The Same Trick Packs a Picnic Bag

**Tags:** `where it's used` (blue), `knapsack` (green), `greedy fails` (red)

- **The picnic bag** — a 10 kg bag; a 6 kg grill worth 30 points and two 5 kg coolers worth 24 each
- **Greedy by bang-per-kg** — the grill scores 5.0 per kg, the coolers 4.8, so greedy packs the grill
- **Stuck at 30** — with only 4 kg of room left, neither 5 kg cooler fits; greedy ends at 30 points
- **DP asks per capacity** — best value at every limit 0..10 kg, item by item, like the stamp row
- **The real answer** — the two coolers together weigh exactly 10 kg and score 48; DP finds it

*Example (italic):* Greedy's grill-first bag scores 30 points; the DP bag holding both coolers scores 48 — a 60% better picnic from the same items.

**Key point:** Coin change and knapsack are one lesson — when indivisible choices compete for a shared budget, build exact best answers per budget instead of ranking items.

### Visualization (canvas `c3`, 720×300)

Two horizontal capacity bars on a shared 0–10 kg axis: greedy's bag (grill plus dead space) above, the DP bag (two coolers, zero waste) below, with each bag's total points at the right.

- **Title (bold 15px, `#1a5276`, top center):** "One 10 kg Bag: Rank-by-Density vs DP".
- **Axis:** horizontal 2px `#999` line at y=250 from x=170 to x=640 (width 470), weight 0 to 10 kg; tick labels "0", "2", "4", "6", "8", "10 kg" every 2 kg (12px `#444`); light `#e5e9ef` gridlines at each tick from y=70 to the axis.
- **Row 1 (bar top y=100, height 40), label 12px `#444` at x=20:** "greedy: grill first"; one segment 0–6 kg, orange `#d95926` fill `rgba(217,89,38,0.35)`, 2px orange border, bold 13px orange centered label "grill — 30 pts"; then 6–10 kg drawn as a dashed 2px `#6b7280` outline with no fill and 12px `#6b7280` centered label "4 kg wasted"; bold 15px orange total at x=655: "30".
- **Row 2 (bar top y=180, height 40), label:** "DP: both coolers"; segments 0–5 and 5–10 kg, green `#008300` fill `rgba(0,131,0,0.30)`, 2px green border, bold 13px green labels "cooler — 24 pts" in each; bold 15px green total at x=655: "48".
- **Annotation (bold 13px `#e74c3c`, centered near x=405, y=60):** "48 vs 30 — the 'best' item per kg left 4 kg dead".
- **Caption (12px `#444`, bottom right):** "illustrative weights and points".

## But Greedy Works Fine at the Grocery Store

**Tags:** `common mistake` (red), `lucky denominations` (orange)

- **The cashier defense** — with 1¢, 5¢, 10¢, 25¢ coins, biggest-first genuinely is always optimal
- **Lucky spacing** — US-style coins are specially spaced ("canonical"), so greedy's remainders stay cheap
- **Not a law** — with 1-3-4 stamps, greedy overpays at 6¢ (3 stamps vs 2) and at 10¢ (4 vs 3)
- **Knapsack cousin** — greedy by density is optimal only when items can be split (fractional knapsack)
- **The tell** — indivisible pieces plus a hard budget is the signature that DP is required

*Example (italic):* The same biggest-first habit that is provably perfect at a US register silently overpays with 1-3-4 stamps — at exactly 6¢ and 10¢.

**Common mistake:** Trusting greedy because it worked on one coin system — the optimality came from those particular denominations, not from the algorithm.

### Visualization (canvas `c4`, 720×300)

Grouped bar chart over amounts 1¢..12¢ with stamps {1, 3, 4}: greedy's stamp count next to the true minimum for every amount, so the two silent overpays stand out in red.

- **Title (bold 15px, `#1a5276`, top center):** "Greedy vs Optimal Stamp Count, Amounts 1¢–12¢ (stamps 1, 3, 4)".
- **Axes:** origin x=60, baseline y=245, plot width 620, plot height 175; y = stamp count 0 to 4 with 12px `#444` tick labels "0"–"4" and light `#e5e9ef` gridlines at 1, 2, 3, 4; x = twelve groups centered at x = 60 + 51.7·(i − 0.5) for i = 1..12, 12px `#444` labels "1"–"12" under each group and 12px label "cents" at the far right.
- **Bars per group:** two 18px-wide bars, 4px apart — greedy (left) then optimal (right), heights scaled 43.75px per stamp.
- **Greedy counts:** `[1, 2, 1, 1, 2, 3, 2, 2, 3, 4, 3, 3]` — fill `rgba(217,89,38,0.35)`, 2px `#d95926` border; at amounts 6 and 10 use fill `rgba(231,76,60,0.35)` with 2px `#e74c3c` border instead.
- **Optimal counts:** `[1, 2, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3]` — fill `rgba(0,131,0,0.30)`, 2px `#008300` border.
- **Mismatch markers:** bold 13px `#e74c3c` "+1" centered above the greedy bar at amount 6 and at amount 10.
- **Legend (12px, top left inside plot, y≈75):** orange swatch "greedy", green swatch "optimal", red swatch "greedy overpays".
- **Annotation (bold 12px `#e74c3c`, near x=430, y=95):** two lines: "identical on 10 of 12 amounts —" / "wrong exactly where you don't check".
- **Caption (12px `#444`, bottom right):** "exact counts; greedy = repeatedly take the largest stamp that fits".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`. Red is reserved for the genuine failure states (greedy's overpays).
- **Data:** all counts, segment widths, table values, and bar heights are the hardcoded arrays above (no randomness); best[] values and greedy counts are exact for stamps {1, 3, 4}; picnic weights/points are invented and captioned "illustrative".
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
