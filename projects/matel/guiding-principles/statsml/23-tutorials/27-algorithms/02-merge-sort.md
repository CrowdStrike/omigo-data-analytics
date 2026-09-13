# Merge Sort

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Merge Sort

**Subtitle:** Merge sort only ever does one easy move — combining two already-sorted piles — so it costs at most about n·log₂n steps no matter how scrambled the input is

## Merging Two Sorted Piles Is the Easy Part

**Tags:** `core idea` (blue), `the merge move` (green), `piles` (orange)

- **The chore** — a coffee shop ends the day with 8 jumbled receipts that must be filed by amount
- **The easy move** — merging two already-sorted piles: compare the two top receipts, take the smaller
- **Repeat** — the tops are always the smallest receipts left, so the output comes out sorted by itself
- **One pass** — merging piles of 4 and 4 takes at most 7 comparisons; no receipt is looked at twice
- **The trick** — merge sort builds sorted piles out of smaller sorted piles, starting from single receipts

*Example (italic):* Piles [3, 27, 38, 43] and [9, 10, 12, 82]: compare 3 vs 9, take 3; compare 27 vs 9, take 9 — seven compares later the whole stack is in order.

**Key point:** Merging two sorted piles is trivially easy, and merge sort's entire plan is to only ever do that easy move.

### Visualization (canvas `c1`, 720×300)

Two sorted receipt piles on the left feeding one sorted output row on the right, with the compare-the-tops rule called out.

- **Title (bold 15px, `#1a5276`, top center):** "Merging Two Sorted Piles: Always Take the Smaller Top".
- **Pile A (blue):** four boxes 70×30, left edge x=70, tops at y = 70, 106, 142, 178; values top-down `[3, 27, 38, 43]`; fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border, values bold 13px `#2a78d6` centered; 12px `#6b7280` label "pile A (sorted)" above at y=60.
- **Pile B (orange):** four boxes 70×30, left edge x=200, same y positions; values top-down `[9, 10, 12, 82]`; fill `rgba(217,89,38,0.12)`, 2px `#d95926` border, values bold 13px `#d95926`; 12px `#6b7280` label "pile B (sorted)" above.
- **Output row (green):** eight boxes 36×30 starting at x=390, y=190, 2px gaps; values left-to-right `[3, 9, 10, 12, 27, 38, 43, 82]`; fill `rgba(0,131,0,0.12)`, 2px `#008300` border, values bold 12px `#008300`; 12px `#6b7280` label "merged output" below at y=245.
- **Arrows:** one 2px `#2a78d6` arrow from pile A's top box and one 2px `#d95926` arrow from pile B's top box, both curving to the left end of the output row, arrowheads 6px.
- **Annotation (bold 13px `#d95926`, two lines near x=390, y=90):** "compare the two tops, take the smaller —" / "7 compares sort all 8 receipts".
- **Caption (12px `#444`, bottom right):** "illustrative — receipt amounts in dollars".

## Eight Receipts, Three Rounds of Merging

**Tags:** `worked example` (blue), `merge rounds` (green)

- **Start** — the day's stack, top to bottom: 38, 27, 43, 3, 9, 82, 10, 12
- **Split** — deal the stack out into 8 piles of one receipt; a single receipt is already sorted
- **Round 1** — merge neighbors into pairs: [27,38], [3,43], [9,82], [10,12] — 4 comparisons
- **Round 2** — merge pairs into fours: [3,27,38,43] and [9,10,12,82] — 6 more comparisons
- **Round 3** — one last merge gives [3,9,10,12,27,38,43,82] — 7 comparisons, 17 in total

*Example (italic):* Piles double in size each round, so 8 receipts need only 3 rounds — and 1,000 receipts would need just 10.

**Key point:** Three rounds of easy merges and 17 comparisons sorted the whole stack — redo it by hand with 8 playing cards.

### Visualization (canvas `c2`, 720×300)

Merge tree read top-down: the 8 single receipts, then pairs, then fours, then the final sorted row, with the comparison count of each round on the right.

- **Title (bold 15px, `#1a5276`, top center):** "Eight Receipts, Three Rounds: 4 + 6 + 7 = 17 Comparisons".
- **Level 1 — singles (y=58, boxes 54×26):** eight boxes centered at x = 72, 148, 224, 300, 376, 452, 528, 604; values `[38, 27, 43, 3, 9, 82, 10, 12]`; fill `#f8f9fa`, 1px `#6b7280` border, bold 12px `#2c3e50` values.
- **Level 2 — pairs (y=118, boxes 116×26):** four boxes centered at x = 110, 262, 414, 566; texts `"27 38"`, `"3 43"`, `"9 82"`, `"10 12"`; fill `rgba(42,120,214,0.12)`, 2px `#2a78d6` border, bold 12px `#2a78d6`.
- **Level 3 — fours (y=178, boxes 240×26):** two boxes centered at x = 186, 490; texts `"3 27 38 43"`, `"9 10 12 82"`; same blue style.
- **Level 4 — final (y=238, box 500×28):** one box centered at x=338; text `"3 9 10 12 27 38 43 82"`; fill `rgba(0,131,0,0.12)`, 2px `#008300` border, bold 13px `#008300`.
- **Connectors:** 1px `#6b7280` lines from each box's bottom center to its merged box's top center.
- **Round labels (12px `#6b7280`, right edge x=700, right-aligned):** "round 1 — 4 compares" at y=131, "round 2 — 6 compares" at y=191, bold 12px `#008300` "round 3 — 7 compares" at y=252.
- **Annotation (bold 12px `#4a3aa7`, near x=630, y=46):** "every pile is born sorted".
- **Caption (12px `#444`, bottom left):** "illustrative — comparison counts are exact for this stack".

## Why Merge Sort Never Has a Bad Day

**Tags:** `where it's used` (blue), `worst case` (red), `databases` (orange)

- **No bad day** — merge sort costs at most about n·log n comparisons; no input order can blow it up
- **The contrast** — a naive quicksort fed an already-sorted list of 64 items burns 2,016 comparisons
- **Merge sort's bill** — the same 64 items cost merge sort at most 384 comparisons, worst case included
- **Databases** — data too big for memory is sorted as small sorted runs on disk, then merged together
- **Streams, not jumps** — merging reads each pile front to back, exactly how disks like to be read
- **Stable** — receipts with equal amounts keep their original order, so earlier sorts aren't undone

*Example (italic):* A database asked to ORDER BY 100 million rows sorts memory-sized chunks, writes each chunk out sorted, then merges the runs — that merge is merge sort's move.

**Key point:** Databases pick merge sort because its worst case is its every case — predictable time, sequential reads, and ties stay in order.

### Visualization (canvas `c3`, 720×300)

Line chart of comparison counts against list size: merge sort's gentle guaranteed curve versus a sort with bad days blowing up on already-sorted input.

- **Title (bold 15px, `#1a5276`, top center):** "Same 64 Items: 384 Comparisons vs 2,016 on a Bad Day".
- **Axes:** origin x=80, baseline y=245, plot width 570, plot height 185; x = list size 0 to 64 with 12px `#444` tick labels at 8, 16, 32, 64; y = comparisons 0 to 2,100 with light `#e5e9ef` gridlines and 12px `#444` labels at 500, 1000, 1500, 2000; axis captions 12px `#6b7280`: "list size" bottom center, "comparisons" rotated at left.
- **Bad-day line:** red `#e74c3c` 3px dashed (dash 6/4) line through hardcoded points (size, comparisons) = `[[8, 28], [16, 120], [32, 496], [64, 2016]]`, 5px red dots; 12px red label "a sort with bad days — already-sorted input" near the top of the line.
- **Merge sort line:** green `#008300` 3px solid line through `[[8, 24], [16, 64], [32, 160], [64, 384]]`, 5px green dots; bold 12px green label "merge sort — worst-case bound" just above its right end.
- **End labels:** bold 13px value labels at size 64 — red "2,016" and green "384".
- **Annotation (bold 13px `#008300`, near x=310, y=130):** "shuffled, sorted, or reversed — never above this line".
- **Caption (12px `#444`, bottom right):** "illustrative — counts from the textbook formulas n·log₂n and n(n−1)/2".

## The Split Does No Work

**Tags:** `common mistake` (red), `where the work lives` (orange)

- **The illusion** — the halving looks like the clever part, but cutting a pile in two sorts nothing
- **Zero compares** — splitting 8 receipts down to 8 piles of one takes no comparisons at all
- **All in the merge** — every one of the 17 comparisons happens while piles are being combined
- **The price** — merging needs a spare table to lay the output on: extra space for all n receipts
- **The trade** — that scratch space is what merge sort pays for its no-bad-day guarantee

*Example (italic):* Hand someone the 8 split-up receipts and nothing is sorted yet; hand them the compare-the-tops rule and the sort finishes itself.

**Common mistake:** Crediting the split. Splitting is free and sorts nothing — budget your attention (and your memory) for the merges.

### Visualization (canvas `c4`, 720×300)

Bar chart of comparisons spent at each stage of the worked example, making the free split and the merge-heavy rounds visible.

- **Title (bold 15px, `#1a5276`, top center):** "Where the Work Lives: 0 + 4 + 6 + 7 Comparisons".
- **Axes:** origin x=80, baseline y=245, plot width 570, plot height 180; y = comparisons 0 to 8 with light `#e5e9ef` gridlines and 12px `#444` labels at 2, 4, 6, 8.
- **Bars (width 90, centers at x = 155, 300, 445, 590):** stages `["split", "round 1 (pairs)", "round 2 (fours)", "round 3 (final)"]`, comparisons `[0, 4, 6, 7]`; split drawn as a 1px dashed `#6b7280` outline of zero height sitting on the baseline; rounds 1–2 fill `rgba(42,120,214,0.35)` with 2px `#2a78d6` border; round 3 fill `rgba(0,131,0,0.25)` with 2px `#008300` border.
- **Value labels:** bold 13px above each bar — `#6b7280` "0", blue "4", blue "6", green "7"; stage names 12px `#444` below the baseline.
- **Annotation (bold 13px `#d95926`, two lines above the split bar near x=155, y=110):** "the 'divide' step" / "costs nothing".
- **Caption (12px `#444`, bottom right):** "counts from the 8-receipt worked example above".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`. Red reserved for the bad-day worst-case line only.
- **Data:** all receipt values, tree levels, line points, and bar heights are the hardcoded arrays above (no randomness); the running stack is `[38, 27, 43, 3, 9, 82, 10, 12]` everywhere and comparison counts 4, 6, 7 (total 17) must match across text, tree, and bar chart; c3 points come from n·log₂n and n(n−1)/2 at n = 8, 16, 32, 64.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
