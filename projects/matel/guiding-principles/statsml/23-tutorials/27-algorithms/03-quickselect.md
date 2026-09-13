# Quickselect

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Quickselect

**Subtitle:** Quickselect finds the median — or any k-th smallest value — by splitting the pile around a pivot and chasing only the half that holds your answer, no full sort needed

## The Middle Delivery Without Lining Them All Up

**Tags:** `core idea` (blue), `pivot & piles` (green), `throw half away` (orange)

- **The shop** — a pizza shop logs 11 delivery times tonight and wants the typical (median) one
- **The median** — line the 11 times up by size and the median is whoever sits in seat 6
- **No lineup needed** — pick any time as a pivot and split the rest into a smaller pile and a bigger pile
- **Count seats** — 6 times are smaller than pivot 34, so 34 sits in seat 7 and seat 6 is in the smaller pile
- **Toss the rest** — the bigger pile can never hold seat 6, so throw it away and repeat on the smaller pile

*Example (italic):* Pivot 34 splits tonight's times into 6 smaller and 4 bigger — the median hides among those 6, and the 4 bigger times are never looked at again.

**Key point:** Quickselect finds the k-th smallest value by splitting around a pivot and chasing only the pile that contains seat k.

### Visualization (canvas `c1`, 720×300)

Two-row partition diagram: the 11 logged times as boxes on top, an arrow down to the same 11 rearranged into smaller pile / pivot / bigger pile, with seat ranges labeled under each pile.

- **Title (bold 15px, `#1a5276`, top center):** "One Pivot Splits 11 Delivery Times into Two Piles".
- **Top row (y=58, boxes 46×30, stride 51, start x=80):** values in logged order `[34, 18, 41, 26, 52, 29, 23, 45, 31, 20, 38]`, bold 12px centered value in each box; pivot box "34" filled ink `#1a5276` with white text, all others white fill with 1px `#6b7280` border; 12px `#444` label "tonight's 11 times (minutes)" above the row at x=80, y=48.
- **Arrow:** 2px `#6b7280` vertical arrow from (360, 95) to (360, 138) with arrowhead; 12px `#444` label to its right: "compare each to the pivot 34".
- **Bottom row (y=150, boxes 46×30, stride 51):** smaller pile `[18, 26, 29, 23, 31, 20]` starting x=80, fill `rgba(42,120,214,0.25)`, border `#2a78d6`; pivot "34" ink box at x=400; bigger pile `[41, 52, 45, 38]` starting x=470, fill `rgba(217,89,38,0.20)`, border `#d95926`.
- **Seat labels (12px, y=200):** "seats 1–6" in `#2a78d6` centered under the blue pile, "seat 7" in `#1a5276` under the pivot, "seats 8–11" in `#d95926` centered under the orange pile.
- **Annotation (bold 13px `#2a78d6`, centered near y=245):** "median = seat 6 → it must be in the smaller pile; the 4 bigger times are never touched again".
- **Caption (12px `#444`, bottom right):** "illustrative delivery times, minutes".

## Two Rounds from 11 Times to the Median

**Tags:** `worked example` (blue), `hand-checkable` (green)

- **Tonight's times** — 34, 18, 41, 26, 52, 29, 23, 45, 31, 20, 38 minutes; the median is seat 6 of 11
- **Round 1** — pivot 34: smaller pile {18, 26, 29, 23, 31, 20}, bigger pile {41, 52, 45, 38} tossed
- **Round 2** — pivot 29 inside the pile of 6: smaller {18, 26, 23, 20} take seats 1–4, 29 takes seat 5
- **One left** — seat 6 must be the lone time bigger than 29: the median is 31 minutes, in two rounds
- **The work** — 10 + 5 = 15 comparisons, versus roughly 38 to fully sort all 11 times

*Example (italic):* Check by hand: sorted, the list reads 18, 20, 23, 26, 29, 31, 34, 38, 41, 45, 52 — and 31 indeed sits in seat 6.

**Key point:** Each round compares every survivor to one pivot and keeps one pile: 11 in play, then 6, then 1.

### Visualization (canvas `c2`, 720×300)

Three-row drill-down: each round's numbers as boxes, with the kept pile colored, the tossed pile greyed out, and the pivot in ink — ending at the single green median box.

- **Title (bold 15px, `#1a5276`, top center):** "Two Rounds: 11 in Play, then 6, then 1".
- **Row layout:** boxes 40×28, stride 44, starting x=190; left-aligned 12px `#444` row labels at x=20, vertically centered on each row.
- **Row 1 (y=62), label "round 1 — pivot 34":** boxes `[34, 18, 41, 26, 52, 29, 23, 45, 31, 20, 38]`; "34" ink `#1a5276` fill with white bold 12px text; smaller-pile members (18, 26, 29, 23, 31, 20) fill `rgba(42,120,214,0.25)` border `#2a78d6`; tossed members (41, 52, 45, 38) fill `#eef1f4` border `#c8cdd4` with 11px `#6b7280` text.
- **Row 2 (y=130), label "round 2 — pivot 29":** boxes `[18, 26, 29, 23, 31, 20]`; "29" ink with white text; "31" (the kept bigger pile) fill `rgba(0,131,0,0.20)` border `#008300`; 18, 26, 23, 20 greyed as above.
- **Row 3 (y=198), label "round 3 — one left":** single box "31" solid `#008300` fill, white bold 12px text, at x=190; bold 13px `#008300` label to its right: "median = 31 minutes".
- **Annotation (bold 12px `#d95926`, near x=500, y=210):** two lines: "15 comparisons total (10 + 5) —" / "a full sort needs about 38".
- **Caption (12px `#444`, bottom right):** "grey boxes are thrown away, never compared again — illustrative".

## Why It Matters When the List Is a Million Rows

**Tags:** `where it's used` (blue), `speed` (green), `percentiles` (orange)

- **Medians at scale** — median income, median house price, median session length are all "seat k" questions
- **Percentiles too** — p95 latency is just seat 950,000 of a million; quickselect fetches it the same way
- **Sorting overworks** — a full sort orders every value, but you only asked where one seat is
- **The cost** — halving piles costs about 2n steps, a few n in practice, versus n·log n to sort
- **Real libraries** — numpy's `np.partition` and C++ `nth_element` are quickselect under the hood

*Example (italic):* With a million delivery records, a full sort costs about 20,000,000 steps while quickselect with halving splits needs about 2,000,000 — a tenth of the work.

**Key point:** When you need one order statistic — a median, a p95, a top-k cutoff — selection beats sorting.

### Visualization (canvas `c3`, 720×300)

Two horizontal bars on a shared step-count axis: full sort versus quickselect on a million records, making the tenth-of-the-work gap visible at a glance.

- **Title (bold 15px, `#1a5276`, top center):** "A Million Records: Steps to Find the Median".
- **Scale:** x=195 to x=665 spans 0 to 20,000,000 steps (470px); axis line 2px `#999` at y=230 with 12px `#444` tick labels "0", "5M", "10M", "15M", "20M" below, evenly spaced.
- **Bar labels (13px `#444`, right-aligned at x=180):** "full sort" at y=110, "quickselect" at y=190.
- **Sort bar:** rect x=195, y=95, width 470, height 30; fill `rgba(26,82,118,0.35)`, border `#1a5276`; bold 13px `#1a5276` label above its right end: "≈ 20,000,000 steps (n·log n)".
- **Quickselect bar:** rect x=195, y=175, width 47, height 30; fill `rgba(0,131,0,0.30)`, border `#008300`; bold 13px `#008300` label to the right of the bar: "≈ 2,000,000 steps with halving splits (≈ 2n)".
- **Annotation (bold 13px `#008300`, centered near x=430, y=265):** "one seat asked for → about a tenth of the work".
- **Caption (12px `#444`, bottom right):** "illustrative step counts".

## Quickselect Is Not Quicksort

**Tags:** `common mistake` (red), `worst case` (orange)

- **Same pivot trick** — both split around a pivot, but quicksort recurses into both piles, quickselect into one
- **Nothing gets sorted** — after quickselect the list is still shuffled; only seat k's value is guaranteed
- **Bad pivots hurt** — always drawing the smallest value as pivot shrinks the pile by 1 each round: n² steps
- **Random pivots** — picking pivots at random makes that slow streak astronomically unlikely in practice
- **Ties are fine** — equal values can go in either pile (or a third "equal" pile); the seat count still works

*Example (italic):* An analyst ran quickselect for the median, then printed the "sorted" list — it came out shuffled, because only seat 6 was ever pinned down.

**Common mistake:** Expecting a sorted list back. Quickselect answers "what value sits in seat k" and leaves every other value unordered.

### Visualization (canvas `c4`, 720×300)

Two side-by-side recursion diagrams built from pile-size bars: quicksort keeps splitting every pile, quickselect follows one pile and greys out the rest.

- **Title (bold 15px, `#1a5276`, top center):** "Quicksort Splits Both Piles — Quickselect Follows One".
- **Panels:** left panel centered on x=200, right panel centered on x=520; bold 13px `#1a5276` panel titles "quicksort" at (200, 55) and "quickselect" at (520, 55); light 1px `#e5e9ef` vertical divider at x=360 from y=60 to y=250.
- **Bar scale:** 20px of width per number in the pile, bar height 22, 11px count label centered inside each bar.
- **Left panel (all bars fill `rgba(42,120,214,0.25)`, border `#2a78d6`):** level 1 (y=75) one bar of 11 (width 220, x=90–310); level 2 (y=135) bars of 6 (x=90–210) and 4 (x=230–310); level 3 (y=195) bars of 4, 1, 2, 1 (widths 80, 20, 40, 20 with 10px gaps starting x=90).
- **Left caption (12px `#444`, centered x=200, y=245):** "every pile gets split — total ≈ n·log n".
- **Right panel:** level 1 (y=75) bar of 11 (x=410–630) blue as above; level 2 (y=135) kept bar of 6 (x=410–530) blue, tossed bar of 4 (x=550–630) fill `#eef1f4` with dashed `#6b7280` border and 11px `#6b7280` label "tossed"; level 3 (y=195) kept bar of 1 (x=410–430) fill `rgba(0,131,0,0.30)` border `#008300`, tossed bar of 4 (x=450–530) grey dashed "tossed".
- **Right caption (12px `#444`, centered x=520, y=245):** "one pile followed each round — ≈ 2n if piles halve".
- **Annotation (bold 13px magenta `#d55181`, centered x=360, y=282):** "same pivot trick, different recursion — that's the whole difference".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all box values, pile memberships, bar widths, and step counts are the hardcoded literals above (no randomness); the worked example's numbers in the text must match the boxes in `c1`/`c2` exactly (median 31, piles of 6 and 4, then 4 and 1, 15 comparisons); the million-record step counts in `c3` are illustrative and labeled as such.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
