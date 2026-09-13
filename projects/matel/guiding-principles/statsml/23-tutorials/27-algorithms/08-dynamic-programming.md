# Dynamic Programming

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Dynamic Programming

**Subtitle:** Break a big problem into small overlapping questions, write each answer down the first time you find it — and never solve the same subproblem twice

## Counting Ways Up a Staircase

**Tags:** `core idea` (blue), `overlapping subproblems` (green), `the notebook` (orange)

- **The staircase** — you climb 10 stairs taking 1 or 2 steps at a time; how many step patterns exist?
- **The split** — every climb ends with a 1-step or a 2-step, so ways(10) = ways(9) + ways(8)
- **The repeat** — ways(9) and ways(8) both need ways(7); the same question keeps coming back
- **The waste** — plain recursion re-answers every repeat from scratch, as if it never saw it before
- **The fix** — write each answer in a notebook the first time; every repeat becomes a one-line lookup

*Example (italic):* For just 5 stairs, plain recursion makes 9 calls but only 5 questions are distinct — ways(3) alone gets solved twice.

**Key point:** Dynamic programming means splitting a problem into overlapping subproblems and never solving any of them twice — recursion plus a notebook.

### Visualization (canvas `c1`, 720×300)

Recursion-tree diagram for the 5-stair climb: nine circular call nodes connected by parent–child lines, with the repeated subproblems color-highlighted so the duplicate work is visible at a glance.

- **Title (bold 15px, `#1a5276`, top center):** "Climbing 5 Stairs: the Same Subproblems Keep Coming Back".
- **Nodes:** circles radius 22, white fill, 2px border, bold 13px `#2c3e50` centered labels; positions (x, y): root "w(5)" (360, 78); level 2: "w(4)" (230, 140), "w(3)" (510, 140); level 3: "w(3)" (150, 202), "w(2)" (310, 202), "w(2)" (455, 202), "w(1)" (575, 202); level 4: "w(2)" (95, 258), "w(1)" (210, 258).
- **Edges:** 2px `#999` straight lines from each parent circle's bottom to each child circle's top: w(5)→w(4), w(5)→w(3); w(4)→w(3), w(4)→w(2); left w(3)→w(2), left w(3)→w(1); right w(3)→w(2), right w(3)→w(1).
- **Highlighting:** the two "w(3)" nodes get orange `#d95926` borders with fill `rgba(217,89,38,0.12)`; the three "w(2)" nodes get magenta `#d55181` borders with fill `rgba(213,81,129,0.10)`; "w(5)" and "w(4)" keep blue `#2a78d6` borders; the two "w(1)" nodes get mute `#6b7280` borders.
- **Duplicate link:** dashed orange (dash 5/4) 2px arc connecting the two "w(3)" circles.
- **Annotation (bold 13px orange `#d95926`, right side near x=560, y=255):** two lines: "w(3) solved twice, w(2) three times" / "— repeated work".
- **Caption (12px `#444`, bottom left):** "w(n) = ways to climb n stairs; 9 calls for only 5 distinct questions".

## Filling the Notebook by Hand

**Tags:** `worked example` (blue), `bottom-up table` (green)

- **Start small** — 1 stair has 1 way; 2 stairs have 2 ways (step-step, or one 2-step)
- **One rule** — every next entry is the sum of the two entries before it: ways(3) = 2 + 1 = 3
- **Keep going** — the notebook fills up: 1, 2, 3, 5, 8, 13, 21, 34, 55, 89 for stairs 1 through 10
- **The answer** — 89 ways to climb 10 stairs, found with 8 additions and no recursion at all
- **Two flavors** — fill the table bottom-up (tabulation) or cache on demand (memoization); same table

*Example (italic):* ways(10) = ways(9) + ways(8) = 55 + 34 = 89 — the last line of the notebook is the final answer.

**Key point:** The whole solve is one small table where each entry is written exactly once, in an order where its two ingredients are already on the page.

### Visualization (canvas `c2`, 720×300)

Bar chart of the finished notebook: one bar per stair count 1–10 with its number of ways, the final bar highlighted, and arrows showing that the last entry is just the sum of the two before it.

- **Title (bold 15px, `#1a5276`, top center):** "The Notebook: Ways to Climb 1–10 Stairs".
- **Axes:** origin x=60, baseline y=245, plot width 620, plot height 185; y = 0 to 90 with light `#e5e9ef` gridlines at 20, 40, 60, 80 and 12px `#444` tick labels; x = ten bars labeled "1".."10" (12px `#444`) under each bar, 12px `#444` axis label "stairs" centered below.
- **Bars:** values `[1, 2, 3, 5, 8, 13, 21, 34, 55, 89]`; bar width 44 with even gaps across the plot; bars 1–9 fill `rgba(42,120,214,0.35)` with 2px `#2a78d6` border; bar 10 fill `rgba(0,131,0,0.25)` with 2px `#008300` border.
- **Value labels:** bold 12px above every bar, `#2a78d6` for bars 1–9 and `#008300` for bar 10 ("1", "2", "3", "5", "8", "13", "21", "34", "55", "89").
- **Sum arrows:** two 2px `#6b7280` curved arrows with small arrowheads from the tops of bar 8 (value 34) and bar 9 (value 55) to the top of bar 10.
- **Annotation (bold 13px green `#008300`, near x=430, y=75):** two lines: "89 ways — each entry is just" / "the sum of the two before it".
- **Caption (12px `#444`, bottom right):** "exact counts — redo them by hand in a minute".

## From 109 Calls to 10

**Tags:** `why it matters` (blue), `exponential blow-up` (red), `where it's used` (green)

- **Same answer** — both versions return 89; the notebook removes the repeats and changes nothing else
- **The blow-up** — plain recursion needs 9, 15, 25, 41, 67, 109 calls as the stairs go 5, 6, ..., 10
- **The collapse** — with the notebook, 10 stairs take 10 solves and 50 stairs take exactly 50
- **At scale** — 50 stairs cost about 25 billion calls without the notebook; the answer never changes
- **Where you meet it** — spell-check edit distance, route planning, and DNA alignment all run on DP

*Example (italic):* A spell-checker comparing two 20-letter words fills one 21-by-21 grid of subanswers instead of exploring an exponential tree of retries.

**Key point:** DP turns exponential blow-ups into small tables — the entire saving comes from repeats, and repeats multiply fast as the problem grows.

### Visualization (canvas `c3`, 720×300)

Two-line growth chart: subproblems solved versus staircase size, plain recursion curving up steeply while the notebook version stays on a flat straight line.

- **Title (bold 15px, `#1a5276`, top center):** "Work Done: Plain Recursion vs Recursion with a Notebook".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 190; x = stairs 2 to 10, tick labels "2".."10" (12px `#444`), 12px `#444` axis label "stairs" below center; y = subproblems solved 0 to 120, light `#e5e9ef` gridlines at 20, 40, 60, 80, 100 with 12px `#444` tick labels.
- **Plain recursion line:** orange `#d95926` 3px line with 4px dots through (stairs, calls) = `[2, 3, 4, 5, 6, 7, 8, 9, 10]` × `[1, 3, 5, 9, 15, 25, 41, 67, 109]`; 12px orange label "no notebook" beside the point at stairs=8.
- **Notebook line:** green `#008300` 3px line with 4px dots through the same stair values × `[1, 3, 4, 5, 6, 7, 8, 9, 10]`; 12px green label "with notebook" beside the point at stairs=9, below the line.
- **End markers:** bold 13px orange label "109" just above the last orange point; bold 13px green label "10" just above the last green point.
- **Annotation (bold 13px ink `#1a5276`, near x=250, y=85):** two lines: "10 stairs: 109 calls vs 10" / "— same answer, 89, both times".
- **Caption (12px `#444`, bottom right):** "exact call counts for the 1-or-2-step climb".

## The Notebook Changes the Work, Not the Answer

**Tags:** `common mistake` (red), `memoization` (orange)

- **The suspicion** — newcomers fear the cached version is a shortcut approximation; it is not
- **Identical output** — every lookup returns exactly what the recursion would have recomputed
- **The counts** — in the 10-stair climb, plain recursion solves ways(2) 34 times and ways(3) 21 times
- **After caching** — every subproblem is solved exactly once, and the final answer is still 89
- **When it fails** — if subproblems never repeat (like halving a list), the notebook saves nothing

*Example (italic):* Sorting by repeatedly halving a list never asks about the same half twice, so caching the halves buys nothing — DP pays only when questions repeat.

**Common mistake:** Reaching for DP when subproblems don't overlap. Caching helps only when the same question gets asked more than once — check for repeats before building the table.

### Visualization (canvas `c4`, 720×300)

Grouped bar chart: for each subproblem w(1)–w(10) in the 10-stair climb, how many times plain recursion solves it (tall blue bars) versus the notebook version (flat green bars of height 1).

- **Title (bold 15px, `#1a5276`, top center):** "Times Each Subproblem Gets Solved (10-Stair Climb)".
- **Axes:** origin x=60, baseline y=245, plot width 620, plot height 180; x = ten groups labeled "w(1)".."w(10)" (11px `#444`, one per group); y = 0 to 36 with light `#e5e9ef` gridlines at 10, 20, 30 and 12px `#444` tick labels.
- **Plain recursion bars:** heights `[21, 34, 21, 13, 8, 5, 3, 2, 1, 1]` for w(1)..w(10); width 26, fill `rgba(42,120,214,0.35)`, 2px `#2a78d6` border; bold 12px `#2a78d6` value label above each bar.
- **Notebook bars:** height `1` for every subproblem, drawn immediately right of each blue bar; width 26, fill `rgba(0,131,0,0.30)`, 2px `#008300` border; 11px `#008300` label "1" above each.
- **Legend (12px, top right inside plot):** blue swatch "no notebook", green swatch "with notebook".
- **Annotation (bold 13px magenta `#d55181`, near x=330, y=80):** two lines: "w(2): solved 34 times without a notebook" / "— once with it; the answer stays 89".
- **Caption (12px `#444`, bottom right):** "exact counts; the ten blue bars sum to 109".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all bar heights, line points, tree node positions, and per-subproblem counts are the hardcoded arrays above (no randomness). The numbers are mathematically exact for the 1-or-2-step staircase: ways table `[1, 2, 3, 5, 8, 13, 21, 34, 55, 89]`, naive call counts `[1, 3, 5, 9, 15, 25, 41, 67, 109]` for stairs 2–10, per-subproblem naive solve counts `[21, 34, 21, 13, 8, 5, 3, 2, 1, 1]` (sum 109), notebook solves = one per distinct subproblem. Text and chart numbers must stay in lockstep (89, 109, 10, 34, 21).
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
