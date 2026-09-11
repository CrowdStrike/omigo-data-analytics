# Top-K Elements

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Top-K Elements

**Subtitle:** To name the 3 best sellers out of thousands you never sort the whole list — keep a tiny shortlist that the weakest member guards, or let a pivot split the list and chase only the winning side

## Three Best Sellers Out of Ten Drinks

**Tags:** `core idea` (blue), `top-k` (green), `avoid full sort` (orange)

- **The counter** — a coffee shop tallies cups sold per drink and the owner wants tonight's top 3 sellers
- **The day's tally** — the ten drinks sold 42, 17, 35, 29, 12, 51, 23, 31, 26, 19 cups, in that order
- **Top-k** — "give me the k best out of n" is called top-k; here k = 3 drinks out of n = 10
- **The waste** — sorting all ten answers questions nobody asked, like which drink came 7th
- **The insight** — to name the top 3 you only ever need to remember 3 candidates at a time

*Example (italic):* The owner wants "best three sellers today" — Cold Brew (51), Latte (42), Mocha (35) — and does not care how Tea and Chai rank below them.

**Key point:** Top-k asks for the k best out of n; when k is tiny, sorting all n items is far more work than the question needs.

### Visualization (canvas `c1`, 720×300)

Single-panel vertical bar chart: the ten drinks' cup counts in arrival order, with the three winners in green and everyone else in muted blue.

- **Title (bold 15px, `#1a5276`, top center):** "One Day at the Counter: Cups Sold per Drink (top 3 in green)".
- **Axes:** origin x=60, baseline y=245, plot width 620, plot height 185; y = cups 0 to 55 with light `#e5e9ef` gridlines and 12px `#444` labels at 10, 20, 30, 40, 50; x = ten bars in arrival order.
- **Bars:** width 40, gap 22, first bar at x=70; heights from cups = `[42, 17, 35, 29, 12, 51, 23, 31, 26, 19]`; 11px `#444` labels under the baseline: "Latte", "Espresso", "Mocha", "Capp.", "Tea", "Cold Brew", "Cocoa", "Flat Wh.", "Chai", "Amer.".
- **Fills:** the top-3 bars (Cold Brew 51, Latte 42, Mocha 35) fill `rgba(0,131,0,0.55)` with 2px `#008300` border; the other seven fill `rgba(42,120,214,0.30)` with 1px `#2a78d6` border; bold 12px value label above every bar.
- **Annotation (bold 13px orange `#d95926`, near x=400, y=70):** two lines: "only 3 of the 10 ranks matter —" / "why pay to sort all 10?".
- **Caption (12px `#444`, bottom right):** "illustrative — one day of cup counts".

## A Shortlist of Three, One Tally at a Time

**Tags:** `worked example` (blue), `min-heap` (green), `bar to beat` (orange)

- **The shortlist** — hold the first three tallies 42, 17, 35; the weakest of them, 17, is the bar to beat
- **Beat it or bounce** — 29 beats 17, so 17 drops and the bar rises to 29; then 12 loses and bounces off
- **Rising bar** — 51 beats 29, so the bar climbs to 35; then 23, 31, 26, 19 all fail and are skipped
- **The answer** — after one pass the shortlist reads 35, 42, 51 — exactly the top 3, no sorting done
- **The heap** — a min-heap is the gadget that hands you the shortlist's weakest member instantly
- **The cost** — every tally gets one look at the bar, plus a small log k fix-up for the few that enter

*Example (italic):* Ten arrivals, one comparison each against the current weakest, and only 5 ever enter the shortlist — the top 3 falls out of a single pass.

**Key point:** Keep a min-heap of size k; each newcomer fights only the weakest member, so one pass over n items costs about n log k.

### Visualization (canvas `c2`, 720×300)

Single-panel dot-and-step chart: each arrival plotted at its cup count, green if it enters the shortlist and hollow if it bounces, with a violet step line tracking the shortlist's minimum — the "bar to beat".

- **Title (bold 15px, `#1a5276`, top center):** "One Pass, a 3-Slot Shortlist: the Bar to Beat Only Rises".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 185; y = cups 0 to 55 with light `#e5e9ef` gridlines at 10–50 (12px `#444` labels); x = arrival order 1–10 at slot centers x = 90 + (i−1)·62, 12px `#444` tick labels "1"–"10", 12px `#444` axis caption "arrival order" bottom center.
- **Arrival dots:** values `[42, 17, 35, 29, 12, 51, 23, 31, 26, 19]` at slots 1–10; admitted arrivals (slots 1, 2, 3, 4, 6 = values 42, 17, 35, 29, 51) are 7px filled green `#008300` dots; rejected arrivals (slots 5, 7, 8, 9, 10 = values 12, 23, 31, 26, 19) are 7px hollow `#6b7280` circles; 12px value label above each dot.
- **Bar-to-beat step line:** violet `#4a3aa7` 3px step line from slot 3 to slot 10 at heights `[17, 29, 29, 35, 35, 35, 35, 35]` (the shortlist minimum after each arrival); bold 12px violet label "bar to beat" above the line near slot 5.
- **Evictions:** 11px `#6b7280` notes just above the baseline under slots 4 and 6: "17 out" and "29 out".
- **Annotation (bold 13px green `#008300`, near x=460, y=80):** "final shortlist: 35, 42, 51 — the top 3 in one pass".
- **Caption (12px `#444`, bottom right):** "green = enters the shortlist, hollow = bounced".

## Quickselect: Let a Pivot Split the List

**Tags:** `where it's used` (blue), `quickselect` (green), `pivot` (orange)

- **The pivot** — grab one tally, say 29, and split the others into "beats 29" and "doesn't beat 29"
- **One split** — the winners' side holds 42, 35, 51, 31: four drinks, so the top 3 all live in there
- **Chase one side** — repeat inside those four with pivot 35: only 42 and 51 beat it, so top 3 = 51, 42, 35
- **Shrinking fast** — each round discards most of the list, so n + n/2 + n/4 + ... ≈ 2n looks on average
- **Picking sides** — the heap wins for streams and tiny k; quickselect wins when the list sits in memory
- **The scale** — for n = 10,000 orders: full sort ≈ 133,000 steps, heap ≈ 16,000, quickselect ≈ 20,000

*Example (italic):* On the ten tallies, two pivot rounds cost 12 comparisons in total and the top 3 popped out — unsorted, but complete.

**Key point:** Quickselect finds the top k in about 2n steps on average, but it needs the whole list at hand and returns the winners unordered.

### Visualization (canvas `c3`, 720×300)

Single-panel horizontal bar chart: illustrative step counts for the three strategies at n = 10,000 and k = 3, making the full sort's wasted work visible.

- **Title (bold 15px, `#1a5276`, top center):** "Steps to Find the Top 3 of 10,000 Orders".
- **Layout:** three horizontal bars at row centers y = 95, 155, 215, bar height 34; left-aligned 12px `#444` row labels ending at x=200: "full sort", "3-slot heap", "quickselect"; bar scale from x=210 to x=690 mapped to 0–140,000 steps; light `#e5e9ef` vertical gridlines at 50,000 and 100,000 with 11px `#6b7280` labels "50k" and "100k" below the bottom bar.
- **Bars:** full sort = 133,000, fill `#6b7280`; 3-slot heap = 16,000, fill `#2a78d6`; quickselect = 20,000, fill `#d95926`; bold 12px value labels just past each bar end: "≈133,000", "≈16,000", "≈20,000".
- **Annotation (bold 13px green `#008300`, near x=420, y=140):** "full sort does ~8× the heap's work".
- **Caption (12px `#444`, bottom right):** "illustrative — sort ≈ n log₂ n, heap ≈ n log₂ k, quickselect ≈ 2n".

## Challenge the Weakest, Not the Champion

**Tags:** `common mistake` (red), `min vs max` (orange)

- **The reflex** — "I want the biggest, so build a max-heap" — but that heap must hold all n items, not k
- **The trick** — the shortlist is a min-heap of size k: its root is the weakest member, ready to fight
- **Why min** — a newcomer can only ever evict the weakest; the current champion never needs checking
- **The moment** — when 51 arrived the shortlist was 29, 35, 42; it fought 29 once, and 29 left
- **Wasted memory** — a max-heap of everything still works, but stores 10,000 tallies to report 3

*Example (italic):* An engineer max-heaps a million rows to fetch the top 3, when a 3-slot min-heap and one pass would have done the same job.

**Common mistake:** Reaching for a max-heap because the goal says "maximum". Top-k keeps a min-heap of k items — the minimum is the only member a newcomer can ever replace.

### Visualization (canvas `c4`, 720×300)

Single-panel diagram of one moment from the worked pass: the 3-slot shortlist as circles inside a box, newcomer 51 arriving, and a curved arrow showing it fights only the weakest member, 29.

- **Title (bold 15px, `#1a5276`, top center):** "The Newcomer Fights Only the Weakest Member".
- **Shortlist box:** rounded rectangle from (100, 95) to (420, 215), 1.5px `#1a5276` border, fill `rgba(42,120,214,0.06)`; 12px `#1a5276` label "shortlist (min-heap, k = 3)" above the box at y=85.
- **Members:** three 26px circles centered at y=155, x = 160, 260, 360, holding bold 13px values "29", "35", "42"; the 29 circle stroked 3px orange `#d95926` with 11px orange label "weakest — the root" below it at y=195; the 35 and 42 circles stroked 2px `#2a78d6`.
- **Newcomer:** 26px circle stroked 3px green `#008300` at (560, 155) holding bold 13px "51"; 12px `#444` label "newcomer" above it at y=120.
- **Arrow:** 3px green `#008300` arrow arcing above the box (quadratic curve from about (545, 138) through (360, 65) to (178, 135)) with an arrowhead at the 29 circle; bold 12px green label "beats 29 → 29 leaves" near the arc's top at (360, 55).
- **Annotation (bold 13px magenta `#d55181`, centered near y=270):** "51 challenges 29, never 42 — after the swap the shortlist reads 35, 42, 51".
- **Caption (12px `#444`, bottom right):** "moment from the worked pass — arrival 6 of 10".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all bar heights, dot values, step-line heights, and bar lengths are the hardcoded arrays above (no randomness); the 133,000 / 16,000 / 20,000 step counts are illustrative order-of-magnitude figures and stay labeled as such; every number drawn in a chart matches the worked example's text.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
