# Lazy Evaluation

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Lazy Evaluation

**Subtitle:** Lazy evaluation means work is not done when it is written down — it is done the first time someone actually looks at the answer, and never if nobody looks

## The Barista Who Waits for the Order

**Tags:** `core idea` (blue), `on demand` (green), `wasted work` (orange)

- **Two baristas** — a coffee shop has a 12-drink menu, and each drink needs a prep base that takes 3 minutes
- **The eager one** — makes all 12 bases at opening time, before a single customer has walked in
- **The lazy one** — makes nothing at open; a base gets made the first time someone orders that drink
- **The day's truth** — customers only ever order 5 of the 12 drink types on the menu
- **The waste** — the eager barista made 7 bases that no one ordered; the lazy one made exactly 5
- **The name** — computing a value only when it is first looked at is called lazy evaluation

*Example (italic):* Nobody ordered a lavender latte today, so the lazy barista never made lavender base — the eager one made it at 9:00 and poured it out at close.

**Key point:** Lazy evaluation delays work until the answer is actually needed — and work whose answer is never needed simply never happens.

### Visualization (canvas `c1`, 720×300)

Two-bar comparison: prep bases made by the eager barista vs the lazy barista, with a dashed guide line at the number of drink types customers actually ordered.

- **Title (bold 15px, `#1a5276`, top center):** "12-Drink Menu, One Day: Bases Made vs Bases Actually Ordered".
- **Axes:** origin x=110, baseline y=245, plot width 500, plot height 180; y = bases made 0 to 12 with 12px `#444` tick labels at 0, 2, 4, 6, 8, 10, 12 and light `#e5e9ef` gridlines; no x-axis line beyond the baseline.
- **Bars (width 130):** "eager — make everything at open" centered at x=250, height for value 12, fill `rgba(42,120,214,0.35)`, 2px blue `#2a78d6` border; "lazy — make on first order" centered at x=470, height for value 5, fill `rgba(0,131,0,0.30)`, 2px green `#008300` border; bold 13px value labels "12" and "5" above the bars; 12px `#444` category labels below the baseline.
- **Guide line:** horizontal dashed `#6b7280` (dash 4/3) line at value 5 across the plot; 12px `#6b7280` label at its right end: "5 drink types actually ordered".
- **Annotation (bold 12px orange `#d95926`, next to the eager bar's top, two lines):** "7 bases made" / "that nobody drank".
- **Caption (12px `#444`, bottom right):** "illustrative — one invented day at the coffee shop".

## A Day of Orders, Minute by Minute

**Tags:** `worked example` (blue), `pay at first look` (green)

- **The cost** — every base takes 3 minutes, whichever barista makes it and whenever she makes it
- **Eager total** — 12 bases × 3 minutes = 36 minutes of prep, all paid before the door opens at 9:00
- **Lazy trickle** — first orders arrive at 9:10, 9:25, 10:05, 11:30, and 13:15; each triggers one 3-minute base
- **Lazy total** — 5 bases × 3 minutes = 15 minutes, spread across the day instead of stacked at open
- **The saving** — 36 − 15 = 21 minutes never worked, because 7 drinks were never looked at
- **The price** — the 9:10 customer waits 3 minutes while her base is made; eager customers never wait

*Example (italic):* At 9:25 the first mocha order lands, the barista spends 3 minutes on mocha base, and her running total ticks from 3 to 6 minutes.

**Key point:** Eager pays 36 minutes up front; lazy pays 3 minutes at each first look and ends the day at 15 — the 21-minute gap is work nobody ever needed.

### Visualization (canvas `c2`, 720×300)

Step chart of cumulative prep minutes over the working day: the eager barista's flat line at 36 from opening, and the lazy barista's staircase climbing 3 minutes at each first order.

- **Title (bold 15px, `#1a5276`, top center):** "Cumulative Prep Minutes: Eager Pays at 9:00, Lazy Pays per First Look".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 185; x = time of day 9:00 to 17:00 with 12px `#444` tick labels "9:00", "10:00", ..., "17:00" every hour; y = cumulative minutes 0 to 40 with 12px `#444` labels at 0, 10, 20, 30, 40 and light `#e5e9ef` gridlines.
- **Eager line:** blue `#2a78d6` 3px line: vertical rise at x=9:00 from 0 to 36, then horizontal at 36 to x=17:00; 12px blue label "eager: 36 min before opening" above the line near x=10:30.
- **Lazy staircase:** green `#008300` 3px stepped line through hardcoded points `[(9:00, 0), (9:10, 3), (9:25, 6), (10:05, 9), (11:30, 12), (13:15, 15), (17:00, 15)]` (horizontal-then-vertical steps); green 6px dots at each of the five step tops with 11px `#444` drink labels "latte", "mocha", "chai", "matcha", "flat white" staggered above/below to avoid overlap.
- **Gap marker:** vertical dashed `#6b7280` (dash 4/3) line at x=16:00 between y-values 15 and 36, with 12px `#6b7280` label "21 min never worked" beside it.
- **Annotation (bold 12px green `#008300`, near x=14:00, y for value 20, two lines):** "lazy ends the day at 15 min —" / "only drinks someone looked at".
- **Caption (12px `#444`, bottom right):** "illustrative order times".

## Where a Data Scientist Meets It: the Recipe That Runs Late

**Tags:** `where it's used` (blue), `dataframes` (green), `generators` (orange)

- **Big-data frames** — in Spark and Polars, `load`, `filter`, `average` build a recipe; nothing runs yet
- **The trigger** — the work happens only when someone looks: `.show()`, `.collect()`, or a saved file
- **Skipping rows** — preview 50 rows of a 1,000,000-row file and only those 50 rows get computed
- **Generators** — Python's `range` and generators hand out one value per request, never the whole list
- **Endless menus** — laziness lets code describe an infinite sequence, since only the viewed part exists
- **Free reordering** — because the recipe runs late, the engine can reshuffle steps to touch less data

*Example (italic):* An analyst chains three transforms on a 1,000,000-row orders table and the notebook returns instantly — the 3 steps only execute when `.show()` asks for 50 rows.

**Key point:** Lazy pipelines separate describing the work from doing it — the plan is written eagerly, but rows are only computed when a result is actually looked at.

### Visualization (canvas `c3`, 720×300)

Left-to-right pipeline diagram: three grey recipe boxes chained by arrows (plan only, nothing computed), then a green trigger box where `.show()` finally runs the work on just the rows requested.

- **Title (bold 15px, `#1a5276`, top center):** "Three Steps Written, Zero Rows Touched — Until Someone Looks".
- **Recipe boxes:** three rounded rectangles 150×60 at y=110, left edges x=30, 210, 390; fill `#f8f9fa`, 2px `#6b7280` border; bold 13px `#2c3e50` labels centered: "load orders.csv", "filter to June", "add discount column"; 11px `#6b7280` sub-label under each box: "plan only".
- **Arrows:** 2px `#6b7280` arrows with solid arrowheads between consecutive boxes, and from the third box to the trigger box.
- **Trigger box:** rounded rectangle 130×60 at y=110, left edge x=570; fill `rgba(0,131,0,0.12)`, 3px green `#008300` border; bold 14px green label centered: ".show()"; bold 12px green label under the box: "someone looks — it runs NOW".
- **Row counter:** 12px `#444` line under the recipe boxes at y=215: "file on disk: 1,000,000 rows"; bold 13px green line at y=240 under the trigger box side: "rows actually computed: 50".
- **Annotation (bold 12px violet `#4a3aa7`, top left area near x=40, y=70):** "written at 9:00, executed at 9:01 — only because .show() was called".
- **Caption (12px `#444`, bottom right):** "illustrative pipeline — row counts invented".

## Lazy Is Not the Same as Remembered

**Tags:** `common mistake` (red), `caching` (orange)

- **The mix-up** — people assume a lazy value, once computed, is kept; plain laziness promises no such thing
- **Look twice, pay twice** — the lazy barista who pours out the chai base redoes 3 minutes at the next chai order
- **Keep the jug** — caching (memoization) stores the first result, so the second look costs 0 minutes
- **In pipelines** — Spark re-runs the whole recipe on every `.collect()` unless you explicitly `.cache()`
- **The tell** — a notebook where the same cell gets slower with reuse is usually lazy-without-cache

*Example (italic):* Two chai orders, no jug kept: 3 minutes plus 3 minutes; with the jug cached, 3 minutes then 0.

**Common mistake:** Treating lazy as "computed once, free forever". Laziness only delays the first computation — remembering the result is a separate choice called caching, and skipping it means every look pays again.

### Visualization (canvas `c4`, 720×300)

Grouped bar chart: minutes paid at the first look vs the second look, for lazy-without-cache and lazy-plus-cache, showing that only the cache makes the second look free.

- **Title (bold 15px, `#1a5276`, top center):** "Two Chai Orders: Lazy Alone vs Lazy + Cache".
- **Axes:** origin x=110, baseline y=245, plot width 500, plot height 170; y = minutes paid 0 to 4 with 12px `#444` tick labels at 0, 1, 2, 3, 4 and light `#e5e9ef` gridlines; two x groups centered at x=260 and x=490 with 13px `#444` labels below: "first look (order #1)", "second look (order #2)".
- **Bars (width 70, 16px gap within a group):** series "lazy, no cache" in orange `#d95926` fill `rgba(217,89,38,0.35)` with 2px orange border, values `[3, 3]`; series "lazy + cache" in green `#008300` fill `rgba(0,131,0,0.30)` with 2px green border, values `[3, 0]` (the 0 bar drawn as a bold 3px green baseline tick); bold 13px value labels "3 min", "3 min", "3 min", "0 min" above each bar in the series color.
- **Legend (12px, top right inside plot):** orange swatch "lazy, no cache"; green swatch "lazy + cache".
- **Annotation (bold 12px magenta `#d55181`, centered above the second group at y=95, two lines):** "without a cache, every look" / "re-does the 3 minutes".
- **Caption (12px `#444`, bottom right):** "illustrative — 3-minute chai base from the running example".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all bar heights, staircase points, order times, and row counts are the hardcoded literals above (no `Math.random()`); the staircase in `c2` must reproduce exactly `(9:00,0) → (9:10,3) → (9:25,6) → (10:05,9) → (11:30,12) → (13:15,15) → (17:00,15)`, and every number shown in a chart must match the same number in the section's text (12, 5, 7, 3, 36, 15, 21, 50, 1,000,000).
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
