# Two Pointers & Sliding Windows

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Two Pointers & Sliding Windows

**Subtitle:** Two fingers walking down sorted lists, or one window sliding over a stream — a way to answer join, dedup, and running-total questions in a single pass instead of rescanning everything

## Two Fingers Down Two Lists

**Tags:** `core idea` (blue), `one pass` (green), `sorted lists` (orange)

- **The coffee shop** — the owner has a sorted member list and a sorted list of today's buyer IDs
- **The question** — which of today's buyers are members? Members: 3, 8, 12, 17, 21, 25, 30
- **Today's buyers** — 5, 8, 9, 17, 22, 25, 28; the naive way checks every buyer against every member
- **Two fingers** — put one finger at the start of each list and compare where the fingers point
- **The rule** — equal means a match, write it down; otherwise move the finger on the smaller number
- **One pass** — each finger only moves forward, so after 10 comparisons all 3 matches are found

*Example (italic):* Fingers start at 3 and 5; 3 is smaller so the member finger moves to 8, and now both fingers say 8 — the first match, found without rescanning anything.

**Key point:** Because both lists are sorted, a match can never be behind a finger — so fingers only move forward, and the whole join finishes in one pass.

### Visualization (canvas `c1`, 720×300)

Two horizontal rows of numbered boxes (members on top, today's buyers below), matching values connected and highlighted, with two arrow markers showing the fingers meeting at a match.

- **Title (bold 15px, `#1a5276`, top center):** "One Pass, Two Fingers: Matching Buyers to Members".
- **Member row (y=95):** seven 58px-wide, 36px-tall rounded boxes starting at x=90, 14px gap, values `[3, 8, 12, 17, 21, 25, 30]` in bold 13px; row label "members (sorted)" in 12px `#444` at x=90, y=75.
- **Buyer row (y=195):** seven boxes same geometry, values `[5, 8, 9, 17, 22, 25, 28]`; row label "today's buyers (sorted)" in 12px `#444` at x=90, y=250.
- **Box style:** non-matches white fill, 2px `#6b7280` border, `#2c3e50` text; matches (8, 17, 25 in both rows) fill `rgba(0,131,0,0.15)`, 2px green `#008300` border, green bold text.
- **Match connectors:** 2px green `#008300` lines joining the vertical centers of each matching pair (8–8, 17–17, 25–25).
- **Finger markers:** blue `#2a78d6` filled triangle pointing down above the member box "8" and one pointing up below the buyer box "8", each with bold 12px blue label "finger" beside it.
- **Annotation (bold 13px orange `#d95926`, right-aligned at x=700, y=150):** two lines: "3 matches in 10 comparisons —" / "fingers never move backward".
- **Caption (12px `#444`, bottom right):** "illustrative IDs — real lists would hold thousands".

## A Week of Sales, Three Days at a Time

**Tags:** `worked example` (blue), `sliding window` (green)

- **Daily cups** — Mon through Sun the shop sells 40, 55, 30, 70, 45, 60, 50 cups
- **The window** — the owner wants every 3-day total: Mon–Wed is 40 + 55 + 30 = 125
- **The lazy slide** — for Tue–Thu, drop Mon's 40 and add Thu's 70: 125 − 40 + 70 = 155
- **Keep sliding** — Wed–Fri: 155 − 55 + 45 = 145; Thu–Sat: 145 − 30 + 60 = 175; Fri–Sun: 175 − 70 + 50 = 155
- **Check by hand** — re-adding Thu–Sat gives 70 + 45 + 60 = 175, exactly what the slide produced
- **Two ops per day** — each new total costs one add and one drop, never a re-sum of the window

*Example (italic):* The five 3-day totals — 125, 155, 145, 175, 155 — took one full sum and then just eight single additions or subtractions.

**Key point:** A sliding window never recomputes: new total = old total − the day leaving + the day entering, so every window after the first costs two operations.

### Visualization (canvas `c2`, 720×300)

Bar chart of the seven daily sales with the Thu–Sat window shaded, and the five 3-day window totals drawn as a green dot-and-line series above the bars at their window centers.

- **Title (bold 15px, `#1a5276`, top center):** "Daily Cups and the Sliding 3-Day Total".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 185; x = days with 12px `#444` labels "Mon"–"Sun" centered under each bar; y = 0 to 200 with light `#e5e9ef` gridlines at 50, 100, 150 and 12px `#444` tick labels.
- **Bars:** seven bars, width 52px, evenly spaced, fill `rgba(42,120,214,0.35)`, 1px `#2a78d6` border, heights from values `[40, 55, 30, 70, 45, 60, 50]`; bold 12px `#2a78d6` value label above each bar.
- **Window shade:** translucent orange `rgba(217,89,38,0.12)` rectangle with 2px dashed `#d95926` border spanning the Thu, Fri, Sat bars from baseline to y=70.
- **Window totals:** green `#008300` 2px line through 7px dots at the window-center days Tue, Wed, Thu, Fri, Sat with values `[125, 155, 145, 175, 155]` on the same y scale; bold 12px green label above each dot.
- **Annotation (bold 12px orange `#d95926`, above the shaded window near y=55):** two lines: "Thu–Sat: 145 + 60 − 30 = 175" / "drop Wed, add Sat".
- **Caption (12px `#444`, bottom right):** "illustrative — one week of coffee sales".

## Where a Data Scientist Meets This

**Tags:** `where it's used` (blue), `dedup` (green), `stream aggregation` (orange)

- **Dedup** — on a sorted export, a slow and a fast pointer drop repeats in one pass, no hash table
- **Merge joins** — databases join two tables sorted on the key exactly like the two-finger match
- **Streaming dashboards** — "orders in the last 30 days" is a window sum kept alive by add-and-drop
- **The cost gap** — re-summing a 30-day window for 365 days costs 10,950 additions; sliding costs 730
- **Memory** — the window only ever holds 30 values, so a year-long stream never fills memory

*Example (italic):* A dashboard that re-sums its 30-day revenue window every midnight does 15 times more work than one that subtracts the day that fell out and adds the day that came in.

**Key point:** Two pointers and sliding windows turn "compare everything with everything" into "touch each item a constant number of times" — the difference between 10,950 and 730 operations over one year.

### Visualization (canvas `c3`, 720×300)

Two horizontal bars comparing total additions over a 365-day stream with a 30-day window: re-summing every day versus sliding, with the 15x gap called out.

- **Title (bold 15px, `#1a5276`, top center):** "One Year of a 30-Day Window: Re-Sum vs Slide".
- **Layout:** horizontal bars starting at x=250, max bar width 420 scaled to 10,950; baseline vertical 2px `#999` line at x=250 from y=70 to y=230; x tick labels "0", "5,000", "10,000" in 12px `#444` at y=250.
- **Row 1 (y=110):** left label "re-sum the window daily" in 12px `#444` at x=20; bar 26px tall, fill `rgba(217,89,38,0.35)`, 2px `#d95926` border, length for 10,950; bold 13px `#d95926` value label "10,950 additions" at the bar's right end.
- **Row 2 (y=185):** left label "slide: one add, one drop" in 12px `#444` at x=20; bar 26px tall, fill `rgba(0,131,0,0.35)`, 2px `#008300` border, length for 730; bold 13px `#008300` value label "730 operations" at the bar's right end.
- **Annotation (bold 13px violet `#4a3aa7`, centered near x=470, y=185):** "15x less work — same answers every day".
- **Caption (12px `#444`, bottom right):** "365 days, 30-day window; counts are exact for this setup".

## The Catch: Order Is the Whole Trick

**Tags:** `common mistake` (red), `sort first` (orange)

- **The trap** — both tricks lean on order; on unsorted data the fingers give silently wrong answers
- **Unsorted buyers** — the raw buyer log reads 8, 5, 8, 9, 5 — the repeats are not next to each other
- **Adjacent dedup** — a pointer that drops only adjacent repeats keeps all 5 entries and removes nothing
- **Sort first** — sorted it reads 5, 5, 8, 8, 9; now the same pointer pass correctly keeps 5, 8, 9
- **Silent failure** — nothing crashes; the count of unique buyers is just wrong, 5 instead of 3

*Example (italic):* An analyst deduped the raw log 8, 5, 8, 9, 5 without sorting, reported 5 unique buyers, and the real answer was 3.

**Common mistake:** Running two-pointer dedup or a merge join on unsorted data. The pass still finishes and returns something — sort on the key first, or the duplicates and matches that are not adjacent get missed.

### Visualization (canvas `c4`, 720×300)

Two rows of numbered boxes: the unsorted log with adjacent-dedup wrongly keeping all five entries, and the sorted log where the same pass correctly collapses to three, with the two answers contrasted.

- **Title (bold 15px, `#1a5276`, top center):** "Same Dedup Pass, Sorted vs Not: 5 'Uniques' vs the True 3".
- **Row 1 (y=100):** label "raw log, adjacent dedup" in 12px `#444` at x=20, y=80; five 58px-wide, 36px-tall rounded boxes starting at x=90, 14px gap, values `[8, 5, 8, 9, 5]`; all five kept: fill `rgba(231,76,60,0.12)`, 2px red `#e74c3c` border; bold 13px red `#e74c3c` result text "kept all 5 — repeats not adjacent" at x=470, y=122.
- **Row 2 (y=200):** label "sorted first, same pass" in 12px `#444` at x=20, y=180; five boxes same geometry, values `[5, 5, 8, 8, 9]`; kept boxes (first 5, first 8, and 9) fill `rgba(0,131,0,0.15)` with 2px green `#008300` border; dropped boxes (second 5, second 8) white fill, 2px dashed `#6b7280` border, `#6b7280` text with a 2px `#6b7280` diagonal strike; bold 13px green `#008300` result text "kept 5, 8, 9 — the true 3" at x=470, y=222.
- **Annotation (bold 13px magenta `#d55181`, centered near x=360, y=270):** "order is the whole trick — sort on the key first".
- **Caption (12px `#444`, bottom right):** "illustrative — a five-entry buyer log".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all list values, bar heights, window totals, and operation counts are the hardcoded arrays and numbers above (no randomness); window totals `[125, 155, 145, 175, 155]` are exact sums of the daily array, and 10,950 = 365 × 30, 730 = 365 × 2.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
