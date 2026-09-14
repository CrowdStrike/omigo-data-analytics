# Windowing & Watermarks

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Windowing & Watermarks

**Subtitle:** A stream never ends, so you can never "count all the orders" — windows cut the stream into finite buckets, and watermarks decide when a bucket is safe to close

## Counting Orders on a Stream That Never Ends

**Tags:** `core idea` (blue), `windows` (green), `streaming` (orange)

- **The shop** — a pizza shop's app emits one event per order, forever; there is no last row
- **The question** — "how many orders per 5 minutes?" needs finite buckets carved from the stream
- **Tumbling** — back-to-back 5-minute buckets: 12:00–12:05, 12:05–12:10; each order lands in exactly one
- **Sliding** — a 5-minute bucket that advances every 1 minute; one order can land in five buckets
- **Session** — no fixed size: a bucket per customer that closes after a 3-minute gap in their activity

*Example (italic):* An order placed 12:03 falls in tumbling window 12:00–12:05, in five sliding windows from 11:59–12:04 through 12:03–12:08, and in whatever session its customer is in.

**Key point:** A window turns an infinite stream into finite groups you can aggregate — tumbling windows partition time, sliding windows overlap it, session windows follow gaps in the data itself.

### Visualization (canvas `c1`, 720×300)

Three horizontal tracks (tumbling / sliding / session) over the same row of order-event dots, showing how each window type buckets identical events differently.

- **Title (bold 15px, `#1a5276`, top center):** "Same Orders, Three Ways to Cut the Stream".
- **Shared time axis:** x maps 12:00–12:15 to pixels 70–670 (40px per minute); 12px `#444` tick labels "12:00", "12:05", "12:10", "12:15" under y=272; light vertical gridlines `#e5e9ef` at those ticks from y=48 to y=265.
- **Event dots (drawn on every track):** order event times (minutes after 12:00) `[1, 2, 3, 4, 7, 8, 9, 11, 12, 13]` as 4px radius `#2c3e50` filled circles on each track's midline.
- **Track 1 "tumbling" (label bold 12px `#1a5276` at x=8, band y=55–95):** three adjacent rounded rects at minutes 0–5, 5–10, 10–15, fill `rgba(42,120,214,0.18)`, 2px `#2a78d6` borders, no gaps.
- **Track 2 "sliding" (label at x=8, band y=125–175):** five overlapping rounded rects (every other 1-min-slide window), each 5 minutes wide, starting at minutes `[0, 2, 4, 6, 8]`, staggered vertically 8px apart, fill `rgba(25,158,112,0.15)`, 2px `#199e70` borders.
- **Track 3 "session" (label at x=8, band y=205–245):** two rounded rects spanning minutes 1–4 and 7–13 (event clusters with gaps < 3 min), fill `rgba(201,133,0,0.15)`, 2px `#c98500` borders; 12px `#6b7280` label "3-min gap" centered between them at minute 5.5.
- **Annotation (bold 12px violet `#4a3aa7`, right side near y=150):** "one order, five sliding windows (every other shown)".
- **Caption (12px `#444`, bottom right):** "order times illustrative".

## Placed at 12:04, Arrived at 12:07

**Tags:** `worked example` (blue), `event time` (green), `watermark` (orange)

- **Two clocks** — event time is when the order was placed; processing time is when the engine receives it
- **The lag** — order D is placed 12:04 in a dead zone and only reaches the engine at 12:07
- **The rule** — watermark = latest event time seen minus 2 minutes: "I've probably seen everything up to T"
- **The trigger** — window 12:00–12:05 fires when the watermark passes 12:05, not when the wall clock does
- **The moment** — order F (placed 12:07) arrives at 12:08, pushing the watermark to 12:05: the window fires
- **The count** — it fires with 4 orders (A, B, C, D) — late D made it in because the watermark waited

*Example (italic):* Ten orders arrive as (placed → arrived): A 12:01→12:01, B 12:02→12:02, C 12:03→12:03, D 12:04→12:07, E 12:06→12:06, F 12:07→12:08, G 12:04→12:10, H 12:09→12:09, I 12:11→12:11, J 12:04→12:14.

**Key point:** The watermark is the engine's moving claim "no event older than T is still coming"; a window closes when the watermark passes its end, which lets slightly-late events like D be counted correctly.

### Visualization (canvas `c2`, 720×300)

Scatter of the ten orders with arrival time on x and event time on y, a diagonal "on time" line, and the watermark staircase trailing 2 minutes behind the max event time.

- **Title (bold 15px, `#1a5276`, top center):** "Two Clocks per Order: Placed (y) vs Arrived (x)".
- **Axes:** origin x=70, baseline y=245, plot width 580, plot height 185; both axes span minutes 0–15 after 12:00; 12px `#444` tick labels "12:00"/"12:05"/"12:10"/"12:15" on both axes; x-axis label "arrival (processing time)", y-axis label "placed (event time)" 12px `#444`; gridlines `#e5e9ef` at ticks.
- **On-time diagonal:** 1px dashed `#6b7280` line where event time = arrival time, 11px `#6b7280` label "on time" along it.
- **Order dots:** at (arrival, placed) minutes `[[1,1],[2,2],[3,3],[7,4],[6,6],[8,7],[10,4],[9,9],[11,11],[14,4]]` — on-time orders (first, second, third, fifth, sixth, eighth, ninth) 5px `#2a78d6` circles; late orders D (7,4), G (10,4), J (14,4) 5px `#d95926` circles with 11px `#d95926` letter labels "D", "G", "J" beside them.
- **Watermark staircase:** 3px `#008300` step line at (arrival x, watermark y) points `[[1,-1],[2,0],[3,1],[6,4],[8,5],[9,7],[11,9],[14,12]]` (max placed so far minus 2), clipped to the plot; bold 12px `#008300` label "watermark = max placed − 2 min" near (x≈9.5 min, y≈6 min).
- **Window-close marker:** horizontal dashed `#e74c3c` (dash 4/3) line at event time y=5 from x=8 min rightward, bold 12px `#e74c3c` label "watermark passes 12:05 at arrival 12:08 — window fires (count 4)" above it at the right.
- **Caption (12px `#444`, bottom right):** "times illustrative; watermark delay fixed at 2 min".

## Why the Clock You Pick Changes the Answer

**Tags:** `why it matters` (blue), `where it's used` (green)

- **Same data, two answers** — bucketing the same ten orders by arrival clock vs placed clock disagrees in every window
- **Processing time** — 12:00–12:05 gets 3 orders, 12:05–12:10 gets 4, 12:10–12:15 gets 3: late orders drift right
- **Event time** — the true placed counts are 6, 3, 1: the busy first window was hiding behind the lag
- **The stakes** — dashboards, billing, and fraud counts keyed to "when it happened" need event time
- **The cost** — event time forces you to wait (watermarks); processing time is instant but rewrites history

*Example (italic):* A demand-forecast model trained on processing-time counts learns a phantom 12:10–12:15 rush of 3 orders when the true count then was 1 — the other 2 were placed 12:04.

**Key point:** Processing time answers "when did my server hear about it"; event time answers "when did it happen" — pick by the question, and accept the watermark wait as the price of event time.

### Visualization (canvas `c3`, 720×300)

Grouped bar chart: for each 5-minute window, orders counted by processing time vs by event time, from the same ten orders of section 2.

- **Title (bold 15px, `#1a5276`, top center):** "Ten Orders, Two Clocks, Different Histories".
- **Axes:** origin x=70, baseline y=245, plot width 560, plot height 180; y = order count 0–6, gridlines `#e5e9ef` at 2/4/6 with 12px `#444` tick labels; x = three window groups centered at x=180, 370, 560 with 12px `#444` labels "12:00–12:05", "12:05–12:10", "12:10–12:15".
- **Bars (each 52px wide, 12px gap within a group):** processing-time bars fill `rgba(42,120,214,0.35)`, 2px `#2a78d6` border, heights for counts `[3, 4, 3]`; event-time bars fill `rgba(0,131,0,0.30)`, 2px `#008300` border, heights for counts `[6, 3, 1]`; bold 13px count labels in the bar color above each bar.
- **Legend (top right, 12px):** blue swatch "by arrival (processing time)", green swatch "by placed (event time)".
- **Annotation (bold 13px magenta `#d55181`, above the first group near y=60):** "half the 12:00–12:05 orders arrived late".
- **Caption (12px `#444`, bottom right):** "same 10 illustrative orders as above; totals match (10 = 10)".

## The Watermark Is a Bet, Not a Guarantee

**Tags:** `common mistake` (red), `allowed lateness` (orange)

- **The bet** — "nothing older than T is coming" is a heuristic; order G (placed 12:04) arrives at 12:10, behind it
- **Allowed lateness** — keep the closed window's state 3 extra minutes; G lands inside and re-fires the count as 5
- **The cliff** — once the watermark passes 12:05 + 3 min of lateness, the window's state is deleted for good
- **Truly late** — order J (placed 12:04) arrives 12:14: no window exists; it is dropped or diverted to a side output
- **The mistake** — treating the first fired count (4) as final and the watermark as a completeness guarantee
- **The trade** — longer lateness catches more stragglers but holds state and delays "final" answers

*Example (italic):* Window 12:00–12:05 truly had 6 orders; it fires at 4, updates to 5 when G lands in the lateness grace, and J is dropped — the final count is 5, and only the side output knows about J.

**Common mistake:** Believing a fired window is finished. Downstream consumers must either tolerate updated results during allowed lateness or accept that events behind the watermark silently vanish.

### Visualization (canvas `c4`, 720×300)

Lifecycle timeline of the single window 12:00–12:05: open, fire, late update, state deleted, and a dropped event, laid out on the engine's arrival clock.

- **Title (bold 15px, `#1a5276`, top center):** "One Window's Life: Fire at 4, Update to 5, Drop the Rest".
- **Time axis:** horizontal 2px `#999` line at y=170, x maps arrival 12:00–12:15 to pixels 60–660 (40px per minute); 12px `#444` tick labels "12:00", "12:05", "12:08", "12:10", "12:14", "12:15" below.
- **Phase bands (rounded rects y=90–150 on the axis):** "window open" band minutes 0–8, fill `rgba(42,120,214,0.18)`, 2px `#2a78d6` border; "allowed lateness (3 min)" band minutes 8–11, fill `rgba(201,133,0,0.18)`, 2px `#c98500` border; "state deleted" band minutes 11–15, fill `rgba(107,114,128,0.12)`, 1px `#6b7280` border; each labeled bold 12px in its border color inside the band.
- **Fire marker:** vertical 3px `#008300` line at minute 8, bold 13px `#008300` label above at y=70: "watermark passes 12:05 — fires count = 4".
- **Late update marker:** `#d95926` 6px dot on the axis at minute 10 with 12px `#d95926` label "G arrives — count updates to 5" at y=200.
- **Drop marker:** red `#e74c3c` 6px dot at minute 14 with bold 12px `#e74c3c` label "J arrives — window gone, dropped" at y=225 and a small red "✗" above the dot.
- **Annotation (bold 13px violet `#4a3aa7`, near x=minute 2, y=235):** "true count was 6; the stream never promised you'd see it".
- **Caption (12px `#444`, bottom right):** "watermark delay 2 min, allowed lateness 3 min — both illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); the ten (placed → arrived) order pairs are invented and labeled illustrative; the watermark staircase, per-window counts (3/4/3 vs 6/3/1), fire count 4, updated count 5, and dropped order J all derive deterministically from those ten pairs with watermark delay 2 min and allowed lateness 3 min — keep them consistent if any pair changes.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
