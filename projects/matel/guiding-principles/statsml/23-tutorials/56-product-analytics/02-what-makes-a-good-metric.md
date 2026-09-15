# What Makes a Good Metric

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** What Makes a Good Metric

**Subtitle:** A good metric passes four tests — measurable, sensitive, hard to game, aligned — and most numbers on your dashboard fail at least one

## One Delivery App, One Question: What Counts as Quality?

**Tags:** `core idea` (blue), `four tests` (green), `metric design` (orange)

- **The choice** — a food-delivery app must pick ONE number to stand for "quality" across teams
- **The candidates** — app rating, % under 30 min, 14-day reorder rate; the chart adds two contrast rows
- **Measurable** — defined precisely from logged events, so two analysts get the same number
- **Sensitive** — it moves when quality moves, and the move is detectable at your traffic level
- **Hard to game** — no team can improve the number without improving the real thing it tracks
- **Aligned** — pushing the metric up actually pushes the business outcome up

*Example (italic):* When couriers had a bad week, only the reorder rate flinched — the rating and the speed stat slept right through it.

**Key point:** A metric is a decision instrument, not a report line — it earns the job only by passing all four tests, and every other candidate here fails at least one.

### Visualization (canvas `c1`, 720×300)

Scorecard matrix: five candidate metrics as rows, the four tests as columns, pass/fail marks in each cell; the winning row highlighted.

- **Title (bold 15px, `#1a5276`, top center):** "Five Candidates, Four Tests — Only One Passes All Four".
- **Column headers (bold 12px `#1a5276`, centered at y=62):** "measurable" at x=280, "sensitive" at x=395, "hard to game" at x=510, "aligned" at x=625.
- **Row labels (12px `#444`, left-aligned at x=20), rows at y = 95, 130, 165, 200, 235:** "% happy customers", "app-store rating", "tickets closed / hr", "% under 30 min", "14-day reorder rate".
- **Row separators:** 1px `#e5e9ef` horizontal lines from x=20 to x=690 midway between rows.
- **Cell marks (bold 15px, centered on the header x positions at each row y):** pass "✓" in green `#008300`, fail "✗" in red `#e74c3c`. Marks per row (columns in header order): row 1 `["✗","✓","✓","✓"]`, row 2 `["✓","✗","✓","✓"]`, row 3 `["✓","✓","✗","✓"]`, row 4 `["✓","✓","✗","✗"]`, row 5 `["✓","✓","✓","✓"]`.
- **Winner highlight:** behind row 5, a rounded band x=12 to x=700, 28px tall, fill `rgba(0,131,0,0.08)`, 1px `#008300` border.
- **Annotation (bold 13px green `#008300`, below the band at x=250, y=262):** "passes all four — hire this metric".
- **Caption (12px `#444`, bottom right):** "pass/fail judgments illustrative; tests defined in text".

## Reorder Rate Passes All Four Tests

**Tags:** `worked example` (blue), `sensitivity` (green)

- **The definition** — an order counts as a reorder if the same customer orders again within 14 days
- **Measurable** — 5,016 of 12,000 logged orders → 41.8% (exact division); both analysts get 41.8%
- **Sensitive** — a supplier's bad-batch week drops reorders to 4,344 of 12,000 → 36.2%, a −5.6 pt cliff
- **The insensitive rival** — app rating slid 4.6 → 4.5 that same week; only 900 of 12,000 orders rate
- **Aligned** — a reorder IS revenue: moving this metric and moving the business are the same act

*Example (italic):* Week 4's bad batch shows as a 5.6-point cliff in the reorder rate but only a 0.1 wobble in the app rating.

**Key point:** Every property is checkable by hand from the orders log — precise event definition, a visible move when quality drops, and, absent promo pushes, no way up except better food and delivery.

### Visualization (canvas `c2`, 720×300)

Dual-axis weekly timeline: 14-day reorder rate (left axis, reacts to the bad-batch week) vs app-store rating (right axis, barely moves), over the same 8 weeks.

- **Title (bold 15px, `#1a5276`, top center):** "Bad-Batch Week: Reorder Rate Screams, App Rating Whispers".
- **Axes:** origin x=60, baseline y=245, plot width 590, plot height 180; x = weeks 1–8, 12px `#444` tick labels "wk1"–"wk8"; left y = reorder rate 30% to 45%, gridlines `#e5e9ef` at 35/40, 12px `#444` labels; right y at x=650 = rating 4.0 to 5.0, 12px `#6b7280` labels at 4.0/4.5/5.0.
- **Reorder line:** green `#008300` 3px line with 4px dots through weeks `[1, 2, 3, 4, 5, 6, 7, 8]`, rates `[41.8, 42.1, 41.5, 36.2, 37.9, 40.4, 41.6, 42.0]` — cliff at week 4, recovery by week 7.
- **Rating line:** mute gray `#6b7280` 2px line, same week grid, ratings `[4.6, 4.6, 4.6, 4.5, 4.6, 4.6, 4.6, 4.6]` plotted on the right-axis scale — visually flat.
- **Incident marker:** vertical dashed `#d95926` (dash 4/3) line at week 4, bold 12px `#d95926` label "supplier bad batch" at its top.
- **Line labels:** bold 12px green "reorder rate (left)" near week 2 above the green line; 12px `#6b7280` "app rating (right)" near week 6 above the gray line.
- **Annotation (bold 13px green `#008300`, near week 5, y=95):** "−5.6 pts — visible at 12,000 orders/wk".
- **Caption (12px `#444`, bottom right):** "weekly values illustrative; 5,016/12,000 = 41.8% exact".

## The Gamed Metric: Tickets Closed per Hour

**Tags:** `where it's used` (blue), `Goodhart's law` (orange), `common mistake` (red)

- **The contrast** — a support team is scored on "tickets closed per hour"; it sounds like productivity
- **The game** — agents close tickets without solving them; the customer just opens a new one
- **The numbers** — closed/hour climbs 6.0 → 9.5 in six weeks; reopen rate climbs 12% → 31%
- **The tell** — the metric improved 58% while the thing it was meant to track got worse
- **The test** — ask "can a team move this number without improving reality?" — if yes, it will be
- **Goodhart's law** — when a measure becomes a target, it ceases to be a good measure

*Example (italic):* The delivery app's version: dispatch quietly rejects far-away orders, and "% under 30 min" jumps while weekly orders shrink.

**Key point:** "Hard to game" is not about trusting your teams — it is about picking a number whose only path upward runs through the real outcome.

### Visualization (canvas `c3`, 720×300)

Dual-axis timeline of the six weeks after the closed-per-hour target is announced: the target metric rising while the reopen rate rises with it.

- **Title (bold 15px, `#1a5276`, top center):** "The Target Went Up 58% — So Did the Reopened Tickets".
- **Axes:** origin x=60, baseline y=245, plot width 590, plot height 180; x = weeks 1–6, 12px `#444` tick labels "wk1"–"wk6"; left y = tickets closed per hour 0 to 12, gridlines `#e5e9ef` at 4/8, 12px `#444` labels; right y at x=650 = reopen rate 0% to 40%, 12px `#6b7280` labels at 0/20/40.
- **Target line:** blue `#2a78d6` 3px line with 4px dots through weeks `[1, 2, 3, 4, 5, 6]`, closed/hour `[6.0, 6.8, 7.7, 8.6, 9.1, 9.5]`.
- **Reopen line:** red `#e74c3c` 3px line, same weeks, reopen % `[12, 16, 21, 25, 28, 31]` on the right-axis scale.
- **Target marker:** vertical dashed `#6b7280` (dash 4/3) line just left of week 1, 12px `#6b7280` label "per-hour target announced" at its top.
- **Line labels:** bold 12px blue "closed per hour (left)" near week 3 above the blue line; bold 12px red "reopen rate (right)" near week 4 below the red line.
- **Annotation (bold 13px red `#e74c3c`, near week 5, y=70):** "closed ≠ solved".
- **Caption (12px `#444`, bottom right):** "all values illustrative".

## The Most Available Number Wins by Default

**Tags:** `common mistake` (red), `availability trap` (orange)

- **The pull** — the app rating is already on a vendor dashboard; delivery times auto-log themselves
- **The work** — the reorder rate needs a 14-day self-join on the orders table that nobody has written
- **The trap** — teams pick the number that is one click away, not the one that passes the four tests
- **The cost** — a quarter spent nudging a rating that the customers who churned never touched
- **The fix** — score the candidates on the four tests FIRST, then pay the engineering cost for the winner

*Example (italic):* The winning metric was one SQL join away; the losing one came pre-installed on the dashboard.

**Common mistake:** Choosing the most available number instead of the most aligned one. Availability is a property of your logging, not of the metric — a great metric that costs a day of pipeline work beats a free one that steers the company sideways.

### Visualization (canvas `c4`, 720×300)

Scatter of the four candidate metrics on two judged axes — how easy the number is to obtain (x) vs how well it tracks the business (y) — with the easy-but-misaligned corner shaded as the trap.

- **Title (bold 15px, `#1a5276`, top center):** "Easy to Pull Is Not the Same as Worth Pulling".
- **Axes:** origin x=70, baseline y=245, plot width 580, plot height 175; 2px `#999` axis lines; x label 12px `#444` centered below baseline: "easy to obtain  →  needs pipeline work"; y label 12px `#444` rotated at x=30: "tracks the business  →".
- **Trap zone:** rectangle x=70 to x=330, y=160 to y=245, fill `rgba(231,76,60,0.08)`, bold 12px `#e74c3c` label "the availability trap" centered inside it.
- **Points (10px filled circles, bold 12px labels beside each):**
  - "app-store rating" — red `#e74c3c` at (130, 210)
  - "tickets closed / hr" — red `#e74c3c` at (185, 228)
  - "% under 30 min" — orange `#d95926` at (240, 168)
  - "14-day reorder rate" — green `#008300` at (545, 88)
- **Annotation (bold 13px green `#008300`, near x=390, y=70):** "the best metric was one SQL join away".
- **Caption (12px `#444`, bottom right):** "positions judged, illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); order counts, weekly rates, ratings, ticket figures, and scatter positions are invented and labeled illustrative; 5,016/12,000 = 41.8% and 4,344/12,000 = 36.2% are exact divisions and the text's numbers must match the `c2` arrays.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
