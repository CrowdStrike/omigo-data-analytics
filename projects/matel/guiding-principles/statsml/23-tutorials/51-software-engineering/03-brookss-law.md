# Brooks's Law

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Brooks's Law

**Subtitle:** Adding people to a late software project makes it later — Fred Brooks, The Mythical Man-Month (1975) — because new people slow the veterans down before they speed anything up

## The Late Project That Got Later

**Tags:** `core idea` (blue), `ramp-up cost` (orange), `Brooks 1975` (green)

- **The project** — a 5-person team has 50 units of work left and a deadline 8 weeks away
- **The math** — at 5 units/week the team needs 10 weeks: the project is already 2 weeks late
- **The rescue** — management adds 4 engineers, expecting 9 people to nearly double the pace
- **The ramp-up** — new hires produce nothing at first AND pull veterans into teaching them
- **The dip** — team output falls from 5 to 2.5 units/week in week 1 and takes 6 weeks to recover
- **The result** — the 9-person team finishes in week 11, one week later than doing nothing

*Example (italic):* Six weeks after the "rescue," the bigger team has completed 22 units of work — the untouched 5-person team would have completed 30.

**Key point:** Brooks's law (Fred Brooks, The Mythical Man-Month, 1975): adding manpower to a late software project makes it later — the ramp-up cost lands immediately, the extra capacity arrives too late to matter.

### Visualization (canvas `c1`, 720×300)

Two cumulative-work lines over 12 weeks: keep the team of 5 vs add 4 people at week 1, with the 50-unit finish line and the week-8 deadline marked.

- **Title (bold 15px, `#1a5276`, top center):** "Adding 4 People Made the Late Project 1 Week Later".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = weeks 0 to 12, 12px `#444` tick labels every 2 weeks; y = units of work done 0 to 65, gridlines `#e5e9ef` at 15/30/45/60.
- **Finish line:** horizontal dashed `#6b7280` (dash 4/3) line at 50 units, 12px `#6b7280` label "50 units = done" at its left end.
- **Deadline marker:** vertical dashed `#e74c3c` line at week 8, 12px `#e74c3c` label "deadline" at its top.
- **Keep-5 line:** blue `#2a78d6` 3px line through weeks `[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]`, cumulative units `[0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60]` — straight, crosses 50 at week 10.
- **Add-4 line:** orange `#d95926` 3px line through the same weeks, cumulative units `[0, 2.5, 5.3, 8.5, 12.3, 16.8, 22.0, 27.8, 34.1, 40.8, 47.8, 55.0, 62.2]` — sags below blue, crosses 50 during week 11.
- **Crossing dots:** 5px blue dot at (10, 50) labeled bold 12px blue "week 10"; 5px orange dot at (10.4, 50) labeled bold 12px orange `#d95926` "week 11".
- **Annotation (bold 13px `#e74c3c`, near week 4, y=95):** "ramp-up: the bigger team does less".
- **Caption (12px `#444`, bottom right):** "weekly outputs illustrative".

## Counting the Communication Pairs

**Tags:** `worked example` (blue), `n(n−1)/2` (green)

- **The formula** — n people who must coordinate form n(n−1)/2 pairs; this count is exact, not a model
- **Small team** — 5 people: 5×4/2 = 10 pairs to keep in sync
- **After the rescue** — 9 people: 9×8/2 = 36 pairs — 1.8× the hands, 3.6× the coordination
- **Big team** — 20 people: 20×19/2 = 190 pairs; every design change ripples through all of them
- **The mismatch** — work capacity grows linearly with n while coordination cost grows quadratically
- **Hand-check** — going 5 → 9 adds 4 workers but 26 new communication pairs (36 − 10)

*Example (italic):* The 9-person standup runs 25 minutes instead of 10, and the schema change that needed 2 conversations now needs 8.

**Key point:** Adding a person adds one worker but n−1 new communication pairs — past a certain size, each new hire consumes more coordination than they contribute in output.

### Visualization (canvas `c2`, 720×300)

Two lines against team size n = 2..20: work capacity (linear, = n) and communication pairs (quadratic, = n(n−1)/2), on separate scales sharing the x axis.

- **Title (bold 15px, `#1a5276`, top center):** "Hands Grow Linearly, Communication Pairs Grow Quadratically".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = team size 2 to 20, 12px `#444` tick labels at 2/5/9/15/20; y = pairs 0 to 200, gridlines `#e5e9ef` at 50/100/150.
- **Pairs line:** magenta `#d55181` 3px line through n `[2, 3, 5, 7, 9, 12, 15, 20]`, pairs `[1, 3, 10, 21, 36, 66, 105, 190]` — exact n(n−1)/2 values.
- **Capacity line:** blue `#2a78d6` 3px line through the same n values, plotted as workers `[2, 3, 5, 7, 9, 12, 15, 20]` on a 0–20 scale mapped onto the same 180px plot height — visibly near-flat next to the quadratic.
- **Marker dots:** 5px magenta dots at n=5 (10 pairs), n=9 (36 pairs), n=20 (190 pairs), each with bold 12px magenta label "10 pairs" / "36 pairs" / "190 pairs".
- **Line labels:** bold 12px blue "work capacity (= n)" near the blue line at n≈17; bold 12px magenta "pairs = n(n−1)/2" near the magenta line at n≈15.
- **Annotation (bold 13px violet `#4a3aa7`, near n=8, y=70):** "5 → 9 people: +4 hands, +26 pairs".
- **Caption (12px `#444`, bottom right):** "pair counts exact: n(n−1)/2".

## Nine Women, One Month, No Baby

**Tags:** `where it's used` (blue), `man-month fallacy` (red)

- **The unit** — schedules are priced in man-months, as if 12 people × 1 month equals 1 person × 12 months
- **When it holds** — only for perfectly partitionable work with no communication: harvesting a field, yes
- **When it fails** — designing a schema is sequential thought; splitting it 12 ways just adds meetings
- **Brooks's line** — the bearing of a child takes nine months, no matter how many women are assigned
- **The floor** — sequential work has a minimum calendar time that no headcount can buy down
- **Real projects** — most software sits in between: time falls at first, flattens, then rises with overhead

*Example (italic):* A 12-month data-pipeline rewrite drops to about 5 months with 3 people, bottoms near 4 at 6, and gets slower again at 12 — it never comes close to 1 month.

**Key point:** The man-month treats people and time as interchangeable currency; that is only true when the work partitions perfectly — for sequential or communication-heavy work the trade is partly or wholly fictional.

### Visualization (canvas `c3`, 720×300)

Three time-vs-workers curves (the classic Mythical Man-Month figure): perfectly partitionable, partitionable with communication overhead, and unpartitionable/sequential.

- **Title (bold 15px, `#1a5276`, top center):** "Months to Finish vs Workers: Only One Curve Obeys the Man-Month".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = workers 1 to 12, 12px `#444` tick labels at 1/2/3/4/6/9/12; y = months 0 to 13, gridlines `#e5e9ef` at 3/6/9/12.
- **Perfectly partitionable line:** green `#008300` 3px line through workers `[1, 2, 3, 4, 6, 9, 12]`, months `[12, 6, 4, 3, 2, 1.33, 1]` — true hyperbola, 12/n exactly; bold 12px green label "harvesting a field (12/n)" near workers≈2.5.
- **With-overhead line:** orange `#d95926` 3px line through the same workers, months `[12, 6.6, 5.0, 4.3, 4.0, 4.4, 5.2]` — falls, bottoms out near 6 workers, rises again; bold 12px orange label "typical software" near workers≈7.
- **Sequential line:** red `#e74c3c` 3px flat line through the same workers, months `[9, 9, 9, 9, 9, 9, 9]`; bold 12px red label "the baby (9 months, always)" above it near workers≈8.
- **Annotation (bold 13px `#1a5276`, near workers=10, y=170):** "past the dip, more people = more months".
- **Caption (12px `#444`, bottom right):** "green curve exact 12/n; overhead curve illustrative".

## What Actually Helps a Late Project

**Tags:** `common mistake` (red), `what works instead` (green)

- **Cut scope** — shipping 80% of the features on time beats shipping 100% a quarter late
- **Remove blockers** — one stalled dependency or approval often costs more than any hiring can recover
- **Protect the team** — killing interruptions and status theater gives back hours with zero ramp-up cost
- **Accept the date** — a re-planned honest schedule is cheaper than a rescue that makes things worse
- **When adding works** — early in the project, on cleanly partitionable work, with real onboarding capacity
- **The tell** — if the work looks like a field to harvest, add people; if it looks like a baby, don't

*Example (italic):* Cutting the two least-used report formats saved 2 weeks instantly; the 4 rescue hires had cost the team a week before their first commit landed.

**Common mistake:** Treating headcount as the universal schedule lever. Late-project help must arrive with no ramp-up bill — cut scope, unblock, and protect focus first; save new hires for early-stage, partitionable work.

### Visualization (canvas `c4`, 720×300)

Diverging horizontal bar chart: schedule effect (weeks gained or lost) of five interventions on the late project from section 1.

- **Title (bold 15px, `#1a5276`, top center):** "Same Late Project, Five Interventions: Weeks Gained or Lost".
- **Axis:** vertical 2px `#999` zero line at x=400; bars extend left (weeks gained, good) or right (weeks lost, bad); scale 60px per week; 12px `#444` scale labels "−3w / −2w / −1w / 0 / +1w" along a baseline at y=265.
- **Rows (top to bottom at y = 62, 102, 142, 182, 222), each with a right-aligned 12px `#444` label ending at x=190:**
  - "cut scope 20%": green `#008300` bar left, width 120 (−2 weeks)
  - "remove the blocked dependency": green bar left, width 90 (−1.5 weeks)
  - "protect focus / kill interruptions": green bar left, width 60 (−1 week)
  - "add 4 people now (late)": red `#e74c3c` bar right, width 60 (+1 week), bold 12px red label "Brooks's law" at bar end
  - "add 4 people at project start": aqua `#199e70` bar left, width 120 (−2 weeks), 11px `#6b7280` note "only if work partitions"
- **Bar style:** 18px tall, 3px radius, fills at full color with 11px white or `#444` week labels ("−2w", "+1w") just outside each bar end.
- **Annotation (bold 13px `#1a5276`, centered near y=285):** "the only lever with no ramp-up bill is doing less, sooner".
- **Caption (12px `#444`, bottom right):** "week effects illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); weekly outputs, the overhead curve, and the intervention week-effects are invented and labeled illustrative; communication-pair counts (1/3/10/21/36/66/105/190) are exact n(n−1)/2 values, and the green 12/n curve in c3 is exact arithmetic. Credit Fred Brooks, The Mythical Man-Month (1975), in the subtitle and section 1 key point.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
