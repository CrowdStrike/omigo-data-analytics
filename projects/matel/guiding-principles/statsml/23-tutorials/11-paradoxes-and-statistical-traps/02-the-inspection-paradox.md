# The Inspection Paradox

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** The Inspection Paradox

**Subtitle:** Buses average one every 10 minutes, yet your wait is nearly 10 — random arrivals land in long gaps more often, so what you sample is bigger than the average

## Ten-Minute Buses, Nine-Minute Waits

**Tags:** `core idea` (blue), `size-biased sampling` (orange)

- **The stop** — 13 buses arrive in two hours, so the average gap between buses is exactly 10 minutes
- **The naive guess** — arrive at a random moment and you expect to wait half a gap: 5 minutes
- **The catch** — the gaps vary: 2, 3, 25, 5, 4, 30, 6, 5, 20, 8, 4, 8 minutes — bunched, then sparse
- **Big gaps catch you** — the 3 longest gaps cover 75 of 120 minutes, so 62.5% of arrivals hit one
- **The real wait** — averaged over every minute you could arrive, the wait is 9.1 minutes, not 5

*Example (italic):* Arrive at any random minute of this schedule and your expected wait is 9.1 minutes — almost the full 10-minute average gap.

**Key point:** A random arrival samples gaps by their length, not by their count — so the long gaps dominate what you experience, and your wait exceeds half the average gap.

### Visualization (canvas `c1`, 720×300)

Timeline of one illustrative two-hour bus schedule with the three long gaps shaded, plus the wait-vs-average callout.

- **Title (bold 15px, `#1a5276`, top center):** "Two Hours at the Bus Stop: 13 Buses, Average Gap 10 Minutes".
- **Data:** arrival times (min) `[0, 2, 5, 30, 35, 39, 69, 75, 80, 100, 108, 112, 120]`; gaps (min) `[2, 3, 25, 5, 4, 30, 6, 5, 20, 8, 4, 8]`.
- **Timeline:** horizontal 2px `#999` line at y=150 from x=60 to x=660 (5 px per minute, 0–120 min); tick labels "0", "30", "60", "90", "120 min" 12px `#444` at y=172 under x = 60, 210, 360, 510, 660.
- **Bus dots:** 7px filled blue `#2a78d6` circles on the line at each of the 13 arrival times.
- **Long-gap bands:** rectangles from y=124 to y=150 filled `rgba(217,89,38,0.25)` spanning minutes 5→30, 39→69, 80→100; each labeled "25 min", "30 min", "20 min" bold 12px orange `#d95926` centered above the band at y=114.
- **Annotation (bold 13px orange `#d95926`, centered x=360, two lines at y=58 and y=76):** "3 long gaps cover 75 of 120 minutes" / "62.5% of random arrivals land in one".
- **Callout (bold 13px magenta `#d55181`, centered x=360, y=225):** "average gap = 10 min — but average wait = 9.1 min, not 5".
- **Caption (12px `#444`, centered x=360, y=272):** "gaps in order (min): 2, 3, 25, 5, 4, 30, 6, 5, 20, 8, 4, 8 — illustrative schedule".

## Doing the Math With Two Gap Sizes

**Tags:** `worked example` (blue), `rule of thumb` (green)

- **A toy schedule** — gaps alternate 2 and 18 minutes: 6 gaps, one hour total, average gap 10
- **Count gaps** — half the gaps are short and half are long: a 50/50 split by count
- **Count minutes** — the long gaps hold 54 of 60 minutes, so 90% of random arrivals land in one
- **Wait inside** — landing in an 18-minute gap means waiting 9 on average; in a 2-minute gap, 1
- **Add it up** — expected wait = 0.9 × 9 + 0.1 × 1 = 8.2 minutes, well above the naive 5

*Example (italic):* Nine out of ten random arrivals hit an 18-minute gap, so the 2-minute gaps barely move the answer.

**Key point:** Weight each gap by its length before averaging: riders experience 0.9 × 18 + 0.1 × 2 = 16.4-minute gaps, and the expected wait is half of that, 8.2 minutes.

### Visualization (canvas `c2`, 720×300)

Dual panel: the one-hour alternating schedule drawn as a colored strip (left) vs two 100% bars comparing share-of-gaps against share-of-minutes (right), split by a dashed divider at x=360.

- **Title (bold 15px, `#1a5276`, top center):** "Alternating 2- and 18-Minute Gaps: Gap Count vs Minutes Covered".
- **Data:** gaps (min) `[2, 18, 2, 18, 2, 18]`; share of gaps short/long `[50, 50]`; share of minutes short/long `[10, 90]`.
- **Left panel (strip):** heading bold 12px `#444` "the toy schedule (one hour)" at x=55, y=62; strip rectangle y=80→110 from x=55, width 280 (280/60 px per minute); segments in gap order, 2-min segments filled `rgba(42,120,214,0.5)`, 18-min segments filled `rgba(217,89,38,0.45)`; each 18-min segment labeled "18" bold 12px `#fff` centered; end labels "0" and "60 min" 12px `#444` at y=126 under x=55 and x=335; note bold 12px blue `#2a78d6` centered x=195, y=146: "narrow blue slivers = the 2-min gaps"; caption 12px `#444` centered x=195, y=272: "gaps (min): 2, 18, 2, 18, 2, 18 — average 10".
- **Right panel (100% bars):** two horizontal bars x=400, width 250, height 24; row headings bold 12px `#444` above each bar — "share of gaps" at y=78 (bar at y=86) and "share of minutes" at y=142 (bar at y=150); each bar split left-to-right blue `rgba(42,120,214,0.5)` (short) then orange `rgba(217,89,38,0.45)` (long): 50/50 and 10/90; segment labels "50%", "50%", "10%", "90%" bold 12px `#fff` centered in each segment (the "10%" label may sit just left of its segment in blue if too narrow).
- **Annotation (bold 13px green `#008300`, centered x=525, two lines at y=215 and y=233):** "expected wait = 0.9×9 + 0.1×1" / "= 8.2 minutes (naive guess: 5)".
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=360 from y=38 to h-12.

## The Same Trap in Class Sizes and Servers

**Tags:** `where it's used` (blue), `sampling bias` (orange)

- **Class sizes** — a college runs 4 classes of 10, 10, 10, and 90 students: the registrar's average is 30
- **Ask students** — 90 of the 120 students sit in the big class, so the student-side average is 70
- **Same paradox** — surveying students samples classes by size, like arrivals sample gaps by length
- **Servers too** — sampling in-flight requests over-picks slow ones; uptime probes over-pick long outages
- **The rider's bus** — riders on the section-1 schedule experience an 18.2-minute average gap, not 10

*Example (italic):* "Average class size is 30" and "the average student sits in a class of 70" describe the same four classes.

**Key point:** Whenever you sample people, requests, or moments in time, big units grab more of the samples — decide which average your question needs before you compute anything.

### Visualization (canvas `c3`, 720×300)

Two mini bar panels: the bus schedule's per-gap vs per-rider average (left, minutes) and the college's per-class vs per-student average (right, students), split by a dashed divider at x=360.

- **Title (bold 15px, `#1a5276`, top center):** "The Two Averages Split Apart: Buses and Class Sizes".
- **Data:** bus averages `[10, 18.2]` minutes (per gap, per rider); class averages `[30, 70]` students (per class, per student); class sizes `[10, 10, 10, 90]`.
- **Annotation (bold 13px magenta `#d55181`, centered x=360, y=54):** "sampling people instead of units always favors the big units".
- **Left panel (bus):** axis origin x=70, baseline y=235, chart height 160, y scale 0–20; two bars width 70 centered at x=150 and x=270; "per gap" bar 10 filled `rgba(42,120,214,0.55)`, "per rider" bar 18.2 filled `rgba(213,81,129,0.5)`; value labels bold 13px above bars in matching solid colors ("10 min" blue `#2a78d6`, "18.2 min" magenta `#d55181`); category labels "per gap", "per rider" 12px `#444` below baseline; caption 12px `#444` centered x=200, y=280: "the section-1 bus schedule".
- **Right panel (classes):** axis origin x=410, baseline y=235, chart height 160, y scale 0–80; bars width 70 centered at x=490 and x=610; "per class" bar 30 blue fill as left, "per student" bar 70 magenta fill as left; value labels bold 13px "30" and "70" in matching colors; category labels "per class", "per student" 12px `#444` below; caption 12px `#444` centered x=540, y=280: "4 classes: 10, 10, 10, 90 students".
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=360 from y=38 to h-12.

## Both Averages Are Right

**Tags:** `common mistake` (red), `variance` (orange)

- **Nobody lied** — the timetable's 10-minute average and your 9.1-minute wait are both correct
- **Different questions** — "average gap" averages over gaps; "average wait" averages over minutes
- **Variance is the culprit** — with perfectly even 10-minute gaps, the wait really is 5 minutes
- **Spread hurts** — with mean gap 10: gaps 8/12 → wait 5.2; 4/16 → 6.8; 2/18 → 8.2 minutes
- **The rule** — wait = half the mean gap only when every gap is identical; variability adds to it

*Example (italic):* Four bus lines all advertise "every 10 minutes on average", yet their true average waits are 5.0, 5.2, 6.8 and 8.2 minutes.

**Common mistake:** Halving the advertised average gap to predict your wait. That only works for perfectly even schedules — the bunchier the gaps, the worse the underestimate.

### Visualization (canvas `c4`, 720×300)

Bar chart of the expected wait for four schedules that all have a 10-minute average gap, with a dashed reference line at the naive answer of 5.

- **Title (bold 15px, `#1a5276`, top center):** "Same 10-Minute Average Gap, Four Very Different Waits".
- **Data:** schedules (alternating gap pairs) `["10 / 10", "8 / 12", "4 / 16", "2 / 18"]`; expected waits `[5.0, 5.2, 6.8, 8.2]` minutes.
- **Axis:** origin x=70, baseline y=235, chart height 165, y scale 0–10 min; horizontal gridlines `#e5e9ef` at 2.5, 5, 7.5, 10 with 12px `#6b7280` labels on the left.
- **Bars:** width 90, centered at x = 145, 290, 435, 580; fills in order aqua `rgba(25,158,112,0.55)`, blue `rgba(42,120,214,0.5)`, yellow `rgba(201,133,0,0.5)`, orange `rgba(217,89,38,0.55)`; value labels bold 13px above each bar in the matching solid color ("5.0", "5.2", "6.8", "8.2 min").
- **Reference line:** dashed green `#008300` (dash 5/4) horizontal line at the y for 5.0 across the plot, labeled bold 12px green "naive answer: 10 ÷ 2 = 5" just above the line at its left end.
- **X labels:** the gap pairs "10 / 10", "8 / 12", "4 / 16", "2 / 18" 12px `#444` below the baseline, with "(even)" appended under the first.
- **Annotation (bold 13px orange `#d95926`, right-aligned near x=625, y=90):** "bunchier gaps → longer waits".
- **Caption (12px `#444`, centered x=360, y=285):** "each schedule alternates two gap lengths that average 10 minutes".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (here it hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
