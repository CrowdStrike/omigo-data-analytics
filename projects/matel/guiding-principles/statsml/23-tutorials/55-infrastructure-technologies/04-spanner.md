# Spanner

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Spanner

**Subtitle:** Google's Spanner runs SQL transactions across continents by admitting its clocks are uncertain — and making every transaction wait out the uncertainty before it commits

## A Transfer That Two Continents Must Agree On

**Tags:** `core idea` (blue), `TrueTime` (green), `Google 2012` (orange)

- **The bank** — one account replicated in Tokyo and Virginia; writes route through the split's Paxos leader
- **Two transfers** — T1 deposits $300 in Tokyo; moments later T2 withdraws $700 in Virginia
- **The stakes** — order T1 then T2 clears the withdrawal; the reverse bounces it — replicas must agree
- **The trick** — TrueTime: GPS receivers and atomic clocks report time as an interval [earliest, latest]
- **The honesty** — the clock never claims one instant; it guarantees true time lies inside the interval
- **The wait** — a transaction waits out its interval before committing, so its timestamp is safely past

*Example (italic):* At the same real instant, Tokyo's clock reads [10:00:00.120, 10:00:00.130] and Virginia's reads [10:00:00.118, 10:00:00.128] — neither knows the exact time, but both intervals contain it.

**Key point:** TrueTime replaces "what time is it?" with "time is somewhere in this small window" — and Spanner turns that bounded doubt into a global transaction ordering guarantee.

### Visualization (canvas `c1`, 720×300)

Interval diagram on a shared millisecond axis: two data centers' TrueTime readings as horizontal bars, with the true instant as a vertical line passing through both.

- **Title (bold 15px, `#1a5276`, top center):** "TrueTime: Two Clocks, Two Intervals, One True Instant Inside Both".
- **Axis:** origin x=60, baseline y=245, plot width 600; x = milliseconds within 10:00:00 from 110 to 150 at 15px/ms (`x(t) = 60 + (t-110)*15`), 12px `#444` tick labels ".110" ".120" ".130" ".140" ".150" every 10 ms; light gridlines `#e5e9ef` at each tick.
- **Tokyo bar (y=110):** blue `rgba(42,120,214,0.30)` bar from ms 120 to 130 (x=210 to x=360), 22px tall, 2px `#2a78d6` border; 12px `#2a78d6` label "Tokyo: [.120, .130]" left of the bar at x=65.
- **Virginia bar (y=180):** green `rgba(0,131,0,0.25)` bar from ms 118 to 128 (x=180 to x=330), 22px tall, 2px `#008300` border; 12px `#008300` label "Virginia: [.118, .128]" at x=65.
- **True time marker:** vertical dashed `#6b7280` (dash 4/3) line at ms 125 (x=285) from y=75 to y=245, bold 12px `#6b7280` label "true time .125" at its top.
- **Annotation (bold 13px violet `#4a3aa7`, near x=430, y=140):** "neither clock knows .125 — both bracket it".
- **Caption (12px `#444`, bottom right):** "interval semantics exact; millisecond values illustrative".

## Waiting Out the Uncertainty, By Hand

**Tags:** `worked example` (blue), `commit wait` (green)

- **The setup** — clock uncertainty ε = 5 ms (illustrative; the 2012 paper reports ε typically under 7 ms)
- **T1 commits** — Tokyo reads TT.now() = [120, 130] and picks timestamp s1 = 130, the latest edge
- **The wait** — Tokyo holds the commit until its own earliest passes 130 — about 2ε = 10 ms of waiting
- **The release** — at real time 135 T1 commits; anyone who sees T1's money knows real time passed 130
- **T2 commits** — Virginia starts T2 at real time 140, reads [135, 145], picks s2 = 145 — above 130 for sure
- **The guarantee** — s2 > s1 with no messages exchanged: the wait alone ordered the two continents

*Example (italic):* T1 waits 10 ms and commits with timestamp 130; T2, started after T1 finished, can never draw a timestamp below 130 — the $700 withdrawal always sorts after the $300 deposit.

**Key point:** Commit wait costs about twice the clock uncertainty (the rule is exact: wait until TT.now().earliest exceeds the chosen timestamp); after it, every later transaction anywhere gets a larger timestamp.

### Visualization (canvas `c2`, 720×300)

Timeline chart of the two commits on one millisecond axis: T1's interval, chosen timestamp, shaded commit-wait window, then T2's interval and timestamp landing safely above.

- **Title (bold 15px, `#1a5276`, top center):** "Commit Wait: T1 Holds 10 ms So T2 Can Never Sort Below It".
- **Axis:** origin x=60, baseline y=245, plot width 600; x = ms 110 to 150 at 15px/ms (`x(t) = 60 + (t-110)*15`), 12px `#444` ticks every 10 ms; gridlines `#e5e9ef`.
- **T1 interval (y=100):** blue `rgba(42,120,214,0.30)` bar ms 120–130 (x=210–360), 22px tall, 2px `#2a78d6` border, 12px `#2a78d6` label "T1 reads [120, 130]" at x=65.
- **s1 marker:** bold 3px `#1a5276` vertical tick at ms 130 (x=360) spanning y=90–130, bold 12px `#1a5276` label "s1 = 130" above it.
- **Commit-wait band:** orange `rgba(217,89,38,0.20)` rectangle ms 125–135 (x=285–435, from when s1 is picked at real 125 to commit at 135), y=90 to y=130, bold 12px `#d95926` label "wait 10 ms" centered above at y=80; small 12px `#444` "commit at 135" just right of x=435.
- **T2 interval (y=180):** green `rgba(0,131,0,0.25)` bar ms 135–145 (x=435–585), 22px tall, 2px `#008300` border, 12px `#008300` label "T2 reads [135, 145]" at x=65.
- **s2 marker:** bold 3px `#008300` vertical tick at ms 145 (x=585) spanning y=170–210, bold 12px `#008300` label "s2 = 145" above it.
- **Annotation (bold 13px violet `#4a3aa7`, near x=200, y=270):** "s2 = 145 > s1 = 130 — order guaranteed, no coordinator asked".
- **Caption (12px `#444`, bottom right):** "wait rule exact; ε = 5 ms and all timestamps illustrative".

## SQL Across Continents, and What It Costs

**Tags:** `where it's used` (blue), `Paxos` (green), `write latency` (orange)

- **The paper** — Google published Spanner in 2012: a SQL database with ACID transactions across continents
- **The replication** — every write is replicated synchronously via Paxos before it is acknowledged
- **No boss** — external consistency (a global transaction order) without any single ordering server
- **The bill** — cross-continent writes pay Paxos round trips plus commit wait before they return
- **The reads** — snapshot reads at a past timestamp run lock-free on any replica that is caught up
- **The echo** — CockroachDB and others rebuilt the design on commodity clocks, with wider uncertainty

*Example (italic):* An illustrative transatlantic write: 2 ms local work + 60 ms Paxos replication + 10 ms commit wait = 72 ms — the price of ACID across an ocean.

**Key point:** Spanner's trade is explicit — SQL semantics and globally ordered transactions everywhere, paid for in write latency; replication dominates the bill and commit wait adds a small fixed slice.

### Visualization (canvas `c3`, 720×300)

Horizontal stacked bar chart: the anatomy of one write's latency at three replica spreads, split into local work, Paxos replication, and commit wait.

- **Title (bold 15px, `#1a5276`, top center):** "Where a Spanner Write Spends Its Milliseconds".
- **Layout:** row labels 12px `#444` left-aligned at x=20; bars start at x=230, scale 5 px/ms, 16px tall; thin 2px `#999` vertical baseline at x=230 from y=55 to y=215.
- **Rows (y = 70, 125, 180), segments drawn left to right as [local, Paxos, wait]:**
  - "same city — 15 ms": blue `#2a78d6` width 10 (2 ms), violet `#4a3aa7` width 15 (3 ms), green `#008300` width 50 (10 ms); total width 75
  - "cross-country — 42 ms": blue width 10 (2 ms), violet width 150 (30 ms), green width 50 (10 ms); total width 210
  - "transatlantic — 72 ms": blue width 10 (2 ms), violet width 300 (60 ms), green width 50 (10 ms); total width 360
- **Totals:** 11px `#444` label at each bar's right end: "15 ms", "42 ms", "72 ms".
- **Legend (12px, y=235, starting x=230):** blue swatch "local work", violet swatch "Paxos replication", green swatch "commit wait".
- **Annotation (bold 13px green `#008300`, near x=440, y=105):** "commit wait is the same 10 ms everywhere — distance drives the rest".
- **Caption (12px `#444`, bottom right):** "all latencies illustrative".

## The Clocks Are Still Not Synchronized

**Tags:** `common mistake` (red), `clock uncertainty` (orange)

- **The confusion** — people hear "atomic clocks" and assume Spanner's clocks agree perfectly
- **Still uncertain** — the clocks drift like any others; TrueTime only bounds how wrong they can be
- **The real trick** — knowing the bound lets a transaction wait until its timestamp is safely in the past
- **Skipping the wait** — commit T1 at real time 125 with s1 = 130 and the timestamp is still in the future
- **The inversion** — T2 starts at real 127, Virginia reads [117, 127], picks s2 = 127 — below s1 = 130
- **Wider windows** — on plain NTP the uncertainty is hundreds of ms; the same wait becomes unusably long

*Example (italic):* Without the wait, a snapshot read at timestamp 128 shows T2's $700 withdrawal but not the $300 deposit it depended on — history reads backwards.

**Common mistake:** Believing the hardware synchronizes the clocks. GPS and atomic clocks only shrink the uncertainty window; the ordering guarantee comes from waiting the window out — and with cheap clocks the window is too wide to wait out.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: committing without the wait (a later transfer draws a smaller timestamp) vs with the wait (timestamps match reality), shown as transaction boxes with real times and timestamps.

- **Title (bold 15px, `#1a5276`, top center):** "Skip the Wait and a Later Transfer Can Sort First".
- **Row 1 (y=95), label 12px `#444` at x=20:** "no commit wait"; blue `#2a78d6` rounded box at x=170 labeled "T1 commits at real 125, s1 = 130" (12px, two lines), 3px arrow to a red `#e74c3c` box at x=430 labeled "T2 at real 127 reads [117, 127], s2 = 127" with bold 12px red "✗ 127 < 130 — history inverted" beneath.
- **Row 2 (y=205), label:** "with commit wait"; blue box at x=170 labeled "T1 waits to real 135, s1 = 130", 3px arrow to a green `#008300` box at x=430 labeled "T2 at real 140 reads [135, 145], s2 = 145" with bold 12px green "✓ 145 > 130 — order matches reality" beneath.
- **Box style:** 200–220px wide, 44px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(231,76,60,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text, 2px borders in the box's line color.
- **Annotation (bold 13px orange `#d95926`, centered near y=272):** "the clocks never agree — the wait is what makes the timestamps honest".
- **Caption (12px `#444`, bottom right):** "millisecond values illustrative; the inversion mechanism exact".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all values are the hardcoded literals above (no randomness). The TrueTime interval semantics, the commit-wait rule (wait until TT.now().earliest exceeds the chosen timestamp, ≈2ε), and the no-wait inversion mechanism are exact per the 2012 Spanner paper; every millisecond figure (ε = 5 ms, timestamps 120/130/135/140/145, real times 125/127, latency splits 2/3/30/60/10 ms and totals 15/42/72 ms) is invented and labeled illustrative. "ε typically under 7 ms" is the paper's reported figure.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
