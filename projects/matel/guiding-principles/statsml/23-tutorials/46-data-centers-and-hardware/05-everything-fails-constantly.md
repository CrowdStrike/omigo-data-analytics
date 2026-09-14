# Everything Fails, Constantly

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Everything Fails, Constantly

**Subtitle:** A disk with a 2% annual failure rate is very reliable alone — buy 100,000 of them and 5–6 die every single day, so reliability has to live in software, not hardware

## The Disk That's 98% Safe — Times 100,000

**Tags:** `core idea` (blue), `fleet scale` (green), `failure rate` (orange)

- **One disk** — a 2% annual failure rate means a 98% chance it survives the whole year
- **The fleet** — a data center runs 100,000 of them; 2% × 100,000 = 2,000 dead disks per year (exact)
- **Per day** — 2,000 ÷ 365 ≈ 5.5, so somewhere in the building 5–6 disks die every single day
- **Not an event** — at fleet scale, failure is not an incident; it is a steady, predictable arrival rate
- **Not just disks** — the same multiplication hits servers, DIMMs, power supplies, and switches

*Example (italic):* The morning report never reads "a disk failed last night" — it reads "6 failed yesterday, 5 expected today", every day, forever.

**Key point:** Multiply a small failure rate by a huge fleet and failure stops being rare — 2% of 100,000 is 2,000 a year, arriving at 5–6 per day.

### Visualization (canvas `c1`, 720×300)

Horizontal bar chart: expected disk failures per year at four fleet sizes, same 2% annual failure rate.

- **Title (bold 15px, `#1a5276`, top center):** "Same 2% Disk, Four Fleet Sizes: Failures per Year".
- **Axis:** vertical 2px `#999` baseline at x=230, bars extend right, max width 440; log-feel achieved by hardcoded pixel widths, not a real log axis.
- **Rows (top to bottom at y = 70, 120, 170, 220), each with a left-aligned 12px `#444` label at x=20:**
  - "1 disk — 0.02 failures/yr": blue `#2a78d6` bar width 4
  - "1,000 disks — 20/yr": blue bar width 170
  - "10,000 disks — 200/yr": blue bar width 300
  - "100,000 disks — 2,000/yr": red `#e74c3c` bar width 430
- **Bar style:** 16px tall, blue fill `rgba(42,120,214,0.30)` with 2px `#2a78d6` edge, red row solid `#e74c3c`; 12px value labels ("0.02", "20", "200", "2,000") at bar ends.
- **Annotation (bold 13px red `#e74c3c`, right of the bottom bar, y=250):** "= 5–6 dead disks every single day".
- **Caption (12px `#444`, bottom right):** "bar widths schematic (log-feel), failure counts exact: fleet × 2%".

## Doing the MTBF Arithmetic Honestly

**Tags:** `worked example` (blue), `MTBF` (green)

- **The spec sheet** — 2% per year means one disk's MTBF is 8,760 h ÷ 0.02 = 438,000 hours (~50 years)
- **The trap** — "50 years" describes the average over many disks, not a promise your disk lasts 50 years
- **Fleet MTBF** — divide by fleet size: 438,000 ÷ 100,000 = 4.4 hours between failures somewhere
- **Per week** — 2,000 ÷ 52 ≈ 38 disk failures a week; a repair tech's cart is never empty
- **Hand-check** — 5.48/day × 7 days ≈ 38/week; 38 × 52 ≈ 2,000/year — the numbers close the loop

*Example (italic):* The fleet dashboard shows Monday 6, Tuesday 5, Wednesday 4, Thursday 7, Friday 5, Saturday 6, Sunday 5 — 38 dead disks in one ordinary week.

**Key point:** MTBF only sounds comforting per device — divide by the fleet and 438,000 hours becomes one failure every 4.4 hours, around the clock.

### Visualization (canvas `c2`, 720×300)

Vertical bar chart: disk failures per day across one sample week, with the long-run daily average as a dashed line.

- **Title (bold 15px, `#1a5276`, top center):** "One Ordinary Week: 38 Dead Disks, Right on Schedule".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = seven bars labeled "Mon"–"Sun" (12px `#444`), evenly spaced; y = failures 0 to 8, gridlines `#e5e9ef` at 2/4/6 with 12px `#444` tick labels.
- **Bars:** heights from the hardcoded array `[6, 5, 4, 7, 5, 6, 5]` (sums to 38); fill `rgba(42,120,214,0.30)`, 2px `#2a78d6` edge, ~52px wide; bold 12px `#2a78d6` count label above each bar.
- **Average line:** dashed `#d95926` (dash 5/4) 2px horizontal line at y for 5.48, 12px `#d95926` label "average 5.48/day (2,000 ÷ 365)" at its right end.
- **Annotation (bold 13px `#1a5276`, upper left near x=120, y=70):** "a failure every 4.4 hours, somewhere".
- **Caption (12px `#444`, bottom right):** "daily counts illustrative; weekly total 38 and average 5.48/day exact from the 2% rate".

## The Inversion: Reliability Moves Into Software

**Tags:** `where it's used` (blue), `design inversion` (green)

- **The old instinct** — buy premium ultra-reliable hardware so failures become rare enough to ignore
- **The arithmetic says no** — even a 0.5% disk at 5× the price still fails 500 times a year at 100,000 scale
- **The inversion** — hyperscalers buy cheap commodity parts and put the reliability in software instead
- **Replication** — every piece of data lives on several machines, so one dead disk loses nothing
- **Self-healing** — software detects the failure, drains the machine, reroutes traffic; humans repair in batches

*Example (italic):* When disk 48,201 dies at 3am, no pager fires — replicas keep serving, a copy is rebuilt within hours, and a tech swaps the disk on Thursday's rack-by-rack round.

**Key point:** At fleet scale you cannot buy your way out of failure — you design for it: replicate the data, fail over automatically, and turn repairs into a scheduled batch chore.

### Visualization (canvas `c3`, 720×300)

Two-row flow diagram: the "buy reliability" path that still fails vs the self-healing software loop (detect → drain → reroute → batch repair).

- **Title (bold 15px, `#1a5276`, top center):** "Two Answers to 2,000 Failures a Year".
- **Row 1 (y=95), label 12px `#444` at x=20:** "buy reliability"; blue `#2a78d6` rounded box at x=170 labeled "premium disk, 0.5% AFR, 5× price" (12px), 3px arrow to a red `#e74c3c` box at x=440 labeled "still 500 failures/yr" with bold 12px red "✗ pagers still fire".
- **Row 2 (y=205), label:** "reliability in software"; four green `#008300` rounded boxes left to right at x=150, 300, 450, 580 labeled "detect", "drain", "reroute", "repair in batches" (12px), 3px arrows between them, with bold 12px green "✓ no user impact" beneath the last box.
- **Box style:** 110–190px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(231,76,60,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=270):** "cheap parts + software loop beats expensive parts + hope".
- **Caption (12px `#444`, bottom right):** "500/yr = 100,000 × 0.5%, exact; prices illustrative".

## Two Replicas Dying Together Is Not Rare

**Tags:** `common mistake` (red), `failure domains` (orange)

- **The comfort** — "two copies of the same data both failing at once? astronomically unlikely" — per pair, yes
- **The arrival clock** — with a failure every 4.4 hours, another lands within the hour ~20% of the time
- **The count** — 1 − e^(−1/4.4) ≈ 0.20, so ~400 of the 2,000 yearly failures have a near-twin
- **Correlated killers** — a rack's switch or power feed dying takes out every disk in it simultaneously
- **The mistake** — placing a file's replicas in the same rack, so one switch failure erases all copies

*Example (italic):* Three replicas in rack 12 survive any single disk death — until rack 12's top-of-rack switch dies and all three vanish in the same second.

**Common mistake:** Multiplying per-disk probabilities as if failures were independent and rare. At fleet scale near-coincident failures are routine, and shared racks make them simultaneous — spread replicas across failure domains.

### Visualization (canvas `c4`, 720×300)

Two-row placement diagram: three replicas packed into one rack (switch failure kills all) vs spread across three racks (one rack failure leaves two copies).

- **Title (bold 15px, `#1a5276`, top center):** "Replica Placement: Same Rack vs Across Failure Domains".
- **Row 1 (y=100), label 12px `#444` at x=20:** "all in rack 12"; one rounded rack outline (2px `#e74c3c`, fill `rgba(231,76,60,0.06)`) at x=170, 220×70px, containing three small blue `#2a78d6` disk boxes (46×28px, 12px labels "R1" "R2" "R3"); bold 12px red `#e74c3c` text at x=430: "✗ switch dies → all 3 copies gone".
- **Row 2 (y=210), label:** "across racks 4 / 12 / 31"; three rack outlines (2px `#008300`, fill `rgba(0,131,0,0.06)`) at x=170, 320, 470, each 110×70px containing one blue disk box ("R1" / "R2" / "R3"); bold 12px green `#008300` text at x=600: "✓ one rack dies → 2 copies survive".
- **Box style:** 8px radius outlines, disk boxes fill `rgba(42,120,214,0.25)` with 12px `#2c3e50` labels.
- **Annotation (bold 13px orange `#d95926`, centered near y=285):** "spread copies across racks, rows, and power feeds — never share a single point of failure".
- **Caption (12px `#444`, bottom right):** "rack IDs illustrative; the ~20% within-an-hour figure is exact for a Poisson arrival every 4.4 h".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness). Fleet failure arithmetic is exact: 2% × 100,000 = 2,000/yr, 2,000 ÷ 365 = 5.48/day, 2,000 ÷ 52 ≈ 38/wk; MTBF 8,760 ÷ 0.02 = 438,000 h per disk and 438,000 ÷ 100,000 = 4.4 h fleet-wide; 100,000 × 0.5% = 500/yr; 1 − e^(−1/4.4) ≈ 0.20 is exact for Poisson arrivals at that rate. Daily counts `[6,5,4,7,5,6,5]`, prices, and rack IDs are invented and labeled illustrative.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
