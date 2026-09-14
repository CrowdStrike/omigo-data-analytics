# Availability Math

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Availability Math

**Subtitle:** "Four nines" sounds like a slogan until you convert it to minutes — and until you notice that every service you call in a chain multiplies your downtime

## What Four Nines Buys a Checkout Page

**Tags:** `core idea` (blue), `the nines table` (green)

- **The store** — an online store's checkout page promises customers it is "almost always up"
- **The question** — "almost always" is vague; ops teams count nines: 99%, 99.9%, 99.99%, 99.999%
- **The conversion** — each extra nine cuts allowed downtime by 10×: 99.9% means 8.77 hours down per year
- **Four nines** — 99.99% allows only 52.6 minutes per year; five nines allows just 5.26 minutes
- **The definition** — availability = fraction of time the service answers; downtime budget = (1 − availability) × one year

*Example (italic):* At 99% the checkout may be dark 87.7 hours a year — two full work weeks; at 99.99% the whole year's outage fits in one lunch break.

**Key point:** Availability is just a fraction of the 8,766-hour year — each added nine divides the downtime budget by ten, and the jump from three to four nines is the jump from "a night" to "a coffee".

### Visualization (canvas `c1`, 720×300)

Horizontal bar chart: yearly downtime budget at each nines level, one bar per level, pixel widths log-feel so all four rows stay readable.

- **Title (bold 15px, `#1a5276`, top center):** "The Nines Table: What One Year of Downtime Budget Looks Like".
- **Axis:** vertical 2px `#999` baseline at x=190, bars extend right, max width 480; log-feel achieved by hardcoded pixel widths, not a real log axis.
- **Rows (top to bottom at y = 75, 125, 175, 225), each with a left-aligned 12px `#444` label at x=20:**
  - "99% — two nines": blue `#2a78d6` bar width 480, 12px label at bar end "87.7 h/yr"
  - "99.9% — three nines": blue bar width 330, label "8.77 h/yr"
  - "99.99% — four nines": green `#008300` bar width 180, label "52.6 min/yr"
  - "99.999% — five nines": green bar width 60, label "5.26 min/yr"
- **Bar style:** 16px tall, fill `rgba(42,120,214,0.30)` for blue rows / `rgba(0,131,0,0.25)` for green rows, 2px solid border in the row color, 12px bold value labels in the row color.
- **Annotation (bold 13px violet `#4a3aa7`, right side near y=250):** "each nine divides the budget by 10".
- **Caption (12px `#444`, bottom right):** "budgets exact (8,766 h year); bar widths schematic (log-feel)".

## Five Services in a Chain

**Tags:** `worked example` (blue), `serial dependencies` (orange)

- **The chain** — one checkout hits auth, then catalog, then cart, then payment, then email — five services
- **Each is good** — every service holds 99.9% on its own, a respectable three nines
- **The rule** — serial availability multiplies: chain availability = 0.999 × 0.999 × 0.999 × 0.999 × 0.999
- **Hand-check** — 0.999² = 99.8%, 0.999³ = 99.7%, 0.999⁴ = 99.6%, 0.999⁵ ≈ 99.5%
- **The bill** — 99.5% is 43.8 hours down per year — five 8.77-hour budgets stacked end to end

*Example (italic):* No single team missed its 99.9% target, yet checkout as a whole is dark 43.8 hours a year — each hop quietly added another 8.77-hour budget.

**Key point:** A request that needs all of N services inherits all N failure budgets — availabilities multiply, so the chain is always worse than its weakest link, never better.

### Visualization (canvas `c2`, 720×300)

Bar chart: chain downtime hours per year as services are added one at a time, auth through email, bars growing left to right.

- **Title (bold 15px, `#1a5276`, top center):** "Each 99.9% Hop Adds 8.77 Hours: the Checkout Chain".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; y = downtime h/yr 0 to 50, gridlines `#e5e9ef` at 10/20/30/40 with 12px `#444` labels; x = five bars centered at x = 120, 240, 360, 480, 600.
- **Bars (56px wide, fill `rgba(42,120,214,0.30)`, 2px `#2a78d6` border):** heights for downtime `[8.77, 17.5, 26.3, 35.0, 43.8]` h/yr; last bar recolored orange `rgba(217,89,38,0.30)` with 2px `#d95926` border.
- **Bar labels:** 12px `#444` under each bar: "+auth", "+catalog", "+cart", "+payment", "+email"; bold 12px `#2c3e50` value on top of each bar: "8.77 h", "17.5 h", "26.3 h", "35.0 h", "43.8 h".
- **Availability labels (11px `#6b7280`, below the service names):** "99.9%", "99.8%", "99.7%", "99.6%", "99.5%".
- **Annotation (bold 13px orange `#d95926`, near x=430, y=80):** "five healthy services, one sick chain".
- **Caption (12px `#444`, bottom right):** "0.999^N, downtime hours exact".

## Two Cheap Copies Beat One Heroic Server

**Tags:** `where it's used` (blue), `redundancy` (green)

- **The flip** — parallel is the mirror of serial: the pair fails only when BOTH replicas are down
- **The rule** — unavailability multiplies: two independent 99% replicas fail together 0.01 × 0.01 = 0.0001 of the time
- **The payoff** — the 99% pair delivers 99.99% — from 87.7 hours down per year to 52.6 minutes
- **The caveat** — the math needs independence; a shared power feed, deploy, or bad config fails both at once
- **In practice** — this is why load balancers, multi-zone databases, and dual payment providers exist

*Example (italic):* The store runs payment against two independent 99% providers; a single provider allows 87.7 hours of outage a year, the pair allows 52.6 minutes.

**Key point:** Serial dependencies multiply availabilities down; parallel replicas multiply unavailabilities down — redundancy buys nines cheaply, but only across failures that are truly independent.

### Visualization (canvas `c3`, 720×300)

Two-row flow diagram: one 99% box (serial, single path) vs two 99% boxes stacked in parallel, each row ending with its downtime figure.

- **Title (bold 15px, `#1a5276`, top center):** "One 99% Server vs Two in Parallel".
- **Row 1 (y=95), label 12px `#444` at x=20:** "single"; one blue `#2a78d6` rounded box at x=170 labeled "payment A — 99%" (12px), 3px `#2a78d6` arrow to bold 13px `#d95926` text at x=470: "down 87.7 h/yr".
- **Row 2 (centered y=205), label:** "parallel pair"; two blue boxes stacked at x=170 (y=175 and y=225) labeled "payment A — 99%" and "payment B — 99%", both with 3px arrows converging to a green `#008300` box at x=400 labeled "pair — 99.99%", then bold 13px `#008300` text at x=560: "down 52.6 min/yr".
- **Box style:** 160px wide, 36px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 12px violet `#4a3aa7`, centered near y=272):** "0.01 × 0.01 = 0.0001 — valid only if A and B fail independently".
- **Caption (12px `#444`, bottom right):** "availabilities exact, independence assumed".

## Promising a Nine You Don't Own

**Tags:** `common mistake` (red), `SLA` (orange)

- **The promise** — the store signs a 99.95% checkout SLA: at most 4.38 hours of downtime per year
- **The dependency** — payment runs on a provider whose own SLA is 99.9% — an 8.77-hour yearly budget
- **The bound** — a serial dependency caps you: checkout availability ≤ payment availability, always
- **The mistake** — quoting an SLA above your weakest serial dependency, hoping it won't be that bad
- **The fix** — either buy a stronger dependency SLA, add an independent parallel provider, or promise less

*Example (italic):* Even with flawless in-house code, the 99.9% payment provider alone can burn 8.77 hours a year — double the 4.38 hours the 99.95% contract allows.

**Common mistake:** Setting your SLA from your own code's track record while ignoring dependency SLAs — a serial chain can never be more available than any service in it, so your promise is bounded by theirs.

### Visualization (canvas `c4`, 720×300)

Horizontal bar chart: downtime the 99.95% SLA allows vs downtime the 99.9% dependency can inflict, the overflow highlighted in red.

- **Title (bold 15px, `#1a5276`, top center):** "A 99.95% Promise on a 99.9% Dependency Cannot Hold".
- **Axis:** vertical 2px `#999` baseline at x=230, bars extend right; scale 48px per hour of downtime; gridline `#e5e9ef` ticks with 12px `#444` labels at 2, 4, 6, 8 h.
- **Row 1 (y=100), label 12px `#444` at x=20:** "your SLA allows (99.95%)"; green `#008300` bar width 210 (4.38 h), bold 12px green label "4.38 h/yr" at bar end.
- **Row 2 (y=170), label:** "payment alone can burn (99.9%)"; blue `#2a78d6` bar width 210 for the first 4.38 h, then red `#e74c3c` continuation bar width 211 to 8.77 h, bold 12px red label "8.77 h/yr" at bar end.
- **Overflow marker:** vertical dashed `#6b7280` (dash 4/3) line at the 4.38 h mark spanning both rows, 12px `#6b7280` label "your whole budget" at its top.
- **Bar style:** 18px tall, fills `rgba(0,131,0,0.25)` / `rgba(42,120,214,0.30)` / `rgba(231,76,60,0.25)`, 2px solid borders in the segment color.
- **Annotation (bold 13px red `#e74c3c`, near x=480, y=235):** "breach possible with zero bugs of your own".
- **Caption (12px `#444`, bottom right):** "hours exact from the SLA percentages".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); the nines/downtime numbers are exact arithmetic on an 8,766-hour (365.25-day) year — 99% = 87.7 h/yr, 99.9% = 8.77 h/yr, 99.99% = 52.6 min/yr, 99.999% = 5.26 min/yr, 0.999^5 ≈ 99.5% = 43.8 h/yr, two parallel 99% replicas = 99.99% = 52.6 min/yr, 99.95% SLA = 4.38 h/yr — label them exact; only the checkout-flow story is illustrative.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
