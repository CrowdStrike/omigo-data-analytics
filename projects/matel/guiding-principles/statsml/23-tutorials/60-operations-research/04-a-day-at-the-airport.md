# A Day at the Airport

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** A Day at the Airport

**Subtitle:** Gates, runways, and crews — three invisible optimizations that decide where your plane parks, when it lands, and who flies it

## Which Plane Gets Which Gate

**Tags:** `in practice` (blue), `assignment` (green)

- **Four arrivals** — flights 101, 202, 303, and 404 each need a gate for a fixed time window
- **Two gates** — only Gate 1 is big enough for a wide-body, and 101 and 404 are both wide-bodies
- **The rules** — one plane per gate at a time, plus a 20-minute gap between planes on a gate
- **A legal plan** — Gate 1 takes 101 then 404, Gate 2 takes 202 then 303; every gap is 30 minutes
- **The trap** — 303 also fits Gate 1 with a legal 20-minute gap, but that strands wide-body 404

*Example (italic):* Flight 303 at Gate 1 breaks no single rule — it only breaks the plan, because 404 then has nowhere legal to park.

**Key point:** A gate assignment is judged as a whole plan, not rule by rule: every flight must get a spot without blocking anyone else's.

### Visualization (canvas `c1`, 720×300)

A Gantt-style gate timeline: two rows (Gate 1 big, Gate 2), colored flight blocks over a 08:45–11:45 time axis, same-gate gaps labeled.

- **Title (bold 15px, `#1a5276`, top center):** "Two Gates, Four Flights — a Legal Plan (illustrative)".
- **Time axis:** minutes since 08:45 (0–180) mapped to x = 90 + m·(600/180); vertical gridlines 1px `#e5e9ef` from y=60 to y=240 every 30 minutes from 09:00 to 11:30, tick labels ("09:00" … "11:30") 12px `#6b7280` centered at y=256; baseline 1px `#999` at y=240 from x=90 to x=690.
- **Row labels (bold 13px `#1a5276`, right-aligned at x=82):** "Gate 1 (big)" at y=112, "Gate 2" at y=192.
- **Flight blocks (height 48; Gate 1 row y=84, Gate 2 row y=164):** each block fill is the flight color at 0.18 alpha with a 2px solid border of the same color; two centered text lines — bold 13px flight label at block y+21, 11px `#2c3e50` time range at y+38:
  - "101 (wide)" 09:00–10:00 on Gate 1, blue `#2a78d6`
  - "404 (wide)" 10:30–11:30 on Gate 1, violet `#4a3aa7`
  - "202" 09:10–09:50 on Gate 2, green `#008300`
  - "303" 10:20–11:10 on Gate 2, aqua `#199e70`
- **Gap labels (bold 12px orange `#d95926`, centered at row mid y+28):** "30-min gap" centered in the Gate 1 idle stretch (10:00–10:30, label at 10:15) and in the Gate 2 idle stretch (09:50–10:20, label at 10:05).
- **Annotation (bold 13px green `#008300`, centered at y=284):** "legal: wide-bodies on the big gate, every same-gate gap ≥ 20 min".

## Who Lands Next — Order Is Money

**Tags:** `worked example` (blue), `sequencing` (orange)

- **Wake turbulence** — a plane leaves swirling air behind it; a jumbo's wake is the dangerous one
- **Pair gaps** — the safety wait depends on the pair: who lands first and who follows behind
- **The table** — after a jumbo: smalls wait 4 min, jumbos 2; after a small: anyone waits just 1
- **Order A** — alternating J1, S1, J2, S2 spends 4 + 1 + 4 = 9 minutes of gaps first-to-last
- **Order B** — smalls first, S1, S2, J1, J2, spends 1 + 1 + 2 = 4 minutes — same planes, 5 saved
- **Why it works** — jumbos grouped last means the costly small-after-jumbo gap is never paid

*Example (italic):* Five minutes saved per landing wave, wave after wave all day, is why towers sequence by pair rather than by queue.

**Key point:** Because the safety gap depends on the pair, the same four planes can cost 9 minutes or 4 — landing order is a real decision variable.

### Visualization (canvas `c2`, 720×300)

Two horizontal landing timelines (Order A above, Order B below) on the same 0–9 minute scale, with gap widths labeled between landings and total-time annotations.

- **Title (bold 15px, `#1a5276`, top center):** "Same Four Planes, Two Orders (illustrative)".
- **Scale:** minute m mapped to x = 90 + m·62 (minute 0 → x=90, minute 9 → x=648).
- **Order A (line y=112):** header bold 13px `#2c3e50` left-aligned at (90, 74): "Order A — alternate: J1 → S1 → J2 → S2"; total bold 13px magenta `#d55181` right-aligned at (690, 74): "total gaps: 9 min". Landings at minutes 0 (J1), 4 (S1), 5 (J2), 9 (S2).
- **Order B (line y=214):** header bold 13px `#2c3e50` left-aligned at (90, 176): "Order B — smalls first: S1 → S2 → J1 → J2"; total bold 13px green `#008300` right-aligned at (690, 176): "total gaps: 4 min — 5 saved". Landings at minutes 0 (S1), 1 (S2), 2 (J1), 4 (J2).
- **Timelines:** 1.5px `#999` horizontal line from x=90 to x=660 at each row's y.
- **Markers:** jumbos (J1, J2) filled circles r=11 violet `#4a3aa7`; smalls (S1, S2) filled circles r=7 aqua `#199e70`; plane labels bold 12px in the marker color centered at y+28; gap labels ("4 min", "1 min", "2 min") bold 12px orange `#d95926` centered midway between consecutive markers at y−16.
- **Caption (12px `#6b7280`, centered at y=284):** "gap after a jumbo: 4 min for a small, 2 for a jumbo — after a small: 1 min".

## Who Flies the Plane

**Tags:** `in practice` (blue), `scheduling` (green)

- **Legal rest** — a pilot must rest a minimum number of hours between duties, no exceptions
- **Type ratings** — a pilot flies only aircraft types they are trained and certified on
- **Getting home** — a good roster routes each crew through the week to end Friday at home base
- **Built ahead** — rosters are solved months early, from the flight schedule and staffing forecasts
- **The scale** — thousands of crews and rules; among the biggest optimizations run commercially

*Example (italic):* Alice is rated on the small jet only, so the Wednesday jumbo leg can never appear in her week, however convenient.

**Key point:** A crew roster is a giant legal-scheduling puzzle — rest hours, type ratings, and getting home — solved months before the first flight.

### Visualization (canvas `c3`, 720×300)

A week grid: two crew rows (Alice, Bob) × five day columns (Mon–Fri) with flight blocks, rest blocks, and a "back home Friday" annotation.

- **Title (bold 15px, `#1a5276`, top center):** "One Week, Two Crews (illustrative)".
- **Grid:** origin x=150, y=66; column width 104 (5 columns, Mon–Fri), row height 78 (2 rows); day headers bold 13px `#2c3e50` centered above each column at y=58; row labels bold 13px `#1a5276` right-aligned at x=142, vertically centered per row: "Crew Alice", "Crew Bob"; cell borders 1px `#e5e9ef`.
- **Cell blocks:** inset rectangles (x+6, y+8, 92×62). Fly blocks: fill `rgba(42,120,214,0.15)`, 2px blue `#2a78d6` border, bold 12px blue "fly" at block y+24 and 11px `#2c3e50` route at y+44. Rest blocks: fill `#eef1f4`, 1px `#b8c4cf` border, 12px `#6b7280` "rest" at y+24 and 11px `#6b7280` location at y+44. All text centered in the block.
  - Alice: Mon fly "Home → P", Tue fly "P → Q", Wed rest "(at Q)", Thu fly "Q → P", Fri fly "P → Home"
  - Bob: Mon rest "(at home)", Tue fly "Home → R", Wed fly "R → R hops", Thu rest "(at R)", Fri fly "R → Home"
- **Annotation (bold 13px green `#008300`, centered at y=252):** "both crews end Friday back at home base".
- **Caption (12px `#6b7280`, centered at y=276):** "rest days and type ratings decide which legs a crew can even touch".

## The Day Never Goes to Plan — and That's Data

**Tags:** `core idea` (blue), `data loop` (orange)

- **One late arrival** — a morning flight lands 25 minutes late and its planned gate is now occupied
- **The cascade** — the gate swap delays two departures, and one crew's legal rest clock runs out
- **Everything is logged** — every swap, delay, and crew change is written down as the day happens
- **The loop closes** — tonight's log is next month's forecast input, which shapes the next plan
- **The chain** — forecasts feed plans, plans meet reality, and reality's logs feed the next forecast

*Example (italic):* Today's 25-minute delay is one row in the dataset that decides how much slack next month's gate plan carries.

**Key point:** An airport day is a loop, not a line: forecasts feed optimizers, optimizers meet the real day, and the day's logs train the next forecast.

### Visualization (canvas `c4`, 720×300)

A circular flow diagram of four boxes — forecast (top) → plan (right) → the actual day (bottom) → logs (left) → back to forecast — with a center annotation.

- **Title (bold 15px, `#1a5276`, top center):** "The Airport Data Loop".
- **Boxes:** fill `#fbfcfd`, 2px colored border, bold 13px colored header centered at box y+19, 11px `#6b7280` sub-line centered at y+36:
  - "FORECAST" blue `#2a78d6` at (285, 44, 150×46), sub "next month's demand"
  - "PLAN" green `#008300` at (505, 128, 150×46), sub "gates · runway · crews"
  - "THE ACTUAL DAY" orange `#d95926` at (280, 226, 160×46), sub "delays and swaps"
  - "LOGS" violet `#4a3aa7` at (65, 128, 150×46), sub "every change recorded"
- **Arrows (1.5px `#6b7280`, clockwise, quadratic curves with filled arrowheads at the ends):** forecast right edge (435,67) → plan top (580,128) via control (540,78); plan bottom (580,174) → actual-day right edge (440,249) via control (555,232); actual-day left edge (280,249) → logs bottom (140,174) via control (165,232); logs top (140,128) → forecast left edge (285,67) via control (180,78).
- **Center annotation (bold 13px magenta `#d55181`, two centered lines at (360,148) and (360,166)):** "tonight's log becomes" / "next month's forecast input".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
