# Waiting Lines & Little's Law

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Waiting Lines & Little's Law

**Subtitle:** People in the system = arrival rate × time inside — one line of algebra that sizes checkouts, call centers, and emergency rooms

## Count Any Two, Get the Third

**Tags:** `core idea` (blue), `Little's Law` (green)

- **The rush** — every morning 2 customers walk into the coffee shop per minute, on average
- **Time inside** — ordering, paying, and waiting for the cup takes each customer about 5 minutes
- **The headcount** — so at any moment the shop holds about 2 × 5 = 10 people, on average
- **Little's Law** — people inside = arrival rate × time inside, written L = λ × W
- **No fine print** — it holds for any stable system, no assumptions about order, bursts, or luck

*Example (italic):* Count heads in the shop at random moments during the rush: the tally hovers around 10 — exactly λ × W.

**Key point:** L = λ × W ties three everyday quantities together — measure any two and the third is fixed, for any system that isn't endlessly growing or draining.

### Visualization (canvas `c1`, 720×300)

A shop-box diagram: an arrival arrow entering from the left, a crowd of 10 dots inside the box, an exit arrow leaving right, and the formula annotated beneath.

- **Title (bold 15px, `#1a5276`, top center):** "The Morning Rush: 2 In per Minute, 5 Minutes Inside (illustrative)".
- **Shop box:** rectangle (230, 72, 260×150), fill `#fbfcfd`, 2px ink `#1a5276` border; label "the coffee shop" bold 13px ink centered at (360, 96).
- **Crowd:** 10 circles r=9, fill `rgba(42,120,214,0.55)`, stroke 1.5px blue `#2a78d6`; two rows of five, centers x = 276/318/360/402/444, rows y=132 and y=170; count label "≈ 10 people inside on average" bold 12px blue centered at (360, 204).
- **Arrival arrow:** 2.5px green `#008300` line from (56, 147) to (222, 147) with a filled green arrowhead at the box edge; label "2 walk in / min" bold 13px green centered at (139, 128); sub-label "arrival rate λ" 12px mute `#6b7280` centered at (139, 166).
- **Exit arrow:** 2.5px aqua `#199e70` line from (498, 147) to (664, 147) with a filled aqua arrowhead at the right end; label "2 walk out / min" bold 13px aqua centered at (581, 128); sub-label "(steady state)" 12px mute centered at (581, 166).
- **Time label (bold 12px orange `#d95926`, centered at (360, 246)):** "each customer spends W = 5 minutes inside".
- **Formula (bold 15px magenta `#d55181`, centered at (360, 282)):** "L = λ × W = 2 × 5 = 10 people inside".

## Deriving the Number You Can't Measure

**Tags:** `worked example` (blue), `backwards use` (orange)

- **The hard number** — how long each support ticket sits open is painful to track one by one
- **Two easy counts** — 120 tickets are open right now (L), and 30 new ones arrive per day (λ)
- **Flip the formula** — W = L ÷ λ = 120 ÷ 30, so the average ticket spends 4 days inside
- **Sanity check** — 30 tickets a day, each staying 4 days, really does keep about 120 in flight
- **The trick** — measure the two easy things and the hard one falls out of the algebra

*Example (italic):* The team never timestamped ticket lifetimes, yet a whiteboard count plus one day of arrivals gave the 4-day answer.

**Key point:** Rearranged as W = L ÷ λ, Little's Law turns two counts anyone can take into the dwell time nobody was logging.

### Visualization (canvas `c2`, 720×300)

A three-box card reading left to right as a division: L = 120 tickets, λ = 30/day, W = ? — with the division worked out and a forward check beneath.

- **Title (bold 15px, `#1a5276`, top center):** "Two Easy Counts, One Hard Answer (illustrative)".
- **Boxes:** three rectangles at y=64, width 192, height 78, x = 40 / 264 / 488; fill `#fbfcfd`, 2px colored border; header bold 13px colored centered at box y+26; value bold 22px same color centered at y+60:
  - "L — open tickets" (blue `#2a78d6`) / value "120"
  - "λ — new per day" (green `#008300`) / value "30"
  - "W — days inside" (magenta `#d55181`) / value "?"
- **Operators (bold 22px ink `#1a5276`):** "÷" centered at (248, 112) between the first two boxes; "=" centered at (472, 112) between the last two.
- **Answer (bold 16px magenta `#d55181`, centered at (360, 192)):** "W = 120 ÷ 30 = 4 days per ticket".
- **Caption (12px `#6b7280`, centered at (360, 222)):** "no per-ticket timestamps needed — two counts anyone can take".
- **Forward check (bold 13px green `#008300`, centered at (360, 258)):** "check it forwards: 30 new/day × 4 days inside = 120 open".

## Why Sizing for the Average Guarantees Lines

**Tags:** `common mistake` (red), `utilization` (orange)

- **The temptation** — 2 customers/min arrive and a barista serves 2/min, so one looks like enough
- **Utilization** — the share of time the baristas are busy: average demand over capacity
- **Bursts** — arrivals clump and service times vary, so a 100%-busy shop can never catch up
- **The hockey stick** — 70% busy → 2 min wait, 80% → 4, 90% → 9, 95% → 19, 99% → 99 (illustrative)
- **The trap** — sizing capacity to average demand means near-100% busy: every burst piles up

*Example (italic):* Enough extra capacity to fall from 95% busy to 80% cuts the average wait from 19 minutes to 4 (illustrative).

**Key point:** Queues are priced by the gap between capacity and demand — waits explode as utilization nears 100%, so slack isn't waste, it's the product.

### Visualization (canvas `c3`, 720×300)

The hockey-stick curve of average wait versus utilization, with the five table points dotted and labeled.

- **Title (bold 15px, `#1a5276`, top center):** "Average Wait vs How Busy the Shop Is (illustrative)".
- **Axes:** 1px `#999`; origin (70, 244), x-axis to (690, 244), y-axis up to (70, 52). Y ticks 0 / 25 / 50 / 75 / 100 min, 12px `#6b7280` right-aligned at x=62; value scale 0–105 over 192px (`yOf(v) = 244 − v/105·192`). Y-axis label "avg wait (min)" 12px mute left-aligned at (74, 46).
- **X mapping:** utilization 65–100% mapped to x = 70 + (u−65)/35·620; tick labels "70%" / "80%" / "90%" / "95%" / "99%" 12px `#444` centered under their x positions at y=262; axis label "utilization — share of time baristas are busy" 12px mute centered at (380, 286).
- **Curve:** 2.5px blue `#2a78d6` polyline through hardcoded points `[[70,2],[75,3],[80,4],[85,6],[88,7.3],[90,9],[92,11.5],[93,13],[94,15.7],[95,19],[96,24],[97,32],[98,49],[99,99]]`.
- **Dots:** the five table points (70,2), (80,4), (90,9), (95,19), (99,99) as r=4.5 filled magenta `#d55181` circles with bold 12px magenta labels "2 min" / "4" / "9" / "19" / "99 min" just above each dot (the "99 min" label shifted left so it stays inside the canvas).
- **Annotation (bold 13px magenta `#d55181`, centered at (290, 106)):** "the last 10% of busy-ness costs most of the waiting (illustrative)".

## The Same Law, Different Nouns

**Tags:** `where it's used` (blue), `rule of thumb` (green)

- **Checkout lanes** — carts in line = cart arrivals per minute × minutes each cart waits
- **Call centers** — callers on hold = calls per minute × average minutes on hold
- **ER beds** — patients in the ER = admissions per hour × hours until discharge
- **CPU queues** — requests in flight = requests per second × average response time
- **Code review** — open pull requests = PRs opened per day × days each waits for review

*Example (italic):* A kitchen with 24 open orders and 8 orders arriving per hour is quoting a 3-hour wait, whatever the menu says.

**Key point:** Little's Law is napkin math — count what's inside, count the arrival rate, and the average time inside has nowhere to hide.

### Visualization (canvas `c4`, 720×300)

Six small labeled boxes (three per side) all connected by lines to one central formula box.

- **Title (bold 15px, `#1a5276`, top center):** "Six Queues, One Formula".
- **Center box:** rectangle (272, 118, 176×66), fill `#fbfcfd`, 2.5px ink `#1a5276` border; "L = λ × W" bold 20px ink centered at (360, 148); sub-line "inside = rate × time" 12px mute `#6b7280` centered at (360, 170).
- **Side boxes:** width 200, height 50, fill `#fbfcfd`, 2px colored border; left column at x=36, right column at x=484, rows y = 56 / 126 / 196. Header bold 13px colored left-aligned at box x+14, y+21; sub-line 12px mute left-aligned at x+14, y+39:
  - Left: "checkout lanes" (blue `#2a78d6`) / "carts in line"; "call center" (green `#008300`) / "callers on hold"; "ER" (orange `#d95926`) / "patients in beds"
  - Right: "kitchen" (magenta `#d55181`) / "orders on the rail"; "CPU queue" (violet `#4a3aa7`) / "requests in flight"; "code review" (aqua `#199e70`) / "open pull requests"
- **Connectors:** 1.5px mute `#6b7280` lines from each left box's right edge (x=236, box mid-height) to the center box's left edge (272, 151), and from each right box's left edge (x=484, box mid-height) to the center box's right edge (448, 151).
- **Takeaway (bold 13px green `#008300`, centered at (360, 288)):** "count what's inside and the arrival rate — the average wait has nowhere to hide".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` and calls `ctx.scale(...)` so drawing stays in logical coordinates; chart draw functions are stored in a `__charts` array and redrawn on debounced (150ms) window resize.
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data integrity:** all series are hardcoded literals (no `Math.random()`); the coffee-shop numbers (2/min, 5 min, 10 inside), the ticket numbers (120, 30/day, 4 days), and the utilization table (70→2, 80→4, 90→9, 95→19, 99→99) must match between bullets and charts; invented numbers carry an "(illustrative)" label.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
