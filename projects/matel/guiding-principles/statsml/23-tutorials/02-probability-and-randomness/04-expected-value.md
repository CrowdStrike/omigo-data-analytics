# Expected Value

**Page type:** detail page (tutorial page: 4 `.card-section` blocks, each h2 + two-column `table.layout` — text left 50%, canvas right 50%)
**HTML title tag:** Expected Value

**Subtitle:** What a $2 raffle ticket is worth on average — the long-run price of a gamble, not what happens to you once

## A $2 Ticket, a 1-in-1,000 Shot at $1,000

**Tags:** `core idea` (blue), `running example` (orange)

- **The raffle** — a ticket costs $2; 1 ticket in 1,000 wins $1,000, the rest win nothing
- **Average it out** — spread the $1,000 prize evenly over all 1,000 tickets: $1 per ticket
- **The math** — (1/1000) × $1,000 + (999/1000) × $0 = $1 expected winnings
- **The verdict** — a $2 ticket returns $1 on average: each ticket "costs" you $1
- **Definition (after the example)** — expected value is each outcome times its probability, added up

*Example:* If the raffle sold its 1,000 tickets, it collected $2,000 and paid out $1,000 — the buyers, together, lost $1 per ticket.

**Key point:** Expected value is the fair long-run price of a random deal — compare it to what you pay.

### Visualization (canvas `c1`, 720×300)

Diagram: a strip of 1,000 tickets with one winner, then the prize spread evenly as an EV bar.

- **Title (bold 15px, top center, ink `#1a5276`):** "1,000 Tickets, One $1,000 Winner".
- **Ticket strip:** at x=60, y=60, 600×42px, drawn as 50 blocks (each block represents 20 tickets), block width 12px with 1.5px gaps; all blocks light gray `#e5e9ef` except block index 31 in yellow `#c98500`; strip outlined in mute gray `#6b7280`, 1px.
- **Labels:** above the gold block, bold 12px yellow `#c98500`: "the 1 winning ticket ($1,000) is inside this block". Below-left of strip, 12px mute gray: "each block = 20 tickets — 999 tickets win $0".
- **EV bar:** at y=168, headed by bold 13px text `#2c3e50` centered: "spread the $1,000 prize evenly over all 1,000 tickets:"; same 50-block strip, 20px tall, all blocks filled `rgba(201,133,0,0.35)`. Below it, bold 12px yellow: "$1 per ticket — that is the expected value".
- **Formula (bold 14px violet `#4a3aa7`, centered at y=246):** "EV = (1/1000) × $1,000  +  (999/1000) × $0  =  $1".
- **Bottom line (bold 13px red `#e74c3c`, centered):** "The ticket costs $2 but is worth $1 on average — you pay $1 to play".

## Buying 1,000 Tickets: Watch the Average Emerge

**Tags:** `worked example` (green), `by hand` (blue)

- **The plan** — buy one ticket in each of 1,000 raffles and track your money
- **Most weeks** — you pay $2, win $0: the balance drifts down $2 at a time
- **The win** — one raffle (here, number 620) pays $1,000: a single big jump up
- **The total** — spent $2,000, won $1,000: net −$1,000 over 1,000 tickets
- **Per ticket** — −$1,000 / 1,000 = −$1, exactly the EV verdict

*Example:* Even with a jackpot in the middle, the ledger lands at −$1,000 — the average loss the EV promised.

**Key point:** Rare big wins and steady small losses net out to the expected value — but only over many repeats.

### Visualization (canvas `c2`, 720×300)

Line chart: cumulative net balance over 1,000 tickets, with a single $1,000 win at ticket #620.

- **Title (bold 15px ink, centered):** "Your Running Balance Over 1,000 Tickets (win at #620)".
- **Axes:** padding top 50, bottom 55, left 75, right 40. Y range −$2,200 to +$400; y gridlines and labels at $0, −$500, −$1,000, −$1,500, −$2,000 (12px mute gray labels right-aligned; the $0 line in `#999`, other gridlines `#e5e9ef`). Y axis line `#999`. X spans tickets 0–1,000.
- **EV reference line:** dashed violet `#4a3aa7` (dash 6/4, width 2) from (0, $0) to (1000, −$1,000); labeled in bold 12px violet at ~x=80: "EV line: −$1 per ticket".
- **Actual path (blue `#2a78d6`, width 2.5):** straight decline from (0, $0) to (620, −$1,240); vertical jump up to (620, −$240); straight decline to (1000, −$1,000).
- **Annotations:** bold 12px yellow `#c98500` at the jump: "+$1,000 win"; bold 12px blue near x=300 at y≈−$350: "paying $2 per ticket"; bold 13px red `#e74c3c` right-aligned near the endpoint: "ends at −$1,000 = 1,000 × EV".
- **X-axis caption (12px mute, bottom center):** "tickets bought (one illustrative win, placed at #620)".

## Why a Data Scientist Computes EV Daily

**Tags:** `where it's used` (blue), `decisions` (green)

- **Ranking options** — EV turns "maybe $40, maybe nothing" into one comparable number
- **Promo A** — 5% of emailed customers buy, at $40 margin each: EV = $2.00 per email
- **Promo B** — 8% buy, but the discount cuts margin to $20: EV = $1.60 per email
- **The call** — A wins despite converting fewer people; probability times value decides
- **Everywhere** — ad bids, insurance pricing, fraud review queues all rank by EV

*Example:* The higher conversion rate lost: 8% × $20 is still less money per email than 5% × $40.

**Key point:** When outcomes are uncertain, multiply each by its probability first — then compare the averages, not the best cases.

### Visualization (canvas `c3`, 720×300)

Two-bar comparison of expected margin per email for two promos.

- **Title (bold 15px ink, centered):** "Which Promo Email Earns More per Send?".
- **Axes:** padding top 55, bottom 78, left 75, right 40; axis lines `#999`. Y from $0.00 to $2.50 with labels every $0.50 (12px mute, format "$X.XX"), gridlines `#e5e9ef`.
- **Bars:** width 190px, evenly spaced.
  - Bar 1: value $2.00, green `#008300`, value label above bold 15px "EV = $2.00", caption below bold 12px `#2c3e50`: "Promo A: 5% buy × $40 margin".
  - Bar 2: value $1.60, orange `#d95926`, value label "EV = $1.60", caption: "Promo B: 8% buy × $20 margin".
- **X-axis caption (12px mute):** "expected margin per email sent (illustrative rates)".
- **Bottom line (bold 13px violet `#4a3aa7`, centered):** "The lower conversion rate wins — EV multiplies probability by value before comparing".

## The Common Confusion: EV Is Not What Happens Once

**Tags:** `common mistake` (red)

- **Two outcomes only** — a single ticket nets either −$2 (almost always) or +$998 (rarely)
- **Never the average** — no ticket ever nets exactly the EV of −$1
- **Where EV lives** — it is the balancing point of the outcomes, not one of them
- **One-shot decisions** — for a single play, the spread of outcomes matters, not just the mean
- **The trap** — "EV is positive, so I can't lose" — you can, and usually will, on any one try

*Example:* 999 buyers walk away $2 poorer; one walks away $998 richer — nobody experiences the −$1 "average".

**Key point:** EV describes the long run of many repeats — a single outcome can sit far from it, and usually does.

### Visualization (canvas `c4`, 720×300)

Two-bar outcome histogram with an EV marker that touches neither bar.

- **Title (bold 15px ink, centered):** "What One Ticket Actually Does to Your Wallet".
- **Axes:** padding top 55, bottom 70, left 75, right 40; axis lines `#999`. Y from 0% to 100%, labels every 25% (12px mute), gridlines `#e5e9ef`. X maps net outcome −$50 to +$1,050 linearly across the plot width.
- **Bars (44px wide, centered on their x value):**
  - At net −$2: height 99.9%, blue `#2a78d6`, value label bold 13px "99.9%" above, x label 12px `#2c3e50` "net −$2" below.
  - At net +$998: 0.1%, yellow `#c98500`, drawn with a visible minimum height of 8px; value label "0.1%", x label "net +$998".
- **EV marker:** vertical dashed red line `#e74c3c` (dash 6/4, width 2.5) near the −$1 position (offset right for legibility), from just below the top to the baseline; bold 13px red label to its right: "EV = −$1: no ticket ever lands here".
- **X-axis caption (12px mute):** "net result of buying one $2 ticket".
- **Bottom line (bold 13px violet, centered):** "The mean is the balance point between two outcomes — not an outcome itself".

## Regeneration instructions

- **Template:** tutorials topic-page layout (simplest-form concept tutorial). `<h1>` concept name (no index number), `.subtitle` line, then 4 `.card-section` blocks. Each section: `<h2>` (1.3rem `#1a5276`, 2px solid `#2980b9` bottom border) + `table.layout` with one `<tr>`: left `td.text-col` (50%) holding `.tags` pill row, a 5-bullet `<ul>` (each `<li>` opens with a `<b>` term in `#1a5276`), one italic `.example` paragraph, one `.key-point` callout; right `td.viz-col` (50%) holding one `<canvas>` 720×300.
- **Page CSS:** body system-ui sans, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem; table cells padded 12px, no borders; canvas `width:100%`, 1px `#e0e0e0` border, 4px radius; `.key-point` background `#f8f9fa`, 3px solid `#e74c3c` left border, 0.9rem; `.example` italic `#555` 0.9rem; `ul` 0.92rem.
- **Tag pills:** `.tag` inline-block 0.72rem bold, padding 2px 10px, radius 10px. Colors — blue: bg `rgba(26,82,118,0.12)` / `#1a5276`; green: bg `rgba(39,174,96,0.15)` / `#27ae60`; red: bg `rgba(231,76,60,0.12)` / `#e74c3c`; orange: bg `rgba(230,126,34,0.15)` / `#e67e22`.
- **Chart palette (JS object `P`):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: #1a5276 primary blue, #27ae60 green, #e74c3c red, #e67e22 orange.
- **Canvas:** all canvases 720×300 logical; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart titles bold 15px, labels 12–13px. Hardcoded literal data arrays, no `Math.random()`. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- No nav bar, no back/home links, no cross-page links. In regenerated HTML, any card links use `.html` extensions.
