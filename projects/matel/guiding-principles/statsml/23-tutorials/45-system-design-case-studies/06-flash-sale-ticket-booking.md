# Flash-Sale / Ticket Booking

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Flash-Sale / Ticket Booking

**Subtitle:** When 100,000 buyers chase 10 seats at the same instant, the problem isn't throughput — it's contention on a tiny inventory under one synchronized spike

## Ten Seats, One Hundred Thousand Buyers

**Tags:** `core idea` (blue), `contention` (red), `flash sale` (orange)

- **The sale** — a concert has 10 remaining seats; they go on sale at exactly 12:00 noon
- **The spike** — 100,000 fans refresh at noon; traffic jumps from ~80 to 25,000 requests/sec
- **The ratio** — 10,000 buyers per seat: 99.99% of requests must fail fast, and fail fairly
- **Not a scale problem** — the same 100,000 buyers spread over a day would be trivial load
- **The defining property** — massive synchronized demand colliding on a tiny shared inventory

*Example (italic):* Three seconds after noon the site has already taken tens of thousands of requests for seats that only 10 people can ever get.

**Key point:** A flash sale is defined by contention, not volume — everyone arrives in the same second and fights over the same handful of rows, so the design centers on the 10 seats, not the 100,000 buyers.

### Visualization (canvas `c1`, 720×300)

Line chart of request rate in the minutes around noon, showing the synchronized spike against an inventory of 10 that is invisible at this scale.

- **Title (bold 15px, `#1a5276`, top center):** "Noon Spike: 25,000 req/s Chasing 10 Seats".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = time 11:58 to 12:04 with 12px `#444` tick labels at each minute ("11:58"…"12:04"); y = requests/sec 0 to 25,000, gridlines `#e5e9ef` at 5,000 / 10,000 / 15,000 / 20,000 with 12px `#444` labels.
- **Traffic line:** blue `#2a78d6` 3px line through minutes-from-noon `[-2, -1, -0.1, 0, 0.25, 0.5, 1, 2, 3, 4]`, req/sec `[30, 80, 120, 25000, 14000, 8000, 3000, 900, 300, 150]` — near-flat, vertical wall at 12:00, decaying tail.
- **Sale marker:** vertical dashed `#6b7280` (dash 4/3) line at minute 0, 12px `#6b7280` label "on sale" at its top.
- **Inventory marker:** short red `#e74c3c` 3px tick on the baseline at x of minute 0, bold 13px red label "inventory: 10 seats — invisible at this scale" beside it.
- **Annotation (bold 13px `#1a5276`, near minute 2, y=90):** "10,000 buyers per seat".
- **Caption (12px `#444`, bottom right):** "request rates illustrative".

## Two Buyers, One Last Seat: the Oversell Race

**Tags:** `worked example` (blue), `race condition` (red), `atomicity` (green)

- **Naive flow** — read the count, check it is ≥ 1, then write count − 1: three separate steps
- **The race** — buyer A reads "1 left"; buyer B reads "1 left" before A writes; both pass the check
- **The oversell** — both write 0 and both get a confirmation: 11 tickets sold for 10 seats
- **Fix 1: atomic decrement** — one indivisible op: `UPDATE seats SET left = left - 1 WHERE left >= 1`
- **Fix 2: reservation + TTL** — hold a specific seat for 10 minutes through checkout; expiry releases it
- **Hand-check** — 10 holds placed at 12:00; 3 buyers abandon, so at 12:10 those 3 seats reopen

*Example (italic):* A and B both read "1 left" at 12:00:04; with the atomic decrement one UPDATE reports 1 row changed and the other reports 0 — B sees "sold out", and seat 11 never exists.

**Key point:** Correctness comes from making check-and-take one indivisible step (atomic decrement on a single row or a distributed counter) or making the take reversible (TTL reservation) — never from read-then-write.

### Visualization (canvas `c2`, 720×300)

Two-row flow diagram: the read-check-write race overselling the last seat vs the atomic decrement letting exactly one buyer through.

- **Title (bold 15px, `#1a5276`, top center):** "Read-Check-Write Oversells; Atomic Decrement Can't".
- **Row 1 (y=95), label 12px `#444` at x=20:** "read-check-write"; two blue `#2a78d6` rounded boxes side by side at x=160 and x=330 labeled "A reads: 1 left" and "B reads: 1 left" (12px), 3px arrows converging on a red `#e74c3c` box at x=520 labeled "both write 0" with bold 12px red "✗ 11 sold / 10 seats" centered beneath the box.
- **Row 2 (y=215), label:** "atomic decrement"; blue boxes "A: left−1 → 1 row" (x=160) and "B: left−1 → 0 rows" (x=330), arrow from A to a green `#008300` box at x=520 labeled "A gets the seat ✓", B's arrow ends at 12px `#6b7280` text "B: sold out — no oversell".
- **Box style:** 150–170px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(231,76,60,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=280):** "the check and the take must be one indivisible step".

## The Waiting Room: Shaping the Spike Into a Trickle

**Tags:** `where it's used` (blue), `demand shaping` (green), `queues` (orange)

- **The queue** — a waiting room in front of checkout admits a controlled 100 users/sec, no more
- **What the core sees** — a flat 100/s instead of a 25,000/s wall; the counter absorbs it easily
- **Browse is cheap** — seat maps and prices are reads, served from pre-scaled read replicas
- **The asymmetry** — reads scale horizontally by adding replicas; the decrement of seat 7 is one row
- **Fast ending** — at 100 admits/sec the 10 seats are gone within the first second of admissions

*Example (italic):* 100,000 people are queued at 12:00; the waiting room would take ~17 minutes to drain at 100/s, but the sale effectively ends in the first second of admits — everyone else gets a clean "sold out" page.

**Key point:** Shape demand before it reaches the serialized core — the waiting room converts a synchronized spike into a steady trickle, and the read replicas keep the harmless browse traffic away from the one row that matters.

### Visualization (canvas `c3`, 720×300)

Two-line chart on a shared time axis: raw arrival rate (spike) vs the rate admitted into checkout (flat trickle), heights schematic.

- **Title (bold 15px, `#1a5276`, top center):** "Waiting Room: 25,000/s Arrive, 100/s Get In".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = time 11:59 to 12:04 with 12px `#444` tick labels at each minute; y unlabeled rate axis — log-feel achieved by hardcoded pixel heights, not a real linear axis; gridlines `#e5e9ef` at y=110 and y=180.
- **Arrivals line:** blue `#2a78d6` 3px line through minutes-from-noon `[-1, 0, 0.5, 1, 2, 3, 4]` at pixel heights above baseline `[8, 170, 120, 85, 45, 25, 15]`, 12px blue label "arrivals (peak 25,000/s)" near its peak.
- **Admitted line:** green `#008300` 3px line, flat at pixel height 26 from minute 0 to 4 (zero before noon), 12px green label "admitted to checkout: 100/s" above its right end.
- **Sale marker:** vertical dashed `#6b7280` (dash 4/3) line at minute 0, 12px `#6b7280` label "on sale".
- **Annotation (bold 13px green `#008300`, near minute 2, y=100):** "the core never sees the spike".
- **Caption (12px `#444`, bottom right):** "heights schematic, rates illustrative".

## More Servers Don't Buy More Seats

**Tags:** `common mistake` (red), `serialization` (orange), `bots` (red)

- **The confusion** — treating contention as a throughput problem: "the site fell over, add servers"
- **What scaling buys** — 8 app servers serve 8× the browse pages: 40,000/s instead of 5,000/s
- **What it doesn't** — every purchase of seat 7 serializes on that one row: still ~1,000 decrements/s
- **Bots** — scripts fire hundreds of holds in the first second, faster than any human can click
- **Per-user limits** — cap holds at 2 per account, tie queue admission to a token so scripts can't skip

*Example (italic):* A bot farm with 50 accounts tries to place 50 ten-minute holds at 12:00:01; a 2-per-account cap plus waiting-room tokens keeps the 10 seats from being cornered and relisted (illustrative).

**Common mistake:** Assuming horizontal scaling fixes the flash sale. It multiplies browse capacity, but the seat counter is one row whose atomic decrement is fundamentally serial — protect it with a waiting room and per-user limits instead of a bigger fleet.

### Visualization (canvas `c4`, 720×300)

Grouped bar chart: capacity vs number of app servers — browse throughput grows linearly while the single-row decrement rate stays flat.

- **Title (bold 15px, `#1a5276`, top center):** "8× the Servers, 8× the Browsing, Same One Row".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = server groups at centers x = 145, 290, 435, 580 with 12px `#444` labels "1 server", "2 servers", "4 servers", "8 servers"; y = requests/sec 0 to 40,000, gridlines `#e5e9ef` at 10,000 / 20,000 / 30,000 with 12px `#444` labels.
- **Browse bars:** blue fill `rgba(42,120,214,0.30)` with 2px `#2a78d6` border, 44px wide, left of each center; heights for `[5000, 10000, 20000, 40000]` pages/sec, 11px `#444` value labels on top ("5k", "10k", "20k", "40k").
- **Decrement bars:** solid orange `#d95926`, 44px wide, right of each center; flat at `1000` decrements/sec each (about 5px tall), 11px `#d95926` label "1k" on top of each.
- **Legend (12px, top left inside plot):** blue swatch "browse pages/s", orange swatch "seat-row decrements/s".
- **Annotation (bold 13px red `#e74c3c`, near x=400, y=80):** "the decrement is serialized per seat row — servers don't help".
- **Caption (12px `#444`, bottom right):** "throughput numbers illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); request rates, admit rates, hold counts, and throughput numbers are invented and labeled illustrative; the concert, seat count (10), buyer count (100,000), and 10-minute TTL are the page's single running example and must stay consistent between text and charts.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
