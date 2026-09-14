# Event-Driven vs Request-Driven

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Event-Driven vs Request-Driven

**Subtitle:** Two ways for services to talk: call someone and wait for the answer, or announce what happened and let whoever cares react later

## One Order, Two Ways to Wire It

**Tags:** `core idea` (blue), `integration styles` (green)

- **The order** — a bookstore checkout at noon must trigger a confirmation email, an inventory update, and analytics
- **Request-driven** — the order service calls email, then inventory, then analytics, and waits for each reply
- **Event-driven** — the order service publishes one "order-placed" event and answers the shopper right away
- **The reactors** — email, inventory, and analytics each subscribe to the event and process it on their own clock
- **The trade** — calling gives an immediate answer and tight coupling; publishing gives independence but no reply

*Example (italic):* The same order flows both ways: three direct calls the shopper waits through, or one publish that three subscribers pick up within a second.

**Key point:** Request-driven means "call and wait for the answer"; event-driven means "announce what happened and let interested services react when they can."

### Visualization (canvas `c1`, 720×300)

Two-row flow diagram: the request-driven call chain on top, the event-driven fan-out below, both starting from the same order box.

- **Title (bold 15px, `#1a5276`, top center):** "The Same Order: Call-and-Wait vs Publish-and-React".
- **Row 1 (boxes centered on y=100), label 12px `#444` at x=20:** "request-driven"; blue `#2a78d6` rounded box at x=110 labeled "Order service" (12px), then three 3px blue arrows in sequence to boxes at x=290 "Email", x=430 "Inventory", x=570 "Analytics", each arrow carrying an 11px `#6b7280` label "call + wait"; a thin dashed return arrow under each box back toward the order box.
- **Row 2 (boxes centered on y=215), label:** "event-driven"; blue box at x=110 "Order service", one 3px green `#008300` arrow labeled "publish" (11px) to a violet `#4a3aa7` box at x=290 "order-placed event"; from it, three 2px green arrows fanning out to boxes at x=490, y = 175 / 215 / 255 labeled "Email", "Inventory", "Analytics", each arrow labeled "react later" (11px `#6b7280`).
- **Box style:** 110–150px wide, 34px tall, 8px radius, fills `rgba(42,120,214,0.15)` for services, `rgba(74,58,167,0.12)` for the event box, 12px `#2c3e50` text.
- **Annotation (bold 12px green `#008300`, right edge near y=140):** "publisher never waits — and never learns who listened".
- **Caption (12px `#444`, bottom right):** "schematic — one order, two wirings".

## Timing the Checkout by Hand

**Tags:** `worked example` (blue), `latency` (green)

- **Own work** — the order service itself needs 50ms to validate the cart and write the order row
- **Request path** — then it waits: email 120ms + inventory 80ms + analytics 200ms = 400ms of calls
- **Hand-check** — 50 + 120 + 80 + 200 = 450ms before the shopper sees "order confirmed"
- **Event path** — 50ms of own work + 10ms to publish = the shopper sees the confirmation at 60ms
- **Later, elsewhere** — subscribers finish at 140ms (inventory), 180ms (email), and 260ms (analytics)

*Example (italic):* Same order, same three downstream jobs: the shopper waits 450ms in the request version and 60ms in the event version — 7.5× faster, because the waiting moved off the checkout path.

**Key point:** Events don't make the work faster — email still takes 120ms — they move the work off the caller's clock, so the shopper's wait shrinks from 450ms to 60ms.

### Visualization (canvas `c2`, 720×300)

Two-lane horizontal timeline (Gantt style) of one checkout: the request-driven lane as one solid chain, the event-driven lane as a short chain plus detached async bars.

- **Title (bold 15px, `#1a5276`, top center):** "One Checkout on the Clock: 450ms vs 60ms to Confirmation".
- **Axis:** x = milliseconds 0 to 900, origin x=90, plot width 590 (≈0.656 px/ms), baseline y=255 with 12px `#444` tick labels at 0 / 300 / 600 / 900ms; gridlines `#e5e9ef` at each tick.
- **Lane 1 (bars 20px tall, centered y=95), label 12px `#444` at x=15:** "request"; contiguous segments: order work 0–50ms blue `#2a78d6`, email 50–170ms magenta `#d55181`, inventory 170–250ms aqua `#199e70`, analytics 250–450ms orange `#d95926`; each with an 11px label above; bold 13px red `#e74c3c` marker + label at 450ms: "confirmed at 450ms".
- **Lane 2 (centered y=175), label:** "event"; segments: order work 0–50ms blue, publish 50–60ms violet `#4a3aa7`; bold 13px green `#008300` marker + label at 60ms: "confirmed at 60ms".
- **Async bars (14px tall, hatched look via 60% alpha fills, y = 210 / 228 / 246):** inventory 60–140ms aqua, email 60–180ms magenta, analytics 60–260ms orange, each with an 11px `#6b7280` right-end label ("done at 140ms", "180ms", "260ms").
- **Annotation (bold 13px green `#008300`, near x=300, y=150):** "same work, off the shopper's clock".
- **Caption (12px `#444`, bottom right):** "durations illustrative".

## When the Flash Sale Hits

**Tags:** `where it's used` (blue), `buffering` (green), `failure` (orange)

- **The spike** — a flash sale pushes checkouts from 50/sec to 500/sec for two minutes
- **The slow link** — the analytics service tops out at 200 events/sec no matter what arrives
- **Request cascade** — with direct calls, analytics timeouts fail whole checkouts: 300/sec are lost at peak
- **Event buffer** — with a queue, all 500/sec are accepted; the backlog peaks at 36,000 events, then drains
- **The cost** — the analytics dashboard runs up to ~3 minutes behind reality until the queue empties

*Example (italic):* Over the 2-minute spike, request-driven loses 300/sec × 120s = 36,000 checkouts outright; event-driven completes every one and drains its 36,000-event backlog in 4 minutes at 150/sec net.

**Key point:** A queue turns a traffic spike into a backlog instead of a failure — the price is eventual consistency: downstream views lag until the backlog clears.

### Visualization (canvas `c3`, 720×300)

Rate-over-time line chart of the spike: incoming checkout rate vs the analytics capacity ceiling, with the gap shaded as the buffered backlog.

- **Title (bold 15px, `#1a5276`, top center):** "Flash Sale: the Queue Absorbs What Direct Calls Would Drop".
- **Axes:** origin x=70, baseline y=245, plot width 590, plot height 180; x = minutes 0 to 8, 12px `#444` tick labels every 2 min; y = events/sec 0 to 500, gridlines `#e5e9ef` at 100/200/300/400.
- **Incoming line:** blue `#2a78d6` 3px step line through minutes `[0, 1, 1, 3, 3, 8]`, rates `[50, 50, 500, 500, 50, 50]` — vertical jumps at minutes 1 and 3.
- **Capacity line:** green `#008300` 2px dashed (dash 6/4) horizontal line at 200 events/sec, 12px green label "analytics capacity 200/sec" above its left end.
- **Buffered area:** orange fill `rgba(217,89,38,0.25)` between the incoming line and the capacity line from minute 1 to 3; bold 12px orange `#d95926` label inside: "queued: 36,000 events".
- **Drain marker:** aqua `#199e70` 2px dashed vertical line at minute 7, 12px aqua label "backlog empty" at its top (drain 36,000 at 150/sec net = 4 min after the spike ends).
- **Annotation (bold 13px red `#e74c3c`, near x=minute 2, y=70):** "request-driven: this shaded area is 36,000 failed checkouts".
- **Caption (12px `#444`, bottom right):** "rates illustrative; area = 300/sec × 120s".

## Not a Religion: Most Systems Use Both

**Tags:** `common mistake` (red), `hybrid` (orange)

- **The mistake** — converting everything to events, including the calls that need an answer right now
- **Needs an answer** — payment authorization stays request-driven: the shopper is waiting to know if the card worked
- **Can react later** — email, analytics, and loyalty points fit events: nobody is standing there waiting on them
- **Fire-and-forget trap** — a published event is not a processed event; a consumer can be down or hours behind
- **Tracing tax** — one request is one call stack; one event fans out into logs across many separate services

*Example (italic):* The bookstore charges the card with a direct call (answer in 300ms, checkout blocks on it), then publishes "order-placed" for everything downstream — one checkout, both styles.

**Common mistake:** Treating event-driven as strictly better. If the caller cannot continue without the result — a payment, a stock reservation before promising delivery — a request is the honest design; events fit the reactions that follow.

### Visualization (canvas `c4`, 720×300)

Decision flow diagram: one question splits the bookstore's checkout steps into the request lane and the event lane.

- **Title (bold 15px, `#1a5276`, top center):** "One Question Decides: Is the Caller Waiting on the Result?".
- **Root (centered x=360, y=80):** ink `#1a5276` rounded box, 240px wide, labeled "is the shopper waiting on the result?" (bold 12px white text, fill `rgba(26,82,118,0.9)`).
- **Yes branch (left):** 3px arrow labeled "yes" (bold 12px `#2a78d6`) to a blue `#2a78d6` box at x=150, y=170 "request: charge the card", with an 11px `#6b7280` sub-label beneath: "blocks 300ms — answer required".
- **No branch (right):** 3px arrow labeled "no" (bold 12px `#008300`) to a violet `#4a3aa7` box at x=430, y=170 "publish: order-placed", then three 2px green `#008300` arrows fanning to boxes at x=610, y = 145 / 185 / 225 labeled "Email", "Analytics", "Loyalty".
- **Box style:** 130–240px wide, 36px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(74,58,167,0.12)` / `rgba(0,131,0,0.10)`, 12px `#2c3e50` text.
- **Warning note (bold 12px red `#e74c3c`, bottom left near y=270):** "fire-and-forget ≠ done: consumers can be down or behind".
- **Annotation (bold 13px orange `#d95926`, bottom center y=290):** "real systems mix both — request for answers, events for reactions".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded values above (no randomness); service latencies (50/120/80/200/10ms), spike rates (50→500/sec, capacity 200/sec), and the 36,000-event backlog are invented and labeled illustrative; derived figures are exact arithmetic on them (450 vs 60ms, 7.5×, 36,000 = 300/sec × 120s, 4-minute drain at 150/sec net).
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
