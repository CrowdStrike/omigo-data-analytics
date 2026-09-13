# Event-Driven Programming

**Page type:** detail page (tutorial card-sections: h2 per section, two-column table.layout with text left 50% / canvas right 50%)
**HTML title tag:** Event-Driven Programming

**Subtitle:** Something happens, an announcement goes out, and whoever cares reacts — nobody keeps asking "did it happen yet?"

## One Order Placed, Three Things React

Tags: `core idea` (blue pill), `running example` (green pill)

- **The moment** — a customer pays for order #4127: two hoodies, $78 total
- **The announcement** — checkout publishes one event: "order_placed, id 4127"
- **Listener 1** — the email service hears it and sends the receipt
- **Listener 2** — the stock service hears it and drops hoodie count 42 → 40
- **Listener 3** — analytics hears it and appends one row to the events table
- **The key** — checkout never calls any of them; it announces and moves on

*Example (italic):* Like a school bell: the bell doesn't fetch each student — it rings once, and everyone who cares reacts on their own.

**Key point:** An event is a broadcast fact ("an order was placed"), and any number of listeners can react to it — the announcer doesn't know or care who is listening.

### Visualization (canvas `c1`, 720×300)

Fan-out flow diagram: one event, three listeners.

- **Title (bold 15px ink `#1a5276`, top center):** "Order #4127: One Event Fans Out to Three Listeners"
- **Checkout box** at (30,120), 120×52: fill `rgba(42,120,214,0.12)`, stroke blue `#2a78d6`; labels "Checkout" (bold) / "saves order".
- **Event node** at (235,112), 180×66: fill `rgba(201,133,0,0.12)`, stroke yellow `#c98500`; labels "EVENT" (bold) / "\"order_placed #4127\"".
- **Blue arrow** from checkout to event, gray 12px label "publish" above at (191,136).
- **Three listener boxes** at x=508, 140×52 each, fill `rgba(26,82,118,0.06)`, colored arrows from event node (415,145):

| y | stroke color | line 1 | line 2 | timing label (bold, right of box) |
|---|-------------|--------|--------|------|
| 58 | green `#008300` | Email service | sends receipt | +12 ms |
| 122 | violet `#4a3aa7` | Stock service | hoodies 42 → 40 | +15 ms |
| 186 | orange `#d95926` | Analytics | appends 1 row | +19 ms |

- **Bottom annotation (bold magenta `#d55181` 13px, centered, y=285):** "checkout never calls the three services — it cannot even see them"

## Following Order #4127, Millisecond by Millisecond

Tags: `worked example` (green pill), `timeline` (blue pill)

- **14:02:05.000** — customer clicks Pay; order #4127 is saved
- **14:02:05.003** — checkout publishes "order_placed" and is done
- **14:02:05.012** — email listener wakes up, queues the receipt
- **14:02:05.015** — stock listener wakes up, hoodies 42 → 40
- **14:02:05.019** — analytics listener writes its row
- **Total** — checkout finished at +3 ms; all reactions done by +19 ms

*Example (italic):* Checkout spent 3 milliseconds; it never waited for email, stock, or analytics to finish.

**Key point:** The announcer's work ends when the event is published — the reactions run on their own time, after it has already moved on.

### Visualization (canvas `c2`, 720×300)

Timeline chart: millisecond timeline of the fan-out.

- **Title (bold 15px ink, top center):** "The Fan-Out on a Clock: 14:02:05 + Milliseconds"
- **Axis:** horizontal line at y=200 from padL=70 to w−40; scale 0–22 ms; ticks every 5 ms labeled "+0 ms" … "+20 ms" in gray 12px; axis caption below center: "time after 14:02:05.000".
- **Event markers** (vertical colored stem from axis, 5px-radius dot at top, bold 12px label above, gray "+ms" value below label; stems alternate heights 44/96 px):

| ms | color | label | stem height |
|----|-------|-------|------|
| 0 | blue `#2a78d6` | order saved | 44 |
| 3 | yellow `#c98500` | event published | 96 |
| 12 | green `#008300` | email queued | 44 |
| 15 | violet `#4a3aa7` | stock 42 → 40 | 96 |
| 19 | orange `#d95926` | analytics row | 44 |

- **Checkout-done marker:** blue dashed vertical line (dash 4/3) at +3 ms extending below the axis to y=262, with bold blue 13px label at its right (y=272): "checkout is already done here — reactions run without it"

## Adding a Fourth Listener Touches No Existing Code

Tags: `why it matters` (blue pill), `where it's used` (green pill)

- **New need** — the fraud team wants to score every new order
- **Old way** — edit checkout to also call fraud; retest checkout; risk breaking payment
- **Event way** — write one new listener for "order_placed"; checkout changes 0 lines
- **Data science runs on this** — clickstreams, Kafka topics, database change feeds are all events
- **Your pipelines** — "new file landed → run the job" is a listener on a storage event

*Example (italic):* The events table a data scientist queries is usually just listener #3's output, one row per event.

**Key point:** New reactions plug in without touching the code that announces — that is why event logs grow new consumers cheaply, and why your raw data is often shaped as events.

### Visualization (canvas `c3`, 720×300)

Before/after twin panels: fourth listener added, checkout unchanged.

- **Title (bold 15px ink, top center):** "Fraud Check Added: Zero Changes to Checkout"
- **Vertical dashed divider** at x=360 (gray `#bdc3c7`, dash 4/3).
- **Each panel** (drawn by a shared `panel()` helper at offset ox=15 left, ox=378 right): Checkout box 92×44 at (ox+10,130) fill `rgba(42,120,214,0.12)` stroke blue; small event box 74×30 at (ox+130,138) fill `rgba(201,133,0,0.12)` stroke yellow labeled "event"; blue arrow between; listener boxes 96×30 at x=ox+240, stacked from y=66 step 46, fill `rgba(26,82,118,0.05)`, colored arrows fanning from the event box.
  - **Left panel title (bold 13px ink):** "Before: 3 listeners" — listeners: Email (green), Stock (violet), Analytics (orange).
  - **Right panel title (bold 13px magenta):** "After: 4 listeners" — listeners: Email (green), Stock (violet), Analytics (orange), Fraud check (magenta `#d55181`).
- **Annotations:** bold green 13px at (439,205): "checkout: 0 lines changed"; bold magenta 12px at (540,285): "new listener: 1 new file, plugged into the same event"; gray 12px at (185,285): "everything old keeps working untouched"

## The Common Confusion: Fired Is Not Finished

Tags: `common mistake` (red pill), `trade-off` (orange pill)

- **No promised order** — listeners run independently; email may finish before or after stock
- **Run 1** — email done at +12 ms, stock at +15 ms, analytics at +19 ms
- **Run 2** — stock done at +9 ms, analytics at +14 ms, email retries until +310 ms
- **Event vs command** — an event states a fact; it never guarantees what happens next
- **For your data** — event timestamps record when things fired, not when effects landed

*Example (italic):* Joining "order_placed" to "stock_updated" by assuming a fixed delay breaks the day one listener is slow.

**Key point:** "The event fired" only means the announcement went out — each reaction finishes on its own schedule, and sometimes late.

### Visualization (canvas `c4`, 720×300)

Horizontal bar (Gantt-style) chart: two runs, listener completion order differs.

- **Title (bold 15px ink, top center):** "Same Event, Two Runs: Finish Order Is Not Promised"
- **Scale:** x from 0 to 330 ms, padL=150, padR=46; axis line at y=248 with gray ticks at +0, +100, +200, +300 ms.
- **Six bars** (16px tall, from 0 to end value; right-aligned 12px dark row label at left; bold colored "+N ms" label at bar end):

| row label | y | end (ms) | color |
|-----------|---|----------|-------|
| Run 1  email | 62 | 12 | green `#008300` |
| Run 1  stock | 90 | 15 | violet `#4a3aa7` |
| Run 1  analytics | 118 | 19 | orange `#d95926` |
| Run 2  stock | 162 | 9 | violet `#4a3aa7` |
| Run 2  analytics | 190 | 14 | orange `#d95926` |
| Run 2  email | 218 | 310 | green `#008300` |

- Bars have a minimum width of 4px so tiny values stay visible.
- **Dashed gray divider** between the two runs at y=145.
- **Bottom annotation (bold magenta 13px, centered, y=282):** "run 2: email retried — landed 300 ms after the event fired"

## Regeneration instructions

- **Template/layout:** tutorials detail page. `<h1>` + `.subtitle`, then four `.card-section` blocks each with `<h2>` (1.3rem `#1a5276`, 2px solid `#2980b9` bottom border) and a `table.layout` (width 100%, border-collapse, td padding 12px, vertical-align top; `td.text-col` 50% / `td.viz-col` 50%).
- **Text column structure:** `.tags` pill row first (pills 0.72rem bold, 2px 10px padding, 10px radius: blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`); then a `<ul>` (0.92rem) of one-line bullets each opening with `<b>` in `#1a5276`; one italic `.example` line (0.9rem `#555`); one `.key-point` callout (background `#f8f9fa`, left border 3px solid `#e74c3c`, padding 8px 12px, 0.9rem).
- **Page CSS:** body system-ui sans, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem. Canvases have `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Chart palette (JS object P):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- **Canvas:** all canvases 720×300 intrinsic; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper; shared `box()` (with built-in centered 1-2 line labels) and `arrow()` drawing helpers. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- In regenerated HTML, any card/page links use `.html` extensions.
