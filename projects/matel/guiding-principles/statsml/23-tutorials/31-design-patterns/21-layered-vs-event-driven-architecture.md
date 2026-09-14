# Layered vs Event-Driven Architecture

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Layered vs Event-Driven Architecture

**Subtitle:** A layered app handles an order as one deep call stack that waits at every step; an event-driven app announces "order placed" and lets independent listeners react — waiting versus telling

## One Latte, Two Ways to Wire the Shop

**Tags:** `core idea` (blue), `two styles` (green), `coffee shop` (orange)

- **The shop** — a coffee shop app takes online orders: charge the card, save the order, email a receipt, add loyalty points
- **Layered** — the request travels down UI → order service → payment → database, each layer calling the next
- **The wait** — every call waits for its answer before returning, like a stack of unfinished phone calls
- **Event-driven** — the order service does the essentials, then publishes "OrderPlaced" onto an event bus
- **The listeners** — email, loyalty, and analytics each hear the event and act on their own clock

*Example (italic):* When you order a latte, the layered shop makes you wait through the receipt email; the event shop hands you the confirmation and emails later.

**Key point:** Layered architecture is call-stack thinking — do this, wait, return; event-driven is message thinking — announce what happened and let whoever cares react.

### Visualization (canvas `c1`, 720×300)

Two-panel flow diagram: the same latte order as one vertical call stack (left) vs one published event fanning out to three listeners (right).

- **Title (bold 15px, `#1a5276`, top center):** "The Same Latte Order: One Call Stack vs One Event, Three Listeners".
- **Left panel (layered), x 40–330:** four rounded boxes 170px wide, 32px tall, at x=80 and y = 70, 118, 166, 214, labeled "UI", "Order service", "Payment", "Database" (12px `#2c3e50`, fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border); solid blue `#2a78d6` down-arrows on the left edge between boxes, dashed mute `#6b7280` (dash 4/3) return-arrows on the right edge; bold 12px blue `#2a78d6` label "calls wait for returns" at (x=85, y=58).
- **Right panel (event-driven), x 380–700:** "Order service" box (same style) at x=460, y=70; solid green `#008300` arrow down to a horizontal event-bus bar at y=140, spanning x 395–685, 22px tall, fill `rgba(25,158,112,0.20)`, 2px `#199e70` border, centered 12px `#199e70` label "event bus — OrderPlaced"; three green arrows down to boxes 88px wide, 32px tall at y=200, x = 400, 505, 610, labeled "Email", "Loyalty", "Analytics" (fill `rgba(0,131,0,0.12)`, 2px `#008300` border).
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=278):** "left: do-and-wait • right: announce-and-move-on".
- **Caption (12px `#444`, bottom right):** "flow schematic, illustrative".

## Timing Order #4127 by Hand

**Tags:** `worked example` (blue), `latency math` (green)

- **The order** — order #4127, one $5 latte, six steps with measured times
- **The steps** — validate 20ms, charge card 180ms, save order 40ms, email 250ms, loyalty 60ms, dashboard 50ms
- **Layered total** — the customer waits the full chain: 20+180+40+250+60+50 = 600ms
- **Event split** — essentials first: 20+180+40 = 240ms, then "OrderPlaced" fires and the app replies
- **The saving** — 360ms of listener work moves off the customer's clock: a 60% shorter wait

*Example (italic):* The receipt email still takes 250ms either way — the event version just stops making the customer stand there while it sends.

**Key point:** Event-driven does not make the work faster; it moves work the caller never needed to wait for off the caller's call stack.

### Visualization (canvas `c2`, 720×300)

Horizontal timeline bars for order #4127: the layered wait (all six steps end to end) vs the event-driven wait (three steps, then listeners run off the customer's clock).

- **Title (bold 15px, `#1a5276`, top center):** "Order #4127: 600ms Wait vs 240ms Wait (1px = 1ms)".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = milliseconds 0 to 600, 12px `#444` tick labels every 100ms, vertical gridlines `#e5e9ef`; row labels 12px `#444` at x=8: "layered" at y=96, "event-driven" at y=160, "listeners" at y=212.
- **Layered bar (y=88, 16px tall):** six segments starting at x=60 with pixel widths `[20, 180, 40, 250, 60, 50]`, colors in order: validate blue `#2a78d6`, charge violet `#4a3aa7`, save aqua `#199e70`, email magenta `#d55181`, loyalty yellow `#c98500`, dashboard orange `#d95926`; 11px `#444` ms labels above the wider segments ("180", "250").
- **Event-driven wait bar (y=152, 16px tall):** the same first three segments `[20, 180, 40]`, ending at x=300; vertical dashed `#6b7280` (dash 4/3) line at x=300 from y=70 to y=245 with 12px `#6b7280` label "app replies at 240ms" at its top.
- **Listener lane:** three thin bars 12px tall all starting at x=300: email width 250 magenta `#d55181` at y=196, loyalty width 60 yellow `#c98500` at y=212, dashboard width 50 orange `#d95926` at y=228.
- **Annotation (bold 13px green `#008300`, near x=340, y=130):** "customer waits 240ms, not 600ms — same total work".
- **Caption (12px `#444`, bottom right):** "step times illustrative".

## Adding the Fourth Listener Without Touching the Till

**Tags:** `where it's used` (blue), `loose coupling` (green), `analytics` (orange)

- **The ask** — the analytics team wants every order the moment it happens, to feed a live sales model
- **Layered cost** — each new consumer edits the order path: email 14, loyalty 9, analytics 12, fraud 17 lines
- **The tally** — four consumers in a year is 52 edited lines (and four redeploys) inside the code that takes money
- **Event cost** — each new consumer just subscribes to "OrderPlaced"; the order service changes zero lines
- **The pattern** — feature stores, fraud scorers, and dashboards are all just one more listener on the bus

*Example (italic):* The fraud model ships as a new subscriber on Friday; the order service is not redeployed and never learns it exists.

**Key point:** Events decouple producers from consumers — the code that creates a fact never has to change when a new team wants that fact.

### Visualization (canvas `c3`, 720×300)

Grouped bar chart: lines changed in the order service to add each of four new consumers, layered vs event-driven.

- **Title (bold 15px, `#1a5276`, top center):** "Lines Changed in the Order Service per New Consumer".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; y = lines changed 0 to 20, gridlines `#e5e9ef` at 5/10/15 with 12px `#444` labels; x = four categories with 12px `#444` labels under the baseline at x centers 150, 300, 450, 600: "Email", "Loyalty", "Analytics", "Fraud".
- **Layered bars:** blue `#2a78d6` fill `rgba(42,120,214,0.55)`, 44px wide, left of each center, heights in px `[126, 81, 108, 153]` for values `[14, 9, 12, 17]` (9px per line), 12px blue value labels on top.
- **Event-driven bars:** green `#008300`, 44px wide, right of each center, all value `[0, 0, 0, 0]` drawn as a solid 3px green tick sitting on the baseline with a 12px green "0" label above each.
- **Total label (12px `#6b7280`, near x=90, y=70):** "layered total: 52 lines / yr".
- **Annotation (bold 13px green `#008300`, near x=380, y=75):** "event bus: 0 lines — the order service never changes".
- **Caption (12px `#444`, bottom right):** "line counts illustrative".

## Events Are Not Slow Function Calls

**Tags:** `common mistake` (red), `eventual consistency` (orange)

- **The trap** — treating an event like a function call that happens to run later, with the same guarantees
- **No return** — a publisher gets no answer back; if the loyalty listener fails, the order code never hears it
- **No order** — two events can be handled out of sequence; the call stack's strict ordering is gone
- **The lag** — loyalty points land about 2s after the order: the app shows 120 points, then 125
- **No single trace** — one stack trace becomes log lines in three services; debugging is a treasure hunt

*Example (italic):* A customer refreshes right after paying, sees 120 points instead of 125, and files a bug — the "bug" fixes itself two seconds later.

**Common mistake:** Expecting call-stack guarantees from messages. Events trade immediate consistency, strict ordering, and one-glance stack traces for decoupling — design for the 2-second window instead of denying it.

### Visualization (canvas `c4`, 720×300)

Step chart of the customer's loyalty balance in the seconds after paying: layered updates when the call returns, event-driven updates when the listener gets around to it.

- **Title (bold 15px, `#1a5276`, top center):** "Loyalty Balance After Paying: the Eventually-Consistent Window".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = seconds after payment 0 to 4 (150px per second), 12px `#444` tick labels "0s"–"4s" every 1s; y = points 115 to 130, gridlines `#e5e9ef` at 120 and 125 with 12px `#444` labels 115/120/125/130.
- **Layered step (blue `#2a78d6`, 3px):** through (seconds, points) pairs `[0, 0.6, 0.6, 4]` / `[120, 120, 125, 125]` — jumps to 125 at t=0.6s when the call stack returns; 12px blue label "layered: updated at 0.6s" above the step.
- **Event-driven step (green `#008300`, 3px):** pairs `[0, 2.1, 2.1, 4]` / `[120, 120, 125, 125]` — jumps at t=2.1s when the listener handles the event; 12px green label "event: updated at 2.1s" below the step.
- **Window band:** vertical dashed `#6b7280` (dash 4/3) lines at x=150 (0.6s) and x=375 (2.1s); between them a fill `rgba(217,89,38,0.10)` from y=65 to y=245, with 12px `#d95926` label "1.5s where the two answers disagree" at the top of the band.
- **Annotation (bold 13px orange `#d95926`, near x=395, y=210):** "not a bug — the event just hasn't been handled yet".
- **Caption (12px `#444`, bottom right):** "timings illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); step times for order #4127 (`[20, 180, 40, 250, 60, 50]` ms, waits 600 vs 240), lines-changed values (`[14, 9, 12, 17]`, total 52, vs all zeros), and loyalty-balance steps (120→125 at 0.6s vs 2.1s, 1.5s window) are invented and labeled illustrative; the 600/240/360/60% arithmetic in the text must keep matching the c2 segment widths.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
