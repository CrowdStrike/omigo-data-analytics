# Instrumentation & Event Logging

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Instrumentation & Event Logging

**Subtitle:** How a click becomes a row — instrumentation is the code that turns a user action into a named, timestamped event that lands in a raw events table

## From a Tap on "Buy Now" to a Row in a Table

**Tags:** `core idea` (blue), `tracking event` (green), `pipeline` (orange)

- **The tap** — at 14:02:11 a shopper taps "Buy Now" on a $34 blender in a shopping app
- **The event** — the SDK builds a small JSON: name `checkout_click`, properties, ids, timestamp
- **The send** — a beacon POSTs it to a collector endpoint without blocking the screen
- **The landing** — the collector appends it to a raw events table: one row per user action
- **The point** — every funnel, DAU chart, and A/B test is downstream of rows born this way

*Example (italic):* The 14:02:11 tap becomes a row in `raw_events` about 90 seconds later — name, properties, timestamp, and ids intact.

**Key point:** Instrumentation is the code that converts a user action into a named, timestamped, id-stamped event — analytics can only ever see what this code chose to record.

### Visualization (canvas `c1`, 720×300)

Left-to-right flow diagram: five stages from the tap to the warehouse row, with the elapsed time under each stage.

- **Title (bold 15px, `#1a5276`, top center):** "One Tap, Five Hops: How a Click Becomes a Row".
- **Boxes:** five rounded boxes (110px wide, 54px tall, 8px radius) centered at y=140, left edges at x = 20, 160, 300, 440, 580; 12px `#2c3e50` two-line labels: "tap 'Buy Now'", "SDK builds event JSON", "beacon POST", "collector buffers", "row in raw_events".
- **Box fills:** stages 1–4 `rgba(42,120,214,0.15)` with 2px `#2a78d6` border; stage 5 `rgba(0,131,0,0.12)` with 2px `#008300` border.
- **Arrows:** 3px `#6b7280` arrows between consecutive boxes at y=140, solid arrowheads.
- **Elapsed labels (12px `#444`, centered under each box at y=215):** "14:02:11.482", "+2 ms", "+38 ms", "+1 s", "+90 s".
- **Payload snippet (11px monospace `#2c3e50`, boxed `rgba(201,133,0,0.10)` with 1px `#c98500` border, x=160 width 260, y=35 height 44):** `{name:"checkout_click", product_id:8817, price:34.00, user_id, session_id, ts}` with a thin `#c98500` connector line down to the "SDK builds event JSON" box.
- **Annotation (bold 13px green `#008300`, near x=560, y=250):** "no event code, no row — ever".
- **Caption (12px `#444`, bottom right):** "latencies illustrative".

## Anatomy of One Event, and the Batch It Rides In

**Tags:** `worked example` (blue), `event schema` (green)

- **The name** — `checkout_click`: object_action, snake_case, exactly one name per action app-wide
- **The properties** — `product_id: 8817`, `price: 34.00`, `cart_size: 2` describe this one click
- **The ids** — `user_id`, `session_id`, `device_id` let this row join every other row later
- **The timestamp** — client time 14:02:11.482 plus server receive time, because device clocks drift
- **The batch** — the SDK queues events and flushes every 10 s; one session's 7 events ship in 3 batches

*Example (italic):* In a 30-second session the shopper fires 7 events; flushes at 10 s, 20 s, and 30 s carry 3, 2, and 2 events into the same raw table.

**Key point:** An event is name + properties + ids + timestamp, and the SDK ships them in batches — so rows arrive in small bursts, not one HTTP call per click.

### Visualization (canvas `c2`, 720×300)

Session timeline: 7 event dots on a 0–30 s axis, three flush markers sweeping them into batches that drop into a raw_events box.

- **Title (bold 15px, `#1a5276`, top center):** "One 30-Second Session: 7 Events, 3 Batches".
- **Axis:** horizontal 2px `#999` timeline at y=120 from x=60 to x=660; 12px `#444` tick labels "0s", "10s", "20s", "30s" at x = 60, 260, 460, 660.
- **Event dots:** 7 filled blue `#2a78d6` circles (radius 6) on the timeline at seconds `[1, 4, 8, 13, 16, 22, 27]` (x = 60 + sec×20); 11px `#2c3e50` labels alternating above/below each dot: "page_view", "product_view", "add_to_cart", "checkout_click", "payment_info", "purchase", "confirmation_view" — "checkout_click" in bold `#1a5276`.
- **Flush markers:** vertical dashed `#d95926` (dash 4/3) lines at 10 s, 20 s, 30 s from y=95 to y=190, each topped with a 12px bold `#d95926` label "flush (3)", "flush (2)", "flush (2)".
- **Batch arrows:** 2px `#d95926` arrows from the foot of each flush line down to a green-bordered box.
- **raw_events box:** rounded box (300px wide, 44px tall) centered at x=360, y=225, fill `rgba(0,131,0,0.12)`, 2px `#008300` border, 12px label "raw_events table — 7 new rows".
- **Annotation (bold 12px violet `#4a3aa7`, near x=520, y=60):** "batching: bursts of rows, not one call per click".
- **Caption (12px `#444`, bottom right):** "event times illustrative".

## Client-Side Sees the Click, Server-Side Sees the Truth

**Tags:** `where it's used` (blue), `client vs server` (orange)

- **Client-side** — the SDK in the app sees everything on screen: views, clicks, scrolls, hovers
- **The leak** — ad blockers and closed tabs silently eat client events: ~9% lost here (illustrative)
- **Server-side** — the backend logs the purchase when the order API commits: complete, but blind to browsing
- **The taxonomy** — a shared event dictionary (names, required properties, owners) keeps teams consistent
- **The pairing** — mature shops track both and reconcile: client for behavior, server for revenue truth

*Example (italic):* Of 1,000 real purchases the client SDK logs 914 and the server logs all 1,000; of 10,000 product views the server logs none.

**Key point:** Client-side tracking gives breadth with loss; server-side gives accuracy without context — count money server-side and behavior client-side, and expect the two to disagree.

### Visualization (canvas `c3`, 720×300)

Grouped horizontal bar chart: capture rate of client-side vs server-side tracking for two event types, as a share of true events.

- **Title (bold 15px, `#1a5276`, top center):** "Who Sees What: Capture Rate by Tracking Side (illustrative)".
- **Axis:** vertical 2px `#999` baseline at x=230, bars extend right, 100% = width 420; 12px `#444` gridline labels "50%" and "100%" at x = 440 and 650, gridlines `#e5e9ef` full height.
- **Group 1 (label 12px `#444` at x=20, y=85): "10,000 product views":**
  - client bar at y=70: blue `#2a78d6` fill `rgba(42,120,214,0.30)`, width 384 (91.4%), 12px label "client: 9,140 (91.4%)" right-aligned inside the bar end
  - server bar at y=100: red `#e74c3c`, width 2 (0%), bold 12px red label "server: 0 — never reaches the backend"
- **Group 2 (label at x=20, y=195): "1,000 purchases":**
  - client bar at y=180: blue, width 384 (91.4%), 12px label "client: 914 (91.4%)" right-aligned inside the bar end
  - server bar at y=210: green `#008300` fill `rgba(0,131,0,0.30)` with 2px `#008300` border, width 420 (100%), bold 12px green label "server: 1,000 (100%)"
- **Bar style:** 18px tall, 11px legend swatches top right ("client SDK" blue, "server log" green).
- **Annotation (bold 13px magenta `#d55181`, centered near y=265):** "the 86 missing client purchases are ad blockers and closed tabs, not lost sales".
- **Caption (12px `#444`, bottom right):** "counts illustrative".

## You Can't Log the Past

**Tags:** `common mistake` (red), `unrecoverable` (orange)

- **The launch** — the blender "Buy Now" redesign ships January 5 with no tracking event on the button
- **The question** — in June a PM asks: did the redesign lift checkout clicks?
- **The fix** — the `checkout_click` event is added April 1; data flows from that day at ~4,200 clicks/day
- **The hole** — January 5 to March 31 is 86 days of clicks that were never logged; no backfill exists
- **The contrast** — a code bug can be patched and stored data reprocessed; an unlogged click is simply gone

*Example (italic):* The June analysis can chart April–June (4,180 / 4,230 / 4,210 clicks per day), but the 86 launch days that mattered most are a permanent blank.

**Common mistake:** Treating instrumentation as something to add later. A bug fix repairs the future and lets you reprocess stored data; a missing event is an unrecoverable hole — you cannot log the past.

### Visualization (canvas `c4`, 720×300)

Line chart of `checkout_click` events per day, January through June: flat zero before instrumentation, a step to ~4,200/day after, with the missing window shaded.

- **Title (bold 15px, `#1a5276`, top center):** "Instrumented April 1: 86 Launch Days Are Gone Forever".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = months "Jan" to "Jun" with 12px `#444` tick labels at equal spacing (x = 60, 180, 300, 420, 540, 660); y = clicks/day 0 to 5,000, gridlines `#e5e9ef` at 1,250 / 2,500 / 3,750 with 11px labels.
- **Missing window:** gray fill `rgba(107,114,128,0.15)` rectangle from x=60 (Jan) to x=420 (Apr 1) over the full plot height, topped by a bold 13px `#6b7280` label "no event existed — 86 days, no backfill" centered at y=90.
- **Data line:** blue `#2a78d6` 3px line through month points `["Jan","Feb","Mar","Apr","May","Jun"]`, clicks/day `[0, 0, 0, 4180, 4230, 4210]` — flat on the baseline through March, vertical step at April.
- **Launch marker:** vertical dashed `#e74c3c` (dash 4/3) line at Jan 5 (x≈76) with 12px `#e74c3c` label "redesign ships" at its top.
- **Instrumentation marker:** vertical dashed `#008300` (dash 4/3) line at x=420 with 12px `#008300` label "event added Apr 1".
- **Annotation (bold 13px red `#e74c3c`, near x=200, y=180):** "the question is about here — the data starts there".
- **Caption (12px `#444`, bottom right):** "clicks/day illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays and counts above (no randomness); latencies, event times, capture rates (914 / 1,000 and 9,140 / 10,000), and clicks/day (0 / 0 / 0 / 4,180 / 4,230 / 4,210) are invented and labeled illustrative; the 86-day gap is the exact day count from Jan 5 to Mar 31.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
