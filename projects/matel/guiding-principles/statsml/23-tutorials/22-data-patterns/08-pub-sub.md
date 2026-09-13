# Pub/Sub

**Page type:** detail page (tutorial card-section layout: one h2 per section, two-column `table.layout` with 50% text / 50% viz)
**HTML title tag:** Pub/Sub

**Subtitle:** Publish a message once; everyone who subscribed gets their own copy — a radio broadcast, not a phone call

## One "order_placed", Three Listeners

**Tags:** `core idea` (blue), `running example` (green)

- **14:02:00** — checkout publishes ONE message, "order_placed", to a topic and moves on
- **Email service** — subscribed; it hears the message and sends the receipt
- **Inventory** — subscribed; it hears the same message and decrements stock
- **Analytics** — subscribed; it hears it too and bumps the daily order counter
- **Radio model** — the station broadcasts once; whoever tuned in, hears it

**Example (italic):** Checkout never emailed, never touched stock, never counted — it said "an order happened" exactly once.

**Key point:** The publisher does not know who is listening — it announces the fact to a topic, and subscribers help themselves.

### Visualization (canvas `c1`, 720×300)

Flow diagram: publisher box → topic box → fan-out arrows to three subscriber boxes.

- **Title (bold 15px ink `#1a5276`, top center):** "Published Once at 14:02 — Heard by Everyone Who Subscribed"
- **Publisher box:** (30, 118), 140×64, fill `rgba(42,120,214,0.08)`, blue `#2a78d6` border; bold 13px blue "checkout", 12px `#444` "publishes at 14:02".
- **Topic box:** (260, 110), 180×80, fill `rgba(26,82,118,0.06)`, ink border; bold 13px ink 'topic: "orders"', monospace 12px `#444` "order_placed #7712 $64", italic 11px mute `#6b7280` "held once, copied to each reader".
- **Publish arrow:** blue arrow from (170,150) to (256,150), labeled above in bold 12px blue "1 publish".
- **Subscriber boxes:** 160×56 at x=540, fill `rgba(0,131,0,0.04)`, colored borders; bold 13px name + 12px `#444` action; a colored arrow from the topic's right edge (440,150) to each:
  | y-center | name | action | color |
  |----------|------|--------|-------|
  | 62 | email service | sends receipt | `#008300` (green) |
  | 150 | inventory | stock 42 → 41 | `#199e70` (aqua) |
  | 238 | analytics | orders +1 | `#4a3aa7` (violet) |
- **Annotation:** bold 12px `#444` "3 copies out" at (488, 128).
- **Caption (bold 13px orange `#d95926`, bottom center, y=285):** "checkout named none of them — subscribe tomorrow and you hear tomorrow's orders"

## Following One Message Through the Fan-Out

**Tags:** `worked example` (green), `core idea` (blue)

- **Published** — order 7712, $64, at 14:02:00.000; the topic now holds one message
- **+300ms** — inventory reads its copy: grinder stock 42 → 41
- **+400ms** — email reads its copy: receipt #R-7712 goes out to the shopper
- **+1.0s** — analytics reads its copy: today's order count 8,314 → 8,315
- **Three copies** — each subscriber consumed the SAME message at its own pace

**Example (italic):** One publish, three independent reactions — none of them knew about the others either.

**Key point:** A message is not "handed to" one service — every subscriber gets its own copy and processes it independently.

### Visualization (canvas `c2`, 720×300)

Swimlane timeline: four horizontal lanes with dots at delivery times; x-axis is milliseconds after publish (0 to 1300ms), left padding 150px, right 90px.

- **Title (bold 15px ink, top center):** "One Message, Three Independent Reactions"
- **Lanes (light grid lines `#e5e9ef`; lane name right-aligned bold 12px in lane color; a 7px-radius dot at the event time with a bold 12px time label above and a 12px `#444` action label below):**
  | y | lane name | dot at (ms) | time label | action label | color |
  |---|-----------|-------------|-----------|--------------|-------|
  | 78 | topic "orders" | 0 | published 14:02:00.000 | order 7712, $64 | `#1a5276` (ink) |
  | 132 | inventory | 300 | +300ms | stock 42 → 41 | `#199e70` (aqua) |
  | 186 | email service | 400 | +400ms | receipt #R-7712 sent | `#008300` (green) |
  | 240 | analytics | 1000 | +1.0s | count 8,314 → 8,315 | `#4a3aa7` (violet) |
- **Delivery lines:** dashed (4/3) lines in each subscriber's color from the publish point on the topic lane down to that subscriber's dot.
- **Caption (bold 13px orange, bottom center, y=283):** "same message, three copies, three speeds — nobody waited for anybody"

**Payload note (italic, below canvas):** The message on the topic — illustrative record.

**Payload block (monospace, `#f8f9fa` background, left border 3px solid `#1a5276`):**

```
topic: "orders"
{ "event": "order_placed",
  "ts": "2026-08-24T14:02:00.000Z",
  "order_id": 7712,
  "user": "u42",
  "item": "coffee-grinder",
  "amount": 64.00 }
// note: nothing here names email, inventory, or analytics —
// the publisher does not know they exist
```

## Why Producers Shouldn't Know Their Consumers

**Tags:** `where it's used` (blue), `best practice` (green)

- **Direct calls** — 3 producers each calling 4 consumers means 12 wires to build and maintain
- **Via a topic** — the same systems need only 7 connections, one per box
- **Adding later** — a new fraud-check service just subscribes; checkout's code is untouched
- **Failure isolation** — if email is down, a durable broker holds its messages; checkout never notices
- **Data science angle** — analytics taps the topic without asking any team to call it

**Example (italic):** The fraud team shipped their subscriber on a Tuesday — no other team deployed anything.

**Key point:** Decoupling is the product — new consumers, retired consumers, and crashed consumers all stop being the producer's problem.

### Visualization (canvas `c3`, 720×300)

Split-panel network comparison: direct point-to-point wiring (left) vs a topic in the middle (right); vertical dashed divider `#bdc3c7` at x=360.

- **Title (bold 15px ink, top center):** "Direct Wiring vs a Topic in the Middle"
- **Left panel:** 3 producer nodes (8px blue `#2a78d6` circles at y=78; labels above: checkout, returns, support) fully connected by thin `rgba(213,81,129,0.55)` lines to 4 consumer nodes (8px magenta `#d55181` circles at y=210; labels below: email, inventory, analytics, fraud). Captions centered at x=185: bold 13px magenta "3 × 4 = 12 wires" (y=262) and 12px `#444` "every new consumer touches every producer" (y=280).
- **Right panel:** the same 3 producers (y=78) each connect by `rgba(42,120,214,0.7)` lines to a small topic box (90×24 at center (545,152), fill `rgba(26,82,118,0.08)`, ink border, bold 12px ink label "topic"), which connects by `rgba(0,131,0,0.7)` lines to 4 consumers (y=232). The 4th consumer (fraud) is orange `#d95926` with a dashed (5/4) orange connection. Consumer node colors: green `#008300` except fraud (orange).
- **Captions:** bold 13px green "3 + 4 = 7 wires" centered at (545, 272); bold 11px orange two lines near the fraud node (x=671): "added later," / "nobody redeployed".

## The Confusion: A Broadcast, Not a To-Do List

**Tags:** `common mistake` (red), `rule of thumb` (blue)

- **Not a queue of chores** — a work queue gives each task to ONE worker; pub/sub copies to ALL subscribers
- **Own bookmark** — in durable-log brokers, each subscriber keeps its own position in the log
- **Slow is fine** — analytics lagging 300 messages behind slows nobody else down
- **No reply** — publishers fire and forget; if you need an answer back, this is the wrong tool
- **Pick by question** — "who should do this ONE job?" → queue; "who cares that this happened?" → pub/sub

**Example (italic):** Email is at message #1504, inventory at #1503, analytics at #1204 — same log, three bookmarks.

**Key point:** In pub/sub every subscriber sees every message — if two services would double-do the work, you wanted a work queue instead.

### Visualization (canvas `c4`, 720×300)

Log strip with three subscriber bookmarks; x-scale maps message numbers 1150–1550, left padding 70px, right 40px.

- **Title (bold 15px ink, top center):** "Same Log, Three Bookmarks — a Slow Reader Blocks Nobody"
- **Log strip:** rectangle at y=70, height 34, ink `#1a5276` 1.5px border; filled `rgba(26,82,118,0.08)` from #1150 up to message #1504 and a fainter `rgba(26,82,118,0.03)` beyond it. Tick marks with 12px `#222` labels "#1200", "#1300", "#1400", "#1500" below the strip. Above the strip: bold 12px ink left label 'messages on topic "orders" →' and 11px mute right label "newest: #1504".
- **Bookmarks:** for each subscriber, a 2px colored vertical pointer line from its label row up to the strip, ending in a small downward triangle just below the strip; bold 13px label "name @ #N" plus 12px `#444` note (labels right-aligned for positions past #1450, else left-aligned):
  | message # | name | note | color | label y |
  |-----------|------|------|-------|---------|
  | 1504 | email | fully caught up | `#008300` (green) | 168 |
  | 1503 | inventory | 1 behind | `#199e70` (aqua) | 210 |
  | 1204 | analytics | 300 behind — and that is fine | `#4a3aa7` (violet) | 252 |
- **Caption (bold 13px orange, bottom center, y=290):** "every subscriber reads every message at its own pace — a queue would have split them instead"

## Regeneration instructions

- **Template:** tutorials topic-page skeleton. `<h1>` (no index number) with 2px `#2980b9` bottom border, `.subtitle` gray line, then four `.card-section` blocks. Each section: `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with `td.text-col` (50%) holding tags/bullets/example/key-point and `td.viz-col` (50%) holding the canvas (plus `.payload-note` and `.payload` pre-block in section 2).
- **Text column structure:** `.tags` row of colored pill spans (0.72rem bold, radius 10px — blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`); 5 one-line bullets each opening with `<b>` in `#1a5276`; one italic `.example` line (`#555`, 0.9rem); one `.key-point` callout (background `#f8f9fa`, left border 3px solid `#e74c3c`).
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; canvases `width:100%` with 1px `#e0e0e0` border, 4px radius. `.payload` monospace 0.78em, `#f8f9fa` background, left border 3px solid `#1a5276`. No nav bar, no back/home links.
- **Canvases:** intrinsic 720×300; shared `setup(id)` helper scales backing store by `window.devicePixelRatio` and calls `ctx.scale` so drawing stays in logical coordinates; shared `boxAt` and `arrowTo` helpers draw bordered boxes and arrowheaded lines. All data hardcoded.
- **Chart palette object `P`:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- In regenerated HTML, any card/grid links use `.html` extensions (this page has none — no cross-page links).
