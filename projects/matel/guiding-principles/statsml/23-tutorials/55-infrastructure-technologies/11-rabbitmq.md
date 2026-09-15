# RabbitMQ

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** RabbitMQ

**Subtitle:** An open-source message broker (AMQP) where the broker is the smart part — it routes each message to the right queues, tracks delivery, and deletes it once a consumer acknowledges it

## One Order, Two Copies

**Tags:** `core idea` (blue), `smart broker` (green), `AMQP` (orange)

- **The shop** — an online store's checkout finishes an order and needs email and warehouse to act
- **The publish** — checkout sends ONE message with routing key `order.placed` to the `orders` exchange
- **The exchange** — a topic exchange holds bindings; the producer never names a queue
- **The bindings** — email queue is bound to `order.*`; warehouse queue is bound to `order.placed`
- **The copies** — both bindings match, so the broker puts an independent copy in each queue
- **The consumers** — the email worker and the warehouse worker each drain their own queue

*Example (italic):* Checkout publishes order #4712 once at 2:00pm; the exchange fans it into the email queue and the warehouse queue, and neither worker knows the other exists.

**Key point:** In RabbitMQ the broker does the routing: producers publish to exchanges, bindings decide which queues get a copy, and consumers only ever see their own queue.

### Visualization (canvas `c1`, 720×300)

Left-to-right flow diagram: producer box → topic exchange box → two queue boxes → two worker boxes, with the routing key on the first arrow and a binding label on each fan-out arrow.

- **Title (bold 15px, `#1a5276`, top center):** "One Publish, Two Queues: the Exchange Does the Routing".
- **Producer box:** at x=30, y=130, 110×44, 8px radius, fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border, 12px `#2c3e50` text "checkout (producer)".
- **Exchange box:** at x=210, y=130, 160×44, fill `rgba(74,58,167,0.12)`, 2px `#4a3aa7` border, text "orders exchange (topic)".
- **Publish arrow:** 3px `#2c3e50` arrow from (140,152) to (210,152); 12px `#6b7280` label "key: order.placed" above it at y=138.
- **Fan-out arrows:** 3px `#2c3e50` arrows from (370,145) to (440,90) and from (370,159) to (440,214); 12px `#6b7280` binding labels "binding: order.*" near (380,95) and "binding: order.placed" near (380,215).
- **Queue boxes:** email queue at x=440, y=70, 140×40, fill `rgba(0,131,0,0.12)`, 2px `#008300` border; warehouse queue at x=440, y=194, same style; 12px text "email queue" / "warehouse queue".
- **Worker boxes:** at x=615, y=70 and x=615, y=194, 90×40, fill `rgba(217,89,38,0.12)`, 2px `#d95926` border, 12px text "email worker" / "wh worker"; 2px arrows from each queue box to its worker.
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=275):** "one publish → two independent copies; the producer never names a queue".

## Following Order #4712 Through Ack and Nack

**Tags:** `worked example` (blue), `ack & nack` (green)

- **Publish** — order #4712 hits the exchange at t=0.0s; copies land in both queues by t=0.1s
- **Warehouse copy** — the warehouse worker picks it up at 0.1s and acks at 0.8s; the broker deletes it
- **Email copy** — the email worker picks it up at 0.1s but crashes mid-send at 2.0s
- **The nack** — the broker gets a nack with requeue=true, so the message goes back on the queue
- **Redelivery** — a second email worker gets it at 2.1s and acks at 3.5s; only then is it deleted
- **Hand-check** — 2 copies, 3 deliveries (one redelivery), 2 acks, 1 nack, 0 messages lost

*Example (italic):* The email copy of order #4712 is delivered twice — crash at 2.0s, nack, redelivery at 2.1s, ack at 3.5s — while the warehouse copy was acked and deleted back at 0.8s.

**Key point:** Every message must be acknowledged per-message; an unacked or nack'd message is requeued and redelivered, so a crashing worker loses no work — the ack is what deletes it.

### Visualization (canvas `c2`, 720×300)

Two-lane event timeline over 4 seconds: the warehouse copy's short life (deliver, ack) on the top lane and the email copy's crash-nack-redeliver-ack story on the bottom lane.

- **Title (bold 15px, `#1a5276`, top center):** "Order #4712: One Copy Acks Fast, One Gets Nack'd and Redelivered".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = seconds 0 to 4, 12px `#444` tick labels "0s"–"4s" every 1s; vertical gridlines `#e5e9ef` at each tick.
- **Lanes:** warehouse lane line 2px `#e5e9ef` at y=110, email lane at y=180; 12px `#444` lane labels "warehouse copy" and "email copy" at x=62 just above each lane.
- **Warehouse events (dots r=6 on y=110):** blue `#2a78d6` dot at t=0.1 labeled "delivered" (12px `#444`, above); green `#008300` dot at t=0.8 labeled bold 12px green "ack → deleted".
- **Email events (dots r=6 on y=180):** blue dot at t=0.1 "delivered"; red `#e74c3c` X marker at t=2.0 labeled bold 12px red "crash → nack (requeue)"; blue dot at t=2.1 "redelivered"; green dot at t=3.5 labeled bold 12px green "ack → deleted".
- **Requeue arc:** dashed `#6b7280` (dash 4/3) 2px arc from the t=2.0 X back down to the t=2.1 dot, hinting the message bounced through the queue.
- **Annotation (bold 13px violet `#4a3aa7`, near t=2.6s, y=70):** "nack put it back — nothing was lost".
- **Caption (12px `#444`, bottom right):** "timings illustrative".

## Why Task Queues Still Pick It

**Tags:** `where it's used` (blue), `task queues` (green)

- **Work queues** — one queue, many workers; the broker deals each job to exactly one worker at a time
- **Safety** — per-message acks mean a dead worker's job is simply redelivered to a live one
- **Knobs** — message priorities jump the line; per-message TTLs expire stale jobs automatically
- **RPC style** — a request message carries a reply-to queue; the answer comes back the same way
- **Scaling math** — 120 queued emails at 2s each: 1 worker drains in 240s, 3 workers in 80s

*Example (italic):* Adding a fourth email worker at the 2:00pm rush cuts the 120-email backlog drain from 80s to 60s — no code change, just one more consumer on the queue.

**Key point:** RabbitMQ is still the right fit when messages are tasks to be done once — routed to the right queue, spread across workers, retried on failure, and deleted when done.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart: time to drain a 120-email backlog (2s handling per email) as the worker count grows from 1 to 4.

- **Title (bold 15px, `#1a5276`, top center):** "Draining 120 Queued Emails: Add Workers, Not Code".
- **Axis:** vertical 2px `#999` baseline at x=230, bars extend right, max width 440 (= 240s); 440px maps 240s linearly, so px width = seconds × 11/6.
- **Rows (bars 18px tall at y = 70, 120, 170, 220), each with a left-aligned 12px `#444` label at x=20:**
  - "1 worker": blue `rgba(42,120,214,0.30)` bar width 440, 12px `#444` end label "240 s"
  - "2 workers": blue bar width 220, end label "120 s"
  - "3 workers": green `rgba(0,131,0,0.30)` bar with 2px `#008300` border, width 147, bold 12px green end label "80 s"
  - "4 workers": blue bar width 110, end label "60 s"
- **Annotation (bold 13px magenta `#d55181`, right side near y=250):** "drain time = 120 × 2 s ÷ workers".
- **Caption (12px `#444`, bottom right):** "email volume and 2 s handling illustrative; the division is exact".

## It Deletes What You Might Need Back

**Tags:** `common mistake` (red), `replay` (orange)

- **Ack means gone** — an acked message is deleted from the queue; RabbitMQ keeps no history of it
- **The mistake** — a team builds analytics off the orders queue, then needs last month's orders back
- **The Kafka contrast** — Kafka is a dumb log with smart consumers: it retains, consumers track offsets
- **Replay** — a new Kafka consumer can reread from offset 0; a new RabbitMQ consumer sees only new work
- **The rule** — RabbitMQ for tasks to do once; Kafka (or a store) when history must be rereadable

*Example (italic):* Six months of order messages flowed through the queue, every one acked and deleted — the new analytics consumer joins and finds nothing to replay.

**Common mistake:** Using a delete-on-ack broker where you needed replayable history. RabbitMQ routes and deletes; Kafka retains and replays — pick by whether anyone will ever need to reread.

### Visualization (canvas `c4`, 720×300)

Two-row diagram: the RabbitMQ queue after acks (empty, crossed-out slots) vs a Kafka log with retained offsets and a replay arrow back to offset 0.

- **Title (bold 15px, `#1a5276`, top center):** "Routes and Deletes vs Retains and Replays".
- **Row 1 (y=85), 12px `#444` label at x=20:** "RabbitMQ queue after acks"; six 44×36 slots at x = 230, 285, 340, 395, 450, 505, 1.5px dashed `#6b7280` borders, no fill, each with a 2px red `#e74c3c` X across it; bold 12px red label "deleted on ack — nothing to replay" at x=230, y=145.
- **Row 2 (y=190), label at x=20:** "Kafka log (retained)"; ten 40×36 boxes at x = 230 + i×46 for i = 0..9, fill `rgba(42,120,214,0.25)`, 1.5px `#2a78d6` borders, 11px `#2c3e50` offset numbers "0"–"9" inside.
- **Replay arrow:** 2.5px green `#008300` arc from above the offset-9 box (x≈690, y=180) back to above the offset-0 box (x≈250, y=180), arrowhead at the offset-0 end; bold 12px green label "new consumer replays from offset 0" centered above the arc at y=160.
- **Annotation (bold 13px orange `#d95926`, centered near y=272):** "RabbitMQ routes and deletes; Kafka retains and replays".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all coordinates, timings, and counts are the hardcoded values above (no randomness); order timings, email volume (120), and the 2s handling time are invented and labeled illustrative; the drain seconds (240 / 120 / 80 / 60 = 120 × 2 ÷ workers) are exact arithmetic on those illustrative inputs; broker behavior (exchanges, bindings, per-message acks, requeue on nack, delete on ack, priorities, TTLs, Kafka's retained log with consumer offsets) reflects public documentation.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
