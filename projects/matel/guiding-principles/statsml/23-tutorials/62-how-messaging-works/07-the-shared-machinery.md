# The Shared Machinery

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** The Shared Machinery

**Subtitle:** Strip the branding off any chat app and the same five moving parts appear — a thin connection, a push wake-up, a mailbox per device, delivery ticks, and a fan-out loop for groups

## Staying Reachable: How the Client Holds the Line

**Tags:** `core idea` (blue), `connections` (green), `battery` (orange)

- **The one-way street** — a server cannot dial your phone; the client must open and hold the line
- **Polling** — early clients asked "anything new?" every few seconds, and most trips came back empty
- **Long polling** — the server holds each request open and answers only when news actually arrives
- **WebSocket** — one thin always-open pipe replaces the asking; tiny heartbeats prove it is still alive
- **The OS push channel** — phones kill background sockets, so one shared OS connection wakes every app
- **Wake then fetch** — the push only says "something arrived"; the app reconnects and fetches it itself

*Example (italic):* Alice's phone sleeps with the chat app's socket closed; one tiny OS push wakes the app, which reconnects and fetches her message in about a second.

**Key point:** The client, not the server, keeps the line open — and on phones the operating system's single shared push connection does the waiting on behalf of every app at once.

### Visualization (canvas `c1`, 720×300)

Three-panel comparison of how a client stays reachable: repeated polling, long polling, and an always-open pipe plus OS push. Each panel is a mini ladder with a client lifeline on the left and a server lifeline on the right, time running downward.

- **Title (bold 15px, `#1a5276`, top center):** "Polling vs Long Polling vs Always-Open Pipe".
- **Panel dividers:** dashed 1px `#bdc3c7` verticals at x=245 and x=485, from y=40 to y=262.
- **Lifelines:** 1.5px `#e5e9ef` verticals from y=78 to y=252; panel 1 client x=60 / server x=205, panel 2 client x=300 / server x=445, panel 3 client x=540 / server x=685. 12px `#444` labels "client" / "server" above each lifeline at y=72.
- **Panel headers (bold 13px, centered over each panel at y=52):** orange `#d95926` "polling"; blue `#2a78d6` "long polling"; green `#008300` "socket + OS push".
- **Panel 1 (polling):** five request arrows client→server (2px `#6b7280`) at y = 92 / 124 / 156 / 188 / 220, each with a return arrow server→client 10px lower; the first, second, third, and fifth returns carry an 11px `#6b7280` label "empty" at midpoint, the fourth return is 2.5px green `#008300` labeled bold 11px "msg". Bottom annotation (bold 12px orange `#d95926`, centered at y=248): "5 trips, 1 message".
- **Panel 2 (long polling):** one request arrow client→server at y=95 labeled 11px `#444` "ask once"; a 3px blue `#2a78d6` vertical "held" bar on the server lifeline from y=100 to y=185 with 11px blue label "server holds the request"; a green 2.5px return arrow server→client at y=192 labeled bold 11px green "msg arrives"; a second gray request arrow at y=222 labeled 11px `#6b7280` "re-ask". Bottom annotation (bold 12px blue `#2a78d6`, y=248): "quiet line, instant answer".
- **Panel 3 (socket + push):** a 5px `rgba(0,131,0,0.25)` vertical band between the two lifelines drawn as a thick 4px green `#008300` horizontal double-headed connector at y=95 labeled 11px green "one open pipe"; two tiny heartbeat double-ticks (11px `#199e70` text "· heartbeat ·") centered at y=122 and y=142; a dashed 2.5px violet `#4a3aa7` arrow server→client at y=172 labeled bold 11px violet "OS push: wake!"; a gray request arrow client→server at y=200 labeled 11px "fetch"; a green return arrow at y=224 labeled bold 11px green "msg". Bottom annotation (bold 12px green `#008300`, y=248): "idle pipe, near-zero battery".
- **Caption (12px `#444`, bottom right):** "trip counts illustrative".

## The Life of One Message

**Tags:** `worked example` (blue), `receipts` (green), `store-and-forward` (orange)

- **Optimistic send** — Alice's client assigns a local ID and shows the message before any server reply
- **One tick** — the server acks and writes the message into Bob's per-device queue on disk: state "sent"
- **The queue** — store-and-forward means the server holds the message durably until Bob's device takes it
- **The push branch** — if Bob is connected the message rides his socket; if not, it waits and a push fires
- **Two ticks** — Bob's device acks receipt back through the server, and the state becomes "delivered"
- **Blue ticks + retries** — the read receipt is its own tiny message; retries dedup by ID, so no doubles

*Example (italic):* Bob's phone is dark for forty minutes; the message sits in his queue the whole time, then flips to two ticks seconds after he regains signal.

**Key point:** Every tick is a transition in a tiny per-message state machine — sent, delivered, read — driven by small acks flowing back along the same open connections.

### Visualization (canvas `c2`, 720×300)

Ladder diagram Alice → chat server → Bob with time running downward, showing the tick state at each hop and a dashed offline branch.

- **Title (bold 15px, `#1a5276`, top center):** "One Message, Three Lifelines: Where Each Tick Is Born".
- **Lifelines:** 1.5px `#e5e9ef` verticals from y=64 to y=268 at x=110 ("Alice"), x=360 ("chat server"), x=610 ("Bob"); bold 13px `#1a5276` name labels centered at y=54.
- **Arrows:** 2.5px with filled arrowheads unless noted; labels 11px placed just above each arrow midpoint.
  - y=88 Alice→server, `#6b7280`, label `#444` "send (local id 42)".
  - y=108 server→Alice, blue `#2a78d6`, bold blue label "ack → ✓ sent".
  - Small orange `rgba(217,89,38,0.15)` rounded box (2px `#d95926` border) centered on the server lifeline at y=126, 150px wide, 20px tall, bold 11px `#d95926` text "write to Bob's queue (disk)".
  - y=152 server→Bob, `#6b7280`, label `#444` "online: push down his socket".
  - y=172 server→Bob, dashed 2px orange `#d95926`, bold orange label "offline: store + OS push, deliver on reconnect".
  - y=196 Bob→server, `#6b7280`, label `#444` "device ack".
  - y=214 server→Alice, green `#008300`, bold green label "✓✓ delivered".
  - y=238 Bob→server, `#6b7280`, label `#444` "opens chat: read".
  - y=256 server→Alice, violet `#4a3aa7`, bold violet label "blue ✓✓ read".
- **Annotation (bold 12px magenta `#d55181`, centered at y=283):** "retries + dedup on id 42 → effectively exactly-once".
- **Caption (12px `#444`, bottom right):** "timings illustrative".

## Presence and Typing: Traffic That Is Allowed to Die

**Tags:** `core idea` (blue), `ephemeral` (orange), `throttling` (green)

- **Two kinds of traffic** — messages are durable and queued; presence and "typing…" are fire-and-forget
- **Never queued** — a typing event goes only to watchers connected right now and is dropped otherwise
- **No receipts** — ephemera carry no ticks and no retries, because a lost typing event costs nothing
- **Fan-out cost** — one status flip notifies everyone watching you, so servers batch and throttle updates
- **Old problem** — this is the 1997 desktop buddy-list problem again, at a hundred times the scale

*Example (italic):* Bob starts typing, loses signal mid-word, and the event simply vanishes — nothing stores it, nothing retries it, and nobody misses it.

**Key point:** The same connection carries two classes of traffic — durable messages that must arrive exactly once, and ephemera that are cheapest when allowed to die.

### Visualization (canvas `c3`, 720×300)

Two horizontal lanes contrasting the durable message path with the ephemeral presence/typing path.

- **Title (bold 15px, `#1a5276`, top center):** "Two Lanes on One Connection: Durable vs Ephemeral".
- **Lane divider:** dashed 1px `#bdc3c7` horizontal at y=160 from x=20 to x=700.
- **Lane 1 (durable, boxes centered on y=100), bold 12px blue `#2a78d6` lane label "message lane" at x=20, y=62:** four rounded boxes (8px radius, 44px tall, 2.5px `#6b7280` arrows between): blue `rgba(42,120,214,0.15)` box at x=90, 130px wide, "Alice sends / id 42"; orange `rgba(217,89,38,0.12)` box (2px `#d95926` border) at x=260, 140px wide, "queue on disk / survives crashes"; green `rgba(0,131,0,0.12)` box at x=440, 130px wide, "Bob's device / acks"; violet `rgba(74,58,167,0.12)` box at x=610, 90px wide, "✓✓ + read". Bold 11px blue annotation centered at x=360, y=145: "stored, retried, receipted — must arrive exactly once".
- **Lane 2 (ephemeral, boxes centered on y=210), bold 12px orange `#d95926` lane label "ephemera lane" at x=20, y=178:** aqua `rgba(25,158,112,0.13)` box at x=90, 130px wide, "Bob types… / event fires"; arrow to aqua box at x=280, 170px wide, "connected watchers / see it now"; a second dashed 2px `#6b7280` arrow from the first box angling down to a mute `rgba(107,114,128,0.10)` box (dashed 1.5px `#6b7280` border) at x=280 centered on y=262, 170px wide, "offline watcher / dropped ✗"; to the right, an orange `rgba(217,89,38,0.12)` box at x=520, 160px wide, "batch + throttle / status floods". Bold 11px orange annotation centered at x=360 under lane at y=291: "no disk, no retry, no receipt".
- **Annotation (bold 12px magenta `#d55181`, right-aligned at x=700, y=178):** "the 1997 buddy-list problem, at 100× scale".

## Groups, Devices, and the Fan-Out Loop

**Tags:** `fan-out` (blue), `multi-device` (green), `scale` (orange)

- **One upload** — a group message goes up from Alice exactly once; the server does all the copying
- **Write amplification** — 200 members means 200 server-side queue writes, not 200 uploads from Alice
- **Every device counts** — phone, laptop, and tablet each get their own queue and encryption session
- **"Delivered" softens** — with many devices it means delivered to at least one of Bob's devices
- **The flip** — huge channels stop pushing to everyone; members pull on demand from one shared log

*Example (italic):* Alice sends one photo to a 200-member group; her phone uploads it once and the server performs the other 199 deliveries for her.

**Key point:** Fan-out happens on the server, one queue write per member device — until groups grow so large that push-to-everyone flips into read-on-demand from a shared log.

### Visualization (canvas `c4`, 720×300)

Fan-out diagram: one upload from Alice exploding into per-member-device queues, with a right-hand inset showing the shared-log model that very large channels flip to.

- **Title (bold 15px, `#1a5276`, top center):** "One Upload, N Queue Writes — Until the Model Flips".
- **Inset divider:** dashed 1px `#bdc3c7` vertical at x=500 from y=40 to y=280.
- **Alice node:** filled blue `#2a78d6` circle, radius 14, at (70, 160), bold 11px white "A" centered; 12px `#444` label "Alice" underneath at y=186.
- **Upload arrow:** 3px blue `#2a78d6` arrow from (86, 160) to (150, 160), bold 12px blue label "1 upload" above at y=150.
- **Server box:** blue `rgba(42,120,214,0.15)` rounded box (2px `#2a78d6` border) at x=152, y=136, 110px wide, 48px tall, 12px `#2c3e50` two-line text "chat server / copies msg".
- **Queue boxes (the fan):** six rounded boxes 150px wide, 26px tall, left edge x=330, centered on y = 70 / 106 / 142 / 178 / 214 / 250; 2px `#6b7280` arrows from the server box's right edge (262, 160) to each box's left edge. Boxes 1-2 mute `rgba(107,114,128,0.10)` with 11px `#2c3e50` text "member 1 — phone" and "member 2 — phone"; boxes 3-4 green `rgba(0,131,0,0.12)` (1.5px `#008300` border) "Bob — phone" and "Bob — laptop"; box 5 mute "member 4 — tablet"; box 6 mute with centered text "… 200 device queues".
- **Annotation (bold 11px green `#008300`, left-aligned at x=330, y=284):** "delivered = at least one of Bob's devices".
- **Inset (huge channels), header bold 13px `#d95926` centered at x=610, y=58: "huge channel":** one orange `rgba(217,89,38,0.12)` rounded box (2px `#d95926` border) at x=545, y=88, 130px wide, 44px tall, text "one shared log / (append only)"; below it three small mute `#6b7280` filled circles radius 8 at (570, 210) / (610, 210) / (650, 210) with 2px `#6b7280` arrows pointing upward from each circle to the log box, bold 11px `#d95926` label "members pull" centered at x=610, y=240; 11px `#444` note centered at x=610, y=262: "read on demand, no fan-out".
- **Caption (12px `#444`, bottom right):** "member counts illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms). Shared helpers: `roundedRect(ctx,x,y,w,h,r)` and `arrow(ctx,x1,y1,x2,y2,color,width)` (filled arrowhead), plus `dashedArrow` variant using `setLineDash`.
- **Chart palette object:** `const P` — blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** everything drawn comes from the hardcoded coordinates and labels above — no randomness, no dates; trip counts, timings, and member counts are invented and labeled illustrative; "pushes = one queue write per member device" is exact arithmetic.
- **Framing:** the page is vendor-neutral — "the chat server", Alice and Bob; it describes the generic anatomy shared by modern messengers and makes no claims about any specific product's internals.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
