# Chat Messaging

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Chat Messaging

**Subtitle:** A billion phones each hold one open connection to a server — the hard part of a messenger is keeping millions of mostly-silent sockets alive, not moving the words

## A Billion Phones, One Open Socket Each

**Tags:** `core idea` (blue), `persistent connections` (green), `Erlang` (orange)

- **The phone** — every online phone keeps one long-lived connection open to a gateway server
- **The silence** — that socket is idle almost all day; it exists so the server can push instantly
- **The count** — one gateway holds ~2,000,000 open sockets; only ~30,000 carry a message in a given second
- **The history** — Erlang-based gateways were publicly reported holding 1M+ connections per server
- **The trade** — the scarce resource is memory per idle socket, not CPU for the messages themselves

*Example (italic):* At the 6pm peak a single gateway holds 2,000,000 open sockets, and 98.5% of them are carrying nothing at that instant.

**Key point:** A mobile messenger is a connection-holding problem first and a message-moving problem second — the server is designed around millions of cheap, mostly-idle sockets.

### Visualization (canvas `c1`, 720×300)

Line chart of one gateway server over 24 hours: open connections (large, slowly varying) vs connections actively sending (a barely visible line hugging the baseline).

- **Title (bold 15px, `#1a5276`, top center):** "One Gateway, One Day: 2M Sockets Open, ~30k Talking".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = hour 0 to 24 with 12px `#444` tick labels "0h" / "6h" / "12h" / "18h" / "24h"; y = connections 0 to 2M, gridlines `#e5e9ef` at 0.5M / 1.0M / 1.5M with 12px `#444` labels "0.5M", "1.0M", "1.5M".
- **Open-sockets line:** blue `#2a78d6` 3px through hours `[0, 3, 6, 9, 12, 15, 18, 21, 24]`, millions `[1.2, 1.0, 0.9, 1.1, 1.5, 1.8, 2.0, 1.9, 1.4]`, filled beneath with `rgba(42,120,214,0.15)`.
- **Active-sockets line:** green `#008300` 3px through the same hour grid, millions `[0.018, 0.015, 0.013, 0.017, 0.022, 0.027, 0.030, 0.028, 0.021]` — visually flat along the baseline.
- **Labels:** bold 12px blue `#2a78d6` "sockets open" above the curve near hour 18; bold 12px green `#008300` "sockets sending right now" just above the green line near hour 10.
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=110):** "at the 6pm peak, 98.5% of 2,000,000 sockets are silent".
- **Caption (12px `#444`, bottom right):** "counts illustrative; 1M+ sockets per server matches historical public reports".

## One Message, Two Ticks, Then Blue

**Tags:** `worked example` (blue), `delivery receipts` (green), `offline queue` (orange)

- **The send** — Asha taps send at t=0; her phone writes the message up its already-open connection
- **One tick** — at t=80ms her gateway acks receipt: state becomes "sent" (the server has it)
- **The route** — a lookup finds which gateway holds Ben's socket; the message hops there
- **Two ticks** — at t=230ms Ben's phone acks the push: state becomes "delivered" (his device has it)
- **Blue ticks** — at t=6s Ben opens the chat: state becomes "read"; every tick is a state transition
- **Offline path** — if Ben's socket is gone, the message waits in a server queue until he reconnects

*Example (italic):* Ben's phone is off in a subway; the message sits queued for 42 minutes and flips to two ticks the moment his connection comes back.

**Key point:** Delivery receipts are a tiny state machine per message — sent → delivered → read — and each transition is just a small ack flowing back along the same open connections.

### Visualization (canvas `c2`, 720×300)

Two-row flow diagram: the online delivery path with tick-state timestamps vs the offline path where the message parks in a queue until reconnect.

- **Title (bold 15px, `#1a5276`, top center):** "Sender → Gateway → Gateway → Recipient: Ticks Are State Transitions".
- **Row 1 (boxes centered on y=100), 12px `#444` label "Ben online" at x=20, y=60:** four rounded boxes, 130px wide, 44px tall, 8px radius, left edges at x=90 / 250 / 410 / 570 sized to fit, connected by 3px `#6b7280` arrows: blue `rgba(42,120,214,0.15)` box "Asha's phone — t=0"; blue box "gateway A — ack 80ms, ✓ sent"; blue box "gateway B — route lookup"; green `rgba(0,131,0,0.12)` box "Ben's phone — 230ms, ✓✓ delivered". Bold 12px violet `#4a3aa7` note under the last box at y=140: "read at t=6s → blue ✓✓".
- **Row 2 (boxes centered on y=210), 12px `#444` label "Ben offline" at x=20, y=170:** blue box "Asha's phone — t=0"; blue box "gateway A — ✓ sent"; orange `rgba(217,89,38,0.12)` box with 2px `#d95926` border "queue — stored 42 min"; green box "reconnect → push, ✓✓". Same widths, positions, and arrow style as row 1.
- **Box text:** 12px `#2c3e50`, two short lines per box.
- **Annotation (bold 12px green `#008300`, centered near y=272):** "the acks ride back along the same open sockets".
- **Caption (12px `#444`, bottom right):** "timings illustrative".

## Groups Turn One Send into a Thousand Pushes

**Tags:** `fan-out` (blue), `groups` (green), `scale` (orange)

- **The group** — a group chat is just a member list of devices, not a special kind of channel
- **The fan-out** — one message to a 200-member group becomes 199 separate pushes, one per device
- **The growth** — pushes per send scale linearly: 8 members → 7, 200 → 199, 1,024 → 1,023
- **The day** — a chatty 200-member group at 500 messages/day generates 99,500 pushes/day
- **The lesson** — write amplification, not storage, is what caps group size in messenger designs

*Example (italic):* One "happy birthday" tapped into a 1,024-member group costs the servers 1,023 deliveries before anyone even replies.

**Key point:** Fan-out is the multiplier that turns a modest per-user message rate into the real server load — which is exactly why messengers cap group sizes.

### Visualization (canvas `c3`, 720×300)

Vertical bar chart: server pushes generated by one sent message, for five group sizes.

- **Title (bold 15px, `#1a5276`, top center):** "One Send, N−1 Pushes: Fan-Out by Group Size".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; y = pushes per message 0 to 1,100, gridlines `#e5e9ef` at 250 / 500 / 750 / 1000 with 12px `#444` labels.
- **Bars:** 56px wide, centered at x = 130 / 250 / 370 / 490 / 610; heights for pushes `[1, 7, 49, 199, 1023]`; the first four bars blue `#2a78d6` fill `rgba(42,120,214,0.30)` with 2px blue edge, the last bar orange `#d95926` fill `rgba(217,89,38,0.25)` with 2px orange edge.
- **Labels:** group sizes under each bar, 12px `#444`: "2", "8", "50", "200", "1,024" plus a shared "members" label under the axis center; bold 12px value labels above each bar: "1", "7", "49", "199", "1,023".
- **Annotation (bold 13px magenta `#d55181`, upper left near x=90, y=80):** "200-member group × 500 msgs/day = 99,500 pushes/day".
- **Caption (12px `#444`, bottom right):** "fan-out exact: pushes = members − 1; traffic figures illustrative".

## Encrypted Does Not Mean the Server Stores Nothing

**Tags:** `common mistake` (red), `encryption` (orange)

- **The lock** — with end-to-end encryption the keys live only on the phones; the server relays ciphertext
- **Still working** — the server still routes, queues, and acks — all on bytes it cannot read
- **The header** — routing needs metadata: sender, recipient, and timestamp stay visible to the server
- **The queue** — offline messages are stored as ciphertext; a database leak exposes no message text
- **The mistake** — assuming "encrypted" means the server holds nothing, or can read what it holds

*Example (italic):* The gateway happily queues Ben's 42-minute-old message without ever knowing whether it says "on my way" or a bank password.

**Common mistake:** Confusing encryption of the body with invisibility of the envelope — end-to-end encryption blinds the server to content, but the server must still see who is talking to whom, and when, in order to route at all.

### Visualization (canvas `c4`, 720×300)

Two-row diagram: the message envelope split into a readable header and an unreadable body, then the key flow showing encryption and decryption happening only on the phones.

- **Title (bold 15px, `#1a5276`, top center):** "What the Gateway Can and Cannot Read".
- **Row 1 (boxes centered on y=95), 12px `#444` label "the envelope" at x=20, y=60:** blue `rgba(42,120,214,0.15)` rounded box at x=170, 220px wide, 44px tall, "header — from: Asha, to: Ben, 14:03" with bold 12px blue `#2a78d6` caption below: "✓ server reads — needed to route"; violet `rgba(74,58,167,0.12)` box at x=430, 220px wide, "body — 8f3a1c9e2b7d…" with bold 12px red `#e74c3c` caption below: "✗ server cannot read".
- **Row 2 (boxes centered on y=205), label "the keys" at x=20, y=170:** green `rgba(0,131,0,0.12)` box at x=90, 170px wide, "Asha's phone — encrypts (key on device)"; 3px `#6b7280` arrow to a mute `rgba(107,114,128,0.12)` box at x=290, 190px wide, "gateway + offline queue — relays ciphertext"; arrow to a green box at x=510, 170px wide, "Ben's phone — decrypts".
- **Box text:** 12px `#2c3e50`, 8px radius, two short lines per box.
- **Annotation (bold 13px orange `#d95926`, centered near y=272):** "encryption hides the letter, not the envelope".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); connection counts, message timings, and group traffic figures are invented and labeled illustrative; the 1M+ connections-per-server figure reflects historical, publicly reported engineering results for Erlang-based messaging gateways; pushes = members − 1 is exact arithmetic.
- **Framing:** treat the whole page as a generic system-design exercise built from publicly reported and well-known concepts — make no claims about any company's current internal systems.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
