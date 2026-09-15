# Mobile Internet Messengers

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Mobile Internet Messengers

**Subtitle:** The recipe that replaced SMS — your phone number is your account, a thin always-on connection replaces the buddy list, and the server holds your messages until every device picks them up

**Organizing principle:** group by technology, not vendor — BBM, WhatsApp, Telegram, WeChat, LINE, and KakaoTalk are different vendors on one shared technical recipe: phone-number (or PIN) identity, a persistent TCP connection, OS push to wake the app, and server-side store-and-forward with delivery receipts. iMessage is deliberately excluded — APNs-as-transport is its own technology and has its own page.

## BBM: Push Before Push Existed

**Tags:** `core idea` (blue), `always-on connection` (green), `receipts` (orange)

- **One connection** — every BlackBerry kept a single socket open to RIM's NOC, and every app shared it
- **Carrier deal** — carriers integrated RIM's push service, so that one link stayed alive cheaply
- **PIN identity** — your address was the device's PIN; there was no username to register
- **D and R** — BBM's delivered and read marks arrived in 2005 and started the receipt culture
- **The lesson** — one shared connection plus a server-side store beats every app polling on its own

*Example (italic):* A 2007 BlackBerry ran email and BBM all day on one socket while rival phones drained batteries polling.

**Key point:** BBM proved the recipe in 2005 — a single always-on connection, a server that stores and forwards, and receipts — years before smartphones made it universal.

### Visualization (canvas `c1`, 720×300)

Relay flow diagram: device → carrier → RIM NOC → carrier → device, with the single persistent connection highlighted and a receipt path flowing back.

- **Title (bold 15px, `#1a5276`, top center):** "BBM's Relay: One Always-On Socket per Device to RIM's NOC".
- **Row 1 (five rounded boxes centered on y=105, 120px wide, 44px tall, 8px radius, left edges at x=20 / 160 / 300 / 440 / 580, connected by 3px `#6b7280` arrows):** blue `rgba(42,120,214,0.15)` box "Alice's device — one socket, always on"; mute `rgba(107,114,128,0.12)` box "carrier network — push-integrated"; orange `rgba(217,89,38,0.12)` box with 2px `#d95926` border "RIM NOC — store & forward"; mute box "carrier network — push-integrated"; green `rgba(0,131,0,0.12)` box "Bob's device — wakes on push".
- **Connection label (bold 12px blue `#2a78d6`, centered at x=240, y=152):** "the single persistent connection — kept alive by carrier integration".
- **Row 2 (receipts):** dashed green `#008300` 2px arrow from x=640, y=205 back to x=90, y=205 (right to left); bold 12px green labels above it: "D = delivered to Bob's device" at x=490, y=192 and "R = Bob read it" at x=190, y=192; 12px `#444` label "acks ride the same socket back" centered at x=360, y=228.
- **Annotation (bold 13px violet `#4a3aa7`, centered at y=262):** "the lesson every later app copied: one shared connection + server-side store beats polling".
- **Caption (12px `#444`, bottom right):** "schematic; RIM's NOC relayed all BlackBerry traffic".

## WhatsApp: SMS Pricing Built an Empire

**Tags:** `identity` (blue), `protocol` (green), `scale` (orange)

- **The gap** — carriers charged per SMS, so a chat app riding cheap mobile data won on price alone
- **Phone number** — your number, verified by one SMS code, is the account; there is no signup form
- **Instant graph** — the uploaded contact book is the buddy list, so day one already has your friends
- **FunXMPP** — WhatsApp stripped XMPP's XML into binary tokens sent over one persistent TCP socket
- **Push wake** — when the OS kills the background socket, an OS push wakes the app to reconnect
- **Erlang scale** — Erlang on FreeBSD held about 2M sockets per server; ~50 engineers ran 900M users

*Example (italic):* The SMS code that verifies your number is the last SMS the app ever needs you to touch.

**Key point:** WhatsApp's real invention was subtraction — no usernames, no buddy lists, no XML; the phone number and contact book you already had became the whole social layer.

### Visualization (canvas `c2`, 720×300)

Two panels split by a dashed divider at x=360: left shows XMPP stripped down to FunXMPP framing; right shows a connections-per-server magnitude comparison.

- **Title (bold 15px, `#1a5276`, top center):** "XMPP Heritage, Binary Framing, a Million Sockets per Box".
- **Divider:** 1px `#bdc3c7` dashed vertical line at x=360 from y=38 to y=285.
- **Left panel header (bold 13px `#1a5276`, centered at x=185, y=54):** "XMPP stripped to FunXMPP".
- **Left panel:** violet `rgba(74,58,167,0.10)` rounded box (x=40, y=72, 290×52, 2px `#4a3aa7` border) with 12px text lines "&lt;message to=...&gt;&lt;body&gt;hi&lt;/body&gt;" and "~120 bytes of XML tags"; a 3px `#6b7280` down arrow from y=124 to y=162 with 11px `#6b7280` label "tokenize the tags into single bytes" beside it; blue `rgba(42,120,214,0.15)` rounded box (x=40, y=166, 290×52, 2px `#2a78d6` border) with lines "FunXMPP: F8 05 A1 ... binary tokens" and "~12 bytes on the wire".
- **Left annotation (bold 12px aqua `#199e70`, centered at x=185, y=250):** "same stanza shape, ~10x fewer bytes".
- **Right panel header (bold 13px `#1a5276`, centered at x=540, y=54):** "connections held per server".
- **Right panel bars:** baseline y=238 (1px `#999` axis from x=395 to x=700), plot height 155; bar 1 centered x=460, 70px wide, height 4px (visually a sliver), fill `rgba(107,114,128,0.5)`, labels 12px `#444` "typical app server" below and bold 12px `#6b7280` "~10k" above; bar 2 centered x=610, 70px wide, height 155px, fill `rgba(0,131,0,0.15)` with 2px `#008300` border, labels "Erlang gateway" below and bold 12px `#008300` "~2,000,000" above.
- **Right annotation (bold 12px magenta `#d55181`, centered at x=548, y=86):** "~50 engineers, 900M users (2015)".
- **Caption (12px `#444`, bottom right):** "byte and socket counts illustrative; 2M+ sockets per box was publicly reported".

## Telegram and the Cloud-Chat Variant

**Tags:** `protocol` (blue), `storage model` (orange), `trade-off` (red)

- **MTProto** — Telegram (2013) wrote its own protocol instead of XMPP, tuned for weak mobile links
- **Cloud chats** — messages live on the server by default, so a new device sees full history instantly
- **The trade** — the server can read default chats; end-to-end encryption is only in secret chats
- **Secret chats** — E2E chats are bound to one device pair: no cloud copy and no multi-device sync
- **The platform** — bots and channels broadcasting to millions treated messaging as a platform

*Example (italic):* Log in on a new laptop and ten years of chats appear in seconds — because they never left the server.

**Key point:** Cloud-resident storage is a deliberate trade — instant multi-device history in exchange for a server that, for default chats, can read what it stores.

### Visualization (canvas `c3`, 720×300)

Comparison of the two message storage models, split by a dashed divider at x=360: device-resident (WhatsApp-style) vs cloud-resident (Telegram-style).

- **Title (bold 15px, `#1a5276`, top center):** "Where the Message Lives: Device-Resident vs Cloud-Resident".
- **Divider:** 1px `#bdc3c7` dashed vertical line at x=360 from y=38 to y=285.
- **Left panel header (bold 13px `#2a78d6`, centered at x=185, y=54):** "WhatsApp-style: device is the record".
- **Left panel (three stacked rounded boxes, 230px wide, 40px tall, 8px radius, centered at x=185, tops at y=68 / 132 / 196, joined by 3px `#6b7280` down arrows):** green `rgba(0,131,0,0.12)` box "Alice's phone — history lives here"; mute `rgba(107,114,128,0.12)` box with dashed 2px `#6b7280` border "server queue — empties on delivery"; green box "Bob's phone — history lives here".
- **Left annotation (bold 12px orange `#d95926`, centered at x=185, y=262):** "new phone = history gone unless backed up".
- **Right panel header (bold 13px `#4a3aa7`, centered at x=540, y=54):** "Telegram-style: server is the record".
- **Right panel:** blue `rgba(42,120,214,0.15)` rounded box (200px wide, 44px tall, centered at x=540, top y=76, 2px `#2a78d6` border) "cloud store — source of truth, full history"; three 3px `#6b7280` arrows fanning down to three small rounded boxes (90px wide, 36px tall, tops at y=176, centered at x=430 / 540 / 650), each violet `rgba(74,58,167,0.10)` with 11px labels "phone", "laptop", "tablet" and 11px `#444` "full history" under each at y=230.
- **Right annotation (bold 12px orange `#d95926`, centered at x=540, y=262):** "trade: server can read default chats — E2E only in secret chats".
- **Caption (12px `#444`, bottom right):** "schematic".

## The Super-App Turn in Asia

**Tags:** `super-app` (blue), `home market` (green), `network engineering` (orange)

- **Same recipe** — KakaoTalk (2010), WeChat (2011), and LINE (2011) share the identity-plus-socket recipe
- **Beyond chat** — payments, official accounts, and games turned the chat client into a platform
- **Sticker economy** — LINE turned sticker packs into a business earning hundreds of millions a year
- **Market sweep** — in their home markets they replaced not just SMS but much of the app ecosystem
- **Flaky networks** — binary protocols and aggressive reconnect logic were tuned for crowded 2G/3G
- **Multi-DC** — near-total penetration of one country forced early multi-datacenter engineering

*Example (italic):* In one chat app a street vendor's QR payment, a doctor's booking, and a grandmother's video call all coexist.

**Key point:** The recipe was shared; the divergence was the client — Western messengers stayed thin while Asia's grew into operating systems for daily life.

### Visualization (canvas `c4`, 720×300)

Timeline 2005–2015 of launches on one lane, with a second lane listing what each app added on top of chat.

- **Title (bold 15px, `#1a5276`, top center):** "One Recipe, Many Vendors: Launches, and What Each Added".
- **Timeline axis:** 2px `#999` horizontal line at y=120 from x=60 to x=680; x maps 2005–2015 linearly (x = 60 + (year − 2005) × 62); 11px `#6b7280` year ticks under the line at y=138 for 2005 / 2007 / 2009 / 2011 / 2013 / 2015 (skip a tick label where a marker label sits below).
- **Markers (7px radius dots on the line, 1px `#6b7280` stem to each name label):** BBM 2005 at x=60, blue `#2a78d6`; WhatsApp 2009 at x=308, green `#008300`; KakaoTalk 2010 at x=370, yellow `#c98500`; WeChat 2011 at x=426, magenta `#d55181`; LINE 2011 at x=438, aqua `#199e70`; Telegram 2013 at x=556, violet `#4a3aa7`.
- **Name labels (bold 12px in the marker's color):** staggered to avoid overlap — "WhatsApp · 2009" and "WeChat · 2011" high above at y=68; "BBM · 2005", "KakaoTalk · 2010", "Telegram · 2013" above at y=95; "LINE · 2011" below the line at y=158.
- **Second lane:** 12px `#444` header "what each added on top of chat" centered at y=192, above a 1px `#e5e9ef` separator line at y=178; two columns of bold 12px entries in each app's color — left column, textAlign left at x=70: "BBM — D/R receipts, PIN identity" (y=216), "WhatsApp — phone number as the account" (y=238), "KakaoTalk — games and sticker commerce" (y=260); right column at x=390: "WeChat — official accounts, payments" (y=216), "LINE — stickers as a business" (y=238), "Telegram — bots and broadcast channels" (y=260).
- **Caption (12px `#444`, bottom right):** "launch years exact; contributions abbreviated".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links, no cross-page links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array, drawn once on load, and re-run on window resize (debounced 150ms). Shared `roundedRect` and `arrow` helpers.
- **Chart palette object:** `P = { blue:'#2a78d6', green:'#008300', magenta:'#d55181', yellow:'#c98500', aqua:'#199e70', orange:'#d95926', violet:'#4a3aa7', ink:'#1a5276', text:'#2c3e50', mute:'#6b7280', grid:'#e5e9ef' }`. Navy `#1a5276` is ink for headings, axes, and callout borders. Red only for genuine error states (none on this page).
- **Data:** no randomness anywhere; all coordinates and values are the hardcoded literals above. Launch years (BBM 2005, WhatsApp 2009, KakaoTalk 2010, WeChat 2011, LINE 2011, Telegram 2013) are factual; byte counts, socket counts, and traffic figures are invented and labeled illustrative; the ~2M connections-per-server and ~50-engineers/900M-users figures reflect historical public reports about WhatsApp's Erlang/FreeBSD stack.
- **Framing:** factual tech history about publicly reported architectures; make no claims about any company's current internal systems. iMessage is intentionally not covered here (own page — APNs-as-transport).
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
