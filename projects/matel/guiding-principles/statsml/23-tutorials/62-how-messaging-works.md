# How Messaging Works

**Page type:** grid page (tutorials category grid: single flat 4-column nav-grid of cards with topic tags)
**HTML title tag:** How Messaging Works

**Subtitle:** Fifty years of sending a line of text to another human — the technologies behind terminal chat, SMS, buddy lists, WhatsApp, iMessage, and phones that message with no internet at all.

## Cards

Each card links to a topic page under `messaging/`. The card shows a colored uppercase category label (`.card-num`), a numbered title, a one-line description, and 2-4 topic tag pills. Pages are grouped by underlying technology, not by vendor — apps that share the same technical recipe share a page. All cards sit in one flat `.nav-grid`; the colored labels carry the grouping.

| # | Category | Title | Link | Description | Topic tags |
|---|----------|-------|------|-------------|------------|
| 1 | PRE-WEB | The Terminal Era | [62-how-messaging-works/01-the-terminal-era.md](62-how-messaging-works/01-the-terminal-era.md) | Before the web there was `talk` echoing keystrokes between terminals, dial-up boards passing mail overnight, and IRC relaying chat rooms across a tree of servers. | unix talk, BBS, usenet, IRC |
| 2 | CARRIER | SMS & Carrier Texting | [62-how-messaging-works/02-sms-and-carrier-texting.md](62-how-messaging-works/02-sms-and-carrier-texting.md) | A text rides the phone network's signaling channel — why it's 160 characters, how it waits for a phone that's off, and why MMS is secretly the web. | SS7, SMSC, 160 chars, RCS |
| 3 | DESKTOP IM | Desktop IM: Buddy Lists & XMPP | [62-how-messaging-works/03-desktop-im-buddy-lists-and-xmpp.md](62-how-messaging-works/03-desktop-im-buddy-lists-and-xmpp.md) | ICQ, AIM, MSN, and Yahoo sold the same tech — a server that knows who's online — behind incompatible walls, until XMPP reinvented it as an open standard. | buddy lists, OSCAR, protocol wars, XMPP |
| 4 | MOBILE | Mobile Internet Messengers | [62-how-messaging-works/04-mobile-internet-messengers.md](62-how-messaging-works/04-mobile-internet-messengers.md) | BBM, WhatsApp, Telegram, and WeChat share one recipe: phone number as identity, a thin always-on connection, and a server holding messages until delivery. | BBM, FunXMPP, MTProto, super-apps |
| 5 | MOBILE | iMessage | [62-how-messaging-works/05-imessage.md](62-how-messaging-works/05-imessage.md) | Apple's messenger is its own technology — the push notification channel is the transport, a key directory maps people to devices, and every copy is encrypted per device. | APNs, IDS, per-device keys, blue vs green |
| 6 | ENCRYPTION | End-to-End Encryption | [62-how-messaging-works/06-end-to-end-encryption.md](62-how-messaging-works/06-end-to-end-encryption.md) | The Signal Protocol made messages unreadable to the server itself — prekeys for offline key agreement, a ratchet that changes keys every message, and the metadata it can't hide. | signal protocol, double ratchet, X3DH, metadata |
| 7 | MACHINERY | The Shared Machinery | [62-how-messaging-works/07-the-shared-machinery.md](62-how-messaging-works/07-the-shared-machinery.md) | What every chat app is built from — a persistent connection, an OS push wake-up, per-device queues, delivery ticks, and fan-out when a group has 200 members. | websockets, push, receipts, fan-out |
| 8 | OFF-GRID | Off-Grid & Mesh Messaging | [62-how-messaging-works/08-off-grid-and-mesh-messaging.md](62-how-messaging-works/08-off-grid-and-mesh-messaging.md) | When there's no internet, phones relay for each other — Bluetooth mesh apps hop messages device to device, and LoRa radios stretch a text across kilometers. | bluetooth mesh, store-and-forward hops, LoRa, no server |

## Regeneration instructions

To rebuild the HTML from this spec:

- **Template:** tutorials category grid. Single page: h1, `.subtitle` paragraph, then one flat `.nav-grid` of `.nav-card` anchors (no h2 section headings).
- **Layout:** `.nav-grid` is CSS grid, `repeat(4, 1fr)`, 16px gap, 15px top margin; responsive: 3 columns below 1400px, 2 below 1100px, 1 below 600px.
- **Links:** the tables above link to the `.md` versions for markdown navigation; in the regenerated HTML, each card's `href` is the same path with an `.html` extension instead.
- **Card structure:** `<a class="nav-card" href="...">` containing `<div class="card-num">CATEGORY LABEL</div>`, `<h3>N. Topic Title</h3>` (unpadded index number matching the 2-digit zero-padded file index), `<p>description</p>`, then `<div class="topics">` of `<span class="topic-tag">` pills.
- **Category label colors:** applied by a small script mapping `.card-num` text to color — "PRE-WEB" `#8e44ad`, "CARRIER" `#2980b9`, "DESKTOP IM" `#16a085`, "MOBILE" `#d35400`, "ENCRYPTION" `#7b241c`, "MACHINERY" `#34495e`, "OFF-GRID" `#27ae60`; the CSS default for `.card-num` is `#2980b9`, 0.75em bold, 4px bottom margin.
- **Card style:** background `#ffffff`, border `1px solid #d8d8d8`, radius 10px, padding 20px, shadow `0 2px 4px rgba(0,0,0,0.05)`, transition on border-color/transform; hover: border `#2980b9`, `translateY(-2px)`. h3 `#1a3a4a` 1em with 6px bottom margin; description `#555` 0.85em. Topic tags: background `#f0f0f0`, border `1px solid #ccc`, radius 4px, padding 2px 6px, 0.7em `#666`, in a flex-wrap row with 4px gap and 8px top margin.
- **Page style:** body system sans-serif, background `#f5f5f0`, text `#2a2a2a`, padding 40px, line-height 1.6; universal `* { margin:0; padding:0; box-sizing:border-box }` reset; h1 1.8em `#2980b9` with 10px bottom margin; subtitle `#666` 1.05em with 30px bottom margin. No nav bar, no back/home links.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22` (label accents also use `#8e44ad`, `#d35400`, `#16a085`, `#7b241c`, `#34495e`).
- **Canvases:** none on this page; any canvases elsewhere in this series use `window.devicePixelRatio` scaling.
