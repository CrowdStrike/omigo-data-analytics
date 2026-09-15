# Desktop IM: Buddy Lists &amp; XMPP

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Desktop IM: Buddy Lists &amp; XMPP

**Subtitle:** One technical idea — a long-lived connection to a central server that tracks who's online — sold by four rival vendors, then reinvented as an open standard

## The Buddy-List Machine

**Tags:** `core idea` (blue), `presence` (green), `client-server` (orange)

- **One connection** — the client opens a persistent TCP connection to the vendor's server and holds it all session.
- **Presence** — the server tracks who is online, away, or idle, and pushes every change to whoever is watching.
- **Buddy list** — your contact list lives on the server, so it followed you to any computer you signed in from.
- **Four brands, one tech** — ICQ (1996), AIM (1997), MSN Messenger (1999), and Yahoo all sold this same design.
- **Server relay** — messages route through the server; only bulky file transfers negotiated a direct connection.
- **Fan-out cost** — one "away" click must be pushed to every buddy who lists you, so N watchers means N pushes.

*Example (italic):* When Alice clicks "away", the server pushes that single change to all 60 people who have her on their buddy lists.

**Key point:** The expensive part of an instant messenger is not the messages — it is presence: every status change fans out through the server to everyone watching, and the server does that work for millions of users at once.

### Visualization (canvas `c1`, 720×300)

Hub-and-spoke fan-out diagram: one client's status change entering the central server, then fanning out to a column of buddy clients.

- **Title (bold 15px, ink `#1a5276`, top center):** "One Status Change, N Pushes: Presence Fan-Out".
- **Left node (Alice):** rounded box 8px radius, blue fill `rgba(42,120,214,0.15)` with 2px blue `#2a78d6` border, ~128×46, centered near x=95, y=155; two 12px `#2c3e50` lines: "Alice's client" / "sets status: away". Bold 12px blue caption below: "1 change in".
- **Center node (server):** rounded box, violet fill `rgba(74,58,167,0.12)` with 2px violet `#4a3aa7` border, ~150×62, centered near x=320, y=155; three 12px lines: "vendor's server" / "presence table" / "who watches Alice?".
- **Arrow in:** 3px blue arrow from Alice's box to the server box, bold 12px blue label "presence update" above it.
- **Right column (buddies):** six rounded boxes ~108×32, green fill `rgba(0,131,0,0.10)` with 1.5px aqua `#199e70` border, stacked at x≈560, y = 52 / 92 / 132 / 172 / 212 / 252; labels 12px `#2c3e50`: "buddy 1" … "buddy 5", last box "… buddy 60"; a 2px aqua arrow from the server's right edge to each box.
- **Annotation (bold 13px orange `#d95926`, centered near x=320, y=250):** "fan-out: 60 watchers = 60 pushes".
- **Footnote (12px `#444`, bottom left, y=292):** "messages route through the same server; file transfer may go direct".
- **Caption (12px `#444`, bottom right):** "watcher count illustrative".

## Walled Gardens and the Protocol Wars

**Tags:** `lock-in` (blue), `incompatibility` (red), `reverse engineering` (orange)

- **Proprietary by design** — AIM spoke OSCAR, MSN spoke MSNP, Yahoo spoke YMSG; each protocol was secret and incompatible.
- **No crossing** — an AIM user could not message an MSN user; your network was picked by where your friends already were.
- **The 1999 war** — Microsoft reverse-engineered AIM to interoperate, and AOL repeatedly changed its servers to block them.
- **Client-side fix** — Trillian and Pidgin (then Gaim) reverse-engineered all four protocols to put four networks in one window.
- **Fragile bridge** — every server-side protocol tweak broke those third-party clients until their authors patched again.

*Example (italic):* Through the summer of 1999 AOL blocked MSN Messenger's access to AIM again and again, and Microsoft shipped a workaround after each block.

**Key point:** The four networks never connected to each other — every interoperability that existed was reverse engineering at the client, tolerated only until the vendor's next server change broke it.

### Visualization (canvas `c2`, 720×300)

Four disconnected network islands on top, one multi-protocol client below bridging into all four from the user's side.

- **Title (bold 15px, ink `#1a5276`, top center):** "Four Islands, No Bridges Between Servers".
- **Islands (top row):** four rounded boxes ~150×74, 8px radius, left edges at x = 22 / 198 / 374 / 550, top y=46; fills/borders: ICQ blue `rgba(42,120,214,0.12)`/`#2a78d6`, AIM yellow `rgba(201,133,0,0.12)`/`#c98500`, MSN aqua `rgba(25,158,112,0.12)`/`#199e70`, Yahoo magenta `rgba(213,81,129,0.12)`/`#d55181`. Each box: bold 13px name + year on line 1 ("ICQ · 1996", "AIM · 1997", "MSN · 1999", "Yahoo · 1998"), 12px protocol on line 2 ("proprietary", "OSCAR", "MSNP", "YMSG"), and three small filled dots (4px radius, island color) under the text for its users.
- **No-crossing marks:** between adjacent islands, at y≈83, a short 2px `#e74c3c` broken line with a small red "×" (bold 14px) at its middle — three gaps, three ×'s.
- **Bridge client (bottom center):** rounded box ~210×48 centered at x=360, y=234, mute fill `rgba(107,114,128,0.12)` with 2px ink `#1a5276` border; two 12px lines: "Trillian / Pidgin" / "speaks all four protocols".
- **Bridge links:** dashed (6,4) 2px lines in each island's color from the client box top edge up to each island's bottom edge.
- **Annotation (bold 12px orange `#d95926`, centered at x=360, y=288):** "bridged at the client by reverse engineering — never between servers".

## XMPP: Instant Messaging as an Open Standard

**Tags:** `open standard` (green), `federation` (blue), `XML` (orange)

- **Jabber 1999** — Jabber launched as an open-source IM server in 1999 and was standardized by the IETF as XMPP in 2004.
- **XML stanzas** — messages, presence, and queries are small XML fragments streamed over one long-lived TCP connection.
- **Email-style address** — an address is user@server, so your identity belongs to a domain, not to one vendor's network.
- **Federation** — any XMPP server can open a connection to any other, exactly the way email servers exchange mail.
- **XEP extensions** — group chat, delivery receipts, and richer presence arrived as published XEPs, not vendor hacks.

*Example (italic):* alice@a.com messages bob@b.org and the two servers simply connect to each other — no single company owns both accounts.

**Key point:** XMPP kept the buddy-list machine — presence, push, server relay — and removed the vendor: an open wire format plus email-style federation means no one company owns the network.

### Visualization (canvas `c3`, 720×300)

Protocol ladder of a federated XMPP conversation: four lifelines (Alice's client, server a.com, server b.org, Bob's client) with stanzas flowing across.

- **Title (bold 15px, ink `#1a5276`, top center):** "alice@a.com → bob@b.org: One Message Across Two Servers".
- **Lifelines:** vertical 1.5px lines from y=70 to y=262 at x = 95 / 275 / 455 / 635; header labels (bold 12px) above each at y=52: "Alice's client" (blue `#2a78d6`), "server a.com" (violet `#4a3aa7`), "server b.org" (violet), "Bob's client" (green `#008300`), each with a 12px `#6b7280` sub-label at y=66 where useful ("alice@a.com" under the first, "bob@b.org" under the last).
- **Ladder arrows (2.5px, arrowheads, monospace 11-12px labels above each):**
  1. y=96, Alice → a.com, blue: label `<stream:stream to="a.com"> + auth` (12px monospace, `#2c3e50`).
  2. y=130, Alice → a.com, blue: label `<presence/>  ("I'm online")`.
  3. y=157, a.com → Alice (short return arrow), aqua `#199e70`: label `contacts' presence pushed back` (11px, `#199e70`).
  4. y=192, Alice → a.com, orange `#d95926`: label `<message to="bob@b.org">hi</message>`.
  5. y=224, a.com → b.org, magenta `#d55181`, slightly thicker (3px): label bold `server-to-server: federation, like email` (bold 12px magenta).
  6. y=252, b.org → Bob, green `#008300`: label `push down Bob's open stream`.
- **Annotation (bold 12px ink `#1a5276`, bottom center, y=288):** "same long-lived connection as AIM — but the wire format is public and the servers interconnect".

## Federation's Rise and Retreat

**Tags:** `adoption` (green), `retreat` (red), `network effects` (blue)

- **Google Talk 2005** — Google built its messenger on XMPP and switched on server-to-server federation in 2006.
- **Facebook too** — Facebook Chat exposed an XMPP interface in 2010, so any standard client could sign in.
- **The retreat** — Google replaced Talk with Hangouts in 2013 and dropped federation; Facebook closed its XMPP API in 2015.
- **Why the walls returned** — once a vendor owns the users, network effects pay better than openness ever did.
- **The afterlife** — XMPP shaped WhatsApp's wire protocol and still runs today in niche federated servers.

*Example (italic):* In 2012 a Google Talk user could chat with a friend on a self-hosted Jabber server; by 2014 that same conversation was impossible.

**Key point:** Open federation lost on incentives, not on technology — when one vendor owns enough of the users, closing the door costs it nothing and locks everyone else out.

### Visualization (canvas `c4`, 720×300)

Gantt-style timeline, 1996–2015: one row per network, bar segments colored by protocol openness (proprietary / open XMPP / closed again).

- **Title (bold 15px, ink `#1a5276`, top center):** "Protocol Openness by Network, 1996–2015".
- **Time axis:** x maps 1996→2016 onto x=150→695; 12px `#444` tick labels at 1996 / 2000 / 2004 / 2008 / 2012 / 2016 along y=252, with 1px `#e5e9ef` vertical gridlines from y=54 to y=244.
- **Rows (bars 22px tall, row labels right-aligned 12px `#2c3e50` at x=142):** row centers y = 68 / 104 / 140 / 176 / 212.
  1. "ICQ" — orange `rgba(217,89,38,0.55)` bar 1996→2016 (proprietary throughout).
  2. "AIM" — orange bar 1997→2016.
  3. "MSN Messenger" — orange bar 1999→2013, tiny 11px `#6b7280` note at bar end: "→ Skype".
  4. "Google Talk" — green `rgba(0,131,0,0.55)` bar 2005→2013, then mute gray `rgba(107,114,128,0.55)` bar 2013→2016, 11px `#6b7280` note "Hangouts" inside the gray segment.
  5. "Facebook Chat" — orange bar 2008→2010, green bar 2010→2015, then a 2px red `#e74c3c` end tick at 2015 with 11px red note "XMPP off".
  - Also a dashed green marker line (2px, dash 5,4) at x(2004) from y=54 to y=244 with vertical-ish bold 11px green label "XMPP becomes IETF standard · 2004" placed beside it at the top.
- **Legend (12px, top right area under the title, y=40):** three swatches 12×12 with labels — orange "proprietary", green "open XMPP", gray "closed again".
- **Annotation (bold 13px red `#e74c3c`, centered near x=422, y=282):** "openness peaked around 2010 — then the walls returned".
- **Caption (12px `#444`, bottom right, y=296):** "bar spans approximate; openness classification per public protocol history".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links, no cross-page links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms). Shared helpers: `roundedRect(ctx,x,y,w,h,r)` and `arrow(ctx,x1,y1,x2,y2,color,width)`.
- **Chart palette object:** `P = { blue:'#2a78d6', green:'#008300', magenta:'#d55181', yellow:'#c98500', aqua:'#199e70', orange:'#d95926', violet:'#4a3aa7', ink:'#1a5276', text:'#2c3e50', mute:'#6b7280', grid:'#e5e9ef' }`. Red `#e74c3c` used only for the blocking "×" marks and the "XMPP off" / walls-returned notes (genuine closure/error states).
- **Data:** no randomness anywhere; the 60-watcher fan-out count is invented and labeled illustrative; product launch years (ICQ 1996, AIM 1997, Yahoo 1998, MSN 1999, Jabber 1999, XMPP RFC 2004, Google Talk 2005, federation 2006, Facebook XMPP 2010, Hangouts 2013, Facebook XMPP shutdown 2015) and the 1999 AOL/Microsoft blocking war are public tech history; timeline bar spans are approximate and captioned as such.
- **Framing:** organized by technology, not vendor — the four proprietary networks are presented as one shared design; XMPP is the era's distinct technology because it was open and federated. Neutral factual tone about real historical products.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
