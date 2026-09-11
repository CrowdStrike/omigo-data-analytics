# Team Chat Workspace

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Team Chat Workspace

**Subtitle:** Every open client holds one live WebSocket per workspace — the hard parts are booting a session and telling everyone who's online, not moving the words

## One Socket per Person per Workspace

**Tags:** `core idea` (blue), `WebSockets` (green), `session state` (orange)

- **The socket** — every open client keeps one persistent WebSocket per workspace it shows
- **The multiplier** — Maya's laptop shows two workspaces and her phone one: that's three open sockets
- **The push** — new messages, edits, typing, and presence all arrive as events down that socket
- **The boot** — before the socket is useful, the client must know its channels, membership, unreads
- **The split** — history lives in a plain database; the socket only carries what changes right now

*Example (italic):* Maya opens her laptop at 9am: two WebSockets dial out, and every message she sees for the rest of the day is a small event pushed down one of them.

**Key point:** A team-chat client is a stateful session over a persistent socket — the design question is how much workspace state the client needs before that socket is worth anything.

### Visualization (canvas `c1`, 720×300)

Flow diagram: one person's three clients on the left, each holding a WebSocket into a gateway, which fans to the three services behind it.

- **Title (bold 15px, `#1a5276`, top center):** "One Person, Three Sockets: What Each Connection Carries".
- **Left column (blue `rgba(42,120,214,0.15)` rounded boxes, 170px wide, 40px tall, 8px radius, left edge x=30, centered on y=95 / 165 / 235):** "laptop — workspace A", "laptop — workspace B", "phone — workspace A"; 12px `#2c3e50` text.
- **Gateway (mute `rgba(107,114,128,0.12)` box, 180px wide, 60px tall, left edge x=280, centered on y=165):** "WebSocket gateway — one session per socket"; 3px `#6b7280` arrows from each client box to its left edge, each arrow labeled 11px `#6b7280` "WebSocket".
- **Right column (boxes 190px wide, 44px tall, left edge x=510, centered on y=95 / 165 / 235):** green `rgba(0,131,0,0.12)` "channel servers — real-time push"; violet `rgba(74,58,167,0.12)` "presence — who is online"; blue `rgba(42,120,214,0.15)` "message DB — history & unreads"; 3px `#6b7280` arrows from the gateway to each.
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=280):** "sockets scale with clients × workspaces, not with people".
- **Caption (12px `#444`, bottom right):** "schematic; components as publicly blogged".

## The 9:02am Thundering Reconnect

**Tags:** `worked example` (blue), `session boot` (green), `lazy loading` (orange)

- **The blip** — an office network hiccup (or a server deploy) drops 8,000 clients at 9:02am
- **The redial** — all 8,000 reconnect within 60 seconds, and each one asks to boot its session
- **The eager way** — the old boot call returned everything: channels, membership, unreads, ~4 MB each
- **The meltdown** — 8,000 × 4 MB = 32 GB demanded in one minute, exactly when servers are busiest
- **The lazy way** — connect with a ~8 KB skeleton (64 MB total), fetch each channel's state when opened
- **The blog** — team-chat vendors documented this evolution: lazy-load session state on demand at the edge

*Example (italic):* The 9:02am storm asks the boot service for 32 GB the eager way but only 64 MB the lazy way — the remaining state trickles in as people actually open channels.

**Key point:** Session establishment is the scaling cliff of team chat — a mass reconnect multiplies the boot payload by every client at once, so the fix is to stop front-loading state and fetch it lazily.

### Visualization (canvas `c2`, 720×300)

Horizontal bar chart: total data requested by 8,000 reconnecting clients in the first minute, eager boot vs lazy connect, plus the on-demand follow-up spread over later minutes.

- **Title (bold 15px, `#1a5276`, top center):** "8,000 Clients Reconnect in One Minute: Eager vs Lazy Boot".
- **Axis:** vertical 2px `#999` baseline at x=250, bars extend right, max width 420; log-feel achieved by hardcoded pixel widths, not a real log axis.
- **Rows (bars 18px tall, centered on y = 90, 155, 220), each with a left-aligned 12px `#444` label at x=20:**
  - "eager boot — 8,000 × 4 MB": red `#e74c3c` bar width 420, fill `rgba(231,76,60,0.25)`, 2px red edge, bold 12px red value label "32 GB in 1 min" at the bar end
  - "lazy connect — 8,000 × 8 KB": green `#008300` bar width 26, fill `rgba(0,131,0,0.25)`, 2px green edge, bold 12px green label "64 MB — 500× less"
  - "on-demand fetches, next 10 min": orange `#d95926` bar width 120, fill `rgba(217,89,38,0.2)`, 2px orange edge, 12px `#444` label "~1.6 GB as channels open"
- **Annotation (bold 13px green `#008300`, right side near y=265):** "the deploy no longer melts the boot service".
- **Caption (12px `#444`, bottom right):** "payload sizes illustrative, bar lengths not to scale; the eager-to-lazy shift is publicly blogged".

## A Channel Is a Pub/Sub Topic

**Tags:** `fan-out` (blue), `channel servers` (green), `offline path` (orange)

- **The topic** — each channel maps to a channel server; posting publishes one event to that topic
- **The fan-out** — #general has 2,000 members, 1,400 connected: one post becomes 1,400 socket pushes
- **The offline 600** — disconnected members get a mobile push only for mentions and DMs, not every post
- **The catch-up** — on reconnect a client asks "what changed since my last event?" and fills the gap
- **The history** — the post also lands in the message database; scrollback is an ordinary paged query

*Example (italic):* Maya posts "standup in 5" to #general: 1,400 sockets light up within a second, and the 600 offline members simply find it in their unreads whenever they next connect.

**Key point:** Real-time delivery is a pub/sub problem keyed by channel and scoped to whoever is connected right now — everyone else is served by the normal database on their own schedule.

### Visualization (canvas `c3`, 720×300)

Flow diagram: one post entering the #general channel server, splitting into the connected fan-out path, the offline notification path, and the plain history write.

- **Title (bold 15px, `#1a5276`, top center):** "One Post to #general (2,000 Members): Three Paths Out".
- **Sender (blue `rgba(42,120,214,0.15)` rounded box, 170px wide, 44px tall, 8px radius, left edge x=25, centered on y=95):** "Maya's client — posts to #general"; 12px `#2c3e50` text.
- **Channel server (violet `rgba(74,58,167,0.12)` box, 180px wide, 50px tall, left edge x=250, centered on y=95):** "channel server — pub/sub topic for #general"; 3px `#6b7280` arrow from the sender.
- **Connected path (green `rgba(0,131,0,0.12)` box, 210px wide, 44px tall, left edge x=490, centered on y=60):** "1,400 connected — one push per open socket"; 3px `#008300` arrow from the channel server.
- **Offline path (orange `rgba(217,89,38,0.12)` box, 210px wide, 44px tall, left edge x=490, centered on y=140):** "600 offline — mobile push if mentioned; catch-up on reconnect"; 3px `#d95926` arrow from the channel server.
- **History path (blue box, 210px wide, 44px tall, left edge x=250, centered on y=215):** "message DB — ordinary insert; scrollback = paged query"; 3px `#2a78d6` arrow straight down from the channel server.
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=280):** "real-time is the special path; history is a normal database problem".
- **Caption (12px `#444`, bottom right):** "member counts illustrative".

## Presence Costs More Than the Messages

**Tags:** `common mistake` (red), `presence` (orange)

- **The dot** — the green "online" dot looks free, but every dot is a live event someone must be sent
- **The blowup** — naively, each of N members flipping online/offline broadcasts to all N−1 others
- **The math** — at 40 flips/member/day, 10,000 members naively generate 4.0 billion events a day
- **The fix** — clients subscribe only to the ~30 users visible on screen, publicly documented by vendors
- **The batch** — updates are throttled and coalesced into one batched presence message, not one per flip
- **The mistake** — designing fan-out for messages and letting presence ride along unthrottled

*Example (italic):* A 10,000-member workspace's messages are a few hundred events a second, while naive presence alone would demand 4.0 billion events a day — 333× more than the 12 million a subscription model needs.

**Common mistake:** Treating presence like just another message. Messages fan out to one channel's members; presence is an N×M broadcast across the whole workspace, and it must be subscription-scoped, throttled, and batched or it becomes the biggest traffic source in the system.

### Visualization (canvas `c4`, 720×300)

Grouped horizontal bar chart: who-is-online events per day at three workspace sizes, naive broadcast-to-everyone vs subscribe-to-visible (~30 watched users, batched), assuming 40 online/offline flips per member per day.

- **Title (bold 15px, `#1a5276`, top center):** "Presence Events per Day: Broadcast to All vs Subscribe to Visible".
- **Axis:** vertical 2px `#999` baseline at x=250, bars extend right, max width 420; log-feel achieved by hardcoded pixel widths, not a real log axis.
- **Groups (bars 14px tall, group labels 12px `#444` at x=20; naive bar / subscribe bar centered on y = 62 & 84, 132 & 154, 202 & 224):**
  - "100 members": naive blue `#2a78d6` bar width 150 with 11px label "396K", subscribe green `#008300` bar width 110 with 11px label "120K"
  - "1,000 members": naive blue bar width 290 labeled "40.0M", subscribe green bar width 195 labeled "1.2M"
  - "10,000 members": naive red `#e74c3c` bar width 420 with bold 12px red label "4.0B", subscribe green bar width 260 labeled "12M"
- **Bar fills:** naive `rgba(42,120,214,0.30)` (red row `rgba(231,76,60,0.25)`), subscribe `rgba(0,131,0,0.25)`, 2px matching edges.
- **Annotation (bold 13px magenta `#d55181`, upper right near x=430, y=40):** "at 10,000 members: 333× fewer events".
- **Caption (12px `#444`, bottom right):** "40 flips/member/day and 30 watched users illustrative; arithmetic exact".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded values above (no randomness); client counts, boot payload sizes (4 MB / 8 KB), channel membership (2,000 / 1,400 / 600), and presence assumptions (40 flips/day, 30 watched users) are invented and labeled illustrative; the presence arithmetic (40 × N × (N−1) naive vs 1,200 × N subscribed; 396K/120K, 40.0M/1.2M, 4.0B/12M) is exact given those assumptions, and 32 GB vs 64 MB follows exactly from 8,000 clients × the stated payloads.
- **Framing:** treat the whole page as a generic system-design exercise built from public team-chat engineering blog posts — the eager-to-lazy session-boot evolution, channel-server pub/sub fan-out, and the presence subscription model are publicly documented; make no claims about current internal systems beyond that.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
