# The Terminal Era

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** The Terminal Era

**Subtitle:** How the first online conversations worked — characters echoed between terminals, boards reached by phone call, and chat rooms relayed across servers

## Talking Through the Terminal

**Tags:** `core idea` (blue), `same machine first` (green), `1971-1983` (orange)

- **write** — Your typed lines landed directly on another logged-in user's terminal on the same machine.
- **wall** — One message was broadcast onto every logged-in terminal at once, mid-edit or not.
- **talk (1983)** — It split the screen in two and echoed each keystroke to the other side as you typed.
- **ntalk** — It crossed machines: a UDP lookup found the other user, then one TCP session carried the keys.
- **No history** — Nothing was stored and nothing waited; both people had to be logged in at the same time.

*Example (italic):* Two students on the same VAX watch each other's typos appear and get backspaced away, live, character by character.

**Key point:** The first chat was terminal-to-terminal keystroke echo — synchronous, unstored, and gone the moment either side logged out.

### Visualization (canvas `c1`, 720×300)

Two panels split by a dashed vertical divider at x=340: a split-screen `talk` terminal mock on the left, a protocol ladder for `ntalk` on the right.

- **Title (bold 15px, ink `#1a5276`, top center):** "talk: a Split Screen of Live Keystrokes".
- **Left panel — terminal mock:** dark rounded rect x=22..320, y=44..252 (fill `#20262e`, 6px radius). Inside, 12px monospace text: top line aqua `#199e70` "[talk] Connection established."; a dashed mute `#6b7280` horizontal divider across the middle of the rect; upper half shows alice's side, green `#7ec699`-ish (use `#27ae60`-adjacent on dark: plain `#8fd19e`) lines "are you still in the lab?" and "my build just died"; lower half shows bob's side in light blue `#9ec5f0` lines "yes, come over" and "watch me typ" followed by a solid cursor block. Caption under the rect, 12px `#444`, centered at x=171: "every keystroke echoed live — typos and all".
- **Right panel — ntalk ladder:** three lifelines, bold 12px ink labels at y=58 with vertical mute 1px lines down to y=240: "alice's talk" x=400, "talkd (daemon)" x=530, "bob's terminal" x=660. Arrows (3px, arrowheads) top to bottom with bold 11-12px labels above each: (1) y=100 alice→talkd, blue `#2a78d6`, "UDP: announce invite"; (2) y=136 talkd→bob, orange `#d95926`, "ring: talk requested"; (3) y=172 bob→talkd, blue, "bob answers: UDP lookup"; (4) y=214 alice↔bob double-headed, green `#008300` 4px, "direct TCP — keystrokes both ways".
- **Annotation (bold 12px violet `#4a3aa7`, centered x=530, y=250):** "UDP to find each other, TCP to chat".

## Boards You Dialed Into

**Tags:** `store-and-forward` (blue), `dial-up` (orange), `1978-1984` (green)

- **CBBS (1978)** — The first board was one hobbyist's computer in Chicago answering its own phone line.
- **One line** — A single modem meant one caller at a time; everyone else heard a busy signal.
- **The visit** — You dialed in, read the boards, left replies, and hung up so the next caller could connect.
- **FidoNet (1984)** — Boards phoned each other overnight in "mail hour" and passed message bundles hop by hop.
- **Usenet (1980)** — Newsgroup posts flooded server to server over UUCP, later NNTP; every server kept a full copy.
- **Propagation** — A post crossed the network in scheduled hops, reaching distant servers in hours or days.

*Example (italic):* A question posted on a Chicago board on Monday night reaches a Seattle board on Wednesday, after two overnight relay calls.

**Key point:** Store-and-forward on a schedule — messages moved when the machines phoned each other, not when you pressed send.

### Visualization (canvas `c2`, 720×300)

Two rows: a FidoNet overnight hop chain on top, a Usenet flood diagram below.

- **Title (bold 15px, ink `#1a5276`, top center):** "Store-and-Forward: Messages Move When the Machines Call".
- **Row 1 (FidoNet), 12px `#444` label "FidoNet (1984): overnight relay, hop by hop" at x=22, y=52:** four rounded boxes (130px wide, 40px tall, 8px radius) centered on y=92, left edges at x=30 / 200 / 370 / 540: blue `rgba(42,120,214,0.15)` box "Chicago BBS / message posted Mon"; two mute `rgba(107,114,128,0.12)` boxes "St. Louis hub / relays the bundle" and "Denver hub / relays the bundle"; green `rgba(0,131,0,0.12)` box "Seattle BBS / read Wednesday". Boxes joined by 3px mute arrows; bold 11px orange `#d95926` call-time labels under each arrow at y=126: "night 1 · 2:00am", "night 1 · 3:40am", "night 2 · 2:10am".
- **Row 2 (Usenet), 12px `#444` label "Usenet (1980): flood a copy to every server" at x=22, y=168:** blue box "origin server / new post" left edge x=30, centered y=215; 3px mute arrows fanning to two mute boxes at x=250, cy=192 "univ. server / full copy" and x=250, cy=240 "ISP server / full copy"; from each, an onward arrow to a violet `rgba(74,58,167,0.12)` box at x=470, cy=215, 200px wide, "…every server / keeps everything".
- **Annotation (bold 12px magenta `#d55181`, right-aligned near x=690, y=126):** "posted Monday, read Wednesday".
- **Caption (12px `#444`, bottom right):** "relay times illustrative".

## IRC: Rooms Relayed Across a Server Tree

**Tags:** `real-time` (blue), `plain text` (green), `netsplit` (red)

- **IRC (1988)** — A client held one plain-text TCP connection and sent typed lines like JOIN and PRIVMSG.
- **Spanning tree** — Servers linked into a tree and relayed each channel line to every server with members.
- **Hand-typable** — The protocol was simple enough to speak by hand over a raw telnet session to port 6667.
- **Netsplit** — When one server-to-server link dropped, the tree partitioned and half the channel vanished.
- **No accounts** — There was no login and no offline delivery; if you were disconnected, you simply missed it.

*Example (italic):* Forty nicknames quit the channel in the same second — not a mass exit, just one broken link between two servers.

**Key point:** One relay tree gave planet-wide chat rooms in 1988, but any single broken link split every channel on the network in half.

### Visualization (canvas `c3`, 720×300)

An IRC server spanning tree with one inter-server link broken, partitioning the channel into two shaded halves.

- **Title (bold 15px, ink `#1a5276`, top center):** "IRC Servers Form a Tree — One Broken Link Splits Every Channel".
- **Left partition shade:** rounded rect x=35..320, y=55..235, fill `rgba(42,120,214,0.08)`, bold 12px blue `#2a78d6` label at top left inside (x=50, y=75): "#chat — 61 users".
- **Right partition shade:** rounded rect x=400..690, y=55..235, fill `rgba(217,89,38,0.10)`, bold 12px orange `#d95926` label at top right inside (right-aligned x=675, y=75): "#chat — 39 users".
- **Servers (13px-radius circles, bold 11px white two-letter labels):** left partition, blue `#2a78d6`: S1 (210,110), S2 (100,150), S3 (270,185); right partition, orange `#d95926`: S4 (480,110), S5 (560,185), S6 (645,130). Tree links (2px mute `#6b7280` lines): S1–S2, S1–S3, S4–S5, S4–S6. Small client dots (4px, matching partition color at 55% alpha) hanging off each server, 2-3 per server, connected by 1px light lines.
- **Broken link:** dashed red `#e74c3c` 3px line S1–S4 with a bold red 16px "✕" at its midpoint (345,110) and bold 12px red label "link drops" at (345,88).
- **Annotation (bold 12px red `#e74c3c`, centered x=360, y=262):** "netsplit: each side sees the other's users quit at once".
- **Caption (12px `#444`, bottom right):** "user counts illustrative".

## What the Era Invented — and What It Lacked

**Tags:** `legacy` (blue), `still with us` (green), `missing pieces` (red)

- **Presence** — Knowing who is online began with finger and /who, and survives as today's green dot.
- **Channels** — IRC's #channels are the direct ancestors of the rooms in every modern team chat.
- **Store-and-forward** — FidoNet's overnight relays prefigure every offline message queue running today.
- **@nick and bots** — Calling someone by nick and scripting bot users both became habits on IRC first.
- **The gaps** — The era had no real identity, no offline delivery for chat, and no encryption anywhere.

*Example (italic):* A 1988 IRC habit — typing someone's nick to get their attention — survives as the @-mention in every modern messenger.

**Key point:** The terminal era invented presence, rooms, relays, and bots; identity, offline delivery, and encryption had to wait for later eras.

### Visualization (canvas `c4`, 720×300)

A 1971-1995 timeline with two horizontal lanes: real-time systems on top, store-and-forward systems below.

- **Title (bold 15px, ink `#1a5276`, top center):** "1971-1995: Two Families of Messaging".
- **Axis:** year scale 1971→1995 mapped to x=80→680; axis line `#999` at y=245 with 12px `#444` tick labels at 1971 / 1975 / 1980 / 1985 / 1990 / 1995; light grid `#e5e9ef` verticals from y=60 to y=245 at each tick.
- **Lane 1 (real-time), guide line `#e5e9ef` at y=115, bold 12px blue `#2a78d6` label "real-time" at x=80 left-aligned, y=95:** blue dots (6px radius) on the lane at talk 1983 and IRC 1988; bold 12px blue name labels above each dot ("talk", "IRC"), 11px `#444` year labels below each dot ("1983", "1988").
- **Lane 2 (store-and-forward), guide line at y=185, bold 12px orange `#d95926` label "store-and-forward" at x=80, y=165:** orange dots at email 1971, CBBS 1978, Usenet 1980, FidoNet 1984; same label pattern (names above, years below); to avoid collision, Usenet's name label sits below its year label instead of above the dot.
- **Dotted mute vertical connectors** from each dot down to the axis at y=245.
- **Annotation (bold 13px violet `#4a3aa7`, centered x=380, y=52):** "every modern messenger blends both lanes".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links, no links to other pages.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms). Shared `roundedRect` and `arrow` helpers.
- **Chart palette object:** `const P = { blue:'#2a78d6', green:'#008300', magenta:'#d55181', yellow:'#c98500', aqua:'#199e70', orange:'#d95926', violet:'#4a3aa7', ink:'#1a5276', text:'#2c3e50', mute:'#6b7280', grid:'#e5e9ef' }`. Navy `#1a5276` is ink for headings/axes/callout borders; red `#e74c3c` only for the genuine failure state (netsplit).
- **Data:** no randomness, no Date calls — all positions and values are the hardcoded literals above; FidoNet relay times and IRC channel user counts are invented and labeled illustrative; the historical dates (email 1971, CBBS 1978, Usenet 1980, talk 1983, FidoNet 1984, IRC 1988) are factual public history.
- **Framing:** factual tech history using real system names (CBBS, FidoNet, Usenet, IRC, talk/ntalk); Alice/Bob-style human names only as storytelling actors.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
