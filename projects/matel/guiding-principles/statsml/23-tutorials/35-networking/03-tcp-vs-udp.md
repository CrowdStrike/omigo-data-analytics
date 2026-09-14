# TCP vs UDP

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** TCP vs UDP

**Subtitle:** TCP sends every packet like registered mail — numbered, receipted, resent if lost; UDP sends postcards — fast, cheap, and no promises

## Registered Mail vs a Stack of Postcards

**Tags:** `core idea` (blue), `guaranteed delivery` (green), `best effort` (orange)

- **The contract** — a 10-page contract goes out as ten numbered envelopes, each sent registered mail
- **The receipts** — every envelope that arrives triggers a receipt back; a missing receipt means resend that page
- **The order** — the pages are reassembled 1 through 10 on arrival, even if the mail truck shuffled them
- **The postcards** — ten vacation postcards drop into the same mailbox: no numbers, no receipts, no resends
- **The names** — the number-receipt-resend scheme is TCP; the fire-and-forget postcard scheme is UDP

*Example (italic):* Envelope 2 vanishes in transit; its missing receipt makes the sender mail page 2 again — a lost postcard is simply never heard of.

**Key point:** TCP numbers every packet, waits for acknowledgments, resends what's missing, and hands data over in order; UDP sends each packet once and promises nothing.

### Visualization (canvas `c1`, 720×300)

Two-row flow diagram: registered envelopes with receipt arrows coming back (one lost, then resent) vs postcards with no return path (one lost, silently).

- **Title (bold 15px, `#1a5276`, top center):** "Registered Mail Has a Receipt Loop; Postcards Have Nothing".
- **Rows:** sender box at x=40 and receiver box at x=590 on each row (110×40, 8px radius, fill `rgba(42,120,214,0.15)`, 12px `#2c3e50` labels "sender" / "receiver"); row 1 center y=105 labeled 12px `#444` "TCP — registered" at x=40 above the row; row 2 center y=225 labeled "UDP — postcards".
- **Row 1 forward arrows:** three 3px blue `#2a78d6` arrows (envelopes "1", "2", "3" labeled 12px) from x=160 to x=580 at y=90/105/120; envelope "2" arrow stops at x=380 with a bold 13px red `#e74c3c` "✗ lost"; a fourth blue arrow below at y=138 labeled "2 (resent)".
- **Row 1 receipt arrows:** dashed (dash 4/3) 2px green `#008300` arrows pointing back for 1, 3, and the resent 2, 11px green labels "got 1" / "got 3" / "got 2".
- **Row 2 forward arrows:** three 3px orange `#d95926` arrows (postcards "A", "B", "C") at y=210/225/240; postcard "B" stops at x=380 with a bold 13px red "✗ lost"; no return arrows at all.
- **Annotation (bold 13px violet `#4a3aa7`, x≈300, y=270):** "the receipt loop is the whole difference".
- **Caption (12px `#444`, bottom right):** "schematic — three packets shown per row".

## Ten Packets, Two Dropped: What Each Protocol Does

**Tags:** `worked example` (blue), `retransmission` (green)

- **The photo** — a 10 KB photo leaves as ten 1 KB packets, one every 10 ms
- **The drops** — the network loses packets 4 and 7: a 20% loss rate on this send
- **UDP's answer** — 8 packets arrive by 100 ms; the sender never learns that two are gone
- **TCP's answer** — missing acknowledgments for 4 and 7 trigger resends; they land at 250 and 300 ms
- **Hand-check** — UDP: 8/10 = 80% delivered at 100 ms; TCP: 10/10 = 100% delivered at 300 ms, 3× longer

*Example (italic):* Over TCP the photo opens perfectly at 300 ms; over UDP it arrives at 100 ms with two 1 KB stripes missing forever.

**Key point:** TCP trades time for certainty — each loss costs a resend round; UDP hands over whatever survived, immediately, and says nothing about the rest.

### Visualization (canvas `c2`, 720×300)

Step chart of packets delivered over time: UDP flattens at 8 of 10 by 100 ms; TCP catches the two losses and steps up to 10 by 300 ms.

- **Title (bold 15px, `#1a5276`, top center):** "Ten Packets, Two Dropped: UDP Stops at 8, TCP Finishes at 10".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = time 0 to 300 ms with 12px `#444` tick labels every 50 ms; y = packets delivered 0 to 10, gridlines `#e5e9ef` at 2/4/6/8.
- **UDP step line:** blue `#2a78d6` 3px through times `[0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 300]` ms, delivered `[0, 1, 2, 3, 3, 4, 5, 5, 6, 7, 8, 8]` — flat at 8 after 100 ms; 12px blue label "UDP: done at 8" near (180 ms, 8).
- **TCP step line:** green `#008300` 3px through times `[0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 250, 300]` ms, delivered `[0, 1, 2, 3, 3, 4, 5, 5, 6, 7, 8, 9, 10]` — two late steps as the resends arrive.
- **Loss markers:** bold 13px red `#e74c3c` "✗" at t=40 and t=70 just above the lines, 12px red label "packets 4 and 7 dropped" near (55 ms, y for 1.5).
- **Annotation (bold 13px green `#008300`, near 190 ms, y for 9.6):** "TCP resends: 100% delivered, 3× the time".
- **Caption (12px `#444`, bottom right):** "timings illustrative — 10 ms per packet, resends land at 250 and 300 ms".

## Picking the Protocol: Downloads, Dashboards, and Video Calls

**Tags:** `where it's used` (blue), `latency vs loss` (green)

- **Downloads and APIs** — file transfers, web requests, and database connections ride TCP: every byte must arrive
- **Streams of moments** — video calls and game position updates ride UDP: a late packet is worthless anyway
- **DNS lookups** — one tiny question over UDP; re-asking is cheaper than setting up a connection
- **The data scientist** — statsd-style metric counters ship over UDP so instrumenting code never slows the app
- **The trade** — TCP charges latency for its guarantees; UDP charges you the cleanup for anything missing

*Example (italic):* A dashboard that drops one CPU reading out of a thousand loses nothing; a model file that drops one packet out of a thousand is corrupt.

**Key point:** Ask which is worse for the application — a lost piece or a late piece. Lost-is-worse points to TCP; late-is-worse points to UDP.

### Visualization (canvas `c3`, 720×300)

Scatter quadrant placing common workloads by two costs: how bad a lost piece is (x) vs how bad waiting for it is (y); TCP workloads cluster right, UDP workloads cluster left.

- **Title (bold 15px, `#1a5276`, top center):** "Two Costs Decide the Protocol: Losing a Piece vs Waiting for It".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; 12px `#6b7280` axis labels "cost of a lost piece →" centered below the baseline and "cost of waiting →" rotated along the left edge; no tick numbers (qualitative axes).
- **Divider:** vertical dashed `#6b7280` (dash 4/3) line at pixel x=390; bold 13px green `#008300` label "UDP territory" at (120, 55); bold 13px blue `#2a78d6` label "TCP territory" at (490, 55).
- **TCP points (7px filled circles, blue `#2a78d6`, 12px `#2c3e50` labels beside):** "file download" at (620, 215), "API call" at (565, 150), "database replication" at (630, 115).
- **UDP points (7px filled circles, green `#008300`):** "video call" at (160, 80), "game position updates" at (235, 95), "DNS lookup" at (250, 170), "live metrics (statsd)" at (140, 200).
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=272):** "ask: is a lost piece or a late piece worse?".
- **Caption (12px `#444`, bottom right):** "positions qualitative, illustrative".

## Reliable Is Not Always Better

**Tags:** `common mistake` (red), `head-of-line blocking` (orange)

- **The reflex** — "guaranteed delivery must be better" ignores what the guarantee costs on live data
- **Stale data** — a video frame is due every 40 ms; a retransmitted frame arriving after its slot is garbage
- **In-order delivery** — TCP releases bytes strictly in order, so frames 4–6 wait behind the resent frame 3
- **The freeze** — one lost packet becomes a 120 ms video freeze on TCP, a single 40 ms glitch on UDP
- **Not lawless** — apps on UDP add back only the reliability they need (QUIC is built exactly this way)

*Example (italic):* Frame 3 is lost and resent, arriving at 240 ms — 120 ms past its 120 ms deadline — while frames 4, 5, and 6 sit blocked behind it.

**Common mistake:** Using TCP for real-time streams because "reliable sounds safer" — in-order retransmission turns one lost packet into a visible multi-frame freeze.

### Visualization (canvas `c4`, 720×300)

Two-row frame-timeline diagram: six video frames due every 40 ms; over TCP the lost frame 3 blocks frames 4–6 until its resend lands; over UDP frame 3 is skipped and the rest play on time.

- **Title (bold 15px, `#1a5276`, top center):** "One Lost Frame: TCP Freezes the Video, UDP Glitches One Frame".
- **Layout:** six frame boxes per row (70×34, 8px radius, 12px `#2c3e50` text "f1".."f6") at x = 110, 200, 290, 380, 470, 560; row 1 boxes centered at y=95, row 2 at y=205; 12px `#444` row labels at x=20: "over TCP" and "over UDP".
- **Row 1 (TCP):** f1, f2 fill `rgba(0,131,0,0.12)` with green `#008300` border (on time); f3 fill `rgba(231,76,60,0.12)` with 2px red `#e74c3c` border, 11px red label "resent — lands 240 ms" below; f4, f5, f6 fill `rgba(217,89,38,0.12)` with orange `#d95926` border, 11px orange label "blocked" below each; bold 12px red bracket label "120 ms freeze" spanning f3–f6 above the row.
- **Row 2 (UDP):** f1, f2, f4, f5, f6 green-bordered (on time); f3 drawn with a dashed 2px `#6b7280` border and 11px mute label "skipped"; bold 12px green `#008300` label "one 40 ms glitch" above f3.
- **Time axis:** thin 2px `#999` line at y=260 with 12px `#444` tick labels "40" "80" "120" "160" "200" "240 ms" under the six box columns.
- **Annotation (bold 13px orange `#d95926`, centered near y=285):** "in-order delivery turns one loss into four late frames".
- **Caption (12px `#444`, bottom right):** "40 ms per frame (25 fps), illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); the packet send (10 packets, 10 ms apart, packets 4 and 7 dropped, resends landing at 250 and 300 ms), the frame timeline (40 ms per frame, frame 3 resent at 240 ms, 120 ms freeze), and the scatter positions are all invented and labeled illustrative; 8/10 = 80% and 10/10 = 100% are exact arithmetic on those invented counts.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
