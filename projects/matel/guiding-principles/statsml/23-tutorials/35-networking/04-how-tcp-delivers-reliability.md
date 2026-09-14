# How TCP Delivers Reliability

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** How TCP Delivers Reliability

**Subtitle:** TCP builds a lossless byte stream on top of a network that drops packets — by numbering every byte, demanding receipts, and resending anything unacknowledged

## Numbered Cards Through a Courier That Loses Mail

**Tags:** `core idea` (blue), `sequence numbers` (green), `ACKs` (orange)

- **The recipe** — a coffee shop's HQ mails a new 6-card recipe to a branch via a lossy courier
- **The numbers** — every card carries its number, 1 through 6, so the branch can spot a hole
- **The receipt** — for each card that arrives, the branch mails back "got card N" (an ACK)
- **The timer** — HQ keeps a copy of every card and resends any card not receipted in time
- **The reorder** — cards 5 and 6 may arrive before 4; the numbers let the branch reshuffle them

*Example (italic):* Card 4 vanishes in the mail; the branch sets 5 and 6 aside, HQ resends 4, and the branch assembles cards 1–6 in perfect order.

**Key point:** TCP does exactly this with bytes: sequence numbers name the data, ACKs confirm receipt, and retransmission fills the holes — a reliable stream built on an unreliable courier.

### Visualization (canvas `c1`, 720×300)

Message sequence diagram between two lifelines: six numbered cards travel left to right, card 4 is lost mid-flight, receipts flow back, and a resend closes the gap.

- **Title (bold 15px, `#1a5276`, top center):** "Six Numbered Cards, One Lost, One Resent — the Whole Trick".
- **Lifelines:** vertical 2px `#1a5276` lines at x=150 and x=570 from y=60 to y=265; bold 13px `#1a5276` centered labels "HQ (sender)" at (150, 52) and "Branch (receiver)" at (570, 52).
- **Card arrows:** 2px blue `#2a78d6` slanted arrows from (150, y) to (570, y+16) for cards 1, 2, 3 at y = 70, 94, 118, each with a 12px `#2c3e50` midpoint label "card 1" / "card 2" / "card 3".
- **Lost card:** 2px orange `#d95926` arrow from (150, 142) to (360, 152), stopping mid-canvas, ending with bold 13px orange `#d95926` label "✗ card 4 lost".
- **Cards 5 and 6:** blue arrows at y = 166 and y = 188; 12px aqua `#199e70` note "held — waiting for 4" beside the receiver lifeline at (565, 200), right-aligned.
- **ACK arrows:** dashed (4/3) 2px green `#008300` arrows right-to-left from (570, 138) to (150, 152) labeled 12px green "got 1–3, still expecting 4".
- **Resend:** 2px green `#008300` arrow from (150, 214) to (570, 230) labeled 12px green "card 4 (resent)"; dashed green ACK arrow from (570, 234) to (150, 248) labeled 12px green "got all 6".
- **Annotation (bold 13px violet `#4a3aa7`, centered near x=360, y=278):** "numbers + receipts + resend = reliability".
- **Caption (12px `#6b7280`, bottom right):** "cards illustrative — TCP numbers bytes, not cards".

## One Lost Segment, Millisecond by Millisecond

**Tags:** `worked example` (blue), `retransmission` (green), `timeout` (orange)

- **The setup** — 6 segments leave together at t=0; the one-way trip is 50 ms, so a receipt takes 100 ms
- **The loss** — segment 4 is dropped by the network at t=25; segments 5 and 6 land fine at t=50
- **The wait** — the receiver buffers 5 and 6 but keeps answering "still expecting 4"
- **The timeout** — no receipt for 4 by t=300 (the 300 ms timer), so the sender resends it
- **The finish** — the resent 4 lands at t=350; the receipt covering all 6 reaches the sender at t=400

*Example (italic):* One lost segment turned a 100 ms transfer into a 400 ms one — every byte arrived intact, just 4× later.

**Key point:** The arithmetic is hand-checkable: resend at 300, arrive at 300 + 50 = 350, final ACK at 350 + 50 = 400 — the retransmission timer, not the data size, sets the finish time.

### Visualization (canvas `c2`, 720×300)

Gantt-style timeline: one row per segment, bars from send time to acknowledgment, showing segment 4's lost first attempt, the 300 ms timer, and the late resend.

- **Title (bold 15px, `#1a5276`, top center):** "One Lost Segment: a 100 ms Transfer Becomes 400 ms".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = time 0 to 400 ms (1.5 px per ms), 12px `#6b7280` tick labels "0" / "100" / "200" / "300" / "400 ms" at x = 60, 210, 360, 510, 660, with vertical `#e5e9ef` gridlines.
- **Rows:** segments 1–6 at y = 75, 103, 131, 159, 187, 215, each with a 12px `#6b7280` label "seg 1".."seg 6" at x=15; bars 12px tall.
- **Segments 1–3:** blue `#2a78d6` bars from t=0 to t=100 (x 60 to 210); 11px `#6b7280` label "acked at 100 ms" after the seg-1 bar only.
- **Segment 4:** orange `#d95926` bar t=0 to t=25 (x 60 to 97) with bold 12px orange "✗ lost at 25 ms"; green `#008300` bar t=300 to t=400 (x 510 to 660) with 11px green label "resent 300, acked 400" above it.
- **Segments 5–6:** blue bar t=0 to t=50, then aqua `#199e70` bar t=50 to t=400; 11px aqua label "buffered at receiver until 4 arrives" on the seg-5 row.
- **Timeout marker:** vertical dashed `#6b7280` (dash 4/3) line at t=300 (x=510) from y=60 to y=245, 12px `#6b7280` label "300 ms timer fires" at its top.
- **Annotation (bold 13px magenta `#d55181`, near x=230, y=62):** "everything arrived — just 4× later".
- **Caption (12px `#6b7280`, bottom right):** "times illustrative — 50 ms one-way, 300 ms timeout".

## Where the Retransmits Show Up in Your Dashboards

**Tags:** `where it's used` (blue), `tail latency` (green)

- **The stream** — every API call, database query, and file download you run rides on TCP
- **The tail** — a small packet-loss rate shows up as rare requests taking 3–4× the median time
- **The dashboard** — p50 latency looks healthy while p99 spikes; retransmits live in that gap
- **The download** — a 10 GB training-data pull survives thousands of drops without one corrupt byte
- **The diagnosis** — a flat median plus a spiky tail on one network path smells like retransmission

*Example (italic):* 18 of 20 identical API calls return in about 100 ms; the 2 that hit a retransmit take about 400 ms — same server, same query.

**Key point:** TCP hides loss from your data but not from your clock — retransmission trades latency for correctness, and that trade surfaces as tail latency, not as errors.

### Visualization (canvas `c3`, 720×300)

Bar chart of 20 identical API call latencies: a flat band near the median with two retransmit spikes at 4× — the shape loss takes in monitoring.

- **Title (bold 15px, `#1a5276`, top center):** "20 Identical API Calls: Retransmits Live in the Tail".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; y = latency 0 to 450 ms with `#e5e9ef` gridlines and 12px `#6b7280` labels at 100/200/300/400; x = call number 1 to 20, 12px `#6b7280` tick labels at calls 1, 5, 10, 15, 20.
- **Bars:** 20 bars ~22px wide with 8px gaps, latencies (ms) `[102, 98, 105, 100, 97, 103, 401, 99, 101, 104, 96, 100, 98, 102, 403, 99, 97, 101, 100, 103]`; fill `rgba(42,120,214,0.35)` with 2px `#2a78d6` top edge.
- **Spike bars:** calls 7 and 15 drawn solid orange `#d95926` with bold 12px orange label "retransmit" above each.
- **Median line:** horizontal dashed (4/3) 2px green `#008300` line at 100 ms, 12px green label "median 100 ms" at its left end above the line.
- **Annotation (bold 13px violet `#4a3aa7`, near x=400, y=70):** "p50 healthy, tail at 4× — the network ate the tail".
- **Caption (12px `#6b7280`, bottom right):** "latencies illustrative — 2 of 20 calls hit one retransmission".

## Delivered to TCP Is Not Delivered to the App

**Tags:** `common mistake` (red), `end-to-end` (orange)

- **The illusion** — a successful send() means the bytes entered the local TCP buffer, nothing more
- **The gap** — the connection can die with data in flight; the sender cannot tell what arrived
- **The receiver side** — even an ACKed byte may sit in the receive buffer, never read by the app
- **The fix** — confirmations that matter come from the application: "payment 8127 recorded"
- **The habit** — make retried requests idempotent, because "unknown" means you may send twice

*Example (italic):* The write returned success, the connection then dropped, and the payment row never appeared — TCP delivered to a buffer, not to the database.

**Common mistake:** Treating TCP's reliability as end-to-end application delivery. It guarantees the byte stream between two sockets while the connection lives — acknowledging the business outcome is your job.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: trusting send() alone (outcome unknown) vs adding an application-level receipt (outcome confirmed), shown as boxes with arrows.

- **Title (bold 15px, `#1a5276`, top center):** "send() Returned — What Does That Actually Prove?".
- **Row 1 (y=95), label 12px `#6b7280` at x=20:** "the assumption"; blue `#2a78d6` rounded box at x=150 labeled "app: send(payment)" (12px), 3px arrow to a blue box at x=345 labeled "bytes in local TCP buffer", 3px arrow to a magenta `#d55181` box at x=545 labeled "link dies — arrived?" with bold 12px magenta "✗ unknown, maybe sent twice".
- **Row 2 (y=205), label:** "the fix"; blue box at x=150 "app: send(payment)", arrow to a blue box at x=345 "TCP delivers and ACKs bytes", arrow to a green `#008300` box at x=545 labeled "reply: payment 8127 recorded" with bold 12px green "✓ app-level receipt".
- **Box style:** 150–170px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(213,81,129,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px orange `#d95926`, centered near y=270):** "TCP guarantees the pipe, not the business outcome".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); the timeline numbers (50 ms one-way, 100 ms RTT, loss at t=25, 300 ms timeout, resend arriving at 350, final ACK at 400) and the 20 call latencies `[102, 98, 105, 100, 97, 103, 401, 99, 101, 104, 96, 100, 98, 102, 403, 99, 97, 101, 100, 103]` are invented and labeled illustrative; the six-card recipe and payment flows are schematic.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
