# Go Channels & CSP

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Go Channels & CSP

**Subtitle:** A channel is a hand-off point between concurrent workers — instead of two goroutines editing the same memory, one passes a value to the other, and ownership travels with it

## A Ticket Rail Between Two Workers

**Tags:** `core idea` (blue), `hand-off` (green), `goroutines` (orange)

- **The shop** — a sandwich shop has a cashier taking orders and a cook making them, both working at once
- **The rail** — the cashier clips each order ticket to a rail; the cook pulls the next ticket when free
- **The channel** — in Go the rail is a channel: one goroutine sends a value in, another receives it out
- **Nothing shared** — the ticket is the only thing that moves; neither worker touches the other's notepad
- **No rail at all** — an unbuffered channel is a direct hand-off: the send waits until the cook is there

*Example (italic):* `orders <- ticket` is the cashier clipping a ticket on; `t := <-orders` is the cook pulling the next one — those two moves are the entire mechanism.

**Key point:** A channel is a hand-off point between goroutines: a value goes in one end and out the other, and whoever received it now owns it.

### Visualization (canvas `c1`, 720×300)

Single-panel schematic: cashier box on the left, cook box on the right, a three-slot ticket rail between them, one ticket mid-flight, arrows showing the one-way flow.

- **Title (bold 15px, `#1a5276`, top center):** "The Ticket Rail: Values Move, Memory Doesn't".
- **Cashier box:** rounded rect x=40–185, y=115–200, 2px blue `#2a78d6` border, fill `rgba(42,120,214,0.10)`; bold 13px blue label "cashier" centered near y=145, 12px `#6b7280` line "goroutine 1" below it, 12px `#2c3e50` line "sends tickets" near y=185.
- **Cook box:** rounded rect x=535–680, y=115–200, 2px green `#008300` border, fill `rgba(0,131,0,0.10)`; bold 13px green label "cook" near y=145, 12px `#6b7280` "goroutine 2", 12px `#2c3e50` "receives tickets" near y=185.
- **Rail:** horizontal 3px `#1a5276` line from x=225 to x=495 at y=150; three slot rects 56×40 (1.5px `#6b7280` border, dash 4/3, fill white) centered on the rail at x=245, 330, 415; 12px `#6b7280` label "channel (rail, holds 3)" centered below at y=215.
- **Ticket:** filled orange `#d95926` rect 44×30 inside the first slot, bold 12px white label "#4" centered on it; the other two slots empty.
- **Arrows:** 3px blue arrow from cashier box edge (x=185) to the rail at x=222; 3px green arrow from x=498 to the cook box edge (x=535); both with solid arrowheads at y=150.
- **Annotation (bold 13px violet `#4a3aa7`, centered near x=360, y=75):** two lines: "the ticket is the only shared thing —" / "and only one worker holds it at a time".
- **Caption (12px `#444`, bottom right):** "schematic — one send, one receive, one channel".

## Tracing the Rail: When the Cashier Stalls

**Tags:** `worked example` (blue), `buffering` (green), `blocking` (orange)

- **The setup** — the cashier finishes an order every 10 s, the cook needs 20 s each, the rail holds 3
- **Falling behind** — the cashier adds one ticket per 10 s but the cook removes one per 20 s
- **Filling up** — the rail holds 1 ticket at t=20, 2 at t=40, and hits its cap of 3 at t=60
- **The stall** — at t=80 the cashier has ticket #8 ready but no free clip, so the send just waits
- **Unblocked** — at t=90 the cook pulls ticket #5, a clip frees, and #8 goes on 10 s late

*Example (italic):* Check it by hand: sends land at t=10, 20, 30, ... while the cook pulls at t=10, 30, 50, 70, 90 — one extra ticket every 20 s is exactly what fills the rail by t=60.

**Key point:** A buffered channel absorbs a burst up to its size; once full, the sender is paused automatically — the fast worker slows to the pace of the slow one with zero extra code.

### Visualization (canvas `c2`, 720×300)

Single-panel step chart: tickets sitting on the rail over the first 100 seconds, with the capacity line, the moment the rail fills, and the cashier's 10-second stall shaded.

- **Title (bold 15px, `#1a5276`, top center):** "Rail Occupancy: Cashier Every 10 s, Cook Every 20 s, Rail Holds 3".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 190; x = time 0 to 100 s with 12px `#444` tick labels "0", "10", ..., "100" every 10 s and axis caption "seconds"; y = tickets on rail 0 to 3, 12px `#444` labels "0"–"3", light `#e5e9ef` gridlines at 1 and 2.
- **Capacity line:** horizontal dashed red `#e74c3c` (dash 6/4) 2px line at occupancy 3; 12px red label "rail full (cap 3)" at its left end.
- **Step line:** blue `#2a78d6` 3px right-continuous steps through hardcoded points at t = `[0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100]`, occupancy = `[0, 0, 1, 1, 2, 2, 3, 3, 3, 3, 3]`; fill under the steps `rgba(42,120,214,0.15)`.
- **Pull markers:** small green `#008300` 6px down-triangles on the baseline at t = `[10, 30, 50, 70, 90]`, 11px green label "cook pulls" under the first one only.
- **Stall band:** shaded rect `rgba(231,76,60,0.15)` from t=80 to t=90 spanning the plot height; bold 13px red `#e74c3c` label above it: "cashier blocked 80–90 s".
- **Fill marker:** vertical dashed `#6b7280` (dash 4/3) line at t=60 from baseline to y=70, 12px `#6b7280` label "full at t=60" at its top.
- **Annotation (bold 12px orange `#d95926`, near t=25, y=105):** two lines: "ticket #8 waits for a clip —" / "backpressure, no code needed".
- **Caption (12px `#444`, bottom right):** "illustrative timings — 10 s vs 20 s per order".

## Share by Communicating, Not by Locking

**Tags:** `where it's used` (blue), `CSP` (green), `no locks` (orange)

- **The old way** — with one shared whiteboard, both workers must lock it before writing or orders get lost
- **CSP** — Hoare's 1978 idea: keep workers fully separate and let them interact only by passing messages
- **Go's slogan** — "don't communicate by sharing memory; share memory by communicating"
- **Free balancing** — point three cooks at one rail and each simply pulls the next ticket when free
- **Where you meet it** — worker pools, pipelines, timeouts via `select`, fan-out/fan-in inside servers

*Example (italic):* Twelve lunch orders, three cooks, one rail: each cook naturally ends up making four — nobody wrote a scheduler, the rail balanced the work.

**Key point:** Because a value lives with exactly one goroutine at a time, whole categories of bugs — races, forgotten locks, torn updates — never get a chance to happen.

### Visualization (canvas `c3`, 720×300)

Single-panel fan-out schematic: one cashier feeding one rail, three cook boxes pulling from it, each stamped with the four tickets it happened to make from the twelve sent.

- **Title (bold 15px, `#1a5276`, top center):** "One Rail, Three Cooks: 12 Tickets Balance Themselves".
- **Cashier box:** rounded rect x=40–175, y=115–195, 2px blue `#2a78d6` border, fill `rgba(42,120,214,0.10)`; bold 13px blue label "cashier", 12px `#6b7280` line "sends #1–#12" below it.
- **Rail:** horizontal 3px `#1a5276` line from x=210 to x=400 at y=155; 12px `#6b7280` label "one channel" centered below at y=180; 3px blue arrow from the cashier box into the rail's left end.
- **Cook boxes (right column, each 150×60, 2px green `#008300` border, fill `rgba(0,131,0,0.10)`, at x=505–655):** top y=55–115 labeled bold 13px green "cook A" with 12px `#2c3e50` line "made #1 #4 #7 #10"; middle y=125–185 "cook B" with "made #2 #5 #8 #11"; bottom y=195–255 "cook C" with "made #3 #6 #9 #12".
- **Fan-out arrows:** three 2.5px green arrows from the rail's right end (x=400, y=155) to the left edge of each cook box, with solid arrowheads.
- **Annotation (bold 13px violet `#4a3aa7`, near x=230, y=90):** two lines: "whoever is free pulls next —" / "no lock, no scheduler".
- **Caption (12px `#444`, bottom right):** "illustrative split — 12 tickets, 4 per cook when speeds match".

## Buffered Doesn't Mean Never Blocks

**Tags:** `common mistake` (red), `unbuffered vs buffered` (orange)

- **The myth** — "buffered channels are async, so sends never block" — true only until the buffer fills
- **Unbuffered** — a send on an unbuffered channel waits for a receiver: it is a meeting, not a mailbox
- **Receives too** — a cook at an empty rail stands and waits; receiving blocks just like sending
- **Deadlock** — if every goroutine is stuck sending or receiving, Go stops the program and says so
- **Sizing** — buffer size tunes how big a burst is absorbed, not correctness; logic must survive size 0

*Example (italic):* With no rail, a cashier holding a ticket at t=0 stands frozen for 6 s until the cook walks up — the hand-off happens the instant both are present.

**Common mistake:** Adding a buffer to "fix" a hang. The buffer only delays the block; if receives can't keep up, a size-1 buffer stalls on the second send — the fix is a receiver, not a bigger rail.

### Visualization (canvas `c4`, 720×300)

Two-row timeline on a shared seconds axis: the top row shows an unbuffered send waiting 6 seconds for its receiver; the bottom row shows a size-1 buffer letting the first send through instantly, then stalling the second one anyway.

- **Title (bold 15px, `#1a5276`, top center):** "Unbuffered vs Buffered(1): Both Sends End Up Waiting".
- **Axis:** horizontal 2px `#999` line at y=250 from x=180 to x=680 (width 500), time 0 to 8 s; 12px `#444` tick labels "0 s" through "8 s" every 1 s.
- **Receiver marker:** vertical dashed `#6b7280` (dash 4/3) line at t=6 from y=60 to the axis, bold 12px `#6b7280` label "cook arrives at t=6" at its top.
- **Row 1 (y=110), 12px `#444` label at x=20:** "unbuffered — send #1 at t=0"; orange `#d95926` 10px-tall rounded bar from t=0 to t=6 (fill `rgba(217,89,38,0.35)`, 11px orange label "waiting" centered on it); green `#008300` 7px dot at t=6 with bold 12px green label "hand-off" above.
- **Row 2 (y=190), label:** "buffered(1) — sends at t=0 and t=2"; green 7px dot at t=0 with bold 12px green label "instant" above; orange 10px-tall rounded bar from t=2 to t=6 (same fill, 11px orange label "waiting"); green 7px dot at t=6 with 12px green label "slot frees".
- **Annotation (bold 13px magenta `#d55181`, centered near x=430, y=285):** "the buffer bought 2 seconds, not correctness — send #2 still blocked for 4 s".
- **Caption (12px `#444`, top right):** "illustrative timings".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, violet `#4a3aa7`, orange `#d95926`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all timelines, step points, and box coordinates are the hardcoded literals above (no randomness); the c2 occupancy trace follows exactly from sends every 10 s, pulls every 20 s, capacity 3 — recompute by hand if edited so text and chart stay in sync.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
