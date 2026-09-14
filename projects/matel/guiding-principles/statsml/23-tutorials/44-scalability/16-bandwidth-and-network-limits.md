# Bandwidth & Network Limits

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Bandwidth & Network Limits

**Subtitle:** A 10 Gbit/s network card moves at most 1.25 GB per second — so a job that shuffles terabytes has a time floor set by the wire, not the CPU

## The Nightly Shuffle Hits a Wire-Speed Wall

**Tags:** `core idea` (blue), `NIC ceiling` (green), `hard floor` (orange)

- **The job** — a nightly analytics job re-sorts 10 TB of order events across a 20-node cluster
- **The split** — each node holds 500 GB and must ship 475 GB of it (19/20) to the other 19 nodes
- **The wire** — each node has a 10 Gbit/s NIC; 10 Gbit/s ÷ 8 = 1.25 GB/s, an exact conversion
- **The floor** — 475 GB ÷ 1.25 GB/s = 380 seconds; no CPU upgrade can push bytes out faster
- **The definition** — a job is network-bound when the wire drains slower than the CPU produces

*Example (italic):* The team doubles CPU cores and the shuffle still takes 380 seconds — the NIC was already sending flat-out the whole time.

**Key point:** Every NIC is a fixed-rate pipe: 10 Gbit/s is exactly 1.25 GB/s, so shipping 475 GB per node has a hard floor of 380 seconds regardless of compute.

### Visualization (canvas `c1`, 720×300)

Line chart of GB sent by one node over time: what the CPU could produce vs what the NIC can drain, showing the wire sets the finish time.

- **Title (bold 15px, `#1a5276`, top center):** "One Node's 475 GB: the NIC Drains at 1.25 GB/s, Full Stop".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = seconds 0 to 400, 12px `#444` tick labels every 100 s; y = GB sent 0 to 475, gridlines `#e5e9ef` at 120/240/360, y-labels "0", "120", "240", "360", "475".
- **CPU line:** dashed violet `#4a3aa7` 2px (dash 6/4) through seconds `[0, 95]`, GB `[0, 475]` — the rate the CPU could hand off data (5 GB/s, illustrative), 12px violet label "CPU could feed 5 GB/s" near (110, 60 GB from top).
- **NIC line:** solid blue `#2a78d6` 3px through seconds `[0, 100, 200, 300, 380]`, GB `[0, 125, 250, 375, 475]` — exactly 1.25 GB/s.
- **Floor marker:** vertical dashed `#6b7280` (dash 4/3) line at x=380 from baseline to the NIC line's end, bold 13px `#1a5276` label "380 s floor" beside it.
- **Annotation (bold 13px blue `#2a78d6`, near x=180, above the NIC line):** "wire speed sets the finish, not the CPU".
- **Caption (12px `#444`, bottom right):** "10 Gbit/s = 1.25 GB/s exact; workload sizes illustrative".

## Racks Share a Thinner Uplink

**Tags:** `worked example` (blue), `oversubscription` (green)

- **The rack** — each rack holds 20 servers; 20 × 10 Gbit/s = 200 Gbit/s of server-facing capacity
- **The uplink** — the rack's uplink to the datacenter fabric carries only 67 Gbit/s: a 3:1 ratio
- **The share** — when every server sends cross-rack at once, each gets 10 ÷ 3 ≈ 3.3 Gbit/s ≈ 0.42 GB/s
- **Hand-check** — 475 GB ÷ 0.42 GB/s ≈ 1,140 seconds, three times the 380-second in-rack floor
- **The rule** — oversubscription means in-rack traffic runs at line rate; cross-rack traffic contends

*Example (italic):* The nightly job lands 10 nodes in each of two racks; with each rack's other servers also sending cross-rack, its shuffle crawls at 0.42 GB/s per node and takes ~19 minutes instead of ~6.3.

**Key point:** Fabrics are built oversubscribed on purpose — a 3:1 rack uplink cuts each server's worst-case cross-rack bandwidth to a third, so where a job's nodes sit changes its floor by 3×.

### Visualization (canvas `c2`, 720×300)

Grouped bar chart with two panels: aggregate rack demand vs uplink capacity (left), and one server's in-rack vs cross-rack bandwidth (right).

- **Title (bold 15px, `#1a5276`, top center):** "3:1 Oversubscription: 200 Gbit/s of NICs Behind a 67 Gbit/s Uplink".
- **Layout:** shared 2px `#999` baseline at y=245; left panel bars centered at x=140 and x=250, right panel bars at x=460 and x=570; bars 70px wide; panel captions 12px `#444` under the baseline: "whole rack (Gbit/s)" at x≈195, "one server (Gbit/s)" at x≈515.
- **Left panel scale:** 0–200 Gbit/s over 180px height; bar 1 blue `#2a78d6` fill `rgba(42,120,214,0.30)` height 180 labeled "20 NICs: 200"; bar 2 orange `#d95926` solid height 60 labeled "uplink: 67".
- **Right panel scale:** 0–10 Gbit/s over 180px height; bar 1 green `#008300` fill `rgba(0,131,0,0.30)` height 180 labeled "in-rack: 10"; bar 2 red `#e74c3c` solid height 60 labeled "cross-rack: 3.3".
- **Value labels:** bold 13px in the bar's color, centered above each bar top.
- **Annotation (bold 13px `#d95926`, top right area, y≈70):** "cross-rack floor: 380 s → 1,140 s".
- **Caption (12px `#444`, bottom right):** "3:1 ratio illustrative of common fabrics; divisions exact".

## Nineteen Senders, One Receiver, One Buffer

**Tags:** `where it's used` (blue), `TCP incast` (red)

- **The fetch** — a reducer node asks all 19 peers for its partition; all 19 answer in the same instant
- **The choke** — 19 × 10 Gbit/s of replies converge on one 10 Gbit/s receiver port at the switch
- **The buffer** — the switch port buffers the excess, overflows in milliseconds, and drops packets
- **The collapse** — every dropped sender waits out a TCP timeout, so the port sits idle between bursts
- **The name** — this many-to-one throughput collapse is called TCP incast, a classic shuffle failure

*Example (italic):* With 4 senders the reducer pulls ~9.3 Gbit/s; with all 19 answering at once, goodput collapses to ~1.2 Gbit/s — slower than a single sender.

**Key point:** Incast is not congestion from too much data overall — it is many synchronized senders overflowing one switch buffer, leaving the receiver's link mostly idle while everyone waits on timeouts.

### Visualization (canvas `c3`, 720×300)

Line chart of receiver goodput vs number of simultaneous senders, showing throughput collapse past the buffer's limit.

- **Title (bold 15px, `#1a5276`, top center):** "Incast: More Senders at Once, Less Data Through".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = simultaneous senders 1 to 19, 12px `#444` tick labels at 1/4/8/12/16/19; y = receiver goodput 0 to 10 Gbit/s, gridlines `#e5e9ef` at 2.5/5/7.5, y-labels "0", "2.5", "5", "7.5", "10".
- **Goodput line:** 3px line, green `#008300` from senders 1–8, red `#e74c3c` from 8–19, through senders `[1, 2, 4, 8, 12, 16, 19]`, goodput `[9.4, 9.4, 9.3, 9.0, 4.1, 1.9, 1.2]`, 5px dots at each point.
- **Collapse marker:** vertical dashed `#6b7280` (dash 4/3) line at senders=10, 12px `#6b7280` label "switch buffer overflows" at its top.
- **Annotation (bold 13px red `#e74c3c`, near senders 15, y≈120):** "19 senders: 1.2 Gbit/s — slower than one".
- **Caption (12px `#444`, bottom right):** "goodput values illustrative; collapse shape is the classic incast curve".

## Faster CPUs Don't Move Bytes

**Tags:** `common mistake` (red), `data locality` (green)

- **The mistake** — the job is slow, so the team buys nodes with twice the cores; the shuffle barely moves
- **The math** — the job is 6 min of compute + 19 min of cross-rack shuffle; halving compute saves 3 min
- **Fix 1: placement** — schedule all 20 nodes in one rack; the shuffle drops from 19 min to 6.3 min
- **Fix 2: less data** — a map-side combine shrinks 475 GB per node to 158 GB (3×): 6.3 min → 2.1 min
- **The rule** — for network-bound jobs, move less data or move it a shorter distance; CPUs are a rounding error

*Example (italic):* Doubling CPUs takes the 25-minute job to 22 minutes; keeping the shuffle in-rack and combining first takes it to 8 minutes on the same hardware.

**Common mistake:** Reading a slow distributed job as a compute problem. If the wire is the bottleneck, faster CPUs just wait faster — data locality and shuffle reduction attack the term that dominates.

### Visualization (canvas `c4`, 720×300)

Horizontal stacked bar chart: total job time under four strategies, each bar split into a compute segment and a shuffle segment.

- **Title (bold 15px, `#1a5276`, top center):** "Same 10 TB Job, Four Strategies: Attack the Shuffle, Not the CPU".
- **Layout:** bars extend right from x=230, scale 25 min = 430px (17.2 px/min); rows at y = 70, 120, 170, 220, bars 22px tall; left-aligned 12px `#444` row labels at x=20.
- **Segments:** compute segment solid blue `#2a78d6`, shuffle segment orange fill `rgba(217,89,38,0.65)` with 1px `#d95926` border; bold 12px total-minutes label at each bar's right end.
  - "baseline (cross-rack)": compute 6 + shuffle 19 = 25 min, bar width 430
  - "2× faster CPUs": compute 3 + shuffle 19 = 22 min, bar width 387
  - "same-rack placement": compute 6 + shuffle 6.3 = 12.3 min, bar width 216
  - "in-rack + combine (3× less data)": compute 6 + shuffle 2.1 = 8.1 min, bar width 143, total label bold green `#008300`
- **Legend (12px, top right under title):** blue swatch "compute", orange swatch "shuffle".
- **Annotation (bold 13px green `#008300`, right of row 4, y≈220):** "3× faster without touching a CPU".
- **Caption (12px `#444`, bottom right):** "minutes illustrative, derived from the 380 s / 1,140 s floors above".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness). Bit-to-byte conversions are exact (10 Gbit/s = 1.25 GB/s; 3.33 Gbit/s = 0.417 GB/s; 475 ÷ 1.25 = 380 s; 475 ÷ 0.417 ≈ 1,140 s). Workload numbers (10 TB, 20 nodes, 3:1 ratio, incast goodput values, compute minutes) are illustrative and labeled so in captions.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
