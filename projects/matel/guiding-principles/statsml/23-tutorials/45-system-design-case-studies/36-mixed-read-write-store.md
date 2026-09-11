# Mixed Read/Write Store

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Mixed Read/Write Store

**Subtitle:** An order-status store where every write is read again within seconds &mdash; the read-optimized layout and the write-optimized layout each ruin the other path, so the answer is not a compromise engine but a decomposition: split by data age, then set the quorum dial

## Both Paths Are Hot at the Same Time

**Tags:** `core idea` (blue), `both paths hot` (red), `no middle ground` (orange)

- **The service** &mdash; an order-status store where each write is read again within seconds by three apps
- **Both paths hot** &mdash; reads and writes are both thousands per second; neither path is the rare one
- **Read-optimized** &mdash; keep four pre-shaped copies: a read is one hop, but a write must touch all four
- **Write-optimized** &mdash; append and compact later: a write is one op, but a read merges four fragments
- **The contradiction** &mdash; each layout turns the other path's one unit of work into four; no middle wins

*Example (italic):* The courier taps "picked up"; seconds later the customer's map, the courier's job card, and the restaurant's board all read that same row.

**Key point:** When both paths are hot, the read-optimized and the write-optimized answers contradict each other &mdash; whichever path you make one hop, the other becomes four. Stop hunting for one layout and decompose the workload instead. Illustrative Example.

### Visualization (canvas `c1`, 720&times;300)

Conceptual before/after diagram, two panels: the same key under a read-optimized layout and under a write-optimized layout, showing the one-hop path and the four-hop path in each. No arithmetic in the chart beyond the hop counts drawn as boxes.

- **Title (bold 15px, `#1a5276`, top center):** "Whichever Path Is One Hop, the Other Is Four".
- **Divider:** vertical dashed `#6b7280` (dash 4/3) line at x=362, y=45 to y=265.
- **Left panel label (bold 13px blue `#2a78d6`, centered x=185, y=56):** "read-optimized: pre-shaped copies".
- **Left read path:** 12px `#444` label "read path" at x=30, y=80; box "read" at x=30, y=88 (54&times;26); 2px arrow to a green box "one copy" at x=112, y=88 (90&times;26); bold 12px green `#008300` "1 hop" at x=212, y=105.
- **Left write path:** 12px `#444` label "write path" at x=30, y=140; box "write" at x=30, y=150 (54&times;26); four 1.5px orange arrows fanning from its right edge to four boxes "copy 1".."copy 4" at x=140, y=148/178/208/238 (90&times;24, orange fill); bold 12px orange `#d95926` "&times;4" at x=238, y=200.
- **Right panel label (bold 13px blue, centered x=540, y=56):** "write-optimized: append, compact later".
- **Right write path:** 12px `#444` "write path" at x=385, y=80; box "write" at x=385, y=88 (54&times;26); arrow to green box "append once" at x=467, y=88 (110&times;26); bold 12px green "1 op" at x=587, y=105.
- **Right read path:** 12px `#444` "read path" at x=385, y=140; box "read" at x=385, y=150 (54&times;26); four 1.5px orange arrows from four boxes "fragment 1".."fragment 4" at x=495, y=148/178/208/238 (100&times;24, orange fill) back into the read box; bold 12px orange "&times;4" at x=601, y=200.
- **Box style:** 6px radius, 1px border, 12px `#2c3e50` centered text; neutral fill `rgba(42,120,214,0.15)` border `#2a78d6`, green fill `rgba(0,131,0,0.12)` border `#008300`, orange fill `rgba(217,89,38,0.14)` border `#d95926`.
- **Annotation (bold 13px violet `#4a3aa7`, centered y=275):** "one engine tuned in the middle wins neither path".
- **Caption (12px `#444`, bottom right):** "Illustrative Example; hop counts illustrative".

## Split by Age, Not by Averaging

**Tags:** `worked example` (blue), `hot window` (green), `two tiers` (orange)

- **The shape** &mdash; reads pile onto rows written moments ago and fall off sharply with row age
- **The hot window** &mdash; about 94% of reads land on rows less than a day old (illustrative mix)
- **The small share** &mdash; one day out of a 30-day retention window is roughly 3% of stored rows
- **The hot tier** &mdash; that 3% fits in memory, pre-shaped, so recent reads stay a single hop
- **The cold tier** &mdash; older rows age out to an append-optimized engine where writes stay cheap

*Example (italic):* A read for an order placed four minutes ago never touches disk; a support agent's read of a three-week-old order pays the merge cost, and nobody minds.

**Key point:** Split by access pattern rather than averaging over it &mdash; a small memory-resident window of recent rows serves most of the reads, and everything older can live in an engine tuned purely for cheap writes. Illustrative Example.

### Visualization (canvas `c2`, 720&times;300)

Bar chart of the shape: share of reads by how old the row was when it was read, with the recent buckets bracketed as the hot window. Every percentage and the hot-window share are computed at render from the plotted counts.

- **Title (computed, bold 15px, `#1a5276`, top center):** hot share printed from the data &mdash; "94% of Reads Land on Rows Less Than a Day Old".
- **Data (hardcoded):** labels `['< 1 min', '1-10 min', '10-60 min', '1-24 h', 'older']`, counts `[55000, 20000, 7000, 12000, 6000]` (total 100,000). Each bar's percentage and the hot-window share (first four buckets) are computed in JS.
- **Axes:** origin x=80, baseline y=240, plot width 590, plot height 150; y = share of reads 0 to 60%, gridlines `#e5e9ef` at 20 / 40 / 60 with 12px `#444` right-aligned labels ending in "%".
- **Bars:** 5 bars, 62px wide, evenly spaced across the plot; first four fill `rgba(0,131,0,0.5)` border `#008300`, last fill `rgba(217,89,38,0.5)` border `#d95926`.
- **Bar value labels (bold 12px in the bar's colour, 8px above each bar):** the computed percentage, e.g. "55%".
- **Hot-window bracket:** 2px `#008300` line at y=78 spanning the first four bars with short down-ticks at each end; bold 12px green centred label at y=66 printed at render &mdash; "hot window &mdash; 94% of reads".
- **Annotation (bold 12px violet `#4a3aa7`, right-aligned x=670, y=140):** "and only about 3% of stored rows (1 day of 30)".
- **X labels (12px `#2c3e50`, centered under baseline at y=258):** the bucket labels; 12px `#444` axis caption "age of the row when it was read" centered at y=278.
- **Caption (12px `#444`, bottom right):** "Illustrative Example; shares computed from plotted counts".

## The Quorum Dial: R + W > N

**Tags:** `worked example` (blue), `tunable consistency` (green), `latency dial` (orange)

- **The replicas** &mdash; each order key sits on N = 3 copies; a write waits for W of them, a read for R
- **The rule** &mdash; R + W &gt; N forces the read set and the write set to share at least one replica
- **Favor reads** &mdash; R=1, W=3: a read answers from the nearest copy, a write waits for all three
- **Favor writes** &mdash; R=3, W=1: a write is acknowledged at once, a read must consult every copy
- **Balanced** &mdash; R=2, W=2: both paths wait for a majority, and both survive one node being down
- **The unsafe setting** &mdash; R=1, W=1 sums to 2, not more than 3, so a read can miss the write entirely

*Example (italic):* Pin the courier's session to the replica that accepted his write and he sees "picked up" instantly, while the restaurant's board catches up a beat later.

**Key point:** R + W &gt; N is the whole guarantee: the read set and the write set must overlap, so a read is forced to touch a replica that saw the write. Which side you make slower is a dial you choose, not a property of the database.

### Visualization (canvas `c3`, 720&times;300)

Three-setting dial diagram (plus the unsafe setting) at N = 3: for each (R, W) a row of three replica slots with the write set drawn above and the read set below, and the shared slots marked. Every verdict and overlap count is computed in JS as R + W &minus; N.

- **Title (bold 15px, `#1a5276`, top center):** "N = 3: The Dial Between Fast Reads and Fast Writes".
- **Legend (12px, y=52, starting x=185):** orange `#d95926` swatch "write set (W)", blue `#2a78d6` swatch "read set (R)", violet `#4a3aa7` swatch "shared".
- **Data (hardcoded):** `N = 3`; configs `[{R:1,W:3,note:'reads fastest'},{R:2,W:2,note:'balanced'},{R:3,W:1,note:'writes fastest'},{R:1,W:1,note:'unsafe'}]`.
- **Rows:** four rows with centers y = 100, 148, 196, 244. Three slots 50px wide starting at x=200 (so slots at x=200, 250, 300, ending x=350).
- **Per row:** write-set bar (`rgba(217,89,38,0.5)` fill, 1px `#d95926`) at y = center&minus;14, height 12, covering the LAST W slots; read-set bar (`rgba(42,120,214,0.5)` fill, 1px `#2a78d6`) at y = center+2, height 12, covering the FIRST R slots; slot outlines 1px `#e5e9ef`.
- **Shared marking:** where the two sets overlap (slot indexes N&minus;W .. R&minus;1, drawn only when R + W &minus; N &gt; 0), a 2px dashed `#4a3aa7` rectangle around both bars for those slots.
- **Left labels (right-aligned at x=190):** bold 12px `#1a5276` "R=1, W=3" at center&minus;6, 12px `#6b7280` note ("reads fastest") at center+14.
- **Verdicts (bold 12px, left-aligned x=368):** printed at render from R + W vs N &mdash; green `#008300` "1+3 = 4 &gt; 3 &mdash; shares 1 replica" style for the safe rows, red `#e74c3c` "1+1 = 2 &le; 3 &mdash; may share none" for the last, with a second red 12px line "a read can miss the write" at center+16.
- **Annotation (bold 12px violet `#4a3aa7`, centered y=278):** "the overlap is what makes a read see the write".
- **Caption (12px `#444`, bottom right):** "quorum arithmetic exact".

## What Goes Wrong

**Tags:** `failure mode` (red), `contention` (orange), `last-write-wins` (violet)

- **The averaged ratio** &mdash; one service-wide read:write number can be true of the service and of no table in it
- **Per table instead** &mdash; a profile table is read-heavy, an event log write-heavy; pick a layout per table
- **Lock contention** &mdash; reads and writes on one hot key serialize, so read latency spikes behind the writes
- **Last-write-wins** &mdash; drop the lock and the highest timestamp wins, whichever write actually came second
- **The silent loss** &mdash; a slow clock makes the later write look earlier, so it is dropped with no error logged

*Example (italic):* The support agent sees "cancelled" confirmed on screen, and twenty seconds later every reader is back to "picked up" with nothing in the logs.

**Common mistake:** Sizing one engine for an averaged ratio, then trusting wall-clock timestamps to order concurrent writes. On a key both paths hammer, the choice is lock contention or a silently lost update &mdash; version vectors or a compare-and-set on the status field, not a clock.

### Visualization (canvas `c4`, 720&times;300)

Two-row conceptual flow: the same two writers on one hot key, routed through a lock (reads queue) and through last-write-wins (one write disappears). No timestamps arithmetic drawn.

- **Title (bold 15px, `#1a5276`, top center):** "Two Writers, One Hot Key: Queue Behind It or Lose One".
- **Row 1 (center y=100), 12px `#444` label "with a lock" at x=20, y=68:** blue box "write A holds lock" at x=110 (130 wide); 3px `#6b7280` arrow to an orange box "write B waits" at x=282 (150 wide); 3px red `#e74c3c` arrow to a red box "reads queue behind" at x=474 (200 wide); bold 12px red "&#10007; read latency spikes" centered at x=574, y=140.
- **Row 2 (12px `#444` label "no lock, last write wins" at x=20, y=178):** two blue boxes at x=110 (130 wide), centers y=190 and y=240, labelled "write A (earlier)" and "write B (later)"; two 3px `#6b7280` arrows converging into a violet box "highest timestamp wins" at x=300 (170 wide), center y=215; 3px red arrow to a red box "B dropped, no error" at x=510 (170 wide), center y=215.
- **Box style:** 40px tall, 8px radius, 2px border, 12px `#2c3e50` centered text; blue fill `rgba(42,120,214,0.15)` border `#2a78d6`, orange fill `rgba(217,89,38,0.14)` border `#d95926`, violet fill `rgba(74,58,167,0.12)` border `#4a3aa7`, red fill `rgba(231,76,60,0.12)` border `#e74c3c`.
- **Annotation (bold 12px violet `#4a3aa7`, centered y=278):** "a slow clock makes the later write look earlier".
- **Caption (12px `#444`, bottom right):** "Illustrative Example".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> &mdash; ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`, violet `rgba(74,58,167,0.14)`/`#4a3aa7`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links, no cross-page links.
- **Canvas:** intrinsic 720&times;300; shared `setup(id)` helper sizes the backing store to the rendered width &times; `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Shared `roundRect()` and `arrow()` helpers. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** no randomness anywhere &mdash; every series is a hardcoded literal array. Derived quantities are computed in JS at render time, never hardcoded beside the drawing: each read-age bucket's percentage from `counts[i]/total`, the hot-window share (55+20+7+12 = 94 of 100 per-100,000 reads = 94%), and every quorum verdict and overlap count from R + W vs N = 3. The four-copy / four-fragment layouts, the read-age mix, and the 30-day retention window are invented and labelled illustrative; the arithmetic built on them closes exactly (bucket shares sum to 100%, and one day of thirty is 3.3% of stored rows, stated as "about 3%"). The R + W &gt; N rule is stated as it holds: the read set and write set are forced to overlap, so a read touches a replica that saw the write.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
