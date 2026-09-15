# NiFi

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** NiFi

**Subtitle:** Apache NiFi lets you draw a data pipeline as boxes and arrows on a canvas — and records every record's full journey, so "where did this bad value come from?" is one query, not a night of log-grepping

## A Pipeline You Draw Instead of Code

**Tags:** `core idea` (blue), `dataflow` (green), `Apache NiFi` (orange)

- **The feed** — 40 bedside monitors drop readings files onto a hospital landing server every 15 minutes
- **The canvas** — instead of code, the team drags processors onto a canvas and wires them: pull, validate, route, deliver
- **FlowFiles** — each file becomes a FlowFile: content (the bytes) plus attributes (filename, device type, bed)
- **The queues** — every arrow between two processors is a queue; a FlowFile waits there until the next box is ready
- **The origin** — NiFi began inside the NSA as "Niagarafiles" and was open-sourced through Apache in 2014

*Example (italic):* At 2:00am GetSFTP pulls `bed07-vitals.csv`; it enters the flow as a FlowFile with attributes filename=bed07-vitals.csv, device.type=vitals.

**Key point:** NiFi is flow-based programming: data travels as FlowFiles through a graph of processors connected by queues — a pipeline you draw, not one you code.

### Visualization (canvas `c1`, 720×300)

Flow diagram of the running example: an SFTP fetch box feeding a validator, then a router that splits by device type, with queues drawn as labeled arrows and one FlowFile chip riding the first arrow.

- **Title (bold 15px, `#1a5276`, top center):** "The Flow: Pull, Validate, Route by Device Type, Deliver".
- **GetSFTP box:** blue `#2a78d6` rounded box (110×44, 8px radius, fill `rgba(42,120,214,0.15)`) centered at (85, 150), 12px `#2c3e50` label "GetSFTP".
- **Validator box:** violet `#4a3aa7` rounded box (140×44, fill `rgba(74,58,167,0.12)`) centered at (255, 150), label "ValidateRecord".
- **Router box:** yellow `#c98500` rounded box (160×44, fill `rgba(201,133,0,0.12)`) centered at (440, 150), label "RouteOnAttribute".
- **Destination boxes (right column centered at x=615), 140×40 each:** green `#008300` "vitals warehouse" at y=70, aqua `#199e70` "ECG archive" at y=150, orange `#d95926` "quarantine folder" at y=240; fills at 0.12 alpha of each color.
- **Arrows:** 3px `#6b7280` arrow GetSFTP→validator with 11px `#6b7280` label "queue" above its midpoint; 3px arrow validator→router labeled "valid"; 3px orange diagonal arrow validator→quarantine labeled "invalid"; 3px arrows router→vitals (green label "vitals") and router→ECG (aqua label "ECG").
- **FlowFile chip:** small 46×24 yellow `#c98500` rounded chip riding the GetSFTP→validator arrow at its midpoint, bold 11px label "FlowFile" above it.
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=278):** "boxes are processors, arrows are queues — the whole pipeline is this picture".

## One Night's 240 Files Through the Flow

**Tags:** `worked example` (blue), `back-pressure` (orange)

- **The batch** — the 2:00am pull delivers 240 device files: 150 vitals, 60 ECG, and 30 that fail validation
- **The route** — ValidateRecord checks each file's schema; RouteOnAttribute reads device type and picks a branch
- **Hand-check** — 150 + 60 + 30 = 240; every file takes exactly one branch, none duplicated, none lost
- **Back-pressure** — the vitals queue is capped at 100 FlowFiles; when it fills, the upstream processor pauses
- **The drain** — the warehouse writer empties the queue, the pause lifts, and the last 50 vitals files flow in

*Example (italic):* 150 vitals files meet a 100-file queue cap: the first 100 queue up, the router pauses, and the remaining 50 enter only as the writer drains the queue.

**Key point:** Queues make the flow self-regulating — back-pressure pauses the upstream box instead of letting a slow database writer get buried.

### Visualization (canvas `c2`, 720×300)

Line chart of the vitals queue's depth during the nightly run, showing it hitting the 100-FlowFile cap, holding flat while back-pressure pauses the router, then draining to empty.

- **Title (bold 15px, `#1a5276`, top center):** "The vitals Queue: Fills to the 100-File Cap, Back-Pressure Holds It There".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = seconds into the run 0 to 60, 12px `#444` tick labels "0s"–"60s" every 15s; y = FlowFiles in queue 0 to 120, gridlines `#e5e9ef` at 30/60/90.
- **Cap line:** dashed red `#e74c3c` (dash 5/4) horizontal line at depth 100, 12px red label "cap = 100" at its left end.
- **Queue depth line:** blue `#2a78d6` 3px line through seconds `[0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60]`, depths `[0, 40, 80, 100, 100, 100, 88, 72, 56, 40, 24, 10, 0]`.
- **Back-pressure band:** shaded `rgba(230,126,34,0.12)` vertical band from x=13s to x=28s, bold 12px orange `#d95926` label "router paused" centered at its top.
- **Annotation (bold 13px green `#008300`, near x=45s, y=90):** "all 150 vitals files delivered — none dropped".
- **Caption (12px `#444`, bottom right):** "file counts and timings illustrative".

## Tracing One Bad Reading Back to Its Source

**Tags:** `where it's used` (blue), `provenance` (green), `audit` (orange)

- **The alarm** — a heart-rate reading of 999 bpm shows up in the warehouse; who put it there?
- **Provenance** — NiFi records an event for every FlowFile at every step: received, routed, modified, sent
- **The trace** — one provenance query returns the record's full lineage, from the SFTP pull to the warehouse landing
- **The verdict** — the 999 was already in the file at RECEIVE: the bed-12 monitor sent it; the flow never altered it
- **Regulated fields** — hospitals, government, and finance adopted NiFi largely for this built-in chain of custody
- **Replay** — provenance keeps enough history to re-send a FlowFile through the flow from a recorded point

*Example (italic):* The bad record's lineage reads RECEIVE 2:00:06 → ROUTE 2:00:07 → ROUTE 2:00:08 → SEND 2:00:10 — four queryable events, with the content visible at each hop.

**Key point:** Per-event data provenance is NiFi's signature feature — a queryable chain of custody for every record, which is why environments that must prove where data came from and where it went choose it.

### Visualization (canvas `c3`, 720×300)

Horizontal lineage timeline for the bad record's FlowFile: four provenance events as nodes on a left-to-right track, each stamped with its time and the processor that produced it.

- **Title (bold 15px, `#1a5276`, top center):** "One FlowFile's Lineage: bed12-vitals.csv, 2:00:06 to 2:00:10".
- **Track:** 3px `#6b7280` horizontal line from (70, 160) to (650, 160), arrowhead at the right end.
- **Event nodes (circles r=14 at x = 110, 280, 450, 620 on the track):** blue `#2a78d6` "RECEIVE", violet `#4a3aa7` "ROUTE", orange `#d95926` "ROUTE", green `#008300` "SEND"; fills at 0.15 alpha with 3px colored rings.
- **Labels:** bold 12px event name in the node's color 26px above each node; 12px `#444` timestamp 26px below each node: "2:00:06", "2:00:07", "2:00:08", "2:00:10"; 11px `#6b7280` processor name a further 16px down: "GetSFTP", "ValidateRecord", "RouteOnAttribute", "PutDatabaseRecord".
- **Annotation (bold 13px green `#008300`, centered near y=60):** "999 bpm was in the content at RECEIVE — the monitor sent it, no processor changed it".
- **Caption (12px `#444`, bottom right):** "timestamps illustrative; event types are NiFi's real provenance vocabulary".

## A Flow Manager, Not a Compute Engine

**Tags:** `common mistake` (red), `scope` (orange)

- **The confusion** — the canvas looks like an ETL tool, so teams try to run heavy joins and aggregations in it
- **What it is** — NiFi moves, routes, validates, and lightly transforms records; it is not built for set-vs-set math
- **The cost** — every hop writes content and provenance to disk, so a 10M-row join thrashes the repositories
- **The split** — the documented pattern: NiFi delivers data to a compute engine, then picks up the results
- **A heap trap** — attributes live in JVM memory; stuffing whole payloads into attributes exhausts the heap

*Example (italic):* A team rebuilds a patient-history join as a NiFi flow; the 10M-row merge that took SQL 40 seconds runs for hours while repositories churn.

**Common mistake:** Treating NiFi as the transformation engine. It is dataflow management with provenance — hand heavy computation to a system built for it and let NiFi manage the movement on either side.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: the anti-pattern (heavy join inside NiFi) vs the documented pattern (NiFi hands off to a compute engine and collects the result).

- **Title (bold 15px, `#1a5276`, top center):** "Heavy Work Inside the Flow vs Handing It Off".
- **Row 1 (y=95), label 12px `#444` at x=20:** "join in NiFi"; blue `#2a78d6` rounded box at x=170 labeled "2 feeds, 10M rows" (12px), 3px arrow to a red `#e74c3c` box at x=420 labeled "merge/lookup chain as a join — hours" with bold 12px red "✗ repositories thrash".
- **Row 2 (y=205), label:** "hand-off pattern"; blue box "2 feeds, 10M rows" at x=170, 3px arrow to a green `#008300` box at x=380 labeled "deliver to warehouse", then arrow to a green box at x=580 labeled "SQL join: 40s" with bold 12px green "✓".
- **Box style:** 150–170px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(231,76,60,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px orange `#d95926`, centered near y=270):** "NiFi owns the movement and the audit trail — not the math".
- **Caption (12px `#444`, bottom right):** "row counts and timings illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); the file counts (150/60/30 of 240 — arithmetic 150+60+30=240 exact), the 100-file queue cap, the queue-depth series, the lineage timestamps, and the 10M-row/40-second join figures are invented and labeled illustrative; the provenance event names (RECEIVE, ROUTE, SEND) and the history facts (NSA "Niagarafiles" origin, Apache open-sourcing in 2014) are NiFi's real, documented vocabulary and record.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
