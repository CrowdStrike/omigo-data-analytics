# Data Integration

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Data Integration

**Subtitle:** A generic data-integration-platform design exercise: the hard axis of scale is hundreds of messy source systems, not requests per second — one ontology over 300 schemas, provenance for every derived number, and need-to-know access designed in from day one

## Scale That Counts Schemas, Not Requests

**Tags:** `core idea` (blue), `integration scale` (green), `ontology` (orange)

- **The setup** — a global freight operator wants one coherent picture of every shipment worldwide
- **The sources** — 300 systems: a 1987 mainframe, regional billing DBs, port spreadsheets, terminal logs
- **The twist** — traffic is tiny, a few hundred analyst queries per second, not millions of users
- **The real load** — the 300 sources, not the request rate, consume the engineering budget here
- **The ontology** — canonical object types, Shipment, Vessel, Customer, one per real-world thing
- **The mapping** — each source is declared once against those objects, by whoever knows it best
- **The payoff** — analysts query the ontology, not 300 schemas; new sources plug into existing objects

*Example (italic):* The platform serves a few hundred analyst queries per second yet integrates 300 source systems — the second number, not the first, is what drives the whole design.

**Key point:** In a data-integration platform "scale" means the number and messiness of heterogeneous source systems to unify — a different axis from requests per second, and one you can't fix by adding servers.

### Visualization (canvas `c1`, 720×300)

Schematic scatter with two axes of scale: traffic (requests/s) on x, source systems integrated on y; consumer systems cluster bottom-right, the integration platform sits top-left alone.

- **Title (bold 15px, `#1a5276`, top center):** "Two Kinds of Scale: Requests per Second vs Source Systems".
- **Axes:** origin x=70, baseline y=245, plot width 580, plot height 185; 2px `#999` axis lines; x tick labels (12px `#444`) "100" at x=160, "10k" at x=360, "1M" at x=560 with 12px `#444` axis label "traffic (requests per second, schematic log scale)" centered at y=278; y tick labels "10" at y=225, "100" at y=150, "300" at y=75 with gridlines `#e5e9ef`, and 12px `#444` label "source systems" near the top-left at (70, 48).
- **Consumer points:** blue `#2a78d6` filled dots radius 6 at (470, 226) "video streaming", (560, 220) "social feed", (612, 231) "web search" — 12px `#2c3e50` labels just above each dot.
- **Platform point:** magenta `#d55181` filled dot radius 8 at (180, 75), bold 13px magenta label "data-integration platform — 300 sources, modest traffic" to its right.
- **Annotation (bold 13px green `#008300`, near x=300, y=115):** "same word 'scale', different axis".
- **Caption (12px `#444`, bottom right):** "positions schematic, illustrative".

## Three Spellings and Two Units for One Container

**Tags:** `worked example` (blue), `schema mapping` (green), `reconciliation` (orange)

- **One box, three records** — container MSCU4810027 sits in the mainframe, a spreadsheet, and a customs log
- **Three spellings** — "MSCU4810027", "MSCU 481 002 7", "mscu-4810027" must resolve to one container_id
- **Two units** — the mainframe says 42,300 lb and the spreadsheet says 19.2 t; both are ≈19,187 kg
- **The mapping** — declared once per source: CNTR_NO → container_id, ETA_DT → eta, WGT_LB → weight_kg
- **The old way** — "which shipments miss their window?" took 7 systems and 11 hand-written joins
- **The new way** — the same question is one query against Shipment, with the joins already declared

*Example (italic):* After the mapping, the analyst writes one query against Shipment.eta — the platform, not the analyst, remembers that ETA_DT is a Julian date inside a 1987 mainframe.

**Key point:** The ontology is the contract: map each source's tables and columns onto canonical objects once, and every analyst afterward queries objects — the 300 schemas become an implementation detail.

### Visualization (canvas `c2`, 720×300)

Mapping flow diagram: three heterogeneous source boxes on the left, per-source transform arrows into one canonical Shipment object, one arrow out to the analyst.

- **Title (bold 15px, `#1a5276`, top center):** "Map Each Schema Once; Analysts Query One Shipment Object".
- **Source boxes (x=20, width 200, height 54, radius 8, fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border, two-line 12px `#2c3e50` text) at y = 62, 132, 202:** "CARGO_MST — 1987 mainframe / CNTR_NO · ETA_DT · WGT_LB", "Port ops spreadsheet / Container · Arrival · Weight (t)", "Customs clearance log / cntr_id · cleared_at".
- **Ontology box:** x=310, y=105, width 180, height 108, radius 8, fill `rgba(0,131,0,0.12)`, 2px `#008300` border; bold 13px `#1a5276` header "Shipment", then 12px `#2c3e50` lines "container_id", "eta", "weight_kg", "customs_status".
- **Mapping arrows:** 2px `#6b7280` arrows from each source box's right edge to the ontology box's left edge, each with an 11px `#6b7280` transform label at midpoint: "Julian date → eta, lb → kg" (top), "normalize id, t → kg" (middle), "epoch → status" (bottom).
- **Analyst box:** x=560, y=131, width 140, height 56, radius 8, fill `rgba(213,81,129,0.12)`, 2px `#d55181` border, 12px text "analyst: 1 query (was 7 systems, 11 joins)"; 3px `#008300` arrow from the ontology box into it.
- **Annotation (bold 12px violet `#4a3aa7`, centered above the ontology box at y=90):** "3 spellings, 2 units → 1 object".
- **Caption (12px `#444`, bottom right):** "columns simplified, illustrative".

## When a Number Looks Wrong, Lineage Answers Why

**Tags:** `where it's used` (blue), `lineage` (green), `provenance` (orange)

- **The scare** — the weekly late-shipment rate jumps from 4% to 21% and nobody trusts the report
- **Lineage** — every derived dataset records its exact inputs and the transform that produced it
- **The walk** — report ← shipment join ← cleaned ETAs ← port spreadsheet, 3 hops back to a source
- **The culprit** — the spreadsheet's arrival column silently flipped from DD/MM to MM/DD mid-quarter
- **The verdict** — no real surge in lateness, just misparsed dates on one upstream column
- **The rule** — a derived number without provenance cannot be debugged, only argued about

*Example (italic):* The 21% figure was never a surge in late shipments — lineage showed every new "late" row came through one spreadsheet column whose date format had flipped.

**Key point:** Pipeline lineage — every dataset knowing its inputs and transform — turns "the number looks wrong" from an all-hands archaeology dig into a 3-hop walk upstream to the broken column.

### Visualization (canvas `c3`, 720×300)

Left-to-right lineage DAG from two sources through cleaning and a join to the final report, with the bad path traced backward in red across 3 hops.

- **Title (bold 15px, `#1a5276`, top center):** "A 21% Late Rate Traced 3 Hops Back to One Date Column".
- **Node style:** rounded boxes 150px wide, 44px tall, radius 8, 12px `#2c3e50` text; default fill `rgba(42,120,214,0.15)` with 2px `#2a78d6` border.
- **Nodes:** "port spreadsheet" at (30, 70) — culprit, fill `rgba(231,76,60,0.12)`, 2px `#e74c3c` border, bold 11px `#e74c3c` sublabel "DD/MM → MM/DD flip"; "CARGO_MST" at (30, 185); "cleaned ETAs — parse dates" at (230, 70); "cleaned cargo" at (230, 185); "shipment join" at (420, 128); "late report — 21% (was 4%)" at (585, 128), width 120.
- **Edges:** 2px `#6b7280` arrows spreadsheet→cleaned ETAs, CARGO_MST→cleaned cargo, cleaned ETAs→join, cleaned cargo→join, join→report; the path spreadsheet→cleaned ETAs→join→report redrawn as 3px `#e74c3c`, each red segment tagged bold 11px `#e74c3c` "hop 3", "hop 2", "hop 1" (numbered walking backward from the report).
- **Annotation (bold 13px green `#008300`, centered near x=300, y=268):** "provenance turns panic into a 3-hop walk".
- **Caption (12px `#444`, bottom right):** "rates illustrative".

## Security Bolted On Is Security Leaked

**Tags:** `common mistake` (red), `access control` (orange)

- **The constraint** — customs rows, personal data, and contract margins each have need-to-know audiences
- **First-class** — access rules live at row and column level and travel into every derived dataset
- **The bolt-on** — restrict only the source table and one exported CSV strips all protection downstream
- **The damage** — that unprotected copy spread restricted margins into 14 further reports
- **The check** — a derived dataset inherits the tightest policy of its inputs, enforced at query time
- **The result** — an uncleared analyst runs the same report and simply sees the margin column redacted
- **The effort** — integration, reconciliation, and access control dominate the engineering here, not QPS

*Example (italic):* An analyst without margin clearance runs the same report and simply sees the margin column redacted — the policy followed the data through two derived datasets.

**Common mistake:** Treating access control as a wrapper around the database. If policy doesn't propagate into derived datasets, the first export defeats it — need-to-know must be a property of the data, not of the door.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: a bolt-on design where an export strips the policy (leak) vs a built-in design where the policy travels with derived data (redaction holds).

- **Title (bold 15px, `#1a5276`, top center):** "Access Control Bolted On vs Built In".
- **Box style:** rounded boxes 150–170px wide, 44px tall, radius 8, 12px `#2c3e50` text; fills `rgba(42,120,214,0.15)` (blue, 2px `#2a78d6`), `rgba(230,126,34,0.15)` (orange, 2px `#e67e22`), `rgba(231,76,60,0.12)` (red, 2px `#e74c3c`), `rgba(0,131,0,0.12)` (green, 2px `#008300`); 3px `#6b7280` arrows between boxes.
- **Row 1 (boxes at y=78), label 12px `#444` "bolt-on" at x=20:** blue box at x=100 "shipment table (margin restricted)" → orange box at x=320 "analyst exports CSV" → red box at x=540 "copy has no policy — margin in 14 reports", with bold 12px red `#e74c3c` "✗ leak" just below it.
- **Row 2 (boxes at y=192), label "built-in" at x=20:** blue box at x=100 "same table + row/column policy" → green box at x=320 "derived report inherits policy" → green box at x=540 "uncleared analyst sees margin redacted", with bold 12px green `#008300` "✓ holds" just below it.
- **Annotation (bold 13px orange `#d95926`, centered near y=274):** "policy must be a property of the data, not the door".
- **Caption (12px `#444`, bottom right):** "report counts illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points, boxes, and rates are the hardcoded values above (no randomness); the source count (300), query counts (7 systems / 11 joins vs 1 query), late rates (4% / 21%), and report count (14) are invented and labeled illustrative; the weight reconciliation holds to one decimal (42,300 lb = 19,187 kg ≈ 19.2 t).
- **Framing:** generic data-integration-platform design using only publicly described product ideas (ontology, pipelines, lineage, fine-grained access control); make no claims about any company's internal systems.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
