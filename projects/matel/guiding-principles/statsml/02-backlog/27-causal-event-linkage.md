# Causal Event Linkage: Source/Target ID over Timestamp Ordering

**Page type:** detail page (backlog kusto-style 2-col layout: text left 45%, canvas right 55%, one `.card-section` per numbered section)
**HTML title tag:** Causal Event Linkage — Discussion Backlog

**Status badge:** TO DISCUSS

**Subtitle:** When event B was *caused by* event A, the logging system must record that link explicitly (source_event_id → target_event_id). Reconstructing causal chains from timestamp proximity is fragile — events arrive out of order, share millisecond timestamps, or get batched and delayed.

## 1. Motivating Example: Search → View

A user searches "running shoes", sees results, clicks a product, views the product page. That view was *caused by* one specific search-result impression — a directed link, not two events that happen to sit near each other on the clock.

- `search_event_id: s_123` → produced `impression_id: imp_456`
- `impression_id: imp_456` → caused `product_view_id: pv_789`
- Each hop carries an explicit **source_id** and **target_id** — the chain is stated, not guessed

**What timestamp-only ordering gets wrong**

- **Out-of-order delivery** — the product_view is logged before the search (async pipelines, uneven service latency).
- **Batched writes** — one flush stamps dozens of events with an identical timestamp.
- **Concurrent actions** — two tabs open: search in A, browsing in B. Timestamps interleave; causality does not cross tabs.
- **Clock skew** — the client phone clock sits seconds behind the search service clock.
- **Retry storms** — the same event lands repeatedly with slightly different timestamps; which one is real?

**Open question (orange-accent callout):** Without linkage, "user searched then viewed" may actually mean: searched, gave up, browsed the homepage, clicked a recommendation, viewed the product. The timestamps read the same; the causal path is completely different.

**Key point (red-accent callout):** **The risk:** analysis that infers search→view from proximity builds false funnels, miscalculates conversion rates, and attributes revenue to the wrong source.

### Visualization (canvas `c1`, 720×380)

Two stacked panels: explicit causal links vs the same events flattened onto one timestamp axis.

- **Title (bold 14px, `#1a5276`, centered, y=25):** "True Causal Path vs Timestamp-Inferred Path".
- **Panel A** (header 11px `#666`: "A. Logged with explicit links (caused_by_event_id)"):
  - Row 1: box "search / s_123" (130×38 at 30,60, blue `#1a5276`) —solid green arrow→ box "impression / imp_456" (140×38 at 210,60) —dashed orange arrow→ orange 10px text "abandoned — no click, chain ends".
  - Row 2: box "homepage / hb_311" (30,116) —green arrow labeled "caused_by"→ box "reco_click / rc_222" (210,116) —green arrow labeled "caused_by"→ box "product_view / pv_789" (150×38 at 400,116, green `#27ae60` border). Gray 10px note at right: "real path: homepage → recommendation → view".
  - Boxes fill `rgba(26,82,118,0.06)`; arrows solid green `#27ae60` (real links) or dashed orange `#e67e22` (abandoned).
- **Dashed `#ccc` divider** at y=176.
- **Panel B** (header: "B. Same events on one timestamp axis (linkage dropped)"):
  - Horizontal time axis at y=300 from x=60 to 680, "time →" label.
  - Event dots (4px radius, blue `#1a5276`; product_view green `#27ae60`) with name above and timestamp below (9px `#888`): search (x=95, t+0.0s), impression (x=155, t+0.4s), homepage (x=330, t+12s), reco_click (x=500, t+20s), product_view (x=570, t+21s).
  - **Spurious edge:** red dashed (`#e74c3c`, dash 5/4, width 2) quadratic arc from search to product_view, arrowhead at the end, labeled in bold 10px red centered at (330,246): "SPURIOUS: search → product_view (false funnel)".
  - **True edge:** solid green (`#27ae60`, width 2) small arc from reco_click to product_view, labeled "true edge" (10px green).
- **Caption (11px `#888`, bottom center):** "Adjacency on the clock is not a causal edge — the search was abandoned".

## 2. Where Timestamp Ordering Fails — and Why

Domains where cause and effect are far apart, interleaved, or reversed on the wall clock:

- **Ad click → conversion** — click on day 1, purchase on day 14. The gap is huge; only a `click_id` carried on the conversion event ties them. Otherwise attribution is guesswork.
- **Security alert → investigation → remediation** — an analyst works a queue of alerts and starts on #3 before closing #1, so the clock reports the wrong causal order.
- **API request → downstream calls** — A calls B calls C, and C's response lands before B's span closes. Distributed tracing (`trace_id`, `span_id`, `parent_span_id`) exists precisely because timestamps do not preserve call trees.
- **Medical order → lab draw → result** — the result timestamp is when the system processed it, not when blood was drawn or when the doctor ordered it. Three timestamps, one chain; only `order_id` links them.
- **CI/CD commit → build → test → deploy** — a build can start before the previous commit's tests finish. The pipeline DAG defines causality, not the wall clock.
- **Trade → settlement → confirmation** — settlement is T+2 and confirmations can arrive out of order; only `trade_id` spans the lifecycle.

**The structural problem** — timestamps give a **total order** of events, but causality is a **partial order** (a DAG). A DAG cannot be reconstructed from a total order without extra information.

- Two events may be **concurrent** with no causal link — the clock still picks an arbitrary winner.
- C may **cause** D even though D's timestamp is earlier (network delay, clock skew).
- Two events at the same timestamp may be causally linked (sub-millisecond) or entirely independent.

**Open question (orange-accent callout):** This is Lamport clocks 101: "happened before" is not "has an earlier timestamp." Distributed systems settled this decades ago; data analytics still treats ORDER BY timestamp as causality.

### Visualization (canvas `c2`, 720×380)

Side-by-side: timestamp-sorted event list with an implied (wrong) chain vs the true happened-before DAG.

- **Title (bold 14px, `#1a5276`, centered, y=25):** "Total Order (clock) vs Partial Order (causality)".
- **Left column** (header bold 11px red `#e74c3c`: "ORDER BY event_ts"): six rows (260×26 boxes at x=40, 40px apart from y=64), each showing timestamp (10px `#888`) and event (bold 11px `#1a5276`):
  | Timestamp | Event | Tie highlight |
  |---|---|---|
  | 12:00:01.900 | A  search | no |
  | 12:00:03.412 | C  alert_open | yes (orange fill/stroke) |
  | 12:00:03.412 | D  alert_close | yes (orange fill/stroke) |
  | 12:00:04.100 | F  lab_result | no |
  | 12:00:05.020 | B  impression | no |
  | 12:00:07.640 | E  deploy | no |
  - Dashed red arrows chain the rows top-to-bottom; caption below: "implied chain (wrong)" (10px red). Side note in orange: "identical ts —" / "order arbitrary".
- **Right column** (header bold 11px green `#27ae60`: "happened-before (DAG)"): nodes 90×28 — "A  search" (500,64, blue), "C  alert_open" (415,132, blue), "B  impression" (605,132, blue), "D  alert_close" (415,200, blue), "F  lab_result" (605,200, orange `#e67e22`), "E  deploy" (500,272, gray `#999`, dashed border). Green arrows: A→C, A→B, C→D; orange arrow B→F. Annotations (9px): orange "effect ts EARLIER" / "than its cause" under F; gray "concurrent — no causal edge" under E.
- **Caption (11px `#888`, bottom center):** "Ties, skew and concurrency are unrecoverable once the DAG is flattened to a clock order".

## 3. What Good Linkage Looks Like

**Explicit causal fields** — clock-independent, stated at write time:

- `event_id` — unique identifier for this event.
- `caused_by_event_id` — the event that triggered this one; nullable for a root or user-initiated event.
- `correlation_id` — groups every event in a single logical flow (like a trace_id).
- `sequence_number` — monotonic counter within a flow, giving ordering without trusting any clock.

**What this enables for analysis**

- **True funnel construction** — follow the chain, not the clock.
- **Accurate attribution** — which search, ad, or recommendation actually caused the purchase.
- **Latency measurement** — time between cause and effect, not between arbitrary adjacent events.
- **Anomaly detection** — a broken chain (effect with no cause) flags a logging bug or system failure.
- **Counterfactual analysis** — what happens to downstream events when a cause is removed.

**Key point (red-accent callout):** **Why it matters:** a single cause usually has many plausible downstream events inside any window. The explicit link collapses that fan-out to exactly one edge — no scoring, no tie-breaking, no window to tune.

### Visualization (canvas `c3`, 720×300)

Fan-out diagram: one cause box with six candidate downstream events; only the explicitly linked one is a real edge.

- **Title (bold 14px, `#1a5276`, centered, y=25):** "Fan-out: Which Downstream Event Was Actually Caused?".
- **Cause box:** 150×46 at (26,130), fill `rgba(26,82,118,0.06)`, stroke `#1a5276` width 2; bold "impression" plus "imp_456  (the cause)" (10px `#555`).
- **Candidates (220×24 boxes at x=430, 36px apart from y=48):**
  | Candidate | Real link? |
  |---|---|
  | product_view  pv_789 | yes — green `#27ae60`, solid border width 2, fill `rgba(39,174,96,0.08)`, bold text, thick solid green arrow |
  | add_to_cart  ac_301 | no |
  | homepage_view  hv_012 | no |
  | product_view  pv_802 | no |
  | search  s_999 | no |
  | reco_click  rc_222 | no |
  - Non-real candidates: dashed gray `#999` border, fill `rgba(26,82,118,0.04)`, thin dashed `#ccc` arrow, red "?" (11px `#e74c3c`) beside each arrowhead.
- **Annotations:** bold 10px green at (300,42): "caused_by_event_id = imp_456"; gray 10px at (300,208/222): "all candidates fall inside the same 30s window," / "same user, same session".
- **Caption (11px `#888`, bottom center):** "Time and session cannot separate the candidates — the explicit link keeps exactly one edge".

## 4. Key Discussion Points

- How does the pipeline **detect** that an analysis is relying on timestamp proximity to infer causality? Candidate signal: the assumed sequence breaks when events are reordered.
- When explicit linkage does not exist in the data, can probable causal chains be **reconstructed** — probabilistic matching on key + time window + constraints — and what confidence should be reported alongside it?
- What **metadata** must a logging system capture at write time to make causal analysis possible downstream at all?
- **Entity key granularity (#25)** — the right key for a causal chain is the chain itself, not any single entity.
- **Temporal dataset handling (#17)** — temporal ordering is necessary but not sufficient for causality.

**Key point (red-accent callout):** **The window trap:** widening the join window lifts coverage and destroys precision; narrowing it does the reverse. Any single window is a magic number chosen to make the report look reasonable — explicit IDs remove the choice entirely.

*Example: same clickstream, same conversions — reported attribution swings by a wide margin depending only on whether the analyst joined on a 1-minute or a 1-day window.*

### Visualization (canvas `c4`, 720×300)

Combo chart: bars for match rate plus two lines (precision, coverage) as a function of join-window width.

- **Title (bold 14px, `#1a5276`, centered, y=25):** "Attribution Result vs Join-Window Width".
- **X categories (log spacing):** 1s, 10s, 1m, 10m, 1h, 1d; x-axis label "join window width (log spacing)".
- **Data (percent of events, y-scale 0–100 labeled 0/25/50/75/100):**
  | Window | Effects matched to some cause (bars) | Precision of the match (green line) | True links recovered (orange line) |
  |---|---|---|---|
  | 1s | 12 | 96 | 11 |
  | 10s | 31 | 88 | 27 |
  | 1m | 54 | 71 | 46 |
  | 10m | 73 | 52 | 58 |
  | 1h | 88 | 33 | 62 |
  | 1d | 97 | 19 | 64 |
- **Marks:** bars fill `rgba(26,82,118,0.35)`, width 42% of slot; lines width 2 with 3px dots — precision `#27ae60`, coverage `#e67e22`. Plot area x=62, width w−120, y=56, height 186; gridlines `#f0f0f0`; rotated y-axis label "Percent of events (%)".
- **Legend (inside plot, top-left):** blue swatch "effects matched to some cause"; green swatch "precision of the match"; orange swatch "true links recovered".
- **Reference line:** dashed red (`#e74c3c`, dash 5/4) horizontal line at 100%, right-aligned label: "explicit caused_by_event_id — window independent".
- **Caption (11px `#888`, bottom center):** "Every window is a magic number — precision and coverage trade off with no correct crossing point".

## Regeneration instructions

- **Layout:** backlog detail page. Body → h1 → `.status` badge ("TO DISCUSS") → `.subtitle` → one `.card-section` per numbered section, each an `<h2>` plus a `table.layout` with one `<tr>`: left `td.text-col` (45%) for paragraphs/bullets/`.key-point`/`.questions`/`.example`, right `td.viz-col` (55%) for the canvas.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6. h1 2rem `#1a5276` with `border-bottom: 2px solid #2980b9`. `.subtitle` `#666` 0.95rem. `.status` inline-block pill: background `#e8f0f8`, color `#1a5276`, padding 3px 10px, radius 12px, 0.8rem bold. h2 1.3rem `#1a5276` with 2px `#2980b9` bottom border. `.key-point` background `#f8f9fa`, `border-left: 3px solid #e74c3c`. `.questions` background `#f8f9fa`, `border-left: 3px solid #e67e22`. `.example` italic `#555` 0.9rem. `strong` in `#1a5276`; `code` background `#e8f0f8`, color `#1a5276`. Canvas: `width: 100%`, border `1px solid #e0e0e0`, radius 4px. No nav bar, no back/home links, no index number in h1.
- **Palette:** primary blue `#1a5276`, accent blue `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, bar fill `rgba(26,82,118,0.35)`.
- **Canvas:** intrinsic width/height attributes per chart (c1, c2 are 720×380; c3, c4 are 720×300); a `setup(id)` helper (with inline equivalents for 380-tall canvases) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Shared `box(...)` and `arrow(...)` helpers draw labeled rectangles and arrowheaded lines. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- Regenerated HTML has no card links (detail page); any links elsewhere use `.html` extensions.
