# On-Prem vs Cloud — Where ML Models and LLMs Run

**Page type:** detail page (backlog-style two-column layout: text left 45%, canvas right 55%, one `.lang-section` per topic; h1 carries a BACKLOG status pill)
**HTML title tag:** On-Prem vs Cloud — Where ML Models and LLMs Run

**Subtitle:** Every ML deployment picks a point between a cloud datacenter and the device in your hand.

**Intro callout:** Three axes decide placement: the compute a model needs, the hardware a rung offers, and how critical the decision is when the network is gone.

## 1. The Deployment Spectrum

Placement is a spectrum with four rungs.

- **Four rungs** — cloud, on-prem servers, edge gateways, on-device.
- **One trade** — compute and scale swap for latency, privacy, offline work.
- **On-prem is not edge** — it serves compliance and residency, not latency.
- **"Device" spans five orders** — car computer, phone NPU, watch SoC, MCU sensor.

**Key point:** Pick the rung per workload — one product often spans three or four rungs.

### Visualization (canvas `c1`, 720×320)

Spectrum diagram: four labeled boxes left to right, with opposing trade-off arrows above and below.

- **Title (bold 16px, `#1a5276`, top center):** "The Deployment Spectrum".
- **Boxes:** four rounded rectangles (radius 8) in a row, y=90, height 120, width = (canvas−80−3·18)/4, gap 18, starting x=40. Fill white, stroke 2px in the box color; name bold 15px centered in the box color at box top+28; two example lines 12px `#5a6875` centered below the name (16px line spacing).
  - "Cloud" `#1a5276` — lines: "frontier LLMs", "training, elastic scale"
  - "On-Prem Server" `#2980b9` — lines: "GPU racks", "data residency"
  - "Edge Gateway" `#27ae60` — lines: "factory server", "5G MEC node"
  - "On-Device" `#e67e22` — lines: "car, phone, watch", "IoT sensor"
- **Top arrow:** horizontal arrow pointing LEFT, from x=canvas−60 to x=60 at y=62, stroke `#1a5276` 2px with filled triangular head; label "raw compute, elastic scale" (13px `#1a5276`, centered above at y=52).
- **Bottom arrow:** horizontal arrow pointing RIGHT, from x=60 to x=canvas−60 at y=252, stroke `#27ae60` 2px with filled triangular head; label "lower latency · more privacy · works offline" (13px `#27ae60`, centered below at y=276).

## 2. Computation Complexity — What Fits Where

Before accuracy or cost, the model must fit its rung.

- **Footprint first** — weights plus activations must fit the rung's memory.
- **Hard limit** — a 70B LLM fits in no phone, however scheduled.
- **Quantization moves the line** — int4, pruning, distillation shrink 4–10×.
- **The payoff** — 1–3B LLMs moved from server-only to on-device.
- **Ops vary wildly** — wake word: millions; vision frame: billions.
- **LLMs repeat it** — billions of ops per generated token.

**Key point:** "Can it run on-device" is arithmetic: footprint and ops vs RAM and speed.

### Visualization (canvas `c2`, 720×320)

Log-scale bar chart: model memory footprint by model class, with device memory ceilings as dashed lines.

- **Title (bold 16px, `#1a5276`, top center):** "Model Footprint vs Device Ceilings (MB, log scale)".
- **Data (footprint in MB):** wake word `0.2`, TinyML vision `2`, mobile CNN `20`, speech model `200`, 3B LLM int4 `1500`, 70B LLM `40000`, frontier LLM `1000000`.
- **Plot area:** x=76, y=48, width = canvas−140, height = canvas−120; L-shaped axes `#95a5a6` (1.4px).
- **Y scale:** log₁₀ from 0.1 to 1e6; tick labels at each decade `0.1, 1, 10, 100, 1K, 10K, 100K, 1M` (11px `#5a6875`, right-aligned).
- **Bars:** 7 slots, bar width 0.6·slot; fill `rgba(41,128,185,0.5)`, stroke `#2980b9` 1.4px; value implied by height only.
- **X labels:** class names (10px `#4a5866`, centered under each slot, two lines where needed: "wake word", "TinyML vision", "mobile CNN", "speech", "3B LLM int4", "70B LLM", "frontier LLM").
- **Ceiling lines (dashed 5/4, 2px, label 12px left-aligned at x=plotX+6 just above the line):** green `#27ae60` at 2 MB, label "MCU-class ceiling"; orange `#e67e22` at 8000 MB, label "phone-class ceiling (~8 GB)".

## 3. The Hardware Ladder

Each rung offers a fixed budget of silicon and power.

- **Eight orders of magnitude** — ~0.01 TOPS MCU up to million-TOPS cloud cluster.
- **Power is the real budget** — milliwatt sensor, 5 W phone, megawatt datacenter.
- **Power caps first** — the energy budget binds long before silicon does.
- **NPUs changed the default** — phones ship accelerators; on-device is normal now.

**Key point:** Hardware sets a hard ceiling per rung — never assume the rung will grow.

### Visualization (canvas `c3`, 720×320)

Log-scale bar chart: compute throughput (TOPS) per device class.

- **Title (bold 16px, `#1a5276`, top center):** "Compute Throughput by Rung (TOPS, log scale)".
- **Data (TOPS):** IoT MCU `0.01`, wearable SoC `0.5`, phone NPU `40`, car autonomy computer `500`, on-prem 8-GPU server `8000`, cloud cluster `1000000`.
- **Plot area:** x=76, y=48, width = canvas−140, height = canvas−120; L-shaped axes `#95a5a6` (1.4px).
- **Y scale:** log₁₀ from 0.01 to 1e6; tick labels at each decade `0.01, 0.1, 1, 10, 100, 1K, 10K, 100K, 1M` (11px `#5a6875`, right-aligned).
- **Bars:** 6 slots, bar width 0.6·slot; fill `rgba(230,126,34,0.5)`, stroke `#e67e22` 1.4px.
- **X labels:** (10px `#4a5866`, centered, two lines where needed): "IoT MCU", "wearable", "phone NPU", "car computer", "on-prem server", "cloud cluster".
- **Annotation (13px `#e74c3c`, left-aligned inside plot near top-left):** "~8 orders of magnitude end to end".

## 4. Criticality — When the Network Can't Be in the Loop

The deciding axis is the cost of a missed deadline, not average latency.

- **No network in the braking loop** — at 65 mph a car covers ~29 m per second.
- **200 ms round trip** — ~6 m of blind travel, and the network guarantees nothing.
- **Failure mode decides** — assistant timeout annoys; lane-keeper timeout crashes.
- **Degrade locally** — autonomy, pacemakers, shutoffs keep the loop on-device.
- **Cloud is auxiliary** — telemetry, fleet learning, and model updates only.

**Key point:** If a missed deadline costs safety, the model belongs on the device.

### Visualization (canvas `c4`, 720×320)

Bar chart: distance a car travels at 65 mph while waiting for an inference answer, per deployment path.

- **Title (bold 16px, `#1a5276`, top center):** "Distance Traveled at 65 mph Awaiting a Decision".
- **Data (latency → metres at 29 m/s):** on-board 10 ms `0.3`, edge/5G MEC 50 ms `1.5`, cloud (good network) 200 ms `5.8`, cloud (congested) 1000 ms `29`.
- **Plot area:** x=66, y=48, width = canvas−130, height = canvas−120; linear y from 0 to 30 metres, ticks every 5 (12px `#5a6875`, right-aligned); L-shaped axes `#95a5a6` (1.4px).
- **Bars:** 4 slots, bar width 0.55·slot; fills/strokes per bar: on-board `rgba(39,174,96,0.5)`/`#27ae60`; edge `rgba(230,126,34,0.5)`/`#e67e22`; both cloud bars `rgba(231,76,60,0.5)`/`#e74c3c`; stroke 1.4px.
- **Value labels:** metres value + " m" (12px, bar's stroke color, centered above each bar).
- **X labels:** (11px `#4a5866`, centered, two lines): "on-board / 10 ms", "edge 5G / 50 ms", "cloud / 200 ms", "congested / 1000 ms".
- **Annotation (13px `#e74c3c`, right-aligned near the last bar's top, offset left):** "~7 car lengths, blind".

## 5. Connectivity, Privacy & Data Gravity

Where data may travel constrains placement more than compute does.

- **Data gravity** — a car senses terabytes daily; its uplink carries gigabytes.
- **Model to the data** — raw sensor streams can never reach a cloud model.
- **Privacy by placement** — on-device means raw data never leaves the device.
- **What stays local** — heart rhythms on a watch; photos, keystrokes on a phone.
- **On-prem is sovereignty** — prompts, weights, logs stay inside the boundary.
- **Who runs it** — banks, hospitals, and governments on their own racks.

**Key point:** Decide where the data may live first, then where the model can.

### Visualization (canvas `c5`, 720×320)

Log-scale funnel bar chart: a connected car's daily data volume shrinking from sensed to retained.

- **Title (bold 16px, `#1a5276`, top center):** "Connected Car — Daily Data Funnel (GB, log scale)".
- **Data (GB/day):** sensors generate `4000`, processed on-board `4000`, events summarized `50`, uploaded `2`, retained in cloud `0.5`.
- **Plot area:** x=76, y=48, width = canvas−140, height = canvas−120; L-shaped axes `#95a5a6` (1.4px).
- **Y scale:** log₁₀ from 0.1 to 10000; tick labels at each decade `0.1, 1, 10, 100, 1K, 10K` (11px `#5a6875`, right-aligned).
- **Bars:** 5 slots, bar width 0.6·slot; first two bars fill `rgba(39,174,96,0.5)` stroke `#27ae60` (stays on the car), last three fill `rgba(41,128,185,0.5)` stroke `#2980b9` (leaves the car); stroke 1.4px.
- **Value labels:** `4 TB, 4 TB, 50 GB, 2 GB, 0.5 GB` (12px, bar's stroke color, centered above each bar).
- **X labels:** (10px `#4a5866`, centered, two lines): "sensors / generate", "processed / on-board", "events / summarized", "uploaded", "retained / in cloud".
- **Annotation (13px `#27ae60`, left-aligned under the title at plot top-left):** "the model goes to the data".

## 6. Hybrid Patterns — Small Local, Big Remote

Real systems route across the spectrum rather than pick one rung.

- **Small local, big remote** — a distilled on-device model serves the common case.
- **Escalate the tail** — hard requests go to a larger cloud or on-prem model.
- **Train centrally, infer locally** — devices get quantized snapshots over the air.
- **Federated learning** — devices ship gradient updates instead of raw data.
- **The router is the design** — escalation policy drives cost, latency, privacy.
- **Routing signals** — confidence, task type, battery, connectivity state.

**Key point:** Placement is a routing problem — the failures hide in the routing policy.

### Visualization (canvas `c6`, 720×320)

Horizontal stacked bars: share of requests served on-device vs escalated to cloud, per workload.

- **Title (bold 16px, `#1a5276`, top center):** "Who Serves the Request — On-Device vs Cloud".
- **Data (percent local / percent cloud):** wake word `100/0`, photo enhancement `95/5`, dictation `90/10`, assistant Q&A `30/70`, agentic coding `2/98`.
- **Plot area:** x=150, y=56, width = canvas−230, height = canvas−130; 5 horizontal slots, bar height 0.55·slot.
- **Segments:** left segment (local) fill `rgba(39,174,96,0.55)` stroke `#27ae60` 1.2px; right segment (cloud) fill `rgba(41,128,185,0.55)` stroke `#2980b9` 1.2px; segments span plot width proportionally.
- **Row labels:** workload names (12px `#4a5866`, right-aligned at x=142, vertically centered per slot).
- **Percent labels:** local percent (11px `#1e6f43`) centered in the green segment when ≥10%, cloud percent (11px `#1a5276`) centered in the blue segment when ≥10%.
- **Legend (below plot, centered):** green swatch + "served on-device", blue swatch + "escalated to cloud" (13px `#2c3e50`).
- **X axis:** baseline only `#95a5a6` 1.4px with "0%", "50%", "100%" ticks (11px `#5a6875`).

## Regeneration instructions

- **Layout:** backlog detail page. `h1` (2rem `#1a5276`, bottom border `2px solid #2980b9`) with inline `.status` pill "BACKLOG" (background `#fef9e7`, border `1px solid #f39c12`, text `#b7950b`, 4px radius, 0.8rem); `.subtitle` (`#666`, 0.95rem); `.intro` callout (background `#f0f4f8`, left border `3px solid #2980b9`, 8px 12px padding, 0.9rem). One `.lang-section` per numbered h2 (1.3rem `#1a5276`, bottom border `2px solid #2980b9`); inside each, `table.layout` with `td.text-col` 45% and `td.viz-col` 55%, both `vertical-align: top`, 12px padding. No index number in the h1.
- **Text blocks:** intro `<p>`, `<ul>` bullets (0.92rem) with `<strong>` lead-ins, `.key-point` callout (background `#f8f9fa`, left border `3px solid #e74c3c`, 0.9rem).
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6. Canvases `width: 100%`, `1px solid #e0e0e0` border, 4px radius.
- **Palette:** `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange; secondary `#2980b9`; gray labels `#5a6875`/`#4a5866`, axes `#95a5a6`.
- **Canvas:** intrinsic 720×320; sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`), `ctx.scale` back to logical coordinates via a shared `setupCanvas(id)` helper.
- In regenerated HTML, any card links use `.html` extensions (this page has none).
