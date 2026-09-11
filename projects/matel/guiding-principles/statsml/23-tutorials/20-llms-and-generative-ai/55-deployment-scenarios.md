# Deployment Scenarios

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Deployment Scenarios

**Subtitle:** Rent a hosted endpoint, run weights on your own cloud GPUs, or serve a model off the laptop — four choices, four different bills

## Four Places a Model Can Run

**Tags:** `core idea` (blue), `four topologies` (green), `trade-offs` (orange)

- **The one axis** — every option differs on a single question: who operates the GPU the tokens come out of
- **1. Vendor's hosted API** — you send a prompt over the internet; they own weights, hardware, scaling, and uptime
- **2. Cloud managed service** — the same kind of weights served inside your own cloud account, network, and invoice
- **3. Your own GPU instances** — you rent hyperscaler GPU machines (AWS, GCP, Azure) and run open weights yourself
- **4. On-device / local** — a quantized model on a laptop, workstation, or phone, answering with the network unplugged
- **Data boundary** — vendor, then your cloud account, then your VPC, then the device; compliance follows it
- **Cost shape** — per-token variable on the top two, per-GPU-hour fixed on your own boxes, one-time hardware locally
- **Latency and ceiling** — a network round trip buys frontier quality; local memory bandwidth caps you at what fits
- **Scaling and ops** — elastic and nobody's problem, versus provisioning peak plus drivers, patching, and evals

*Example (italic):* On option 1 the prompt travels to another company's data centre; on option 4 it never leaves local memory — the same question under two different data boundaries.

**Key point:** Pick by asking who has to operate the GPU, because that one answer moves the data boundary, the cost shape, and the ops burden together — and no option wins every row.

### Visualization (canvas `c1`, 720×300)

Topology × dimension matrix, four rows by six columns, each cell a short verdict coloured by whether it helps you or is the cost you take on, with a left-hand bracket showing how much of the stack you operate.

- **Title (bold 15px, `#1a5276`, top center):** "Four Topologies, Six Dimensions".
- **Grid geometry:** row-label column x=30 → 186; six data columns of width 78 starting at x=190 (lefts 190, 268, 346, 424, 502, 580; right edge 658). Header baseline y=54. Four rows, tops y = `[62, 102, 142, 182]`, height 38.
- **Column headers (bold 11px `#1a5276`, centred at column centre, y=54):** "prompt", "cost", "latency", "ceiling", "scaling", "ops".
- **Row labels (bold 11px `#1a5276`, left-aligned x=30, baseline top+24):** "1. vendor API", "2. cloud managed", "3. your GPUs", "4. on-device".
- **Cells:** rounded rect (left+3, top+3, 72×32, r=4), fill verdict colour at 12% alpha, 1px verdict-colour border; verdict text bold 11px in the verdict colour, centred at (left+39, top+23).
  - Verdict colours: green `#008300` = easier for you, yellow `#c98500` = mixed, magenta `#d55181` = the constraint you accept.
  - **Row 1 — vendor API:** "vendor" (magenta), "per-token" (yellow), "network" (yellow), "frontier" (green), "elastic" (green), "none" (green)
  - **Row 2 — cloud managed:** "your VPC" (green), "per-token" (yellow), "network" (yellow), "catalog" (yellow), "elastic" (green), "light" (yellow)
  - **Row 3 — your GPUs:** "only you" (green), "per-hour" (magenta), "in-region" (green), "fits VRAM" (yellow), "you plan" (magenta), "heavy" (magenta)
  - **Row 4 — on-device:** "only you" (green), "one-time" (green), "local" (green), "small only" (magenta), "1 user" (magenta), "updates" (yellow)
- **Left bracket:** 1.5px `#4a3aa7` vertical line at x=20 from y=62 to y=220 with 6px end ticks pointing right; rotated bold 11px `#4a3aa7` label centred on that span reading "more of it is yours to operate" (drawn with `ctx.save()`, translate to (16, 141), `rotate(-Math.PI/2)`, centred text, `ctx.restore()`).
- **Legend (three swatch/label pairs at y=242):** 9×9 swatches at x = `[150, 300, 470]` with 11px `#2c3e50` labels left-aligned 14 px to the right: green `#008300` "easier for you", yellow `#c98500` "mixed", magenta `#d55181` "the cost you take on".
- **Annotation (bold 12px orange `#d95926`, centered at y=270):** "no row is green everywhere — every step down trades ops work for control".
- **Caption (11px `#444`, bottom right, y=293):** "illustrative; verdicts are typical, not universal".

## Running It in Your Own Cloud

**Tags:** `worked example` (blue), `rule of thumb` (green), `capacity` (orange)

- **The managed option** — a cloud's managed model service gives you an endpoint in your own account with no GPU to run
- **The self-managed option** — you pick an instance by VRAM and run a serving stack on top of open weights
- **VRAM arithmetic** — 8B at 16-bit is 8e9 × 2 = 16 GB of weights, about 20 GB once cache and activations land
- **It jumps fast** — 70B at 16-bit is 140 GB of weights, about 175 GB served: a multi-GPU node, not one card
- **The serving stack earns its keep** — continuous batching and KV-cache reuse are what turn a GPU into throughput
- **Idle bills like busy** — a reserved GPU charges the same hour whether you send one request or a thousand
- **Real constraints** — cold starts on scale-up and plain GPU-instance unavailability both bite in production
- **The common hybrid** — one always-on instance for the floor, an autoscaled pool for the peaks

*Example (illustrative):* An 8B model at 16-bit needs 8e9 × 2 = 16 GB of weights, and about 20 GB with cache and activations — one 24 GB card; the same arithmetic on 70B lands at 175 GB and three 80 GB cards.

**Key point:** Utilisation is the entire economic argument for self-managed GPUs — the bill is set by hours reserved, not tokens served, so a half-idle box quietly doubles your effective per-token price.

### Visualization (canvas `c2`, 720×300)

Stacked VRAM bars for three parameter counts, with every segment and total computed at render time from parameters × bytes per parameter.

- **Title (bold 15px, `#1a5276`, top center):** "VRAM a 16-bit Model Actually Needs".
- **Computed data (no literals for the results):** `models = [{name:'8B', p:8}, {name:'24B', p:24}, {name:'70B', p:70}]` in billions; `bytesPerParam = 2`; per model `weights = p * bytesPerParam` GB, `kv = weights * 0.15`, `act = weights * 0.10`, `total = weights + kv + act` (= weights × 1.25). Yields 16/2.4/1.6 → 20.0; 48/7.2/4.8 → 60.0; 140/21/14 → 175.0.
- **Axes:** baseline y=230, plot top y=60, y scale 0–200 GB over 170 px (0.85 px per GB). Gridlines 1px `#e5e9ef` at 50/100/150/200 GB (y = 187.5, 145, 102.5, 60) spanning x=100 → 660; 11px `#6b7280` right-aligned labels at x=92: "50", "100", "150", "200". Y-axis title 11px `#444` left-aligned at (100, 50): "GB of GPU memory". Axis lines 1px `#999` from (100,60) to (100,230) to (660,230).
- **Bars:** width 90, lefts x = `[150, 320, 490]`. Stack bottom-up: weights `#2a78d6`, KV cache `#199e70`, activations `#c98500`; each segment separated by a 1px white stroke.
- **Bar labels:** bold 12px `#1a5276` centred under the baseline at y=248: model name + " params". Bold 12px `#1a5276` centred 8 px above each bar top: total formatted `total.toFixed(0) + ' GB'`. Bold 11px `#d95926` centred 24 px above the bar top: the GPU class, computed — if `total <= 80`, the smallest of `[24, 48, 80]` that is ≥ total, rendered "1 × NN GB card"; otherwise `Math.ceil(total/80) + ' × 80 GB cards'`. Gives "1 × 24 GB card", "1 × 80 GB card", "3 × 80 GB cards".
- **Legend (11px `#2c3e50`, top right):** three 10×10 swatches at x=560 and y = `[70, 86, 102]` in blue/aqua/yellow with labels 14 px to the right: "weights", "KV cache", "activations".
- **Annotation (bold 12px orange `#d95926`, centered at y=270):** "the step from one card to a multi-GPU node is where self-hosting gets hard".
- **Caption (11px `#444`, bottom right, y=293):** "illustrative; overhead assumed 25% of weights, hardware varies".

## Choosing Without Ideology

**Tags:** `rule of thumb` (blue), `common mistake` (red)

- **Start hosted** — begin on the simplest topology and move only when a named constraint actually forces it
- **The five forcing constraints** — data boundary, unit cost at proven volume, latency floor, offline use, version pinning
- **Pick by the hardest to relax** — a legal data boundary does not negotiate; a cost target usually does
- **The low-volume trap** — self-hosting "to save money" at small volume, where the fixed GPU bill dwarfs the token bill
- **Compute your own crossover** — divide the monthly instance cost by the per-token price and compare to real traffic
- **Local buys privacy, not scale** — offline and private, but capped model size, one user at a time, and battery drain
- **Keep the seam thin** — one internal interface for "generate text" so the topology can change without an app rewrite
- **Revisit on a schedule** — volume, prices, and open-weight quality all move; last year's crossover is already stale

*Example (illustrative):* At $2,400 a month for one GPU instance and $3.00 per million tokens hosted, the box only wins in the band where it is nearly full — and the crossings sit exactly where the arithmetic puts them.

**Common mistake:** Choosing the topology first and discovering the constraint afterwards — the decision is cheap while it is still a paragraph and expensive once it is a cluster.

### Visualization (canvas `c3`, 720×300)

Three monthly-cost curves against volume — hosted per-token (linear), cloud GPU (flat with capacity steps), local hardware (amortized, capacity-capped) — with every crossing computed in JS from the same constants used to plot.

- **Title (bold 15px, `#1a5276`, top center):** "Monthly Cost vs Tokens Served (illustrative)".
- **Constants (the only literals):** `rate = 3.0` dollars per million tokens hosted; `gpuCost = 2400` dollars per instance-month; `gpuCap = 1000` million tokens per instance-month; `hw = 6000` dollars of local hardware; `amortMonths = 24`; `localCap = 60` million tokens per month. Volume axis 0 → 2000 M.
- **Derived (computed, never written as text literals):** `localMonthly = hw / amortMonths` = 250; hosted cost `= rate * v`; GPU cost `= gpuCost * Math.max(1, Math.ceil(v / gpuCap))`.
- **Axes:** origin x=80, baseline y=232, plot right x=660, plot top y=62; y scale $0–$6,000 over 170 px; x scale 0–2000 M over 580 px.
  - Gridlines 1px `#e5e9ef` at $2k/$4k/$6k (y = 175.3, 118.7, 62); 11px `#6b7280` right-aligned labels at x=72: "$2k", "$4k", "$6k".
  - Axis lines 1px `#999`; x ticks at 0/500/1000/1500/2000 M (x = 80, 225, 370, 515, 660) with 11px `#444` centred labels at y=249: "0", "500M", "1B", "1.5B", "2B".
  - X-axis title 12px `#444` centred at (370, 266): "tokens served per month".
- **Hosted line:** 2.5px `#2a78d6` straight from (0, 0) to (2000, 6000) in data space; bold 11px `#2a78d6` left-aligned label at (462, 96): "hosted: $3 / 1M tokens".
- **Cloud GPU line:** 2.5px `#008300` step function sampled every 5 M with vertical risers at each `gpuCap` boundary; bold 11px `#008300` left-aligned label at (150, 148): "cloud GPU: $2,400 per instance-month".
- **Local line:** 2.5px `#4a3aa7` horizontal at $250 from v=0 to `localCap`, ending in a 1.5px `#4a3aa7` dashed vertical up 26 px; bold 11px `#4a3aa7` left-aligned note at (100, 212) built from the computed `localMonthly` and `localCap`, e.g. "local: $250/mo amortized — caps at 60M".
- **Crossings (computed):** for each instance count `k` where `k * gpuCap <= 2000`, solve `rate * v = gpuCost * k` → `v = gpuCost * k / rate`; keep it only if it lies in `((k-1)*gpuCap, k*gpuCap]`. With these constants that keeps v=800 (k=1) and v=1600 (k=2). For each kept crossing draw a 5px `#d95926` filled dot, a 1.5px `#d95926` dashed vertical to the baseline, and a bold 11px `#d95926` centred label 14 px above reading the computed volume formatted `v >= 1000 ? (v/1000).toFixed(1)+'B' : Math.round(v)+'M'`.
- **Local payback check (computed):** `payback = localMonthly / rate` (= 83.3 M). Since `payback > localCap`, print bold 11px `#4a3aa7` left-aligned at (100, 228): "pays back above " + rounded payback + "M — past its own cap", i.e. never on cost alone.
- **Annotation (bold 12px orange `#d95926`, centered at y=283):** "the GPU box only wins in the band where it is nearly full".
- **Caption (11px `#444`, bottom right, y=296):** "illustrative; hardware and prices vary".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`), matching `48-copyright-complications` exactly. Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then **three** `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Canvas list:** exactly three canvases — `c1` topology × dimension matrix, `c2` stacked VRAM bars, `c3` three-line monthly-cost chart. Intrinsic 720×300, `width:100%`.
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label). Section 1 carries 9 bullets because it absorbs both the topology list and the dimension comparison — the four dimension bullets each pack two related facts to stay inside the cap; sections 2 and 3 carry 8 each. Never more than 9 in a section.
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links, no cross-page links.
- **Canvas:** shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms). Shared `roundRect` helper.
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`.
- **Data integrity:** no `Math.random()` anywhere. c2 segments and totals are computed from `params × bytesPerParam` and the 15%/10% overhead factors, and the GPU class beside each bar is computed from the total — never a literal. c3 crossings are solved from `rate`, `gpuCost`, and `gpuCap` at render time and printed from that computation; the local payback volume is computed as `localMonthly / rate` and compared to `localCap` in code. Arithmetic closes: 8×2=16, ×1.25=20; 24×2=48, ×1.25=60; 70×2=140, ×1.25=175; ceil(175/80)=3; 2400÷3=800 and 4800÷3=1600; 6000÷24=250; 250÷3≈83.3 > 60.
- **Content discipline:** no model vendors or model family names anywhere. Cloud platforms are referred to generically ("a cloud's managed model service", "hyperscaler GPU instances"); AWS/GCP/Azure appear once, as examples of the category. No named-actor scenarios. Every constructed figure is labelled illustrative. Neutral tone with no advocacy for any topology; the page teaches the decision, not a preferred answer.
- **Scope boundaries:** this page is about deployment topologies and choosing between them. It does not re-teach what open weights are, licence terms, model hubs as registries, the cost/latency/quality triangle, or why inference is memory-bound — those ideas appear only as a phrase where needed, with no links.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
