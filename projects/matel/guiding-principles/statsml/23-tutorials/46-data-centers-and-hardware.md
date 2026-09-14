# Data Centers & Hardware

**Page type:** grid page (tutorials category grid: single flat 4-column nav-grid of cards with topic tags)
**HTML title tag:** Data Centers & Hardware

**Subtitle:** The physical machinery behind computation — buildings sized in megawatts, fleets where something is always failing, and chips built for one job done a billion times.

## Cards

Each card links to a topic page under `data-centers-hardware/`. The card shows a colored uppercase subcategory label (`.card-num`), a numbered title, a one-line description, and 2-4 topic tag pills. All cards sit in one flat `.nav-grid`; the colored labels carry the grouping.

| # | Category | Title | Link | Description | Topic tags |
|---|----------|-------|------|-------------|------------|
| 1 | INSIDE THE BUILDING | Anatomy of a Data Center | [46-data-centers-and-hardware/01-anatomy-of-a-data-center.md](46-data-centers-and-hardware/01-anatomy-of-a-data-center.md) | A server slots into a rack, racks into rows, rows into halls — and every level is a group of machines that can die together. | racks and rows, failure domains, physical layout |
| 2 | INSIDE THE BUILDING | Power | [46-data-centers-and-hardware/02-power.md](46-data-centers-and-hardware/02-power.md) | Sites are sized in megawatts — power, not land or servers, decides how big a data center can get. | megawatts, grid to chip, capacity limits |
| 3 | INSIDE THE BUILDING | Cooling | [46-data-centers-and-hardware/03-cooling.md](46-data-centers-and-hardware/03-cooling.md) | Every watt a server consumes comes back out as heat — the building's main job is moving that heat outdoors. | heat removal, hot and cold aisles, liquid cooling |
| 4 | INSIDE THE BUILDING | Data Center Networking | [46-data-centers-and-hardware/04-data-center-networking.md](46-data-centers-and-hardware/04-data-center-networking.md) | Servers talk to each other far more than to users, so the network is a leaf-spine fabric where every rack is the same short distance from every other. | leaf-spine, east-west traffic, fabric |
| 5 | FLEETS & GEOGRAPHY | Everything Fails, Constantly | [46-data-centers-and-hardware/05-everything-fails-constantly.md](46-data-centers-and-hardware/05-everything-fails-constantly.md) | A disk with a 2% annual failure rate is very reliable alone — buy 100,000 and several die every day, so reliability has to live in software. | failure rates, scale effects, software resilience |
| 6 | FLEETS & GEOGRAPHY | Regions & Availability Zones | [46-data-centers-and-hardware/06-regions-and-availability-zones.md](46-data-centers-and-hardware/06-regions-and-availability-zones.md) | Clouds arrange data centers so a fire, a flood, or a bad deploy each destroys a pre-agreed slice and nothing more. | blast radius, redundancy, geography |
| 7 | FLEETS & GEOGRAPHY | Colo, Hyperscale, Edge | [46-data-centers-and-hardware/07-colo-hyperscale-edge.md](46-data-centers-and-hardware/07-colo-hyperscale-edge.md) | Three ways to have a data center — rent a cage in someone's building, rent slices of a giant's campus, or scatter tiny sites near your users. | colocation, hyperscale, edge sites |
| 8 | FLEETS & GEOGRAPHY | The AI Data Center | [46-data-centers-and-hardware/08-the-ai-data-center.md](46-data-centers-and-hardware/08-the-ai-data-center.md) | A GPU training rack draws ten times the power of a cloud rack — electricity, not chips, now decides where and how fast AI capacity gets built. | GPU racks, power density, training clusters |
| 9 | CHIPS & PERFORMANCE | CPU Pipelining & Branch Prediction | [46-data-centers-and-hardware/09-cpu-pipelining-and-branch-prediction.md](46-data-centers-and-hardware/09-cpu-pipelining-and-branch-prediction.md) | A CPU runs instructions like an assembly line and guesses which way every if will go — why the same loop runs faster on a sorted array. | assembly line, branch guessing, sorted vs unsorted |
| 10 | CHIPS & PERFORMANCE | SIMD & Vectorization | [46-data-centers-and-hardware/10-simd-and-vectorization.md](46-data-centers-and-hardware/10-simd-and-vectorization.md) | Modern CPUs can add 8 numbers in one instruction — NumPy's array operations use this, a Python for-loop never can. | one instruction, many data; vector registers; NumPy speed |
| 11 | CHIPS & PERFORMANCE | GPU Architecture | [46-data-centers-and-hardware/11-gpu-architecture.md](46-data-centers-and-hardware/11-gpu-architecture.md) | A CPU makes a few cores fast at anything; a GPU makes thousands of weak cores fast at doing the same operation to different data. | thousands of cores, parallel math, throughput |
| 12 | CHIPS & PERFORMANCE | TPUs & Custom Silicon | [46-data-centers-and-hardware/12-tpus-and-custom-silicon.md](46-data-centers-and-hardware/12-tpus-and-custom-silicon.md) | A TPU gives up the ability to run any program so that nearly every transistor can do matrix math — hardware built for exactly one workload. | matrix math, specialization, accelerators |
| 13 | CHIPS & PERFORMANCE | Profiling | [46-data-centers-and-hardware/13-profiling.md](46-data-centers-and-hardware/13-profiling.md) | Before you optimize, measure where the time actually goes — a flame graph usually points somewhere nobody suspected. | measure first, flame graphs, bottlenecks |
| 14 | FLEET LIFECYCLE | Decommissioning & Data Destruction | [46-data-centers-and-hardware/14-decommissioning-and-data-destruction.md](46-data-centers-and-hardware/14-decommissioning-and-data-destruction.md) | A retired drive is a readable copy of your data leaving the building — wiped, key-destroyed, or shredded, and a format is none of those. | wiping drives, crypto-erase, shredding |

## Regeneration instructions

To rebuild the HTML from this spec:

- **Template:** tutorials category grid. Single page: h1, `.subtitle` paragraph, then one flat `.nav-grid` of `.nav-card` anchors (no h2 section headings).
- **Layout:** `.nav-grid` is CSS grid, `repeat(4, 1fr)`, 16px gap, 15px top margin; responsive: 3 columns below 1400px, 2 below 1100px, 1 below 600px.
- **Links:** the tables above link to the `.md` versions for markdown navigation; in the regenerated HTML, each card's `href` is the same path with an `.html` extension instead.
- **Card structure:** `<a class="nav-card" href="...">` containing `<div class="card-num">SUBCATEGORY LABEL</div>`, `<h3>N. Topic Title</h3>` (unpadded index number matching the 2-digit zero-padded file index), `<p>description</p>`, then `<div class="topics">` of `<span class="topic-tag">` pills.
- **Category label colors:** applied by a small script mapping `.card-num` text to color — "INSIDE THE BUILDING" `#2980b9`, "FLEETS & GEOGRAPHY" `#27ae60`, "CHIPS & PERFORMANCE" `#8e44ad`, "FLEET LIFECYCLE" `#e67e22`; the CSS default for `.card-num` is `#2980b9`, 0.75em bold, 4px bottom margin.
- **Card style:** background `#ffffff`, border `1px solid #d8d8d8`, radius 10px, padding 20px, shadow `0 2px 4px rgba(0,0,0,0.05)`, transition on border-color/transform; hover: border `#2980b9`, `translateY(-2px)`. h3 `#1a3a4a` 1em with 6px bottom margin; description `#555` 0.85em. Topic tags: background `#f0f0f0`, border `1px solid #ccc`, radius 4px, padding 2px 6px, 0.7em `#666`, in a flex-wrap row with 4px gap and 8px top margin.
- **Page style:** body system sans-serif, background `#f5f5f0`, text `#2a2a2a`, padding 40px, line-height 1.6; universal `* { margin:0; padding:0; box-sizing:border-box }` reset; h1 1.8em `#2980b9` with 10px bottom margin; subtitle `#666` 1.05em with 30px bottom margin. No nav bar, no back/home links.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22` (page accents here use `#2980b9`, `#27ae60`, `#8e44ad`).
- **Canvases:** none on this page; any canvases elsewhere in this series use `window.devicePixelRatio` scaling.
