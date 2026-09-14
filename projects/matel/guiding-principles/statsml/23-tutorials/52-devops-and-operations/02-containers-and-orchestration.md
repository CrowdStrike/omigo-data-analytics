# Containers & Orchestration

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Containers & Orchestration

**Subtitle:** A container ships the entire environment with the code, so it runs identically anywhere — and an orchestrator keeps hundreds of them alive by reconciling reality toward a declared goal

## The Model That Only Ran on One Laptop

**Tags:** `core idea` (blue), `environment as artifact` (green), `works on my machine` (orange)

- **The handoff** — a churn model works perfectly on the analyst's laptop, then dies on the team server
- **The real bug** — the laptop has pandas 2.1.4 and libgomp; the server has pandas 1.5.3 and neither
- **The diagnosis** — "works on my machine" is never about the code; it is about the environment around it
- **The fix** — package code, Python runtime, libraries, and OS packages into one immutable image
- **The payoff** — the image is the unit of shipping: run it on a laptop, a server, or a cloud, unchanged

*Example (italic):* The same image that passed tests on the laptop Monday runs unchanged on the server Tuesday — same pandas 2.1.4, same libgomp, byte for byte.

**Key point:** A container ships the ENTIRE environment — code, runtime, dependencies, OS libraries — as one immutable image that runs identically anywhere a container runtime exists.

### Visualization (canvas `c1`, 720×300)

Two-row flow diagram: shipping bare code into a different environment (crash) vs shipping the whole image (identical run), shown as boxes with arrows.

- **Title (bold 15px, `#1a5276`, top center):** "Ship the Code vs Ship the Environment".
- **Row 1 (y=95), label 12px `#444` at x=20:** "code only"; blue `#2a78d6` rounded box at x=150 labeled "laptop: pandas 2.1.4 + libgomp" (12px), 3px arrow to a red `#e74c3c` box at x=440 labeled "server: pandas 1.5.3, no libgomp" with bold 12px red "✗ ImportError — works on my machine" beneath it.
- **Row 2 (y=205), label:** "the image"; blue box at x=150 labeled "image: code + runtime + deps + OS libs", 3px arrow to a green `#008300` box at x=440 labeled "any machine with a runtime" with bold 12px green "✓ identical run everywhere" beneath it.
- **Box style:** 190–210px wide, 44px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(231,76,60,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=270):** "the environment travels with the code — nothing is 'installed' at the far end".

## Inside the Image: Four Layers and a Cache

**Tags:** `worked example` (blue), `layers & cache` (green), `vs virtual machines` (orange)

- **The layers** — the image stacks base OS 78 MB, Python 3.11 121 MB, pip deps 342 MB, code 2 MB
- **Hand-check** — 78 + 121 + 342 + 2 = 543 MB total; each layer is content-addressed by its hash
- **The cache** — editing one line of model code rebuilds only the 2 MB top layer: 9 s, not 6 min
- **The dedupe** — registries store each layer hash once, so ten images sharing a base cost one base
- **Not a VM** — a VM ships a whole guest OS (4.2 GB, ~45 s boot); a container shares the host kernel
- **The isolation** — namespaces and cgroups fence off processes, so sharing a kernel still feels private

*Example (italic):* Changing one line of model code rebuilds only the 2 MB code layer — the 541 MB beneath it comes straight from cache, and the push uploads 2 MB, not 543.

**Key point:** Containers share the host kernel (isolated by namespaces and cgroups), so they cost megabytes and start in milliseconds; images are layered and content-addressed, so builds and pulls only pay for what changed.

### Visualization (canvas `c2`, 720×300)

Two side-by-side layer stacks: build #1 assembling all four layers vs build #2 after a code edit reusing three cached layers, with sizes on every layer.

- **Title (bold 15px, `#1a5276`, top center):** "One Image, Four Layers: a Code Change Rebuilds 2 MB, Not 543".
- **Left stack (x=110, boxes 210px wide, 38px tall, stacked upward from baseline y=252), header bold 12px `#444`:** "build #1 — 6 min"; bottom to top: gray `rgba(107,114,128,0.20)` box "base OS — 78 MB", blue `rgba(42,120,214,0.18)` box "Python 3.11 — 121 MB", blue `rgba(42,120,214,0.30)` box "pip deps — 342 MB", green `rgba(0,131,0,0.25)` box "model code — 2 MB"; all 12px `#2c3e50` labels, 1px `#6b7280` borders.
- **Right stack (x=400, same geometry), header:** "build #2 after code edit — 9 s"; bottom three boxes drawn at 45% alpha with bold 12px green `#008300` "cached ✓" at each right edge; top code layer solid orange `rgba(217,89,38,0.30)` with 2px `#d95926` border labeled "model code — 2 MB (rebuilt)".
- **Annotation (bold 13px green `#008300`, at x≈625, y≈150, right of stacks):** "541 MB from cache".
- **Footnote (12px `#6b7280`, bottom left at y=285):** "VM equivalent: 4.2 GB, ~45 s boot — this container: 543 MB, 0.4 s start".
- **Caption (12px `#444`, bottom right):** "sizes and times illustrative".

## Six Replicas, Declared Once, Kept True

**Tags:** `where it's used` (blue), `desired state` (green), `orchestration` (orange)

- **The scale problem** — hundreds of containers across dozens of machines need placing and restarting
- **The declaration** — you state desired state: "6 replicas of this image behind this load balancer"
- **The loop** — the orchestrator continuously compares actual vs desired and edits reality to match
- **The crash** — a replica dies; the loop sees 5 ≠ 6 and starts a replacement — no human, no pager
- **Riding on it** — rolling updates swap replicas gradually; health probes decide what counts as alive
- **And more** — autoscaling adds replicas under load; service discovery finds them wherever they land

*Example (italic):* At minute 3.1 a replica crashes; the reconciler sees 5 ≠ 6 and starts a replacement — 24 seconds later reality matches the declaration again, untouched by humans.

**Key point:** You declare the desired state; the orchestrator runs a reconciliation loop that continuously edits reality toward it — a crashed container is replaced, not mourned, with no human in the loop.

### Visualization (canvas `c3`, 720×300)

Timeline line chart: running replicas over ten minutes against a dashed desired-state line at 6, with a pod crash and a node loss both self-healing.

- **Title (bold 15px, `#1a5276`, top center):** "Declared: 6 Replicas. The Loop Keeps It True".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = minutes 0 to 10 with 12px `#444` tick labels every 2 minutes; y = running replicas 0 to 8, gridlines `#e5e9ef` at 2/4/6.
- **Desired line:** dashed `#1a5276` 2px (dash 6/4) horizontal at replicas=6, 12px `#1a5276` label "desired = 6" at its left end.
- **Actual line:** blue `#2a78d6` 3px line through minutes `[0, 1, 2, 3, 3.1, 3.5, 4, 5, 6, 7, 7.1, 8, 9, 10]`, replicas `[6, 6, 6, 6, 5, 6, 6, 6, 6, 6, 4, 6, 6, 6]` — a one-replica dip at 3.1 healed by 3.5, a two-replica dip at 7.1 healed by 8.
- **Event markers:** red `#e74c3c` 5px dots at (3.1, 5) and (7.1, 4), 12px red labels "pod crashes" and "node dies — 2 lost".
- **Annotation (bold 13px green `#008300`, near minute 7.6, y=85):** "replaced automatically — no human involved".
- **Caption (12px `#444`, bottom right):** "recovery timings illustrative".

## The Bill: YAML Sprawl and a Second Job

**Tags:** `common mistake` (red), `YAML sprawl` (orange)

- **The confusion** — treating the orchestrator as "docker run, but bigger": it is a system you operate
- **The sprawl** — every service needs deployment, service, ingress, and autoscaler configs in YAML
- **The count** — 1 service: 400 app vs 180 YAML lines; 20: 8,000 vs 7,800 — cross-service config compounds
- **The curve** — networking, storage, permissions, and upgrades each have their own learning cliff
- **The escape hatch** — managed platforms exist precisely because running the orchestrator is a job

*Example (italic):* A four-person team ships 5 model services (2,000 lines of app code) and finds itself maintaining 1,400 lines of YAML before starting feature work on any of them.

**Common mistake:** Treating the orchestrator as free infrastructure. The reconciliation loop is bought with YAML sprawl and a steep operational learning curve — budget for both, or pay a managed platform to hide them.

### Visualization (canvas `c4`, 720×300)

Grouped bar chart: application code lines vs orchestration YAML lines as the number of services grows from 1 to 20 — the config grows toward parity with the app.

- **Title (bold 15px, `#1a5276`, top center):** "The Config Grows Like a Codebase: App Lines vs YAML Lines".
- **Axes:** origin x=70, baseline y=245, plot width 580, plot height 180; y = lines of code 0 to 8,000, gridlines `#e5e9ef` at 2,000/4,000/6,000 with 12px `#444` labels; x = three groups centered at x=200, 380, 560 labeled "1 service", "5 services", "20 services" (12px `#444`).
- **Bars:** per group two bars 44px wide with an 8px gap — blue `rgba(42,120,214,0.75)` app-code bar and orange `rgba(217,89,38,0.75)` YAML bar; app values `[400, 2000, 8000]`, YAML values `[180, 1400, 7800]`; 11px `#444` value labels above each bar top.
- **Legend (12px, top left inside plot):** blue swatch "app code", orange swatch "orchestration YAML".
- **Annotation (bold 13px orange `#d95926`, near x=430, y=75):** "at 20 services the YAML nearly matches the app".
- **Caption (12px `#444`, bottom right):** "line counts illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); layer sizes (78/121/342/2 = 543 MB), build times (6 min vs 9 s), VM comparison (4.2 GB / 45 s vs 543 MB / 0.4 s), replica timelines, and YAML line counts are invented and labeled illustrative; text and chart numbers must stay in sync.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
