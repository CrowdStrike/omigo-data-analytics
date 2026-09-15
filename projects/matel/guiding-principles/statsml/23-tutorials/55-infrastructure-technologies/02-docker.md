# Docker

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Docker

**Subtitle:** Docker (2013) packages an app with its entire environment into one image — instead of shipping your code and hoping the server matches your machine, you ship the machine

## The Deploy That Broke on the Server

**Tags:** `core idea` (blue), `works on my machine` (red), `2013` (orange)

- **The app** — a small Python web service: Python 3.11, the libpq system library, 14 pip packages
- **The laptop** — everything installed over months of tinkering; the app runs perfectly
- **The server** — Python 3.8, no libpq: the deploy crashes on import before serving one request
- **The old fix** — a wiki page of setup steps that drifts out of date the week after it's written
- **The Docker fix** — build one image holding the app, its packages, and libpq; run that everywhere
- **The claim** — the image IS the environment, so "works on my machine" means works anywhere

*Example (italic):* The same `docker run myapp` command starts an identical service on the laptop and the server — the server's Python 3.8 is never even consulted.

**Key point:** Docker moves the unit of shipping from "your code" to "your code plus every library and system file it needs" — one immutable image that runs the same on any machine with a Docker engine.

### Visualization (canvas `c1`, 720×300)

Two-row flow diagram: shipping code alone (breaks on the server) vs shipping the image (runs identically), boxes flowing left to right.

- **Title (bold 15px, `#1a5276`, top center):** "Ship the Code vs Ship the Machine".
- **Row 1 (y=95), label 12px `#444` at x=20:** "code only"; blue `#2a78d6` rounded box at x=130 labeled "laptop: py3.11 + libpq ✓" (12px), 3px arrow labeled "copy app/" (11px `#6b7280`), then red `#e74c3c` box at x=430 labeled "server: py3.8, no libpq" with bold 12px red "✗ ImportError on boot".
- **Row 2 (y=205), label:** "image"; blue box at x=130 labeled "build: app + py3.11 + libpq", 3px arrow labeled "push / pull image" (11px `#6b7280`), then green `#008300` box at x=430 labeled "server runs the image" with bold 12px green "✓ identical behavior".
- **Box style:** 160–180px wide, 44px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(231,76,60,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=270):** "the environment travels with the app".
- **Caption (12px `#444`, bottom right):** "versions illustrative".

## Building the Image in Layers

**Tags:** `worked example` (blue), `Dockerfile` (green), `layer cache` (orange)

- **The recipe** — a Dockerfile is an ordered list of steps; each step produces one image layer
- **Layer 1** — `FROM python:3.11-slim`, the shared base: 130 MB, downloaded once, reused by every build
- **Layer 2** — `RUN apt-get install libpq5`, the system dependency: 12 MB
- **Layer 3** — `COPY requirements.txt` + `RUN pip install`, the 14 packages: 210 MB
- **Layer 4** — `COPY app/`, your actual code: 3 MB on top of a 355 MB image
- **The cache** — a step reruns only if its inputs changed; unchanged layers are reused as-is

*Example (italic):* Editing one Python file and rebuilding reuses layers 1–3 from cache and rebuilds only the 3 MB code layer — 3 seconds instead of the first build's 95.

**Key point:** Layers make images cheap to rebuild and cheap to share — order the Dockerfile from least- to most-frequently-changed, and daily rebuilds touch only the thin top layer.

### Visualization (canvas `c2`, 720×300)

Layer stack diagram: the four image layers drawn as horizontal slabs bottom-up, with a right-hand column comparing first-build vs rebuild cost per layer.

- **Title (bold 15px, `#1a5276`, top center):** "One Image, Four Layers: a Code Change Rebuilds Only the Top".
- **Stack (left, slabs 300px wide starting x=70, heights proportional-ish but readable):** bottom slab y=200 h=52 blue fill `rgba(42,120,214,0.30)` labeled "FROM python:3.11-slim — 130 MB"; above it y=162 h=34 aqua `rgba(25,158,112,0.25)` "apt libpq5 — 12 MB"; above y=100 h=58 violet `rgba(74,58,167,0.20)` "pip install (14 pkgs) — 210 MB"; top y=72 h=24 green `rgba(0,131,0,0.25)` "COPY app/ — 3 MB". All labels 12px `#2c3e50`, 1px `#6b7280` slab borders.
- **Rebuild column (right of stack, x=420):** header bold 12px `#1a5276` "rebuild after a code edit" at y=60; per layer, aligned to its slab, 12px text: bottom three rows gray `#6b7280` "cached — 0s"; top row bold green `#008300` "rebuilt — 3s".
- **Totals (bold 12px, x=420):** at y=250 `#2c3e50` "first build: 95s · image 355 MB"; at y=270 green `#008300` "rebuild: 3s".
- **Annotation (bold 13px orange `#d95926`, near x=430, y=110):** "put what changes most at the top".
- **Caption (12px `#444`, bottom right):** "sizes and times illustrative".

## One Image, Three Machines

**Tags:** `where it's used` (blue), `registry` (green), `OCI` (orange)

- **The push** — the built image is pushed once to a registry such as Docker Hub, keyed by digest
- **The pulls** — laptop, CI runner, and production all pull the same digest: identical bytes everywhere
- **CI honesty** — tests run against the exact image production will run, not a lookalike environment
- **Rollback** — yesterday's image still sits in the registry; rolling back is pulling the old tag
- **The standard** — Docker's format grew into the OCI project (2015), so any OCI runtime runs the image
- **The date** — Docker itself launched in 2013 and made existing kernel features usable overnight

*Example (italic):* Image `myapp@sha256:9f2c…` is pulled by the laptop, the CI runner, and the production host — three machines, one environment, zero setup wikis.

**Key point:** A registry turns the image into the single source of truth for "what runs" — build once, test that artifact, deploy that artifact, roll back to a previous one by name.

### Visualization (canvas `c3`, 720×300)

Hub-and-spoke diagram: one registry box in the center-left, one push arrow in, three pull arrows out to laptop / CI / production, all carrying the same digest.

- **Title (bold 15px, `#1a5276`, top center):** "Build Once, Pull Everywhere: the Registry as Source of Truth".
- **Builder (left):** blue `#2a78d6` rounded box at x=40, y=130, 130×44, label "build machine" (12px); 3px blue arrow to the hub labeled "push" (bold 11px `#2a78d6`).
- **Hub (center-left):** ink-bordered box at x=250, y=118, 180×68, fill `rgba(26,82,118,0.10)`, bold 13px `#1a5276` label "registry (Docker Hub)" plus 11px `#6b7280` "myapp@sha256:9f2c…".
- **Spokes (right, boxes 140×40 at x=540):** green `#008300` boxes at y=50 "laptop", y=130 "CI runner", y=210 "production", each fed by a 3px green arrow from the hub labeled "pull" (11px), each with 11px `#6b7280` sub-label "sha256:9f2c…".
- **Annotation (bold 13px green `#008300`, near x=420, y=270):** "same digest = same bytes = same behavior".
- **Caption (12px `#444`, bottom right):** "digest shortened for display".

## A Container Is Not a Small VM

**Tags:** `common mistake` (red), `namespaces + cgroups` (orange)

- **The confusion** — containers get called "lightweight VMs", but they virtualize nothing
- **What it really is** — a normal Linux process, isolated by namespaces and limited by cgroups
- **Namespaces** — control what the process can see: its own filesystem, network, process list
- **cgroups** — control what it can use: caps on CPU and memory for that process group
- **No guest OS** — every container shares the host kernel; a VM boots a whole OS of its own
- **The trade** — startup in ~0.3s vs ~45s, ~5 MB overhead vs ~1 GB, but a weaker isolation wall

*Example (italic):* Ten containers on one host run ten processes on one shared kernel; ten VMs boot ten guest operating systems — which is why the VM host needs ~10 GB before any app code runs.

**Common mistake:** Treating a container as a security boundary equal to a VM. It is one shared kernel wearing ten disguises — kernel-level isolation is real but thinner, which is why hostile multi-tenant workloads still get VMs.

### Visualization (canvas `c4`, 720×300)

Side-by-side stack diagrams: the VM stack (hypervisor + a guest OS per app) vs the container stack (shared kernel, thin containers), with startup/overhead callouts.

- **Title (bold 15px, `#1a5276`, top center):** "VM Stack vs Container Stack: Where the Guest OS Disappears".
- **Left stack ("virtual machines", bold 12px `#444` header at x=110, y=55; slabs 240px wide at x=50):** bottom slab y=238 h=26 gray `rgba(107,114,128,0.20)` "hardware"; y=208 h=26 blue `rgba(42,120,214,0.25)` "host OS"; y=178 h=26 violet `rgba(74,58,167,0.20)` "hypervisor"; then three side-by-side columns (each 72px wide) from y=90 to y=174: orange `rgba(217,89,38,0.20)` lower half "guest OS" + green `rgba(0,131,0,0.15)` upper half "app". 11–12px `#2c3e50` labels, 1px `#6b7280` borders.
- **Right stack ("containers", header at x=560; slabs 240px wide at x=430):** bottom slab y=238 h=26 "hardware"; y=208 h=26 "host OS + shared kernel" (blue, drawn slightly taller emphasis border 2px `#2a78d6`); y=178 h=26 aqua `rgba(25,158,112,0.25)` "Docker engine"; three thin columns (72px wide) from y=130 to y=174 green `rgba(0,131,0,0.15)` "app + libs" only — no guest OS band.
- **Callouts (bold 12px):** red `#e74c3c` at x=60, y=75 "boot ~45s · ~1 GB per guest OS"; green `#008300` at x=440, y=115 "start ~0.3s · ~5 MB overhead".
- **Annotation (bold 13px magenta `#d55181`, centered near y=285):** "containers isolate a process; VMs virtualize a machine".
- **Caption (12px `#444`, bottom right):** "startup and overhead figures illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all geometry and values are the hardcoded literals above (no randomness); layer sizes (130/12/210/3 MB), build times (95s/3s), and VM-vs-container figures (~45s vs ~0.3s, ~1 GB vs ~5 MB) are invented and labeled illustrative; the 2013 launch, the 2015 OCI standard, and namespaces/cgroups as the isolation mechanism are documented facts.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
