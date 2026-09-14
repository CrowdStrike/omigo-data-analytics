# Virtual Machines & Hypervisors

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Virtual Machines & Hypervisors

**Subtitle:** One physical computer can pretend to be several complete computers at once — the hypervisor is the program that keeps up the act

## The Office With One Server and Four Teams

**Tags:** `core idea` (blue), `one machine, many` (green), `isolation` (orange)

- **The server** — a small analytics firm owns one beefy server: 32 cores and 128 GB of RAM
- **Four teams** — analytics, web, database, and testing each want "their own machine" to break freely
- **The trick** — a hypervisor carves the server into four virtual machines, each a complete fake computer
- **The guest** — each VM boots its own operating system, which believes it runs on real hardware
- **The wall** — a crash, a full disk, or a bad experiment inside one VM cannot touch the other three

*Example (italic):* The testing team's VM freezes mid-experiment at 3pm; the analytics, web, and database VMs never notice.

**Key point:** A virtual machine is a computer made of software — the hypervisor slices one physical machine into several isolated ones, each running its own operating system.

### Visualization (canvas `c1`, 720×300)

Stack diagram: one hardware box at the bottom, a hypervisor layer above it, and four independent VM boxes on top, each with its own guest OS.

- **Title (bold 15px, `#1a5276`, top center):** "One Physical Server, Four Complete Computers".
- **Hardware box:** rounded rect x=60, y=248, width 600, height 34, fill `rgba(44,62,80,0.10)`, 2px `#2c3e50` border, centered 12px `#2c3e50` label "physical server — 32 cores, 128 GB RAM".
- **Hypervisor layer:** rounded rect x=60, y=206, width 600, height 34, fill `rgba(74,58,167,0.12)`, 2px violet `#4a3aa7` border, centered bold 12px `#4a3aa7` label "hypervisor — slices and referees the hardware".
- **VM boxes (y=88, height 108, width 138, at x = 60, 214, 368, 522):** each 8px radius with a bold 12px name line and two 11–12px lines below ("guest OS", "apps"):
  - "VM 1 — analytics", fill `rgba(42,120,214,0.15)`, 2px blue `#2a78d6` border
  - "VM 2 — web", fill `rgba(0,131,0,0.12)`, 2px green `#008300` border
  - "VM 3 — database", fill `rgba(25,158,112,0.14)`, 2px aqua `#199e70` border
  - "VM 4 — testing", fill `rgba(201,133,0,0.14)`, 2px yellow `#c98500` border
- **Connectors:** short 2px `#6b7280` vertical lines from each VM box bottom to the hypervisor layer top.
- **Annotation (bold 13px magenta `#d55181`, top right near y=70):** "each VM believes it owns the whole machine".
- **Caption (12px `#444`, bottom right):** "teams and sizes illustrative".

## Carving 32 Cores Into Four Machines

**Tags:** `worked example` (blue), `resource slices` (green)

- **The budget** — the hypervisor has exactly 32 cores and 128 GB of RAM to hand out
- **The slices** — analytics gets 12 cores / 48 GB, web 8 / 32, database 8 / 32, testing 4 / 16
- **Hand-check cores** — 12 + 8 + 8 + 4 = 32, so every core is assigned and none is invented
- **Hand-check RAM** — 48 + 32 + 32 + 16 = 128 GB, the exact size of the physical machine
- **Same ratio** — each team's RAM share matches its core share (analytics holds 12/32 = 3/8 of both)

*Example (italic):* Inside its VM, the analytics team runs `nproc` and sees 12 cores — it has no way to tell the other 20 exist.

**Key point:** The hypervisor is an accountant: the virtual machines' slices are fractions of one real budget, and in this simple no-overcommit setup the fractions add up to the machine you actually bought.

### Visualization (canvas `c2`, 720×300)

Two stacked horizontal bars — one for CPU cores, one for RAM — split into four colored segments, one per VM, with identical proportions.

- **Title (bold 15px, `#1a5276`, top center):** "The Hypervisor's Ledger: 32 Cores and 128 GB, Fully Assigned".
- **Rows:** bar rows at y=105 (height 36) and y=195 (height 36); left-aligned 12px `#444` row labels at x=20: "CPU — 32 cores" and "RAM — 128 GB"; bars start at x=150, full width 540.
- **Cores bar segments (pixel widths 540 × share):** analytics 12 cores → 202px fill `rgba(42,120,214,0.75)`; web 8 → 135px `rgba(0,131,0,0.70)`; database 8 → 135px `rgba(25,158,112,0.75)`; testing 4 → 68px `rgba(201,133,0,0.80)`; centered 12px white labels "12", "8", "8", "4".
- **RAM bar segments (same shares, same colors):** widths 202 / 135 / 135 / 68px, centered 12px white labels "48", "32", "32", "16".
- **Legend (12px `#444`, y=68, small color squares):** "analytics · web · database · testing" using the four segment colors.
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=260):** "12+8+8+4 = 32 — with no overcommit, every core handed out really exists".
- **Caption (12px `#444`, bottom right):** "allocations illustrative".

## Every Cloud Machine You Rent Is One of These

**Tags:** `where it's used` (blue), `cloud computing` (green), `utilization` (orange)

- **The cloud** — a rented cloud instance is a VM; the provider's hypervisor slices giant physical hosts
- **The waste** — before consolidation, the firm's four dedicated servers idled at 12%, 18%, 9%, and 21% busy
- **The sum** — 12 + 18 + 9 + 21 = 60, so the combined load fills 60% of one same-size server
- **The payoff** — one machine, one power bill, and 40% headroom left over instead of four idle boxes
- **The snapshot** — a VM is files on disk, so it can be copied, paused, moved, or rolled back like data

*Example (italic):* A data scientist's "16-core cloud machine" is a hypervisor slice of a much larger host shared with strangers' VMs.

**Key point:** Virtualization is why cloud computing works: hardware is bought big and idle-proofed by packing many customers' virtual machines onto each physical host.

### Visualization (canvas `c3`, 720×300)

Vertical bar chart: utilization of the four old dedicated servers, then one taller bar for the single virtualized server that absorbs all four loads.

- **Title (bold 15px, `#1a5276`, top center):** "Four Idle Servers Become One 60%-Busy Server".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; y = utilization 0 to 100%, gridlines `#e5e9ef` at 25/50/75 with 12px `#444` labels; x tick labels 12px `#444` under each bar.
- **Dedicated-server bars (width 70, fill `rgba(42,120,214,0.45)`, 2px `#2a78d6` border), centers at x = 130, 240, 350, 460:** heights for 12%, 18%, 9%, 21%; labels "server A 12%", "server B 18%", "server C 9%", "server D 21%"; bold 12px `#2a78d6` value labels above each bar.
- **Consolidated bar (width 80, fill `rgba(0,131,0,0.55)`, 2px `#008300` border), center x=590:** height for 60%, label "one VM host 60%", bold 13px `#008300` value "60%" above.
- **Divider:** vertical dashed `#6b7280` (dash 4/3) line at x=520 from y=70 to y=245.
- **Annotation (bold 13px green `#008300`, near x=300, y=80):** "12+18+9+21 = 60% — the whole office fits on one box".
- **Caption (12px `#444`, bottom right):** "utilization percentages illustrative".

## A VM Is Not a Container

**Tags:** `common mistake` (red), `containers` (orange)

- **The confusion** — people say "container" and "VM" interchangeably; the stacks are genuinely different
- **A VM ships an OS** — every VM carries a full guest operating system, tens of GB, booting in ~60 s
- **A container borrows one** — containers share the host's kernel; images are hundreds of MB, starting in ~1 s
- **The wall differs** — a VM's isolation is hardware-enforced; a container's is one shared kernel's promises
- **The mistake** — running untrusted code in a container "because it's isolated like a VM" — it is not

*Example (italic):* The testing team switches a job from a VM (~60 s boot) to a container (~1 s start) and gains speed — but now shares a kernel with its neighbors.

**Common mistake:** Treating containers as lightweight VMs. A container is a fenced-off corner of one running OS; a VM is a whole separate computer — the isolation guarantees are not the same.

### Visualization (canvas `c4`, 720×300)

Side-by-side stack diagrams: the VM stack (four layers including a guest OS) versus the container stack (shared host OS), with start-time labels under each.

- **Title (bold 15px, `#1a5276`, top center):** "Two Stacks: a VM Ships Its Own OS, a Container Borrows Yours".
- **Left stack (VM), boxes x=90, width 230, height 32, stacked at y = 196, 160, 124, 88 (bottom to top):** "hardware" fill `rgba(44,62,80,0.10)` border `#2c3e50`; "hypervisor" fill `rgba(74,58,167,0.12)` border violet `#4a3aa7`; "guest OS (full copy)" fill `rgba(42,120,214,0.15)` border blue `#2a78d6`; "app" fill `rgba(0,131,0,0.12)` border green `#008300`; centered 12px `#2c3e50` labels.
- **Right stack (container), boxes x=400, width 230, height 32, stacked at y = 196, 160, 124, 88:** "hardware" (same style); "host OS (shared kernel)" fill `rgba(42,120,214,0.15)` border blue; "container runtime" fill `rgba(25,158,112,0.14)` border aqua `#199e70`; "app" fill `rgba(0,131,0,0.12)` border green.
- **Column headers (bold 13px `#1a5276`, y=70):** "virtual machine" centered over left stack, "container" centered over right stack.
- **Start-time labels (bold 12px, y=252):** blue `#2a78d6` "boots in ~60 s, image tens of GB" under the left stack; aqua `#199e70` "starts in ~1 s, image hundreds of MB" under the right stack.
- **Annotation (bold 13px orange `#d95926`, centered near y=282):** "a container shares the host kernel — a VM brings its own".
- **Caption (12px `#444`, bottom right):** "boot times and sizes illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all numbers are the hardcoded literals above (no randomness); the server spec (32 cores / 128 GB), VM slices (12/8/8/4 cores, 48/32/32/16 GB), utilization figures (12/18/9/21 → 60%), and boot times (~60 s vs ~1 s) are invented and labeled illustrative; the slice sums (32 cores, 128 GB) and utilization sum (60%) must stay arithmetically exact so the hand-checks in the text hold.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
