# Sandboxing

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Sandboxing

**Subtitle:** Your browser, phone, and CI system survive running strangers' code every day because untrusted code runs inside a box with deliberately limited reach

## Every Tab Runs a Stranger's Code

**Tags:** `core idea` (blue), `untrusted code` (red), `browser` (orange)

- **The habit** — every site you visit ships JavaScript, and your browser runs it within milliseconds
- **The stranger** — you never read that code, and neither did anyone you trust
- **The box** — the browser runs each page's code in a renderer process with almost no rights
- **Blocked doors** — the renderer cannot open your files, your camera, or another tab's memory
- **The one door** — anything it needs goes through the browser's broker, which checks every request

*Example (italic):* A malicious ad executes inside your news-site tab; it can compute whatever it likes, but every path to your tax documents is a wall.

**Key point:** A sandbox is a deliberately weakened environment for running untrusted code — the code runs at full speed, but its reach is limited to what it was explicitly granted.

### Visualization (canvas `c1`, 720×300)

Diagram: hostile code inside a sandbox box, with blocked arrows to sensitive targets and one allowed arrow through a broker.

- **Title (bold 15px, `#1a5276`, top center):** "The Renderer Sandbox: Full Speed Inside, Almost No Reach Outside".
- **Sandbox box:** rounded rectangle from (170, 80) to (400, 220), 8px radius, fill `rgba(42,120,214,0.10)`, 2px `#2a78d6` border, bold 13px `#1a5276` label "renderer sandbox" at its top edge; inside it a smaller rounded box (200, 130)–(370, 175), fill `rgba(231,76,60,0.12)`, 12px `#2c3e50` text "hostile JS from any site".
- **Blocked targets (right column, 12px `#444` labels at x=560):** "your files" at y=100, "camera & mic" at y=150, "other tabs' memory" at y=200; each gets a 3px `#e74c3c` arrow from the box's right edge ending in a bold 14px red "✗" at x=520.
- **Allowed path (bottom):** 3px `#008300` arrow from the box's bottom edge down to a green-bordered rounded box at (215, 245)–(455, 280) labeled "browser broker — checks each request" (12px), bold 14px green "✓" beside the arrow.
- **Annotation (bold 13px `#008300`, near x=470, y=262):** "one guarded door, not many".
- **Caption (12px `#444`, bottom right):** "schematic — Chromium-style architecture".

## Counting the Doors a Process May Knock On

**Tags:** `worked example` (blue), `least privilege` (green), `seccomp` (orange)

- **The doors** — a normal Linux process can ask the kernel for roughly 330 different syscalls
- **The filter** — a seccomp-style policy for a renderer allows about 60 and rejects the rest
- **Hand-check** — 330 minus 60 leaves 270 requests that now fail before the kernel even looks
- **Coarser box** — a default container profile still allows about 290 — a fence, not a vault
- **Finest box** — a Wasm module gets 0 syscalls; it can only call functions the host handed it

*Example (italic):* An attacker who fully controls the renderer asks to open your SSH key file — the syscall filter returns an error before the kernel ever sees the path.

**Key point:** Sandboxing is subtraction — start from everything the code could ask for, then hand-pick the short list it actually needs. That is least privilege made mechanical.

### Visualization (canvas `c2`, 720×300)

Horizontal bar chart: how many kernel doors stay open under each kind of box, from unsandboxed process down to WebAssembly.

- **Title (bold 15px, `#1a5276`, top center):** "Syscalls Still Allowed, by Kind of Box".
- **Axis:** vertical 2px `#999` baseline at x=230, bars extend right, max width 440 (linear scale: 330 syscalls = 440px).
- **Rows (bars 16px tall, top edges at y = 70, 120, 170, 220), each with a left-aligned 12px `#444` label at x=20 and an 11px count label at the bar's right end:**
  - "unsandboxed process": blue `#2a78d6` fill `rgba(42,120,214,0.30)`, width 440, label "≈330"
  - "default container": aqua `#199e70` fill at 0.30 alpha, width 387, label "≈290"
  - "browser renderer (seccomp)": green `#008300` fill at 0.30 alpha, width 80, label "≈60"
  - "Wasm module": no bar; bold 12px violet `#4a3aa7` text at x=240 reading "0 — imports only"
- **Annotation (bold 13px `#008300`, near x=330, y=195):** "270 doors welded shut for the renderer".
- **Caption (12px `#444`, bottom right):** "syscall counts approximate, illustrative".

## A Toolbox of Boxes, Coarse to Fine

**Tags:** `where it's used` (blue), `defense in depth` (green)

- **Coarse** — a VM brings its own kernel: the strongest wall, and the heaviest to start
- **Medium** — a container shares your kernel but sees its own filesystem and process list
- **Fine** — a process sandbox shares everything and filters what the process may ask for
- **Finest** — a Wasm module lives inside one process and sees only the functions it imported
- **Daily contact** — CI runners, notebook services, and plugin systems each pick a box from this ladder

*Example (italic):* A CI service runs your pull request's build script — arbitrary code from a stranger — inside a throwaway VM that is destroyed minutes after the build ends.

**Key point:** Defense in depth assumes the code WILL be malicious: choose a box, grant the least privilege that still lets the job run, and the worst case is bounded by the box.

### Visualization (canvas `c3`, 720×300)

Nested-boxes diagram: four sandboxing layers drawn one inside the other, each labeled with what it still shares with the host.

- **Title (bold 15px, `#1a5276`, top center):** "Smaller Box, Less to Steal".
- **Box 1 (VM):** rounded rectangle (60, 55)–(660, 265), 2px `#2a78d6` border, fill `rgba(42,120,214,0.06)`, bold 12px `#2a78d6` label at top-left inside: "VM — own kernel, own everything".
- **Box 2 (container):** (110, 90)–(610, 250), 2px `#199e70` border, fill `rgba(25,158,112,0.08)`, bold 12px `#199e70` label "container — shared kernel, own filesystem view".
- **Box 3 (process sandbox):** (160, 125)–(560, 235), 2px `#d95926` border, fill `rgba(217,89,38,0.08)`, bold 12px `#d95926` label "process sandbox — syscalls filtered".
- **Box 4 (Wasm):** (210, 160)–(510, 220), 2px `#4a3aa7` border, fill `rgba(74,58,167,0.10)`, bold 12px `#4a3aa7` centered label "Wasm module — only imported functions".
- **Annotation (bold 13px `#1a5276`, bottom center near y=285):** "layers stack: browsers run Wasm inside a filtered process inside your OS".
- **Caption (12px `#444`, bottom right):** "schematic, not to scale".

## A Sandbox Is a Second Lock, Not a Guarantee

**Tags:** `common mistake` (red), `sandbox escape` (orange)

- **The mistake** — treating "it's sandboxed" as "a compromise of it doesn't matter"
- **The truth** — a sandbox turns one required bug into two: code execution AND a separate escape
- **The market** — bug bounties pay a large premium for escapes because escapes break the model
- **Container caveat** — containers share the kernel, so one kernel bug can open every box on the host
- **The tell** — escapes are the crown-jewel bug class precisely because the sandbox model works

*Example (italic):* A full exploit chain needs a renderer bug plus an escape; illustratively the chain pays $150k against $30k for the renderer bug alone — the escape is the half that reaches your files.

**Common mistake:** Relying on the box instead of also fixing the bug inside it. The sandbox bounds the damage of the first bug — it was never a reason to stop patching.

### Visualization (canvas `c4`, 720×300)

Horizontal bar chart: illustrative bug-bounty payouts by how far the bug reaches, showing the escape premium.

- **Title (bold 15px, `#1a5276`, top center):** "What Bounties Pay: the Escape Is the Expensive Half".
- **Axis:** vertical 2px `#999` baseline at x=250, bars extend right, max width 440 (linear scale: $150k = 440px).
- **Rows (bars 18px tall, top edges at y = 80, 140, 200), each with a left-aligned 12px `#444` label at x=20 and a bold 12px payout label at the bar's right end:**
  - "script runs on one site (XSS)": blue `#2a78d6` fill `rgba(42,120,214,0.30)`, width 9, label "$3k"
  - "code exec inside the sandbox": orange `#d95926` fill at 0.30 alpha, width 88, label "$30k"
  - "full chain with sandbox escape": red `#e74c3c` fill at 0.30 alpha, width 440, label "$150k"
- **Annotation (bold 13px `#d55181`, near x=300, y=250):** "5× premium for crossing the boundary".
- **Caption (12px `#444`, bottom right):** "payouts illustrative — the premium pattern is real".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded values above (no randomness); syscall counts (330 / 290 / 60 / 0) and bounty payouts ($3k / $30k / $150k) are invented and labeled illustrative; the 270 blocked-doors figure is the arithmetic 330 − 60 and must match both text and chart annotation.
- Educational/defensive framing only: the page teaches why sandboxes exist and how they bound damage; no exploit instructions.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
