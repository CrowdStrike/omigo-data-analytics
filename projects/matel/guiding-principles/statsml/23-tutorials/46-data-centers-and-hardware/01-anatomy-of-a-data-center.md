# Anatomy of a Data Center

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Anatomy of a Data Center

**Subtitle:** A server slots into a rack, racks line up into rows, rows fill a hall, halls make a site — and every level is a group of machines that can die together

## From Pizza Box to Campus

**Tags:** `core idea` (blue), `physical hierarchy` (green), `hardware` (orange)

- **The server** — a flat 1–2U "pizza box" with CPUs, RAM, and disks; on its own it is just one machine
- **The rack** — ~42U of vertical slots; about 40 servers share one power strip and one network switch
- **The switch** — the top-of-rack (ToR) switch is the rack's only door to the network
- **The row** — 20 racks stand shoulder to shoulder, fed by the same power distribution unit
- **The hall and site** — rows fill a data hall; a few halls (plus cooling and generators) make a site

*Example (italic):* One search query lands on a server in slot U23 of rack 07, in row 14 of hall 2 — four nested layers of shared hardware around one machine.

**Key point:** A data center is a strict physical hierarchy — server inside rack inside row inside hall inside site — and each layer shares power, network, or cooling with its neighbors.

### Visualization (canvas `c1`, 720×300)

Nested-box diagram of the hierarchy: a site box containing hall boxes, one hall containing rows, one row containing racks, one rack containing a highlighted server.

- **Title (bold 15px, `#1a5276`, top center):** "One Server, Four Layers of Shared Hardware".
- **Site box:** rounded rect at x=40, y=50, width 640, height 220, 2px `#1a5276` border, fill `rgba(26,82,118,0.05)`, bold 12px `#1a5276` label "site — 4 halls" at top-left inside.
- **Hall boxes:** four rounded rects inside the site at x=60/175/290/405, y=80, each width 105, height 175, 2px `#2a78d6`, fill `rgba(42,120,214,0.08)`; only the first labeled (12px) "hall — 25 rows"; halls 2–4 get muted 11px `#6b7280` labels "hall".
- **Row strips:** inside hall 1, five horizontal strips at y=105/130/155/180/205, x=70, width 85, height 18, 1px `#199e70`, fill `rgba(25,158,112,0.12)`; middle strip labeled 11px `#199e70` "row — 20 racks" with a short callout line to the right.
- **Rack box:** inside the middle strip, a small rect at x=78, width 14, height 14, 2px `#d95926`, fill `rgba(217,89,38,0.25)`; callout path (routed left of the row strips, then above the hall boxes along y=75) to a zoomed rack drawn right of the halls at x=560, y=95: a 60×140 rect, 2px `#d95926`, with 8 thin horizontal server slats, labeled bold 12px `#d95926` "rack — 40 servers + ToR switch" beneath.
- **Server highlight:** one slat in the zoomed rack filled solid `#008300`, bold 12px green label "your server (1U)" with arrow.
- **Caption (12px `#444`, bottom right):** "counts illustrative — layouts vary by operator".

## Counting the Blast Radius

**Tags:** `worked example` (blue), `failure domain` (red)

- **Shared fate** — everything inside a layer dies together when that layer's shared part fails
- **ToR dies** — the rack's 40 servers are healthy but unreachable: blast radius 40 machines
- **Row feed dies** — 20 racks × 40 servers lose power together: blast radius 800 machines
- **Hall event** — fire, flood, or cooling failure takes 25 rows × 800: blast radius 20,000 machines
- **Site event** — 4 halls × 20,000 = 80,000 machines gone at once; hand-check: 40 × 20 × 25 × 4

*Example (italic):* A single failed ToR switch silently removes 40 servers; a hall-level cooling failure removes 20,000 — a 500× bigger hole from one physical event.

**Key point:** Each level of the hierarchy is a failure domain — one broken shared component (switch, power feed, chiller) takes out every machine beneath it, from 40 to 80,000 in four steps.

### Visualization (canvas `c2`, 720×300)

Horizontal bar chart of machines lost per failure, one bar per level, log-feel widths.

- **Title (bold 15px, `#1a5276`, top center):** "Blast Radius: Machines Lost When One Shared Part Fails".
- **Axis:** vertical 2px `#999` baseline at x=250, bars extend right, max width 350; log-feel achieved by hardcoded pixel widths, not a real log axis.
- **Rows (top to bottom at y = 75, 125, 175, 225), each with a left-aligned 12px `#444` label at x=20:**
  - "ToR switch fails — 1 rack": green `#008300` bar width 60, 12px value label "40 machines" at bar end
  - "power feed fails — 1 row": aqua `#199e70` bar width 180, label "800 machines"
  - "cooling fails — 1 hall": orange `#d95926` bar width 320, label "20,000 machines"
  - "site event — whole campus": red `#e74c3c` bar width 350, label "80,000 machines"
- **Bar style:** 18px tall, solid fills at 0.75 alpha, 2px matching border, value labels 12px bold in the bar color.
- **Annotation (bold 13px `#e74c3c`, right side near y=50):** "one part, thousands of machines".
- **Caption (12px `#444`, bottom right):** "40 × 20 × 25 × 4 hierarchy, illustrative".

## Cattle, Coordinates, and Megawatts

**Tags:** `where it's used` (blue), `fleet thinking` (green)

- **Cattle, not pets** — hyperscalers buy one identical server SKU by the thousand, not hand-built boxes
- **Coordinates, not names** — a machine is "hall2-row14-rack07-u23", never "zeus" or "db-master"
- **Drain, don't panic** — a dead machine is drained of work and left dark; nobody runs to fix it
- **Batch repair** — at ~80 failures/day in an 80,000-machine site, techs sweep once a week
- **Megawatts, not machine counts** — capacity is bought as power: this site draws about 24 MW

*Example (italic):* By Friday the repair queue holds ~560 dead machines — 0.7% of the fleet — and one technician sweep fixes them all in a day while software routed around them all week.

**Key point:** At fleet scale, individual machines stop mattering — operators manage identical units by coordinate, tolerate a steady ~1% dead pool, and size everything in megawatts of power.

### Visualization (canvas `c3`, 720×300)

Step/area chart of the dead-machine pool over one week: failures accumulate daily, then a Friday batch repair empties the queue.

- **Title (bold 15px, `#1a5276`, top center):** "The Repair Queue: Drain All Week, Fix in One Batch".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = days "Mon"–"Sun" with 12px `#444` tick labels; y = dead machines 0 to 700, gridlines `#e5e9ef` at 175/350/525.
- **Queue steps:** blue `#2a78d6` 3px step line with fill `rgba(42,120,214,0.25)` through day-end values `[240, 320, 400, 480, 560, 80, 160]` (Mon–Sun) — +80 per day, peaking at 560 on Friday, cliff to 80 after the Friday sweep (Saturday's failures land on a near-empty queue).
- **Repair marker:** vertical dashed `#008300` (dash 4/3) line at Friday, bold 12px green label "batch repair: 560 fixed" at its top.
- **Reference line:** horizontal dashed `#6b7280` line at y for 560, 11px `#6b7280` label "0.7% of the 80,000 fleet" at its right end.
- **Annotation (bold 13px violet `#4a3aa7`, near Wed, y=80):** "no urgent tickets — software routes around the dead".
- **Caption (12px `#444`, bottom right):** "failure counts illustrative (~0.1%/day)".

## Three Copies in the Same Rack

**Tags:** `common mistake` (red), `replica placement` (orange)

- **The mistake** — storing all 3 replicas of a data block on servers that share a rack
- **Why it happens** — same-rack servers are "close" (fast network), so naive placement clusters them
- **Shared fate strikes** — one ToR failure hides all 3 copies at once; redundancy was an illusion
- **The fix** — placement must know the hierarchy: spread copies across racks, and across halls
- **The rule** — replicas only protect you against failures of the layers they do not share

*Example (italic):* Three copies in rack 07 survive any single disk failure but zero switch failures; spread across three halls, they survive a whole hall burning down.

**Common mistake:** Counting copies instead of counting failure domains. Three replicas behind one ToR switch is one failure domain — software must map placement onto the physical rack/row/hall tree.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: replicas packed in one rack (ToR failure loses all 3) vs replicas spread across three racks in different halls (2 of 3 survive).

- **Title (bold 15px, `#1a5276`, top center):** "Same Data, Two Placements: One ToR Failure Later".
- **Row 1 (y=95), label 12px `#444` at x=20:** "all in rack 07"; three small blue `#2a78d6` boxes (46×30, 6px radius, fill `rgba(42,120,214,0.15)`) side by side at x=150/205/260 labeled "R1" "R2" "R3", enclosed in a dashed `#6b7280` rect labeled 11px "rack 07 — one ToR"; 3px arrow to a red `#e74c3c` box at x=430 (170×40) labeled "ToR fails — 0 copies reachable" with bold 12px red "✗ data offline".
- **Row 2 (y=205), label:** "spread across halls"; three blue boxes at x=150/260/370, each in its own dashed rect labeled 11px "hall 1 / hall 2 / hall 3"; the hall-1 box turns red with a small ✗ (that rack's ToR fails), 3px arrow to a green `#008300` box at x=520 (170×40) labeled "2 of 3 copies alive" with bold 12px green "✓ data serves".
- **Box style:** fills `rgba(42,120,214,0.15)` / `rgba(231,76,60,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px orange `#d95926`, centered near y=270):** "spread replicas across failure domains, not just across disks".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); the 40 / 20 / 25 / 4 hierarchy, blast-radius counts (40 / 800 / 20,000 / 80,000), repair-queue steps `[240, 320, 400, 480, 560, 80, 160]`, the 560-machine batch, 0.7% dead-pool figure, and 24 MW site draw are invented and labeled illustrative; 42U rack height and 1–2U server sizes are standard hardware facts.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
