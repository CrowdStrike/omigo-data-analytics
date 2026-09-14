# Power

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Power

**Subtitle:** A data center is a machine for turning grid power into computation — sites are sized in megawatts, and power, not land or servers, decides how big they can get

## A Machine for Turning Megawatts into Computation

**Tags:** `core idea` (blue), `megawatts` (green), `capacity` (orange)

- **The site** — a new data center is announced as "30 megawatts", not as square feet or server count
- **The rack** — a modern rack of servers draws about 10 kW, roughly seven electric kettles running flat out
- **The division** — 30 MW at 10 kW per rack is at most 3,000 racks; the power budget fixes compute
- **The constraint** — land, concrete, and servers are all buyable; a 30 MW grid connection can take years
- **The consequence** — operators pick sites by substation capacity first, geography second

*Example (italic):* Two identical buildings sit side by side; the one with a 30 MW utility feed holds up to 3,000 racks, the one with a 3 MW feed holds 300 — same walls, 10× the computer.

**Key point:** A data center's size is its power number: megawatts in fixes racks in, and the grid connection — not land or hardware — is the binding constraint on new capacity.

### Visualization (canvas `c1`, 720×300)

Horizontal bar chart: four site sizes in MW, each bar annotated with the rack count it buys at 10 kW per rack.

- **Title (bold 15px, `#1a5276`, top center):** "Power In = Racks In: Site Size at 10 kW per Rack".
- **Axis:** left labels end at x=195, bars start at x=200, max bar width 430; pixel widths schematic (not to a linear MW scale), 2px `#999` vertical baseline at x=200.
- **Rows (top to bottom at y = 70, 120, 170, 220), each with a right-aligned 12px `#444` label at x=190 and a 22px-tall bar:**
  - "small colo — 1 MW": blue `rgba(42,120,214,0.30)` bar width 60, 12px `#2a78d6` label "100 racks" at bar end
  - "enterprise — 5 MW": blue bar width 140, label "500 racks"
  - "hyperscale hall — 30 MW": green `rgba(0,131,0,0.25)` bar width 300 with 2px `#008300` edge, bold 12px `#008300` label "3,000 racks"
  - "campus — 300 MW": violet `rgba(74,58,167,0.22)` bar width 430, 12px `#4a3aa7` label "30,000 racks"
- **Annotation (bold 13px green `#008300`, near x=330, y=95):** "30 MW ÷ 10 kW/rack = 3,000 racks (exact)".
- **Caption (12px `#444`, bottom right):** "bar widths schematic; rack arithmetic exact, 10 kW/rack illustrative".

## From the Grid to the Rack: the Power Chain

**Tags:** `worked example` (blue), `UPS` (green), `generators` (orange)

- **The chain** — utility feed → transformers → switchgear → UPS → PDUs → rack: every watt walks this path
- **The failure** — at 14:00:00 the utility feed drops; 2,000 racks are one second from going dark
- **The bridge** — UPS batteries take the full 20 MW IT load instantly; they can hold it for minutes, not hours
- **The start** — diesel generators auto-start at the outage signal and are up to speed in about 15 seconds
- **The transfer** — at ~30 seconds switchgear moves the load from batteries to generators; racks never blinked

*Example (italic):* The grid fails at 14:00:00; batteries carry 20 MW until 14:00:30, generators carry it after — every server sees clean, uninterrupted power the whole time.

**Key point:** The UPS is not the backup — the generators are; batteries exist only to bridge the seconds between the grid failing and the generators taking the load.

### Visualization (canvas `c2`, 720×300)

Step/timeline chart of the 60 seconds around a grid failure: IT load stays flat at 20 MW while the source underneath it hands off from grid to battery to generator.

- **Title (bold 15px, `#1a5276`, top center):** "Grid Fails at t=0: Batteries Bridge, Generators Take Over".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = seconds -10 to 60, 12px `#444` tick labels at -10/0/15/30/45/60; y = MW 0 to 25, gridlines `#e5e9ef` at 5/10/15/20.
- **Load line:** ink `#1a5276` 3px horizontal line at 20 MW across the full x-range — dead flat, no dip.
- **Source bands (filled rectangles under the load line, 0 to 20 MW):** grid band blue `rgba(42,120,214,0.25)` from t=-10 to 0; battery band orange `rgba(217,89,38,0.25)` from t=0 to 30; generator band green `rgba(0,131,0,0.22)` from t=30 to 60; 12px `#444` band labels "grid" / "UPS battery" / "generator" centered in each band at y≈180.
- **Event markers:** vertical dashed `#6b7280` (dash 4/3) lines at t=0 ("feed lost", 12px `#6b7280` label at top), t=15 ("generators at speed"), t=30 ("switchgear transfers load").
- **Annotation (bold 13px ink `#1a5276`, near t=40, y=70):** "the load line never dips — that is the whole job".
- **Caption (12px `#444`, bottom right):** "timings typical, illustrative; 20 MW is this page's running IT load".

## PUE: What Every Watt of Compute Really Costs

**Tags:** `worked example` (blue), `PUE` (green), `efficiency` (orange)

- **Two meters** — one meter reads total facility power at the fence, one reads IT power at the racks
- **The site** — the racks draw 20 MW, but the fence meter reads 30 MW; 10 MW went to cooling and losses
- **The ratio** — 30 MW total ÷ 20 MW IT = 1.5; that ratio is the PUE (Power Usage Effectiveness)
- **The reading** — PUE 1.5 means every watt of compute costs an extra half-watt of overhead, mostly cooling
- **The bar** — modern hyperscale sites publish PUE near 1.1: 20 MW of IT needs only 22 MW at the fence

*Example (italic):* At 20 MW of IT load, cutting PUE from 1.5 to 1.1 drops the fence meter from 30 MW to 22 MW — 8 MW saved without removing a single server (arithmetic exact).

**Key point:** PUE = total facility power ÷ IT power; it prices the overhead of computing — a PUE of 1.5 means half a watt of cooling and losses rides on every watt of useful work.

### Visualization (canvas `c3`, 720×300)

Stacked horizontal bar chart: three PUE levels at the same 20 MW IT load, blue IT segment constant, orange overhead segment shrinking.

- **Title (bold 15px, `#1a5276`, top center):** "Same 20 MW of Compute, Three PUE Levels (exact arithmetic)".
- **Axis:** bars start at x=200, scale 14 px per MW (20 MW = 280 px), 2px `#999` vertical baseline at x=200; light dashed `#e5e9ef` vertical reference line at the 20 MW mark; x tick labels 0/10/20/30 MW (12px `#444`) along y=255.
- **Rows (top to bottom at y = 80, 140, 200), each with a right-aligned 12px `#444` label at x=190 and a 26px-tall stacked bar:**
  - "PUE 1.5 — older site": blue `rgba(42,120,214,0.30)` IT segment width 280, orange `rgba(217,89,38,0.35)` overhead segment width 140, 12px `#444` end label "30 MW total"
  - "PUE 1.2": blue segment width 280, orange segment width 56, end label "24 MW total"
  - "PUE 1.1 — hyperscale": blue segment width 280, orange segment width 28, bold 12px `#008300` end label "22 MW total"
- **Segment labels (12px, inside bars):** white "IT 20 MW" in each blue segment; `#d95926` "overhead 10 MW" / "4 MW" / "2 MW" beside each orange segment.
- **Annotation (bold 13px green `#008300`, near x=480, y=272, below the x tick labels):** "1.5 → 1.1 frees 8 MW: room for 800 more racks".
- **Caption (12px `#444`, bottom right):** "PUE arithmetic exact; 20 MW IT load illustrative".

## N+1 Is Not 2N

**Tags:** `common mistake` (red), `redundancy` (orange)

- **The vocabulary** — N is just enough equipment to carry the load; N+1 adds one spare; 2N doubles everything
- **N+1 in practice** — the load needs 4 UPS units, so 5 are installed; any one unit can fail and be covered
- **The gap** — those 5 units feed one shared distribution path; if that path fails, the spare is useless
- **2N in practice** — two fully independent chains, A and B; every rack plugs into both feeds at once
- **The mistake** — reading "N+1 redundant" as "survives anything"; it survives a component, not a path

*Example (italic):* An electrician's error de-energizes the shared bus of an N+1 room and all 2,000 racks go dark; in a 2N room the same error kills the A feed and every rack rides through on B.

**Common mistake:** Treating N+1 and 2N as synonyms for "redundant". N+1 spares a component inside one path; 2N gives every rack two independent paths — they fail in completely different ways and are priced accordingly.

### Visualization (canvas `c4`, 720×300)

Two-row schematic: an N+1 room where a shared-bus failure blacks out the load, vs a 2N room where the B feed carries the load through the same failure.

- **Title (bold 15px, `#1a5276`, top center):** "One Spare vs Two Paths: the Failure N+1 Cannot Cover".
- **Row 1 (centerline y=95), label 12px `#444` at x=20:** "N+1"; five small blue `rgba(42,120,214,0.15)` rounded boxes (34×26, 6px radius) at x=110..270 labeled "UPS 1"–"UPS 5" (11px), the fifth tagged 11px `#2a78d6` "spare"; 3px arrows converging to one red `#e74c3c` box (150×40) at x=340 labeled "shared bus — FAILS" (12px), then a dashed arrow to a grey `rgba(107,114,128,0.15)` box at x=560 labeled "racks dark" with bold 12px red "✗ spare can't help".
- **Row 2 (centerline y=215), label:** "2N"; two stacked chains — blue box (110×32) at x=110 "A: UPS + bus" (y=195) with a red 12px "✗ fails" strike, green `rgba(0,131,0,0.12)` box at x=110 "B: UPS + bus" (y=237) — both feeding 3px arrows into one green box (150×40) at x=430 labeled "rack: dual feeds" with bold 12px green "✓ B carries 100%".
- **Box style:** 6-8px radius, 11-12px `#2c3e50` text, arrow color matches source box edge.
- **Annotation (bold 13px orange `#d95926`, centered near y=280):** "N+1 spares a component; 2N spares a whole path".
- **Caption (12px `#444`, bottom right):** "topology schematic; unit counts illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all values are the hardcoded numbers above (no randomness); PUE arithmetic (20 MW IT × 1.5 = 30 MW, × 1.2 = 24 MW, × 1.1 = 22 MW; 30 MW ÷ 10 kW/rack = 3,000 racks) is exact; the 20 MW running load, 10 kW/rack density, outage timings, and UPS unit counts are invented and labeled illustrative.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
