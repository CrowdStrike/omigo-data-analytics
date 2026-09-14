# Regions & Availability Zones

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Regions & Availability Zones

**Subtitle:** Clouds arrange data centers so a fire, a flood, or a bad deploy each destroys a pre-agreed slice and nothing more — geography as blast-radius engineering

## One Flood Shouldn't Stop Checkout

**Tags:** `core idea` (blue), `blast radius` (green), `geography` (orange)

- **The service** — an online store's checkout runs in a cloud region that contains three availability zones
- **The AZ** — each zone is one or more data centers with its own power feed, cooling, and network
- **The spacing** — zones sit miles apart: close enough for ~1 ms links, far enough that one flood hits one
- **The region** — a region is that cluster of zones; a second region sits thousands of miles away at ~35 ms
- **The design** — geography is chosen so a defined failure takes out a defined slice, never everything

*Example (italic):* A transformer fire blacks out zone A at noon; zones B and C, on different power grids ten miles away, keep checkout running.

**Key point:** An availability zone is an independent-infrastructure failure domain; a region groups a few of them close enough for fast links but far enough apart to fail separately.

### Visualization (canvas `c1`, 720×300)

Map-style diagram: one region box containing three AZ boxes joined by ~1 ms links, and a distant second region joined by a ~35 ms link.

- **Title (bold 15px, `#1a5276`, top center):** "One Region = Three Independent Failure Domains, One Millisecond Apart".
- **Region A box:** dashed 2px `#1a5276` rounded rectangle x=30, y=55, w=400, h=210, 12px `#1a5276` label "region us-east" at its top-left inside edge.
- **AZ boxes (inside region A):** three rounded boxes 100×64 at (x=55, y=110), (x=180, y=110), (x=305, y=110); fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border; 12px `#2c3e50` two-line labels "AZ-a / own power+net", "AZ-b / own power+net", "AZ-c / own power+net".
- **Inter-AZ links:** 2px `#199e70` lines connecting the three boxes pairwise along y=195 (a-b, b-c) plus an arc a-c; each with a bold 12px `#199e70` label "~1 ms".
- **Region B box:** dashed 2px `#6b7280` rounded rectangle x=530, y=85, w=160, h=150, 12px `#6b7280` label "region eu-west"; two small AZ boxes 60×44 inside at (x=545, y=130) and (x=615, y=130), fill `rgba(107,114,128,0.12)`.
- **Cross-region link:** 2px dashed `#d95926` line from region A's right edge (x=430, y=160) to region B's left edge (x=530, y=160), bold 12px `#d95926` label "~35 ms — 4,500 mi" above it.
- **Annotation (bold 13px `#008300`, centered near y=282):** "far enough apart for separate floods, close enough for synchronous replication".
- **Caption (12px `#444`, bottom right):** "latencies illustrative of typical clouds".

## Spreading Checkout Across Three Zones

**Tags:** `worked example` (blue), `replication` (green)

- **The layout** — three copies of checkout, one per zone, behind a load balancer splitting traffic evenly
- **The write** — an order commits once the primary in zone A and one replica in B or C both acknowledge
- **The math** — local write 1 ms + inter-AZ round trip 2 ms ≈ 3 ms per order; customers never notice
- **Cross-region** — the same synchronous ack to eu-west costs 35 ms each way: ~71 ms per order, too slow
- **The rule** — replicate synchronously inside a region, asynchronously (seconds behind) across regions

*Example (italic):* An order placed at 12:00:00.000 is durable in two zones by .003; the eu-west copy catches up about a second later.

**Key point:** Physics draws the boundary — single-digit-ms links inside a region permit synchronous replication; a ~70 ms round trip between regions forbids it, so cross-region copies are always slightly stale.

### Visualization (canvas `c2`, 720×300)

Horizontal bar chart: order-commit latency under four replication choices, showing why sync stops at the region edge.

- **Title (bold 15px, `#1a5276`, top center):** "Where Synchronous Replication Stops Being Affordable".
- **Axis:** vertical 2px `#999` baseline at x=250, bars extend right, max width 430; log-feel achieved by hardcoded pixel widths, not a real log axis.
- **Rows (top to bottom at y = 70, 120, 170, 220), each with a left-aligned 12px `#444` label at x=20:**
  - "commit in one zone — 1 ms": blue `#2a78d6` bar width 12
  - "sync commit across 2 AZs — 3 ms": green `#008300` bar width 36, bold 12px green label "the standard design"
  - "async copy to eu-west — ack still 3 ms": green bar width 36, 11px `#6b7280` note "region copy lags ~1 s"
  - "sync commit to eu-west — 71 ms": red `#e74c3c` bar width 430, bold 12px red label "24× slower — physics says no"
- **Bar style:** 16px tall, solid fills, 11px `#444` millisecond labels at bar ends where no colored label sits.
- **Annotation (bold 13px violet `#4a3aa7`, right side near y=255):** "sync within a region, async across regions".
- **Caption (12px `#444`, bottom right):** "pixel widths schematic, milliseconds illustrative".

## Every Failure Gets a Fence

**Tags:** `where it's used` (blue), `containment` (green)

- **Blast radius** — before anything fails, you decide the largest thing each failure is allowed to break
- **Bad deploy** — roll to one zone first; a crashing build takes out a third of capacity, not all of it
- **Zone outage** — flood, fire, or grid loss kills one AZ; the other two absorb its traffic
- **Region outage** — rare but real; the async eu-west copy is promoted, losing the last few seconds
- **The ladder** — each rung caps the damage: one host, one zone, one region — never the whole system

*Example (italic):* A bad config pushed zone-by-zone crashes zone A at 12:00; the rollout halts automatically and zones B and C never receive it.

**Key point:** The hierarchy exists so every failure mode has a pre-drawn containment line — you know before the incident how much of the fleet it may take with it.

### Visualization (canvas `c3`, 720×300)

Three-row grid diagram: nine server squares (three per AZ) with the blast of each failure type shaded red, showing the containment ladder.

- **Title (bold 15px, `#1a5276`, top center):** "Blast Radii: What Each Failure Is Allowed to Take".
- **Rows (y = 80, 150, 220), left-aligned 12px `#444` labels at x=20:** "bad deploy (canary zone)", "AZ outage", "region outage".
- **Server squares:** each row draws 9 squares 26×26 starting at x=250, spacing 32 within an AZ group of 3, extra 16px gap between groups (group starts x=250, 362, 474); healthy fill `rgba(42,120,214,0.25)` with 2px `#2a78d6` border, dead fill `rgba(231,76,60,0.25)` with 2px `#e74c3c` border and 12px red "✗" centered.
- **Row 1 shading:** squares 1–3 dead (the whole canary zone, AZ-a), bold 12px `#e74c3c` right-side label at x=600 "1 zone = 3/9".
- **Row 2 shading:** squares 1–3 dead (all of AZ-a), right-side label "3/9 of fleet".
- **Row 3 shading:** all 9 dead, right-side label "9/9 — fail over to eu-west".
- **AZ group captions (11px `#6b7280`, above row 1 at y=58):** "AZ-a", "AZ-b", "AZ-c" centered over each group.
- **Annotation (bold 13px `#008300`, centered near y=278):** "each rung of the ladder caps the damage before it happens".
- **Caption (12px `#444`, bottom right):** "9-server fleet schematic".

## Three Zones Are Not Automatic Safety

**Tags:** `common mistake` (red), `headroom` (orange)

- **The headroom trap** — two survivors must absorb the dead zone's traffic; too busy and they can't
- **The math** — 300 servers at 60% busy = 180 servers of work; lose a zone, 200 remain: 90% busy
- **The stampede** — every tenant fails over to the same surviving zones at once, and capacity runs out
- **Control plane** — if the tool that moves traffic lives in the dead zone, the failover never starts
- **Multi-region** — active-active across regions is a cost and complexity step-change: justify it, don't default

*Example (italic):* Run the same fleet at 75% busy and losing a zone leaves 225 servers of work on 200 servers — 112%, and the survivors cascade.

**Common mistake:** Counting zones instead of headroom and dependencies — three AZs only contain a blast if the survivors have spare capacity and the failover machinery lives outside the blast.

### Visualization (canvas `c4`, 720×300)

Grouped bar chart: fleet utilization before and after losing one of three zones, at two operating points — one survives, one cascades.

- **Title (bold 15px, `#1a5276`, top center):** "Losing One of Three Zones Multiplies Load on Survivors by 1.5×".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; y = utilization 0 to 120% with 12px `#444` tick labels at 0/30/60/90/120, gridlines `#e5e9ef`; dashed 2px `#e74c3c` line across the plot at 100% with 12px red label "capacity ceiling" at its right end.
- **Bars (70px wide):** heights at 1.5 px per percent —
  - pair 1 "run at 60% busy" (12px `#444` group label centered under x≈220): blue `#2a78d6` bar at x=150, value 60% (height 90) labeled "before"; orange `#d95926` bar at x=250, value 90% (height 135) labeled "after AZ loss"
  - pair 2 "run at 75% busy" (group label under x≈500): blue bar at x=430, value 75% (height 113) labeled "before"; red `#e74c3c` bar at x=530, value 112% (height 168) crossing the ceiling line, labeled "after AZ loss"
- **Value labels:** bold 12px above each bar in the bar's color: "60%", "90%", "75%", "112%".
- **Annotation (bold 13px `#e74c3c`, near x=430, y=55):** "112% — the survivors cascade".
- **Caption (12px `#444`, bottom right):** "arithmetic exact: demand ×1.5 when 1 of 3 zones dies; fleet sizes illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all values are the hardcoded numbers above (no randomness); latencies (1 / 3 / 71 / 35 ms) and distances are illustrative of typical clouds; the utilization arithmetic in c4 (60→90%, 75→112% when 1 of 3 zones dies) is exact given the stated fleet sizes, which are themselves illustrative.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
