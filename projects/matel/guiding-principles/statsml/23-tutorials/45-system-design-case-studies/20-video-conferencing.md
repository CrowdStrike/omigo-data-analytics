# Video Conferencing

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Video Conferencing

**Subtitle:** In four months of 2020 daily meeting participants went from ~10 million to ~300 million — surviving a 30× step-change is possible only when the workload shards perfectly and capacity can be rented, not just bought

## Ten Million to Three Hundred Million

**Tags:** `core idea` (blue), `step-change` (green), `the pandemic 30×` (orange)

- **The baseline** — before the 2020 surge the platform publicly reported ~10 million daily participants
- **The shock** — offices, schools, and family dinners all moved onto video calls within the same month
- **The jump** — publicly reported ~200 million daily participants in March 2020, ~300 million in April
- **The multiplier** — 300M / 10M = 30× the load, arriving as a step change, not a gentle yearly ramp
- **The question** — most systems break at 3×; what let this one absorb 30× in about four months?

*Example (italic):* A capacity plan written in January 2020 for 20% yearly growth had to serve 30× the traffic by April — a decade of growth compressed into one quarter.

**Key point:** The 30× step-change is the publicly reported fact; the rest of this page is about the two properties that made surviving it possible — a shardable-by-session workload and the ability to rent capacity overnight.

### Visualization (canvas `c1`, 720×300)

Line chart of daily meeting participants from December 2019 to April 2020, with the three publicly reported points marked and the in-between path drawn as an illustrative dashed segment.

- **Title (bold 15px, `#1a5276`, top center):** "Daily Meeting Participants: ~10M (Dec 2019) → ~300M (Apr 2020)".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = months with 12px `#444` tick labels "Dec", "Jan", "Feb", "Mar", "Apr" at equal spacing; y = participants 0 to 320M, gridlines `#e5e9ef` at 100M / 200M / 300M with 12px `#444` labels "100M", "200M", "300M".
- **Reported points:** blue `#2a78d6` filled 6px circles at months `[0, 3, 4]` (Dec, Mar, Apr), millions `[10, 200, 300]`, each with a bold 12px `#2a78d6` label: "~10M", "~200M", "~300M".
- **Path:** blue `#2a78d6` 3px line; solid from Mar to Apr, dashed (dash 5/4) from Dec through Jan/Feb illustrative waypoints at millions `[10, 12, 25, 200]` for months `[0, 1, 2, 3]`.
- **Annotation (bold 13px magenta `#d55181`, near month Feb, y=90):** "30× in about four months".
- **Caption (12px `#444`, bottom right):** "Dec/Mar/Apr points publicly reported; Jan–Feb path illustrative".

## Route, Don't Mix: One Upload per Participant

**Tags:** `worked example` (blue), `SFU` (green), `media routing` (orange)

- **Mesh dies fast** — peer-to-peer mesh has each phone upload to every peer: 8 uploads in a 9-person call
- **Mixing is heavy** — an MCU decodes every stream, composites one picture, re-encodes: heavy CPU per meeting
- **The SFU** — a selective forwarding unit takes ONE upload per participant, forwards it, no re-encoding
- **Hand-check** — a 25-person mesh call needs 24 uploads per phone, ≈ 24 Mbps up, past home uplinks; SFU: 1
- **Selective** — the SFU forwards only what each viewer shows: the 9 visible gallery tiles, not all 24 streams
- **Bitrate adapts** — clients send layered/simulcast video, so a weak link gets low-res, not a dropped call

*Example (italic):* In a 25-person meeting the mesh design asks each laptop for 24 simultaneous uploads; the SFU design asks for exactly 1, and copying packets is so cheap the server barely notices.

**Key point:** An SFU turns video conferencing from an n² bandwidth problem (mesh) or a CPU-per-meeting problem (mixing) into cheap packet forwarding — one upload per participant, selective copies out. This is the same SFU pattern standard in WebRTC systems.

### Visualization (canvas `c2`, 720×300)

Grouped vertical bar chart: video uploads required per participant, mesh vs SFU, for five meeting sizes.

- **Title (bold 15px, `#1a5276`, top center):** "Uploads per Participant: Mesh Explodes, SFU Stays at 1".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; y = uploads per participant 0 to 100, gridlines `#e5e9ef` at 25 / 50 / 75 / 100 with 12px `#444` labels.
- **Groups:** five pairs of bars centered at x = 130 / 250 / 370 / 490 / 610, bars 34px wide with a 6px gap inside each pair; 12px `#444` meeting-size labels under each group: "2", "5", "9", "25", "100", plus a shared "participants" label under the axis center.
- **Mesh bars (left of each pair):** heights for uploads `[1, 4, 8, 24, 99]`, fill `rgba(217,89,38,0.25)` with 2px orange `#d95926` edge, bold 12px `#d95926` value labels above: "1", "4", "8", "24", "99".
- **SFU bars (right of each pair):** heights all `[1, 1, 1, 1, 1]`, fill `rgba(0,131,0,0.30)` with 2px green `#008300` edge, bold 12px `#008300` value label "1" above each.
- **Legend (12px, top right inside plot):** orange swatch "mesh: n−1 uploads", green swatch "SFU: 1 upload".
- **Annotation (bold 13px violet `#4a3aa7`, upper left near x=90, y=75):** "25-person mesh ≈ 24 Mbps up — beyond a home uplink".
- **Caption (12px `#444`, bottom right):** "uploads = exact arithmetic (n−1 vs 1); 1 Mbps per stream illustrative".

## Every Meeting Lives on One Server — and Extra Servers Can Be Rented

**Tags:** `why it matters` (blue), `sharding` (green), `cloud burst` (orange)

- **Independence** — meeting A never needs data from meeting B: no shared state, no cross-meeting queries
- **Perfect shard** — a meeting lands on one media server at join time; that single server is the whole shard
- **Linear scale** — doubling servers doubles meeting capacity, because nothing couples one shard to another
- **The bottleneck** — buying and racking hardware takes months; the 30× of demand arrived in a few weeks
- **The burst** — the platform publicly discussed its own data centers plus bursting to public cloud providers
- **The pairing** — sharding made extra servers useful; cloud rental made extra servers available overnight

*Example (italic):* When February demand doubled again in March, new racks were still on order — but a rented cloud region could host tens of thousands of independent meeting shards the same week.

**Key point:** Shardable-by-session workloads scale by adding servers, and the cloud lets you add servers faster than any hardware purchase — architecture and capacity agility had to work together to absorb the 30×.

### Visualization (canvas `c3`, 720×300)

Area chart of demand vs capacity, January to June 2020: a steep demand curve, a slow stair-step of owned data-center capacity, and a shaded cloud-burst band filling the gap between them.

- **Title (bold 15px, `#1a5276`, top center):** "Demand Outran the Racks: the Cloud Filled the Gap".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = months with 12px `#444` tick labels "Jan" through "Jun" at equal spacing; y = capacity/demand in relative units 0 to 32 (1 = Jan demand), gridlines `#e5e9ef` at 10 / 20 / 30 with 12px `#444` labels "10×", "20×", "30×".
- **Owned-capacity steps:** blue `#2a78d6` 3px stair-step line through month points `[0, 1, 2, 3, 4, 5]`, units `[3, 3, 5, 8, 12, 16]` (flat segments with vertical jumps at each new-hardware arrival), filled beneath with `rgba(42,120,214,0.15)`; bold 12px blue label "own data centers" inside the area near month Feb, y=215.
- **Demand curve:** magenta `#d55181` 3px line through the same month grid, units `[1, 2, 6, 20, 28, 30]`.
- **Cloud-burst band:** green fill `rgba(0,131,0,0.20)` between the owned-capacity steps and the demand curve wherever demand exceeds owned capacity (months Feb through Jun); bold 12px green `#008300` label "cloud burst" centered in the band near month Apr, y=120.
- **Annotation (bold 13px orange `#d95926`, near month Mar, y=60):** "racks take months; a cloud region takes days".
- **Caption (12px `#444`, bottom right):** "all values illustrative; the own-DC-plus-cloud-burst strategy is publicly reported".

## Adding Servers Only Works When Sessions Don't Talk

**Tags:** `common mistake` (red), `shardability` (orange)

- **The mistake** — reading this story as "any system survives 30× if you buy enough cloud capacity"
- **The condition** — the trick works because meetings share no state; every new server is a clean, empty shard
- **The counterexample** — a search index or social graph couples users, so one query touches many shards
- **The cross-talk tax** — coupled systems pay coordination cost that grows with servers; capacity is sublinear
- **The design test** — ask if one session can live and die on one server; if not, cloud burst won't save you

*Example (italic):* Doubling the servers behind a shared friend-graph does not double its capacity — every request still fans out across shards — while doubling meeting servers exactly doubles meeting capacity.

**Common mistake:** Attributing the survival to cloud spending instead of workload shape — the cloud supplied machines, but only the no-cross-session-state architecture made those machines add up linearly. Shardable-by-session workloads survive step-changes best.

### Visualization (canvas `c4`, 720×300)

Two-row diagram contrasting a shardable-by-session workload (independent meeting boxes, add a server → clean extra capacity) with a coupled workload (one query fanning out to every shard).

- **Title (bold 15px, `#1a5276`, top center):** "Shardable vs Coupled: Where 'Just Add Servers' Actually Works".
- **Row 1 (boxes centered on y=95), 12px `#444` label "meetings (shardable)" at x=20, y=58:** four rounded boxes, 120px wide, 44px tall, 8px radius, left edges at x=180 / 315 / 450 / 585: three green `rgba(0,131,0,0.12)` boxes "server 1 — meetings A,B", "server 2 — meetings C,D", "server 3 — meetings E,F", then a dashed-border green box "server 4 — rented" with bold 12px green `#008300` caption below at y=138: "+1 server = +1 server of capacity"; no arrows between boxes (nothing talks to anything).
- **Row 2 (boxes centered on y=210), label "shared graph (coupled)" at x=20, y=172:** one violet `rgba(74,58,167,0.12)` box at x=180, 120px wide, "one query"; 3px `#6b7280` arrows from it fanning out to three orange `rgba(217,89,38,0.12)` boxes at x=380 / 380 / 380 stacked at y=178 / 210 / 242 (each 200px wide, 26px tall), "shard 1", "shard 2", "shard 3", with bold 12px red `#e74c3c` caption at x=600, y=210: "every query touches every shard".
- **Box text:** 12px `#2c3e50`, one short line per box (two for the meeting servers).
- **Annotation (bold 13px magenta `#d55181`, centered near y=280):** "the cloud rents you machines; only shardability makes them add up".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); the ~10M (Dec 2019) / ~200M (Mar 2020) / ~300M (Apr 2020) daily-meeting-participant figures and the own-data-centers-plus-public-cloud-burst strategy are publicly reported for video-conferencing platforms of that period; mesh-vs-SFU upload counts (n−1 vs 1) are exact arithmetic; per-stream bandwidth, the Jan–Feb demand path, and all capacity/demand curve values are invented and labeled illustrative.
- **Framing:** treat the whole page as a generic system-design exercise built from publicly reported facts and standard WebRTC/SFU concepts — make no claims about any company's current internal systems.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
