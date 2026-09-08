# cloud platform A / cloud platform B Cloud Platforms

**Page type:** detail page (one h2 per pitfall, each followed by a two-column obj-table row: text left 50%, canvas right 50%)
**HTML title tag:** 26. cloud platform A / cloud platform B Cloud Platforms — Domain Pitfalls

**Subtitle:** Data pitfalls unique to multi-cloud and hyperscaler environments

## Callout (philosophy box)

Cloud platforms introduce data challenges that don't exist in on-premises environments: service names change, billing is delayed, metrics aren't comparable across providers, and multi-tenancy creates invisible confounders. These pitfalls compound when organizations operate across multiple clouds.

## SKU Churn and Service Renaming

**Obj-title:** Services rename faster than data pipelines update

- cloud platform A renamed services 3x in 5 years (e.g., cloud platform A AD → provider A cloud directory)
- Historical data with old SKU names doesn't join to current catalog
- "Same service" has different telemetry schema after migration
- Breaking changes disguised as "rebranding"

**Impact:** Time-series analysis breaks at rename boundaries. Year-over-year comparisons require manual mapping tables that are never complete. Automated reports silently drop historical data when JOIN keys stop matching.

### Visualization (canvas `c1`, 720×300)

Timeline diagram of three service-name eras with broken joins between them.

- **Timeline base:** horizontal gray line (`#bbb`, width 2) at y=150 from x=60 to x=680.
- **Era boxes (each 120px tall at y=90, 20% alpha fill, 3px solid stroke of same color, era name in bold 15px centered on the timeline):**
  - x 80–250: "cloud platform A AD", `#3498db`
  - x 270–440: "cloud platform A Active Directory", `#2ecc71`
  - x 460–640: "provider A cloud directory", `#9b59b6`
- **Broken joins:** dashed red (`#e74c3c`, dash 6/4, width 2) horizontal connector segments at y=130 and y=170 in each gap between eras, with a bold red "✗" (22px) centered in each gap; bold red labels "JOIN fails" above each gap (at x=260 and x=450) with short arrows pointing down at the breaks.
- **Year labels (`#666`, centered under each era):** "2019", "2021", "2023"; below them in `#888` 13px: "Schema v1", "Schema v2", "Schema v3".

## Cross-Tenant Isolation Assumptions

**Obj-title:** Invisible external factors in multi-tenant environments

- Multi-tenant service: "noisy neighbor" affects your metrics but you can't see their load
- Performance variance is partially caused by invisible external factors
- Latency spikes correlate with nothing in YOUR data
- Root cause analysis hits a wall at the tenant boundary

**Impact:** Models trained on your metrics alone have unexplained variance. Anomaly detection fires false positives during neighbor activity. Capacity planning underestimates because baseline performance is non-deterministic.

### Visualization (canvas `c2`, 720×300)

Split diagram: your visible latency line on the left, the neighbor's invisible load bars on the right, divided by a tenant-boundary wall.

- **Wall:** solid gray (`#95a5a6`) vertical bar 8px wide at x=350 from y=30 to y=270, with rotated bold gray label "TENANT BOUNDARY".
- **Left panel title (bold 15px `#1a5276`):** "Your Metrics (visible)".
- **Your latency line (`#2980b9`, width 2):** 16 points starting at x=60, 18px apart, values `[40, 38, 42, 80, 35, 37, 90, 40, 38, 85, 36, 40, 38, 42, 95, 37]` scaled ×1.8 up from y=220. Red "?" marks above the unexplained spikes at indices 3, 6, 9, 14. Axis caption "Latency (ms)" in `#2980b9`.
- **Right panel title (bold 15px `#888`):** "Neighbor Load (invisible)".
- **Neighbor bars (40% alpha, 12px wide, starting x=385, 18px apart):** values `[20, 15, 25, 95, 10, 20, 90, 15, 18, 88, 12, 22, 18, 20, 92, 15]` scaled ×1.8; bars >80 colored `#e74c3c`, others `#95a5a6`.
- **Correlation:** dashed red (`#e74c3c`, dash 4/3, width 1.5) lines connecting each of your spikes (indices 3, 6, 9, 14) to the corresponding neighbor bar.
- **Caption (red, bottom center):** "Correlated but invisible to you".

## Billing Lag

**Obj-title:** Usage today, invoice in 30+ days

- Usage today → invoice in 30+ days
- Cost analysis is always retrospective
- Real-time cost = estimate only (can change by 15-20%)
- Budget alerts fire late — damage already done

**Impact:** "Real-time" cost dashboards are fiction. Decisions based on estimated costs get revised weeks later. Budget overruns are detected after the fact. Chargeback to teams uses stale data that doesn't match their memory of events.

### Visualization (canvas `c3`, 720×300)

Timeline diagram from usage event to invoice arrival with an estimate zone between.

- **Title (bold 15px `#1a5276`, top left):** "Cloud Billing Timeline".
- **Timeline:** horizontal `#333` line at y=180 from x=80 to x=660 ending in an arrowhead; tick marks every 100px labeled "Day 0", "Day 7", "Day 14", "Day 21", "Day 30", "Day 37" in `#666` 13px.
- **Usage event:** green (`#2ecc71`) box at (90,100) 60×50 with white bold text "USAGE" / "occurs".
- **Estimate zone:** orange (`#f39c12`) box at (160,60) 280×100, 20% alpha fill, dashed (5/3) orange border; bold orange label "ESTIMATE ZONE" above and 12px lines inside: "±15-20% variance" / "Budget alerts based on estimates" / "(may fire late or not at all)".
- **Invoice:** red (`#e74c3c`) box at (490,80) 80×70 with white bold text "INVOICE" / "arrives" / "(actual)".
- **Lag arrow:** dashed red (dash 6/3, width 2) line from the usage box to the invoice box.
- **Captions (centered below timeline):** bold red 15px "30+ day lag"; `#666` 13px "Decisions made on estimates, corrected weeks later".

## Resource Tagging Inconsistency

**Obj-title:** Inconsistent tags destroy cost attribution

- Team A: "env:prod", Team B: "environment:production", Team C: doesn't tag
- Cost attribution by tag: 40% untagged → "unknown"
- Analysis on tagged subset only = survivorship bias
- Tag policies enforced too late — historical resources untagged

**Impact:** Cost reports show 40% as "unknown" — useless for decision-making. Analysis restricted to tagged resources is biased toward compliant teams (survivorship bias). Trends based on partial data mislead executives about true cost drivers.

### Visualization (canvas `c4`, 720×300)

Pie chart of cost attribution by tag with legend and survivorship-bias annotation.

- **Title (bold 15px `#1a5276`, top left):** "Cost Attribution by Tag".
- **Pie:** center (280,150), radius 110, starting at 12 o'clock, white 2px slice borders. Slices:
  - 40% `#e74c3c` — "Unknown/Untagged (40%)"
  - 20% `#3498db` — "env:prod (Team A)"
  - 18% `#2ecc71` — "environment:production (Team B)"
  - 12% `#f39c12` — "ENV=PROD (Team D)"
  - 10% `#9b59b6` — "Tagged other"
- **Legend:** right side (x=440), 14px color squares with the labels above in `#333` 13px.
- **Annotation (right, red):** bold "Survivorship bias:" then 12px "Analysis on tagged" / "subset only misses 40%".
- **Caption (bottom right, `#666` 12px):** "All mean \"production\" but won't aggregate".

## Multi-Cloud Metric Incompatibility

**Obj-title:** "85% CPU" doesn't mean the same thing across providers

- CPU% on cloud provider vs cloud platform A vs cloud platform B = different measurement methods
- cloud provider: CPU% includes hypervisor-level steal time; cloud platform A: guest-only measurement
- cloud platform B: CPU% normalized to a baseline, so the same number describes a different load
- "85% CPU" doesn't mean the same thing across providers
- Can't compare or combine into unified dashboards

**Impact:** Multi-cloud dashboards that show metrics side-by-side are misleading. "Normalize to same threshold" doesn't work because the underlying measurements differ. Capacity decisions based on cross-cloud comparisons allocate resources incorrectly.

### Visualization (canvas `c5`, 720×300)

Grouped comparison: for each provider, a dashed-outline "reported" bar overlaid with a filled "real workload" bar.

- **Title (bold 15px `#1a5276`, top center):** "All report \"85% CPU\" — but mean different things".
- **Bars (100px wide, 80px gap, baseline y=250, max height 180 = 100%):**
  - "cloud provider" — reported 85%, real 70%, color `#ff9900`, note "Includes steal time"
  - "cloud platform A" — reported 85%, real 85%, color `#0078d4`, note "Guest-only metric"
  - "cloud platform B" — reported 85%, real 60%, color `#4285f4`, note "Normalized to baseline"
- **Styles:** reported = dashed (6/3) 3px outline rectangle in provider color; real = 50% alpha fill of provider color with solid 2px border. Labels: provider name bold below in provider color; "Reports: 85%" in `#333` above the outline; "Real: N%" bold red `#e74c3c` inside the filled bar; note in `#666` 11px below the name.
- **Legend (right):** dashed outline swatch "Reported"; gray 50%-alpha filled swatch "Actual work".

## Auto-Scaling Feedback Loops

**Obj-title:** Scale policies create oscillating instability

- High load → scale out → more instances → monitoring shows "healthy"
- Scale policy says "reduce" → instances killed → load spikes again
- Cycle repeats: sawtooth pattern in instance count
- Cooldown periods help but don't eliminate the fundamental feedback loop

**Impact:** Instance counts oscillate instead of stabilizing. Cost is higher than steady-state because of constant provisioning/deprovisioning. Metrics show "healthy averages" that hide the instability. Time-series analysis on auto-scaled services confounds load with capacity.

### Visualization (canvas `c6`, 720×300)

Sawtooth line chart of instance count oscillating over time against an ideal steady-state line.

- **Title (bold 15px `#1a5276`, top center):** "Auto-Scaling Oscillation (Instance Count Over Time)".
- **Axes:** origin (80,240), width 580, height 180, stroked `#333`; y ticks 0, 5, 10; x label "Time →"; rotated y label "Instances".
- **Sawtooth (red `#e74c3c`, width 2.5):** 5 cycles of 20 points each; within each cycle instances ramp linearly 3→10 over the first 60%, hold a brief plateau at 10 until 70%, then drop sharply 10→3 over the last 30%.
- **Steady-state line:** dashed green (`#2ecc71`, dash 8/4, width 2) horizontal line at 6 instances, labeled "Ideal steady state" in green to the right.
- **Phase annotations (red 11px above the plot):** "↑ scale out" and "↓ scale in" over the first cycle.
- **Caption (bold red 13px, bottom center):** "Feedback loop: never reaches steady state".

## Regeneration instructions

- **Layout:** domains detail-page template: h1, `.subtitle`, one `.philosophy` callout, then per pitfall an unnumbered `<h2>` (with an id slug; 1.4em `#1a5276`, 2px solid `#2980b9` bottom border) followed by a `.obj-table` with one `<tr>`: left `<td>` (45%) holding `.obj-title` + `<ul>` of bullets + an **Impact:** paragraph, right `<td>` (55%, centered) holding the canvas. Even rows background `#fafcfe`. No nav, no cross-page links.
- **Page CSS:** body system sans-serif, white background, text `#2a2a2a`, padding 40px 20px, line-height 1.6; h1 1.8em `#1a5276`; `.subtitle` `#666` 1.05em; `ul` 0.9em `#333`; `strong` `#1a5276`; `.philosophy` background `#f0f4f8`, left border `4px solid #2980b9`, padding 12px 16px, 0.9em; table cell borders `1px solid #e0e0e0`, padding 20px 24px; `.obj-title` 1.05em weight 600 `#1a5276`.
- **Canvas:** each canvas declares intrinsic `width="720" height="300"`; a shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Charts drawn immediately in IIFEs. Base chart font 17px system sans-serif. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, mid blue `#2980b9`/`#3498db`, green `#27ae60`/`#2ecc71`, red `#e74c3c`, orange `#f39c12`/`#ff9900`, purple `#9b59b6`, provider blues `#0078d4`/`#4285f4`, grays `#333`/`#666`/`#888`/`#95a5a6`.
- Card links elsewhere pointing to this page use the `.html` extension in regenerated HTML.
