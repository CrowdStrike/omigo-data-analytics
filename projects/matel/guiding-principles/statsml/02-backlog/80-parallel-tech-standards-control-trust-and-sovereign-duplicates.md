# Parallel Tech Standards — Control, Trust, and Sovereign Duplicates

**Page type:** detail page (backlog-style two-column layout: text left 50%, canvas right 50%, one `.lang-section` per topic; h1 carries a BACKLOG status pill)
**HTML title tag:** Parallel Tech Standards — Control, Trust, and Sovereign Duplicates

**Subtitle:** A useful technology held by one operator gets rebuilt by everyone who cannot afford to lose it.

**Intro callout:** Satellite positioning, interbank messaging, and card rails all acquired parallel systems. The driver is control and trust, not quality: the duplicate may end up better, worse, or equivalent, and it gets built either way.

## 1. The Chokepoint Structure

The same structure produces duplication every time: one operator, global reach, and no substitute.

- **The shape** — one operator, global reach, cheap to use, and catastrophic to lose access to.
- **Cost is not the brake** — the decision tracks the loss from withdrawal, not the build price.
- **Withdrawal need not happen** — the ability to withhold is itself enough to justify the spend.
- **Low-loss chokepoints survive** — screw threads and container sizes stay single because losing them is cheap.
- **Duplication is insurance** — what is bought is an option on continued access, paid as capital expenditure.
- **The trigger is a demonstration** — one visible degradation or exclusion converts discussion into budget.

**Key point:** Duplication follows the cost of losing access, not the cost of rebuilding.

### Visualization (canvas `c1`, 720×360)

Quadrant scatter: cost to build an alternative (x) against loss if access is withdrawn (y).

- **Title (bold 16px, `#1a5276`, top center):** "What Actually Triggers a Parallel Build".
- **Subtitle (12px italic `#7f8c8d`, centered under title):** "Illustrative Example — positions are structural, not measured".
- **Plot area:** x=76, y=64, width = canvas−150, height = canvas−128; L-shaped axes `#95a5a6` (1.4px).
- **Scales:** both axes 0–100, no numeric ticks; axis labels "Cost to build an alternative →" (13px `#4a5866`, centered below) and "Loss if access is withdrawn →" (13px `#4a5866`, rotated −90°, centered on the y axis).
- **Threshold band:** fill `rgba(231,76,60,0.07)` across the full plot width from y=70 to y=100; dashed red `#e74c3c` line (dash 5/4, 1.6px) at y=70; label "duplicated regardless of build cost" (13px `#e74c3c`, left-aligned just above the dashed line at x=6 inside the plot).
- **Points (radius 6, fill `rgba(26,82,118,0.35)`, stroke `#1a5276` 1.6px) with labels (12px `#2c3e50`) placed 10px right of the dot, or left-aligned inside if it would overflow:**
  - satellite positioning: (92, 95)
  - interbank messaging: (55, 90)
  - card rails: (62, 80)
  - naming and addressing roots: (40, 88)
  - public time distribution: (15, 72)
  - container dimensions: (22, 26)
  - fastener thread standards: (10, 15)
- **Point color rule:** dots above y=70 stroke `#e74c3c` with fill `rgba(231,76,60,0.30)`; dots below stroke `#27ae60` with fill `rgba(39,174,96,0.28)`.
- **Annotation (13px `#27ae60`, left-aligned near (26, 34)):** "cheap to lose — never duplicated".

## 2. Quality Is Not the Driver

Five satellite navigation systems reached service; their capability ranks are mixed, their existence is not.

- **Five systems in service** — GPS, GLONASS, Galileo, BeiDou and NavIC all reached operational status.
- **Ranks are mixed** — each leads somewhere: signal design, global coverage, revisit rate, regional accuracy.
- **Existence is invariant** — capability spans a wide band, yet every one of the five still got built.
- **Same story in payments** — national card and messaging rails appeared beside incumbents of every quality.
- **The inference to avoid** — a duplicate's existence says nothing about the original's technical quality.
- **Newer builds can leapfrog** — a later start uses modern signal design and carries no legacy compatibility debt.

**Key point:** Capability differences explain adoption share, not why the duplicate exists.

### Visualization (canvas `c2`, 720×360)

Diverging bars around a zero baseline (relative capability) plus a constant right-hand column showing all systems exist.

- **Title (bold 16px, `#1a5276`, top center):** "Capability Varies, Existence Does Not".
- **Subtitle (12px italic `#7f8c8d`, centered under title):** "Illustrative Example — relative capability index, not a measurement".
- **Layout:** left panel occupies x=150 to x=canvas−210 for the diverging bars; right panel is a 150px column starting at canvas−190.
- **Data (system, relative capability index in −30…+30):** GPS `+8`, GLONASS `−14`, Galileo `+16`, BeiDou `+11`, NavIC `−22`.
- **Baseline:** vertical `#95a5a6` line (1.4px) at the midpoint of the left panel, label "peer average" (12px `#5a6875`, centered above the line).
- **Bars:** one row per system, row height = panel height / 5, bar thickness 0.42·row height; positive bars extend right with fill `rgba(39,174,96,0.45)` stroke `#27ae60` 1.4px; negative bars extend left with fill `rgba(231,76,60,0.45)` stroke `#e74c3c` 1.4px.
- **Row labels (13px `#2c3e50`, right-aligned at x=138):** system names.
- **Value labels (12px, matching bar stroke color):** signed index printed 6px beyond each bar's free end.
- **Right column:** header "built anyway" (13px bold `#1a5276`, centered); one filled `#1a5276` circle (radius 7) per row at the column centre, all at identical x — the visual point is that this column is constant while the bars are not.
- **Footer note (12px `#5a6875`, centered below the plot):** "capability spread: 38 index points — existence spread: none".
- **Reconciliation:** the footer spread is computed at render time as `max − min` of the data array (`+16 − (−22) = 38`), never hardcoded.

## 3. What Duplication Costs

The cost of parallel standards does not land on the operators; it lands on every device at the edge.

- **Cost lands at the edge** — the receiver, terminal, or client absorbs every additional standard.
- **Combinations, not additions** — n systems create 2ⁿ − 1 non-empty subsets a device may face in the field.
- **The matrix explodes** — one system gives 1 case to validate; five systems give 31 of them.
- **Silicon and power** — more correlator channels, more bands, larger firmware, shorter battery life.
- **Governance splits too** — separate time references, coordinate frames, and update cadences to reconcile.
- **Published interfaces cap it** — open signal specs let one device serve all systems instead of one each.

**Key point:** Fragmentation cost is combinatorial at the edge even when each system is sound on its own.

### Visualization (canvas `c3`, 720×360)

Bar chart with log-ish growth: validation cases a multi-system device faces as systems are added.

- **Title (bold 16px, `#1a5276`, top center):** "Every Added System Multiplies the Test Matrix".
- **Plot area:** x=76, y=64, width = canvas−150, height = canvas−128; L-shaped axes `#95a5a6` (1.4px).
- **Data (computed at render time, never hardcoded):** for k = 1…5 systems, cases = `Math.pow(2, k) - 1` → 1, 3, 7, 15, 31.
- **Scales:** y from 0 to 32 (linear), tick labels at 0, 8, 16, 24, 32 (12px `#5a6875`, right-aligned); x is 5 slots, labels "1", "2", "3", "4", "5" (13px `#4a5866`), axis label "Systems a device must support" centered below.
- **Bars:** slot width = plotW / 5, bar width 0.46·slot; fill `rgba(26,82,118,0.35)`, stroke `#1a5276` 1.4px.
- **Value labels (12px bold `#1a5276`, centered 8px above each bar):** the computed case count.
- **Reference curve:** dashed orange `#e67e22` line (dash 4/4, 2px) through the bar tops to show the doubling shape.
- **Annotation (13px `#e67e22`, right-aligned above the 5-system bar):** "2ⁿ − 1 combinations".
- **Footer note (12px `#5a6875`, centered below the axis label):** "Cases counted as non-empty subsets of systems a receiver may need to validate."

## 4. The Redundancy Dividend

The non-obvious twist: receivers that track several systems made the duplication a net technical win.

- **The twist** — multi-system receivers turned duplication built for control into an accuracy improvement.
- **Geometry, not politics** — more usable satellites in view shrink positioning error roughly as 1/√N.
- **Illustrative arithmetic** — at 8 usable satellites per system the error index falls from 1.00 to 0.50.
- **Urban canyons gain most** — a blocked sky view needs spare satellites far more than an open field does.
- **Availability jumps first** — the chance of holding a 4-satellite fix rises from 63.7% to 98.9% at two systems.
- **Resilience is a separate gain** — one system degraded or offline no longer stops the receiver working.

**Key point:** Duplication chosen for control produced a technical dividend nobody designed for.

### Visualization (canvas `c4`, 720×360)

Dual-axis chart: relative error index (bars, left axis) and fix availability (line, right axis) against number of systems used.

- **Title (bold 16px, `#1a5276`, top center):** "More Systems, Less Error — the Unintended Dividend".
- **Subtitle (12px italic `#7f8c8d`, centered under title):** "Illustrative Example — 8 usable satellites per system, each visible with probability 0.5".
- **Plot area:** x=76, y=68, width = canvas−160, height = canvas−140; L-shaped axes `#95a5a6` (1.4px).
- **Bars (left axis, error index 0 to 1.1):** for k = 1…4, index = `1 / Math.sqrt(k)` computed at render time → 1.00, 0.71, 0.58, 0.50; bar width 0.40·slot, fill `rgba(26,82,118,0.35)`, stroke `#1a5276` 1.4px; value printed to 2 decimals (12px bold `#1a5276`) 8px above each bar.
- **Left tick labels (12px `#5a6875`, right-aligned):** 0.00, 0.25, 0.50, 0.75, 1.00.
- **Line (right axis, availability 0–100%):** P(at least 4 of n visible) with n = 8k and p = 0.5, computed at render time from the binomial tail — 63.7, 98.9, 100.0, 100.0 (exact values 163/256 = 63.672%, 1 − 697/65536 = 98.936%, 1 − 2325/2²⁴ = 99.986%, 1 − 5489/2³² = 99.99987%; the last two round to 100.0 at one decimal).
- **Line style:** stroke `#27ae60` 3px, filled `#27ae60` circles radius 5 at each k; percentage printed to 1 decimal (12px `#27ae60`) 12px below each point.
- **Right tick labels (12px `#27ae60`, left-aligned outside the plot):** 0%, 25%, 50%, 75%, 100%.
- **X labels (13px `#4a5866`):** "1", "2", "3", "4"; axis label "Systems the receiver tracks" centered below.
- **Legend (top-left inside plot, 13px `#2c3e50`):** blue swatch + "relative error index", green swatch + "4-satellite fix availability".
- **Annotation (13px `#e67e22`, left-aligned above the k=2 point):** "biggest jump is the second system".
- **Reconciliation:** every printed number in this chart is derived in JS from `k`; the prose figures 1.00, 0.50, 63.7% and 98.9% match those derivations.

## Regeneration instructions

- **Layout:** backlog detail page. `h1` (2rem `#1a5276`, bottom border `2px solid #2980b9`) with inline `.status` pill "BACKLOG" (background `#fef9e7`, border `1px solid #f39c12`, text `#b7950b`, 4px radius, 0.8rem); `.subtitle` (`#666`, 0.95rem); `.intro` callout (background `#f0f4f8`, left border `3px solid #2980b9`, 8px 12px padding, 0.9rem). One `.lang-section` per numbered h2 (1.3rem `#1a5276`, bottom border `2px solid #2980b9`); inside each, `table.layout` with `td.text-col` 50% and `td.viz-col` 50%, both `vertical-align: top`, 12px padding. No index number in the `h1` or `<title>`.
- **Text blocks:** intro `<p>`, `<ul>` bullets (0.92rem) with `<strong>` lead-ins, `.key-point` callout (background `#f8f9fa`, left border `3px solid #e74c3c`, 0.9rem).
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6. Canvases `width: 100%`, `1px solid #e0e0e0` border, 4px radius.
- **Palette:** `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange, `rgba(26,82,118,0.35)` bar fill; gray labels `#5a6875`/`#4a5866`, axes `#95a5a6`.
- **Canvas:** intrinsic 720×360; the backing store is sized to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and `ctx.scale`d back to logical coordinates by a shared `setupCanvas(id)` helper; all four charts are registered as draw functions and re-run on `window.resize`.
- **Determinism:** no `Math.random()` anywhere. Chart 1 and chart 2 use fixed literal arrays because the positions and the capability spread carry the lesson; charts 3 and 4 compute every plotted value and every printed statistic from `k` at render time. If a seeded draw is ever added, use the canonical inline `lcg(seed)` Park–Miller generator, one per chart.
- **Neutrality:** describe the structural incentive only. Name systems and rails as public infrastructure facts; attribute no motives to any nation or operator, and label every constructed figure "Illustrative Example".
- In regenerated HTML, any card links use `.html` extensions (this page has none).
