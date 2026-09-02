# A/B Test as Launch Justification

**Page type:** detail page (two-column obj-table layout: text left ~40%, canvas right ~60%, one row per section)
**HTML title tag:** A/B Test as Launch Justification — Common Bad Practices

**Subtitle:** Process — Every feature must pass an A/B test, not to learn, but to produce a green slide for the launch review.

## Section 1: The Mandatory Test Theater

- **The practice:** Organizations mandate A/B tests for all launches regardless of whether the change is genuinely testable or whether anyone would actually kill the feature on a negative result. The test exists to check a box, not to inform a decision.
- **Zero information value:** If you won't cancel the launch on a negative outcome, the test has zero information value. A test only matters if at least one outcome changes your action. When the decision is already made, the experiment is theater — expensive, slow theater.
- **System gaming:** Engineers learn the game quickly — small holdbacks (1% control), short windows (3 days), cherry-picked metrics. The goal shifts from "learn whether this works" to "produce a positive number I can put on a slide." Review committees rubber-stamp because they too need the launches to ship.
- **False rigor:** The mandate creates a false sense of data-drivenness. "We A/B test everything" sounds rigorous. In practice, the org is launch-driven with a testing veneer. Real data-driven means killing things that don't work — not testing things you'll ship regardless.
- **Untestable changes tested anyway:** Legal compliance changes (can't NOT launch), brand redesigns (will ship regardless of metric movement), infrastructure migrations (outcome predetermined), accessibility fixes (ethical obligation regardless of engagement metrics).

### Visualization (canvas `c1`, 720×400)

Ship/kill dot plot: one year of launch-review experiments placed by measured lift on the x-axis, colored by the ship/kill decision — revealing that virtually everything ships regardless of the result.

- **Title (bold 15px, `#1a5276`, top center):** "A Year of Mandatory A/B Tests: Ship vs. Kill Decisions".
- **X-axis:** measured lift on the primary metric, −6% to +6%, mapped linearly to x = 60..680; solid `#555` 1px axis line at y=316; 5px tick marks every 2% with 11px `#555` labels ("-6%", "-4%", "-2%", "0%", "+2%", "+4%", "+6%"); axis title 12px `#555` centered at y=352: "Measured lift on primary metric (%)".
- **Zero line:** dashed (4/3) `#999` 1px vertical at lift 0 from y=92 down to the axis; 11px `#777` label "no effect" centered above it at y=86.
- **Data (deterministic, hardcoded — 42 experiments, NO Math.random):** lifts array `[-5.2, -4.4, -3.8, -3.3, -2.9, -2.6, -2.3, -2.0, -1.8, -1.5, -1.3, -1.1, -0.9, -0.7, -0.5, -0.3, -0.2, -0.1, 0.0, 0.0, 0.1, 0.1, 0.2, 0.3, 0.4, 0.5, 0.7, 0.9, 1.1, 1.3, 1.5, 1.7, 1.9, 2.2, 2.5, 2.8, 3.1, 3.5, 3.9, 4.3, 4.8, 5.4]`. Killed set: {−5.2, −4.4, −3.3}; the other 39 all ship — including 15 of the 18 negative results.
- **Dot placement (beeswarm, deterministic):** x from the true lift value; y stacks upward from a base of y=300 in 16px steps within each half-percent bin (`bin = Math.round(lift / 0.5)`, stack order = array order); dot radius 6px.
- **Dot colors:** shipped = fill `rgba(39,174,96,0.85)`, stroke `#27ae60` 1px; killed = fill `rgba(231,76,60,0.9)`, stroke `#e74c3c` 1px.
- **Legend (top right, x=590):** green dot + 12px `#333` "Shipped" at y=52; red dot + "Killed" at y=72.
- **Insight annotation (bold 13px `#e74c3c`, centered at w/2, y=60):** "18 experiments came back negative — 15 of them shipped anyway".
- **Caption (italic 12px `#555`, centered at y=380):** "Each dot is one experiment from the launch-review pipeline; color is the ship/kill decision that followed."

## Section 2: Opportunity Cost of Test Capacity

- **Finite capacity:** Experiment capacity is a scarce resource. Concurrent tests interfere with each other, user population is limited, traffic must be split across experiments. Every slot occupied by a theater test is a slot unavailable for genuine learning.
- **Queue congestion:** Filling the experiment queue with mandatory justification tests crowds out genuinely uncertain experiments — the ones where you'd actually learn something, where the outcome could go either way, where the result would change your roadmap.
- **Lost learning:** The org loses the ability to run the tests that matter because the queue is full of predetermined outcomes. Teams with genuinely uncertain hypotheses wait 6+ weeks for capacity while theater tests occupy the pipeline.
- **Correct approach — triage:** Mandatory tests only for genuinely uncertain outcomes where the result changes the decision. For predetermined launches, use observational monitoring instead: before/after comparison with guardrail metrics (latency, error rate, revenue). You still detect regressions — you just don't pretend the launch decision was contingent on the data.
- **The distinction:** "Should we launch X?" requires a test. "X is launching — did it break anything?" requires monitoring. Conflating these wastes the expensive one on tasks suited to the cheap one.

### Visualization (canvas `c2`, 720×400)

Capacity-allocation diagram: two horizontal capacity bars (current vs ideal) plus a monitoring row.

- **Title (bold 15px, `#1a5276`, top center):** "Experiment Capacity: Theater vs. Ideal Allocation".
- **Current state (label, bold 13px `#555`, left):** "CURRENT: Capacity filled with theater".
  - Capacity bar: 640×50 at (40, 65), background `#f5f5f5`, border `#ccc`. First 80% of width filled with 8 equal slot blocks, fill `rgba(231,76,60,0.7)`, stroke `#e74c3c`; each labeled "theater" below in `#e74c3c` 11px. Last 20% has 2 blocks, fill `rgba(39,174,96,0.7)`, stroke `#27ae60`, each labeled "real" in `#27ae60`.
  - Right-aligned bold orange (`#e67e22`) label above the bar: "Queue Wait: 6 weeks".
  - Below: green (`#27ae60`) 12px label "Waiting (genuine):" followed by 5 small dashed-outline boxes (38×16, fill `rgba(39,174,96,0.4)`, stroke `#27ae60` dashed 2/2) representing queued genuine experiments.
- **Ideal state (label at y=195, bold 13px `#555`):** "IDEAL: Theater replaced with monitoring, queue runs genuine experiments".
  - Second capacity bar: same size/background, filled with 10 equal green blocks (fill `rgba(39,174,96,0.7)`, stroke `#27ae60`), each labeled "genuine" below in `#27ae60` 11px.
  - Right-aligned bold green label above the bar: "Queue Wait: <1 week".
- **Monitoring row:** bold 12px `#555` left label "Predetermined launches use monitoring instead:", then 4 boxes (140×55, spaced 160px, fill `rgba(26,82,118,0.1)`, stroke `#1a5276`), each with a two-line bold 11px `#1a5276` title — "Legal Compliance", "Brand Redesign", "Infra Migration", "Exec Feature" — and a `#555` 10px sub-line "guardrail metrics only".
- **Summary (bold 13px `#1a5276`, bottom center):** "Triage: test uncertain outcomes, monitor predetermined ones".

## Regeneration instructions

- **Layout:** standard detail-page `.obj-table`: full-width table, one `<tr>` per section; left `<td>` (40%) holds `.obj-title` + bullets, right `<td>` (60%, centered) holds the canvas. Single table with two rows.
- **Page style:** body system sans-serif, white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.6em `#1a5276`; subtitle `#666` 1.0em; ul 0.9em `#333`; table cell borders `1px solid #e0e0e0`, padding 20px 24px, vertical-align middle; `.obj-title` 1.05em, weight 600, `#1a5276`; `strong` in `#1a5276`. No nav bar, no back/home links.
- **Canvas:** declare intrinsic `width`/`height` attributes (720×400 for both); scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, gray text `#555`/`#333`.
- Note: in regenerated HTML, any card/page links use `.html` extensions.
