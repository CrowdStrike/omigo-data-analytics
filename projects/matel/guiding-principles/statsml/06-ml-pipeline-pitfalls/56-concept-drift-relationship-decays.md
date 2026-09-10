# Pitfall: Concept Drift (Relationship Decays, Inputs Stable)

**Page type:** detail page (card-section layout: h2 per section, two-column table with text left 50% / canvas right 50%)
**HTML title tag:** Concept Drift (Relationship Decays, Inputs Stable)

**Subtitle:** P(y|X) moves while every input-distribution monitor stays green.

## The Problem

**Tags:** `the trap` (red), `concept drift` (blue)

- **The formal split** — covariate shift is P(X) moving, label shift is P(y) moving, concept drift is P(y|X) moving with both marginals stable
- **Inputs unchanged** — every feature histogram still matches training bin for bin, so nothing looks wrong
- **Meaning moved** — the same input value now implies a different outcome, and the model still trusts the old mapping
- **Not label shift** — class proportions are unchanged, so recalibrating scores to a new base rate fixes nothing
- **Not a pipeline break** — no schema, unit, or encoding changed upstream, and the label definition is fixed and versioned
- **Monitors miss it** — a KS test or PSI on every feature can pass while the relationship inverts underneath
- **Caught late** — without freshly matured labels there is no observable signal that the relationship moved

*Example:* Across two years, 12 segments keep identical login and chargeback marginals, yet the relationship flips from r = +0.967 to r = −0.967.

**Impact:** The model keeps scoring confidently on data that looks exactly like training, while the association it encodes has reversed sign.

### Visualization (canvas `c1`, 720×300)

Two scatter panels — the same 12 segments in two periods — with each period's correlation, slope, and marginals computed in JS from the plotted points. Illustrative Example.

- **Data (hardcoded literal arrays, never `Math.random()`):** shared `xs = [1,2,3,4,5,6,7,8,9,10,11,12]` (segment mean new-device logins per month).
  - Period 1 `y1 = [12,10,20,16,28,24,32,36,44,40,52,48]` (chargebacks per 1,000 accounts).
  - Period 2 `y2 = [48,52,40,44,36,32,24,28,16,20,10,12]` — the **same multiset** re-paired, so both y marginals are identical by construction and the X marginal is literally the same array.
- **Title (bold 13px, `#1a5276`, centered, y=20):** "Same X Marginal, Same y Marginal, Opposite Relationship".
- **Two panels,** each 280 wide: panel 1 at x=60, panel 2 at x=390; plot band y=48 (top) to y=196 (baseline); x mapped from 0–13, y mapped from 0–60.
  - Panel headers (bold 11px, y=40): "Period 1: 2023 H1" in `#1a5276`; "Period 2: 2024 H1" in `#e74c3c`.
  - Gray `#bbb` 1px axes; y ticks at 10 / 30 / 50 with 9px `#666` labels left of panel 1 only.
  - Points: radius 4 filled dots — panel 1 `#1a5276`, panel 2 `#e74c3c`.
  - Fit line: 2px least-squares line computed from that panel's own points (`#2980b9` / `#e67e22`).
  - Axis caption (9px `#666`, y=212, centered under each panel): "new-device logins / month".
- **Computed stat line (bold 11px, y=232):** per panel, `"r = " + r.toFixed(3) + "    slope = " + slope.toFixed(2)`. Computed values: panel 1 `r = 0.967, slope = 3.80`; panel 2 `r = -0.967, slope = -3.80`.
- **Computed marginal line (10px `#333`, centered, y=252):** `"X mean 6.50 / 6.50, sd 3.45 / 3.45    y mean 30.17 / 30.17, sd 13.55 / 13.55"` — every figure computed from the arrays at render time (population sd).
- **Bottom annotation (bold 11px `#e74c3c`, centered, y=272):** "Marginals identical to the digit. Only P(y|X) moved — no input monitor can see this.".
- **Footnote (9px `#666`, centered, y=290):** "Illustrative Example".

## Why It Happens

**Tags:** `root cause` (orange), `drift shapes` (blue)

- **Population adapts** — behaviour that once flagged risk becomes ordinary, so the feature stops separating anyone
- **Threshold erosion** — yesterday's extreme value is today's median, and a fixed cut now sits inside the bulk
- **Market context** — a competitor's price move changes what your own unchanged price means to a buyer
- **Users learn the system** — people optimise against the score, so the signal decays precisely because it is used
- **Abrupt drift** — a policy change flips the relationship overnight, which needs fast detection and an immediate retrain
- **Gradual and incremental drift** — an old concept fades, or creeps in small steps; use a rolling window and cadence retraining
- **Recurring drift** — seasonal concepts return on a calendar, so keep per-season models instead of retraining each turn
- **Label lag** — the model can be wrong for a full labelling cycle before the first matured label reveals anything

*Example:* With a 30-day chargeback maturation window, a relationship that inverts on day 1 is invisible until day 31 — a full labelling cycle of wrong scores.

**Root Cause:** Monitoring is aimed at the inputs, so a KS test or PSI on every feature can pass while the feature→target relationship inverts underneath it.

### Visualization (canvas `c2`, 720×300)

Monitoring dashboard: five green input-drift tiles whose PSI is computed in JS from literal bin proportions, above one red outcome tile. Snapshot is week 12 of the `c3` timeline. Illustrative Example.

- **Data (literal bin proportions, each row sums to 1.00):**

  | Feature | Training bins | Live bins | PSI (computed) |
  |---|---|---|---|
  | avg_txn_amount | 0.20, 0.25, 0.25, 0.20, 0.10 | 0.19, 0.26, 0.24, 0.21, 0.10 | 0.0018 |
  | session_count | 0.30, 0.25, 0.20, 0.15, 0.10 | 0.31, 0.24, 0.21, 0.14, 0.10 | 0.0019 |
  | account_age | 0.10, 0.20, 0.30, 0.25, 0.15 | 0.11, 0.19, 0.29, 0.26, 0.15 | 0.0022 |
  | device_count | 0.45, 0.30, 0.15, 0.07, 0.03 | 0.44, 0.31, 0.15, 0.07, 0.03 | 0.0006 |
  | days_since_last | 0.25, 0.25, 0.20, 0.20, 0.10 | 0.24, 0.26, 0.20, 0.19, 0.11 | 0.0023 |

  PSI is computed at render time as `Σ (live − train) · ln(live / train)` and printed with `toFixed(4)`; the alarm threshold drawn on the tiles is 0.10. Largest PSI is `days_since_last` at 0.002266.
- **Title (bold 13px, `#1a5276`, centered, y=20):** "Every Input Monitor Green, One Outcome Monitor Red".
- **Input tile row:** five tiles 125×78 at y=40, starting x=18, gap 8 (span 18→675). Each: white fill, 2px `#27ae60` border; bold 10px `#1a5276` feature name; 11px `#333` `"PSI " + psi.toFixed(4)`; bold 10px `#27ae60` "OK  (< 0.10)".
- **Outcome tile:** 640×90 at (40, 140), white fill, 3px `#e74c3c` border; bold 12px `#e74c3c` heading "MODEL OUTCOME — fresh labels"; 12px `#333` line `"precision@100 flagged: " + (71/100).toFixed(2) + " → " + (44/100).toFixed(2)`; bold 11px `#e74c3c` line `"relative drop " + (((71-44)/71)*100).toFixed(1) + "%"` (computed = 38.0%); 10px `#666` "71 of 100 flagged were true positives, now 44 of 100".
- **Bottom lines (centered):** bold 11px `#e67e22` at y=258 — `"Max input PSI = " + maxPsi.toFixed(4) + " — " + (0.10/maxPsi).toFixed(0) + "x below the 0.10 alarm"` (computed: 0.0023 and 44x); 9px `#666` at y=282 — "Illustrative Example — inputs are stable; what they mean for the outcome is not.".

## The Correct Approach

**Tags:** `the fix` (green), `relationship monitoring` (blue)

- **Watch the relationship** — monitor the feature→target association on fresh labels, not feature histograms
- **Rolling performance** — recompute precision and calibration every cycle as newly labelled data matures
- **Perpetual canary** — hold out the most recent time slice permanently and never let training touch it
- **Calibration as early warning** — calibration error drifts before precision does, so alert on it first
- **Naive baseline** — race the model against a simple always-retrained model; losing means the concept moved
- **Drift-matched cadence** — set retrain frequency from the measured drift rate, not from a habitual monthly job
- **Champion/challenger** — keep a fresher challenger scoring in shadow so promotion is a decision, not a project
- **Seasonal models** — for recurring concepts, switch to the matching season's model instead of retraining

*Example:* Calibration error crosses 0.05 in week 6 while precision@100 only breaches 0.60 in week 8, buying two weeks of warning.

**Fix:** Monitor P(y|X) directly — rolling performance and calibration on freshly labelled data, with a challenger and a retrain trigger tied to the observed drift rate.

### Visualization (canvas `c3`, 720×300)

Three stacked monitoring lanes over 12 weeks: input PSI (never fires), calibration error (fires first), precision on fresh labels (fires late). All series are hardcoded literal arrays; both breach weeks and the lead time are computed in JS. Illustrative Example.

- **Data (weeks 1–12):**
  - `psiMax = [0.0018,0.0021,0.0019,0.0024,0.0022,0.0020,0.0023,0.0025,0.0021,0.0019,0.0022,0.0023]`, alarm 0.10 — never breached; week 12 value 0.0023 matches the max PSI computed in `c2` (0.002266 → 0.0023).
  - `ece = [0.021,0.023,0.022,0.028,0.041,0.052,0.062,0.071,0.078,0.083,0.086,0.088]`, alarm 0.05 — first breach week 6.
  - `precision = [0.71,0.70,0.72,0.69,0.68,0.65,0.61,0.57,0.52,0.48,0.46,0.44]`, alarm 0.60 — first breach week 8. Endpoints 0.71 and 0.44 are the two values shown in `c2`.
- **Title (bold 13px, `#1a5276`, centered, y=20):** "Three Monitors, One Drift: Which One Fires?".
- **Lanes:** plot x from 150 to 670; three bands of height 40 with tops at y=38, 96, 154 (baseline = top + 40). Left labels (bold 10px, right-aligned at x=140): "input PSI" `#27ae60`, "calibration (ECE)" `#e67e22`, "precision@100" `#e74c3c`, each with a 9px `#666` second line giving its alarm level.
- **Series:** 2.5px polyline per lane in its lane colour; each lane scales to its own min/max with 4px padding; dashed (4,3) 1.5px `#999` alarm line drawn only where the alarm level falls inside the lane's range (lanes 2 and 3).
- **Breach markers:** filled 5px dot plus a bold 9px label at the first breach point — lane 2 `"fires W" + eceWeek` (6), lane 3 `"fires W" + precWeek` (8), both found by scanning the arrays.
- **Lane 1 annotation (bold 9px `#27ae60`, right of the line):** `"never fires (max " + maxPsi.toFixed(4) + ")"` — computed, 0.0025.
- **Week axis (9px `#666`, y=212):** ticks "W1" … "W12" at the plotted x positions.
- **Bottom annotation (bold 11px `#1a5276`, centered, y=240):** `"Calibration warns " + (precWeek - eceWeek) + " weeks before precision breaches"` — computed, 2 weeks.
- **Footnote (9px `#666`, centered, y=266):** "Illustrative Example — input PSI never leaves the green band across all 12 weeks.".

## Regeneration instructions

- **Layout:** `.card-section` per section: `<h2>` with 2px `#2980b9` bottom border, then `table.layout` (border-collapse, full width) with one `<tr>`: `td.text-col` (50%) holding `.tags` pills + `<ul>` bullets + `.example` italic paragraph + `.key-point` callout; `td.viz-col` (50%) holding the canvas. Shrink a chart via canvas `max-width`/`max-height`, never by narrowing the column.
- **Page style:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem; h2 1.3rem `#1a5276`. No nav bar, no back/home links, no cross-page links.
- **Tag pills:** `.tag` inline-block 0.72rem bold, padding 2px 10px, radius 10px; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Key-point callout:** background `#f8f9fa`, left border 3px solid `#e74c3c`, padding 8px 12px, 0.9rem. `.example` italic `#555` 0.9rem. `li b` colored `#1a5276`.
- **Canvas:** intrinsic 720×300, CSS `width: 100%`, 1px `#e0e0e0` border, 4px radius; scaled by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Data rule:** all chart data is hardcoded literal arrays — no `Math.random()`, no seeded generator needed. Every statistic printed beside plotted data (correlations, slopes, means, sds, PSI, breach weeks, lead time, relative drop) is computed in JS from those exact arrays at render time.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, accent `#2980b9`, bar fill `rgba(26,82,118,0.35)`.
- In regenerated HTML, any card links use `.html` extensions.
