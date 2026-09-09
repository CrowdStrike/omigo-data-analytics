# Self-Driving & Fleet Learning — Domain-Specific Pitfalls

**Page type:** detail page (one h2 per pitfall, each followed by a two-column obj-table row: labeled bullets left ~40%, canvas right ~60%)
**HTML title tag:** Self-Driving & Fleet Learning — Domain-Specific Pitfalls

## Long-Tail Scenario Collection

**Obj-title:** Long-Tail Scenario Collection

- **The split:** 99.99% of miles driven are routine; the remaining 0.01% are the real edge cases.
- **What the tail costs:** Those rare miles are the ones that cause fatalities and system failures.
- **The math:** A fleet may need 100 million miles just to encounter one specific rare scenario.
- **What rare looks like:** A mattress falling off a truck; a child chasing a ball in rain at dusk.
- **Drowning in normalcy:** Collecting more routine miles does not help — you starve for the tail.
- **Imperfect fixes:** Targeted scenario mining and synthetic data are necessary, but add distributional biases.

### Visualization (canvas `canvas1`, 720×200)

Long-tail frequency distribution with the critical tail region highlighted.

- **Axes:** `#2c3e50` 1.5px L-shape from (60,20) to (60,165) to (690,165). **Title (17px `#1a5276`, centered at y=18):** "Long-Tail Scenario Distribution". Axis labels (13px `#555`): "Scenario Rarity →" below at (380,185); "Frequency" rotated -90° at (15,100).
- **Curve:** steep power-law decay from (65,30) to (680,~160): y = 30 + 130·(1 − (1−t)^0.08) over the x range — an extremely sharp drop then a long flat tail. Stroke `#2980b9` 2.5px; area under curve filled with a horizontal gradient of `rgba(41,128,185,α)` fading from α=0.6 at left through 0.3 (15%), 0.1 (50%) to 0.05 at right.
- **Tail highlight:** region from x=520 to x=680 under the curve filled `rgba(231,76,60,0.45)`, separated by a dashed (4/4) vertical `#c0392b` 1.5px line at x=520.
- **Annotations (bold 13px, centered):** in `#c0392b` at (600,140)/(600,155): "0.01% — Fatal Edge Cases" / "(100M+ miles to encounter)"; in `#2980b9` at (160,60): "99.99% Routine".

## Fleet Disagreement as Signal

**Obj-title:** Fleet Disagreement as Signal

- **The outlier:** 999 of 1,000 cars proceed through an intersection and 1 hesitates, brakes, or alerts.
- **Why it matters:** That single outlier is a powerful signal, not a glitch in one vehicle's stack.
- **Hidden causes:** A partially obscured stop sign, or unusual pedestrian behavior at that corner.
- **Time-specific causes:** A sun-glare condition that only appears at one hour of one season.
- **Voting fails:** Majority voting dismisses the minority report as noise and discards it.
- **Safety inverts it:** In safety-critical systems that minority is often the most important data point.
- **The method:** Fleet disagreement mining detects vehicles diverging at the same location.
- **What it finds:** Latent hazards that standard fleet-wide safety metrics miss entirely.

### Visualization (canvas `canvas2`, 720×200)

Schematic intersection diagram: a stream of proceeding vehicles plus one highlighted outlier that stops.

- **Title (17px `#1a5276`, centered at y=18):** "Fleet Behavior at Intersection — 1000 Vehicles".
- **Intersection:** gray `#ddd` cross — vertical road rect (300,50,120×130) and horizontal road rect (200,90,320×50), with dashed yellow `#f39c12` center-line markings (dash 8/6).
- **Proceeding traffic:** ~30 small green arrow glyphs (stroke `rgba(39,174,96,0.6)`, 2px) approaching from the left around y≈105 with slight sinusoidal jitter, and ~25 more (alpha 0.5) beyond the intersection from x=440 rightward. Bold 13px green label "999 proceed normally →" at (440,80).
- **Outlier:** red `#e74c3c` car rectangle (30×16) stopped at (258,100) before the intersection, white "!" glyph inside, red brake-line strokes behind it, encircled by a dashed (3/3) red 2.5px circle of radius 24.
- **Outlier labels (centered at x=273):** bold 13px `#c0392b` "1 hesitates — SIGNAL!" at y=148; 11px `#7f8c8d` "(Something unusual here)" at y=164.
- **Legend (13px, right-aligned at x=700):** "● Normal pass-through" in green (y=40); "● Outlier hesitation = hazard signal" in red (y=58).

## HD Map Staleness

**Obj-title:** HD Map Staleness

- **Centimeter fiction:** HD maps encode lanes, signs, and signals to centimeter accuracy.
- **Reality moves:** Construction zones appear overnight and lane markings shift by whole meters.
- **Stale ground truth:** The stack trusts the map for localization, planning, and perception validation.
- **The failure mode:** So the car may confidently drive where a lane used to be, not where it is.
- **Continuous decay:** Map freshness degrades continuously, not in discrete, detectable jumps.
- **No trust signal:** That makes it hard to know the moment when trust should be withdrawn.
- **Mismatch:** The weeks-to-months update cycle is far slower than how fast the world changes.

### Visualization (canvas `canvas3`, 720×200)

Line chart of map-vs-reality divergence growing over time past a safety threshold.

- **Title (17px `#1a5276`, centered at y=18):** "HD Map Divergence from Reality Over Time".
- **Axes:** `#2c3e50` 1.5px from (70,30) to (70,160) to (680,160). Y label (13px `#555`, rotated): "Divergence (m)". X labels (11px `#555`): `Day 1, Week 1, Week 2, Week 3, Week 4, Week 6, Week 8` at x `[90, 180, 280, 370, 460, 560, 650]`.
- **Map belief line:** blue `#2980b9` 2px dashed (6/4) flat horizontal at y=145 (map never updates).
- **Reality line:** red `#e74c3c` 2.5px through points y = `[145, 140, 120, 105, 70, 55, 42]` at the seven x positions, with 4px red dots at each point; the wedge between reality and the map line filled `rgba(231,76,60,0.15)`.
- **Safety threshold:** dashed (3/3) yellow-orange `#f39c12` 1.5px horizontal line at y=90, labeled in bold 11px `#f39c12`: "⚠ Safety threshold (1m)".
- **Event marker:** bold 11px purple `#8e44ad` text "🚧 Construction starts" at (230,108).
- **Legend (13px, upper right):** dashed blue swatch "Map belief"; solid red swatch "Reality".

## Disengagement Report Bias

**Obj-title:** Disengagement Report Bias

- **Incomparable definitions:** Some companies count only unplanned safety-critical takeovers.
- **Others count more:** Others fold in planned stops, software resets, or comfort interventions.
- **Apples to oranges:** 0.1 disengagements/1000mi on easy suburban roads reads as best-in-class.
- **Same metric, harder road:** 2.0 on dense urban streets measures a different thing entirely.
- **PR metric:** It conflates operational domain difficulty, reporting standards, and real capability.
- **Rankings are void:** Leaderboards built on it are statistically meaningless, not merely noisy.

### Visualization (canvas `canvas4`, 720×200)

Bar chart of disengagement rates across five companies, each footnoted with its incompatible definition.

- **Title (17px `#1a5276`, centered at y=18):** "Disengagements per 1000 Miles — Incomparable Definitions".
- **Axes:** `#2c3e50` 1px from (70,35) to (70,155) to (690,155).
- **Bars:** width 80px, gap 50px, starting x=95, baseline y=155, max height 100px scaled to 3.5:
  - Company A: 0.09, green `#27ae60`, note "Suburban only, excludes planned stops".
  - Company B: 1.8, red `#e74c3c`, note "Urban dense, all interventions".
  - Company C: 0.5, yellow-orange `#f39c12`, note "Highway only, safety-critical only".
  - Company D: 3.2, purple `#8e44ad`, note "Mixed, includes comfort stops".
  - Company E: 0.3, blue `#2980b9`, note "Geo-fenced, critical only".
- **Labels:** bold 13px `#2c3e50` value above each bar; 11px `#555` company name below baseline; bold red `#c0392b` asterisk `*` beside each bar top; two-line 9px `#7f8c8d` definition note under each company name.
- **Warning note (bold 11px `#c0392b`, centered at y=195):** "* Different definitions make cross-company comparison meaningless".

## Shadow Mode vs Real Deployment Gap

**Obj-title:** Shadow Mode vs Real Deployment Gap

- **Open loop:** In shadow mode the system says "I would have braked here" and nothing follows.
- **No consequences:** It never experiences the outcome of its own actions, only of the human's.
- **Counterfactual gap:** The scenario that unfolds is always the one where the human drove.
- **Never observed:** So the system never sees what happens after its hypothetical braking.
- **Closed-loop surprise:** Once deployed, its actions change world state — drivers and pedestrians react.
- **New dynamics:** That reactive world produces interactions shadow mode never encountered.
- **Miscalibrated:** Shadow performance can dramatically over- or underestimate real-world results.

### Visualization (canvas `canvas5`, 720×200)

Diverging-path timeline: the shadow-mode hypothetical action vs the human's actual action.

- **Title (17px `#1a5276`, centered at y=18):** "Shadow Mode Prediction vs Actual Human Action".
- **Timeline:** gray `#bdc3c7` 2px horizontal line at y=100 from x=60 to x=690, with tick marks and 11px `#7f8c8d` labels `t=0` (x=120), `t=1s` (x=260), `t=2s` (x=400), `t=3s` (x=540).
- **Shadow path (upper):** red `#e74c3c` 2.5px dashed (6/4) curve from (120,75) rising via quadratic curves to a flat y=45 by x=400, ending at x=540 in a red car rectangle labeled "STOP" (white 9px text).
- **Human path (lower):** green `#27ae60` 2.5px solid line from (120,75) staying near y≈78-82 through to (650,78), ending in a green car rectangle labeled "GO".
- **Decision point:** orange-filled `#f39c12` circle (radius 6, stroke `#e67e22` 2px) at (120,75), labeled in bold 11px `#e67e22` "Decision point" at (120,130).
- **Path labels (bold 12px):** "Shadow: \"I would brake here\"" in red at (130,40); "Human: drives through safely" in green at (130,150).
- **Gap annotations:** bold 11px purple `#8e44ad` "↕ Counterfactual gap" at (470,68); 10px `#7f8c8d` "Model never sees outcome of its own action" at (470,135).

## Edge Case Labeling Ambiguity

**Obj-title:** Edge Case Labeling Ambiguity

- **The scenario:** A phone-distracted jaywalker 30 meters ahead, walking at a slow pace.
- **Four answers:** Five trained annotators give four labels: stop, slow down, change lanes, proceed.
- **No schema captures it:** The "correct" action depends on speed, weather, and traffic density.
- **Context keeps growing:** It also depends on local driving norms and pedestrian body language.
- **Averaged harm:** Training on ambiguous labels yields a model that learns an average action.
- **Worse than any human:** That compromise may be worse than any individual annotator's judgment.
- **Not a QC problem:** Annotator disagreement on safety-critical scenes is genuine behavioral ambiguity.
- **Unfixable by process:** The labeling framework itself cannot resolve it with better guidelines.

### Visualization (canvas `canvas6`, 720×200)

Annotator-disagreement panel: one scenario box, five annotator verdict boxes, and a label-distribution bar.

- **Title (17px `#1a5276`, centered at y=18):** "Annotator Disagreement — Same Jaywalking Scenario".
- **Scenario box:** light gray `#ecf0f1` rect (30,35,160×70, border `#bdc3c7`), containing centered text: bold 11px "SCENARIO:", then 10px lines "Pedestrian jaywalking", "looking at phone", "30m ahead, slow pace"; pedestrian emoji glyphs "🚶📱" (20px) below at (110,130).
- **Annotator columns:** five 95px columns starting x=230, each with a bold 10px `#555` header, a colored action box (white bold 11px text), and a 9px `#7f8c8d` reasoning line:
  - Annotator 1 — STOP, red `#e74c3c`, "Pedestrian unaware".
  - Annotator 2 — SLOW DOWN, yellow-orange `#f39c12`, "Prepare to stop".
  - Annotator 3 — CHANGE LANE, blue `#3498db`, "Avoid entirely".
  - Annotator 4 — SLOW DOWN, yellow-orange `#f39c12`, "Will clear in time".
  - Annotator 5 — PROCEED, green `#27ae60`, "30m = enough space".
- **Disagreement frame:** dashed (4/3) `#c0392b` 2px rectangle around all five columns (225,32,485×65).
- **Stat line (bold 12px `#c0392b`, centered at y=125):** "5 annotators → 4 different labels".
- **Distribution bar:** 400px-wide stacked bar at (160,138), height 18px — Stop 20% red, Slow 40% `#f39c12`, Lane 20% `#3498db`, Go 20% green — with white bold 9px segment labels "Stop", "Slow", "Lane", "Go".
- **Bottom notes (11px `#7f8c8d`, centered):** "Model trained on averaged labels learns a compromise worse than any single judgment" (y=180); "Inter-annotator agreement: 40% — reflects genuine behavioral ambiguity" (y=194).

## Regeneration instructions

- **Layout:** standard domains detail page. h1, then one `h2` per pitfall (six total) followed by a `.obj-table` (full-width, border-collapse) with a single `<tr>`: left `<td>` (40%) holding `.obj-title` div + a `<ul>` of labeled bullets (each `<li>` starting with a bold `<strong>` label), right `<td>` (60%, centered) holding the canvas. No subtitle paragraph on this page. No nav bar, no back/home links, no thead, no badges.
- **Page CSS:** body system sans-serif (-apple-system, BlinkMacSystemFont, 'Segoe UI'), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6. h1 1.8em `#1a5276`. h2 1.4em `#1a5276` with 2px bottom border `#2980b9`, padding-bottom 8px, margin 40px 0 15px. `ul` 0.9em `#333`. `.obj-table td` border `1px solid #e0e0e0`, padding 20px 24px; even rows background `#fafcfe`. `.obj-title` 1.05em weight 600 `#1a5276`. `strong` `#1a5276`. `.subtitle` and `.philosophy` styles defined but unused.
- **Canvases:** each 720×200 with inline `style="width:720px;height:200px"`; shared `setupCanvas(id)` sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`), and calls `ctx.scale` so drawing stays in logical coordinates. Font constants: 17px system titles (`CHART_FONT`), 13px (`SMALL_FONT`), 11px (`TINY_FONT`). The whole script is wrapped in one IIFE.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, yellow-orange `#f39c12`, purple `#8e44ad`, secondary blues `#2980b9`/`#3498db`, dark red `#c0392b`, axis dark `#2c3e50`, muted gray `#7f8c8d`.
- In regenerated HTML, any card links use `.html` extensions (this page has none).
