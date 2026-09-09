# Chemical Plants / Process Manufacturing

**Page type:** detail page (one h2 per pitfall, each followed by a two-column obj-table row: text left 50%, canvas right 50%; text is a bold one-line obj-title punchline followed by labeled bullets)
**HTML title tag:** 102. Chemical Plants / Process Manufacturing

**Subtitle:** Repeating a specific recipe with tight metrics at each stage — where ±0.1% deviation = off-spec batch worth $500K.

## Recipe Precision Requirements (±0.1% = Failure)

**±0.2°C Off Target Scraps a 50,000-Liter Batch**

- **The recipe:** Reagent A at 72.3°C ±0.2°C, exactly 45 minutes, pH 6.8 ±0.05.
- **One parameter out:** The whole 50,000-liter batch is off-spec — scrapped or reworked at huge cost.
- **How narrow:** The acceptable range is far tighter than most manufacturing tolerances.
- **The silent killer:** An undetected 0.5°C temperature sensor drift biases every batch.
- **Why it lingers:** Weeks of failures pass before anyone blames the sensor, not the process.

### Visualization (canvas `canvas1`, 720×300 declared; scaleCanvas renders at 720×200)

Control chart: process signal drifting out of a tight tolerance band.

- **Background:** light gray `#f9f9f9`. **Title (17px `#1a5276`, top center):** "Temperature Over Time: Tight Tolerance Band (±0.2°C)".
- **Tolerance band:** rectangle x=50–670, y=80–120 filled `rgba(41,128,185,0.1)`; band borders dashed red (`#e74c3c`, dash 6/4, width 2) at y=80 and y=120; green target line (`#27ae60`, width 1) at y=100.
- **Process signal:** blue line (`#2980b9`, width 2.5) from x=50 to 670: y = 100 + sin(0.15x)·4 + cos(0.07x)·3 + quadratic drift t²·35 (t = fraction across), so the line drifts down past the lower band.
- **Labels:** 13px red "Upper Spec (+0.2°C)" and "Lower Spec (−0.2°C)"; 13px green "Target: 72.3°C" at (580,96); 12px `#c0392b` right-aligned "← Sensor drift causes excursion" at (665,170).

## Multi-Stage Cascading Errors

**Every Stage Passed QC; the Final Product Still Failed**

- **The chain:** Stage 1's output is stage 2's input, so deviation is inherited downstream.
- **Amplification:** An edge-of-spec deviation at stage 1 grows at stage 2, breaks at stage 3.
- **Compounding:** Each stage adds its own variability on top of what it inherited.
- **The blind spot:** "All stages passed QC" hides that each one sat at its boundary.
- **What to change:** Model the trajectory across stages, not per-stage pass/fail flags.

### Visualization (canvas `canvas2`, 720×300 declared; scaleCanvas renders at 720×200)

Bar chart of deviation amplification across stages.

- **Background:** `#f9f9f9`. **Title (17px `#1a5276`):** "Cascading Deviation Amplification Across Stages".
- **Bars:** stages `['Stage 1', 'Stage 2', 'Stage 3', 'Stage 4', 'Final']`, deviation values `[8, 18, 38, 65, 90]`; bar width 80, gap 50 from x=80, height = value×1.4, baseline y=170, outlined `#1a5276`. Color by value: >60 red `#e74c3c`, >30 amber `#f39c12`, else blue `#2980b9`. Value labels above bars formatted "±0.08%", "±0.18%", "±0.38%", "±0.65%", "±0.90%" (12px, in bar color); stage labels below (13px `#333`).
- **Arrows:** gray (`#7f8c8d`, width 1.5) horizontal arrows between consecutive bars at y=130.
- **Failure threshold:** dashed red line (dash 5/3, width 1.5) at value 60 (y=86), right-aligned label "Failure Threshold" (12px red).

## Raw Material Variability (Same Supplier, Different Batch)

**In-Spec Raw Material Still Moves the Optimal Recipe**

- **The spec:** Chemical A ships against a "99.5% minimum" purity guarantee, always met.
- **The reality:** Actual purity wanders between 99.5% and 99.9% from batch to batch.
- **Scale of effect:** That 0.4% is noise for most industries, decisive for a pharmaceutical.
- **What it shifts:** Reaction kinetics, yield, and impurity profile all move with purity.
- **The trap:** Parameters tuned on the last lot are wrong for the next, in-spec lot.

### Visualization (canvas `canvas3`, 720×300 declared; scaleCanvas renders at 720×200)

Bar chart of purity by batch with the optimized-on batch highlighted.

- **Background:** `#f9f9f9`. **Title (17px `#1a5276`):** "Raw Material Purity: Same Supplier, Different Batches".
- **Spec-range background:** rectangle (60,40) 640×130 filled `rgba(39,174,96,0.08)`.
- **Bars:** 12 batches with purity values `[99.52, 99.71, 99.88, 99.55, 99.63, 99.91, 99.50, 99.78, 99.85, 99.60, 99.73, 99.54]`; bar width 40, spacing 52 from x=70, baseline y=170, y-scale 99.4–100.0 over 120px. Color by |value − 99.88|: >0.3 red `#e74c3c`, >0.15 amber `#f39c12`, else blue `#2980b9`. Value labels "99.52%"… above bars (10px `#555`).
- **Highlight:** batch 3 (99.88, index 2) backed by a `rgba(41,128,185,0.15)` column, with 11px blue caption below: "↑ Optimized on this batch".
- **Min spec line:** dashed green (`#27ae60`, dash 4/3, width 2) at 99.5, labeled "Min Spec: 99.5%" (12px green).

## Sensor Fouling & Drift (Gradual Degradation)

**Screen Says pH 6.8; Reality Is 7.1 and Out of Spec**

- **The mechanism:** Two weeks of chemical exposure coats the probe; it reads 0.3 units low.
- **Control makes it worse:** The loop corrects toward the false value and overshoots the other way.
- **What operators see:** An accurate-looking control screen the whole time it is wrong.
- **Where it surfaces:** The product fails QC with no alarm raised anywhere upstream.
- **Time to diagnose:** Three weeks before the drifted sensor was named as root cause.

### Visualization (canvas `canvas4`, 720×300 declared; scaleCanvas renders at 720×200)

Two-line chart: stable sensor reading vs drifting actual pH.

- **Background:** `#f9f9f9`. **Title (17px `#1a5276`):** "Sensor Reading vs Reality Over 3 Weeks".
- **Axes:** gray L-shape (`#666`) from (70,35) down to (70,170) and across to (690,170); week labels "Week 1" (x=170), "Week 2" (380), "Week 3" (590) at y=185 (11px `#666`).
- **Target line:** dashed green (`#27ae60`, dash 3/3) at y=100, labeled "Target: pH 6.8" (12px green).
- **Spec limits:** dashed amber lines (`#f39c12`, dash 5/4, width 1) at y=75 and y=125, labeled "±0.05 spec" (12px amber).
- **Sensor reading:** blue line (`#2980b9`, width 2.5) hugging y=100 with small noise (sin(0.1x)·2 + cos(0.23x)·1.5); label "Sensor Reading (looks fine)" (12px blue) at (80,145).
- **Actual pH:** red line (`#e74c3c`, width 2.5): y = 100 − t²·45 + small noise, drifting up out of spec; label "Actual pH (drifting out of spec)" (12px red) at (80,55).

## Transition States Between Recipes (Unmeasured Danger Zone)

**The 30-Minute Recipe Changeover Is Where Accidents Happen**

- **What's in flux:** Product A residue still in the pipes, temperatures moving, flow ramping.
- **No owner:** Neither recipe's control system models the 30-minute switchover at all.
- **Guaranteed loss:** The transition itself produces waste and off-spec material.
- **The rare hazard:** It occasionally creates chemical combinations nobody anticipated.
- **The data sin:** Transition rows are discarded as "not representative" — the riskiest ones.

### Visualization (canvas `canvas5`, 720×300 declared; scaleCanvas renders at 720×200)

Three-zone process chart with a chaotic transition region.

- **Background:** `#f9f9f9`. **Title (17px `#1a5276`):** "Recipe Transition: Unmeasured Danger Zone".
- **Zones (y=35, h=145):** Product A x=50–280 filled `rgba(41,128,185,0.1)`; transition x=280–440 filled `rgba(231,76,60,0.15)` with diagonal red hatching (`rgba(231,76,60,0.3)`, lines every 12px); Product B x=440–680 filled `rgba(39,174,96,0.1)`.
- **Temperature line:** red (`#e74c3c`, width 2.5): stable ~y=120 in zone A, wild sine excursion (dip of 50px plus sin(0.3x)·8 wobble) in transition, stable ~y=80 in zone B.
- **Flow rate line:** blue (`#2980b9`, width 2): stable ~y=140 in zone A, ramping down with sin(0.2x)·10 wobble in transition, stable ~y=100 in zone B.
- **Zone labels (13px, y=50):** blue "Product A" at x=165; bold red "TRANSITION" / "(No Model)" at x=360; green "Product B" at x=560.
- **Caption (11px `#c0392b`, bottom center):** `"Data discarded as not representative"`.

## Golden Batch Comparison Fallacy

**The Golden Batch May Have Simply Been Lucky**

- **The practice:** Compare each current batch against the best batch in plant history.
- **Hidden conditions:** That batch had its own material lot, ambient state, equipment age.
- **Not reproducible:** Those conditions have changed, so exact replication is impossible.
- **The statistics:** A record result can be random variation landing in the right direction.
- **The cost:** Optimizing toward an unrepeatable outlier wastes effort on an unreachable target.

### Visualization (canvas `canvas6`, 720×300 declared; scaleCanvas renders at 720×200)

Scatter plot: normal batch cluster plus one golden-batch outlier.

- **Background:** `#f9f9f9`. **Title (17px `#1a5276`):** "Golden Batch vs Actual Batches: Unrepeatable Outlier".
- **Axes:** gray L-shape (`#666`) from (70,35)–(70,170)–(690,170); rotated y-axis label "Yield Quality" (11px `#666`).
- **Scatter:** 35 blue dots (`rgba(41,128,185,0.5)`, radius 5) pseudo-randomly placed (deterministic LCG seeded 7) with x in 100–600, y in 90–150; label "Normal batch distribution" (12px blue) at (100,165).
- **Golden batch:** yellow dot (`#f1c40f`, radius 10, stroked `#f39c12`) at (580,45) with an orange "★" glyph on top; labeled above "\"Golden Batch\"" (12px `#f39c12`) and below, at y=185, "(Lucky outlier — unrepeatable)" (12px red); dashed red vertical line (dash 3/2) from the dot down to the x-axis.

## Yield Optimization vs Safety Constraint Conflict

**The Yield-Optimal Setpoint Sits 5°C From Thermal Runaway**

- **The gradient:** Higher temperature means faster reaction, better yield, more profit.
- **The cliff:** The same temperature moves closer to runaway threshold and explosion risk.
- **Opposing objectives:** Optimization pushes to the boundary; safety demands margin from it.
- **Where it lands:** A yield model's "optimal" point can be 5°C from catastrophic runaway.
- **Root cause:** With no safety constraint encoded, dangerous is "optimal" by the objective.

### Visualization (canvas `canvas7`, 720×300 declared; scaleCanvas renders at 720×200)

Yield curve rising toward a thermal-runaway boundary.

- **Background:** `#f9f9f9`. **Title (17px `#1a5276`):** "Yield Optimization vs Safety: Dangerous Proximity".
- **Axes:** gray L-shape (`#666`) from (80,35)–(80,175)–(680,175); x caption "Temperature →" (12px `#666`, center bottom).
- **Yield curve:** green (`#27ae60`, width 2.5) rising from (80,160) to x=550: y = 160 − t·120 − sin(3t)·5; label "Yield curve" (12px green) at (85,150).
- **Runaway zone:** red rectangle `rgba(231,76,60,0.1)` from x=580 to 680 with a thick red vertical boundary line (`#e74c3c`, width 3) at x=580; labels "THERMAL" / "RUNAWAY" (12px red, centered x=630).
- **Points:** red dot (radius 7) at (555,52) labeled "\"Optimal\" (5°C from explosion)" (12px red); blue dot (radius 7) at (430,72) labeled "Safe operating point" (12px blue).
- **Safety margin:** amber (`#f39c12`, width 2) horizontal bracket from x=435 to 550 at y=52 with end ticks, labeled "Safety margin" (12px amber).

## Cleaning Validation — "Is the Equipment Actually Clean?"

**One Swabbed Square Inch Certifies 1000 Square Feet**

- **The requirement:** Equipment is cleaned between batches to prevent cross-contamination.
- **The definition:** "Clean" means residue below a ppm limit, measured by a swab test.
- **The coverage gap:** One sq inch sampled from a reactor with 1000 sq ft of surface.
- **Where residue hides:** Dead legs with no flow, valve seats, gasket surfaces, agitator shafts.
- **What a pass means:** Only that the one tested spot was clean; the rest stays unknown.

### Visualization (canvas `canvas8`, 720×300 declared; scaleCanvas renders at 720×200)

Reactor-vessel diagram: one tested spot vs many hidden residue spots.

- **Background:** `#f9f9f9`. **Title (17px `#1a5276`):** "Cleaning Validation: 1 sq inch tested of 1000 sq ft".
- **Vessel:** gray ellipse outline (`#7f8c8d`, width 2) centered (360,110), radii 200×70; internal agitator shaft and blades in `#95a5a6` (width 3): vertical line (360,40)–(360,180), horizontal blade (320,110)–(400,110), diagonal blade (340,130)–(380,90).
- **Dead legs:** thick light-gray stubs (`#bdc3c7`, width 4) at (160,100)→(120,80), (560,100)→(610,75), (200,150)→(170,170); gray 11px labels "Dead legs" (×2) and "Valve seat".
- **Residue spots:** 8 red dots (`rgba(231,76,60,0.6)`, radius 5) at (120,78), (608,73), (168,168), (280,130), (440,95), (320,75), (410,140), (500,120); label "✗ Hidden residue (untested)" (12px red) at (80,55).
- **Swab test spot:** solid green dot (`#27ae60`, radius 8) at vessel center (360,110) ringed by a dashed green circle (radius 16, dash 3/2); label "✓ Swab test: PASS" (12px green).

## Regeneration instructions

- **Layout:** detail page — h1 + `.subtitle`, then one `h2` per pitfall (1.4em `#1a5276`, bottom border `2px solid #2980b9`), each followed by a single-row `.obj-table`: left `<td>` (40%) holds `.obj-title` (a one-line punchline, not a repeat of the h2) followed by a `<ul>` of 4-5 `<li>` labeled bullets (`<strong>Label:</strong> phrase`, each fitting one line); right `<td>` (60%, centered) holds the canvas. Even table rows background `#fafcfe`.
- **Page style:** body system sans-serif (-apple-system stack), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.8em `#1a5276`; subtitle `#666` 1.05em; table cell borders `1px solid #e0e0e0`, padding 20px 24px; `.obj-title` 1.05em, weight 600, `#1a5276`; `ul` margin `8px 0 8px 20px`, 0.9em `#333`; `li` margin `4px 0`; `strong` in `#1a5276`; `.philosophy` callout style defined (background `#f0f4f8`, left border `4px solid #2980b9`) though unused on this page. No nav bar, no back/home links.
- **Canvas:** each `<canvas>` declares `width="720" height="300"`, but the shared `scaleCanvas(canvas)` helper sets both backing store and CSS size to 720×200 (× `window.devicePixelRatio`, then `ctx.scale` back to logical coordinates). Each chart fills a `#f9f9f9` background and has a 17px `#1a5276` centered title at y=20.
- **Palette:** primary blue `#1a5276`, secondary blue `#2980b9`, green `#27ae60`, red `#e74c3c`, dark red `#c0392b`, amber/orange `#f39c12`, yellow `#f1c40f`, grays `#666`/`#7f8c8d`/`#95a5a6`/`#bdc3c7`.
