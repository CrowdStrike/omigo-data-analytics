# Military Planning / C2 Domain Pitfalls

**Page type:** detail page (two-column obj-table layout: text left ~40%, canvas right ~60%, one table per h2 section)
**HTML title tag:** Military Planning / C2 Domain Pitfalls

**Subtitle:** Statistical and analytical traps in warfare planning and command-and-control systems.

## Fog of War: Deliberately Incomplete Data

**Commanders Decide on 10-30% of Ground Truth**

- **Adversarial gaps:** The enemy hides capabilities, conceals force disposition, and masks intent.
- **Fragments, not pictures:** Intelligence arrives as scattered pieces, never a full operational picture.
- **Not like civilian data:** In civilian analytics missing data is accidental — a collection gap, not a plan.
- **Engineered gaps:** In warfare the missing data is intentional, engineered by the adversary.
- **Model failure:** Any model assuming data completeness fails catastrophically against adversarial concealment.

### Visualization (canvas `canvas1`, 720×200 drawn; HTML attribute 720×300)

Horizontal bar chart: ground-truth visibility percentage by scenario.

- **Title (bold `#1a5276`, at x=140, y=22):** "Ground Truth Visibility in Decision-Making".
- **Bars:** background track `#ecf0f1` from x=200, width 420, height 28, gap 12, first row y=42; filled portion = 420 × (pct/100); border stroke `#bbb`.
- **Data (label, pct, fill color):** Civilian Analytics 85% `#27ae60`; Peacetime Military 55% `#f39c12`; Active Combat 25% `#c0392b`; Deep Behind Lines 10% `#7b241c`.
- **Labels:** scenario name right-aligned at x=190 in `#333`; percentage inside bar in white when pct>20, otherwise dark `#333` just right of the fill.
- **Footnote (italic 14px, `#c0392b`, at x=200, y=195):** "Decisions made on 10-30% of truth = catastrophic model assumptions".

## Deception as Deliberate Signal Pollution

**Your Data Is Adversarially Poisoned — The Signal-to-Noise Ratio Is Weaponized**

- **Fake signals:** Dummy tanks, decoy communications, and false radio traffic are crafted to be collected.
- **Physical props:** Inflatable equipment and spoofed electronic emissions round out the deception package.
- **Studied collection:** Intelligent adversaries study your collection methods before deciding what to show.
- **Fed narrative:** They then feed you exactly what they want you to believe about their posture.
- **The trap:** Trusting collected intelligence at face value means trusting your enemy's narrative.
- **Not random noise:** Ordinary noise averages out; adversarial noise is aimed, so more data does not help.

### Visualization (canvas `canvas2`, 720×200 drawn; HTML attribute 720×300)

Pie chart plus legend and annotation box: composition of collected intelligence.

- **Title (bold `#1a5276`, at x=120, y=22):** "Collected Intelligence: Real vs Adversarial Decoys".
- **Pie:** center (160, 120), radius 65; slices — Real Signals 35% `#27ae60`, Enemy Decoys 45% `#c0392b`, Ambiguous Noise 20% `#f39c12` (drawn clockwise from angle 0).
- **Legend (swatches 16×16 at x=280, first row y=60, 30px row spacing, text `#333`):** "Real Signals (35%)", "Enemy Decoys (45%)", "Ambiguous Noise (20%)".
- **Annotation (bold 14px `#c0392b`, two lines below legend):** "Majority of your \"intelligence\"" / "is enemy-crafted narrative".
- **Right box:** dashed `#c0392b` rectangle (510, 60, 200×55) containing two lines of 13px `#555` text: "Dummy tanks | Decoy comms | False radio" / "Spoofed emissions | Inflatable SAMs"; below it, bold 15px `#c0392b`: "ADVERSARIALLY POISONED".

## Classification Prevents Data Sharing

**The Full Operational Picture Requires Joins That Are Legally Forbidden**

- **Hard boundaries:** TOP SECRET/SCI data cannot be joined with UNCLASSIFIED sensor data at all.
- **Channel rules:** SIGINT analysts cannot share with HUMINT analysts without going through proper channels.
- **Coalition friction:** Partners operate at different clearance levels, so analysis is compartmented by design.
- **Forced silos:** The security mechanisms that protect sources and methods also prevent data fusion.
- **Best decision unreachable:** The join that would yield the best decision is exactly the forbidden one.

### Visualization (canvas `canvas3`, 720×200 drawn; HTML attribute 720×300)

Diagram: four classification compartments separated by red walls.

- **Title (bold `#1a5276`, at x=160, y=22):** "Classification Boundaries Block Data Fusion".
- **Compartment boxes (each 140×70 at y=45, fill = border color + 22-hex alpha, 2px border; bold 13px label + 12px `#333` sublabel):**
  - x=20: "TOP SECRET" / "SCI / SIGINT", color `#c0392b`
  - x=185: "SECRET" / "HUMINT Reports", color `#e67e22`
  - x=350: "CONFIDENTIAL" / "Sensor Feeds", color `#f1c40f`
  - x=515: "UNCLASSIFIED" / "Open Source", color `#27ae60`
- **Walls:** three vertical 3px `#c0392b` lines between adjacent boxes (from y=40 to y=120), each with a bold 18px `#c0392b` "⊘" symbol below at y=135.
- **Bottom labels (centered at x=360):** bold 14px `#c0392b` "LEGALLY FORBIDDEN to join across boundaries → Forced suboptimal analysis" (y=165); 13px `#555` "Full operational picture exists nowhere — compartmentation by design" (y=188).

## After-Action Reports Are Survivor-Biased

**Training Data Censored by Death — The Most Important Points Are Missing**

- **Only survivors report:** The patrol that was ambushed and annihilated leaves no debrief behind.
- **Silent failures:** Destroyed units cannot tell you what went wrong, so their errors never get recorded.
- **Biased lessons:** "Lessons learned" are lessons from survivors — what the units that came back did.
- **Missing errors:** You never learn the critical errors that led directly to a unit's destruction.
- **Systematic censoring:** The failures that matter most are permanently absent from the training data.

### Visualization (canvas `canvas4`, 720×200 drawn; HTML attribute 720×300)

Pictogram + summary: 10 deployed units, only survivors produce reports.

- **Title (bold `#1a5276`, at x=170, y=22):** "After-Action Reports: Only Survivors Report".
- **Left group label (12px `#333`, centered at x=130, y=48):** "10 Units Deployed".
- **Unit squares:** 10 squares of 28px, 8px gap, in a 5×2 grid starting at (40, 58). First 4 = survived: green `#27ae60` fill with white bold "✓". Remaining 6 = destroyed: light red `#f5b7b1` fill with a `#c0392b` 2px X drawn corner to corner.
- **Dashed gray arrow** (`#555`, dash 4/3) from (240, 90) to (300, 90).
- **Middle text:** bold 14px `#27ae60` "Reports received: 4" (310, 65); bold 14px `#c0392b` "Destroyed (silent): 6" (310, 90); 13px `#333` "Critical failure data = MISSING" (310, 112).
- **Right box:** 1px `#2980b9` rectangle (500, 42, 200×100), centered text: bold 13px `#1a5276` "\"Lessons Learned\""; 12px `#27ae60` "What survivors did right ✓"; 12px `#c0392b` "What got units killed ✗"; italic 11px "(unknown — they are dead)"; bold 13px `#c0392b` "Training data censored by death".
- **Bottom note (13px `#555`, centered at x=360, y=185):** "Survivorship bias: most important lessons are from units that cannot report".

## Wargame ≠ Reality Gap

**Simulations Have No Real Casualties, No Genuine Fog, No Panic**

- **Missing friction:** Exercises omit logistics breakdown, morale collapse, and sheer exhaustion.
- **Compounding failures:** They also omit the thousand small failures that compound in real operations.
- **Systematic optimism:** Models calibrated on clean exercise data overestimate unit effectiveness.
- **Understated losses:** The same models underestimate attrition and ignore cascading failures entirely.
- **The dictum:** "No plan survives contact with the enemy" (Moltke) — friction is the rule, not the exception.
- **Underestimated friction:** Wargame-trained models therefore understate friction by a drastic margin.

### Visualization (canvas `canvas5`, 720×200 drawn; HTML attribute 720×300)

Dual line chart: predicted vs actual unit effectiveness over days of operation.

- **Title (bold `#1a5276`, at x=185, y=22):** "Wargame Predictions vs Combat Reality".
- **Axes:** plot area x 80→660, y 55→170, gray `#555` L-shaped axes; rotated y-axis label "Unit Effectiveness %" (12px `#555`); x-axis label "Days of Operation" centered at (370, 192); x tick labels D0…D7, evenly spaced.
- **Wargame line (dashed 6/4, `#2980b9`, width 2.5):** values by day `[100, 95, 88, 82, 76, 70, 65, 60]` (% mapped to plot height).
- **Reality line (solid `#c0392b`, width 2.5):** values `[100, 78, 55, 38, 28, 22, 18, 15]`.
- **Annotation (bold 13px `#c0392b`, near day 5, stacked two lines):** "FRICTION" / "GAP".
- **Legend (upper right):** dashed blue sample line + "Wargame prediction" (`#2980b9`, 13px); solid red sample line + "Combat reality" (`#c0392b`).

## Tempo and OODA Loop

**A Perfect Model at 30-Minute Latency Loses to an Imperfect One at 30 Seconds**

- **Data decay:** Data 6 hours old is tactically worthless — the enemy has moved, regrouped, or struck.
- **Decision advantage:** The side with the faster OODA loop (Observe, Orient, Decide, Act) sets the pace.
- **Reactive opponent:** The slower side is forced into reactive mode, answering moves already made.
- **Speed beats accuracy:** "Good enough now" dominates "perfect later" in essentially every engagement.
- **Perfection as enemy:** Analytical perfection is the enemy of operational tempo when latency is the cost.

### Visualization (canvas `canvas6`, 720×200 drawn; HTML attribute 720×300)

Diagram: two OODA loop circles (fast vs slow) plus a comparison mini-chart.

- **Title (bold `#1a5276`, at x=180, y=22):** "OODA Loop: Speed > Accuracy in Combat".
- **Fast loop:** solid green `#27ae60` circle, center (130, 115), radius 42, 3px stroke; bold 11px "O", "O", "D", "A" placed at top/right/bottom/left of the circle; below: bold 13px green "30-sec cycle" / "WINS"; small green triangular rotation arrow at upper right of the circle.
- **Slow loop:** dashed (5/4) red `#c0392b` circle, center (330, 115), radius 55, 2px stroke; same O/O/D/A labels in bold 12px red; below: bold 13px "30-min cycle" / "LOSES".
- **Right comparison block (starting at x=460, y=50):** row labels in `#333` 13px — "Decision Quality:", "Latency:" (+50px), "Combat Outcome:" (+100px).
  - Quality bars: green `#27ae60` bar 60px wide labeled "70%" (white); blue `#2980b9` bar 90px wide labeled "95%" (white).
  - Latency bars: green bar 20px wide labeled "30s"; red `#c0392b` bar 180px wide labeled "30 min".
  - Outcome: bold 14px green "VICTORY" and bold 14px red "DEFEAT".
- **Bottom quote (italic 13px `#555`, centered at x=360, y=192):** "\"6-hour-old data is tactically worthless — the enemy has already moved\"".

## Regeneration instructions

- **Layout:** standard detail-page structure — h1, `.subtitle` paragraph, then per pitfall an `<h2>` section heading followed by a one-row `.obj-table`: left `<td>` (40%) with `.obj-title` div + `<ul>` of labeled bullets, right `<td>` (60%, centered) with a `<canvas>` (HTML attributes `width="720" height="300"`).
- **Page CSS:** body system sans-serif (-apple-system, BlinkMacSystemFont, 'Segoe UI'), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.8em `#1a5276`; h2 1.4em `#1a5276` with 2px `#2980b9` bottom border; subtitle `#666` 1.05em; `ul` 0.9em `#333`; `strong` `#1a5276`; `.obj-table td` border `1px solid #e0e0e0`, padding 20px 24px, even rows background `#fafcfe`; `.obj-title` 1.05em weight 600 `#1a5276`; `.philosophy` callout class defined (background `#f0f4f8`, left border `4px solid #2980b9`) though unused on this page. No nav bar, no back/home links.
- **Canvas:** shared `setupCanvas(id)` helper scales by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates), default font `17px -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif`; constants `HEADER_COLOR = #1a5276`, `BORDER_COLOR = #2980b9`. Note the drawn size (720×200) overrides the 720×300 HTML attribute.
- **Palette:** primary blue `#1a5276`, accent blue `#2980b9`, green `#27ae60`, red `#e74c3c` (this page mostly uses dark red `#c0392b`), orange `#e67e22`/`#f39c12`, gray text `#555`/`#333`.
- In regenerated HTML, any card links use `.html` extensions (this page has no outbound links).
