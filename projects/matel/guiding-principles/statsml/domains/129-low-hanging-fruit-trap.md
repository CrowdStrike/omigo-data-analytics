# Low-Hanging Fruit Trap (Simple Wins Justifying Complex Systems)

**Page type:** detail page (h2 section heading per pitfall, each followed by a one-row two-column obj-table: text left ~40%, canvas right ~60%)
**HTML title tag:** 129. Low-Hanging Fruit Trap (Simple Wins Justifying Complex Systems)

**Subtitle:** Early easy wins set linear expectations that logarithmic reality — and sunk-cost politics — can never fulfill.

## The 80% Illusion

- First ML model catches 80% of easy cases
- ROI looks amazing initially
- Funds "Phase 2" based on linear expectations

**Example:** Remaining 20% = hard cases, 10x cost per percent. Business expected linear progress, got logarithmic.

### Visualization (canvas `c1`, 720×200)

Two-curve line chart comparing expected vs actual progress against cost.

- **Title (17px `#1a5276`, at 20,25):** "Progress vs Cost: Expected (Linear) vs Reality (Logarithmic)".
- **Expected line:** solid blue `#2980b9`, width 2 — straight line from (80,180) rising linearly to (680,30) (y = 180 − (i/600)·150 over i=0..600).
- **Reality line:** dashed red `#e74c3c` (dash 5/5), width 2 — logarithmic curve y = 180 − log(1+10i)/log(6001)·150 over the same x range (fast rise then flattening).
- **Legend labels:** "Expected" in `#2980b9` at (600,55); "Reality" in `#e74c3c` at (600,75).
- **Axis labels (14px `#666`):** "Cost/Effort →" at (300,195); "% Solved" rotated −90° at left (translate 15,120).

## Simple Rules Ignored

- Simple rules could catch same 80% at 1/100th cost
- "We use AI" sounds better in board decks
- Complexity chosen for optics, not outcomes

**Example:** Rule-based filter achieves 78% accuracy at $500/mo; ML system achieves 81% at $50,000/mo. Board funds the ML.

### Visualization (canvas `c2`, 720×200)

Two horizontal bars comparing monthly cost at similar accuracy.

- **Title (17px `#1a5276`, at 20,25):** "Monthly Cost: Simple Rules vs ML System (similar accuracy)".
- **Bars:** green `#27ae60` rect at (100,60) size 80×100 (rules); red `#e74c3c` rect at (350,60) size 250×100 (ML) — bar width encodes cost.
- **Labels (15px `#333`):** "Rules: $500" at (90,180); "ML: $50,000" at (400,180); accuracy values inside bars: "78%" at (120,110), "81%" at (460,110).

## Political Momentum Lock-in

- Easy head creates unstoppable political momentum
- "We can't shut down the AI team — look at Phase 1 success!"
- Sunk cost + ego prevent course correction

**Example:** Phase 1 success means team grows from 3 to 15 people. Phase 2 delivers marginal gains but team is now "strategic."

### Visualization (canvas `c3`, 720×200)

Bar chart of team size vs marginal accuracy gain per phase.

- **Title (17px `#1a5276`, at 20,25):** "Team Size vs Marginal Accuracy Gain per Phase".
- **Data:** team sizes `[3, 8, 15, 22]`, gains `[80, 8, 3, 1]` (%).
- **Bars:** blue `#2980b9`, 60px wide at x = 120 + i·150, height = gain·1.8, baseline y=170.
- **Labels (14px `#333`):** under each bar "Team:3", "Team:8", "Team:15", "Team:22" at y=188; above each bar "+80%", "+8%", "+3%", "+1%".

## Sunk Cost Infrastructure

- Phase 1 complexity (infrastructure, team, tools) becomes sunk cost
- Demands Phase 2 justification
- Organization can't admit simpler was better

**Example:** $2M in GPU clusters, MLOps pipelines, and ML engineers — must justify continued use even when heuristics suffice.

### Visualization (canvas `c4`, 720×200)

Rising line chart of cumulative sunk cost.

- **Title (17px `#1a5276`, at 20,25):** "Cumulative Sunk Cost Driving Phase 2 Commitment".
- **Data:** cumulative costs `[200, 500, 900, 1400, 2000, 2800]` ($K), plotted as red `#e74c3c` line (width 3) at x = 100 + i·105, y = 180 − cost/2800·140.
- **X labels (13px `#666`):** "GPUs", "+Team", "+Tools", "+Ops", "+Data", "+Maint" at y=195.
- **Annotation:** "$2.8M total sunk" in red `#e74c3c` at (540,55).

## Invisible Counterfactual

- Phase 2 "improvements" are 5% gains at 10x cost each
- Nobody compares to what simple rules would have achieved
- Baseline never established properly

**Example:** $500K spent for 83% to 88% improvement. Simple rules + one engineer could have reached 85% for $80K.

### Visualization (canvas `c5`, 720×200)

Two horizontal bars: actual ML spend vs simple-rules counterfactual.

- **Title (17px `#1a5276`, at 20,25):** "ML Spend vs Simple-Rules Counterfactual".
- **Bars:** blue `#2980b9` rect at (100,80) size 200×40 (ML); green `#27ae60` rect at (100,140) size 40×40 (rules) — width encodes spend.
- **Labels (15px `#333`):** "ML: $500K → 88%" at (320,105); "Rules: $80K → 85%" at (320,165); gray `#999` note "3% difference, 6x cost" at (320,190).

## Chatbot Escalation Disaster

- Chatbot handles 60% of tickets (FAQs) — Phase 1 success
- Phase 2 target: 80% automation
- Next 20% = complex emotional issues

**Example:** Chatbot attempts empathy on billing disputes. Makes customers angrier. Resolution cost 3x higher than human-first approach.

### Visualization (canvas `c6`, 720×200)

Three-bar chart of cost per resolution by handling mode.

- **Title (17px `#1a5276`, at 20,25):** "Cost per Resolution: FAQ vs Complex Emotional Issues".
- **Bars:** green `#27ae60` rect at (80,70) size 120×90; orange `#e67e22` rect at (280,70) size 120×90; red `#e74c3c` rect at (480,50) size 120×110.
- **Labels (14px `#333`):** "Bot FAQ" / "$2/ticket" at (100,180)/(100,195); "Bot Complex" / "$45/ticket" at (285,180)/(295,195); "Human First" / "$15/ticket" at (490,180)/(500,195).

## Self-Driving Last Mile

- 99.99% of driving handled — business case approved
- Last 0.01% = edge cases
- May take 10x longer/$ than first 99.99%

**Example:** $1B to reach 99.99%. Remaining 0.01% (construction zones, unusual weather) may require $10B+ and a decade more.

### Visualization (canvas `c7`, 720×200)

Bar chart: cost per "nine" of reliability, exponentially increasing.

- **Title (17px `#1a5276`, at 20,25):** "Self-Driving: Cost per \"Nine\" of Reliability".
- **Data:** nines `["90%", "99%", "99.9%", "99.99%", "99.999%"]`, bar heights `[10, 40, 80, 130, 170]`.
- **Bars:** blue `#2980b9`, 80px wide at x = 80 + i·130, baseline y=180; x labels (12px `#333`) at y=195.
- **Annotation:** "Each \"nine\" costs exponentially more" in red `#e74c3c` (14px) at (300,25).

## Hidden Economics of "% Automated"

- "% automated" hides that easy vs hard cases differ completely
- Different economics, difficulty, and risk profiles
- Aggregate metrics mask bimodal distributions

**Example:** "90% automated" = 90% trivial cases handled. Remaining 10% carry 80% of revenue risk and 95% of complexity.

### Visualization (canvas `c8`, 720×200)

Scatter of two case clusters showing hidden bimodality.

- **Title (17px `#1a5276`, at 20,25):** "Hidden Bimodal: Easy Cases vs Hard Cases".
- **Easy cluster:** 20 small dots (radius 6) in translucent green `rgba(39,174,96,0.6)`, arranged in a 5-column grid near x≈100–250, y≈70–170 with small random jitter.
- **Hard cluster:** 8 larger dots (radius 9) in translucent red `rgba(231,76,60,0.6)`, in a 4-column grid near x≈420–650, y≈70–130 with random jitter.
- **Captions (14px):** green `#27ae60` "Easy (90%): Low cost, low risk" at (80,185); red `#e74c3c` "Hard (10%): 80% revenue risk, 95% complexity" at (380,185).

## Regeneration instructions

- **Layout:** for each of the 8 pitfalls, an `<h2>` section heading (1.4em `#1a5276`, bottom border `2px solid #2980b9`, padding-bottom 8px) followed by a one-row `.obj-table`: full-width table, left `<td>` (40%) holds `.obj-title` + `<ul>` bullets + an `<p><strong>Example:</strong> ...</p>` paragraph, right `<td>` (60%, centered) holds the canvas. Even rows background `#fafcfe`; cell borders `1px solid #e0e0e0`, padding 20px 24px.
- **Page style:** body system sans-serif (-apple-system stack), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.8em `#1a5276`; `.subtitle` `#666` 1.05em; `ul` 0.9em `#333`; `strong` `#1a5276`; `.obj-title` 1.05em weight 600 `#1a5276`. Unused `.philosophy` class: background `#f0f4f8`, left border `4px solid #2980b9`. No nav bar, no back/home links.
- **Canvas:** HTML attributes declare `width="720" height="300"` but a shared init loop overrides every canvas to a 720×200 logical size — backing store sized to rendered width × `window.devicePixelRatio` (display capped via `style.maxWidth`), CSS size fixed at 720×200 px, `ctx.scale` back to logical coordinates. All chart coordinates above are in the 720×200 space. Chart titles 17px, labels 12–15px, `-apple-system, sans-serif`.
- **Palette:** primary blue `#1a5276`, accent blue `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, gray text `#666`/`#333`/`#999`.
- Card links elsewhere referencing this page use the `.html` extension in regenerated HTML.
