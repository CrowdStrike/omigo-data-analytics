# MCMC & Particle Filters

**Page type:** detail page (four titled sections, each a two-column row: text left 50%, canvas right 50%)
**HTML title tag:** MCMC & Particle Filters — Calibrating Simulators Against Reality

**Subtitle:** A simulator turns settings into predicted data — but reality hands us the data and hides the settings. These are the machines that run the movie backwards.

**Intro callout (blue-left-border box):** Weather models, epidemic models, and engineering simulators all play forward: dial in the settings, get out a prediction. The question science actually asks is the reverse — which settings would make the simulator reproduce what we measured? There is rarely a formula for that answer, so two workhorse machines approximate it by sheer repetition: a guided random walk through the space of settings (MCMC), and a swarm of candidate states pruned every time a new observation arrives (particle filters).

## 1. Running the Movie Backwards

A simulator is a movie that only plays forward — settings in, predicted data out — while reality hands us the ending and hides the settings (the inverse problem).

- **Forward is easy:** Give the simulator its settings and it plays forward to predicted data.
- **Reality reverses it:** We hold the measured data, and the settings that produced it are hidden.
- **Many settings fit:** Several different dial positions can produce data that looks like ours.
- **The answer is a cloud:** The honest answer is a weighted cloud of settings, not one best value.
- **Weigh by fit:** Each candidate setting is weighted by how well it explains the data.
- **Weigh by plausibility:** That weight is multiplied by how believable the setting was beforehand.
- **The one rule:** Belief after seeing data is fit times prior plausibility (Bayes' rule).

Key point: Calibration never returns the setting — it returns a weighted cloud of settings the data cannot rule out, and every method on this page is just a different way of drawing samples from that cloud.

### Visualization (canvas `c1`, 720×340)

Two-row diagram: top row shows the easy forward direction (settings → simulator → predicted data), bottom row shows the same pipeline reversed with the settings box replaced by a question mark; a purple belief-update rule strip sits underneath.

- **Title (bold 14px `#1a5276`, centered, y=22):** "Forward is easy — reality asks the reverse question"
- **Top row header:** bold 11px `#27ae60` left-aligned at (50, 58): "FORWARD — what the simulator does".
- **Top row boxes (y=70, height 60):** Box A — 150×60 at (50, 70), white fill, 1.5px `#1a5276` border; bold 12px `#1a5276` centered "SETTINGS" at (125, 96); 10px `#666` centered "the model's dials" at (125, 114). Box B — 160×60 at (280, 70), fill `rgba(26,82,118,0.12)`, 2px `#1a5276` border; bold 12px `#1a5276` centered "SIMULATOR" at (360, 96); 10px `#666` centered "plays the movie forward" at (360, 114). Box C — 150×60 at (520, 70), white fill, 1.5px `#27ae60` border; bold 12px `#27ae60` centered "PREDICTED DATA" at (595, 96); 10px `#666` centered "what you would observe" at (595, 114).
- **Top arrows:** 2px `#27ae60` arrows with filled arrowheads from (210, 100) to (275, 100) and from (450, 100) to (515, 100); 10px `#27ae60` centered "run" at (242, 88).
- **Bottom row header:** bold 11px `#e74c3c` left-aligned at (50, 170): "INVERSE — what reality hands us".
- **Question mark:** bold 16px `#e74c3c` centered "?" at (125, 186), hovering over the settings box.
- **Bottom row boxes (y=190, height 60):** Box D — 150×60 at (520, 190), white fill, 1.5px `#1a5276` border; bold 12px `#1a5276` centered "MEASURED DATA" at (595, 216); 10px `#666` centered "this is all we get" at (595, 234). Box E — 160×60 at (280, 190), fill `rgba(26,82,118,0.12)`, 2px `#1a5276` border; bold 12px `#1a5276` centered "SIMULATOR" at (360, 216); 10px `#666` centered "same machine, run in reverse?" at (360, 234). Box F — 150×60 at (50, 190), white fill, dashed (4/3) 1.5px `#e74c3c` border; bold 12px `#e74c3c` centered "SETTINGS = ?" at (125, 216); 10px `#666` centered "hidden from us" at (125, 234).
- **Bottom arrows (right to left):** 2px `#e74c3c` arrows with filled arrowheads from (515, 220) to (450, 220) and from (275, 220) to (210, 220); 10px `#e74c3c` centered "which settings?" at (482, 208).
- **Belief rule strip:** bold 11px `#8e44ad` centered at (360, 288): "belief after seeing data ∝ how well a setting explains it × how plausible it was beforehand"; 10px `#666` centered at (360, 306): "(Bayes' rule: posterior ∝ likelihood × prior)".
- **Caption (12px `#999`, centered, y = h−14):** "The answer is not one best setting — it is a weighted cloud of settings the data cannot rule out"

## 2. MCMC — a Guided Random Walk

When the cloud of plausible settings cannot be written down as a formula, walk through it — a random stroll that spends its time where the data fit is good (Markov chain Monte Carlo).

- **Propose nearby:** From the current setting, nudge the dials a little at random.
- **Keep if better:** If the new setting explains the data better, move there.
- **Sometimes keep worse:** Accept a worse setting with a probability tied to how much worse (Metropolis rule).
- **Why accept worse:** Occasional downhill moves keep the walk from freezing on one peak.
- **Throw away the start:** Early steps remember the arbitrary starting point and are discarded (burn-in).
- **Healthy walks roam:** A good walk covers the whole plausible region instead of getting stuck (mixing).
- **Check with rivals:** Run several walks from scattered starts and demand that they agree.
- **Every step costs:** Each proposed step requires one full run of the simulator.

Key point: MCMC turns "map the whole cloud" into "take a long guided stroll through it" — but every step buys one simulator run, and a stuck walk quietly reports the wrong cloud with full confidence.

### Visualization (canvas `c2`, 720×400)

2D contour rings marking the plausible region of settings, a gray early path snaking in from the bottom-left corner, a dense blue cloud of later samples inside the rings, and a short orange second walk arriving from the top-right.

- **Title (bold 14px `#1a5276`, centered, y=22):** "A guided random walk maps the plausible region"
- **Sub-note:** 11px `#666` centered at (390, 44): "each dot below cost one full simulator run".
- **Contour rings:** three ellipses centered (390, 215) with radii (170, 105), (115, 70), (60, 36), strokes 1.5px `#ccc`, `#bbb`, `#999` respectively (outer to inner).
- **Gray burn-in path:** 1.5px `#999` polyline with a filled `#999` dot of radius 2 at every vertex, through (70, 310), (115, 285), (100, 255), (150, 265), (175, 235), (160, 210), (210, 225), (245, 198), (275, 212), (300, 192).
- **Blue sample cloud:** 40 filled circles of radius 2.5, fill `rgba(26,82,118,0.35)`, generated deterministically: for i = 0…39, a = i × 2.399963, r = 14 + ((i × 37) mod 100) × 0.85, x = 390 + r·cos(a), y = 215 + 0.62·r·sin(a).
- **Orange second walk:** 1.5px `#e67e22` polyline with filled `#e67e22` dots of radius 2 at every vertex, through (655, 75), (610, 105), (630, 140), (575, 130), (545, 165), (500, 150), (470, 185), (440, 200).
- **Legend (three left-aligned lines at x=36):** bold 11px `#999` at y=336: "gray — early steps from an arbitrary start, thrown away (burn-in)"; bold 11px `#1a5276` at y=352: "blue — later steps roam the whole plausible region (mixing)"; bold 11px `#e67e22` at y=368: "orange — a second walk from a different start must land in the same region".
- **Caption (12px `#999`, centered, y = h−14):** "Healthy walks forget where they started and agree with each other — disagreement means keep walking"

## 3. Particle Filters — Tracking as Data Streams In

When data arrive as a stream, keep a swarm of candidate states alive and let each new observation prune it (particle filter).

- **Swarm, not estimate:** Track a crowd of candidate states instead of one best guess (particles).
- **Predict forward:** The simulator pushes every candidate one step ahead in time.
- **Reweight on arrival:** Each new observation scores every candidate by agreement with it.
- **Clone and drop:** Good candidates are copied and bad ones are deleted (resampling).
- **Spread is honesty:** The swarm's spread at any moment is the current uncertainty.
- **Weather does this:** Forecast centers fold fresh observations into running models the same way (data assimilation).
- **Kalman cousin:** Their tool of choice is a bell-curve cousin of the swarm (ensemble Kalman filter).
- **Collapse risk:** The swarm can shrink to a few identical clones and lose its diversity (degeneracy).

Key point: A particle filter is survival of the fittest for hypotheses — each observation prunes the swarm — but let it collapse to clones of one winner and it becomes confidently blind.

### Visualization (canvas `c3`, 720×380)

Particle clouds at four successive observation times narrowing around a dashed true trajectory, extreme poor-fit particles marked red in the early clouds, spread visibly shrinking after each observation tick.

- **Title (bold 14px `#1a5276`, centered, y=22):** "A swarm of guesses, pruned by every new observation"
- **Annotations:** bold 11px `#27ae60` centered at (360, 48): "spread of the swarm = current uncertainty — it shrinks after every observation"; 10px `#666` centered at (360, 64): "weather centers fold observations into forecasts the same way (data assimilation)".
- **Axis:** 1.5px `#999` horizontal line at y=308 from x=55 to x=680; four 1.5px `#999` tick marks 6px tall below the axis at x = 150, 290, 430, 570; 10px `#999` centered labels "obs 1", "obs 2", "obs 3", "obs 4" at y=324 under each tick; 10px `#999` centered "time →" at (655, 324).
- **True trajectory:** dashed (5/4) 2px `#555` quadratic curve from (80, 250) with control point (370, 215) to (660, 168); 10px `#555` left-aligned label "true state (unknown, dashed)" at (84, 236).
- **Particle clouds:** four clouds centered on the trajectory at (150, 241), (290, 223), (430, 203), (570, 182) with vertical half-spreads 62, 40, 24, 13. Each cloud has 12 dots of radius 2.5 at vertical offset fractions f = [−1, −0.75, −0.55, −0.38, −0.22, −0.08, 0.08, 0.22, 0.38, 0.55, 0.75, 1] (dot y = center y + f × half-spread) and horizontal jitter xj = ((i × 7) mod 13) − 6 for dot index i = 0…11. Fill `rgba(26,82,118,0.35)`, except the two extreme dots (f = ±1) of clouds 1 and 2, which are filled `rgba(231,76,60,0.45)`.
- **Legend:** 10px `#1a5276` left-aligned at (60, 344): "blue — surviving candidates (particles)"; 10px `#e74c3c` left-aligned at (360, 344): "red — poor fits, deleted and replaced by clones (resampling)".
- **Caption (12px `#999`, centered, y = h−14):** "If every particle becomes a clone of one winner, the filter is confidently blind — watch the swarm's diversity"

## 4. Traps in the Inverse Road

Running the movie backwards has failure modes of its own, and the worst ones look exactly like success.

- **Twins in the data:** Different setting combinations can produce identical outputs (non-identifiability).
- **Data can't split twins:** No amount of the same data separates them — only a new kind of measurement can.
- **Hours per step:** When one simulator run takes hours, a walk of a hundred thousand steps is off the table.
- **Fast stand-ins:** Train a cheap lookalike of the simulator and walk on that instead (emulator).
- **Compare summaries:** Or accept settings whose simulated summary statistics land near the real ones (approximate Bayesian computation).
- **Wrong simulator, sure answer:** If the simulator itself is wrong, calibration confidently converges on wrong settings (model discrepancy).
- **Replay the fit:** Always simulate from the fitted settings and check the output reproduces the data (posterior predictive check).

Key point: Calibration measures how well the settings fit the simulator, not how well the simulator fits the world — a confidently converged answer from a wrong model is the most dangerous output on this page.

### Visualization (canvas `c4`, 720×380)

A banana-shaped ridge of equally good settings in a 2D parameter plane, two distant points on the ridge both labeled as fitting equally well, and a purple trade-off annotation above.

- **Title (bold 14px `#1a5276`, centered, y=22):** "Two different settings, one identical fit — the data cannot choose"
- **Axes:** 1.5px `#999` lines — vertical from (70, 50) to (70, 320), horizontal from (70, 320) to (680, 320); 10px `#666` centered x-label "how fast it spreads →" at (375, 340); 10px `#666` left-aligned y-label "↑ how fast people recover" at (76, 60).
- **Annotations:** bold 11px `#8e44ad` centered at (400, 60): "infection rate and recovery rate trade off — only their ratio is pinned down"; 10px `#666` centered at (400, 78): "every point on the ridge reproduces the data exactly as well".
- **Outer ridge band:** filled `rgba(26,82,118,0.12)` path: moveTo (140, 268), quadratic curve with control (330, 185) to (560, 88), lineTo (560, 132), quadratic curve with control (330, 235) to (160, 312), closePath.
- **Inner ridge band:** filled `rgba(26,82,118,0.35)` path: moveTo (146, 280), quadratic curve with control (330, 197) to (558, 100), lineTo (558, 120), quadratic curve with control (330, 223) to (154, 300), closePath.
- **Point A:** filled `#e74c3c` circle radius 5 at (205, 266); bold 11px `#e74c3c` centered label "Setting A — fits equally well" at (185, 220); 1px `#e74c3c` connector line from (203, 258) to (187, 226).
- **Point B:** filled `#e74c3c` circle radius 5 at (491, 140); bold 11px `#e74c3c` right-aligned label "Setting B — fits equally well" at (665, 168); 1px `#e74c3c` connector line from (500, 148) to (560, 162).
- **Tie-breaker note:** 10px `#27ae60` centered at (400, 292): "no amount of this data breaks the tie — only a new kind of measurement can".
- **Caption (12px `#999`, centered, y = h−14):** "A ridge of equally good settings means the answer is the ridge, not a point on it"

## Regeneration instructions

- **Template:** backlog multi-section detail page. After h1, `.subtitle`, and `.intro` callout, one `.lang-section` per numbered section: `h2` (1.3rem `#1a5276`, `border-bottom: 2px solid #2980b9`, 4px padding-bottom), then a `table.layout` with one `<tr>`: left `<td class="text-col">` (50%) with lead paragraph, bullets, `.key-point`; right `<td class="viz-col">` (50%) with the canvas. Section h2 headings carry the "1.–4." numbers shown above. The h1 carries no index number.
- **Bullet style:** each bullet is a bold label + one short complete sentence that fits on one line at normal page width; split dense content into more bullets rather than longer ones. Bullet `<strong>` labels are colored `#1a5276` (via `li strong { color: #1a5276; }`).
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with `border-bottom: 2px solid #2980b9`; `.subtitle` `#666` 0.95rem; `.intro` — background `#f0f4f8`, `border-left: 3px solid #2980b9`, padding 8px 12px, 0.9rem, 32px margin-bottom; `.key-point` — background `#f8f9fa`, `border-left: 3px solid #e74c3c`, padding 8px 12px, 0.9rem; `ul` 0.92rem; canvases `width: 100%`, `border: 1px solid #e0e0e0`, radius 4px; `.lang-section` 40px margin-bottom.
- **Canvases:** intrinsic width 720, heights 340/400/380/380 as specified; shared `setupCanvas(id, h)` helper scales by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates). Canvas fonts use `-apple-system, sans-serif`. Every canvas gets a bold 14px `#1a5276` centered title at y=22 and a 12px `#999` centered caption at y = h−14.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, purple `#8e44ad`, accent `#2980b9`, teal `#16a085`; grays `#555`/`#666`/`#999`/`#bbb`/`#ccc`; translucent fills `rgba(26,82,118,0.12)`, `rgba(26,82,118,0.35)`, `rgba(231,76,60,0.45)`.
- **Layman language:** lead with the plain-words explanation everywhere; the jargon term appears at most once, in parentheses after its translation. h2 titles may keep the proper term.
- No nav bar, no back/home links, no cross-references to other pages, no item counts in text.
