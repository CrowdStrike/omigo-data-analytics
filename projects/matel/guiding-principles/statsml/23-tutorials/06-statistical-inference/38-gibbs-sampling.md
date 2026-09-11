# Gibbs Sampling

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Gibbs Sampling

**Subtitle:** An MCMC method that explores a joint distribution one coordinate at a time — hold everything else fixed, draw one variable from its conditional, repeat, and the staircase walk maps the whole region

## Guessing Lunch and Dinner Together, One at a Time

**Tags:** `core idea` (blue), `one coordinate at a time` (green), `MCMC` (orange)

- **The truck** — a food truck logs lunch and dinner sales; big-lunch days are usually big-dinner days
- **The problem** — picturing all plausible (lunch, dinner) pairs at once — the joint — is hard
- **The trick** — fix dinner, draw a plausible lunch; fix that lunch, draw a plausible dinner; repeat
- **The staircase** — every move changes one number, so the path is all horizontal and vertical steps
- **Gibbs sampling** — this alternate-one-coordinate walk is exactly what a Gibbs sampler does

*Example (italic):* Starting from a bad guess ($250 lunch, $750 dinner), ten one-knob updates land the walk inside the plausible zone.

**Key point:** Gibbs sampling explores a joint distribution by repeatedly sampling ONE coordinate from its conditional distribution, holding all the others at their current values.

### Visualization (canvas `c1`, 720×300)

Single 2-D plot: a tilted elliptical "plausible zone" for (lunch, dinner) with a fixed axis-aligned staircase walk entering it from a bad starting guess.

- **Title (bold 15px, `#1a5276`, top center):** "Ten One-Knob Updates: the Staircase Walk of Gibbs Sampling".
- **Plot area:** origin x=70, baseline y=252, width 540, height 205; x axis = lunch $200–$600 (ticks $250/$400/$550), y axis = dinner $450–$800 (ticks $500/$600/$700/$800); axis lines 2px ink `#1a5276`, tick labels 12px `#444`; axis titles 12px `#444` "lunch sales ($)" below, "dinner ($)" rotated left.
- **Plausible zone:** ellipse centered at data point (400, 600), rotated −30° in screen coordinates, semi-axes 150×55 px; fill `rgba(42,120,214,0.12)`, border 1.5px `rgba(42,120,214,0.5)`; ink `#1a5276` 12px label "plausible (lunch, dinner) pairs" near its upper edge.
- **Walk (hardcoded, no randomness):** points `[[250,750],[540,750],[540,690],[450,690],[450,610],[380,610],[380,560],[410,560],[410,630],[460,630],[460,650]]`; orange `#d95926` 2.5px polyline with 4px orange dots at each point.
- **Start marker:** magenta `#d55181` 6px dot at (250, 750), bold 12px magenta label "start ($250, $750)".
- **End marker:** green `#008300` 6px dot at (460, 650), bold 12px green label "after 10 updates".
- **Annotation (bold 13px green):** "every move changes one coordinate only" placed in the lower-right white space.

## The Update Rule, With Actual Dollars

**Tags:** `worked example` (blue), `conditional distribution` (green)

- **The plausible zone** — lunch averages $400, dinner $600, each give-or-take $50, moving together
- **Lunch rule** — given dinner D, lunch is bell-shaped around 400 + 0.8×(D − 600), spread $30
- **Dinner rule** — given lunch L, dinner is bell-shaped around 600 + 0.8×(L − 400), spread $30
- **Step 1** — dinner sits at $750, so lunch centers at $520; the draw comes out $540
- **Step 2** — lunch is now $540, so dinner centers at $712; the draw comes out $690
- **Keep going** — the next lunch centers at $472 (draw $450); every step is one 1-D bell draw

*Example (italic):* Two steps move the guess from ($250, $750) to ($540, $690) — and each step was just a one-variable draw.

**Key point:** You never sample the joint directly — each update is a one-dimensional draw whose center depends on the other coordinate's current value.

### Visualization (canvas `c2`, 720×300)

Dual-panel bell curves: the two conditional distributions used in steps 1 and 2, each with its actual draw marked, split by a vertical dashed divider at x=360.

- **Title (bold 15px, `#1a5276`, top center):** "Each Gibbs Step Is One Easy Bell-Curve Draw".
- **Left panel (step 1):** axis origin x=55, width 280, baseline y=245, chart height 165; horizontal axis lunch $420–$620 with tick labels "$450", "$520", "$590" (12px `#444`); bell curve `y = exp(-0.5*((v-520)/30)^2)` scaled to chart height, blue `#2a78d6` 3px; vertical dashed magenta `#d55181` line at $540 with bold 12px magenta label "draw: $540"; heading bold 12px ink `#1a5276` "step 1: lunch | dinner = $750"; caption 12px `#444` "centers at 400 + 0.8×(750−600) = $520, spread $30".
- **Right panel (step 2):** axis origin x=400, width 280, same baseline/height; horizontal axis dinner $610–$810 with tick labels "$640", "$712", "$780"; bell centered 712, spread 30, green `#008300` 3px; vertical dashed magenta line at $690 labeled "draw: $690" (bold 12px magenta); heading "step 2: dinner | lunch = $540"; caption "centers at 600 + 0.8×(540−400) = $712, spread $30".
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=360 from y=38 to h-12.

## Why Samplers Do This: Hard Joints, Easy Conditionals

**Tags:** `where it's used` (blue), `burn-in` (orange)

- **Where it shows up** — Bayesian models with many unknowns: topic models, hierarchies, missing data
- **Why it wins** — the joint over 10+ parameters is intractable, but each conditional is an easy 1-D draw
- **Burn-in** — early samples still remember the $250 starting guess and must be thrown away
- **After burn-in** — the lunch trace wanders around the true $400 center, mapping out its spread
- **The payoff** — averaging the kept draws estimates anything: means, intervals, probabilities

*Example (italic):* After dropping the first 3 iterations, the remaining 17 lunch draws average $404 — close to the true $400.

**Key point:** Gibbs turns one impossible many-dimensional sampling problem into thousands of easy one-dimensional draws — at the cost of burn-in and correlated samples.

### Visualization (canvas `c3`, 720×300)

Trace plot of the lunch coordinate across 20 Gibbs iterations, with the burn-in region shaded and the true average marked.

- **Title (bold 15px, `#1a5276`, top center):** "Lunch Draws Across 20 Iterations: Burn-In, Then a Steady Wander".
- **Data (hardcoded):** lunch trace `[250, 540, 450, 380, 410, 460, 430, 370, 400, 350, 420, 455, 390, 360, 415, 440, 385, 405, 370, 425]` at iterations 0–19 (mean of iterations 3–19 is $404).
- **Plot area:** axis origin x=65, baseline y=245, width 590, chart height 185; y range $220–$570 with tick labels "$250", "$400", "$550" (12px `#444`); x tick labels "0", "5", "10", "15", "19" (12px `#444`); axis title 12px `#444` "iteration" below.
- **Burn-in band:** iterations 0–2 shaded `rgba(217,89,38,0.10)` full chart height; bold 12px orange `#d95926` label "burn-in: drop these 3" at the top of the band.
- **Reference line:** dashed green `#008300` (dash 5/4) horizontal line at $400, 12px green label "true average $400" at its right end.
- **Trace:** blue `#2a78d6` 2.5px polyline with 3.5px blue dots.
- **Annotation (bold 13px green):** "kept 17 draws average $404" placed in the upper-right white space.

## When the Staircase Crawls

**Tags:** `common mistake` (red), `correlated samples` (orange)

- **Not independent** — each draw starts from the last one, so consecutive samples look alike
- **Correlation hurts** — when lunch and dinner move in near lock-step, the conditionals get narrow
- **Tiny steps** — at correlation 0.98 the one-knob spread shrinks from $30 to about $10 per move
- **The crawl** — eleven tiny moves cover the ground two moderate strides covered at correlation 0.8
- **The fix** — reparametrize, update correlated variables together as a block, or change samplers

*Example (italic):* At correlation 0.98 the walk needs eleven moves to cross ground that two moderate strides covered before.

**Common mistake:** Treating Gibbs draws as independent data. When coordinates are strongly correlated the chain crawls, and a short run badly understates the true spread — thin the chain or run it much longer.

### Visualization (canvas `c4`, 720×300)

Dual-panel comparison: the same one-knob staircase walk inside a moderate ellipse (left) vs a near-lock-step narrow ridge (right), split by a dashed divider at x=360.

- **Title (bold 15px, `#1a5276`, top center):** "Moderate vs Lock-Step Correlation: the Staircase Crawls".
- **Both panels:** data ranges lunch $300–$500 (horizontal), dinner $480–$720 (vertical); baseline y=250, chart height 190; axis lines 1.5px `#999`; no tick labels (shape comparison only).
- **Left panel:** origin x=55, width 285; ellipse centered at (400, 600), rotated −30° in screen coordinates, semi-axes 120×48 px, fill `rgba(42,120,214,0.12)`, border 1.5px `rgba(42,120,214,0.5)`; staircase (hardcoded) `[[350,540],[430,540],[430,640],[390,640],[390,580],[450,580],[450,660],[410,660]]`, blue `#2a78d6` 2.5px polyline with 4px dots; heading bold 12px ink `#1a5276` "correlation 0.8: one-knob spread $30"; blue bold 12px annotation "big strides cover the zone".
- **Right panel:** origin x=400, width 285; narrow ridge ellipse centered at (400, 600), same −30° rotation, semi-axes 130×14 px, fill `rgba(213,81,129,0.12)`, border 1.5px `rgba(213,81,129,0.5)`; staircase (hardcoded) `[[330,520],[345,520],[345,545],[360,545],[360,560],[370,560],[370,575],[385,575],[385,590],[395,590],[395,600],[405,600]]`, magenta `#d55181` 2.5px polyline with 3px dots; heading "correlation 0.98: one-knob spread ~$10"; magenta bold 12px annotation "eleven moves, barely anywhere".
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=360 from y=38 to h-12.
- **Caption (12px `#444`, bottom center):** "the tight ridge forces tiny axis-aligned steps — the chain mixes slowly (illustrative)".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (here it hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all walk points, trace values, bell parameters, and ellipse geometry above are hardcoded literals — no `Math.random()` anywhere; the staircase sequences are fixed illustrative draws.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
