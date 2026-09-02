# Monte Carlo & Ensemble Methods

**Page type:** detail page (four titled sections, each a two-column row: text left 50%, canvas right 50%)
**HTML title tag:** Monte Carlo & Ensemble Methods — The Statistical Engine Under Every Simulation

**Subtitle:** Hurricane cones, epidemic fans, and hazard maps are all the same move — sample what you don't know, run the model many times, and report the distribution instead of a number.

**Intro callout (blue-left-border box):** Every serious simulation of nature runs many times, not once. Monte Carlo methods were born in 1940s weapons research, when the deterministic equations of neutron transport were intractable and the only way forward was to follow thousands of random particles and count what happened. The trick generalizes to everything: replace an unsolvable integral over uncertainty with brute-force sampling, and the pile of outputs becomes the answer.

## 1. The core move: sample, run, aggregate

When uncertainty cannot be propagated through a model analytically, sample it instead — draw random inputs, run the model once per draw, and let the collection of outputs be the answer.

- **Intractable integrals:** Real models are too nonlinear to push uncertainty through by hand.
- **Sample the unknowns:** Each uncertain input is drawn at random from its distribution.
- **Run per draw:** Every draw gets one complete run of the full model.
- **Outputs are the answer:** The collection of results is the product, not any single run.
- **Distribution, not point:** The method returns a spread of futures instead of one number.
- **Statistics by counting:** Means, percentiles, and tail probabilities are just counts over outputs.
- **1940s origin:** The method was invented when weapons equations defeated analysis.

Key point: Monte Carlo replaces an unsolvable integral over uncertainty with brute-force sampling — once the outputs are collected, any statistic you want is a matter of counting.

### Visualization (canvas `c1`, 720×340)

Pipeline: input distribution with sampled tick marks on the left, simulation box in the center, output histogram with a percentile marker on the right, triple arrows showing many draws flowing through.

- **Title (bold 14px `#1a5276`, centered, y=22):** "Sample the inputs, run the model, count the outputs"
- **Input panel (left):** header bold 12px `#1a5276` centered at (130, 62): "INPUT UNCERTAINTY". Baseline 1.5px `#999` line at y=240 from x=40 to x=220. Gaussian curve 2px `#1a5276`: ~180-step polyline of `y = 240 − 110 × exp(−((x−130)/40)²)` for x in 40–220. Sampled draws: nine 1.5px `#e67e22` vertical ticks 10px tall rising from the baseline at x = 85, 105, 118, 126, 133, 141, 152, 168, 190. Sub-label 11px `#666` centered at (130, 262): "random draws from the distribution".
- **Model box:** 150×80 at (285, 160), fill `rgba(26,82,118,0.12)`, 2px `#1a5276` border; bold 12px `#1a5276` centered "SIMULATION" at (360, 192); 11px `#666` centered "full model run" at (360, 210); 11px `#999` centered "once per draw" at (360, 226).
- **Arrows:** three parallel 1.5px `#999` arrows with filled arrowheads at y=185, 200, 215 — left set from x=230 to x=280, right set from x=440 to x=485; 10px `#999` centered label "N draws" at (255, 172) and "N outputs" at (462, 172).
- **Output panel (right):** header bold 12px `#27ae60` centered at (585, 62): "OUTPUT DISTRIBUTION". Baseline 1.5px `#999` line at y=240 from x=490 to x=680. Eight histogram bars width 20, gap 3, starting x=492, heights [12, 30, 62, 105, 88, 50, 24, 10], fill `rgba(26,82,118,0.35)`, 1px `#1a5276` stroke, tops at 240 − height. Percentile marker: dashed (4/3) 1.5px `#e74c3c` vertical line at x=652 from y=240 to y=150; bold 10px `#e74c3c` centered "95th pct" at (652, 142). Sub-label 11px `#666` centered at (585, 262): "any statistic is just counting".
- **Caption (12px `#999`, centered, y = h−14):** "The distribution of outputs IS the answer — a point forecast is a summary of it, not a substitute"

## 2. Where the randomness enters

An ensemble's spread is only as complete as the uncertainty sources it samples — and randomness enters a simulation through four distinct doors.

- **Initial conditions:** Weather ensembles perturb today's measured state slightly per member.
- **Parameters:** Epidemic ensembles draw β and γ from their plausible ranges.
- **Stochastic dynamics:** Who infects whom is a fresh coin flip inside every run.
- **Model structure:** Multi-model ensembles run entirely different models side by side.
- **Missing doors narrow fans:** A spread that ignores one door understates total uncertainty.
- **Data helps unevenly:** More data shrinks parameter uncertainty but not structural doubt.
- **Audit the sources:** Always ask which of the four doors a published fan actually sampled.

Key point: A fan that samples only one door is a lower bound on the real uncertainty — and while parameter uncertainty shrinks as data accumulates, structural uncertainty does not.

### Visualization (canvas `c2`, 720×400)

Four labeled source boxes across the top, each with a small icon-style sketch, feeding one ensemble box below via converging arrows, with a red under-statement warning underneath.

- **Title (bold 14px `#1a5276`, centered, y=22):** "Four doors where randomness enters the simulation"
- **Source boxes:** four 150×110 rectangles at x = 30, 205, 380, 555, top y=50, white fill, 1.5px borders in `#1a5276`, `#27ae60`, `#e67e22`, `#8e44ad` respectively. Each: bold 11px centered header in the border color at (cx, y+20); icon sketch in the region y+30 to y+80; 10px `#666` centered description at (cx, y+98), where cx is the box center.
  - **Box 1 "INITIAL CONDITIONS" (`#1a5276`):** filled `#1a5276` circle radius 4 at (105, 108); six filled `rgba(26,82,118,0.35)` circles radius 2.5 perturbed around it at (93, 100), (117, 102), (98, 118), (114, 116), (105, 94), (110, 122). Description: "perturb today's state".
  - **Box 2 "PARAMETERS" (`#27ae60`):** dial — 2px `#27ae60` semicircle arc radius 22 centered (280, 118) from π to 2π; 2px `#27ae60` needle line from (280, 118) to (292, 100); filled `#27ae60` hub circle radius 3 at (280, 118). Description: "draw β, γ from ranges".
  - **Box 3 "STOCHASTIC DYNAMICS" (`#e67e22`):** coin — 2px `#e67e22` circle radius 16 at (455, 108); bold 14px `#e67e22` centered "?" at (455, 113). Description: "coin flips inside each run".
  - **Box 4 "MODEL STRUCTURE" (`#8e44ad`):** two model shapes — 1.5px `#8e44ad` rectangle 30×20 at (595, 96); 1.5px `#8e44ad` triangle with vertices (645, 116), (660, 90), (675, 116). Description: "different models entirely".
- **Connectors:** 1.5px `#999` lines with filled arrowheads from each box's bottom-center (cx, 160) to the ensemble box top edge at (300, 230), (340, 230), (380, 230), (420, 230) respectively.
- **Ensemble box:** 220×70 at (250, 230), fill `rgba(26,82,118,0.12)`, 2px `#1a5276` border; bold 12px `#1a5276` centered "ENSEMBLE OF RUNS" at (360, 260); 11px `#666` centered "each member samples all four doors" at (360, 282).
- **Warning:** bold 11px `#e74c3c` centered at (360, 336): "ignore any one door and the spread understates total uncertainty"; 11px `#666` centered at (360, 354): "more data shrinks parameter uncertainty — structural uncertainty stays".
- **Caption (12px `#999`, centered, y = h−14):** "Initial conditions, parameters, chance, and model choice each widen the fan for a different reason"

## 3. Reading an ensemble honestly

An ensemble is a poll of plausible futures, and reading it well means using the whole distribution rather than collapsing it back into a single line.

- **Mean smooths extremes:** The ensemble mean is the best single guess but flattens the tails.
- **Spread is confidence:** A tight fan earns trust and a wide fan warns against it.
- **Probability by counting:** Event probability is the fraction of members showing the event.
- **Bands beat the line:** Percentile bands communicate far more than the mean alone.
- **Extremes are signal:** The worst member is information about the tail, not noise.
- **Members are scenarios:** Each member is one internally coherent future, not random scatter.

Key point: The worst ensemble member is not an error to discard — it is exactly the tail risk the ensemble was built to reveal, and deleting it recreates the false certainty the method exists to prevent.

### Visualization (canvas `c3`, 720×380)

Fan chart: observed history entering from the left, a median line, shaded 50% and 90% bands widening with lead time, faint individual member lines, and one extreme member highlighted with an annotation.

- **Title (bold 14px `#1a5276`, centered, y=22):** "The fan is the forecast — median, bands, members, and one extreme"
- **Axis:** 1.5px `#999` horizontal line at y=300 from x=50 to x=670; 10px `#999` centered labels "today" at (100, 318) and "lead time →" at (400, 318).
- **90% band:** filled `rgba(26,82,118,0.12)` polygon from apex (100, 210): upper edge to (640, 95), lower edge to (640, 292), both slightly bowed (quadratic curves through (370, 160) and (370, 258)).
- **50% band:** filled `rgba(26,82,118,0.35)` polygon from the same apex: upper edge to (640, 155), lower edge to (640, 245), bowed through (370, 185) and (370, 232).
- **Member lines:** seven 1px `#bbb` polylines from (100, 210), each a 10-segment random-looking wiggle ending at x=640 at y = 120, 150, 175, 205, 225, 250, 275.
- **Median:** 2.5px `#1a5276` gently curved line from (100, 210) to (640, 198).
- **Observed history:** 2px `#555` polyline from (55, 222) through (70, 215), (85, 218) to (100, 210); 10px `#666` label "observed" left-aligned at (55, 205).
- **Extreme member:** 2px `#e74c3c` polyline from (100, 210) wiggling upward through (250, 170), (400, 125), (520, 95) to (640, 68); bold 11px `#e74c3c` right-aligned annotation at (635, 52): "worst member — information, not noise"; thin 1px `#e74c3c` connector from (640, 58) to (615, 76).
- **Band labels:** 10px `#1a5276` left-aligned "50% band" at (648, 200) and "90% band" at (648, 100).
- **Caption (12px `#999`, centered, y = h−14):** "Tight fan means trust, wide fan means don't — event probability is the share of members showing it"

## 4. How many runs — and the classic traps

Monte Carlo precision is bought with runs at a brutal exchange rate, and two quiet traps make real fans narrower than they should be.

- **1/√N error:** Monte Carlo error shrinks as one over the square root of N.
- **Quadratic cost:** Doubling the precision costs four times the runs.
- **Rare events:** Tail probabilities need enormous N or importance-sampling tricks.
- **Under-dispersion:** Operational ensembles chronically spread too little.
- **Reality escapes the fan:** Outcomes land outside the band more often than advertised.
- **Correlation trap:** Correlated inputs sampled as independent silently narrow the spread.
- **Verify coverage:** Compare the advertised band coverage against the actual hit rate.

Key point: The 1/√N law sets the price of precision, but the cheaper failure is a fan that is confidently too narrow — independence assumptions and missing uncertainty sources shrink it silently.

### Visualization (canvas `c4`, 720×380)

Left: convergence plot of a Monte Carlo estimate versus N on a log axis, narrowing inside a 1/√N envelope toward the true value. Right inset: two bars contrasting the advertised 90% band with the actual hit rate.

- **Title (bold 14px `#1a5276`, centered, y=22):** "Error shrinks as 1/√N — and fans are narrower than advertised"
- **Convergence plot (left):** axes 1.5px `#999` — vertical from (70, 60) to (70, 300), horizontal from (70, 300) to (430, 300). X ticks 10px `#999` centered at y=316 for x = 90, 170, 250, 330, 410 labeled "10", "100", "1k", "10k", "100k"; 10px `#666` centered axis label "number of runs N (log scale)" at (250, 336).
- **True value:** dashed (4/3) 1.5px `#27ae60` horizontal line at y=190 from x=70 to x=430; 10px `#27ae60` left-aligned "true value" at (74, 182).
- **Envelope:** two 1px dashed (3/3) `#e74c3c` curves at y = 190 ± 85/√(N/10) evaluated at the five ticks — half-widths 85, 27, 8.5, 2.7, 0.9 at x = 90, 170, 250, 330, 410 — drawn as smooth polylines from (90, 105) and (90, 275) converging to (410, ~190). Bold 11px `#e74c3c` left-aligned label "± 1/√N envelope" at (150, 96).
- **Estimate path:** 2px `#1a5276` polyline through (90, 258), (130, 132), (170, 210), (210, 172), (250, 196), (290, 186), (330, 192), (370, 189), (410, 190) — jagged early, settling onto the true line.
- **Inset (right):** header bold 12px `#e67e22` centered at (575, 70): "UNDER-DISPERSIVE FANS". Baseline 1.5px `#999` at y=280 from x=480 to x=670. Dashed (4/3) 1px `#999` reference line at y=110 from x=480 to x=670 with 10px `#999` right-aligned "90% target" at (668, 104). Bar 1: 60 wide at x=500, top y=110 (height 170), fill `rgba(26,82,118,0.35)`, 1.5px `#1a5276` stroke; bold 11px `#1a5276` centered "90%" at (530, 100); 10px `#666` centered "advertised" at (530, 296). Bar 2: 60 wide at x=595, top y=133 (height 147), fill `rgba(231,76,60,0.45)`, 1.5px `#e74c3c` stroke; bold 11px `#e74c3c` centered "78%" at (625, 123); 10px `#666` centered "actual hit rate" at (625, 296). Note 10px `#666` centered at (575, 320): "reality lands outside the fan too often".
- **Caption (12px `#999`, centered, y = h−14):** "Four times the runs buys twice the precision — and a fan can still be confidently too narrow"

## Regeneration instructions

- **Template:** backlog multi-section detail page. After h1, `.subtitle`, and `.intro` callout, one `.lang-section` per numbered section: `h2` (1.3rem `#1a5276`, `border-bottom: 2px solid #2980b9`, 4px padding-bottom), then a `table.layout` with one `<tr>`: left `<td class="text-col">` (50%) with lead paragraph, bullets, `.key-point`; right `<td class="viz-col">` (50%) with the canvas. Section h2 headings carry the "1.–4." numbers shown above. The h1 carries no index number.
- **Bullet style:** each bullet is a bold label + one short complete sentence that fits on one line at normal page width; split dense content into more bullets rather than longer ones. Bullet `<strong>` labels are colored `#1a5276` (via `li strong { color: #1a5276; }`).
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with `border-bottom: 2px solid #2980b9`; `.subtitle` `#666` 0.95rem; `.intro` — background `#f0f4f8`, `border-left: 3px solid #2980b9`, padding 8px 12px, 0.9rem, 32px margin-bottom; `.key-point` — background `#f8f9fa`, `border-left: 3px solid #e74c3c`, padding 8px 12px, 0.9rem; `ul` 0.92rem; canvases `width: 100%`, `border: 1px solid #e0e0e0`, radius 4px; `.lang-section` 40px margin-bottom.
- **Canvases:** intrinsic width 720, heights 340/400/380/380 as specified; shared `setupCanvas(id, h)` helper scales by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates). Canvas fonts use `-apple-system, sans-serif`. Every canvas gets a bold 14px `#1a5276` centered title at y=22 and a 12px `#999` centered caption at y = h−14.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, purple `#8e44ad`, accent `#2980b9`; grays `#555`/`#666`/`#999`/`#bbb`/`#ccc`; translucent fills `rgba(26,82,118,0.12)`, `rgba(26,82,118,0.35)`, `rgba(231,76,60,0.45)`.
- No nav bar, no back/home links, no cross-references to other pages, no item counts in text.
