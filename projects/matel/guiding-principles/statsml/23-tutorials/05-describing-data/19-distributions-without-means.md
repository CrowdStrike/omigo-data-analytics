# Distributions Without Means

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Distributions Without Means

**Subtitle:** Some data is so heavy-tailed that the average has no target to settle on — collect ten thousand points and the mean is still as wild as the first one; the Cauchy distribution is the classic case

## A Spinning Laser and a Very Long Wall

**Tags:** `core idea` (blue), `heavy tails` (orange), `Cauchy` (green)

- **The setup** — a laser 1 m from a long wall spins to a random angle; we mark where the dot lands
- **Most spins** — small angles land near center: a 30° spin hits 0.58 m out, a 60° spin hits 1.73 m
- **The rare spin** — near-parallel beams fly: 85° lands 11.4 m away and 89.9° lands 573 m away
- **The shape** — landing spots follow the Cauchy distribution: a bell-like middle with monster tails
- **The definition** — a distribution has no mean when tails are so heavy the balancing sum diverges

*Example (italic):* In 15 recorded spins, 11 dots landed within 2 m of center — but one near-parallel spin put its dot 38 m down the wall.

**Key point:** The Cauchy curve looks like a slightly wide bell, yet its tails carry so much probability that the balance point — the mean — does not exist. The wall's center is still the median.

### Visualization (canvas `c1`, 720×300)

Top half: geometry diagram of the spinning laser and the wall with four sample rays; bottom half: a number-line strip of 15 hardcoded landing spots.

- **Title (bold 15px, `#1a5276`, top center):** "One Spinning Laser: Where 15 Dots Landed on the Wall".
- **Wall:** 2px ink `#1a5276` horizontal line at y=60 from x=70 to x=650, labeled "wall" 12px `#444` at its left end.
- **Laser:** blue `#2a78d6` 6px dot at (270, 170), label 12px `#444` below: "laser, 1 m from wall". Scale: 1 m = 30 px along the wall, center directly above the laser at x=270.
- **Rays:** four 1.5px lines from the laser to wall hits — 0° to x=270, 30° to x=287, 60° to x=322 (all blue `#2a78d6`), 85° to x=612 (orange `#d95926`); angle labels 12px near the laser; hit labels 11px `#444` above the wall: "0 m", "0.58 m", "1.73 m", "11.4 m".
- **Annotation (orange `#d95926` bold 12px, right of the 85° hit):** "89.9° would land 573 m →".
- **Landing strip:** 2px `#999` number line at y=250 from x=70 to x=650 spanning −5 m to +5 m (center x=360, 58 px per m), ticks with labels "−5", "0", "+5" (12px `#444`); heading bold 12px `#444` above left: "15 landing spots (m from center)".
- **Data:** landings `[-4.8, -1.6, -0.7, -0.4, -0.2, -0.1, 0.0, 0.1, 0.15, 0.3, 0.5, 1.1, 2.4, 9.5, 38.0]`; plot the 13 in-range values as blue `#2a78d6` 5px dots on the strip.
- **Annotation (magenta `#d55181` bold 12px, near the strip's right end with a small arrow):** "9.5 m and 38 m: off this chart →".

## Watching the Average Refuse to Settle

**Tags:** `worked example` (blue), `running average` (green), `law of large numbers` (orange)

- **The experiment** — 200 spins; after each one we recompute the average landing spot so far
- **The control** — 200 dice rolls averaged the same way settle near 3.5 by roll 60 and stay put
- **Spin 42** — one dot lands 105 m out and the running average leaps from 0.08 m to 2.6 m at once
- **Slow drift** — the average creeps back for 120 spins, then spin 163 lands −260 m and wrecks it
- **No target** — the laser average has nothing to converge to; new wild spins arrive forever

*Example (italic):* After 200 spins the "average landing spot" reads −0.68 m — one more near-parallel spin could push it past +5 m.

**Key point:** The law of large numbers promises a settling average only when the mean exists. Cauchy data breaks the promise: the average after 200 spins jumps as wildly as the average after 10.

### Visualization (canvas `c2`, 720×300)

Dual-panel running-average line chart: dice rolls converging (left) vs laser spins never converging (right), split by a vertical dashed divider at x=360. All paths are fixed illustrative arrays — no randomness.

- **Title (bold 15px, `#1a5276`, top center):** "Running Average: 200 Dice Rolls vs 200 Laser Spins (illustrative)".
- **Left panel (dice):** axis origin x=55, width 280, baseline y=245, chart height 185; x = roll count 1–200, y range 3.0–6.2; hardcoded path points (n, avg): `[[1,6.0],[5,4.2],[10,3.9],[20,3.7],[40,3.62],[60,3.44],[80,3.55],[100,3.47],[120,3.52],[140,3.49],[160,3.51],[180,3.50],[200,3.50]]`; green `#008300` 3px polyline; dashed `#6b7280` horizontal reference at y-value 3.5 labeled "true mean 3.5" (12px); green bold 12px annotation "settles by ~60 rolls"; caption 12px `#444` "dice: the average has a target".
- **Right panel (laser):** axis origin x=400, width 280, same baseline/height; x = spin count 1–200, y range −2 to 3 (meters); hardcoded path points (n, avg): `[[1,0.4],[5,-0.2],[10,0.1],[20,0.05],[30,0.12],[40,0.08],[42,2.6],[50,2.2],[70,1.6],[100,1.1],[130,0.85],[162,0.72],[163,-0.88],[170,-0.81],[200,-0.68]]`; magenta `#d55181` 3px polyline; dashed `#6b7280` horizontal reference at 0 labeled "wall center (median)" (12px); orange `#d95926` bold 12px annotations at the two jumps: "spin 42: +105 m" and "spin 163: −260 m"; caption 12px `#444` "y clipped to ±3 m; the wild spins are far off-chart".
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=360 from y=38 to h-12.

## Eight Teams, Eight Wildly Different Means

**Tags:** `where it's used` (blue), `median wins` (green), `failure mode` (red)

- **The contest** — eight teams each run 500 spins and report the average landing spot (illustrative)
- **The means** — they report 1.9, −0.4, 12.6, 0.7, −6.2, 0.3, 3.8, −22.5 m: total disagreement
- **The medians** — the same eight datasets give 0.02, −0.03, 0.01, 0.04, −0.02, 0.00, 0.03, −0.01 m
- **Real data** — ratios of two noisy numbers, extreme losses, and viral counts behave this way
- **The fix** — report medians or trimmed means; they aim at stable targets even when no mean exists

*Example (italic):* Team 3's mean of 12.6 m came from a single spin at +6,100 m; their median barely felt it.

**Key point:** When tails are this heavy, the median is the honest summary — it answers "where do typical dots land?", and all eight teams would agree on it.

### Visualization (canvas `c3`, 720×300)

Two horizontal dot strips on one shared meter scale: the eight medians collapse into one cluster at zero while the eight means scatter across 35 m.

- **Title (bold 15px, `#1a5276`, top center):** "Eight Teams, 500 Spins Each: Medians Agree, Means Don't (illustrative)".
- **Shared scale:** −25 m to +15 m mapped to x=70..650 (14.5 px per m); axis ticks with 12px `#444` labels at −25, −20, −15, −10, −5, 0, 5, 10, 15 drawn on the lower strip; thin dashed `#e5e9ef` vertical gridline at 0 m through both strips.
- **Median strip (y=105):** heading bold 12px `#444` "the eight medians (m)"; 2px `#999` line x=70..650; green `#008300` 6px dots at data `[0.02, -0.03, 0.01, 0.04, -0.02, 0.00, 0.03, -0.01]` (they overlap into one cluster at 0); green bold 13px annotation with a short pointer: "all eight within ±0.04 m".
- **Mean strip (y=195):** heading bold 12px `#444` "the eight means (m)"; 2px `#999` line x=70..650; magenta `#d55181` 6px dots at data `[1.9, -0.4, 12.6, 0.7, -6.2, 0.3, 3.8, -22.5]`; 11px `#444` value labels above each dot, alternating two label rows to avoid overlap; magenta bold 12px annotation "means span 35 m".
- **Takeaway (bold 13px green `#008300`, bottom center):** "same experiment, same n — the median is reproducible, the mean is a lottery".

## More Data Won't Save You

**Tags:** `common mistake` (red), `rule of thumb` (green)

- **The hope** — "the mean is noisy? collect more data" — the usual √n cure for sampling noise
- **Dice** — the average of n rolls has typical error 1.71/√n: 0.54 at n=10, 0.017 at n=10,000
- **Cauchy fact** — the average of n Cauchy values is Cauchy again, with the SAME spread as one value
- **No progress** — averaging 10,000 laser spins gives an estimate exactly as wild as spin #1
- **The test** — before trusting a mean, ask if a few points carry most of the sum; if so, beware

*Example (italic):* An analyst quadrupled the sample from 2,500 to 10,000 spins expecting half the noise — the average was exactly as jumpy as before.

**Common mistake:** Assuming more data always tames an average. The √n shrinkage is a theorem about finite-variance data; with Cauchy tails, n = 10,000 buys you nothing.

### Visualization (canvas `c4`, 720×300)

Line chart of the typical spread of the average vs sample size on a log-spaced x axis: the dice curve shrinks like 1/√n while the Cauchy line stays flat at 1 m.

- **Title (bold 15px, `#1a5276`, top center):** "Typical Spread of the Average vs Sample Size".
- **Axes:** origin x=70, width 580, baseline y=245, chart height 185; x ticks evenly spaced at n = 1, 10, 100, 1,000, 10,000 with 12px `#444` labels; y linear 0–1.8 with ticks 0, 0.5, 1.0, 1.5 and 12px label "spread of the average (m)" rotated on the left.
- **Dice line:** green `#008300` 3px polyline with 4px dots at spreads `[1.71, 0.54, 0.17, 0.054, 0.017]` for the five n values; green bold 12px label near the curve: "dice: shrinks like 1/√n".
- **Cauchy line:** magenta `#d55181` 3px polyline with 4px dots at spreads `[1, 1, 1, 1, 1]`; magenta bold 13px label above the line: "laser (Cauchy): never shrinks".
- **Annotation (orange `#d95926` bold 13px, right side with a short pointer to the flat line at n=10,000):** "10,000 spins = as wild as 1 spin".
- **Caption (12px `#444`, bottom center):** "dice spread = 1.71/√n; the average of n Cauchy values keeps half-width 1 m at every n".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (here it hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data integrity:** all arrays above are fixed illustrative values — no `Math.random()`; the running-average paths in c2 are literal (n, avg) point lists drawn as-is.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
