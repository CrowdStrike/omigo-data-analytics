# The EM Algorithm

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** The EM Algorithm

**Subtitle:** When the labels are missing, guess them, refit, and repeat — EM turns a chicken-and-egg problem into a loop that reliably settles

## One Till, Two Kinds of Customers

**Tags:** `core idea` (blue), `hidden labels` (orange), `mixture` (green)

- **The shop** — a coffee till logs 11 spends: $3, $4, $4, $5, $5, $8, $12, $13, $14, $15, $16
- **Two crowds** — quick regulars and brunching tourists made these, but nobody wrote down who
- **The catch** — to average each group you need the labels; to guess labels you need the averages
- **The trick** — EM breaks the circle: guess the averages, label softly, re-average, repeat
- **The start** — we begin with two blind guesses: group A at $9 and group B at $11

*Example (italic):* The $4 receipts scream "regular" and the $15 ones scream "tourist" — only the $8 receipt is genuinely ambiguous.

**Key point:** EM (Expectation–Maximization) is for learning when part of the data — here, who bought what — is missing. It alternates between guessing the missing labels and refitting the group averages.

### Visualization (canvas `c1`, 720×300)

Dot-strip number line of the 11 unlabeled spends with the two initial mean guesses marked as dashed vertical lines.

- **Title (bold 15px, `#1a5276`, top center):** "One Column of Spends, No Labels: 11 Coffee Receipts".
- **Data:** spends `[3, 4, 4, 5, 5, 8, 12, 13, 14, 15, 16]`; initial guesses A = 9, B = 11.
- **Axis:** horizontal 2px `#999` line at y=175 from x=60, width 600; dollar scale $0–$18 with ticks and 12px `#444` labels at $0, $3, $6, $9, $12, $15, $18.
- **Dots:** 7px radius, gray `#6b7280` fill (unlabeled), centered on the axis; duplicate values ($4, $4 and $5, $5) stacked vertically 16px apart.
- **Guess markers:** dashed (dash 4/3) vertical lines from y=60 to y=175 at $9 in blue `#2a78d6` and $11 in orange `#d95926`; bold 12px labels above: "guess A = $9" (blue), "guess B = $11" (orange).
- **Annotations:** ink `#1a5276` bold 13px labels "regulars?" above the $3–$8 cluster and "tourists?" above the $12–$16 cluster; magenta `#d55181` bold 12px callout with a short arrow pointing at the $8 dot: "$8 — which crowd?".
- **Caption (12px `#444`, bottom center):** "same till, two hidden crowds (illustrative)".

## Score, Then Re-Average: One Full Loop

**Tags:** `worked example` (blue), `E-step` (green), `M-step` (orange)

- **E-step (score)** — guesses $9, $11 give each receipt an illustrative group-A share: $3 → 0.90, $16 → 0.05
- **Torn receipt** — the $8 spend sits between the guesses and splits almost evenly: 0.55 vs 0.45
- **Soft labels** — no receipt is forced to pick a side; ambiguous ones simply split their vote
- **M-step (re-average)** — weighted means using the shares move A to $5.5 and B to $12.5
- **By hand** — group A's new mean = sum(share × spend) ÷ sum(share) ≈ 30.4 ÷ 5.5 ≈ $5.5

*Example (italic):* One loop already pulled the guesses from ($9, $11) out to ($5.5, $12.5) — each mean got dragged toward its own crowd.

**Key point:** E-step = score every row against the current guesses; M-step = refit the guesses using those scores as weights. That is the entire algorithm.

### Visualization (canvas `c2`, 720×300)

Eleven stacked bars, one per receipt, showing each receipt's soft split between group A (bottom, blue) and group B (top, orange) after the first E-step.

- **Title (bold 15px, `#1a5276`, top center):** "E-Step Scores from Guesses ($9, $11): Each Receipt's Group-A Share".
- **Data:** bar labels `["$3","$4","$4","$5","$5","$8","$12","$13","$14","$15","$16"]`; group-A shares `[0.90, 0.88, 0.88, 0.85, 0.85, 0.55, 0.20, 0.15, 0.10, 0.08, 0.05]` (rounded illustrative scores, not exact Gaussian responsibilities); group-B share = 1 − A share.
- **Layout:** origin x=60, baseline y=240, chart height 175, y scale 0–1 with a `#e5e9ef` gridline and 12px `#444` label at 0.5; 11 bars, width 40, gap 14; receipt labels 12px `#444` below each bar.
- **Bars:** bottom segment (A share) fill `rgba(42,120,214,0.55)`, top segment (B share) fill `rgba(217,89,38,0.5)`; each bar's A share printed bold 11px blue `#2a78d6` just above the bar.
- **Annotation:** magenta `#d55181` bold 13px above the $8 bar: "torn: 0.55 / 0.45".
- **Legend (top right, 12px `#444`):** blue swatch "group A share", orange swatch "group B share".
- **Caption (12px `#444`, bottom center):** "M-step next: weighted means move A to $5.5 and B to $12.5"; muted 12px `#6b7280` note bottom right: "shares illustrative".

## Watching the Guesses Settle

**Tags:** `convergence` (green), `where it's used` (blue)

- **Group A's walk** — $9.0 → $5.5 → $4.8 → $4.6 → $4.6; when the mean stops moving, EM stops
- **Group B's walk** — $11.0 → $12.5 → $13.3 → $13.6 → $13.6, settling on the tourist crowd
- **Guarantee** — every loop makes the data look at least as likely as before; the fit never gets worse
- **Stopping** — quit when the means barely move between loops; here loop 3 → 4 changes nothing
- **Everywhere** — the same loop powers mixture models, hidden Markov models, and missing-data fills

*Example (italic):* By loop 2 the means ($4.8, $13.3) are already close to the final answer — the first loop does most of the work.

**Key point:** EM always climbs — each iteration improves (or ties) the likelihood — which is why "guess, fit, repeat" reliably settles somewhere instead of wandering forever.

### Visualization (canvas `c3`, 720×300)

Two-line convergence chart: both group means across EM loops 0–4, flattening onto their final values.

- **Title (bold 15px, `#1a5276`, top center):** "The Two Means Across EM Loops".
- **Data:** loops `[0, 1, 2, 3, 4]`; mean A `[9.0, 5.5, 4.8, 4.6, 4.6]`; mean B `[11.0, 12.5, 13.3, 13.6, 13.6]`.
- **Axes:** origin x=70, baseline y=245, width 560, chart height 190; y scale $0–$16 with 12px `#444` tick labels at $0, $4, $8, $12, $16; x labels 12px `#444`: "start", "1", "2", "3", "4".
- **Lines:** mean A blue `#2a78d6` 3px with 4px dots; mean B green `#008300` 3px with 4px dots; 11px value labels beside each dot.
- **Reference lines:** dashed `#e5e9ef` horizontal lines at the final levels $4.6 and $13.6.
- **Annotations:** green bold 13px near loops 3–4: "loop 3 → 4: nothing moves — stop"; ink `#1a5276` bold 12px upper middle: "each loop can only improve the fit".
- **Caption (12px `#444`, bottom center):** "means converge to $4.6 and $13.6 (illustrative soft-EM trace)".

## Settling Is Not the Same as Solving

**Tags:** `common mistake` (red), `local optimum` (orange), `rule of thumb` (green)

- **Local traps** — EM climbs to the nearest hilltop, not the highest one; where you start matters
- **A bad start** — guesses of $3 and $10.50 settle at $3.5 and $9.8, never finding the tourists
- **The fix** — run EM from several different starts and keep the answer with the best likelihood
- **Soft vs hard** — k-means is EM's hard-label cousin; it forces the torn $8 receipt to pick a side
- **Not magic** — EM never sees the true labels; it only finds groupings the numbers can support

*Example (italic):* Both runs report "converged", but ($3.5, $9.8) fits the receipts far worse than ($4.6, $13.6) — only the likelihood tells you which to trust.

**Common mistake:** Trusting the first converged answer. Convergence means "stopped moving", not "found the truth" — compare several starts before believing the groups.

### Visualization (canvas `c4`, 720×300)

Dual-panel convergence chart: the good start (left) and the bad start (right) on the same data, split by a vertical dashed divider at x=360.

- **Title (bold 15px, `#1a5276`, top center):** "Same Data, Two Starts, Two Different Endings".
- **Left panel (good start):** origin x=55, width 280, baseline y=240, chart height 170, y scale $0–$16; mean A `[9.0, 5.5, 4.8, 4.6, 4.6]` blue `#2a78d6` 3px line with 4px dots; mean B `[11.0, 12.5, 13.3, 13.6, 13.6]` green `#008300` 3px line with dots; heading bold 12px `#444` "start at ($9, $11)"; green bold 12px annotation "finds both crowds: $4.6 / $13.6"; x labels "start", 1–4.
- **Right panel (bad start):** origin x=400, width 280, same baseline/height/scale; mean A `[3.0, 3.3, 3.5, 3.5, 3.5]` violet `#4a3aa7` 3px line with dots; mean B `[10.5, 10.0, 9.8, 9.8, 9.8]` orange `#d95926` 3px line with dots; heading bold 12px `#444` "start at ($3, $10.50)"; magenta `#d55181` bold 12px annotation, two lines: "converged, but wrong:" / "$3.5 / $9.8 — tourists never found".
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=360 from y=38 to h-12.
- **Caption (12px `#444`, bottom center):** "both runs converge; only one finds the real groups (illustrative)".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (here it hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
