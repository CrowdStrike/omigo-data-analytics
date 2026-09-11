# Effect Size

**Page type:** detail page (tutorial: 4 card-sections, each h2 + two-column layout table, text left 50% with tag pills / canvas right 50%)
**HTML title tag:** Effect Size

**Subtitle:** Not just "is there a difference?" but "how big is it?" — the number decisions should actually rest on.

## Six Extra Points on a Test: Big or Small?

Tags: `core idea` (blue), `how big` (green)

- **The study** — a school pilots a tutoring program; tutored students average 76, others 70
- **The raw gap** — 6 points; whether that is "big" depends on how spread out students are
- **The yardstick** — student-to-student spread is 12 points (one standard deviation)
- **The ratio** — 6 ÷ 12 = 0.5: the gap is half a standard deviation, written d = 0.5
- **Reading it** — the two score bells shift by half their own width and still overlap a lot

*Example:* Six points would be enormous if students varied by 6, and trivial if they varied by 60.

**Key point:** effect size measures the gap in units of natural spread — that makes 6 points comparable across tests.

### Visualization (canvas `c1`, 720×300)

Two overlapping bell curves of test scores, means 70 and 76, spread 12.

- **Title (bold 15px, `#1a5276`, top center):** "Tutored (avg 76) vs Untutored (avg 70), Spread 12 — d = 0.5".
- **Curves:** unnormalized Gaussians drawn in region x:70, y:55, w:580, h:165 over score range 30–115 — untutored mean 70, filled `rgba(42,120,214,0.20)` stroked blue `#2a78d6` width 2.5; tutored mean 76, filled `rgba(0,131,0,0.20)` stroked green `#008300` width 2.5; both sd 12.
- **Axis:** gray `#999` baseline; tick labels at 40, 55, 70, 85, 100; axis title "test score".
- **Mean markers:** vertical dashed lines (dash 4/3, width 2) at 70 (blue) and 76 (green); labels bold 12px — "untutored: 70" (blue, right-aligned left of its line), "tutored: 76" (green, left-aligned right of its line).
- **Takeaway (bold 13px violet `#4a3aa7`, centered, y=282):** "the gap is 6 points = half the 12-point spread: a real shift, big overlap".

## Cohen's d by Hand: One Division, Three Scenarios

Tags: `worked example` (green), `Cohen's d` (blue)

- **The recipe** — d = (group average A − group average B) ÷ standard deviation
- **Our pilot** — d = (76 − 70) ÷ 12 = 0.5
- **Noisier test** — same 6-point gap, spread 30: d = 6 ÷ 30 = 0.2, barely visible
- **Tighter test** — same 6-point gap, spread 6: d = 6 ÷ 6 = 1.0, unmistakable
- **Benchmarks** — Cohen's rough labels: 0.2 small, 0.5 medium, 0.8 large

*Example:* One identical 6-point gap earns three different verdicts — the spread does the judging.

**Key point:** the same raw difference can be a whisper or a shout; d tells you which by dividing by the noise.

### Visualization (canvas `c2`, 720×300)

Three small-multiple panels: the same 6-point gap (means 70 vs 76) under three spreads.

- **Title (bold 15px, `#1a5276`, top center):** "One 6-Point Gap, Three Spreads, Three Verdicts".
- **Panels** (each region 195×140, y=60; panel x origins 40, 265, 490; x range 70−2.6×sd to 76+2.6×sd per panel (so each bell pair fits its own panel); blue/green bell pair as in c1 at 0.18 alpha fills):
  - spread 30 → big overlap; panel label "d = 0.2" (bold 20px `#1a5276`, y=236) with 12px sublabel "spread 30 → d = 0.2 (small)"
  - spread 12 → "d = 0.5", sublabel "spread 12 → d = 0.5 (medium)"
  - spread 6 → "d = 1.0", sublabel "spread 6 → d = 1.0 (large)"
- **Corner note (11px gray `#6b7280`, top-left, y=46):** "averages 70 vs 76 in every panel".
- **Takeaway (bold 13px orange `#d95926`, centered, y=288):** "d = 6 ÷ spread — the division is the whole formula".

## From d to a Decision

Tags: `where it's used` (blue), `actionability` (green)

- **Plain meaning** — at d = 0.5, the average tutored student beats 69% of untutored ones
- **Still overlap** — the two bells share about 80% of their area; many untutored score higher
- **Planning** — sample-size calculators need an expected d, not a hoped-for p
- **Comparing** — d puts a math program (d = 0.5) and a reading one (d = 0.3) on one scale
- **Budgeting** — cost per student divided by d is a rough price of impact

*Example:* "Beats 69% of the control group" convinces a school board; "p = 0.003" does not.

**Key point:** d converts into statements people can act on — percent outperformed, students needed, cost per gain.

### Visualization (canvas `c3`, 720×300)

Two-part chart: a Cohen's d benchmark ladder on top, and "beats X% of control" bars below.

- **Title (bold 15px, `#1a5276`, top center):** "What a d Value Buys You".
- **Ladder (axis y=110, x=80, width 560):** horizontal d axis 0 to 1.2 with tick labels 0.0–1.2 at 0.2 steps and axis title "Cohen's d"; radius-8 benchmark dots with bold 13px labels above: "small" at d=0.2 (gray `#6b7280`), "medium" at d=0.5 (blue `#2a78d6`), "large" at d=0.8 (green `#008300`). A violet `#4a3aa7` pointer line at d=0.5 with bold 13px label "our tutoring pilot: d = 0.5".
- **Lower band (heading bold 13px `#1a5276`, y=171):** "average treated person beats this % of the untreated group:" followed by three horizontal bars (rows start y=185, stride 32, track x=200 width 380 filled `#e5e9ef`; U3 values):
  - d = 0.2 → 58% (fill `rgba(42,120,214,0.55)`)
  - d = 0.5 → 69% (fill violet `#4a3aa7` — the highlighted pilot row)
  - d = 0.8 → 79% (fill `rgba(42,120,214,0.55)`)
  Each row labeled "d = X" (bold 12px) on the left and its percent (bold 12px) at the bar end.
- **50% reference:** vertical dashed gray `#6b7280` line (dash 4/3, width 1.5) through the bars at 50%, labeled "50% = no effect" (11px, centered below).

## Significance Can Be Bought; Effect Size Cannot

Tags: `common mistake` (red), `misreading` (orange)

- **The misread** — "p is tinier, so study B found a bigger effect" — not necessarily
- **Study A** — 100 students per group, d = 0.50: p ≈ 0.0005
- **Study B** — 500,000 users per group, d = 0.05: p < 0.0000001 — tinier p, tenth the effect
- **Why** — piling on data shrinks p toward zero for ANY real effect, however small
- **The habit** — report d (with its interval) next to every p; never let p stand alone

*Example:* Study B's p has more zeros, yet its effect would move a student about 0.6 points, not six.

**Key point:** p answers "is it real?" and rewards big samples; d answers "how big?" and cannot be inflated by n.

### Visualization (canvas `c4`, 720×300)

Two effect-size bars with p-value tags: the tinier p belongs to the far smaller effect.

- **Title (bold 15px, `#1a5276`, top center):** "Tinier p Does NOT Mean Bigger Effect".
- **Bars** (baseline y=220, width 90, height scaled d/0.6 × 150, at 0.65 alpha):
  - Study A at x=110: d = 0.50, green `#008300`; labels "d = 0.50" (bold 15px above), "Study A" (bold 13px below), "100 per group" (12px gray)
  - Study B at x=420: d = 0.05, orange `#d95926`; labels "d = 0.05", "Study B", "500,000 per group"
- **P-value tags:** beside each bar a 130×30 box filled `rgba(74,58,167,0.10)` outlined violet `#4a3aa7` width 1.5 containing bold 12px violet text — "p ≈ 0.0005" (Study A), "p < 0.0000001" (Study B).
- **Y-axis:** shared gray `#999` baseline from x=70 to x=650; rotated 12px gray label on the left: "effect size d".
- **Takeaway (bold 13px red `#e74c3c`, centered, y=282):** "Study B wins the p contest with one-tenth the effect — its sample bought the zeros".

## Regeneration instructions

- **Layout:** tutorial page — `<h1>` + `.subtitle`, then 4 `.card-section` blocks. Each has an `<h2>` (1.3rem `#1a5276`, 2px solid `#2980b9` bottom border) and a `table.layout` (width 100%, border-collapse, cell padding 12px, vertical-align top) with one row: `.text-col` (50%) and `.viz-col` (50%, containing the canvas).
- **Text cell structure:** `.tags` row of pills, then a `<ul>` of bullets each opening with `<b>bold term</b>` (bold terms colored `#1a5276`), one italic `.example` paragraph (italic, `#555`, 0.9rem), one `.key-point` callout (background `#f8f9fa`, left border 3px solid `#e74c3c`, padding 8px 12px, 0.9rem) whose "Key point:" prefix is `<strong>`. Math symbols appear as HTML entities (`&minus;`, `&divide;`) in the source.
- **Tag pills:** `.tag` inline-block, 0.72rem, weight 600, padding 2px 10px, radius 10px. Variants: blue `rgba(26,82,118,0.12)`/`#1a5276`; green `rgba(39,174,96,0.15)`/`#27ae60`; red `rgba(231,76,60,0.12)`/`#e74c3c`; orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page style:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` bottom border; subtitle `#666` 0.95rem. Canvases have `width: 100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas JS:** shared palette object `P = { blue:'#2a78d6', green:'#008300', magenta:'#d55181', yellow:'#c98500', aqua:'#199e70', orange:'#d95926', violet:'#4a3aa7', ink:'#1a5276', text:'#2c3e50', mute:'#6b7280', grid:'#e5e9ef' }`; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates; helpers `gauss(x, mu, sd)` and `drawBells(ctx, region, mu1, mu2, sd, xMin, xMax, c1, c2)` (fill-then-stroke each curve) draw the bell pairs in c1 and c2. Hardcoded literal data — no `Math.random()`. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Site palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- In regenerated HTML, any card links use `.html` extensions (this page has none).
