# The F1 Score

**Page type:** detail page (tutorial card-sections: h2 per section, two-column table.layout with text left 50%, canvas right 50%)
**HTML title tag:** The F1 Score

**Subtitle:** One number that merges precision and recall — but it is the harmonic mean, which drops fast when the two disagree

## Two Scores Squeezed Into One

**Tags:** `core idea` (blue), `running example` (green)

- **The tuned fraud model** — catches 40 of the 50 frauds in our 1,000 transactions: recall 80%
- **Its flags** — about 3 of every 5 alerts turn out to be real fraud: precision 60%
- **The naive merge** — (60 + 80) / 2 = 70 — the plain average
- **The F1 score** — 2 × 60 × 80 / (60 + 80) = 68.6 — a bit lower
- **Why lower** — F1 is the harmonic mean, which leans toward the weaker score
- **Equal only at a tie** — precision 70 and recall 70 give 70 under both means

*Example:* Precision 60 and recall 80 average to 70 the naive way — and to 68.6 the F1 way.

**Key point:** F1 is the harmonic mean of precision and recall — always at or below the plain average, equal only when the two tie.

### Visualization (canvas `c1`, 720×300)

Four-bar column chart comparing precision, recall, plain average, and F1.

- **Title (bold 15px, `#1a5276`, top center):** "Precision 60 + Recall 80: Where Should \"One Number\" Land?".
- **Data:** labels `["precision", "recall", "plain average", "F1 (harmonic)"]`, values `[60, 80, 70, 68.6]` (%), bar colors `[#d95926 orange, #2a78d6 blue, #6b7280 mute gray, #4a3aa7 violet]`.
- **Axes:** padding top 56, bottom 56, left 70, right 30; y 0–105 with tick labels at 0, 50, 100 (gray 12px) and light gridlines `#e5e9ef`; L-shaped `#999` axes. Bars 100px wide, evenly gapped; value labels bold 13px `#2c3e50` above each bar, category labels 12px below.
- **Reference line:** dashed gray (`#6b7280`, dash 6/4, width 1.5) at y=70 spanning from just left of the third bar to the right edge of the plot.
- **Annotation (violet `#4a3aa7` bold 13px, centered near top of plot):** "the harmonic mean lands 1.4 points below the plain average".

## The Arithmetic, Step by Step

**Tags:** `worked example` (green), `core idea` (blue)

- **Multiply** — 2 × 0.60 × 0.80 = 0.96
- **Add** — 0.60 + 0.80 = 1.40
- **Divide** — 0.96 / 1.40 = 0.686 → F1 = 68.6%
- **Widen the gap** — (50, 90) also averages 70, but F1 = 64.3%
- **Widen it more** — (40, 100) still averages 70; F1 drops to 57.1%

*Example:* Four models all "average 70": their F1s are 70, 68.6, 64.3, 57.1 as the gap widens.

**Key point:** Hold the plain average fixed and every point of imbalance between precision and recall pulls F1 further down.

### Visualization (canvas `c2`, 720×300)

Grouped bar chart: four precision/recall pairs (all averaging 70) with a violet F1 marker line across each group.

- **Title (bold 15px, `#1a5276`, top center):** "Same Plain Average (70) Every Time — F1 Slides Down".
- **Data:** pairs `[[70,70], [60,80], [50,90], [40,100]]` (precision/recall), F1 values `[70, 68.6, 64.3, 57.1]`.
- **Axes:** padding top 60, bottom 66, left 70, right 160; y 0–105, ticks at 0, 50, 100 with gridlines `#e5e9ef`; L-shaped `#999` axes.
- **Bars:** per group, precision bar orange `#d95926` and recall bar blue `#2a78d6`, each 30px wide, 8px apart, value labels 12px `#2c3e50` above; group x-axis labels (gray 12px) "70 / 70", "60 / 80", "50 / 90", "40 / 100".
- **F1 markers:** thick violet `#4a3aa7` horizontal line (width 3) spanning each bar pair at its F1 height, labeled below in bold violet 12px: "F1 70", "F1 68.6", "F1 64.3", "F1 57.1".
- **Legend (top right, x=w-148):** orange swatch "precision", blue swatch "recall", violet line swatch "F1".
- **Annotations (centered):** violet bold 13px at bottom: "70 → 68.6 → 64.3 → 57.1: the wider the gap, the harder F1 punishes"; gray 12px below the x labels: "precision / recall pairs — every pair sums to 140".

## Why Punish Lopsidedness At All?

**Tags:** `where it's used` (blue), `gaming` (orange)

- **The cheat** — flag all 1,000 transactions: recall 100% (all 50 caught), precision 5%
- **Plain average** — (100 + 5) / 2 = 52.5 — sounds like a mediocre-but-OK model
- **F1** — 2 × 100 × 5 / 105 = 9.5% — correctly calls it junk
- **Both must work** — fraud review needs alarms that are real AND fraud that gets caught
- **Leaderboards** — a model tuned to max out one metric cannot game F1 as easily

*Example:* The flag-everything model "catches all fraud" — and buries the team under 950 false alarms.

**Key point:** The harmonic mean makes one terrible score fatal — recall 100% cannot buy back precision 5%.

### Visualization (canvas `c3`, 720×300)

Two side-by-side four-bar panels (tuned model vs flag-everything cheat), separated by a vertical dashed divider.

- **Title (bold 15px, `#1a5276`, top center):** "The Flag-Everything Cheat: Plain Average 52.5, F1 9.5".
- **Divider:** vertical dashed line (`#bdc3c7`, dash 4/3) at x=360 from y=40 to y=h-14.
- **Panels:** each 300px wide (left at x=30, right at x=390), baseline y=226, height scale 140px per 105; four bars per panel (58px wide) labeled "precision", "recall", "plain avg", "F1" (11px) in colors orange `#d95926`, blue `#2a78d6`, gray `#6b7280`, violet `#4a3aa7`; value labels bold 12px `#2c3e50` above bars; thin `#999` baseline.
  - Left panel title (bold 13px `#1a5276`): "tuned model (60 / 80)", values `[60, 80, 70, 68.6]`.
  - Right panel title: "flag all 1,000 (5 / 100)", values `[5, 100, 52.5, 9.5]`.
- **Takeaway (red `#e74c3c` bold 13px, centered, y=274):** "the plain average calls the cheat \"52.5\" — F1 calls it what it is: 9.5".

## When One Number Helps — and When It Hurts

**Tags:** `common mistake` (red), `trade-off` (orange)

- **Helps** — comparing many models quickly, tuning a threshold, one line in a report
- **Hides the mix** — (precision 90, recall 50) and (precision 50, recall 90) both give F1 64.3%
- **Ignores costs** — F1 weighs a missed fraud and a false alarm equally; your business doesn't
- **Ignores true negatives** — the 890 correctly-passed transactions never enter the formula
- **When to skip it** — if one error is far costlier, tune for that error, not for F1

*Example:* Two models tie at F1 64.3% — one misses half the fraud, the other floods analysts with alarms.

**Common mistake:** Treating F1 as a verdict — it is a summary; check the two numbers behind it before trusting any F1.

### Visualization (canvas `c4`, 720×300)

Grouped bar chart of two models with identical F1 but opposite precision/recall mixes, with a shared dashed F1 line.

- **Title (bold 15px, `#1a5276`, top center):** "Identical F1 = 64.3, Opposite Failure Modes".
- **Axes:** padding top 60, bottom 60, left 70, right 160; y 0–105, ticks at 0, 50, 100 with gridlines `#e5e9ef`; L-shaped `#999` axes.
- **Data:** model A "model A: picky" (sub-label "misses half the fraud") P=90, R=50; model B "model B: trigger-happy" (sub-label "floods analysts with alarms") P=50, R=90.
- **Bars:** per group, precision bar orange `#d95926` and recall bar blue `#2a78d6`, 70px wide, 12px apart; value labels bold 12px above ("P 90", "R 50", "P 50", "R 90"); group name bold 12px `#1a5276` and sub-label gray 12px below the baseline.
- **F1 line:** dashed violet `#4a3aa7` (dash 7/4, width 2) across the whole plot at y=64.3, labeled to the right in bold violet 12px: "F1 = 64.3 for both".
- **Legend (top right, x=w-148):** orange swatch "precision", blue swatch "recall".
- **Annotation (red `#e74c3c` bold 13px, centered above the plot):** "one summary number cannot tell these two models apart".

## Regeneration instructions

- **Template:** tutorial detail page (tutorials/ style). h1 (no index number) with 2px `#2980b9` bottom border, `.subtitle` paragraph, then four `.card-section` blocks. Each section: `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one `<tr>`: left `<td class="text-col">` (50%) holding `.tags` pills, a `<ul>` of one-line bullets with `<b>` lead terms, an italic `.example` paragraph, and a `.key-point` callout; right `<td class="viz-col">` (50%) holding one canvas.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; ul 0.92rem; `li b` in `#1a5276`; `.example` italic `#555` 0.9rem; `.key-point` background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem; canvases `width:100%`, `1px solid #e0e0e0` border, 4px radius.
- **Tag pills:** `.tag` inline-block, 0.72rem bold, padding 2px 10px, radius 10px; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Chart palette (JS object P):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- **Canvas:** intrinsic 720×300 attributes; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates; all data hardcoded literal arrays (no Math.random). Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- In regenerated HTML, any card links use `.html` extensions. No nav bar, no back/home links.
