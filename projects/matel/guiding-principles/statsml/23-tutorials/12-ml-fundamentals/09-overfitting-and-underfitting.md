# Overfitting & Underfitting

**Page type:** detail page (tutorial: 4 card-sections; section 1 holds both canvases side by side in a `.viz-pair` flex row inside its viz cell; all sections use the two-column text/canvas layout)
**HTML title tag:** Overfitting & Underfitting

**Subtitle:** A line that misses the pattern, a curve that catches it, and a wiggle that memorizes every point — the train-vs-test gap is the fingerprint

## Three Ways to Draw Through the Same 12 Points

**Tags:** `core idea` (blue), `running example` (green)

- **The data** — 12 points that rise fast, then level off, with a little noise
- **Too simple** — a straight line cannot bend; it misses the flattening
- **About right** — a gentle curve follows the bend and ignores the jitter
- **Memorized** — a wiggle hits all 12 points exactly, noise included
- **The trap** — the wiggle has zero error on these points and is the worst of the three

*Example:* A tailor can cut a suit too loose, just right, or so tight it fits only today's lunch.

**Key point:** **Underfitting** = too simple to catch the pattern. **Overfitting** = so flexible it learns the noise too. Both are failures to generalize.

Shared data for both canvases (12 hardcoded rise-then-level points with noise):
- XS = `[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]`
- YS = `[15, 22, 33, 36, 44, 45, 51, 50, 55, 53, 58, 56]`
- Scales: x mapped over 0.5–12.5 domain, y over 0–70; padding top 44, bottom 40, left 40, right 16; L-shaped `#999` axis frame; points drawn as ink `#1a5276` dots radius 4.5.

### Visualization (canvas `c1a`, 310×300)

Scatter of the 12 points with two overlaid fits: an underfit straight line and a good gentle curve.

- **Title (bold 14px, `#1a5276`, top center):** "Too Simple vs About Right".
- **Straight line fit:** y = 19.6 + 3.62x drawn from x=0.5 to x=12.4, orange `#d95926`, width 3.
- **Gentle curve:** y = 60·(1 − e^(−x/4)) sampled at 61 points over x=0.5–12.4, green `#008300`, width 3.
- **Annotations (bold 12px):** orange, right-aligned at the plot's right edge on data rows y=24 / y=17: "line: cannot bend," / "misses the leveling-off"; green, left-aligned two lines top-left at (1, 62)/(1, 56): "curve: follows the" / "pattern, skips the jitter".
- **Caption (gray `#6b7280` 12px, bottom center of canvas):** "the same 12 training points".

### Visualization (canvas `c1b`, 310×300)

Scatter of the same 12 points with an overfit wiggle passing exactly through every point.

- **Title (bold 14px, `#1a5276`, top center):** "Memorized: Hits Every Point".
- **Wiggle:** magenta `#d55181` quadratic curve, width 2.5, through each consecutive point pair with control point at the segment midpoint offset ±11 y-units (alternating +11 / −11 by segment parity) — overshooting between points.
- **Annotation (magenta bold 12px, three lines top-left at data coords (1, 65)/(1, 59)/(1, 53)):** "zero error here —" / "every swerve is noise," / "not pattern".
- **Caption (gray 12px, bottom center of canvas):** "new points land on the curve, not the wiggle".

## The Fingerprint: Grade Each Fit Twice

**Tags:** `worked example` (green), `arithmetic` (blue)

- **Straight line** — train 72%, test 70%: both low, gap of 2
- **Gentle curve** — train 88%, test 86%: both high, gap of 2
- **Wiggle** — train 100%, test 61%: perfect at home, lost outside
- **Read the gap** — a small gap means honest learning; a 39-point gap means memorization
- **Read the level** — two low-but-close scores mean the model is too simple

*Example:* The student with 100% on practice problems and 61% on the exam memorized the answer key.

**Key point:** **The fingerprint:** overfitting is invisible on training data by definition — it only shows when train and test scores are placed side by side.

### Visualization (canvas `c2`, 720×300)

Grouped bar chart: train vs test accuracy for the three fits, with a gap bracket on the wiggle.

- **Title (bold 16px, `#1a5276`, top center):** "Train Score vs Test Score (illustrative accuracy)".
- **Groups:** "straight line", "gentle curve", "wiggle"; train values `[72, 88, 100]` in blue `#2a78d6`, test values `[70, 86, 61]` in violet `#4a3aa7`; fill alpha 0.45, 2px stroke; bar width 62, 8px between the pair, group width 170, first group x=110.
- **Axes:** y from 0 to 100%, labels every 25% (gray `#6b7280` 12px); baseline y=230, chart height 160, left pad 70; L-shaped `#999` axis.
- **Value labels:** bold 13px in bar color above each bar; group labels 13px `#2c3e50` below baseline.
- **Gap bracket:** magenta `#d55181` square bracket (width 2) to the right of the wiggle group spanning y(100%) to y(61%), with bold 13px two-line label "39-point gap =" / "memorization" centered on it.
- **Legend (top-left, x=90, y=44):** blue swatch "train", violet swatch "test" (12px).
- **Annotation (green `#008300` bold 13px, under the middle group, 44px below baseline):** "the curve wins: high AND close".

## Why It Matters: The Sweet Spot in Between

**Tags:** `where it's used` (blue), `rule of thumb` (green)

- **Turn the flexibility dial** — from a rigid line up to a wiggle that bends 10 times
- **Train error only falls** — more flexibility always fits the seen points better
- **Test error is U-shaped** — falls while real pattern is learned, rises when noise is
- **The sweet spot** — here around flexibility 4, where test error bottoms out at 12%
- **Every model has this dial** — tree depth, polynomial degree, network size, training time

*Example:* Choosing a model by training score alone always picks the wiggle — the dial only ever says "more".

**Key point:** **Rule of thumb:** never pick flexibility by training error; pick the setting where held-out error is lowest — that is the whole job of a validation set.

### Visualization (canvas `c3`, 720×300)

Two-line chart of train and test error versus model flexibility, with shaded zones and a sweet-spot marker.

- **Title (bold 16px, `#1a5276`, top center):** "Error vs Flexibility: the U-Shape (illustrative)".
- **Data (flexibility 1–10):**
  - train error: `[28, 18, 12, 10, 9, 8, 7, 6, 5, 4]` — blue `#2a78d6`, solid, width 3.
  - test error: `[30, 20, 14, 12, 13, 16, 21, 27, 33, 39]` — violet `#4a3aa7`, solid, width 3.
- **Axes:** y from 0 to 42 (labels 0–40% every 10, gray 12px); x ticks 1–10; padding top 52, bottom 55, left 70, right 170; L-shaped `#999` axis.
- **Zones:** left zone (flexibility 1 to 3) tinted `rgba(217,89,38,0.07)` labeled bold 12px orange "too simple" near top; right zone (from flexibility 6 onward) tinted `rgba(213,81,129,0.07)` labeled bold 12px magenta "memorizing noise" near top.
- **Sweet spot:** green `#008300` dot radius 7 at (flexibility 4, 12%) with dashed green drop-line (5/4, width 1.5) to the x-axis; bold 13px green label "sweet spot: test error lowest at 12%" above-right.
- **Legend (right side, x=w−158):** blue swatch "train error"; violet swatch "test error" (12px).
- **X-axis caption (gray 12px, bottom center):** "model flexibility (how many bends allowed)".

## The Confusion: The Two Failures Need Opposite Fixes

**Tags:** `common mistake` (red), `caution` (orange)

- **Diagnose first** — the same symptom "test score too low" has two opposite causes
- **Both scores low** — underfitting: add flexibility, features, or training time
- **Train high, test low** — overfitting: simplify, regularize, or get more data
- **The classic error** — seeing a low test score and always reaching for a bigger model
- **Bigger can backfire** — more flexibility applied to an overfit model widens the gap

*Example:* Treating the wiggle's 61% with an even wigglier model is medicine for the wrong disease.

**Key point:** **The confusion:** a high training score feels like progress but proves nothing — 100% on train with 61% on test is worse than 88% with 86%.

### Visualization (canvas `c4`, 720×300)

Diagnosis flow diagram: read both scores, branch to two diagnoses, each with its opposite fix.

- **Title (bold 16px, `#1a5276`, top center):** "Two Failures, Opposite Medicine".
- **Boxes** (fill alpha 0.12, 2px stroke, colored 12px text, first line bold):
  - Root at (270, 48, 180×40), ink `#1a5276`: "look at BOTH scores".
  - Left diagnosis (60, 128, 260×72), orange `#d95926`: "train 72%, test 70%" / "both low, gap small" / "diagnosis: UNDERFIT".
  - Right diagnosis (400, 128, 260×72), magenta `#d55181`: "train 100%, test 61%" / "train high, gap huge" / "diagnosis: OVERFIT".
  - Left fix (60, 222, 260×56), green `#008300`: "fix: MORE flexibility," / "more features, train longer".
  - Right fix (400, 222, 260×56), green `#008300`: "fix: LESS flexibility," / "regularize, get more data".
- **Arrows:** filled-head arrows (width 2): orange root→left diagnosis, magenta root→right diagnosis, green from each diagnosis down to its fix.
- **Annotation (magenta bold 13px, bottom center):** "same complaint, opposite cures — diagnose before treating".

## Regeneration instructions

- **Template:** tutorials topic page (per `tutorials/CLAUDE.md`). `<h1>` (no index number), `.subtitle` line, then 4 `.card-section` blocks, each an `<h2>` with 2px `#2980b9` bottom border plus a `table.layout` row. Every section uses `td.text-col` (50%) + `td.viz-col` (50%); one section places canvases `c1a`/`c1b` (310×300 each) side by side inside its single viz cell, wrapped in a `.viz-pair` flex row (`display:flex; gap:10px`, each canvas `flex:1 1 0; min-width:0`), sections 2–4 hold one 720×300 canvas. Text cells hold `.tags` pills, 5 one-line bullets (each opening with `<b>` in `#1a5276`), an italic `.example` line, and a `.key-point` callout.
- **Page CSS:** body system-ui sans, white background, text `#2c3e50`, padding 40px, line-height 1.6. h1 2rem `#1a5276` with 2px `#2980b9` bottom border; subtitle `#666` 0.95rem; h2 1.3rem `#1a5276`. `.key-point`: background `#f8f9fa`, left border 3px solid `#e74c3c`, padding 8px 12px, 0.9rem. `.example`: italic `#555` 0.9rem. ul 0.92rem. Canvas: `width:100%`, 1px `#e0e0e0` border, 4px radius. `.viz-pair`: `display:flex; gap:10px; align-items:flex-start`, with `.viz-pair canvas { flex:1 1 0; min-width:0 }`.
- **Tag pills:** 0.72rem bold, padding 2px 10px, radius 10px; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Chart palette (JS object `P`):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette anchors: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- **Canvas scaling:** shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates; intrinsic width/height attributes as given per chart. Shared `makeScales`/`drawFrame`/`drawPoints` helpers used by c1a/c1b. All data arrays hardcoded (no `Math.random()`). Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- No nav bar, no back/home links, no cross-page links. In regenerated HTML, any card links would use `.html` extensions.
