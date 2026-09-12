# Sigmoid

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Sigmoid

**Subtitle:** The S-shaped curve that squashes any number — from minus infinity to plus infinity — into a value between 0 and 1 that reads as a probability

## One Spam Filter, Three Emails

**Tags:** `core idea` (blue), `S-curve` (green), `probability` (orange)

- **The filter** — a spam filter adds up clues and gives each email a raw score: any number, no limits
- **Three emails** — a newsletter scores −4, a borderline note scores 0, a "free prize" email scores +3
- **The problem** — "score +3" means nothing to a user; "95% likely spam" means everything
- **The squash** — sigmoid bends the whole number line into 0–1: σ(−4) = 0.02, σ(0) = 0.50, σ(+3) = 0.95
- **The shape** — an S: flat near 0 on the far left, steep through the middle, flat near 1 on the right

*Example (italic):* The "free prize" email's raw score of +3 becomes σ(3) = 0.95 — the filter can now say "95% spam" instead of "score three".

**Key point:** Sigmoid is a translator: raw score in, probability out. σ(x) = 1/(1+e^−x) — negative scores land below 0.5, zero lands exactly at 0.5, positive scores land above.

### Visualization (canvas `c1`, 720×300)

Single sigmoid curve over x ∈ [−6, +6] with the three example emails marked as labeled dots and a dashed 0.5 guide line.

- **Title (bold 15px, `#1a5276`, top center):** "The Sigmoid: Raw Spam Score → Probability".
- **Curve:** plot y = 1/(1+Math.exp(−x)) for x from −6 to +6 in steps of 0.1 (hardcoded formula, hardcoded range); axis origin x=60, plot width 600, baseline y=250, plot height 195 mapping y 0→1; blue `#2a78d6` 3px line.
- **Axes:** 2px `#1a5276` x-axis at y=250 and y-axis at x=360 (score 0); x ticks at −6, −4, −2, 0, 2, 4, 6 with 12px `#444` labels; y labels "0", "0.5", "1" at left (12px `#444`).
- **Guide:** dashed `#bdc3c7` (dash 4/3) horizontal line at y = 0.5 level across the plot.
- **Dots:** three 6px dots on the curve at (−4, 0.02) green `#008300` labeled "newsletter: σ(−4)=0.02", (0, 0.50) yellow `#c98500` labeled "borderline: σ(0)=0.50", (3, 0.95) magenta `#d55181` labeled "free prize: σ(3)=0.95"; labels bold 12px in each dot's color, offset to avoid the curve.
- **Caption (12px `#444`, bottom center):** "any raw score, squashed into 0–1".

## Computing σ by Hand

**Tags:** `worked example` (blue), `rule of thumb` (green)

- **The recipe** — σ(x) = 1/(1+e^−x): flip the sign, exponentiate, add 1, take the reciprocal
- **Score 0** — e^0 = 1, so σ(0) = 1/(1+1) = 0.50 — total coin flip
- **Score +2** — e^−2 ≈ 0.135, so σ(2) = 1/1.135 ≈ 0.88
- **Score −2** — e^2 ≈ 7.39, so σ(−2) = 1/8.39 ≈ 0.12
- **Mirror rule** — σ(−x) = 1 − σ(x): 0.12 and 0.88 add to exactly 1, so compute one side only
- **Quick anchors** — memorize σ(0)=0.50, σ(2)≈0.88, σ(4)≈0.98; the rest interpolates

*Example (italic):* Five emails scoring −4, −2, 0, +2, +4 come out at probabilities 0.02, 0.12, 0.50, 0.88, 0.98 — a perfect mirror around 0.50.

**Key point:** One anchor and the mirror rule cover most mental math: σ(2) ≈ 0.88, therefore σ(−2) ≈ 0.12. Every 2 points of raw score moves you roughly one "confidence band".

### Visualization (canvas `c2`, 720×300)

Bar chart of the five worked-example scores and their sigmoid outputs, with mirror pairs color-matched and a dashed 0.5 line.

- **Title (bold 15px, `#1a5276`, top center):** "Five Scores Through the Sigmoid — a Mirror Around 0.50".
- **Data:** scores `[-4, -2, 0, 2, 4]`, probabilities `[0.02, 0.12, 0.50, 0.88, 0.98]`.
- **Layout:** axis origin x=70, plot width 570, baseline y=245, plot height 185, y scale 0→1; five bars 62px wide, evenly spaced; y ticks at 0, 0.25, 0.5, 0.75, 1 (12px `#444`).
- **Bar fills:** mirror pairs share a hue — score −4 and +4 fill `rgba(0,131,0,0.45)` green; −2 and +2 fill `rgba(42,120,214,0.45)` blue; score 0 fill `rgba(201,133,0,0.5)` yellow.
- **Labels:** score label 12px `#444` below each bar ("−4", "−2", "0", "+2", "+4"); probability bold 13px in the bar's solid color above each bar ("0.02", "0.12", "0.50", "0.88", "0.98").
- **Guide:** dashed `#bdc3c7` horizontal line at the 0.5 level, labeled "0.50" 11px `#6b7280` at right.
- **Annotation (bold 13px green `#008300`, upper left):** two lines "mirror rule:" / "0.12 + 0.88 = 1".
- **Caption (12px `#444`, bottom center):** "σ(−x) = 1 − σ(x): compute one side, mirror the other".

## The Last Layer Before a Decision

**Tags:** `where it's used` (blue), `decision threshold` (orange)

- **Logistic regression** — is exactly a weighted score fed through a sigmoid; sigmoid IS the model's face
- **Neural nets** — a binary classifier's final neuron is a sigmoid: everything before it builds the score
- **Eight emails** — raw scores −5.1, −3.2, −1.4, −0.3, 0.8, 1.9, 3.5, 5.2 span an unreadable range
- **After sigmoid** — the same eight become 0.006, 0.04, 0.20, 0.43, 0.69, 0.87, 0.97, 0.995
- **The cut** — flagging emails above probability 0.50 is the same as flagging raw scores above 0
- **Comparable** — probabilities share a 0–1 scale, but need calibration to compare across models

*Example (italic):* With a 0.50 cutoff, the −0.3 email (0.43) passes and the 0.8 email (0.69) gets flagged — the decision happens right at score zero.

**Key point:** Sigmoid is the standard last step of a binary classifier. Thresholding the probability at 0.50 is exactly thresholding the raw score at 0 — the sigmoid never changes the ordering, only the units.

### Visualization (canvas `c3`, 720×300)

Two horizontal rulers: eight email scores on a raw-score line (top) and the same eight after sigmoid on a 0–1 line (bottom), with the 0 / 0.50 cutoff marked on both.

- **Title (bold 15px, `#1a5276`, top center):** "Eight Emails: Raw Scores (top) vs Sigmoid Probabilities (bottom)".
- **Data:** raw scores `[-5.1, -3.2, -1.4, -0.3, 0.8, 1.9, 3.5, 5.2]`; probabilities `[0.006, 0.04, 0.20, 0.43, 0.69, 0.87, 0.97, 0.995]`.
- **Rulers:** both from x=70, width 580, 2px `#999` lines; raw ruler at y=100, probability ruler at y=210.
- **Raw ruler:** heading bold 12px `#444` "raw scores (−6 → +6)"; dots positioned at (score+6)/12 of the width, 6px; dots below 0 in green `#008300`, above 0 in magenta `#d55181`; end labels "−6" and "+6" 12px `#444`; vertical dashed `#bdc3c7` cutoff tick at score 0 labeled "0" bold 12px `#1a5276` above.
- **Probability ruler:** heading "after sigmoid (0 → 1)"; same eight emails at probability × width, same 6px dots and green/magenta split; labels "0.006", "0.43", "0.69", "0.995" 11px `#444` below their dots (others overlap, skip); dashed cutoff tick at 0.50 labeled "0.50" bold 12px `#1a5276`.
- **Connector hint (bold 12px `#1a5276`, right of rulers):** "same emails," / "same order".
- **Takeaway (bold 13px green `#008300`, bottom center):** "probability > 0.50 is exactly raw score > 0 — sigmoid keeps the order, changes the units".

## The Flat Tails Problem

**Tags:** `common mistake` (red), `vanishing gradient` (orange)

- **Steep middle** — moving a score from 0 to 1 lifts the probability from 0.50 to 0.73 — a jump of 0.23
- **Flat tail** — moving from 4 to 6 lifts it from 0.982 to 0.998 — a crawl of 0.016
- **The slope** — sigmoid's slope is σ(x)(1−σ(x)): 0.25 at score 0, 0.018 at 4, 0.002 at 6
- **Training stalls** — in deep nets, near-zero slope means near-zero learning signal: vanishing gradients
- **False certainty** — 0.998 looks like near-proof, but out in the tail the score barely moves the output
- **Modern fix** — hidden layers now use ReLU-style activations; sigmoid survives mostly at the output

*Example (italic):* Doubling the "free prize" evidence pushed the score from 3 to 6, yet the probability only crept from 0.95 to 0.998.

**Common mistake:** Reading the tail as sensitive. Past a score of ±4 the sigmoid is nearly flat — huge score changes barely move the probability, and gradients there are too small to train on.

### Visualization (canvas `c4`, 720×300)

Sigmoid curve (top) with its slope curve (bottom) sharing the same x-axis, highlighting the steep middle versus flat tails.

- **Title (bold 15px, `#1a5276`, top center):** "Steep Middle, Flat Tails: Where the Sigmoid Stops Responding".
- **Shared x-axis:** x ∈ [−6, +6] mapped from x=60 to x=660; ticks at −6, −4, −2, 0, 2, 4, 6 with 12px `#444` labels at y=262.
- **Top curve (sigmoid):** plot y = 1/(1+Math.exp(−x)), step 0.1, band from y=40 (value 1) to y=145 (value 0); blue `#2a78d6` 3px line; 5px dots at (0, 0.50), (1, 0.73), (4, 0.982), (6, 0.998) in ink `#1a5276` with bold 11px value labels.
- **Brackets:** magenta `#d55181` bold 12px annotation over the middle "0→1: +0.23"; orange `#d95926` bold 12px annotation over the right tail "4→6: +0.016".
- **Bottom curve (slope):** plot y = s(x)·(1−s(x)) where s(x)=1/(1+Math.exp(−x)), step 0.1, band from y=175 (value 0.25) to y=250 (value 0); green `#008300` 3px line; bold 11px green labels "0.25" at the x=0 peak and "0.018" near x=4.
- **Divider label (11px `#6b7280`, left):** "sigmoid" beside the top band, "slope" beside the bottom band.
- **Annotation (bold 13px orange `#d95926`, lower right):** two lines "flat tail = tiny slope" / "= vanishing gradient".
- **Caption (12px `#444`, bottom center):** "slope σ(x)(1−σ(x)) peaks at 0.25 and dies in the tails".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (here it hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- Sigmoid and slope curves are computed from the hardcoded formula `1/(1+Math.exp(-x))` over the hardcoded range [−6, 6], step 0.1 — no random values anywhere.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
