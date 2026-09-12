# ReLU

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** ReLU

**Subtitle:** max(0, x) — pass positives through unchanged, replace negatives with zero — this bare clamp trains deep networks better than decades of smoother, fancier curves

## A Sales Bonus That Never Goes Negative

**Tags:** `core idea` (blue), `hinge rule` (green), `max(0, x)` (orange)

- **The desk** — a sales desk pays $1 of bonus for every dollar of sales above a $1,000 quota
- **Below quota** — selling $400 or $700 earns exactly $0; a bad month never turns into a debt
- **Above quota** — $1,300 pays $300, $1,600 pays $600, $1,900 pays $900 — dollar for dollar
- **The rule** — bonus = max(0, sales − 1,000): flat at zero, then a straight 45-degree line
- **That's ReLU** — a neuron's output = max(0, z): negatives become 0, positives pass through as-is

*Example (italic):* A rep selling $1,600 gets max(0, 1,600 − 1,000) = $600; a rep selling $700 gets max(0, −300) = $0.

**Key point:** ReLU (Rectified Linear Unit) is exactly the bonus rule: keep any positive signal untouched, and replace anything negative with a hard zero.

### Visualization (canvas `c1`, 720×300)

Single hinge-shaped line chart: bonus paid vs monthly sales, flat at $0 until the $1,000 quota, then rising dollar-for-dollar.

- **Title (bold 15px, `#1a5276`, top center):** "Bonus = max(0, sales − $1,000): the ReLU Shape".
- **Data:** sales `[400, 700, 1000, 1300, 1600, 1900]`, bonus `[0, 0, 0, 300, 600, 900]`.
- **Axes:** origin x=70, baseline y=245, plot width 580, plot height 185; x scale $0–$2,000, y scale $0–$1,000; 1.5px `#1a5276` axis lines; x tick labels "$400", "$700", "$1,000", "$1,300", "$1,600", "$1,900" 12px `#444` under each point; y labels "$0", "$500", "$1,000" 12px `#444` at left.
- **Line:** blue `#2a78d6` 3px through all six points, 5px filled dots at each.
- **Quota marker:** vertical dashed `#6b7280` line (dash 4/3) at sales=$1,000 from baseline to y=60, labeled "quota" bold 12px `#6b7280` at its top.
- **Annotations:** green `#008300` bold 13px "flat at $0 below quota" over the left flat segment; blue `#2a78d6` bold 13px "climbs $1 per $1 above" beside the rising segment.
- **Caption (12px `#444`, bottom right):** "same rule a ReLU neuron applies to its input z".

## The Same Clamp Inside a Neuron

**Tags:** `worked example` (blue), `activation curves` (orange)

- **The input** — a neuron's weighted sum z arrives as −2.0, −0.5, 0, 1.2, or 3.0
- **ReLU output** — max(0, z) gives 0, 0, 0, 1.2, 3.0 — no math beyond one sign check
- **Sigmoid output** — the older S-curve squashes the same z to 0.12, 0.38, 0.50, 0.77, 0.95
- **Squashing** — sigmoid crams every z, however large, into the 0–1 band; 3.0 becomes 0.95
- **Cheap** — ReLU is one comparison; sigmoid and tanh each need an exponential per call

*Example (italic):* For z = 3.0, ReLU keeps 3.0 exactly, while sigmoid flattens it to 0.95 — barely above sigmoid(1.2) = 0.77.

**Key point:** After decades of tuning smooth S-shaped curves, the winner just checks the sign and keeps the value — differences between big inputs stay big.

### Visualization (canvas `c2`, 720×300)

Overlay of three activation curves — ReLU, sigmoid, tanh — on one set of axes, with the worked example's five z values dotted on the ReLU and sigmoid curves.

- **Title (bold 15px, `#1a5276`, top center):** "ReLU vs Sigmoid vs Tanh on the Same Inputs z".
- **Axes:** plot area x=70–650; value range x from −4 to 4, y from −1.2 to 4.2 mapped to y=250 (bottom) – y=45 (top); 1.5px `#1a5276` horizontal axis drawn at value y=0 with x tick labels "−4", "−2", "0", "2", "4" 12px `#444`; light dashed `#e5e9ef` horizontal gridlines at y-values 1, 2, 3 with 11px `#6b7280` labels at left.
- **Curves (plot by formula, step x by 0.05):** ReLU `max(0, x)` blue `#2a78d6` 3px; sigmoid `1/(1+e^-x)` magenta `#d55181` 2.5px; tanh orange `#d95926` 2.5px.
- **Curve labels (bold 12px, curve color, near right ends):** "ReLU", "sigmoid", "tanh".
- **Dots:** ReLU dots blue 5px at (−2, 0), (−0.5, 0), (0, 0), (1.2, 1.2), (3, 3); sigmoid dots magenta 4px at (−2, 0.12), (−0.5, 0.38), (0, 0.50), (1.2, 0.77), (3, 0.95).
- **Annotations:** blue `#2a78d6` bold 13px "ReLU keeps 3.0 as 3.0" with a short arrow to the (3, 3) dot; magenta `#d55181` bold 12px "sigmoid squashes 3.0 to 0.95" with an arrow to the (3, 0.95) dot.

## Why the Fancy Curves Lost: Vanishing Gradients

**Tags:** `where it's used` (blue), `vanishing gradient` (red), `deep networks` (orange)

- **Training signal** — backprop multiplies one slope per layer; the product must survive the trip
- **Sigmoid slope** — at best 0.25, so ten layers multiply to 0.25^10 ≈ 0.00000095 — the signal dies
- **ReLU slope** — exactly 1 for positive z, so ten slopes give 1^10 = 1 — weights still scale it
- **The unlock** — saturating nets stalled at a few layers; ReLU made 10+ layers trainable
- **No saturation** — ReLU never flattens on the positive side, so large activations keep learning

*Example (italic):* After just 6 sigmoid layers the gradient is 0.25^6 ≈ 0.00024 — the first layer receives about one four-thousandth of the signal.

**Key point:** Fancier meant smoother and saturating; saturating means slopes below 1, and a deep product of numbers below 1 vanishes. max(0, x) wins because its slope is exactly 1.

### Visualization (canvas `c3`, 720×300)

Line chart on a log10 y-axis: gradient surviving after n layers, ReLU flat at 1 vs best-case sigmoid decaying by ×0.25 per layer.

- **Title (bold 15px, `#1a5276`, top center):** "Gradient Surviving n Layers: ReLU vs Best-Case Sigmoid (log scale)".
- **Data:** layers `[1, 2, 4, 6, 8, 10]`; ReLU `[1, 1, 1, 1, 1, 1]`; sigmoid `[0.25, 0.0625, 0.0039, 0.00024, 0.000015, 0.00000095]`.
- **Axes:** origin x=80, baseline y=240, plot width 560, plot height 180; x positions evenly spaced for the six layer counts, labels "1", "2", "4", "6", "8", "10" 12px `#444` under each with axis caption "layers deep"; y maps log10(value) over range −6.2 to 0.2; y tick labels "1", "0.01", "0.0001", "0.000001" 12px `#444` at log10 = 0, −2, −4, −6 with dashed `#e5e9ef` gridlines.
- **ReLU line:** green `#008300` 3px, flat at value 1, 5px dots; green bold 13px annotation above it "ReLU: ×1 per layer — arrives intact".
- **Sigmoid line:** magenta `#d55181` 3px diving line, 5px dots; magenta bold 13px annotation, two lines: "sigmoid: ×0.25 per layer" / "≈ 0.00000095 by layer 10".
- **Caption (12px `#444`, bottom right):** "activation slopes only — weights also scale the gradient; 0.25 is sigmoid's best case".

## The Catch: Neurons That Die at Zero

**Tags:** `common mistake` (red), `dying ReLU` (orange), `leaky fix` (green)

- **Flat side** — for negative z, ReLU outputs 0 with slope 0, so no learning signal flows back
- **Stuck** — a neuron pushed into always-negative z outputs 0 on every input; it is "dead"
- **How common** — in one trained layer of 100 neurons, 28 were dead vs 3 with the leaky fix
- **Leaky fix** — leaky ReLU uses max(az, z), small a such as 0.01 or 0.1: keeps neurons revivable
- **Still ReLU** — the fixes are tiny tweaks; the pass-positives-through core survived them all

*Example (italic):* With leaky ReLU, z = −2.0 outputs −0.2 instead of 0, so a gradient of 0.1 still reaches the weights.

**Common mistake:** Assuming ReLU's zero region is harmless. A too-high learning rate can knock many neurons permanently to zero — monitor the fraction of always-zero activations.

### Visualization (canvas `c4`, 720×300)

Dual panel split by a dashed divider at x=360: ReLU vs leaky ReLU curves on the negative side (left), dead-neuron counts per 100 as two bars (right).

- **Title (bold 15px, `#1a5276`, top center):** "The Dead Zone and the Leaky Fix (illustrative)".
- **Left panel (curves):** plot area x=55–335; value range x from −4 to 4, y from −1 to 4 mapped to y=250–60; 1.5px `#1a5276` horizontal axis at value y=0, x tick labels "−4", "−2", "0", "2", "4" 12px `#444`; ReLU `max(0, x)` blue `#2a78d6` 3px; leaky ReLU `max(0.1x, x)` green `#008300` 2.5px dashed (dash 6/4) so both are visible where they overlap on the positive side; magenta `#d55181` bold 12px annotation, two lines over the negative flat segment: "ReLU slope 0 here:" / "no learning"; green bold 12px label "leaky: slope 0.1" under the green negative segment; caption 12px `#444` "z = −2.0 → ReLU 0, leaky −0.2".
- **Right panel (bars):** heading bold 12px `#444` "dead neurons per 100 after training (illustrative)"; baseline y=240, scale max 30; bar "plain ReLU" at x=420, width 90, value 28, fill `rgba(217,89,38,0.55)`, bold 13px orange `#d95926` label "28 / 100" above; bar "leaky ReLU" at x=560, width 90, value 3, fill `rgba(0,131,0,0.4)`, bold 13px green `#008300` label "3 / 100" above; bar names 12px `#444` below the baseline.
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=360 from y=38 to h-12.

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (here it hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
