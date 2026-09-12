# Batch Normalization

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Batch Normalization

**Subtitle:** Curve each layer's outputs like exam scores — center every batch at 0 with spread 1 — so later layers train against a steady ruler instead of a moving target

## Curving Two Exam Classes

**Tags:** `core idea` (blue), `exam curving` (green), `moving target` (orange)

- **Two exams** — class A averages 80 with spread 5; class B's harder paper averages 45, spread 10
- **The curve** — subtract the class average, divide by the spread; a score becomes "how unusual"
- **Same ruler** — both classes' curved scores now run −1.4 to +1.4 and are directly comparable
- **A layer's scores** — a neural layer's outputs drift the same way; each batch is a class to curve
- **Batch norm** — curve every layer's outputs per batch so the next layer sees a steady ruler

*Example (italic):* A 75 in class A and a 35 in class B both curve to −1.0 — the same standing on either exam.

**Key point:** Batch normalization is exam curving inside the network: re-center each layer's batch of outputs at 0 with spread 1, so training stops chasing a moving target.

### Visualization (canvas `c1`, 720×300)

Dual-panel dot-strip chart: raw scores of the two classes (left) vs their curved z-scores (right), split by a vertical dashed divider at x=360.

- **Title (bold 15px, `#1a5276`, top center):** "Two Exams, One Ruler: Raw Scores vs Curved Scores".
- **Data:** class A scores `[73, 75, 75, 79, 81, 85, 85, 87]` (mean 80, std 5); class B scores `[31, 35, 35, 43, 47, 55, 55, 59]` (mean 45, std 10); shared curved values `[-1.4, -1.0, -1.0, -0.2, 0.2, 1.0, 1.0, 1.4]`.
- **Left panel (raw):** scale 0–100 mapped over x 55–335; light 2px `#999` axis at y=230 with ticks 0, 20, 40, 60, 80, 100 (12px `#444`); class A blue `#2a78d6` 6px dots on a strip at y=110 with bold 12px blue label "class A (mean 80)" above at x=55; class B orange `#d95926` 6px dots at y=185 with bold 12px orange label "class B (mean 45)"; dashed 1px vertical mean markers at score 80 (blue) and 45 (orange) from y=90 to y=230; magenta `#d55181` bold 12px annotation near top: "different centers, different spreads".
- **Right panel (curved):** scale −2 to +2 mapped over x 400–680; same 2px `#999` axis at y=230 with ticks −2, −1, 0, 1, 2; class A blue filled 6px dots at y=110; class B orange open circles (2px ring, 6px radius) at y=185 at IDENTICAL x positions; dashed ink `#1a5276` vertical line at z=0 from y=90 to y=230; green `#008300` bold 13px annotation: "same eight positions after curving".
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=360 from y=38 to h-12.

## One Batch Through the Curve, by Hand

**Tags:** `worked example` (blue), `γ and β` (green)

- **The batch** — eight activations from one neuron: 73, 75, 75, 79, 81, 85, 85, 87
- **Step 1: center** — the batch mean is 80; subtracting it gives −7, −5, −5, −1, 1, 5, 5, 7
- **Step 2: scale** — squared deviations average to 25, so std is 5; divide every value by it
- **The z's** — the batch becomes −1.4, −1.0, −1.0, −0.2, 0.2, 1.0, 1.0, 1.4: mean 0, spread 1
- **Step 3: γ, β** — learned scale 10 and shift 50 remake it as 36, 40, 40, 48, 52, 60, 60, 64
- **Not locked** — γ and β let the network undo or reshape the curve if 0-and-1 isn't optimal

*Example (italic):* The largest activation, 87, becomes (87 − 80)/5 = 1.4, then 10 × 1.4 + 50 = 64.

**Key point:** Batch norm is center, scale, re-dress: z = (x − mean)/std, output = γz + β, where γ and β are trained like any other weight.

### Visualization (canvas `c2`, 720×300)

Four horizontal dot strips forming a pipeline — raw batch, centered, normalized, and re-dressed — with small arrows between stages.

- **Title (bold 15px, `#1a5276`, top center):** "One Batch Through Batch Norm, Step by Step".
- **Data:** raw `[73, 75, 75, 79, 81, 85, 85, 87]`; centered `[-7, -5, -5, -1, 1, 5, 5, 7]`; normalized `[-1.4, -1.0, -1.0, -0.2, 0.2, 1.0, 1.0, 1.4]`; output with γ=10, β=50 `[36, 40, 40, 48, 52, 60, 60, 64]`.
- **Strips:** four 2px `#999` horizontal lines from x=170, width 480, at y=70, 125, 180, 235; value-to-x ranges: raw 70–90, centered −8 to 8, normalized −1.6 to 1.6, output 30–70.
- **Dots:** 6px, one color per strip: raw blue `#2a78d6`, centered aqua `#199e70`, normalized green `#008300`, output violet `#4a3aa7`.
- **Stage labels (bold 12px `#1a5276`, left-aligned at x=15):** "raw batch", "− mean (80)", "÷ std (5)", "× 10 + 50" beside their strips.
- **Endpoint labels (11px `#444`):** first and last dot of each strip labeled below: 73/87, −7/7, −1.4/1.4, 36/64.
- **Arrows:** short downward `#6b7280` arrows between consecutive strips at x=410.
- **Caption (bold 12px green `#008300`, bottom center):** "after step 2 the batch has mean 0, spread 1 — γ and β re-dress it".

## Why Training Speeds Up

**Tags:** `where it's used` (blue), `faster training` (green), `regularizer` (orange)

- **Moving target** — without BN, layer 5's input distribution shifts each update as layers 1–4 change
- **Steady input** — BN re-centers every batch, so each layer trains against a stable scale
- **Bigger steps** — stable scales tolerate higher learning rates without the loss blowing up
- **Easier init** — outputs get re-centered every batch, so exact weight init matters much less
- **Bonus noise** — batch statistics jitter from batch to batch, acting as a mild regularizer

*Example (italic):* On the illustrative run, the BN model hits loss 0.75 by epoch 5; the plain one is still at 1.26 at epoch 12.

**Key point:** Batch norm's practical win is speed and tolerance — larger learning rates, less fussy initialization, and a small regularizing side effect for free.

### Visualization (canvas `c3`, 720×300)

Line chart of training loss over 12 epochs for the same model with and without batch norm, with the epoch-5 BN point called out.

- **Title (bold 15px, `#1a5276`, top center):** "Training Loss With vs Without Batch Norm (illustrative)".
- **Data:** epochs 1–12; without BN `[2.30, 2.10, 1.95, 1.82, 1.70, 1.60, 1.51, 1.44, 1.38, 1.33, 1.29, 1.26]`; with BN `[2.30, 1.70, 1.25, 0.95, 0.75, 0.62, 0.53, 0.47, 0.43, 0.40, 0.38, 0.37]`.
- **Axes:** origin x=60, baseline y=245, plot width 600, height 190; y scale 0–2.5 with ticks 0, 0.5, 1.0, 1.5, 2.0, 2.5 (12px `#444`); x ticks at each epoch 1–12 (12px `#444`); light `#e5e9ef` horizontal gridlines at the y ticks.
- **Without BN:** orange `#d95926` 3px line with 4px dots; bold 12px orange end label "no BN: 1.26" right of the last point.
- **With BN:** green `#008300` 3px line with 4px dots; bold 12px green end label "with BN: 0.37".
- **Callout:** at epoch 5 on the BN curve (loss 0.75), a 6px green dot, a dashed ink `#1a5276` drop line to the baseline, and a green bold 13px annotation "loss 0.75 by epoch 5".
- **Caption (12px `#444`, bottom left):** "same model and data; numbers illustrative".

## Batch Norm vs Layer Norm

**Tags:** `common mistake` (red), `layer norm` (green), `train vs test` (orange)

- **The axis** — batch norm curves one feature across the batch: a column of the activation table
- **Layer norm** — curves one example across its features: a row; no other examples needed
- **Tiny batches** — a batch-of-2 mean and std are noisy, so BN degrades; layer norm doesn't care
- **Inference** — BN has no batch at test time; it swaps in running averages saved during training
- **Who uses what** — CNNs lean on batch norm; transformers and RNNs lean on layer norm

*Example (italic):* With a batch of one, BN's spread is 0 and the curve breaks; layer norm still has 4 features to average.

**Common mistake:** Forgetting the train/test switch — evaluating a BN network with batch statistics instead of the saved running averages makes a prediction depend on whoever else is in the batch.

### Visualization (canvas `c4`, 720×300)

A 4×4 activation table (4 examples × 4 features) with a blue column highlight for batch norm and a green row highlight for layer norm.

- **Title (bold 15px, `#1a5276`, top center):** "Batch Norm Curves a Column; Layer Norm Curves a Row".
- **Data (rows = examples b1–b4, columns = features f1–f4):** b1 `[12, 300, 0.4, 7]`; b2 `[18, 420, 0.6, 9]`; b3 `[24, 540, 0.8, 11]`; b4 `[30, 660, 1.0, 13]`.
- **Table:** 90×42 cells starting at x=170, y=70; feature headers "f1"–"f4" bold 12px `#1a5276` above the columns; row labels "b1"–"b4" bold 12px `#1a5276` left of the rows; cell values 12px `#444` centered; 1px `#e5e9ef` grid lines.
- **Batch norm highlight:** column f2 (300, 420, 540, 660) filled `rgba(42,120,214,0.18)` with a 2px blue `#2a78d6` border; blue bold 12px label above-right: "batch norm: one feature, whole batch (mean 480)".
- **Layer norm highlight:** row b1 (12, 300, 0.4, 7) outlined with a 2px green `#008300` border, no fill; green bold 12px label below-left of the table: "layer norm: one example, all features".
- **Note (bold 12px magenta `#d55181`, bottom center):** "batch of one: the column shrinks to a single cell — BN breaks, LN unaffected".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (here it hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
