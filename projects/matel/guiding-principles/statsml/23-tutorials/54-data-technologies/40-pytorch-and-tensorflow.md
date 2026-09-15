# PyTorch & TensorFlow

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** PyTorch & TensorFlow

**Subtitle:** You write only the forward computation — the framework records it, derives the gradients for you, and runs the whole thing on a GPU

## The Framework Writes the Calculus for You

**Tags:** `core idea` (blue), `autograd` (green), `tensors` (orange)

- **The task** — fit y = wx + b to three points (1,3), (2,5), (3,7); the true line is y = 2x + 1
- **The tensor** — the framework's array type; the same code runs unchanged on a CPU or a GPU
- **The forward pass** — you write pred = w·x + b and a loss; the framework records every operation
- **The tape** — that recording is a graph: w and x feed a multiply, then an add, then the loss
- **The backward pass** — loss.backward() walks the tape in reverse and fills in dloss/dw and dloss/db
- **No hand calculus** — you never write a derivative; the framework derives it from the recorded ops

*Example (italic):* Starting from w = 0, b = 0, one call — loss.backward() — hands back dw = −22.67 and db = −10 for the three points above, with no derivative written by hand.

**Key point:** A deep-learning framework is two things glued together: tensors that run on GPUs, and autograd — you describe the forward computation and it produces the gradients.

### Visualization (canvas `c1`, 720×300)

Computation-graph flow diagram for the running example: inputs flow right through recorded ops to the loss; gradient arrows flow back left with the exact gradient values.

- **Title (bold 15px, `#1a5276`, top center):** "Autograd: Forward Records the Ops, Backward Replays Them for Gradients".
- **Input boxes (left column at x=40, each 110px wide × 34px tall, 8px radius, fill `rgba(42,120,214,0.15)`, 12px `#2c3e50` text):** "w = 0" at y=60, "x = [1, 2, 3]" at y=125, "b = 0" at y=190.
- **Op nodes:** circle "×" (radius 18, 2px `#1a5276` stroke, bold 14px ink label) at (240, 96); circle "+" at (380, 150); rounded box at x=500, y=133 (170px × 40px, fill `rgba(0,131,0,0.12)`) labeled "MSE loss = 27.67" (12px).
- **Forward arrows (blue `#2a78d6`, 2px, solid):** w-box → "×", x-box → "×", "×" → "+", b-box → "+", "+" → loss box; 11px `#6b7280` label "pred = w·x + b" under the "+" node.
- **Backward arrows (green `#008300`, 2px, dashed 5/4), drawn 18px below each forward arrow, pointing right-to-left:** loss → "+" → "×" → w-box, and "+" → b-box; bold 12px green labels "dw = −22.67" near (150, 250) and "db = −10" near (330, 250).
- **Annotation (bold 13px violet `#4a3aa7`, top right near x=520, y=70):** "you wrote only the blue path".
- **Caption (12px `#444`, bottom right):** "gradients exact for the three points".

## One Training Step, Checked by Hand

**Tags:** `worked example` (blue), `gradient descent` (green)

- **Start** — w = 0, b = 0, so every prediction is 0 and the errors are −3, −5, −7
- **The loss** — mean squared error = (9 + 25 + 49) / 3 = 27.67
- **The gradients** — backward gives dw = (2/3)(−3−10−21) = −22.67 and db = (2/3)(−15) = −10
- **The step** — SGD with learning rate 0.1: w ← 0 − 0.1·(−22.67) = 2.27, b ← 0 − 0.1·(−10) = 1.0
- **Check it** — new predictions 3.27, 5.53, 7.80 against targets 3, 5, 7; the loss falls to 0.33

*Example (italic):* One optimizer step moves the line from flat at zero to y = 2.27x + 1.0 — nearly the true y = 2x + 1 — and cuts the loss from 27.67 to 0.33.

**Key point:** Training is this loop repeated: forward pass, loss, backward pass for gradients, optimizer step — the five lines in every framework tutorial are exactly these four moves.

### Visualization (canvas `c2`, 720×300)

Scatter of the three data points with the fitted line before and after one SGD step, showing the exact loss drop.

- **Title (bold 15px, `#1a5276`, top center):** "One SGD Step: Loss 27.67 → 0.33 (exact)".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = 0 to 3.5 with 12px `#444` tick labels at 0/1/2/3; y = 0 to 9, gridlines `#e5e9ef` at 3/6/9 with 12px labels.
- **Data points:** filled ink `#1a5276` circles radius 5 at (1,3), (2,5), (3,7), each with an 11px `#444` coordinate label offset up-right.
- **Before line:** red `#e74c3c` 3px horizontal line at y-value 0 from x=0 to x=3.5; dashed red 1.5px vertical error segments from each point down to it; bold 12px red label "before: w=0, b=0 — loss 27.67" near (0.4 data-x, y≈1.0 data-y).
- **After line:** green `#008300` 3px line from data (0, 1.0) to (3.5, 8.95) — the exact y = 2.27x + 1.0; bold 12px green label "after one step: w=2.27, b=1.00 — loss 0.33" near (0.9 data-x, 7.6 data-y).
- **Annotation (bold 13px violet `#4a3aa7`, near data (2.3, 2.2)):** "one gradient step, most of the fit".
- **Caption (12px `#444`, bottom right):** "all numbers exact".

## Why the GPU (and the Ecosystem) Matter

**Tags:** `where it's used` (blue), `GPU` (green), `ecosystem` (orange)

- **The scale** — a real model has millions of weights; the same four moves run on all of them at once
- **The GPU** — tensor math is thousands of independent multiplies, and GPUs run them in parallel
- **One line** — moving to the GPU is model.to("cuda") in PyTorch; TF places ops on devices for you
- **Research** — PyTorch's run-it-like-Python style won most paper code and researcher mindshare
- **Deployment** — TensorFlow with Keras built strong serving and mobile paths (TF Serving, TFLite)
- **Same math** — both fit y = wx + b identically; the real choice is ecosystem, not the gradients

*Example (italic):* A training epoch that takes about 3 hours on a CPU finishes in about 6 minutes on a GPU (illustrative) — that ratio is why training moved to GPUs.

**Key point:** Autograd removes the calculus and the GPU removes the wait; together they turned neural-net training from a specialist chore into a few lines of Python.

### Visualization (canvas `c3`, 720×300)

Horizontal paired-bar chart: CPU vs GPU wall-clock time for three workloads, pixel widths schematic, times labeled illustrative.

- **Title (bold 15px, `#1a5276`, top center):** "Same Code, Different Device (times illustrative)".
- **Axis:** vertical 2px `#999` baseline at x=250, bars extend right, max width 430.
- **Rows (CPU bar / GPU bar pairs; CPU bar top at y = 62, 132, 202; GPU bar 20px below; left-aligned 12px `#444` row label at x=20 beside each pair):**
  - "1k×1k matrix multiply": CPU blue `#2a78d6` bar width 60 with 11px label "0.4 s", GPU green `#008300` bar width 6 with label "0.01 s"
  - "one epoch, small CNN": CPU bar width 230 with label "180 s", GPU bar width 24 with label "6 s"
  - "one epoch, large image model": CPU bar width 430 with label "~3 h", GPU bar width 40 with label "~6 min"
- **Bar style:** 14px tall, CPU fill `rgba(42,120,214,0.30)` with 1px `#2a78d6` stroke, GPU solid `#008300`; 11px `#444` time labels at bar ends.
- **Legend (12px, top right near x=560, y=45):** blue swatch "CPU", green swatch "GPU".
- **Annotation (bold 13px magenta `#d55181`, centered near y=265):** "the Python you write does not change — only the device".
- **Caption (12px `#444`, bottom right):** "times illustrative; ratios order-of-magnitude".

## The 2017 Rivalry That Faded

**Tags:** `common mistake` (red), `eager vs graph` (orange)

- **The old split** — TF 1.x was define-then-run: declare the whole graph, then push data via a session
- **The PyTorch way** — define-by-run: each line executes immediately, so print() and pdb just work
- **The mistake** — picking a framework from 2017 blog posts: TF 2.x has run eagerly by default since 2019
- **The other direction** — PyTorch added torch.compile, which captures a graph for speed on request
- **What's left** — the remaining differences are ecosystem: serving, mobile, and research code availability

*Example (italic):* A team rejects TensorFlow because "you can't debug a static graph" — but TF 2 executes line by line, exactly like the PyTorch they chose instead.

**Common mistake:** Treating define-then-run vs define-by-run as a permanent divide. The frameworks converged — TF 2 is eager by default, PyTorch compiles graphs on demand — so the 2017 rivalry is mostly history.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram contrasting define-then-run (TF 1.x) with define-by-run (PyTorch, TF 2), each row as three boxes with arrows.

- **Title (bold 15px, `#1a5276`, top center):** "Define-Then-Run vs Define-By-Run".
- **Row 1 (boxes centered on y=105), label bold 12px `#444` at x=20, y=70:** "define-then-run (TF 1.x)"; orange `#d95926` rounded box at x=60 labeled "declare graph: w·x + b" (nothing computed), 3px `#6b7280` arrow to box at x=290 labeled "session.run(feed data)", arrow to box at x=520 labeled "results appear at the end"; 11px `#6b7280` note under the first box: "print here shows a symbol, not a number".
- **Row 2 (boxes centered on y=215), label at x=20, y=180:** "define-by-run (PyTorch, TF 2)"; blue `#2a78d6` box at x=60 labeled "pred = w·x + b runs now", arrow to green `#008300` box at x=290 labeled "print(pred) → real numbers", arrow to green box at x=520 labeled "loss.backward() on demand".
- **Box style:** 190px wide, 40px tall, 8px radius, fills `rgba(217,89,38,0.12)` (row 1) / `rgba(42,120,214,0.15)` and `rgba(0,131,0,0.12)` (row 2), 12px `#2c3e50` text, 1.5px matching strokes.
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=278):** "today each framework offers both modes — eager to debug, compiled graph for speed".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays and values above (no randomness). The regression math is exact: data (1,3), (2,5), (3,7); from w=0, b=0 the loss is 83/3 ≈ 27.67, gradients dw = −68/3 ≈ −22.67 and db = −10, one SGD step at lr 0.1 gives w = 2.27 (2.2667), b = 1.0, new loss ≈ 0.33 — label these "exact". CPU/GPU timings in c3 are invented and labeled "illustrative".
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
