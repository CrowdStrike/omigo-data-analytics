# JAX

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** JAX

**Subtitle:** JAX is NumPy with superpowers — write a plain function once, then transform it: grad differentiates it, jit compiles it, vmap batches it

## One Function, Three Superpowers

**Tags:** `core idea` (blue), `autograd` (green), `Google research` (orange)

- **The function** — a tiny loss written in plain code: loss(w) = (2·w − 10)², a model fitting y=10 from x=2
- **Mirror API** — jax.numpy copies NumPy's interface: swap `import numpy` for `import jax.numpy` and it runs
- **grad** — jax.grad(loss) returns a brand-new Python function that computes the derivative
- **Exact answer** — by hand, dloss/dw = 8w − 40; at w=3 the loss is 16 and the gradient is −16 (exact)
- **No math by you** — you never typed the derivative; JAX derived it from the code itself

*Example (italic):* Calling grad(loss)(3.0) returns −16.0 — the same −16 you get differentiating (2w − 10)² with pencil and paper.

**Key point:** JAX transformations take a function and return a new function — grad(loss) is ordinary Python you can call, compose, or transform again.

### Visualization (canvas `c1`, 720×300)

Line chart of the loss curve L(w) = (2w − 10)² with the tangent line at w=3 whose slope is the gradient −16.

- **Title (bold 15px, `#1a5276`, top center):** "grad Reads the Slope: loss(w) = (2w − 10)², Tangent at w = 3".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = w from 0 to 10, 12px `#444` tick labels every 2; y = loss 0 to 100, gridlines `#e5e9ef` at 25/50/75.
- **Loss curve:** blue `#2a78d6` 3px line through w = `[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]`, loss = `[100, 64, 36, 16, 4, 0, 4, 16, 36, 64, 100]` — a parabola with its minimum at w=5.
- **Tangent line:** orange `#d95926` 2px dashed (dash 6/4) segment through the exact tangent points (w=2, loss=32) and (w=4, loss=0) — slope −16, touching the curve at (3, 16).
- **Point marker:** filled `#1a5276` 5px-radius dot at (w=3, loss=16), bold 12px `#1a5276` label "w=3, loss=16" beside it.
- **Annotation (bold 13px orange `#d95926`, near w=6, y=70):** "grad(loss)(3.0) = −16 (exact)".
- **Caption (12px `#444`, bottom right):** "curve and slope exact".

## grad, jit, and vmap on the Same Loss

**Tags:** `worked example` (blue), `composable transforms` (green)

- **The batch** — four data points: x = [1, 2, 3, 4], targets y = [2, 4, 6, 8], weight w = 3
- **Per-example loss** — loss = (w·x − y)²; predictions [3, 6, 9, 12] miss targets by [1, 2, 3, 4]
- **vmap** — vmap(loss) maps over the batch axis and returns [1, 4, 9, 16] with no Python loop
- **Hand-check** — third point: 3·3 − 6 = 3, squared is 9 — matching the vmap output (exact)
- **jit** — jit(loss) traces the function and hands it to XLA, which compiles one fused kernel
- **Stacking** — jit(vmap(grad(loss))) is legal: transformations compose like nested wrappers

*Example (italic):* vmap(loss)(w, xs, ys) returns [1, 4, 9, 16] — exactly the four squared errors you can redo by hand.

**Key point:** grad, jit, and vmap each take the same loss function and hand back a new one — differentiated, compiled, or batched — without editing a line of its body.

### Visualization (canvas `c2`, 720×300)

Flow diagram: one source function on the left fanning out to three transformation boxes, each showing its concrete output for the worked example.

- **Title (bold 15px, `#1a5276`, top center):** "One loss Function In, Three New Functions Out".
- **Source box:** blue `#2a78d6` rounded box at x=30, y=115, 190px wide, 50px tall, fill `rgba(42,120,214,0.15)`, 12px `#2c3e50` two-line text "loss(w, x, y)" / "= (w·x − y)²".
- **Three arrows:** 3px `#6b7280` lines from the source box's right edge to each target box's left edge.
- **grad box (top):** green `#008300` rounded box at x=330, y=45, 350px wide, 50px tall, fill `rgba(0,131,0,0.12)`, 12px text "grad(loss)(3.0, 2.0, 10.0) → −16.0" with bold 11px green tag "differentiate (exact)".
- **jit box (middle):** violet `#4a3aa7` rounded box at x=330, y=125, 350px wide, 50px tall, fill `rgba(74,58,167,0.10)`, 12px text "jit(loss) → XLA-compiled kernel" with bold 11px violet tag "compile".
- **vmap box (bottom):** aqua `#199e70` rounded box at x=330, y=205, 350px wide, 50px tall, fill `rgba(25,158,112,0.12)`, 12px text "vmap(loss)(3, [1,2,3,4], [2,4,6,8]) → [1, 4, 9, 16]" with bold 11px aqua tag "batch (exact)".
- **Box style:** 8px corner radius, 2px colored borders matching each tag color.
- **Annotation (bold 13px `#1a5276`, centered near y=285):** "same body, three superpowers — and they stack".

## Why It Runs Google's Research

**Tags:** `where it's used` (blue), `XLA` (green), `TPU` (orange)

- **The library** — JAX is Google's research library: NumPy's API re-hosted on an accelerator compiler
- **XLA** — jit lowers the traced function to XLA, which fuses many array ops into one kernel
- **One code path** — the same function runs on CPU, GPU, or TPU; JAX dispatches to the backend
- **Many devices** — pmap and sharding APIs split a batch across devices for one parallel update
- **The users** — much of Google DeepMind's published research and TPU training runs on JAX
- **The price** — the first jitted call pays a compile cost; every later call reuses the cached kernel

*Example (italic):* An illustrative training step: 50 ms as a looped-NumPy call, 180 ms on the first jit call (compiling), 2 ms per call after.

**Key point:** The compile cost is paid once at the first call; every call after replays the cached XLA kernel — that steady-state 2 ms is where the speed lives.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart: per-call time for a looped NumPy step vs the first jitted call (compile included) vs steady-state jitted calls.

- **Title (bold 15px, `#1a5276`, top center):** "Pay to Compile Once, Then Replay the Kernel".
- **Axis:** horizontal 2px `#999` baseline at x=230, bars extend right, max width 440; pixel widths proportional to milliseconds (440 px = 180 ms).
- **Rows (top to bottom at y = 80, 140, 200), each with a left-aligned 12px `#444` label at x=20:**
  - "NumPy loop, per call — 50 ms": blue `#2a78d6` bar width 122
  - "jit, call 1 (compiling) — 180 ms": orange `#d95926` bar width 440
  - "jit, calls 2+ — 2 ms": green `#008300` bar width 5
- **Bar style:** 18px tall, fills `rgba(42,120,214,0.30)` / `rgba(217,89,38,0.30)` / solid `#008300`, 2px borders in the row color, 11px `#444` ms labels at bar ends.
- **Annotation (bold 13px green `#008300`, right side near y=240):** "steady state: 25× faster than the loop".
- **Caption (12px `#444`, bottom right):** "timings illustrative".

## The Print That Fires Only Once

**Tags:** `common mistake` (red), `functional purity` (orange)

- **The rule** — jit runs your Python once with tracer values to record the math, then replays compiled code
- **The print** — a print() inside a jitted function fires at trace time only: 1 print across 1,000 calls
- **In-place** — `arr[0] = 5` raises an error; JAX arrays are immutable, so you write `arr.at[0].set(5)`
- **Hidden state** — reading a global inside jit bakes its trace-time value into the kernel forever
- **Randomness** — there is no global seed; every random draw takes an explicit key you split yourself
- **The symptom** — code "works" un-jitted, then goes silent or stale the moment jit is added

*Example (italic):* A debugging print inside a jitted loss shows up once during the first call, then never again across the next 999 calls.

**Common mistake:** Assuming jitted code runs your Python line by line. jit executes Python once to record the computation — side effects belong to that single recording, not to later calls.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: the same printing function called 1,000 times without jit (1,000 prints) vs with jit (one trace-time print, then silence).

- **Title (bold 15px, `#1a5276`, top center):** "print() Inside jit: One Trace, Then Silence".
- **Row 1 (y=95), label 12px `#444` at x=20:** "no jit"; blue `#2a78d6` rounded box at x=150 labeled "loss(w) with print()" (12px), 3px arrow to a green `#008300` box at x=430 labeled "1,000 calls → 1,000 prints" with bold 12px green "✓ runs every call".
- **Row 2 (y=205), label:** "with jit"; blue box at x=150 "jit(loss) traces once", 3px arrow to a green box at x=360 labeled "call 1: print fires", then arrow to a red `#e74c3c` box at x=545 labeled "calls 2–1,000: silent" with bold 12px red "✗ 999 silent calls".
- **Box style:** 150–170px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(0,131,0,0.12)` / `rgba(231,76,60,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px orange `#d95926`, centered near y=270):** "jit replays recorded math — your Python side effects were left behind at trace time".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); the loss curve, tangent slope −16, per-example losses [1, 4, 9, 16], and the hand derivative 8w − 40 are exact math; the 50 / 180 / 2 ms timings and print counts are invented and labeled illustrative.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
