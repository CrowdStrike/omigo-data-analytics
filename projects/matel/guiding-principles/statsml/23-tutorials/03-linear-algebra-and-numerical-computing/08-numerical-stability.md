# Numerical Stability

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Numerical Stability

**Subtitle:** Computers do arithmetic with ~16 digits, and some computations quietly amplify the rounding — the classic self-inflicted wound is computing a matrix inverse when solving the system would do

## Two Coffee Blends, One Rounded Penny

**Tags:** `core idea` (blue), `ill-conditioning` (orange), `running example` (green)

- **The blends** — blend A: 2 kg arabica + 1 kg robusta = $9.00; blend B: 4 kg + 2.01 kg = $18.03
- **The unknowns** — two equations, two unknowns: the per-kg prices of arabica and robusta beans
- **The clean answer** — solving the system exactly gives arabica $3.00/kg and robusta $3.00/kg
- **The typo** — a clerk rounds the $18.03 invoice down to $18.00, a 0.2% change in one number
- **The blow-up** — the solved prices jump to arabica $4.50/kg and robusta $0.00/kg
- **The name** — the system is ill-conditioned: blend B is almost exactly two bags of blend A

*Example (italic):* A 3-cent rounding on an $18 invoice moved the computed arabica price by $1.50 — a 50% swing from a 0.2% input change.

**Key point:** Numerical stability asks how much tiny input and rounding errors grow inside a computation. This problem is touchy on its own — a stable algorithm must not add fuel.

### Visualization (canvas `c1`, 720×300)

Price-space line plot: each blend equation is a line in the (arabica price, robusta price) plane; the two lines are nearly parallel, so nudging one slides the crossing point far away.

- **Title (bold 15px, `#1a5276`, top center):** "Two Nearly Parallel Blend Equations: a 3¢ Nudge Moves the Answer $1.50".
- **Axes:** origin pixel (70, 245); x axis to x=630 labeled "arabica $/kg" (12px `#444`), range 0–6, ticks at 0..6; y axis up to y=50 labeled "robusta $/kg", range 0–9, ticks at 0, 3, 6, 9; 1px `#e5e9ef` gridlines at the ticks; axes 2px `#1a5276`.
- **Line A (blue `#2a78d6`, 3px):** r = 9 − 2a, drawn from (0, 9) to (4.5, 0); 12px blue label "blend A: 2a + 1r = $9.00" along its upper part.
- **Line B (green `#008300`, 3px):** r = (18.03 − 4a)/2.01, drawn from (0, 8.97) to (4.5075, 0); 12px green label "blend B: 4a + 2.01r = $18.03".
- **Line B rounded (orange `#d95926`, 3px dashed, dash 6/4):** r = (18.00 − 4a)/2.01, drawn from (0, 8.955) to (4.5, 0).
- **Dots:** green 6px dot at (3, 3) with bold 13px green label "true prices ($3.00, $3.00)"; orange 6px dot at (4.5, 0) with bold 13px orange label "after 3¢ rounding ($4.50, $0.00)".
- **Annotation (bold 12px magenta `#d55181`, near the mid-left):** "the three lines are almost the same line — the crossing point is fragile".
- **Caption (12px `#444`, bottom right):** "shallow crossings slide far; this is what a large condition number looks like".

## Solving the Blends Without an Inverse

**Tags:** `worked example` (blue), `rule of thumb` (green)

- **By hand** — double blend A to get 4 kg + 2 kg = $18.00, then subtract it from blend B
- **What remains** — 0.01 kg of robusta explains the leftover $0.03, so robusta = $3.00/kg
- **Back-substitute** — plug $3.00 into blend A: 2a + 3.00 = 9.00, so arabica = $3.00/kg
- **That is solve** — elimination plus back-substitution is exactly what np.linalg.solve does
- **Digit budget** — float64 carries ~16 digits; each power of ten in the condition number eats one
- **The inverse tax** — routing through A⁻¹ typically burns 1–2 extra digits on top of that loss

*Example (italic):* At condition number 10⁸, solve keeps ~8 correct digits while the inv(A) route keeps ~6 (illustrative).

**Key point:** x = A⁻¹b is math notation, not a computation plan — in code it should always become solve(A, b), never inv(A) @ b.

### Visualization (canvas `c2`, 720×300)

Grouped bar chart: correct digits remaining after solve vs after invert-then-multiply, at four condition numbers, under the ~16-digit float64 ceiling.

- **Title (bold 15px, `#1a5276`, top center):** "Correct Digits Left: solve(A, b) vs inv(A) @ b (illustrative)".
- **Data:** condition numbers `["10²", "10⁵", "10⁸", "10¹¹"]`; solve digits `[14, 11, 8, 5]`; invert digits `[12, 9, 6, 3]`.
- **Axes:** origin pixel (60, 240); y axis up to y=55, range 0–16 digits, ticks at 0, 4, 8, 12, 16 with 1px `#e5e9ef` gridlines; y label 12px `#444` "correct digits"; x axis to x=690.
- **Ceiling:** dashed `#6b7280` (dash 5/4) horizontal line at digits = 16, labeled 12px `#6b7280` "float64 budget ≈ 16 digits" above it on the right.
- **Bars:** four groups centered at x = 140, 295, 450, 605; per group two bars 44px wide with an 8px gap: solve bar fill `rgba(42,120,214,0.55)` with 2px `#2a78d6` outline, invert bar fill `rgba(217,89,38,0.55)` with 2px `#d95926` outline; bold 12px value labels ("14", "12", ...) above each bar in the bar's color; group label ("κ = 10²", etc.) 12px `#444` below the baseline.
- **Legend (12px, top left inside plot):** blue swatch "solve", orange swatch "invert then multiply".
- **Annotation (bold 13px orange `#d95926`, right of the κ = 10¹¹ group):** "inverting throws away ~2 extra digits at every κ".
- **Caption (12px `#444`, bottom right):** "each power of ten in κ costs about one digit; the inverse route pays a surcharge".

## Where Regression Hits It

**Tags:** `where it's used` (blue), `regression` (orange), `failure mode` (red)

- **The formula** — every textbook prints β = (XᵀX)⁻¹Xᵀy, which both forms XᵀX and inverts it
- **Squaring** — forming XᵀX squares the condition number: κ(X) = 10³ becomes κ(XᵀX) = 10⁶
- **Twin features** — near-duplicate columns (height in cm and inches) inflate κ(X) from the start
- **The fix** — np.linalg.lstsq factors X directly (SVD under the hood) and never forms XᵀX
- **The symptom** — coefficients that flip sign or explode when a handful of rows change slightly

*Example (italic):* With κ(X) = 10³, lstsq keeps ~13 correct digits; the normal-equations route keeps ~10 (illustrative).

**Key point:** If regression coefficients swing wildly between nearly identical reruns, suspect conditioning and the (XᵀX)⁻¹ recipe before suspecting the data.

### Visualization (canvas `c3`, 720×300)

Dual-panel bar chart: forming XᵀX squares the condition number (left); the digits that squaring plus inverting costs the fitted coefficients (right); dashed divider at x=360.

- **Title (bold 15px, `#1a5276`, top center):** "Normal Equations Square the Condition Number (illustrative)".
- **Left panel (condition number):** origin pixel (60, 240), panel width 250, chart height 175 (top y=65), y range 0–8 in log10-of-κ units with ticks 0, 2, 4, 6, 8 and 1px `#e5e9ef` gridlines; y label 12px `#444` "powers of ten in κ"; two bars 70px wide centered at x = 130 and 250: "X" bar height 3 in fill `rgba(42,120,214,0.55)` with 2px `#2a78d6` outline, "XᵀX" bar height 6 in fill `rgba(217,89,38,0.55)` with 2px `#d95926` outline; bold 12px labels "κ = 10³" and "κ = 10⁶" above the bars in matching colors; x labels "X" / "XᵀX" 12px `#444` below the baseline; bold 12px magenta `#d55181` annotation between the bars: "squared".
- **Right panel (digits kept):** origin pixel (410, 240), panel width 250, same height, y range 0–16 with ticks 0, 4, 8, 12, 16; y label "correct digits in β"; two bars 70px wide centered at x = 480 and 600: "lstsq on X" bar height 13 in fill `rgba(0,131,0,0.5)` with 2px `#008300` outline, "(XᵀX)⁻¹Xᵀy" bar height 10 in fill `rgba(213,81,129,0.45)` with 2px `#d55181` outline; bold 12px value labels "13" and "10" above the bars in matching colors; x labels 11px `#444` below the baseline.
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=360 from y=40 to h−12.
- **Caption (12px `#444`, bottom center):** "same data, same model — the recipe alone decides how many digits survive".

## The Inverse Nobody Needed

**Tags:** `common mistake` (red), `rule of thumb` (green)

- **The excuse** — "I have many right-hand sides, so I need A⁻¹" — no: factor A once and reuse it
- **The flop bill** — at n = 2,000: LU solve ≈ 5.3 GFLOP, invert-then-multiply ≈ 16 GFLOP (~3×)
- **Reuse** — after one factorization, each extra right-hand side costs ≈ 0.008 GFLOP — near free
- **Sparsity** — a mostly-zero A usually has a fully dense A⁻¹, so inverting can explode memory too
- **Real exceptions** — compute A⁻¹ only when its entries are themselves the answer you must report

*Example (italic):* An analyst cached inv(A) "for speed" and paid 3× the flops up front for answers with fewer correct digits.

**Common mistake:** Caching inv(A) "for reuse". The reusable object is the factorization — factor once, then every new b is a cheap pair of triangular solves.

### Visualization (canvas `c4`, 720×300)

Horizontal bar chart: the arithmetic cost of three routes through the same n = 2,000 system, showing the inverse costs ~3× the honest solve and each reused solve is near free.

- **Title (bold 15px, `#1a5276`, top center):** "Cost per Route, n = 2,000 System (GFLOP)".
- **Data:** routes and costs — "invert A, then A⁻¹b" = 16.0; "LU factor + solve one b" = 5.3; "each extra b (factors reused)" = 0.008.
- **Layout:** bars start at x=250 with max length 420px scaled to 16 GFLOP (26.25 px per GFLOP), 26px tall, at y = 80, 140, 200; route labels right-aligned 12px `#444` at x=240 beside each bar.
- **Bar 1 (invert):** length 420px, fill `rgba(217,89,38,0.55)` with 2px `#d95926` outline; bold 12px orange value label "16.0 GFLOP" just past the bar end.
- **Bar 2 (LU solve):** length 139px, fill `rgba(42,120,214,0.55)` with 2px `#2a78d6` outline; bold 12px blue label "5.3 GFLOP".
- **Bar 3 (extra b):** true length <1px, drawn at the 5px minimum, fill `rgba(0,131,0,0.6)` with 2px `#008300` outline; bold 12px green label "0.008 GFLOP — near free".
- **Annotation (bold 13px magenta `#d55181`, under the bars at y=250, centered):** "~3× the work for a less accurate answer — solve, don't invert".
- **Caption (12px `#444`, bottom right):** "LU ≈ (2/3)n³ flops; explicit inverse ≈ 2n³; a reused solve ≈ 2n²".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (here it hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- All chart data is hardcoded literal arrays (no randomness); invented numbers are labeled "illustrative" in titles or captions.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
