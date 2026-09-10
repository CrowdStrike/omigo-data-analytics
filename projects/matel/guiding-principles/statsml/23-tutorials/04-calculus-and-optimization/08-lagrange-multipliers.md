# Lagrange Multipliers

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Lagrange Multipliers

**Subtitle:** To optimize while stuck on a constraint, walk along it until the objective's gradient lines up with the constraint's gradient — at that point no allowed step can improve you

## A Fixed Fence, the Biggest Garden

**Tags:** `core idea` (blue), `constraint` (orange), `contour lines` (green)

- **The gardener** — 40 m of fence for a rectangle, so width + height must equal 20 m
- **The goal** — choose width and height on that line to make area = width × height as big as possible
- **Contours** — curves of equal area (64, 96, 100 m²) are hyperbolas fanning out from the origin
- **The touch** — the best contour is the biggest one still touching the fence line: area 100 m²
- **Tangency** — at the touching point (10, 10) the contour and the fence line are tangent, not crossing

*Example (italic):* A 4×16 garden and a 12×8 garden use the same 40 m of fence but give only 64 and 96 m² — the 10×10 square gives 100.

**Key point:** On a constraint, the optimum sits where the objective's contour just kisses the constraint line — wherever a contour crosses it, sliding along the fence still improves the area.

### Visualization (canvas `c1`, 720×300)

Contour plot: area hyperbolas over the (width, height) plane with the fence line w + h = 20, tangent at (10, 10).

- **Title (bold 15px, `#1a5276`, top center):** "Area Contours Meeting the Fence Line w + h = 20".
- **Plot area:** axis origin x=210, baseline y=255, plot width 340, plot height 205; both axes span 0–24 (units: meters); axis lines 2px ink `#1a5276`; ticks and labels 0, 10, 20 (12px `#444`); axis captions "width w (m)" below and "height h (m)" rotated left.
- **Contours (hardcoded):** for each level A in `[64, 96, 100, 144]` draw h = A/w sampled at w = `[3, 3.5, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 18, 20, 22, 24]`, clipping points with h > 24; levels 64 and 96 in 2px mute `#6b7280`, level 100 in 3px green `#008300`, level 144 in 2px dashed (dash 4/3) `#bdc3c7`; small 11px level labels "64", "96", "100", "144" at each curve's upper-left end.
- **Fence line:** blue `#2a78d6` 3px segment from (w=0, h=20) to (w=20, h=0), labeled "w + h = 20" bold 12px blue near (2, 18).
- **Tangency point:** magenta `#d55181` 6px dot at (10, 10); magenta bold 13px annotation "A = 100 just touches here" with a short 1px leader line.
- **Second annotation (11px `#6b7280`):** "A = 144 never reaches the fence" near the dashed curve.
- **Caption (12px `#444`, bottom):** "bigger contours = more area; the best one is tangent to the constraint".

## Checking the Gradients by Hand

**Tags:** `worked example` (blue), `gradients` (green)

- **Two arrows** — area's gradient at (w, h) is (h, w); the fence line w + h = 20 has gradient (1, 1)
- **At (4, 16)** — area gradient (16, 4) is not parallel to (1, 1), so sliding along the fence helps
- **At (16, 4)** — area gradient (4, 16) tilts the other way; sliding back toward square helps
- **At (10, 10)** — area gradient (10, 10) = 10 × (1, 1): the arrows align and no slide improves
- **The recipe** — solve (h, w) = λ(1, 1) together with w + h = 20: it gives w = h = λ = 10

*Example (italic):* At (4, 16) the area is 64; slide one meter along the fence to (5, 15) and it jumps to 75 — the unaligned gradient was pointing the way.

**Key point:** "Gradients align" means ∇f = λ∇g: the objective pulls straight off the constraint surface, so its component along the constraint is zero and walking farther gains nothing.

### Visualization (canvas `c2`, 720×300)

The fence line drawn in the (w, h) plane with gradient arrow pairs at three points; only the middle pair is parallel.

- **Title (bold 15px, `#1a5276`, top center):** "Gradient of Area vs Gradient of the Fence at Three Points".
- **Plot area:** axis origin x=210, baseline y=255, plot width 340, plot height 205, both axes 0–24; same axis styling as `c1` (2px ink, ticks at 0, 10, 20, captions "width w (m)" / "height h (m)").
- **Fence line:** ink `#1a5276` 2px from (0, 20) to (20, 0).
- **Anchor points (hardcoded):** (4, 16), (10, 10), (16, 4); 5px dots — mute `#6b7280` at the outer two, green `#008300` at (10, 10).
- **Area-gradient arrows (blue `#2a78d6`, 3px, arrowheads):** from each point along its (h, w) direction scaled 3.5 px per unit: at (4, 16) direction (16, 4); at (10, 10) direction (10, 10); at (16, 4) direction (4, 16).
- **Constraint-gradient arrows (orange `#d95926`, 2px dashed, arrowheads):** from each point along (1, 1), drawn 50 px long.
- **Alignment annotation (green bold 13px):** "aligned: (10, 10) = 10 × (1, 1) → λ = 10" pointing at the middle pair; mute 11px labels "not parallel — keep sliding" at the outer two.
- **Legend (12px, top-right):** blue swatch "∇area = (h, w)", orange dashed swatch "∇fence = (1, 1)".

## Lambda Is a Price

**Tags:** `where it's used` (blue), `shadow price` (orange)

- **One more meter** — raise the budget w + h from 20 to 21 and the best garden becomes 10.5 × 10.5
- **The payoff** — best area goes 100 → 110.25, a gain of 10.25 m² — almost exactly λ = 10
- **Shadow price** — λ is what one extra unit of constraint budget is worth to the objective
- **In ML** — ridge and lasso penalty weights act as multipliers on a coefficient-size budget
- **Elsewhere** — SVM margins, max-entropy models, and portfolio budgets run on the same trick

*Example (italic):* Budgets 18, 19, 20, 21, 22 give best areas 81, 90.25, 100, 110.25, 121 — each extra meter of budget buys about 10 m², which is λ.

**Key point:** λ is not algebra debris — it is the exchange rate between loosening the constraint by one unit and improving the objective, which is why regularization knobs are multipliers in disguise.

### Visualization (canvas `c3`, 720×300)

Line chart of constraint budget vs best achievable area, with a slope triangle showing the marginal gain equals λ.

- **Title (bold 15px, `#1a5276`, top center):** "Best Area vs Budget: the Slope Is λ".
- **Data (hardcoded):** budgets `[18, 19, 20, 21, 22]`, best areas `[81, 90.25, 100, 110.25, 121]`.
- **Axes:** origin x=90, baseline y=245, plot width 560, plot height 180; x from 17.5 to 22.5 with ticks at each budget (12px `#444`); y from 75 to 125 with ticks 80, 100, 120; axis captions "budget b = w + h (m)" and "best area (m²)".
- **Curve:** green `#008300` 3px line through the five points with 5px dots; 12px `#444` value labels "81", "90.25", "100", "110.25", "121" above each dot.
- **Slope triangle:** dashed magenta `#d55181` right triangle between the points (20, 100) and (21, 110.25) — horizontal leg labeled "+1 m", vertical leg labeled "+10.25 m²" (bold 12px magenta).
- **Annotation (magenta bold 13px):** "slope ≈ λ = 10: lambda prices the constraint".
- **Caption (12px `#444`, bottom):** "best area = (b/2)², so the slope at b = 20 is exactly b/2 = 10".

## Aligned Does Not Mean Best

**Tags:** `common mistake` (red), `rule of thumb` (green)

- **Flat spots** — alignment marks every stationary point on the constraint: maxima, minima, saddles
- **The hill** — walking the fence line, area rises 64, 96, 100 and then falls 96, 64: (10, 10) is a top
- **Other problems** — the same equations elsewhere can hand you the worst point, not the best
- **The check** — compare the objective's value at every aligned candidate (and any endpoints)
- **λ = 0** — a zero multiplier means the constraint is not actually binding at that point

*Example (italic):* Solving ∇f = λ∇g on a circle constraint typically returns two points — one is the maximum, the other is the minimum.

**Common mistake:** Treating any solution of ∇f = λ∇g as the answer. Alignment only nominates candidates — evaluate f at each one before declaring a winner.

### Visualization (canvas `c4`, 720×300)

Profile of the area as you walk along the fence line, showing the aligned point as a flat spot on a hill.

- **Title (bold 15px, `#1a5276`, top center):** "Area While Walking the Fence Line w + h = 20".
- **Data (hardcoded):** w = `[2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18]`, area = w(20−w) = `[36, 51, 64, 75, 84, 91, 96, 99, 100, 99, 96, 91, 84, 75, 64, 51, 36]`.
- **Axes:** origin x=80, baseline y=245, plot width 570, plot height 180; x from 2 to 18 with ticks at 2, 6, 10, 14, 18 (12px `#444`); y from 0 to 110 with ticks 0, 50, 100; captions "width w (m), height = 20 − w" and "area (m²)".
- **Curve:** blue `#2a78d6` 3px line through all 17 points.
- **Flat tangent:** green `#008300` dashed (dash 4/3) horizontal line at area = 100 from w = 8 to w = 12; green 6px dot at (10, 100); green bold 13px annotation "aligned point = flat spot (here, the top)".
- **Reference dots:** mute `#6b7280` 5px dots at (4, 64) and (16, 64) with 11px labels "64".
- **Warning annotation (magenta `#d55181` bold 12px, two lines):** "on other problems the flat spot" / "can be a valley — compare candidates".
- **Caption (12px `#444`, bottom):** "λ finds where the walk is flat; you still decide top vs bottom".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (here it hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
