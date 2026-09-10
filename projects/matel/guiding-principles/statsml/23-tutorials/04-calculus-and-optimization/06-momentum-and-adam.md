# Momentum & Adam

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Momentum & Adam

**Subtitle:** Plain gradient descent bounces across narrow valleys instead of moving forward — momentum remembers direction to cancel the bounce, and Adam gives every knob its own step size

## A Hiker in a Foggy Canyon

**Tags:** `core idea` (blue), `gradient descent` (green), `zigzag problem` (orange)

- **The hiker** — a hiker in thick fog descends a long narrow canyon by feeling the slope underfoot
- **The rule** — every step goes straight downhill from where she stands; that rule is gradient descent
- **The shape** — the canyon walls slope 3 units sideways but the floor slopes only 0.3 toward the exit
- **The zigzag** — steepest-downhill points at the far wall, so she bounces side to side and drifts slowly
- **The cost** — after 10 honest downhill steps she is only about halfway along the canyon floor

*Example (italic):* Training a spam filter is the same walk — thousands of knobs, and the loss surface is full of narrow canyons exactly like this one.

**Key point:** Plain gradient descent only knows the slope right underfoot, so a narrow canyon makes it spend its steps bouncing sideways instead of moving toward the goal.

### Visualization (canvas `c1`, 720×300)

Top-view contour map of the canyon with the hardcoded zigzag path of plain gradient descent bouncing wall to wall while drifting slowly toward the low point.

- **Title (bold 15px, `#1a5276`, top center):** "One Foggy Canyon: Plain Gradient Descent Bounces Wall to Wall".
- **Contours:** five ellipses centered at (500, 168) with (rx, ry) pairs (60, 15), (120, 30), (180, 45), (240, 60), (300, 75); stroke 1.5px `#e5e9ef`; the outermost ring labeled 11px `#6b7280` "canyon wall" near its top edge.
- **Goal:** green `#008300` 6px dot at (500, 168) with bold 12px green label "lowest point" to its right.
- **Path data (pixel coords):** `[[210,118],[232,208],[252,128],[270,196],[286,136],[300,188],[313,142],[325,182],[336,147],[346,177],[355,151]]` — 11 points = 10 steps; blue `#2a78d6` 2.5px polyline with 3.5px dots; start point drawn 5px with bold 12px blue label "start" above it.
- **Annotation (magenta `#d55181`, bold 12px, near (330, 92), two lines):** "10 steps: only halfway —" / "most effort wasted bouncing".
- **Annotation (orange `#d95926`, bold 12px, near (150, 240)):** "sideways slope ≈ 10× the forward slope".
- **Caption (12px `#444`, bottom left):** "top view; rings are equal-height contours (illustrative)".

## Momentum: Steps That Remember

**Tags:** `worked example` (blue), `momentum` (green), `rule of thumb` (orange)

- **The fix** — keep a running velocity instead of a fresh step: new = 0.9 × old + today's slope
- **Cancel** — the wall slope flips sign every step (+3, −3, ...), so the memory shrinks it toward ±1.6
- **Add up** — the floor slope is a steady 0.3, so velocity compounds toward 3.0 (= 0.3 / (1 − 0.9))
- **The flip** — raw slopes favor the wall (3 vs 0.3); with momentum the floor wins (3.0 vs 1.6)
- **The picture** — it behaves like a heavy rolling ball: side bumps cancel, the steady tilt adds speed

*Example (italic):* After 10 steps the sideways velocity has fallen 3.00 → 1.03 while the forward velocity has climbed 0.30 → 1.95 — you can redo both by hand with new = 0.9 × old + slope.

**Key point:** Momentum averages recent slopes: pulls that keep flipping direction cancel out, while a consistent pull compounds up to 10× (= 1/(1 − 0.9)) its raw size.

### Visualization (canvas `c2`, 720×300)

Dual-panel line chart of the momentum velocity over 10 steps: the alternating wall direction shrinking (left) vs the steady floor direction compounding (right), split by a dashed divider at x=360.

- **Title (bold 15px, `#1a5276`, top center):** "Momentum Velocity: Bounces Cancel, Steady Pull Compounds".
- **Left panel (wall direction):** heading bold 12px `#444` "sideways velocity — slope alternates +3 / −3"; axis origin x=55, width 280, zero line at y=155, half-height 90 mapping ±3.5; steps 1–10 on the x axis (12px `#444` labels); orange `#d95926` 2.5px line with 3.5px dots through `[3.00, -0.30, 2.73, -0.54, 2.51, -0.74, 2.33, -0.90, 2.19, -1.03]`; dashed `#d55181` guide lines at +1.58 and −1.58; magenta bold 12px annotation "bounce settles at ±1.6".
- **Right panel (floor direction):** heading "forward velocity — steady slope 0.3"; axis origin x=400, width 280, baseline y=245, chart height 180, y scale 0–3.2; green `#008300` 2.5px line with 3.5px dots through `[0.30, 0.57, 0.81, 1.03, 1.23, 1.41, 1.57, 1.71, 1.84, 1.95]`; dashed green line at 3.0 labeled bold 12px green "ceiling 3.0 = 0.3/(1−0.9)"; green bold 13px annotation "keeps building".
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=360 from y=38 to h-12.

## Adam: One Ruler per Knob

**Tags:** `adam` (blue), `where it's used` (green), `per-knob step size` (orange)

- **Leftover problem** — momentum still uses one step size for every knob, and knobs differ wildly
- **Adam's trick** — divide each knob's step by the typical (RMS) size of its own recent slopes
- **The arithmetic** — wall: 3 ÷ 3 = 1; floor: 0.3 ÷ 0.3 = 1 — both knobs now move at the same pace
- **The step** — with learning rate 0.05, every knob steps about 0.05 whatever its raw slope size
- **Plus momentum** — Adam keeps the velocity memory too: it is momentum plus per-knob scaling
- **The default** — this is why Adam (lr 0.001, betas 0.9 / 0.999) is the usual first choice for neural nets

*Example (italic):* A model where one knob's slopes run ~100× another's still trains evenly — Adam hands each knob its own ruler instead of one shared step size.

**Key point:** Adam ≈ momentum + a per-knob step size, so no single learning rate has to fit the steepest and the flattest parameter at the same time.

### Visualization (canvas `c3`, 720×300)

Dual-panel bar chart: the two knobs' raw slopes 10× apart (left) vs their actual Adam steps coming out equal (right), split by a dashed divider at x=360.

- **Title (bold 15px, `#1a5276`, top center):** "Adam Gives Each Knob Its Own Ruler".
- **Left panel (raw slope):** heading bold 12px `#444` "raw slope per knob"; axis origin x=70, width 250, baseline y=235, chart height 165, y scale 0–3.3; two bars 60px wide: "wall knob" value 3.0 fill `rgba(217,89,38,0.55)`, "floor knob" value 0.3 fill `rgba(42,120,214,0.55)`; knob names 12px `#444` below the baseline; bold 12px value labels "3.0" and "0.3" above each bar; magenta `#d55181` bold 13px annotation "10× apart".
- **Right panel (Adam step):** heading "actual Adam step per knob (lr = 0.05)"; axis origin x=415, width 250, same baseline/height, y scale 0–0.065; both bars value 0.05 fill `rgba(25,158,112,0.55)`; bold 12px value labels "0.05" and "0.05"; green `#008300` bold 13px annotation "equal pace: slope ÷ its own RMS = 1"; caption 12px `#444` "step = lr × slope ÷ RMS(recent slopes)".
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=360 from y=38 to h-12.

## The Wrong Fix: Just Crank the Learning Rate

**Tags:** `common mistake` (red), `learning rate` (orange), `rule of thumb` (green)

- **The temptation** — the zigzag looks like "steps too small", so people simply raise the learning rate
- **What happens** — bigger steps overshoot the steep walls harder; the bounce grows and loss blows up
- **The numbers** — small rate takes loss 100 → 25 in 12 steps; a 10× rate swings it up to 300 and diverges
- **Momentum instead** — the same small rate with momentum takes loss 100 → 2: fast floor, calm walls
- **Not magic** — momentum can overshoot a sharp minimum too; Adam's defaults are good, not guaranteed

*Example (italic):* A team "fixed" slow training by 10×-ing the learning rate and watched the loss curve turn into a growing sawtooth instead of a descent.

**Common mistake:** Reading a zigzagging loss as "learning rate too small". The real problem is that consecutive steps disagree on direction — momentum fixes that; a bigger rate only amplifies the disagreement.

### Visualization (canvas `c4`, 720×300)

Single-panel line chart of loss per step for three runs on the same canyon: small learning rate (slow), 10× learning rate (diverging sawtooth), and momentum with the small rate (fast).

- **Title (bold 15px, `#1a5276`, top center):** "Three Ways Down: Loss per Step".
- **Axes:** origin x=60, width 620, baseline y=250, chart height 195; x = steps 0–12 with tick labels 0, 4, 8, 12 (12px `#444`); y = loss 0–320 with tick labels 0, 100, 200, 300; light gridlines `#e5e9ef` at each y tick.
- **Small rate (blue `#2a78d6`, 2.5px line, 3px dots):** `[100, 88, 78, 69, 61, 54, 48, 43, 38, 34, 31, 28, 25]` for steps 0–12; blue bold 12px label near its end "small rate: safe but slow (100 → 25)".
- **10× rate (orange `#d95926`, 2.5px line, 3px dots):** `[100, 62, 118, 55, 150, 48, 210, 40, 300]` for steps 0–8, ending with a short upward orange arrow at the last point; orange bold 13px label "10× rate: bounce grows — diverges".
- **Momentum (green `#008300`, 2.5px line, 3px dots):** `[100, 82, 60, 40, 25, 15, 9, 6, 4, 3, 2.5, 2.2, 2.0]` for steps 0–12; green bold 13px label "momentum, same small rate (100 → 2)".
- **Caption (12px `#444`, bottom left):** "same canyon, three optimizers (illustrative)".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (here it hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
