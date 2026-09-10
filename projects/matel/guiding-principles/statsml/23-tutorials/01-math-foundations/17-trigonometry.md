# Trigonometry

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Trigonometry

**Subtitle:** three side ratios in a right triangle, stretched onto a circle so they work for any angle — that one move turns them into slopes, rotations, and waves

## Three Ratios on One Ramp

**Tags:** `core idea` (blue), `running example` (green)

- **The ramp** — a loading ramp rises 3 m over a horizontal run of 4 m, so its slanted face is 5 m
- **Right triangle** — rise, run, and face are the opposite side, adjacent side, and hypotenuse
- **sine** — opposite over hypotenuse: 3 / 5 = 0.6, the share of the face spent going up
- **cosine** — adjacent over hypotenuse: 4 / 5 = 0.8, the share of the face spent going along
- **tangent** — opposite over adjacent: 3 / 4 = 0.75, rise per unit of run, i.e. the slope
- **The angle** — only one angle fits those ratios: 36.87&deg;, recovered as arctan(0.75)

*Example (italic):* Double the ramp to 6 m rise and 8 m run and all three ratios are unchanged — 0.6, 0.8, 0.75 — because ratios ignore size.

**Key point:** sin, cos, and tan are not new operations. They are three divisions between the sides of a right triangle, and they depend on the angle alone, never on the triangle's size.

### Visualization (canvas `c1`, 720&times;300)

Scale drawing of the 3-4-5 ramp with sides labeled, plus a right-side panel of the three ratios computed in JS from the side lengths.

- **Title (bold 15px, `#1a5276`, top center):** "One Ramp, Three Ratios: 3 m Up, 4 m Along, 5 m Across".
- **Scale:** 60 px per metre. Vertices A = (70, 240) bottom-left, B = (310, 240) bottom-right, C = (310, 60) top-right.
- **Triangle:** run A&rarr;B green `#008300` 3px; rise B&rarr;C magenta `#d55181` 3px; hypotenuse A&rarr;C blue `#2a78d6` 3px.
- **Right-angle marker:** 1px `#6b7280` 14 px square tucked inside vertex B.
- **Angle arc:** orange `#d95926` 2px arc of radius 42 at A from 0 to &minus;36.87&deg; (counterclockwise), computed as `Math.atan2(3,4)`; label the computed angle (bold 13px orange) at A + (50, &minus;10) as "36.9&deg;" via `toFixed(1)`.
- **Side labels (bold 12px):** green "run = 4 m (adjacent)" centered below A&rarr;B at y=262; magenta "rise = 3 m" / "(opposite)" left-aligned at x=B+12, two lines near the rise midpoint; blue "face = 5 m (hypotenuse)" rotated-free, centered above the hypotenuse midpoint at (190, 132).
- **Vertex dots:** 5px ink `#1a5276` at A, B, C.
- **Right-side panel (left-aligned at x=400, all values computed from `o=3, a=4, hyp=Math.hypot(3,4)`):** ink bold 13px "Divide the sides:" (y=76); magenta bold 13px "sin = 3/5 = 0.60" (y=106) with mute 12px "opposite / hypotenuse" (y=124); green bold 13px "cos = 4/5 = 0.80" (y=154) with mute 12px "adjacent / hypotenuse" (y=172); blue bold 13px "tan = 3/4 = 0.75" (y=202) with mute 12px "opposite / adjacent = slope" (y=220); orange bold 12px "arctan(0.75) = 36.9&deg;" (y=252) and mute 12px "same ratios at any scale" (y=272).

## Stretching the Ratios Onto a Circle

**Tags:** `core idea` (blue), `common mistake` (red)

- **The limit** — a right triangle exists only for 0&deg;&ndash;90&deg;, so sin 150&deg; has no triangle
- **The fix** — put a point on a circle of radius 1 and let cos be its x, sin its y
- **Still consistent** — inside the first quarter turn the point's legs form exactly the old triangle
- **Any angle now** — 150&deg;, 210&deg;, &minus;40&deg;, or 400&deg; all name a point, so all have sin and cos
- **Signs come free** — y below the centre makes sin negative; x left of centre makes cos negative
- **Symmetry visible** — sin 150&deg; = sin 30&deg; = 0.5 because both points sit at the same height

*Example (italic):* sin 210&deg; = &minus;0.5 because the point at 210&deg; sits half a radius below the centre — no triangle needed, just a coordinate.

**Common mistake:** Treating sin and cos as triangle-only objects. SOH-CAH-TOA is the special case; the circle is the definition, and it never runs out of angles.

### Visualization (canvas `c2`, 720&times;300)

Unit circle with axes through the centre, four points at 30&deg;/150&deg;/210&deg;/330&deg; pinned to the &plusmn;0.5 height lines, plus a right-side reading panel whose values are computed with `Math.sin`.

- **Title (bold 15px, `#1a5276`, top center):** "Past 90&deg; the Triangle Stops, the Circle Keeps Going".
- **Circle:** centre cx=210, cy=168, radius R=100, 2px `#c9d4de`; 1px `#ccc` horizontal axis cx&minus;R&minus;24 to cx+R+24 and vertical axis cy&minus;R&minus;18 to cy+R+18.
- **Height lines:** dashed `#bbb` 1px (dash 4/3) at +0.5R and &minus;0.5R, spanning cx&minus;R&minus;10 to cx+R+10; mute 11px labels "+0.5" and "&minus;0.5" left-aligned at cx+R+14.
- **Four points (6px dots, bold 12px labels, offsets dx/dy):** 30&deg; blue `#2a78d6` (12, &minus;8); 150&deg; green `#008300` (&minus;46, &minus;8); 210&deg; violet `#4a3aa7` (&minus;50, 16); 330&deg; orange `#d95926` (12, 16). Position x = cx + R&middot;cos(a), y = cy &minus; R&middot;sin(a).
- **Right-side reading panel (left-aligned at x=445, each sin value printed via `Math.sin(a).toFixed(2)`):** ink bold 13px "sin is the height, cos is the width:" (y=72); blue bold 13px "sin 30&deg; = +0.50" (y=100); green "sin 150&deg; = +0.50 (same height)" (y=124); violet "sin 210&deg; = &minus;0.50 (below centre)" (y=148); orange "sin 330&deg; = &minus;0.50" (y=172); mute 12px "A right triangle exists only for 0&deg;&ndash;90&deg;," (y=208) / "so the triangle rule is silent past it." (y=226); magenta `#d55181` bold 12px "The circle answers every angle &mdash;" (y=254) / "signs and symmetries come with the picture." (y=272).

## Radians: Angle Measured in Radii

**Tags:** `rule of thumb` (green), `where it's used` (blue)

- **Degrees** — a full turn cut into 360 arbitrary pieces, a Babylonian counting convention
- **Radians** — the angle whose arc along the circle is exactly one radius long, no arbitrary cut
- **Full turn** — the circumference is 2&pi; radii, so one turn is 2&pi; = 6.2832 radians
- **Conversion** — multiply degrees by &pi;/180: 90&deg; = 1.5708 rad, 45&deg; = 0.7854 rad
- **One radian** — 180/&pi; = 57.2958&deg;, an awkward number in degrees but the natural unit
- **Code default** — NumPy and JavaScript sine expect radians, so degrees silently give wrong answers
- **Small angles** — in radians sin&theta; &asymp; &theta;: at 5&deg; the two differ by 0.13%

*Example (italic):* `Math.sin(30)` returns &minus;0.988 because 30 is read as 30 radians (about 4.8 turns), not 30 degrees.

**Key point:** Radians exist so arc length, angle, and the derivative of sine all agree without a conversion factor. Degrees are for humans; radians are for the math and the library call.

### Visualization (canvas `c3`, 720&times;300)

Circle with one radius-length arc highlighted to define a radian, plus a conversion table whose radian column is computed as `d*Math.PI/180`.

- **Title (bold 15px, `#1a5276`, top center):** "One Radian: the Angle Whose Arc Is One Radius Long".
- **Circle:** centre cx=190, cy=170, R=98, 2px `#c9d4de`; 4px ink centre dot.
- **Radii:** blue `#2a78d6` 3px line from centre to angle 0 (right), labeled bold 12px blue "radius r" centered at (cx+R/2, cy+18); blue 3px line from centre to angle 1 rad, labeled bold 12px blue "r" near its midpoint offset (&minus;6, &minus;10).
- **Highlighted arc:** orange `#d95926` 5px arc of radius R from 0 to &minus;1 rad (counterclockwise); bold 13px orange label "arc length = r" at (cx + (R+34)&middot;cos(0.5), cy &minus; (R+34)&middot;sin(0.5)), centered.
- **Angle arc:** mute `#6b7280` 1.5px arc of radius 34 from 0 to &minus;1 rad; mute bold 12px "1 rad" at (cx+46, cy&minus;16), left-aligned.
- **Caption under circle (centered at cx):** violet `#4a3aa7` bold 12px "1 rad = 180/&pi; = 57.30&deg;" (y=cy+R+34, value printed via `(180/Math.PI).toFixed(2)`); mute 12px "a full turn = 2&pi; = 6.2832 rad" (y=cy+R+52, printed via `(2*Math.PI).toFixed(4)`).
- **Conversion table (left-aligned, x=430 for degrees, x=560 for radians):** ink bold 13px headers "degrees" and "radians" (y=72) with a 1px `#ccc` rule at y=80 from x=425 to x=690; rows for `[30, 45, 60, 90, 180, 360]` at y = 104 + 26&middot;i, degrees in `#2c3e50` 12px as "N&deg;", radians in blue bold 12px as `(d*Math.PI/180).toFixed(4)`.
- **Footer note (left-aligned x=430, y=274):** magenta `#d55181` bold 12px "library functions take the right-hand column".

## Tangent Is Just Slope

**Tags:** `worked example` (blue), `where it's used` (orange)

- **Same ratio** — tan&theta; = rise / run, which is the definition of the slope of a line
- **Both directions** — tan turns an angle into a slope; arctan turns a slope back into an angle
- **Checkpoints** — tan 30&deg; = 0.577, tan 45&deg; = 1, tan 60&deg; = 1.732, tan 75&deg; = 3.732
- **Blow-up** — a vertical line has infinite slope: tan 90&deg; is undefined, tan 89&deg; is 57.29
- **Not linear** — doubling the angle from 30&deg; to 60&deg; triples the slope, so slope averages mislead
- **In data work** — arctan reads the angle of a fitted line, a gradient, or a heading

*Example (italic):* A regression line with slope 0.75 tilts at arctan(0.75) = 36.87&deg;, the very same angle as the 3-4-5 ramp.

**Key point:** Slope and angle are the same fact in two units, and tan / arctan is the exchange rate — nonlinear, so equal angle steps are not equal slope steps.

### Visualization (canvas `c4`, 720&times;300)

Left: four rays from a corner at 30&deg;/45&deg;/60&deg;/75&deg; with their computed slopes. Right: the tan curve from 0&deg; to 88&deg; with the same four checkpoints marked, all labels computed with `Math.tan`.

- **Title (bold 15px, `#1a5276`, top center):** "tan&theta; = rise / run: the Angle-to-Slope Exchange Rate".
- **Left panel:** origin at (75, 250), a 1px `#999` L-shaped frame 190 px wide and 170 px tall; mute 11px "run" centered at (170, 268).
- **Rays:** angles `[30, 45, 60, 75]` with colors blue `#2a78d6`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`; each ray 2.5px, clipped to the frame (length = min(190/cos, 170/sin)); bold 11px label at 0.92 of the ray, in the ray's color, reading "N&deg;: slope M" where M = `Math.tan(rad).toFixed(2)`.
- **Right panel:** axis origin x=380, width 300, baseline y=250, chart height 195; x maps 0&deg;&ndash;90&deg;, y maps slope 0&ndash;5; 1px `#999` L-shaped axes; x ticks `[0, 30, 45, 60, 75, 90]` as "N&deg;" (11px `#444`, below baseline); y ticks `[0, 1, 2, 3, 4, 5]` right-aligned 11px `#444` at x=376 with dashed `#e5e9ef` 1px gridlines.
- **Axis captions:** mute 12px "angle" centered at (530, h&minus;6); mute 12px "slope = tan&theta;" left-aligned at (384, 62).
- **Curve:** ink `#1a5276` 2.5px plotting tan(&theta;) for &theta; = 0&deg;&ndash;88&deg; in 0.5&deg; steps, clipped at slope 5.
- **Vertical asymptote:** dashed red `#e74c3c` 1.5px (dash 5/4) at 90&deg;; bold 12px red rotated-free label "tan 90&deg; undefined" left-aligned at (x-of-76&deg;, 84) on two lines.
- **Checkpoint dots:** same four angles, 5px dots in matching ray colors, each with a bold 12px label above reading the computed `Math.tan` value to 3 decimals ("0.577", "1.000", "1.732", "3.732").
- **Annotation (bold 12px orange, left-aligned at (x-of-32&deg;, 232)):** "30&deg;&rarr;60&deg; doubles the angle but triples the slope".

## The Wave View and Where Data Meets It

**Tags:** `where it's used` (blue), `worked example` (green)

- **Feed an angle that grows** — plot sin of a steadily rising angle and a repeating wave appears
- **Amplitude** — half the peak-to-trough distance, how far the wave swings from its centre
- **Period** — the input span for one full repeat, 360&deg; or 2&pi; in raw angle terms
- **Phase** — a horizontal shift; cos is sin shifted a quarter period, nothing more
- **Seasonality fit** — add sin and cos of 2&pi;&middot;month/12 as features; regression finds the swing
- **Rotations** — turning a point by &theta; mixes its coordinates with cos&theta; and sin&theta; weights
- **Fourier view** — any repeating signal is a sum of such waves, which is what a spectrum plots

*Example (italic):* Fitting sin and cos of 2&pi;&middot;month/12 to 24 months of orders gives a centre of 100.3, an amplitude of 24.3, and a peak at month 6.0.

**Key point:** One pair of sin/cos features captures a yearly cycle with two numbers instead of eleven month dummies — the wave view is what makes that compression possible.

### Visualization (canvas `c5`, 720&times;320)

Monthly order counts for 24 months as dots, with a fitted sine curve overlaid; centre, amplitude, peak month, and period labels are all computed in JS from the plotted array.

- **Title (bold 15px, `#1a5276`, top center):** "Two Features, sin and cos of the Month, Fit a Yearly Cycle".
- **Caption (mute 11px, centered, y=h&minus;6):** "month index (illustrative example, fixed data)".
- **Data (hardcoded literal array, 24 values, no `Math.random`):** `[78.4, 82.8, 83.0, 96.7, 117.9, 123.6, 119.1, 121.3, 118.5, 98.9, 81.8, 81.0, 80.0, 74.4, 83.5, 104.9, 115.2, 116.0, 123.7, 127.6, 112.2, 94.1, 89.3, 83.8]`.
- **Fit computed at render time:** `w = 2*Math.PI/12`; `mean` = average of the array; `b1 = (2/24)*&Sigma; y&middot;sin(w t)`; `b2 = (2/24)*&Sigma; y&middot;cos(w t)`; `amp = Math.hypot(b1, b2)` &rarr; 24.31; `phase = Math.atan2(b2, b1)`; `peak = ((Math.PI/2 - phase)/w + 12) % 12` &rarr; 6.01. Fitted curve value = `mean + b1&middot;sin(w t) + b2&middot;cos(w t)`.
- **Layout:** axis origin x=70, width 590, baseline y=272, chart height 210; x maps month 0&ndash;23, y maps orders 60&ndash;140; 1px `#999` L-shaped axes; x ticks every 3 months as "N" (11px `#444`, below baseline); y ticks `[60, 80, 100, 120, 140]` right-aligned 11px `#444` with dashed `#e5e9ef` 1px gridlines.
- **Centre line:** dashed mute `#6b7280` 1.5px (dash 5/4) at the computed `mean`, labeled bold 12px mute "centre = 100.3" (printed via `mean.toFixed(1)`) left-aligned just above the line at x=74.
- **Data dots:** 4.5px blue `#2a78d6` dots at each month.
- **Fitted curve:** magenta `#d55181` 3px line over t = 0&ndash;23 in 0.1 steps using the computed coefficients.
- **Amplitude bracket:** green `#008300` 2px vertical line at the peak month from `mean` to `mean + amp`, with 5px end ticks; bold 12px green label "amplitude = 24.3" (printed via `amp.toFixed(1)`) left-aligned at (x-of-peak + 8, y-of-(mean+amp/2)).
- **Peak marker:** orange `#d95926` 2px dashed vertical line at the computed peak month down to the baseline; bold 12px orange "peak at month 6.0" (printed via `peak.toFixed(1)`) centered above the curve top.
- **Period bracket:** violet `#4a3aa7` 2px horizontal line at orders 137 from the peak month to peak + 12 with 4px end ticks; bold 12px violet "period = 12 months" centered below it.

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then five `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** logical size passed to a shared `setup(id, W, H)` helper (720&times;300 for `c1`&ndash;`c4`, 720&times;320 for `c5`); it caps display width at the logical width via `style.maxWidth`, sizes the backing store to the rendered CSS width &times; `window.devicePixelRatio`, and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150 ms).
- **Numbers:** no literal statistic is hardcoded into a chart label — ramp ratios come from `o=3, a=4, Math.hypot`, radian conversions from `d*Math.PI/180`, slopes from `Math.tan`, and the seasonal centre/amplitude/peak from the least-squares fit over the hardcoded 24-value array.
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
