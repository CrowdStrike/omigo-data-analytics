# Complex Numbers

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Complex Numbers

**Subtitle:** A complex number is a 2-D point packed into one number — multiplying by e^iθ rotates it by θ, which is why Fourier analysis and eigenvalues cannot live without it (e^iθ rendered as `e<sup>i&theta;</sup>`)

## One Ferris Wheel Cabin, One Number

**Tags:** `core idea` (blue), `the complex plane` (green)

- **The wheel** — a Ferris wheel cabin sits 4 m right of the hub and 3 m above it: two coordinates.
- **One number** — write the pair as 4 + 3i, where the "i part" is simply the up-down coordinate.
- **The plane** — the real axis runs left-right, the imaginary axis runs up-down; a point is a number.
- **Distance** — the cabin hangs √(4²+3²) = 5 m from the hub; that 5 is the number's size.
- **Angle** — the spoke makes about 37° with the ground, so "5 m out at 37°" names the same point.

*Example (italic):* The cabin at (4 m across, 3 m up) is the single complex number 4 + 3i — equally, 5 m out at 37°.

**Key point:** A complex number is nothing exotic — it is a 2-D point written as one number, so ordinary arithmetic can move points around the plane.

### Visualization (canvas `c1`, 720×300)

Complex-plane diagram: the cabin plotted at 4 + 3i inside a dashed Ferris-wheel rim, with a blue spoke arrow, dashed drop lines, an angle arc, and a right-side annotation panel giving both descriptions of the point.

- **Title (bold 15px, ink `#1a5276`, top center):** "The Cabin at 4 + 3i on the Complex Plane".
- **Geometry:** plane center cx=250, cy=172, scale sc=20 px/unit; point at (cx + 4·sc, cy − 3·sc).
- **Wheel rim:** dashed circle (dash 5/4), radius 5 units (100px), 2px grid color `#e5e9ef`.
- **Axes:** 1px `#999`; horizontal from cx−130 to cx+150, vertical from cy+115 to cy−122; labels 12px `#444`: "real (across)" right of the x-axis, "imaginary (up)" above the y-axis.
- **Drop lines:** dashed (3/3) 1px mute `#6b7280` from the point down to the real axis and left to the imaginary axis; labels 12px `#444`: "4" centered below the horizontal run at cy+16, "3i" right-aligned at cx−6.
- **Spoke:** blue `#2a78d6` 3px line from center to point with a size-10 filled arrowhead; cabin dot 6px blue with bold 13px label "cabin = 4 + 3i" at (px+12, py−6).
- **Angle arc:** orange `#d95926` 2px arc radius 34 from 0 to the spoke angle; bold 12px orange label "37°" at (cx+40, cy−10).
- **Distance label:** green `#008300` bold 12px "distance = 5", rotated along the spoke (translated to the spoke midpoint offset (−10, −12)).
- **Right-side annotation (x=500):** ink bold 13px "one complex number," (y=120) / "two equal descriptions:" (y=138); blue 13px "4 + 3i   (across, up)" (y=164); green 13px "5 at 37°  (size, angle)" (y=186).

## A Quarter Turn Is Just "Multiply by i"

**Tags:** `worked example` (blue), `rotation` (green)

- **The move** — the wheel turns a quarter revolution; the cabin at 4 + 3i must land at −3 + 4i.
- **One multiply** — i × (4 + 3i) = 4i + 3i² = −3 + 4i: plain algebra lands exactly there.
- **Why i² = −1** — two quarter turns are a half turn, and a half turn flips every sign.
- **Keep going** — the next multiplies give −4 − 3i, then 3 − 4i, then back to 4 + 3i.
- **No geometry needed** — no sines, no rotation matrix; the arithmetic does the turning for you.

*Example (italic):* One multiplication, i × (4 + 3i) = −3 + 4i, moves the cabin a quarter turn around the hub.

**Key point:** Multiplying by i means "rotate 90°". The famous i² = −1 just says two quarter turns leave you pointing backwards.

### Visualization (canvas `c2`, 720×300)

Wheel diagram showing the cabin's four quarter-turn positions as colored spokes and dots, curved "× i" arrows between consecutive positions, and a hand-computation panel on the right.

- **Title (bold 15px, ink, top center):** 'Each "× i" Turns the Cabin a Quarter Revolution'.
- **Geometry:** center cx=235, cy=172, scale sc=19, rim radius r = 5·sc = 95px.
- **Rim + axes:** dashed (5/4) 2px grid-color circle; 1px `#ccc` axes extending 15px beyond the rim each way.
- **Data (four points):** `[[4,3], [-3,4], [-4,-3], [3,-4]]` with labels `['4 + 3i', '−3 + 4i', '−4 − 3i', '3 − 4i']`, colors `[blue #2a78d6, green #008300, orange #d95926, violet #4a3aa7]`, label offsets `[[14,-4], [-10,-12], [-14,18], [12,18]]`, alignments `['left','right','right','left']`; each drawn as a 2px spoke from center plus a 6px dot and bold 12px label.
- **Quarter-turn arrows:** four curved arcs at radius r+16, each spanning one quarter (start angle atan2(−3,4), trimmed by 0.28 rad at each end), 2px, colored with the destination point's color, size-8 arrowheads; green bold 12px label "× i" at (cx+12, cy−r−22).
- **Hand computation panel (x=462):** ink bold 13px "check the first turn by hand:" (y=92); `#444` 13px lines "i × (4 + 3i)" (y=120), "= 4i + 3i²" (y=143), "= 4i + 3(−1)" (y=166); green bold 13px "= −3 + 4i  ✓" (y=192); magenta `#d55181` bold 12px "four × i steps = full circle," (y=232) / "back to 4 + 3i" (y=250).

## The Spin Dial e^iθ — Fourier's Building Block

(h2 rendered as `The Spin Dial e<sup>i&theta;</sup> — Fourier's Building Block`)

**Tags:** `core idea` (blue), `where it's used` (orange)

- **Any angle** — e^iθ = cos θ + i sin θ is the unit-circle point at angle θ.
- **Rotation dial** — multiplying by e^iθ turns any point by θ; at θ = 90° it is just i.
- **Angles add** — e^iα × e^iβ = e^i(α+β): composing turns is adding exponents.
- **Spin makes waves** — as θ grows, the cabin's shadow on the ground traces exactly cos θ.
- **Payoff** — Fourier writes any signal as a sum of spins; complex eigenvalues mean a system rotates.

*Example (italic):* At θ = 60°, e^iθ = 0.50 + 0.87i — its shadow on the real axis reads exactly cos 60° = 0.50.

**Key point:** e^iθ is rotation stored as a number. Fourier analysis and complex eigenvalues are both "find the hidden spins in the data".

### Visualization (canvas `c3`, 720×300)

Dual diagram: unit circle with the point at θ = 60° and its shadow on the real axis (left), and the cosine wave the shadow traces over 0°–360° (right).

- **Title (bold 15px, ink, top center):** "Spin e^(iθ) Around the Circle — Its Shadow Is a Wave".
- **Left panel (unit circle):** center cx=130, cy=168, radius r=82; 1px `#999` axes extending 18–20px beyond the circle; 2px grid-color circle; θ = π/3 (60°).
- **Radius arm:** blue 3px line from center to point (px = cx + r·cos θ, py = cy − r·sin θ); blue 6px dot; blue bold 12px label "e^(iθ) = 0.50 + 0.87i" at (px−30, py−14).
- **Shadow:** orange dashed (3/3) 2px vertical drop from the point to the axis, plus a solid orange 4px segment from center to (px, cy); orange bold 12px centered label "shadow = 0.50" at (px−8, cy+20).
- **Angle arc:** green 2px arc radius 26 from 0 to −θ; green bold 12px label "θ = 60°" at (cx+30, cy−16).
- **Caption:** 12px `#444` centered "the unit circle" at (cx, cy+r+34).
- **Right panel (cosine wave):** axis origin gx=300, width gw=390, midline gy=168, amplitude amp=78; 1px `#999` horizontal axis and vertical axis (from gy−amp−14 to gy+amp+14); orange 3px curve y = gy − amp·cos(d·π/180) sampled every 3° from 0 to 360.
- **Ticks:** at `[0, 90, 180, 270, 360]` degrees, 8px tick marks, 12px `#444` labels "0°"…"360°" at gy+amp+32.
- **Marked point at 60°:** mute dashed (3/3) drop line from (mx, gy−amp·0.5) to the axis; orange 6px dot; bold 12px left-aligned label "cos 60° = 0.50" at (mx+10, my−8).
- **Annotations:** magenta bold 13px centered "spin → wave: the building block Fourier sums up" at (gx+gw/2, 62); `#444` 12px centered "shadow of the spinning point, as θ grows" at (gx+gw/2, gy+amp+48).

## "Imaginary" Is a Terrible Name

**Tags:** `common mistake` (red), `rule of thumb` (green)

- **Bad branding** — "imaginary" was a 1600s put-down that stuck; i is as real as the up direction.
- **What i is** — the answer to "what move, done twice, flips a sign?" — a quarter turn does exactly that.
- **√−1 panic** — nothing on the number line squares to −1; you need the plane's second direction.
- **Euler, demystified** — e^iπ = −1 only says a 180° turn carries the point 1 onto −1.
- **Silent damage** — discarding an imaginary part mid-computation deletes the rotation it encoded.

*Example (italic):* e^iπ = −1 looks mystical until you read it as: turn the point 1 by half a revolution and you land on −1.

**Common mistake:** Treating i as an error to be squashed. It is a direction. If eigenvalues come out complex, the system genuinely rotates or oscillates — do not throw the imaginary part away.

### Visualization (canvas `c4`, 720×300)

Unit-circle diagram with the three landmark points 1, i, −1, two quarter-turn arc arrows (1 → i, i → −1), and a right-side panel reading the picture as i² = −1 and e^(iπ) = −1.

- **Title (bold 15px, ink, top center):** "Why i² = −1: Two Quarter Turns Point Backwards".
- **Geometry:** center cx=240, cy=172, radius r=88; 1px `#999` axes (horizontal extends 30px beyond the circle, vertical 18px); dashed (5/4) 2px grid-color circle.
- **Landmark points:** `[cx+r, cy]` labeled "1" in blue (offset 14, 22); `[cx, cy−r]` labeled "i" in green (offset 12, −8); `[cx−r, cy]` labeled "−1" in orange (offset −30, 22); each a 7px filled dot with bold 14px label.
- **Arc arrows (radius r+20):** green 3px arc from −0.12 to −π/2+0.14 rad (1 → i) with size-9 green arrowhead and green bold 12px centered label "× i  (turn 90°)" at (cx+r−4, cy−r+4); orange 3px arc from −π/2−0.14 to −π+0.12 rad (i → −1) with size-9 orange arrowhead and orange label "× i again" at (cx−r+4, cy−r+4).
- **Right-side reading panel (x=452):** ink bold 13px "read the picture three ways:" (y=92); `#444` 13px "i × i × 1 = −1" (y=122), "so  i² = −1" (y=145), "two 90° turns = one 180° turn" (y=175); violet `#4a3aa7` bold 13px "e^(iπ) = −1" (y=210); `#444` 13px '"half a revolution (π radians)' (y=233), 'carries 1 onto −1" — no magic' (y=253).

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Math markup:** exponents and symbols are HTML entities/tags in the source — `<sup>i&theta;</sup>`, `&radic;`, `&sup2;`, `&minus;`, `&times;`, `&deg;`, `&alpha;`, `&beta;`, `&pi;`; chart strings use plain-text forms like "e^(iθ)".
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates; shared `arrowHead(ctx, x, y, angle, size, color)` helper draws filled triangular arrowheads (wing spread 0.4 rad). Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
