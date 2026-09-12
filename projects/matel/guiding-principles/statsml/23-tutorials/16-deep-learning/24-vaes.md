# VAEs (Variational Autoencoders)

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** VAEs (Variational Autoencoders)

**Subtitle:** A VAE compresses each example into a small code, but makes every code a fuzzy blob packed around zero — so the whole code space is smooth and any point you sample decodes to something plausible

## A Shoe App with Two Sliders

**Tags:** `core idea` (blue), `latent space` (green), `smooth map` (orange)

- **The app** — a design tool compresses every shoe photo down to two slider numbers: its code
- **Plain autoencoder** — codes land on far-apart islands: sneakers near (3.0, 2.5), boots near (−3.5, 1.5)
- **The gap problem** — pick a point between islands, like (0.5, 0.0), and the decoder outputs garbage
- **The VAE fix** — each shoe becomes a fuzzy blob, and all blobs are packed around (0, 0)
- **Smooth map** — the blobs overlap, so every point in the space decodes to some plausible shoe

*Example (italic):* Dragging the sliders to a spot no training shoe ever occupied still yields a wearable design — because the blobs cover the space between shoes.

**Key point:** An autoencoder learns a code per example; a VAE learns a smooth, packed code space where the points between examples still mean something.

### Visualization (canvas `c1`, 720×300)

Dual-panel 2D scatter: plain-autoencoder codes as far-apart islands (left) vs VAE codes as overlapping fuzzy blobs near the origin (right), split by a vertical dashed divider at x=360.

- **Title (bold 15px, `#1a5276`, top center):** "Shoe Codes: Autoencoder Islands vs VAE Fuzzy Map".
- **Left panel (islands):** plot origin x=55, width 280, baseline y=245, chart height 185; both axes span −4 to 4; light `#e5e9ef` gridlines at 0 only, ink `#1a5276` 1px axes. Sneaker dots blue `#2a78d6` 5px at `[[2.7,2.3],[3.1,2.6],[3.3,2.2],[2.9,2.8],[3.0,2.4]]`; boot dots violet `#4a3aa7` at `[[-3.7,1.3],[-3.3,1.7],[-3.5,1.2],[-3.6,1.8],[-3.2,1.5]]`; sandal dots aqua `#199e70` at `[[1.8,-2.8],[2.2,-3.1],[2.0,-3.3],[2.3,-2.9],[1.9,-3.2]]`; 11px group labels next to each cluster. Red `#e74c3c` bold X marker at (0.5, 0.0) with magenta `#d55181` bold 12px annotation "gap point → garbage". Caption 12px `#444` below: "plain autoencoder: islands with dead space".
- **Right panel (VAE blobs):** plot origin x=400, width 280, same baseline/height; both axes span −2 to 2. Three 2σ blob circles of radius 0.9 (data units): sneakers center (0.8, 0.6) fill `rgba(42,120,214,0.15)` border blue dashed; boots center (−0.9, 0.5) fill `rgba(74,58,167,0.15)` border violet dashed; sandals center (0.3, −0.9) fill `rgba(25,158,112,0.15)` border aqua dashed. Sample dots 4px in the matching solid colors: sneakers `[[0.6,0.5],[1.0,0.8],[0.9,0.3],[0.5,0.9],[0.8,0.6]]`, boots `[[-1.1,0.3],[-0.7,0.7],[-0.9,0.2],[-1.2,0.7],[-0.6,0.4]]`, sandals `[[0.1,-0.7],[0.5,-1.1],[0.3,-0.6],[0.0,-1.1],[0.5,-0.8]]`. Green `#008300` 6px dot at (0, 0) with green bold 13px annotation "any point decodes to a shoe". Caption: "VAE: fuzzy blobs packed near (0, 0)".
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=360 from y=38 to h−12.
- Corner note 11px `#6b7280`: "codes illustrative".

## Encoding a Sneaker as a Blob, Not a Pin

**Tags:** `worked example` (blue), `reparameterization` (orange)

- **Two outputs** — the encoder returns a center μ = (1.2, −0.5) and a spread σ = (0.3, 0.4)
- **Sampling** — each training pass draws noise ε and computes the code z = μ + σ × ε
- **Draw 1** — ε = (0.5, −1.0) gives z₁ = (1.35, −0.90); redo the multiply-add by hand
- **Draw 2** — ε = (−1.0, 0.5) gives z₂ = (0.90, −0.30)
- **Draw 3** — ε = (1.5, 1.0) gives z₃ = (1.65, −0.10)
- **Same shoe** — all three codes must decode back to sneaker A, so nearby codes learn to agree

*Example (italic):* Three training passes see the same sneaker at three nearby codes — the decoder learns that the whole neighborhood around μ means "this sneaker."

**Key point:** The fuzz is the mechanism: forcing every point near μ to decode to the same shoe is exactly what makes the code space smooth.

### Visualization (canvas `c2`, 720×300)

Left: a 2D plot of sneaker A's blob (2σ ellipse) with its center μ and the three sampled codes. Right: the three lines of z = μ + σ × ε arithmetic in matching colors.

- **Title (bold 15px, `#1a5276`, top center):** "One Sneaker, Three Sampled Codes — μ = (1.2, −0.5), σ = (0.3, 0.4)".
- **Left plot:** origin x=60, width 320, baseline y=250, chart height 190; x axis spans 0.4 to 2.0, y axis spans −1.4 to 0.2; ink 1px axes with 12px end labels. Blob: ellipse centered at (1.2, −0.5) with 2σ radii (0.6, 0.8) in data units, fill `rgba(42,120,214,0.15)`, dashed blue `#2a78d6` border. Center dot ink `#1a5276` 6px labeled bold 12px "μ = (1.2, −0.5)". Sample dots 6px with 12px labels: z₁ at (1.35, −0.90) green `#008300`, z₂ at (0.90, −0.30) orange `#d95926`, z₃ at (1.65, −0.10) magenta `#d55181`.
- **Right column (from x=430):** heading bold 13px `#1a5276` "z = μ + σ × ε", then three 12px monospace-style lines in the matching colors: "ε = ( 0.5, −1.0) → z₁ = (1.35, −0.90)" (green), "ε = (−1.0,  0.5) → z₂ = (0.90, −0.30)" (orange), "ε = ( 1.5,  1.0) → z₃ = (1.65, −0.10)" (magenta); lines spaced 28px starting y=110.
- **Takeaway (bold 13px green, bottom of right column):** "all three must decode to sneaker A".
- Caption 12px `#444` under the plot: "the blob is the 2σ region of the encoder's Gaussian".

## Sampling New Shoes That Never Existed

**Tags:** `where it's used` (blue), `generation` (green)

- **Generate** — sample any point near (0, 0) and decode it: a brand-new, plausible shoe design
- **Walk the map** — slide from the boot blob (−0.9, 0.5) to the sandal blob (0.3, −0.9) in 5 steps
- **Every stop works** — the midpoint (−0.3, −0.2) decodes to a closed shoe, not to static
- **Beyond shoes** — the same trick generates faces, molecules, and voices from smooth code spaces
- **Spot oddballs** — an item that encodes far from every blob is an anomaly worth flagging

*Example (italic):* Step halfway between the boot and the sandal and the decoder produces a closed low shoe — an in-between design the app never saw in training.

**Key point:** This is the payoff of "a smooth space you can sample from": any point decodes, so you can generate new items, blend two items, and flag outliers.

### Visualization (canvas `c3`, 720×300)

A single latent-space map with three faint blobs and a straight 5-step walk from the boot blob to the sandal blob, each stop labeled with what it decodes to.

- **Title (bold 15px, `#1a5276`, top center):** "Walking from Boot to Sandal in Five Steps".
- **Plot:** origin x=90, width 380, baseline y=250, chart height 195; x axis spans −1.6 to 1.0, y axis spans −1.4 to 1.0; ink 1px axes, tick labels 11px at −1, 0, 1 on both axes.
- **Blobs (faint, radius 0.5 data units):** boots violet center (−0.9, 0.5) fill `rgba(74,58,167,0.12)`; sandals aqua center (0.3, −0.9) fill `rgba(25,158,112,0.12)`; sneakers blue center (0.8, 0.6) fill `rgba(42,120,214,0.12)`; 11px `#6b7280` label beside each.
- **Walk:** dashed ink `#1a5276` (dash 5/4) polyline through `[[-0.9,0.5],[-0.6,0.15],[-0.3,-0.2],[0.0,-0.55],[0.3,-0.9]]`; orange `#d95926` 7px dots at each point; bold 12px `#444` labels offset right of each dot: "boot", "short boot", "closed shoe", "open-toe", "sandal".
- **Annotation (green `#008300` bold 13px, right side from x=520, two lines):** "every stop decodes" / "to a wearable shoe".
- **Caption (12px `#444`, bottom):** "straight-line walk in code space (illustrative)".

## The Tug-of-War (and the Blur It Causes)

**Tags:** `common mistake` (red), `loss trade-off` (orange)

- **Pull 1** — "rebuild the input exactly" pushes blobs apart so each shoe keeps a private code
- **Pull 2** — "stay a standard bell around (0, 0)" squeezes every blob toward the center
- **Rebuild-only** — island gaps around 3.2 units and sharp copies (sharpness 95), but dead space
- **Over-pulled** — gaps shrink to 0.2 and everything decodes to one blurry average (sharpness 45)
- **Balanced** — gaps near 1.0 with sharpness 80: slightly soft outputs, fully usable space

*Example (italic):* A team cranked up the "stay standard" pull to get perfect samples and instead got one grey average shoe from every point on the map.

**Common mistake:** Expecting VAE samples to be as crisp as the training photos — the smooth, sample-anywhere space is bought with a little blur; that trade is the design, not a bug.

### Visualization (canvas `c4`, 720×300)

Grouped bar chart: three loss settings (rebuild-only, balanced, over-pulled), each with a code-space gap bar and an output sharpness bar, scaled to each measure's own max.

- **Title (bold 15px, `#1a5276`, top center):** "Two Pulls: Code-Space Gaps vs Output Sharpness (illustrative)".
- **Data:** settings `["rebuild-only", "balanced", "over-pulled"]`; gap sizes `[3.2, 1.0, 0.2]` (units apart); sharpness `[95, 80, 45]` (score /100).
- **Layout:** baseline y=240, chart height 170; three groups starting at x=90, 290, 490; per group two bars 48px wide with a 12px gap: gap bar blue fill `rgba(42,120,214,0.55)` with height = value/3.2 × 170, sharpness bar orange fill `rgba(217,89,38,0.55)` with height = value/100 × 170.
- **Value labels (bold 12px above each bar):** gaps "3.2", "1.0", "0.2" in blue `#2a78d6`; sharpness "95", "80", "45" in orange `#d95926`.
- **Group labels:** 12px `#444` centered below each group; ink 1px baseline axis.
- **Legend (top right, 12px):** blue swatch "gap between blobs", orange swatch "output sharpness".
- **Annotation (magenta `#d55181` bold 12px above the balanced group):** "the usable middle".
- **Caption (11px `#6b7280`, bottom):** "each measure scaled to its own max; numbers illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (here it hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- All coordinates in the visualization specs are hardcoded literal arrays — no `Math.random()` anywhere; the sampled codes, blob centers, and walk points must be reproduced exactly as listed.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
