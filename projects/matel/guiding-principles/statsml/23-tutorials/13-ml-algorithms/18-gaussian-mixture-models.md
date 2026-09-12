# Gaussian Mixture Models

**Page type:** detail page (tutorial card-sections: h2 per section, two-column table layout text left 50% / canvas right 50%)
**HTML title tag:** Gaussian Mixture Models

**Subtitle:** Assume your data is several bell curves blended together, then work out — for every single point — which curve it probably came from

## One Lumpy Histogram, Two Hidden Bell Curves

Tags: `core idea` (blue), `bell curves` (green), `running example` (orange)

- **The data** — heights of 200 gym members in one unlabeled column, sex not recorded
- **The look** — the histogram is one wide lump; you would never guess two groups by eye
- **The assumption** — pretend it's two bells blended: women near 162 cm, men near 176 cm
- **What GMM finds** — each bell's center, width, and share of the crowd (here 50/50)
- **The payoff** — every member gets a probability of coming from each bell, not a hard label

*Example (italic):* Illustrative numbers throughout: women ~N(162, 6), men ~N(176, 7), 100 of each.

**Key point:** **Gaussian Mixture Model:** "this data is k bell curves added together — find the bells, then say which bell each point likely came from." That sentence is the whole model.

### Visualization (canvas `c1`, 720×300)

Histogram with two fitted Gaussian curves overlaid.

- **Title (bold 15px `#1a5276`, center):** "200 Gym-Member Heights: One Lump Hiding Two Bells (illustrative)".
- **Histogram:** 10 bins of 5 cm from 145 to 195 cm, counts `[2, 10, 32, 36, 33, 33, 28, 17, 7, 2]` (sums to 200), bars filled `rgba(26,82,118,0.25)`, y scale max 40. Padding: top 55, bottom 55, left 60, right 30.
- **X-axis:** tick labels 145–195 step 10 (12px `#222`); caption "height, cm" (12px `#444`). Gray `#999` L-axes.
- **Fitted bells (line width 3, Gaussian density × 500 on the count scale):** N(162, 6) in magenta `#d55181`; N(176, 7) in blue `#2a78d6`.
- **Labels (bold 13px):** "bell 1: 162 cm" magenta near x=157 top; "bell 2: 176 cm" blue near x=183.
- **Annotation (bold 13px green `#008300`, centered near bottom at x=170):** "the eye sees one lump — the model recovers both bells".

## How Sure Is 169 cm? Soft Assignment by Hand

Tags: `worked example` (green), `soft clustering` (blue)

- **The member** — someone 169 cm tall, right between the two bell centers
- **Bell 1 (162, sd 6)** — 169 is 1.17 sd away; the curve's height there is about 0.034
- **Bell 2 (176, sd 7)** — 169 is 1.00 sd away; the curve's height there is about 0.035
- **Split the credit** — 0.034 / (0.034 + 0.035) = 49% bell 1, 51% bell 2
- **Contrast 158 cm** — bell heights 0.053 vs 0.002: 96% bell 1, no real doubt

*Example (italic):* The 49/51 answer is honest: at 169 cm the data genuinely cannot tell the groups apart.

**Key point:** **Soft clustering:** instead of forcing "you are in cluster 2", GMM says "51% cluster 2, 49% cluster 1" — the uncertainty is part of the answer.

### Visualization (canvas `c2`, 720×300)

Two Gaussian density curves with vertical reference lines at 169 cm and 158 cm showing credit splits.

- **Title (bold 15px `#1a5276`, center):** "At 169 cm the Two Bells Are Almost Equally Tall".
- **Axes:** x from 140 to 200 cm (ticks every 10, 12px `#222`), caption "height, cm"; y density scale max 0.075 (unlabeled). Gray `#999` L-axes; padding: top 55, bottom 55, left 60, right 30.
- **Curves (line width 3, computed as true Gaussian densities):** N(162, 6) magenta `#d55181`; N(176, 7) blue `#2a78d6`.
- **Line at 169:** vertical dashed green `#008300` (width 2, dash 5/4) from near top to baseline; 5px dots where it crosses each curve (magenta dot at density 0.034, blue dot at 0.035).
- **Line at 158:** vertical dashed gray `#6b7280` (width 1.5, dash 4/4), shorter; magenta and blue 5px dots at densities 0.053 and 0.002.
- **Annotations:** bold 13px green at top above the 169 line: "169 cm: heights 0.034 vs 0.035 → 49% / 51%"; bold 12px gray near x=153: "158 cm: 96% bell 1".

## The EM Loop: Guess, Split the Credit, Re-Fit, Repeat

Tags: `EM algorithm` (blue), `mechanism` (green)

- **Chicken and egg** — the bells give the memberships; the memberships give the bells
- **Start with a guess** — put the two centers anywhere, say 155 and 183
- **E-step** — score every member: what fraction of them belongs to each current bell?
- **M-step** — recompute each bell's center as the weighted average of its members
- **Repeat** — here the centers walk 155→162 and 183→176 in about four rounds

*Example (italic):* It's the same loop as k-means, except members split their vote instead of picking one side.

**Key point:** **EM caveat:** the loop always settles, but only on a local best — different starting guesses can end at different answers, so real libraries restart several times.

### Visualization (canvas `c3`, 720×300)

Two-series line chart: estimated bell centers converging over EM rounds.

- **Title (bold 15px `#1a5276`, center):** "EM in Action: Bad Starting Guesses Walk to the Truth".
- **Data (5 rounds, x labels "round 0" … "round 4"):** bell 1 center `[155, 159.8, 161.4, 161.9, 162.0]` in magenta `#d55181`; bell 2 center `[183, 178.6, 176.9, 176.2, 176.0]` in blue `#2a78d6`. Lines width 3 with 4.5px dots.
- **True values:** horizontal dashed target lines (width 1.5, dash 4/4) at 162 (magenta) and 176 (blue).
- **Axes:** y from 150 to 188, tick labels at 155/162/169/176/183 (12px `#666`); rotated y title "estimated center, cm" (12px `#444`). Gray `#999` L-axes; padding: top 55, bottom 55, left 65, right 170.
- **Legend (right, x = w−158):** magenta swatch "center of bell 1", blue swatch "center of bell 2" (12px `#222`).
- **Annotation (bold 13px green `#008300`, under legend, three lines):** "guessed 155 & 183 —" / "settled on 162 & 176" / "in 4 rounds".

## Why Soft Beats Hard at the Border

Tags: `where it's used` (blue), `common mistake` (red)

- **k-means at 169 cm** — exactly 7 cm from both centers, yet it must pick one side
- **GMM at 169 cm** — reports 49/51, so downstream code knows this member is genuinely ambiguous
- **Different widths** — GMM lets one bell be wide and one narrow; k-means can't
- **Real jobs** — speaker voices in audio, cell types in biology, customer segments in spend data
- **The mistake** — treating clusters as real groups; GMM finds bells even in bell-free data

*Example (italic):* Fit a 2-bell GMM to perfectly uniform data and it will still happily report two bells.

**Key point:** **Rule of thumb:** use the soft probabilities, not just the winning label — and check the histogram first, because the mixture story is an assumption, not a discovery.

### Visualization (canvas `c4`, 720×300)

Line chart comparing GMM's soft responsibility curve to k-means' hard step function.

- **Title (bold 15px `#1a5276`, center):** "P(bell 2) Across Heights: Soft Ramp vs Hard Cliff".
- **Axes:** x from 150 to 190 cm (ticks every 10), caption "height, cm"; y from 0 to 1 with labels "0", "0.5", "1" (12px `#666`). Gray `#999` L-axes; padding: top 55, bottom 55, left 65, right 175.
- **GMM curve (green `#008300`, width 3):** responsibility p = d2/(d1+d2) computed from the same two bells N(162,6) and N(176,7), a smooth S-shaped ramp from 0 to 1.
- **k-means step (orange `#d95926`, width 3, dashed 7/5):** hard step — 0 for heights below the midpoint 169, jumping vertically to 1 at 169.
- **Marker:** 6px green dot on the GMM curve at 169 cm (p ≈ 0.51).
- **Legend (right, x = w−162):** green swatch "GMM: soft", orange swatch "k-means: hard" (12px `#222`).
- **Annotation (bold 13px green, under legend, two lines):** "at 169 cm GMM says 51%;" / "k-means fakes certainty".

## Regeneration instructions

- **Layout:** tutorial detail page. h1 (2rem, `#1a5276`, bottom border 2px solid `#2980b9`), `.subtitle` (`#666`, 0.95rem), then four `.card-section` blocks. Each section: `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one `<tr>`: `.text-col` (50%) text, `.viz-col` (50%) canvas 720×300.
- **Text cell structure:** `.tags` row of pill spans first, then `<ul>` of one-line bullets each opening with `<b>` term (bold terms colored `#1a5276`), one italic `.example` paragraph (`#555`, 0.9rem), one `.key-point` callout (background `#f8f9fa`, left border 3px solid `#e74c3c`, padding 8px 12px, 0.9rem).
- **Tag pills:** 0.72rem, weight 600, padding 2px 10px, radius 10px. blue = `rgba(26,82,118,0.12)`/`#1a5276`; green = `rgba(39,174,96,0.15)`/`#27ae60`; red = `rgba(231,76,60,0.12)`/`#e74c3c`; orange = `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius.
- **Chart palette (JS object `P`):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Doc palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- **Canvas:** all canvases 720×300 logical; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via shared `setup(id)` helper; shared `gauss(x, mu, sd)` helper computes normal density for curves c1, c2, c4. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- No nav bar, no back/home links. In regenerated HTML, any card links use `.html` extensions.
