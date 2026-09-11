# Standard Error vs Standard Deviation

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table row, text left 50% / canvas right 50%)
**HTML title tag:** Standard Error vs Standard Deviation

**Subtitle:** One measures how spread out the data is; the other measures how wobbly your average is

## Two Different Questions About the Same Loaves

Tags: `core idea` (blue), `running example` (green)

- **The bakery** — every morning it weighs a batch of sourdough loaves
- **Question 1** — "how much do individual loaves differ?" → standard deviation (SD)
- **Question 2** — "how far off could today's batch average be?" → standard error (SE)
- **SD** — a fact about loaves: some come out 470 g, some 530 g
- **SE** — a fact about your estimate: the batch average barely moves day to day

*Example (italic):* Customers feel the SD (their loaf varies); the owner watching the daily average feels the SE.

**Key point:** SD describes the data, SE describes the average of the data — same units, entirely different questions.

### Visualization (canvas `c1`, 720×300)

Split-panel scatter: individual loaf weights (wide spread, left) vs daily batch averages (tight cluster, right), on a shared weight scale.

- **Title (bold 15px, `#1a5276`, top center):** "Individual Loaves vs Daily Batch Averages (grams, illustrative)".
- **Divider:** vertical dashed line (`#bdc3c7`, dash 4/3) at x=360 from y=38 to bottom.
- **Weight scale:** 440–560 g mapped to y (y = 250 − (v−440)/120 × 190).
- **Left panel — 25 individual loaves**, dots radius 5 in `rgba(42,120,214,0.6)`, values: `[462, 505, 528, 491, 476, 517, 499, 538, 483, 508, 471, 524, 495, 512, 458, 502, 533, 487, 519, 466, 497, 509, 480, 526, 493]`, laid out in a 9-column jittered grid starting at x=60. Dashed SD band lines (`#2a78d6`, width 2, dash 5/4) at 480 g and 520 g spanning x=50–335. Bold blue label below: "single loaves: SD ±20 g".
- **Right panel — 25 daily batch averages**, dots radius 5 in `rgba(25,158,112,0.75)`, values: `[498, 503, 500, 496, 505, 501, 499, 504, 497, 502, 500, 495, 503, 501, 498, 506, 500, 502, 497, 501, 499, 504, 500, 496, 502]`, 9-column jittered grid starting at x=400. Dashed SE band lines (`#199e70`, width 2, dash 5/4) at 496 g and 504 g spanning x=390–690. Bold aqua label below: "batch averages: SE ±4 g".
- **Annotation (bold orange `#d95926`, near top right):** "same bakery — 5x tighter".

## 25 Loaves, One Division: SE = 20 ÷ √25 = 4

Tags: `worked example` (green), `by hand` (blue)

- **The batch** — 25 loaves in 10 g bins: 1, 2, 3, 4, 5, 4, 3, 2, 1 loaves from 460 g to 540 g
- **Mean** — the bins are symmetric around 500, so the mean is 500 g
- **SD** — squared distances: 2×(1·40² + 2·30² + 3·20² + 4·10²) ÷ 25 = 400, so SD = 20 g
- **SE** — SD ÷ √n = 20 ÷ √25 = 4 g
- **Read it** — loaves vary by ±20 g; the batch average is trustworthy to about ±4 g

*Example (italic):* Every number above can be checked with pencil and paper — 25 loaves, one square root.

**Key point:** SE = SD ÷ √n. The whole difference between the two ideas is one division by √n.

### Visualization (canvas `c2`, 720×300)

Histogram of the 25-loaf batch with SD and SE range arrows overlaid.

- **Title (bold 15px, `#1a5276`, top center):** "The 25-Loaf Batch: mean 500 g, SD 20 g, SE 4 g".
- **Bars:** bin centers `[460, 470, 480, 490, 500, 510, 520, 530, 540]`, counts `[1, 2, 3, 4, 5, 4, 3, 2, 1]`; bar width 52px, evenly spaced; y scale max 6.2. Center bar (500) solid `#2a78d6`, others `rgba(42,120,214,0.45)`. Count labels bold 12px above each bar; bin-center labels 12px below on a gray `#999` baseline. Padding: top 56, bottom 56, left 65, right 30.
- **SD arrow (orange `#d95926`, width 2.5, end ticks):** horizontal span from bin 480 to bin 520 at y = top+26, labeled bold above: "SD: loaves live within ±20 g".
- **SE arrow (green `#008300`, width 2.5, end ticks):** short horizontal span of ±0.4 bin-widths around the 500 bin at y = top+64, labeled bold below: "SE: the mean is pinned to ±4 g".
- **Caption (muted `#6b7280`, 12px, bottom center):** "loaf weight bin (g), count of loaves shown on bars".

## One Shrinks With n, the Other Never Does

Tags: `where it's used` (orange), `rule of thumb` (blue)

- **Weigh more loaves** — SD stays near 20 g: the oven's variability is what it is
- **But SE falls** — 20 ÷ √n: n = 25 gives 4 g; n = 100 gives 2 g; n = 400 gives 1 g
- **Reports** — quote SD to describe loaves; quote SE to say how sure you are of the mean
- **A/B tests** — the "significance" machinery runs on SE, never on SD
- **Gone wrong** — mixing them up makes results look 5x too precise (or too noisy) at n = 25

*Example (italic):* A dashboard that swaps SD for SE turns an honest ±20 g spread into a false ±4 g certainty.

**Key point:** collecting more data cannot make loaves more alike — it can only make your knowledge of the average sharper.

### Visualization (canvas `c3`, 720×300)

Two-line chart: SD flat vs SE falling as n grows.

- **Title (bold 15px, `#1a5276`, top center):** "Weigh More Loaves: SD Stays, SE Falls (20 ÷ √n)".
- **X categories (equally spaced):** n = `['4', '16', '25', '64', '100', '400']`; **SD series:** `[20, 20, 20, 20, 20, 20]` in `#2a78d6`, width 3; **SE series:** `[10, 5, 4, 2.5, 2, 1]` in `#008300`, width 3, with 4px green dots and value labels above each point.
- **Axes:** y from 0 to 25, gray `#999` L-shaped axis; padding top 52, bottom 56, left 70, right 185. X-axis label (muted, center): "loaves weighed (n)"; rotated y-axis label: "grams".
- **Annotations:** bold blue at left near SD line: "SD stuck at 20 g — the oven does not care about your n"; bold green near SE curve: "SE melts toward 0".
- **Legend (right side, x = w−170):** blue swatch "SD (loaf spread)", green swatch "SE (mean wobble)".

## The Error-Bar Trap

Tags: `common mistake` (red), `reading charts` (orange)

- **Two ovens** — oven A averages 500 g, oven B averages 515 g, both SD 20 g, n = 25 each
- **SD bars** — ±20 g bars overlap heavily: the ovens look indistinguishable
- **SE bars** — ±4 g bars are far apart: the 15 g mean difference is very real
- **Both honest** — SD bars answer "do loaves overlap?", SE bars answer "do the means differ?"
- **Always ask** — every error bar you see: is it SD, SE, or a confidence interval?

*Example (italic):* The same two ovens look identical with SD bars and clearly different with SE bars.

**Common mistake:** comparing means using SD error bars — overlap of the data says nothing about whether the averages differ.

### Visualization (canvas `c4`, 720×300)

Two side-by-side panels: identical means with SD error bars (left) vs SE error bars (right).

- **Title (bold 15px, `#1a5276`, top center):** "Oven A (500 g) vs Oven B (515 g): Same Data, Two Stories".
- **Divider:** vertical dashed line (`#bdc3c7`, dash 4/3) at x=360.
- **Weight scale:** 460–550 g mapped to y (y = 245 − (v−460)/90 × 175); each panel has its own y-axis with ticks at 460, 500, 540.
- **Each panel:** two mean dots (radius 6) at 500 g (oven A, `#2a78d6`) and 515 g (oven B, `#d95926`) with I-beam error bars (line width 2.5, 9px caps); labels "A" and "B" below the axis.
  - Left panel (x0=40): half-bar ±20, panel label (bold, `#1a5276`): "error bars = SD (±20 g)", verdict (bold red `#e74c3c`, bottom): 'bars overlap — "no difference"?'.
  - Right panel (x0=390): half-bar ±4, panel label: "error bars = SE (±4 g)", verdict (bold green `#008300`, bottom): "means clearly differ by 15 g".
- **Annotation (bold violet `#4a3aa7`, top center):** "identical data".

## Regeneration instructions

- **Layout:** tutorial detail page. h1 (2rem, `#1a5276`, 2px `#2980b9` bottom border) + `.subtitle` (`#666`, 0.95rem), then four `.card-section` blocks. Each section: `<h2>` (1.3rem, `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one `<tr>`: left `<td class="text-col">` (50%) holds `.tags` pill row, `<ul>` bullets (each starting with `<b>` term in `#1a5276`), one italic `.example` paragraph, one `.key-point` callout; right `<td class="viz-col">` (50%) holds one canvas.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6. Bullets 0.92rem; `.example` italic `#555` 0.9rem; `.key-point` background `#f8f9fa`, left border 3px solid `#1a5276`, padding 8px 12px, 0.9rem.
- **Tag pills:** `.tag` inline-block 0.72rem bold, padding 2px 10px, radius 10px. Colors — blue: bg `rgba(26,82,118,0.12)` / `#1a5276`; green: bg `rgba(39,174,96,0.15)` / `#27ae60`; red: bg `rgba(231,76,60,0.12)` / `#e74c3c`; orange: bg `rgba(230,126,34,0.15)` / `#e67e22`.
- **Canvas:** 720×300 intrinsic attributes, CSS `width:100%`, 1px `#e0e0e0` border, radius 4px; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object `P`:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- No nav bar, no back/home links. In regenerated HTML any card links use `.html` extensions (this page has none).
