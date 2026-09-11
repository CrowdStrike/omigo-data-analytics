# The P-Value

**Page type:** detail page (tutorial: 4 card-sections, each h2 + two-column layout table, text left 50% with tag pills / canvas right 50%)
**HTML title tag:** The P-Value

**Subtitle:** One number that answers one question: IF nothing unusual is going on, how often would luck alone produce data at least this extreme?

## The 61-Heads Coin Gets a Number

Tags: `core idea` (blue), `running example` (green)

- **The data** — a coin flipped 100 times lands heads 61 times
- **The IF** — pretend the coin is fair, and ask what a fair coin usually does
- **The question** — how often does a fair coin give a result at least this lopsided?
- **The answer** — about 3.5% of the time; that 0.035 is the p-value
- **Reading it** — small p means the data and the "fair coin" story clash badly

*Example:* p = 0.035 says: "a fair coin does something this lopsided in only 35 of 1,000 tries."

**Key point:** the p-value measures surprise under an assumption — it is computed in a pretend world where the coin is fair.

### Visualization (canvas `c1`, 720×300)

Bar chart of the exact Binomial(100, 0.5) distribution with the two tails shaded red = the p-value.

- **Title (bold 15px, `#1a5276`, top center):** "The P-Value Is the Shaded Area (fair coin, 100 flips)".
- **Data:** exact Binomial(100, 0.5) probabilities for k = 35..65: `[0.0009, 0.0016, 0.0027, 0.0045, 0.0071, 0.0108, 0.0159, 0.0223, 0.0301, 0.039, 0.0485, 0.058, 0.0666, 0.0735, 0.078, 0.0796, 0.078, 0.0735, 0.0666, 0.058, 0.0485, 0.039, 0.0301, 0.0223, 0.0159, 0.0108, 0.0071, 0.0045, 0.0027, 0.0016, 0.0009]`.
- **Axes:** y scale max 0.09 (no y labels); x tick labels at k = 35, 40, 45, 50, 55, 60, 65; x-axis title "heads out of 100 flips". Gray `#999` L-shaped axes; padding top 56, bottom 52, left 55, right 25.
- **Bars:** one bar per k; fill red `#e74c3c` when k >= 61 or k <= 39 (the tails), otherwise `rgba(42,120,214,0.35)`.
- **Annotations (bold 13px red `#e74c3c`):** "1.8% here" right-aligned near the left tail (at k≈39, y = top+60); "1.8% here (61+)" left-aligned near the right tail; centered "p = 0.018 + 0.018 = 0.035" near the top (y = top+16).
- **Annotation (bold 12px blue `#2a78d6`, centered mid-plot):** "96.5% of fair-coin results land here".

## Computing It by Hand: the Surprise Scale

Tags: `worked example` (green), `small numbers` (blue)

- **Step 1** — assume fair: expected heads 50, typical wobble ±5
- **Step 2** — add up the fair coin's chances of 61, 62, ..., 100 heads: 1.8%
- **Step 3** — add the mirror side (39 or fewer heads): another 1.8%
- **Total** — p = 0.018 + 0.018 ≈ 0.035
- **Calibrate** — 52 heads: p-side 38%; 55 heads: 18%; 58: 6.7%; 61: 1.8%; 65: 0.2%
- **Feel it** — 55 heads is Tuesday; 61 is eyebrow-raising; 65 is call-the-manager

*Example:* Each extra head above 58 cuts the surprise probability by about a third.

**Key point:** "at least this extreme" — the p-value counts 61 AND everything more lopsided, in both directions.

### Visualization (canvas `c2`, 720×300)

Bar chart: chance a fair coin reaches at least K heads, one colored bar per K with a "feel" label.

- **Title (bold 15px, `#1a5276`, top center):** "Chance a Fair Coin Reaches at Least K Heads".
- **Data:** K values `[52, 55, 58, 61, 65]`; exact binomial tail percentages `[38.2, 18.4, 6.7, 1.8, 0.2]`; feel labels below the x labels `['ordinary', 'Tuesday', 'hmm...', 'eyebrow', 'manager!']` (11px gray `#6b7280`).
- **Bar colors** (0.7 alpha): `#2a78d6`, `#199e70`, `#c98500`, `#d95926`, `#d55181` respectively; bar width = half a slot; value labels ("38.2%" etc.) bold 13px above each bar.
- **Axes:** y scale max 45%; gray `#999` L-axes; padding top 56, bottom 56, left 70, right 25; x-axis title "K, the head count reached (out of 100 flips)".
- **5% reference line:** horizontal dashed red `#e74c3c` (dash 5/4, width 1.5) at 5%, labeled "5% line" bold 12px red, right-aligned at the plot's right edge.
- **Annotation (bold 13px orange `#d95926`, centered at 62% width near top):** "61 heads is the first count shown that dips under the 5% line".

## Why One Number Runs So Many Decisions

Tags: `where it's used` (blue), `rule of thumb` (orange)

- **Shared language** — A/B tests, medical trials, and model comparisons all report p
- **One scale** — a coin test and a checkout test become comparable surprise numbers
- **The 0.05 habit** — p below 0.05 is called "significant"; it is a convention, not physics
- **Cliff illusion** — p = 0.049 and p = 0.051 are nearly identical evidence, opposite labels
- **Better habit** — report the p-value itself, not just the pass/fail stamp

*Example:* Two teams ran the same experiment: p = 0.049 "shipped it", p = 0.051 "killed it" — same evidence.

**Key point:** p is a smooth dial of surprise; 0.05 is just a line someone drew on the dial.

### Visualization (canvas `c3`, 720×300)

Number-line "dial" diagram: p-value axis from 0 to 0.20 with an arbitrary 0.05 cliff and two near-identical points on either side.

- **Title (bold 15px, `#1a5276`, top center):** "P Is a Smooth Dial — the 0.05 Cliff Is Man-Made".
- **Axis:** horizontal gray `#999` line at y=170, p from 0 to 0.20, tick labels at 0.00, 0.05, 0.10, 0.15, 0.20; axis title "p-value"; padding left 70, right 40.
- **Zones (rects from y=70 to the axis):** left of 0.05 filled `rgba(231,76,60,0.15)`; right of 0.05 filled `rgba(42,120,214,0.10)`.
- **Cliff:** vertical dashed red `#e74c3c` line (dash 5/4, width 2) at p=0.05 from y=60 to the axis; zone labels bold 12px: `"significant"` in red centered at p=0.025 (y=60), `"not significant"` in blue `#2a78d6` centered at p=0.115 (y=60).
- **Twin points:** filled circles radius 8 at y=120 — green `#008300` at p=0.049, orange `#d95926` at p=0.051. Labels bold 13px: `p = 0.049: "shipped"` in green right-aligned left of its dot; `p = 0.051: "killed"` in orange left-aligned right of its dot.
- **Takeaways (centered):** bold 13px violet `#4a3aa7` "two nearly identical pieces of evidence, opposite decisions" (y=245); 12px gray `#6b7280` "report the number, not just the verdict stamp" (y=270).

## What the P-Value Is NOT

Tags: `common mistake` (red), `misreading` (orange)

- **Not P(fair)** — p = 0.035 does NOT mean "3.5% chance the coin is fair"
- **Wrong direction** — p assumes fairness and grades the data, not the other way around
- **Not P(wrong)** — it is not the chance your conclusion is mistaken
- **Not size** — a tiny p can come from a tiny bias measured on millions of flips
- **The chart's test** — 1,000 coins, 999 fair: most coins showing 61+ heads are still fair

*Example:* Screen 1,000 coins (999 fair, 1 biased): about 18 fair ones hit 61+ heads, the 1 biased one does so about half the time — roughly 40-to-1 the flagged coin is fair.

**Key point:** p = P(data this extreme | coin fair). Flipping it into P(coin fair | data) needs to know how common biased coins are.

### Visualization (canvas `c4`, 720×300)

Flow diagram: a population box of 1,000 coins on the left, an arrow, and flagged-group composition bars on the right.

- **Title (bold 15px, `#1a5276`, top center):** "Screen 1,000 Coins: Who Shows 61+ Heads? (999 fair, 1 biased to 60%)".
- **Left population box** labeled "the 1,000 coins" (bold 13px `#1a5276`): rect at (45, 70), 240×120, fill `rgba(42,120,214,0.25)`, stroke blue `#2a78d6` width 1.5, centered bold blue label "999 fair"; an 8px-wide magenta `#d55181` sliver on its right edge labeled "1 biased" (bold 12px, to the right of the box).
- **Arrow:** gray `#6b7280` line with triangular head from (360, 130) to (424, 130); caption 12px gray in two lines above/below: "flip each 100x," / "flag 61+ heads".
- **Right group** titled "coins flagged \"suspicious\"" (bold 13px `#1a5276`): two horizontal bars starting at x=460 (max width 210, scale max 20):
  - fair coins flagged: value 18, blue `#2a78d6` at 0.7 alpha, value label "~18 coins" bold 13px, sublabel "fair coins flagged" 12px gray
  - biased coin flagged: value 0.5 (drawn at minimum 4px), magenta `#d55181` at 0.7 alpha, value label "~0.5 coins", sublabel "biased coin flagged"
- **Takeaways (centered):** bold 13px red `#e74c3c` "a flagged coin is ~97% likely to be FAIR — even though each flag had p = 0.018" (y=245); 12px gray "p grades the data assuming fairness; the share of fair coins among flags depends on how rare bias is" (y=270).

## Regeneration instructions

- **Layout:** tutorial page — `<h1>` + `.subtitle`, then 4 `.card-section` blocks. Each has an `<h2>` (1.3rem `#1a5276`, 2px solid `#2980b9` bottom border) and a `table.layout` (width 100%, border-collapse, cell padding 12px, vertical-align top) with one row: `.text-col` (50%) and `.viz-col` (50%, containing the canvas).
- **Text cell structure:** `.tags` row of pills, then a `<ul>` of bullets each opening with `<b>bold term</b>` (bold terms colored `#1a5276`), one italic `.example` paragraph (italic, `#555`, 0.9rem), one `.key-point` callout (background `#f8f9fa`, left border 3px solid `#e74c3c`, padding 8px 12px, 0.9rem) whose "Key point:" prefix is `<strong>`.
- **Tag pills:** `.tag` inline-block, 0.72rem, weight 600, padding 2px 10px, radius 10px. Variants: blue `rgba(26,82,118,0.12)`/`#1a5276`; green `rgba(39,174,96,0.15)`/`#27ae60`; red `rgba(231,76,60,0.12)`/`#e74c3c`; orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page style:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` bottom border; subtitle `#666` 0.95rem. Canvases have `width: 100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas JS:** shared palette object `P = { blue:'#2a78d6', green:'#008300', magenta:'#d55181', yellow:'#c98500', aqua:'#199e70', orange:'#d95926', violet:'#4a3aa7', ink:'#1a5276', text:'#2c3e50', mute:'#6b7280', grid:'#e5e9ef' }`; a shared `setup(id)` helper reads the intrinsic width/height attributes, sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`), and calls `ctx.scale` so drawing stays in logical coordinates. Hardcoded literal data arrays — no `Math.random()`.
- **Site palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- In regenerated HTML, any card links use `.html` extensions (this page has none).
