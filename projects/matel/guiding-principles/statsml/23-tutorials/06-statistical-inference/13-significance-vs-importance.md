# Significance vs Importance

**Page type:** detail page (tutorial: 4 card-sections, each h2 + two-column layout table, text left 50% with tag pills / canvas right 50%)
**HTML title tag:** Significance vs Importance

**Subtitle:** A result can be statistically real yet too tiny to matter — "significant" means detectable, not big.

## The Same Tiny Lift, Tested at Five Sample Sizes

Tags: `core idea` (blue), `big samples` (orange)

- **The test** — a new checkout button lifts conversion from 5.00% to 5.05%: a 0.05-point nudge
- **Small trial** — 10,000 shoppers per side: p = 0.87; the nudge drowns in noise
- **Bigger trial** — 1 million per side: p = 0.10; still not "significant"
- **Huge trial** — 2 million per side: p = 0.02; 10 million: p < 0.0001
- **The lesson** — the effect never changed; only the microscope did

*Example:* The same 0.05-point lift goes from "invisible" to "highly significant" purely by adding shoppers.

**Key point:** p mixes two things — effect size and sample size. A tiny p can mean a big effect OR a huge sample.

### Visualization (canvas `c1`, 720×300)

Bar chart: the p-value of the SAME 0.05-point lift at five sample sizes.

- **Title (bold 15px, `#1a5276`, top center):** "Same 0.05-Point Lift, Same Truth — Only the Sample Grows".
- **Data:** x labels `['10k', '100k', '1M', '2M', '10M']` (shoppers per arm); p-values `[0.87, 0.61, 0.10, 0.02, 0.0001]` displayed as `['0.87', '0.61', '0.10', '0.02', '<0.0001']`.
- **Bars:** 62px wide, evenly spaced; fill green `#008300` when p < 0.05, otherwise `rgba(42,120,214,0.45)`; minimum bar height 3px; p-value text bold 13px above each bar; below the x label a status line in 11px — "significant" (green) or "not signif." (gray `#6b7280`).
- **Axes:** y scale 0 to 1.0; gray `#999` L-axes; padding top 56, bottom 62, left 70, right 30; x-axis title "shoppers per arm".
- **0.05 line:** horizontal dashed red `#e74c3c` (dash 5/4, width 1.5) at p = 0.05, labeled "p = 0.05 line" bold 12px red.
- **Annotation (bold 13px violet `#4a3aa7`, centered near top):** "significance was bought with sample size — the effect is 0.05 points in every bar".

## 2 Million Shoppers Each: Real, and Almost Invisible

Tags: `worked example` (green), `tiny but real` (blue)

- **Arm A** — old button: 2,000,000 shoppers, 100,000 buy → 5.00% conversion
- **Arm B** — new button: 2,000,000 shoppers, 101,000 buy → 5.05% conversion
- **Significant?** — yes: p ≈ 0.02, so luck alone rarely makes a gap this big
- **Big?** — the lift is 0.05 points: 1 extra sale per 2,000 shoppers shown the button
- **Both true** — the difference is almost certainly real AND almost certainly tiny

*Example:* Drawn on the same 0–6% axis, the two conversion bars are indistinguishable by eye.

**Key point:** "significant" answers "is it real?" — it never answers "is it worth anything?"

### Visualization (canvas `c2`, 720×300)

Side-by-side pair: honest-axis bar chart on the left, zoomed inset on the right.

- **Title (bold 15px, `#1a5276`, top center):** "5.00% vs 5.05% — Significant (p ≈ 0.02), Invisible to the Eye".
- **Left (honest axis 0–6%):** axis at x=70, baseline y=240, height 175, width 260; y gridlines (`#e5e9ef`) and labels at 0%, 2%, 4%, 6%; caption above (bold 12px `#1a5276`): "honest axis (0–6%)". Two 70px bars: "old button" 5.00% filled `rgba(42,120,214,0.55)` with sublabel "100,000 sales"; "new button" 5.05% filled `rgba(25,158,112,0.65)` with sublabel "101,000 sales"; value labels bold 12px above each bar.
- **Right (zoom inset 4.90–5.15%):** panel background `#fdf6ec` outlined orange `#d95926` width 1.5 (rect ~(385, 60) to (725, 280)); inner axis at x=430, baseline y=240, height 165, width 220; y labels at 4.90%, 5.00%, 5.10% (11px). Same two bars (60px wide) drawn on the zoomed scale so the gap is visible. Caption bold 12px orange: "zoomed axis: the whole story lives here".
- **Takeaway (bold 12px red `#e74c3c`, centered, y=294):** "a chart must zoom 24x before the \"significant\" gap is visible".

## Ship It or Skip It: Decisions Need a Size, Not a Stamp

Tags: `where it's used` (blue), `practical meaning` (green)

- **Big-data trap** — at millions of rows, nearly every A/B difference is "significant"
- **Better report** — the lift with its range: +0.05 points, 95% CI 0.007 to 0.093 points
- **Set a bar first** — say the rollout only pays for itself above +0.20 points (illustrative)
- **Compare** — even the CI's best case (+0.093) sits below the +0.20 bar: skip it
- **Flip side** — a clinic's 20-patient pilot: p = 0.09 but a huge effect — worth a bigger study

*Example:* The stamp said "ship"; the size said "this pays 1 extra sale per 2,000 shoppers — skip."

**Key point:** decide against a practical bar set in advance — the p-value cannot know what a sale is worth.

### Visualization (canvas `c3`, 720×300)

Confidence-interval plot on a lift axis, judged against a practical cost bar instead of zero.

- **Title (bold 15px, `#1a5276`, top center):** "Judge the Lift Against a Practical Bar, Not Against Zero".
- **Axis:** horizontal gray `#999` line at y=190; lift axis from −0.05 to +0.30 percentage points; tick labels "+0.00" through "+0.30" at 0.05 steps; axis title "conversion lift, percentage points"; padding left 90, right 60.
- **Zero line:** vertical dashed gray `#6b7280` (dash 3/3, width 1.5) at 0, labeled "zero" bold 11px.
- **Practical bar:** vertical dashed orange `#d95926` (dash 6/4, width 2.5) at +0.20, labeled bold 12px orange "rollout pays for itself: +0.20" with 11px sublabel "(illustrative cost bar)".
- **CI bar (y=125):** blue `#2a78d6` horizontal line width 5 from +0.007 to +0.093 with end caps; radius-7 blue point at +0.05; labels bold 12px "measured lift +0.05" and 11px "95% CI: +0.007 to +0.093" above.
- **Verdicts:** bold 13px green `#008300` "CI clears zero: statistically real" (y=250); bold 13px red `#e74c3c` "CI far below the cost bar: not worth shipping" (y=275).

## Two Dials, Four Corners

Tags: `common mistake` (red), `misreading` (orange)

- **The misread** — "p = 0.0001, so the effect must be huge" — p measures evidence, not size
- **Corner 1** — big and significant: the rare clean win; ship it
- **Corner 2** — tiny and significant: our button; real but not worth the rollout
- **Corner 3** — big but not significant: promising pilot; get more data, don't discard
- **Corner 4** — tiny and not significant: nothing to see; move on

*Example:* "Not significant" never means "zero effect" — corner 3 is where good ideas get wrongly buried.

**Key point:** always read both dials — how big (importance) and how sure (significance) are different questions.

### Visualization (canvas `c4`, 720×300)

2×2 grid: effect size (rows) vs significance (columns).

- **Title (bold 15px, `#1a5276`, top center):** "How Big vs How Sure: Four Different Situations".
- **Grid:** 2×2 cells, each 215×86 (grid origin (190, 62), cell stride 225×96), fill + 2px colored border + centered bold 14px main text and 12px subtext:
  - top-left: green `#008300`, fill `rgba(0,131,0,0.10)` — "big + significant" / "clean win — ship it"
  - top-right: yellow `#c98500`, fill `rgba(201,133,0,0.10)` — "big + not significant" / "promising — get more data"
  - bottom-left: violet `#4a3aa7`, fill `rgba(74,58,167,0.10)` — "tiny + significant" / "our button: real, skip it"
  - bottom-right: gray `#6b7280`, fill `rgba(107,114,128,0.10)` — "tiny + not significant" / "nothing to see — move on"
- **Row labels (right-aligned, bold 13px `#1a5276`):** "BIG effect", "TINY effect". **Column labels (centered, bold 12px gray):** "significant (p < 0.05)", "not significant".
- **Takeaway (bold 13px red `#e74c3c`, centered, y=282):** "the classic mistakes: treating tiny+significant as a win, and big+not-significant as zero".

## Regeneration instructions

- **Layout:** tutorial page — `<h1>` + `.subtitle`, then 4 `.card-section` blocks. Each has an `<h2>` (1.3rem `#1a5276`, 2px solid `#2980b9` bottom border) and a `table.layout` (width 100%, border-collapse, cell padding 12px, vertical-align top) with one row: `.text-col` (50%) and `.viz-col` (50%, containing the canvas).
- **Text cell structure:** `.tags` row of pills, then a `<ul>` of bullets each opening with `<b>bold term</b>` (bold terms colored `#1a5276`), one italic `.example` paragraph (italic, `#555`, 0.9rem), one `.key-point` callout (background `#f8f9fa`, left border 3px solid `#e74c3c`, padding 8px 12px, 0.9rem) whose "Key point:" prefix is `<strong>`.
- **Tag pills:** `.tag` inline-block, 0.72rem, weight 600, padding 2px 10px, radius 10px. Variants: blue `rgba(26,82,118,0.12)`/`#1a5276`; green `rgba(39,174,96,0.15)`/`#27ae60`; red `rgba(231,76,60,0.12)`/`#e74c3c`; orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page style:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` bottom border; subtitle `#666` 0.95rem. Canvases have `width: 100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas JS:** shared palette object `P = { blue:'#2a78d6', green:'#008300', magenta:'#d55181', yellow:'#c98500', aqua:'#199e70', orange:'#d95926', violet:'#4a3aa7', ink:'#1a5276', text:'#2c3e50', mute:'#6b7280', grid:'#e5e9ef' }`; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Hardcoded literal data arrays — no `Math.random()`. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Site palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- In regenerated HTML, any card links use `.html` extensions (this page has none).
