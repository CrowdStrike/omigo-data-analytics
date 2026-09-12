# Information Gain

**Page type:** detail page (tutorial layout: h1 + subtitle, then card-sections each with an h2 and a text/viz table row, 50% text / 50% canvas)
**HTML title tag:** Information Gain

**Subtitle:** Score a question by how much uncertainty it removes — entropy before the split minus entropy after

## Twenty Customers and One Good Question

Tags: `core idea` (blue), `running example` (green)

- **The setup** — 20 customers, 10 churned and 10 stayed: a 50/50 mix, 1 full bit of uncertainty
- **The question** — "is the customer on an annual plan?" splits them into two groups of 10
- **Annual side** — 10 customers, only 1 churned: almost all stayers
- **Monthly side** — 10 customers, 9 churned: almost all churners
- **Purer groups** — each side is now a 90/10 mix instead of a 50/50 mix

*Example:* Before the question, guessing churn is a coin flip; after it, you're right 9 times out of 10.

**Key point:** A good question sorts a mixed group into subgroups that are each mostly one thing.

### Visualization (canvas `c1`, 720×300)

Split diagram: 20 customer dots dividing into two groups of 10 via arrows.

- **Title (bold 15px, `#1a5276`, top center):** '"On an Annual Plan?" Sorts a 50/50 Mix into Two 90/10 Groups'.
- **Legend:** magenta dot (`#d55181`) "churned", blue dot (`#2a78d6`) "stayed".
- **Top group:** 20 dots (6px radius, 17px spacing) in 2 rows of 10 starting at (285, 68): 10 magenta (churned) then 10 blue (stayed). Gray caption to the right, right-aligned at the canvas edge: "20 customers: 10 churned, 10 stayed — 1.00 bit".
- **Arrows:** two gray (`#999`, width 2) arrows with filled arrowheads from the top group down-left and down-right; bold ink labels "annual plan: yes" (at 215,145) and "annual plan: no" (at 510,145).
- **Left group (annual):** 10 dots at (105, 190): 1 magenta, 9 blue; caption "10 annual: 1 churned, 9 stayed"; bold green (`#008300`) "90% stayers — 0.47 bits".
- **Right group (monthly):** 10 dots at (465, 190): 9 magenta, 1 blue; caption "10 monthly: 9 churned, 1 stayed"; bold orange (`#d95926`) "90% churners — 0.47 bits".
- **Bottom annotation (bold ink, center):** "one question → two nearly-pure groups".

## Computing the Gain by Hand

Tags: `worked example` (green)

- **Before** — 10 of 20 churned: entropy of a 50/50 mix = 1.00 bit
- **Annual side** — 1 of 10 churned: entropy of a 90/10 mix = 0.47 bits
- **Monthly side** — 9 of 10 churned: also a 90/10 mix = 0.47 bits
- **Weighted after** — (10/20) × 0.47 + (10/20) × 0.47 = 0.47 bits
- **Gain** — 1.00 − 0.47 = 0.53 bits of uncertainty removed by one question

*Example:* One question wiped out half the uncertainty: from a coin flip to a 9-in-10 call.

**Key point:** Information gain = entropy before − the size-weighted average entropy after. Bigger gain = better question.

### Visualization (canvas `c2`, 720×300)

Two-bar before/after chart with a gain bracket.

- **Title:** "Gain = Entropy Before − Entropy After".
- **Data:** labels `['before the question', 'after (weighted average)']`, sub-captions `['10 of 20 churned: 50/50', '(10/20)×0.47 + (10/20)×0.47']`, values `[1.00, 0.47]`, colors `[#4a3aa7 violet, #199e70 aqua]`.
- **Axes:** L-shaped gray axes; y max 1.15; padding top 56, bottom 66, left 62, right 200 (extra right space for the bracket). Bars 150px wide at 25%/75% of chart width, 0.75 alpha; bold value labels "1.00 bit" / "0.47 bits" above bars.
- **Gain bracket:** orange (`#d95926`, width 2) bracket to the right of the second bar spanning y(1.00) to y(0.47), with dashed light-gray (`#ccc`, dash 4/3) guide lines from each bar top to the bracket.
- **Bracket labels (bold orange, two lines):** "gain = 1.00 − 0.47" / "= 0.53 bits".
- **Y-axis label (rotated):** "entropy, bits".

## This Is How Decision Trees Pick Questions

Tags: `where it's used` (blue), `rule of thumb` (orange)

- **The contest** — a decision tree tries every available question and computes each one's gain
- **Annual plan?** — gain 0.53 bits: the winner, so it becomes the top of the tree
- **Tenure over a year?** — splits 12/8 into 4-of-12 and 6-of-8 churn: gain only 0.12 bits
- **Uses mobile app?** — splits 10/10 with 5 churners on each side: gain 0.00, learned nothing
- **Repeat** — inside each branch the same contest runs again on the customers that landed there

*Example:* A split that leaves both sides still 50/50 has zero gain, no matter how natural the question sounds.

**Key point:** Decision trees are greedy — at every node they ask the single question with the highest information gain.

### Visualization (canvas `c3`, 720×300)

Bar chart: three candidate questions compared by gain.

- **Title:** "The Tree Tries Every Question and Keeps the Biggest Gain".
- **Data:** labels `['on annual plan?', 'tenure > 1 year?', 'uses mobile app?']`, sub-captions `['1/10 vs 9/10 churn', '4/12 vs 6/8 churn', '5/10 vs 5/10 churn']`, values `[0.53, 0.12, 0.00]` bits, colors `[#008300 green, #c98500 yellow, #6b7280 mute]`.
- **Axes:** y max 0.62; padding top 56, bottom 66, left 62, right 30; bars 140px wide at 18%/50%/82% width, 0.75 alpha (no bar for 0.00); bold value labels "0.53 bits" / "0.12 bits" / "0.00 bits"; rotated y-axis label "information gain, bits".
- **Annotations:** bold green "winner → becomes the root split" above the first bar; bold mute "both sides still 50/50: nothing learned" near the third bar's baseline.

## The Confusion: One Pure Branch Isn't Enough

Tags: `common mistake` (red)

- **The trap** — "is a VIP?" isolates 2 customers who both churned: a perfectly pure branch
- **The other side** — 18 customers with 8 churners remain: still nearly 50/50, 0.99 bits
- **Weighted after** — (2/20) × 0 + (18/20) × 0.99 = 0.89 bits
- **Small gain** — 1.00 − 0.89 = 0.11 bits: the pure branch is too small to matter
- **Compare** — the balanced annual-plan split gains 0.53 bits, five times more

*Example:* Carving out two sure churners feels smart, but 18 coin-flips of confusion remain behind them.

**Key point:** Gain weights each branch by its size — purity on a tiny branch removes almost no uncertainty overall.

### Visualization (canvas `c4`, 720×300)

Two side-by-side area panels: remaining uncertainty as width × height rectangles.

- **Title:** "Remaining Uncertainty as Area: Branch Width × Branch Entropy"; gray subtitle: "width = share of the 20 customers, height = entropy of that branch, area = weighted entropy after".
- **Panels:** each 260px wide, baseline y=235, 150px height = 1 bit, with a dashed `#ccc` 1-bit reference line labeled "1 bit (before)".
  - Left panel at x=60, magenta `#d55181`, title '"is a VIP?" — gain 0.11'; segments: 10% width at 0 entropy (drawn as a dashed 2px sliver) and 90% width at 0.99 entropy; caption "weighted entropy after = 0.89 bits".
  - Right panel at x=400, green `#008300`, title '"annual plan?" — gain 0.53'; segments: 50% width at 0.47 and 50% width at 0.47; caption "weighted entropy after = 0.47 bits".
- **Segment fills:** panel color at 0.55 alpha with solid stroke.
- **Segment labels:** "2 VIPs, 0 bits" (with a short gray leader line), "18 others: 8 churned — 0.99 bits", "10 annual: 0.47 bits", "10 monthly: 0.47 bits".
- **Annotations:** bold green "smaller area = better split" above the right panel; bold magenta "the pure branch is only 10% wide" below the left panel; gray "both branches shrink to 0.47" below the right panel.

## Regeneration instructions

- **Template/layout:** tutorial concept page (tutorials category). h1 (no index number) with 2px `#2980b9` bottom border, `.subtitle` gray one-liner, then 4 `.card-section` blocks. Each section: `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one `<tr>`: left `td.text-col` (50%) and right `td.viz-col` (50%, one 720×300 canvas).
- **Text column structure:** `.tags` pill row first, then a `<ul>` of 5 one-line bullets each opening with `<b>bold term</b>` (bold terms colored `#1a5276`), one italic `.example` line, one `.key-point` callout (`#f8f9fa` background, 3px `#e74c3c` left border).
- **Tag pills:** 0.72rem, weight 600, padding 2px 10px, radius 10px. Colors: blue `rgba(26,82,118,0.12)`/`#1a5276`; green `rgba(39,174,96,0.15)`/`#27ae60`; red `rgba(231,76,60,0.12)`/`#e74c3c`; orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; ul 0.92rem; canvases `width:100%` with 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Chart palette (JS object `P`):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette anchors: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- **Canvas:** intrinsic `width`/`height` attributes per chart; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates; a `dotGroup(ctx, x0, y0, churned, stayed, perRow)` helper draws customer dots (6px radius, 17px grid; churned magenta first, then stayed blue). Chart titles bold 15px system-ui; labels 11–13px; all data hardcoded (no `Math.random()`). Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- In regenerated HTML, any card links use `.html` extensions (this page has no links).
