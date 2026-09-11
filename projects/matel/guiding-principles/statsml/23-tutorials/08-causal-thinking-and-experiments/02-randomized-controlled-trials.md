# Randomized Controlled Trials

**Page type:** detail page (tutorial page: `.card-section` blocks, each h2 + two-column layout table, text left 50% / canvas right 50%)
**HTML title tag:** Randomized Controlled Trials

**Subtitle:** Give each new signup a coin flip between the old and new onboarding flow — then any systematic gap between the groups points at the flow

## One Coin Flip per Signup

**Tags:** `core idea` (blue), `running example` (orange)

- **The test** — a redesigned onboarding flow might help more signups finish setup
- **The rule** — every signup gets a coin flip: heads see the new flow, tails keep the old
- **Nobody chooses** — not the user, not the designer, not the account manager
- **Side by side** — both flows run the same week, so news and seasons hit both equally
- **The name** — this design is a randomized controlled trial, or RCT

*Example:* 1,000 signups arrived this week; the coin sent 500 to the new flow and 500 to the old one.

**Key point:** The coin flip is the design; everything after it is just counting.

### Visualization (canvas `c1`, 720×300)

Flow diagram: the RCT design as boxes and arrows from signups through a coin flip to two arms and a counting step.

- **Title (bold 16px, ink `#1a5276`, top center):** "The Design: a Coin Flip Splits the Week’s Signups".
- **Boxes** (2px colored border, bold 13px centered text in the border color):
  - Ink `#1a5276` box at (30, 125), 140×50, white fill: "1,000 signups" / "this week".
  - Coin: circle at (255, 150), radius 30, fill `#fdf3e0`, stroke yellow `#c98500` 2.5px, bold 12px yellow text "COIN" / "FLIP".
  - Blue `#2a78d6` box at (360, 65), 175×50, fill `#eef4fd`: "500 signups" / "NEW flow".
  - Orange `#d95926` box at (360, 185), 175×50, fill `#fdf1ea`: "500 signups" / "OLD flow".
  - Green `#008300` box at (585, 125), 115×50, white fill: "count who" / "finishes setup".
- **Arrows** (2px, filled triangular heads): ink arrow from the signups box to the coin; blue arrow from the coin up to the NEW-flow box, labeled "heads" (bold 12px blue at 320, 100); orange arrow from the coin down to the OLD-flow box, labeled "tails" (bold 12px orange at 320, 205); green arrows from both flow boxes converging to the count box.
- **Annotation (bottom center, bold 13px magenta `#d55181`):** "no human decides who sees what — the coin does".

## Counting the Results: 68% vs 58%

**Tags:** `worked example` (green)

- **New flow** — 340 of 500 signups finished setup: 68%
- **Old flow** — 290 of 500 finished setup: 58%
- **The gap** — 68 − 58 = 10 points, about 100 extra finished setups per 1,000 signups
- **Redo it by hand** — 340 ÷ 500 and 290 ÷ 500; no model, no adjustment needed
- **Why trust it** — the coin made the groups alike, so the flow is the only suspect

*Example:* If the new flow did nothing, both groups would land near the same rate, give or take chance.

**Key point:** In an RCT the analysis is subtraction — randomization did the hard work up front.

### Visualization (canvas `c2`, 720×300)

Bar chart: setup-completion rate for the two arms with a gap bracket.

- **Title (bold 16px, ink `#1a5276`, top center):** "Finished Setup: New Flow vs Old Flow".
- **Bars:** width 130, baseline at y=240, chart height 165, y scale max 80%; gray `#999` baseline from x=70 to x=560.
  - "new flow" at x=130: 68%, fill blue `#2a78d6`; value label "68%" bold 14px above bar; white bold 12px count "340 of 500" inside the bar near the baseline; label "new flow" (12px) below baseline.
  - "old flow" at x=390: 58%, fill orange `#d95926`; value label "58%"; inside count "290 of 500"; label "old flow".
- **Gap bracket:** green `#008300`, width 2, at right (x 590–605) spanning from the 68% bar top height to the 58% bar top height, labeled "+10" / "points" in bold 13px green to its right.
- **Annotation (bottom center, bold 13px green):** "+10 points — the coin removed every explanation except the flow and chance".

## Why the Coin Flip Is the Whole Trick

**Tags:** `core idea` (blue), `rule of thumb` (green)

- **Before the flip** — one crowd of 1,000 signups: one mix of ages, devices, keenness
- **After the flip** — two groups that differ in exactly one thing: which flow they saw
- **One difference** — so an outcome gap has only one possible source
- **No self-selection** — keen users can't pile into one group; the coin ignores keenness
- **Without it** — compare volunteers to non-volunteers and you're back to guessing

*Example:* In the 1,000 signups, iOS share landed at 41% in one group and 40% in the other — the coin balanced it.

**Key point:** Randomization doesn't make groups perfect — it makes them alike, which is all a comparison needs.

### Visualization (canvas `c3`, 720×300)

Grouped bar chart: three user traits compared across the two randomized groups, showing near-identical shares.

- **Title (bold 16px, ink `#1a5276`, top center):** "After the Flip, the Groups Look Like Twins".
- **Clusters:** three clusters of two bars each (bar width 55, in-cluster gap 8, cluster spacing 200px starting at x=90), baseline at y=235, chart height 150, y scale max 50%; gray `#999` baseline from x=60 to x=660. Bar colors: new-flow group = blue `#2a78d6`, old-flow group = orange `#d95926`.
  - "iOS share": 41% vs 40%.
  - "under-25 share": 30% vs 29%.
  - "existing-account share": 22% vs 23%.
  - Value labels bold 12px above each bar; trait labels 12px below baseline under each cluster.
- **Legend (top left):** 11px blue swatch + "new-flow group"; orange swatch + "old-flow group" (12px text).
- **Annotations:** bottom center, bold 13px green `#008300`: "every trait within a point — the only real difference left is the flow"; top right, 12px muted `#6b7280`, right-aligned: "illustrative shares".

## The Gold Standard, From Medicine to Product

**Tags:** `where it's used` (blue), `history` (orange)

- **Medicine** — new drugs must beat a control group in an RCT before approval
- **Placebo** — the control pill looks identical, so hope and belief affect both groups equally
- **Product** — the same design renamed: an A/B test is an RCT run on users
- **Everywhere** — pricing, ranking, emails, ads: the coin-flip logic never changes
- **When you can't flip** — nobody randomizes smoking or earthquakes; causal answers get much harder

*Example:* The 1948 streptomycin tuberculosis trial randomized patients with sealed envelopes — the same trick as your onboarding test.

**Key point:** If this design is trusted to approve medicine, it can settle whether your onboarding flow works.

### Visualization (canvas `c4`, 720×300)

Diagram: two-column vocabulary mapping between drug trials and onboarding tests, five connected box pairs.

- **Title (bold 16px, ink `#1a5276`, top center):** "One Design, Two Vocabularies".
- **Column headers (bold 13px, centered at y=54):** "DRUG TRIAL" in violet `#4a3aa7` over the left column; "ONBOARDING TEST" in aqua `#199e70` over the right column.
- **Rows:** five pairs of boxes (255 wide × 34 tall, 6px vertical gap, starting y=62; left column x=65, right column x=400), each pair joined by a 1.5px gray `#6b7280` horizontal connector line. Left boxes: fill `#f7f5fc`, 1.5px violet border; right boxes: fill `#eefaf5`, 1.5px aqua border; box text 13px `#2c3e50` centered.
  - patients ↔ signups
  - new drug ↔ new onboarding flow
  - placebo / standard care ↔ old onboarding flow
  - random envelope ↔ random assignment code
  - recovery rate ↔ setup completion rate
- **Annotation (bottom center, bold 13px magenta `#d55181`):** "rename the boxes and it is the same experiment".

## Regeneration instructions

- **Layout:** tutorials topic-page template. h1 (no index number) with `border-bottom: 2px solid #2980b9`, `.subtitle` paragraph, then four `.card-section` blocks. Each section: `<h2>` (1.3rem, `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with `td.text-col` (50%) and `td.viz-col` (50%) holding one canvas.
- **Text column structure:** `.tags` row of `.tag` pills first, then a `<ul>` of one-line bullets each opening with `<b>bold term</b>` (bold terms colored `#1a5276`), one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, `border-left: 3px solid #e74c3c`, padding 8px 12px, 0.9rem) starting with `<strong>Key point:</strong>`.
- **Tag pill styles:** inline-block, 0.72rem, weight 600, padding 2px 10px, radius 10px. blue: `rgba(26,82,118,0.12)` bg / `#1a5276` text; green: `rgba(39,174,96,0.15)` / `#27ae60`; red: `rgba(231,76,60,0.12)` / `#e74c3c`; orange: `rgba(230,126,34,0.15)` / `#e67e22`. The "history" tag uses the orange class.
- **Page style:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; ul 0.92rem; canvases have `width: 100%`, `1px solid #e0e0e0` border, 4px radius. No nav bar, no back/home links, no cross-page links.
- **Canvas:** intrinsic `width="720" height="300"` per chart; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper; shared `box(...)` and `arrow(...)` helpers draw the diagram elements. Hardcoded literal data arrays — no `Math.random()`. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette (JS `P` object):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
