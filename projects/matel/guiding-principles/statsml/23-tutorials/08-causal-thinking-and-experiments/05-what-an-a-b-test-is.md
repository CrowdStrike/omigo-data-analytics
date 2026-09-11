# What an A/B Test Is

**Page type:** detail page (tutorial layout: h2 card-sections, two-column table 50% text / 50% viz; one 3-column section 38/31/31 with two charts)
**HTML title tag:** What an A/B Test Is

**Subtitle:** Show two versions to two random groups at the same time, and let the difference in one number decide

## One Store, Two Buttons

Tags: `core idea` (blue), `running example` (green)

- **The question** — does a green checkout button get more purchases than the blue one?
- **The split** — each visitor is randomly sent to blue (A) or green (B), like a coin flip
- **Same time** — both versions run side by side, so a sale or a holiday hits both
- **One difference** — the two pages are identical except the button color
- **The name** — that is an A/B test: a randomized comparison of two versions

*Example (italic):* Out of 20,000 visitors this week, 10,000 saw the blue button and 10,000 saw the green one.

**Key point:** A random same-time split makes the two groups alike in every way except the button — so a gap in purchases points at the button.

### Visualization (canvas `c1`, 720×300)

Flow diagram: 20,000 visitors split 50/50 into two arms, each ending in a purchase-count box.

- **Title (bold 16px, `#1a5276`, centered, y=26):** "One Week, One Coin Flip per Visitor".
- **Top box:** centered (190×40 at y=42), fill `#f2f6fa`, stroke ink `#1a5276`; bold 13px ink text "20,000 visitors".
- **Split label (mute `#6b7280` 12px, centered, y=108):** "random 50 / 50 split".
- **Arrows (filled heads):** blue `#2a78d6` from top box down-left to arm A; green `#008300` down-right to arm B.
- **Arm boxes (180×52 at y=132):** A at x=110, fill rgba(42,120,214,0.10), stroke blue; bold blue 13px "A: blue button", 12px `#2c3e50` "10,000 visitors". B at x=430, fill rgba(0,131,0,0.10), stroke green; bold green "B: green button", "10,000 visitors".
- **Vertical arrows** (blue and green) from arm boxes down to result boxes.
- **Result boxes (180×52 at y=218, white fill):** A stroke blue, bold blue 14px "310 purchases", 12px "310 / 10,000 = 3.1%"; B stroke green, bold green "340 purchases", "340 / 10,000 = 3.4%".
- **Center annotation (bold orange `#d95926` 13px, centered between result boxes):** "only the button differs —" / "the gap is its doing".

## Counting the Purchases

Tags: `worked example` (green), `rule of thumb` (blue)

- **Blue (A)** — 310 purchases from 10,000 visitors = 310 / 10,000 = 3.1%
- **Green (B)** — 340 purchases from 10,000 visitors = 340 / 10,000 = 3.4%
- **Absolute lift** — 3.4% − 3.1% = +0.3 percentage points
- **Relative lift** — 0.3 / 3.1 ≈ +10% more purchases
- **In people** — 30 extra purchases per 10,000 visitors sent to green

*Example (italic):* At 1,000,000 visitors a year, +0.3 points is roughly 3,000 extra purchases.

**Key point:** Report both forms — "+0.3 points" sounds tiny, "+10% more purchases" shows the business size. Same numbers.

### Visualization (canvas `c2`, 720×300)

Two-bar chart: purchase rate by button, with lift annotations in a right margin.

- **Title (bold 16px, `#1a5276`, centered, y=26):** "Purchase Rate by Button".
- **Axes/grid:** padding top 56 / bottom 56 / left 64 / right 220; y-scale 0–4% with gridlines (`#e5e9ef`) and mute labels "0%"–"4%" at each whole percent; `#999` L-axes.
- **Bars (110px wide, 90px gap, centered, 75% alpha fill):** "Blue (A)" 3.1%, blue `#2a78d6`, sub-label "310 / 10,000"; "Green (B)" 3.4%, green `#008300`, sub-label "340 / 10,000". Bold 14px colored value labels above bars; bold 12px labels and mute 12px subs below the baseline.
- **Bracket:** orange (`#d95926`, 2px) bracket connecting the two bar tops.
- **Right-margin annotations (left-aligned at chart right + 16px):** bold orange 13px "+0.3 points absolute" and "= +10% relative"; 12px `#2c3e50` "30 extra purchases" / "per 10,000 visitors".

## Why Not Just Change It and Compare to Last Week?

Tags: `why it matters` (orange), `common mistake` (red)

Three-column row (`text-col3` 38%, two `viz-col3` 31% each).

- **Before/after** — swap the button, compare weeks; but the weeks differ in more ways
- **Confounds** — a sale, payday, or weather can move conversion more than 0.3 points
- **Same-time split** — whatever happens during the test hits blue and green equally
- **Fair credit** — the leftover gap belongs to the button, not to the calendar

*Example (italic):* Week 1 had a 20%-off sale; its 3.8% conversion had nothing to do with button color.

**Key point:** An A/B test does not remove outside events — it makes them hit both groups equally so they cancel out.

### Visualization (canvas `c3a`, 420×300)

Two-bar chart: the misleading before/after comparison.

- **Title (bold 15px, `#1a5276`, centered, y=24):** "Before / After: Misleads".
- **Axes:** padding top 58 / bottom 62 / left 52 / right 20; y-scale max 4.5% with mute labels "4%" and "2%"; `#999` L-axes.
- **Bars (100px wide, 70px gap, 75% alpha):** "Week 1: blue" (sub "during 20%-off sale") 3.8%, blue `#2a78d6`; "Week 2: green" (sub "no sale") 3.4%, green `#008300`. Bold 13px value labels above; bold 12px labels and 11px mute subs below.
- **Annotations (bold red `#e74c3c` 13px, centered):** under the title: "verdict: "green lost 0.4pt""; at the bottom: "— the sale did that, not the color".

### Visualization (canvas `c3b`, 400×300)

Two-bar chart: the fair same-time split.

- **Title (bold 15px, `#1a5276`, centered, y=24):** "Same-Time Split: Fair".
- **Axes:** same style as `c3a`, y-scale max 4.5% with "4%" and "2%" labels.
- **Bars (95px wide, 65px gap, 75% alpha):** "Blue (A)" 3.1%, blue; "Green (B)" 3.4%, green; each with mute 11px sub-label "same week".
- **Annotations (bold green `#008300` 13px, centered):** under the title: "verdict: green +0.3pt"; at the bottom: "any sale would hit both bars".

## The Full Loop: Hypothesis, Split, Measure, Decide

Tags: `core idea` (blue), `where it's used` (orange)

- **Hypothesis** — written first: "the green button raises purchase rate"
- **Split** — random 50/50 assignment; each visitor stays in one group all week
- **Measure** — one primary number, purchase rate, tracked for both groups
- **Decide** — ship green, keep blue, or run longer — by a rule set in advance
- **Repeat** — every product change can go through the same four-step loop

*Example (italic):* The team wrote the hypothesis and the decision rule down before a single visitor was split.

**Key point:** Deciding still needs a statistical check — is +0.3 points more than luck? Sample size and reading results are the next tutorials' job.

### Visualization (canvas `c4`, 720×300)

Process diagram: four step boxes in a row with connecting arrows and a dashed loop-back arrow.

- **Title (bold 16px, `#1a5276`, centered, y=28):** "The Loop, Filled In With the Button Test".
- **Step boxes (145×96, 34px gaps, centered row at y=78; fill `#f8f9fa`, 2.5px colored stroke; bold 14px colored title, two 12px `#2c3e50` detail lines):**
  1. "1. Hypothesis" (violet `#4a3aa7`): ""green raises" / "purchase rate""
  2. "2. Split" (blue `#2a78d6`): "random 50/50" / "10,000 per arm"
  3. "3. Measure" (aqua `#199e70`): "blue 3.1%" / "green 3.4%"
  4. "4. Decide" (orange `#d95926`): "ship / keep /" / "run longer"
- **Arrows:** mute gray (`#6b7280`) horizontal arrows between consecutive boxes.
- **Loop-back:** dashed mute (dash 5/4, 1.5px) path from below box 4 across to below box 1 with an upward arrowhead; mute 12px centered label "next change goes through the same loop".
- **Bottom annotation (bold orange 13px, centered):** "steps 1 and 4 are written down BEFORE the data arrives".

## Regeneration instructions

- **Template:** tutorials topic-page layout (see `tutorials/CLAUDE.md`). Page: `<h1>` (no index number) with 2px `#2980b9` bottom border, `.subtitle`, four `.card-section` blocks each with `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) and a `table.layout` row. Sections 1, 2, 4 use `.text-col` (50%) / `.viz-col` (50%); section 3 uses `.text-col3` (38%) plus two `.viz-col3` (31% each) holding canvases `c3a` (420×300) and `c3b` (400×300).
- **Text column structure:** `.tags` pill row (`.tag.blue` rgba(26,82,118,0.12)/#1a5276, `.tag.green` rgba(39,174,96,0.15)/#27ae60, `.tag.red` rgba(231,76,60,0.12)/#e74c3c, `.tag.orange` rgba(230,126,34,0.15)/#e67e22; 0.72rem bold pills, 10px radius), `<ul>` (0.92rem) of one-line bullets opening with `<b>` (`#1a5276`), italic `.example` (`#555`, 0.9rem), `.key-point` callout (background `#f8f9fa`, left border `3px solid #e74c3c`, 0.9rem).
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; canvases `width:100%`, border `1px solid #e0e0e0`, radius 4px.
- **Canvas palette (JS object `P`):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Doc palette anchors: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- **Canvas:** intrinsic `width`/`height` attributes per chart; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. All data hardcoded — no `Math.random()`. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- No nav bar, no back/home links. In regenerated HTML, any card links use `.html` extensions (this page has none).
