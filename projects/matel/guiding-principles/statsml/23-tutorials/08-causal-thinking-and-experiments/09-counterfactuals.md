# Counterfactuals

**Page type:** detail page (tutorial page: `.card-section` blocks, each h2 + two-column `table.layout` — text left 50%, canvas right 50%, tag pills above bullets)
**HTML title tag:** Counterfactuals

**Subtitle:** Every "X caused Y" claim quietly compares reality to an invisible world where X never happened — the counterfactual.

## "The campaign drove 500 sales" — compared to what?

Tags: `core idea` (blue), `hidden assumption` (orange)

- **The claim** — sales: 2,000 in May, 2,500 in June with a campaign, so "+500 from it"
- **The hidden step** — that math assumes June without the campaign would have been exactly 2,000
- **The counterfactual** — the June that never happened: same month, same customers, no campaign
- **The problem** — you can never observe that world; sales were already growing on their own
- **The real effect** — observed June minus counterfactual June, not observed June minus last month

*Example:* If June was heading to 2,200 anyway, the campaign added 300 — not the 500 in the slide deck.

**Key point:** A causal effect is a comparison between two versions of the same moment. One of them is always invisible — the whole game is estimating it honestly.

### Visualization (canvas `c1`, 720×300)

Three-bar chart: May actual, June actual, and a dashed "ghost" bar for June without the campaign.

- **Title (bold 16px, ink `#1a5276`, top center):** "One June You Saw, One June You Did Not".
- **Bars:** baseline at y=250 (thin `#999` line from x=70 to w−40), chart height 175px, scale max 2,800; bar width 130, gap 70, first bar at x=100.
  - "May (actual)": 2,000, solid mute `#6b7280` at 0.75 alpha.
  - "June (actual, campaign)": 2,500, solid blue `#2a78d6` at 0.75 alpha (label wraps to two lines: "June (actual" / "campaign)").
  - "June without campaign": 2,200, ghost bar — dashed outline only (orange `#d95926`, dash 6/4, width 2.5), no fill.
  - Bold 13px value labels above each bar ("2,000", "2,500", "2,200"); 12px labels below the baseline.
- **Effect brackets on the June-actual bar:**
  - Right side, green `#008300` vertical bracket from the 2,500 level to the 2,200 level, bold 13px label: "true effect: 300".
  - Left side, magenta `#d55181` vertical bracket from the 2,500 level to the 2,000 level, bold 13px label: "claimed: 500".
- **Caption (bold orange 13px, bottom center):** "the dashed bar is invisible in real life — it must be estimated".

## Redoing the math: the trend says 2,200 was coming anyway

Tags: `worked example` (green), `by hand` (blue)

- **The data** — monthly sales: Feb 1,400, Mar 1,600, Apr 1,800, May 2,000, June 2,500
- **The pattern** — sales grew by a steady +200 every month before the campaign
- **The forecast** — extend the trend: June without a campaign ≈ 2,000 + 200 = 2,200
- **The naive claim** — 2,500 − 2,000 = 500 sales credited to the campaign
- **The honest claim** — 2,500 − 2,200 = 300 sales; growth was doing the rest

*Example:* Two subtractions, two stories: "before vs after" says 500; "actual vs expected" says 300.

**Key point:** The trend line standing in for the missing world is itself an assumption — but at least it is a stated one you can argue with.

### Visualization (canvas `c2`, 720×300)

Line chart of monthly sales with a dashed trend extension to the counterfactual June.

- **Title (bold 16px, ink, top center):** "Extend the Trend to Estimate the Missing June".
- **Axes:** L-shaped `#999` axes; padding top 50, bottom 48, left 70, right 130; x = months Feb–Jun (5 evenly spaced points), y range 1,200–2,700.
- **Actual series:** blue `#2a78d6` line width 3 through `[1400, 1600, 1800, 2000, 2500]`, blue dots radius 5, bold 12px value labels above each point ("1,400" … "2,500"), 12px month labels below the axis.
- **Trend extension:** dashed orange `#d95926` segment (dash 7/5, width 3) from (May, 2,000) to (Jun, 2,200); counterfactual point at (Jun, 2,200) drawn as a white-filled circle radius 6 with orange 2.5px stroke; bold orange 12px label right of it: "2,200 expected".
- **Gap bracket:** green `#008300` vertical line 60px right of the Jun point from the 2,500 level to the 2,200 level; bold green labels: "+300" (13px) and "campaign" (12px).
- **Annotations:** bold violet `#4a3aa7` 13px inside the plot: "+200/month growth was already there — it is not the campaign". Mute 12px centered at bottom: "monthly sales".

## Why data scientists keep a holdout group

Tags: `where it's used` (blue), `best practice` (green)

- **The trick** — hold 1,000 of the 10,000 customers out of the campaign, at random
- **What happened** — exposed customers: 253 sales per 1,000; holdout: 220 sales per 1,000
- **The stand-in** — the holdout lives in the "no campaign" world on your behalf
- **The estimate** — 253 − 220 = 33 extra sales per 1,000, so ≈ 300 across the 9,000 exposed
- **Everywhere** — A/B control arms, uplift models, incrementality tests: built counterfactuals

*Example:* The holdout's 220 per 1,000 matches the trend forecast — two independent routes to the same missing number.

**Key point:** Randomly holding people out is the closest you can get to watching both worlds at once — that is why experiments beat before/after charts.

### Visualization (canvas `c3`, 720×300)

Two-panel diagram: customer split on the left, outcome bars on the right, divided by a dashed vertical line at x=360.

- **Title (bold 16px, ink, top center):** "The Holdout Group Lives in the \"No Campaign\" World".
- **Left panel — split block:** rectangle at (45, 70), 210×120, split 90/10: exposed portion filled `rgba(42,120,214,0.25)` with blue `#2a78d6` stroke, holdout portion filled `rgba(217,89,38,0.30)` with orange `#d95926` stroke. Labels: bold blue 13px "9,000 exposed" with 12px "see the campaign" inside the big block; bold orange 12px "1,000" / "held out" below the small slice; 12px above the block: "10,000 customers, split at random"; bold violet `#4a3aa7` 12px below: "random split = the only difference" / "between the groups is the campaign".
- **Right panel — bars:** baseline at y=235, chart height 145, scale max 300, bar width 110; bold ink 13px panel title: "June sales per 1,000 customers". Bars: "exposed" 253 (blue, 0.7 alpha) and "holdout" 220 (orange, 0.7 alpha), bold 13px value labels above, 12px group labels below. Green `#008300` vertical bracket between the 253 and 220 levels midway between the bars, bold green 13px label "+33".
- **Caption (bold green 13px, bottom, centered right of middle):** "33 per 1,000 x 9,000 exposed = ~300 extra sales — matches the trend estimate".

## The confusion: "before" is not the counterfactual

Tags: `common mistake` (red), `baseline choice` (orange)

- **Not the past** — the counterfactual is this June without the campaign, not last month
- **Baseline shopping** — vs May: +500; vs last June: +200; vs trend: +300; vs holdout: +300
- **Same data, four effects** — the reported number is really a choice of comparison world
- **Smell test** — any uplift claim without a stated baseline is quietly picking the flattering one
- **First question** — when someone says "this drove X", always ask: compared to what?

*Example:* The press release picked "+500 vs last month"; the holdout said 300 — same campaign, same June.

**Key point:** Before/after comparisons smuggle in "nothing else changed" — trends, seasons, and news all changed. Name the counterfactual before you name the effect.

### Visualization (canvas `c4`, 720×300)

Horizontal bar chart: the claimed effect under four different baselines.

- **Title (bold 16px, ink, top center):** "Same June, Four \"Effects\" — the Baseline Is the Claim".
- **Layout:** left label gutter 235px, vertical `#999` axis line at x=235 from y=50 down; rows start y=62, row height 46, bar height 24; value scale max 550.
- **Rows (fill at 0.7 alpha, right-aligned 12px row label, bold 13px colored value label after the bar):**
  - "vs last month (2,000)": +500, magenta `#d55181`.
  - "vs last June (2,300)": +200, yellow `#c98500`.
  - "vs trend forecast (2,200)": +300, aqua `#199e70`.
  - "vs random holdout (2,200)": +300, green `#008300`.
- **Annotations:** bold magenta 13px: "\"compared to what?\" changes the answer by 2.5x". Mute 12px bottom center: "claimed campaign effect (extra June sales) under each choice of counterfactual".

## Regeneration instructions

- **Template:** tutorials topic-page layout (see `tutorials/CLAUDE.md`). `<h1>` (no index number) with 2px `#2980b9` bottom border, `.subtitle` in `#666` 0.95rem, then four `.card-section` blocks. Each section: `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one `<tr>`: left `td.text-col` (50%) and right `td.viz-col` (50%) holding the canvas.
- **Left column structure:** `.tags` row of colored pill spans (0.72rem, 600 weight, radius 10px — blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`); then a `<ul>` (0.92rem) of one-line bullets each opening with `<b>` in `#1a5276`; one italic `.example` line (`#555`, 0.9rem); one `.key-point` callout (background `#f8f9fa`, left border 3px solid `#e74c3c`, 0.9rem) beginning with `<strong>Key point:</strong>`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; universal reset; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius.
- **Canvas JS:** shared palette object `P = { blue:'#2a78d6', green:'#008300', magenta:'#d55181', yellow:'#c98500', aqua:'#199e70', orange:'#d95926', violet:'#4a3aa7', ink:'#1a5276', text:'#2c3e50', mute:'#6b7280', grid:'#e5e9ef' }`; shared `setup(id)` helper with fixed 720×300 logical size that sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. All data hardcoded (no `Math.random()`). Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Site palette:** `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange. No nav bar, no back/home links. In regenerated HTML, any card links use `.html` extensions (this page has none).
