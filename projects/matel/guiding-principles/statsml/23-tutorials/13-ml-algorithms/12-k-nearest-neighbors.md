# k-Nearest Neighbors

**Page type:** detail page (tutorial card-sections: one h2 per section, two-column `table.layout` text 50% / canvas 50%; section 3 uses a 3-column 38/31/31 layout with two canvases)
**HTML title tag:** k-Nearest Neighbors

**Subtitle:** To predict what a new customer will do, find the 5 most similar past customers and let them vote

## Ask the Five Most Similar Past Customers

Tags: `core idea` (blue), `running example` (green)

- **The question** — will a new customer ($62 spend, 8 visits a month) upgrade to premium?
- **The move** — find the 5 past customers most similar on spend and visits, let them vote
- **The vote** — 3 of those 5 upgraded and 2 did not, so we predict this customer upgrades
- **No training step** — kNN builds no model; it just keeps every past customer in memory
- **Work happens at prediction time** — each new customer means measuring distance to all

*Example (italic):* It is how a barista guesses a stranger's order: think of the regulars who look most like them.

**Key point:** **k-Nearest Neighbors:** to label a new point, find the k most similar labeled points and take a majority vote. The example above is the entire algorithm.

### Visualization (canvas `c1`, 720×300)

Scatter plot of past customers with the new customer as a star and dashed lines to its 5 nearest neighbors.

- **Title (bold 15px, `#1a5276`, top center):** "New Customer (★) and the 5 Nearest Past Customers".
- **Data (spend $, visits/month):**
  - Upgraded (green `#008300` 6px dots): `[[60,9],[65,7],[66,10],[72,11],[75,9],[70,12],[78,10],[68,11],[74,12],[80,11],[71,9],[76,13]]`
  - Stayed basic (blue `#2a78d6` 6px dots): `[[58,8],[57,6],[45,4],[40,5],[38,3],[50,5],[42,6],[35,4],[48,3],[52,6],[44,2],[37,5],[55,7],[46,5]]`
  - New customer: `[62, 8]`, drawn as an 11px/4.5px 5-point orange `#d95926` star.
- **Axes:** x range 30–85 (labels "$30"…"$85" every $10), y range 0–14 (labels every 4); padding top 42 / bottom 44 / left 58 / right 145; axis titles "monthly spend" (bottom center) and rotated "visits / month" (left). Gray `#999` L axes, muted `#6b7280` 12px labels.
- **Nearest-neighbor lines:** dashed yellow `#c98500` lines (dash 4/3, width 1.5) from the star to A(60,9), B(65,7), C(58,8), D(66,10), E(57,6); bold dark letter labels A–E above those five points.
- **Legend (right side):** green dot "upgraded", blue dot "stayed basic", bold orange 13px "★ new: $62, 8"; below it bold green 13px two lines "3 of 5 nearest upgraded" / "→ predict upgrade".

## Measuring Similarity by Hand

Tags: `worked example` (green), `arithmetic` (blue)

- **The ruler** — distance = √(spend gap² + visit gap²), a straight line on the chart
- **A ($60, 9, upgraded)** — √(2² + 1²) = √5 ≈ 2.2, the closest of everyone
- **B ($65, 7, upgraded)** — √(3² + 1²) = √10 ≈ 3.2
- **C ($58, 8, stayed)** — √(4² + 0²) = 4.0
- **D ($66, 10, upgraded) and E ($57, 6, stayed)** — √20 ≈ 4.5 and √29 ≈ 5.4
- **The verdict** — the five vote upgraded 3 to 2, so the prediction is upgrade

*Example (italic):* Set k = 3 instead and the vote comes from A, B, C: 2 to 1, still upgrade.

**Key point:** **Fully hand-checkable:** five subtractions, five squares, five square roots, one vote — nothing else happens inside kNN.

### Visualization (canvas `c2`, 720×300)

Horizontal bar chart of the 5 computed distances, colored by label.

- **Title (bold 15px, `#1a5276`, top center):** "The Five Distances, Computed by Hand".
- **Rows (label, distance, color):**
  - "A  $60, 9 — upgraded" — 2.2, green `#008300`
  - "B  $65, 7 — upgraded" — 3.2, green
  - "C  $58, 8 — stayed" — 4.0, blue `#2a78d6`
  - "D  $66, 10 — upgraded" — 4.5, green
  - "E  $57, 6 — stayed" — 5.4, blue
- **Layout:** padding top 46 / bottom 42 / left 190 / right 40; x scale 0–6.5 with tick labels 0, 2, 4, 6; x-axis title "distance to the new customer". Bars at 0.75 alpha, bar height 55% of row; row labels right-aligned to the left of the axis; bold colored distance values ("2.2"…"5.4") just past each bar end.
- **Annotation (bold green 13px, upper right of plot):** "vote: 3 upgraded vs 2 stayed → predict upgrade".

## Choosing k: One Neighbor Is Jumpy, Fifty Are Blurry

Tags: `rule of thumb` (blue), `trade-off` (orange). Three-column row: text 38%, two canvases 31% each.

- **k = 1** — copy the single closest customer; one mislabeled oddball flips the answer
- **k = 50** — with 200 customers on file, that polls a quarter of everyone
- **The trade-off** — small k chases every wiggle of noise; big k flattens real local patterns
- **Rule of thumb** — try a few odd values (5, 7, 9 ... 15) and keep the one that wins on held-out data
- **Why odd** — an odd number of voters can never tie on a yes/no question

*Example (italic):* At ($72, 10) one lapsed oddball makes k=1 say "stays" while k=5 still says "upgrades".

**Key point:** **k is a smoothness dial:** turn it down and the model memorizes noise; turn it up and every answer drifts toward the overall majority.

### Visualization (canvas `c3a`, 420×340)

Zoomed-in scatter: k=1 flipped by one oddball.

- **Title (bold 15px, `#1a5276`, top center):** "k = 1: Jumpy".
- **Axes:** zoomed region x 65–82, y 8–13.5; padding top 48 / bottom 46 / left 50 / right 18; gray `#999` L axes, no tick labels.
- **Points:** green `#008300` 6px dots (upgraded) at `[[72,11],[75,9],[70,12],[78,10],[68,11],[74,12],[71,9],[76,13],[66,10]]`; one blue `#2a78d6` 7px oddball dot at (72.6, 10) — strictly the nearest point to the new customer.
- **New point:** orange `#d95926` 10px/4px star at (72, 10) with a dashed magenta `#d55181` line (dash 4/3, width 2) to the oddball.
- **Annotations:** bold blue 12px "one "stayed" oddball" under the oddball; bold magenta 13px "k=1 copies the oddball: "stays"" near the bottom of the plot; bold green 12px "k=5 outvotes it 4–1: "upgrades"" near the top.
- **Caption (muted 12px, bottom center):** "zoomed-in corner of the customer map".

### Visualization (canvas `c3b`, 420×340)

Full-map scatter: k=50 neighborhood circle swallowing the map.

- **Title (bold 15px, `#1a5276`, top center):** "k = 50: Blurry".
- **Axes:** x 30–85, y 0–14; same padding as c3a; gray L axes, no tick labels.
- **Points:** the shared upgraded/stayed customer arrays from c1, drawn as 5px dots at 0.45 alpha (green upgraded, blue stayed).
- **Neighborhood:** dashed violet `#4a3aa7` circle (dash 7/5, width 2.5, radius 118px) centered on the new point (62, 8); orange 10px/4px star at the center.
- **Annotations:** bold violet 13px "the "neighborhood" swallows the map" near the top; bold dark 12px "every answer drifts to the overall majority" near the bottom of the plot.
- **Caption (muted 12px, bottom center):** "illustrative — 26 of the 200 customers drawn".

## Scale the Features First, or One of Them Runs the Show

Tags: `common mistake` (red), `the fix` (green)

- **Add income in dollars** — now gaps look like $20,000 next to visit gaps like 3
- **Unscaled distance** — √(20000² + 3²) ≈ 20000.0002; the visit gap changes nothing
- **The damage** — "nearest" silently becomes "nearest income"; spend and visits are ignored
- **The fix** — z-score every feature first: subtract its mean, divide by its spread
- **After scaling** — the same gaps become 0.8 vs 1.0, and both features get a real say

*Example (italic):* Record income in cents and it dominates 100× harder — the units chose your neighbors.

**Key point:** **kNN trusts your ruler blindly:** scaling the features is what makes "distance" mean "similarity" instead of "whichever column has the biggest numbers".

### Visualization (canvas `c4`, 720×300)

Two side-by-side bar panels (raw vs z-scored) showing each feature's share of the distance, split by a dashed vertical divider at x=360.

- **Title (bold 15px, `#1a5276`, top center):** "Share of the Distance Each Feature Controls".
- **Left panel (x=50, title bold ink 13px "Raw units: gap $20,000 vs gap 3 visits"):** two bars "income gap" (violet `#4a3aa7`) and "visits gap" (aqua `#199e70`), shares 100% and ~0% (labels "100%" and "~0%"); bold magenta `#d55181` note below: "√(20000² + 3²) ≈ 20000.0002".
- **Right panel (x=420, title "After z-scoring: gap 0.8 SD vs 1.0 SD"):** same two bars with shares 39% and 61%; bold green `#008300` note below: "both features now get a vote".
- **Panel geometry:** panel width 250, baseline y=226, bar scale height 132, bars 88px wide at 0.8 alpha, bold dark % labels above, feature labels below.
- **Annotation (bold magenta 13px, centered at x=175 near top):** "unscaled, "nearest" just means "nearest income"".

## Regeneration instructions

- **Template/layout:** tutorials topic page. h1 (no index number) with 2px `#2980b9` bottom border, `.subtitle` gray one-liner, then 4 `.card-section` blocks. Each section: `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) followed by `table.layout` with one row: sections 1, 2 and 4 use `td.text-col` 50% + `td.viz-col` 50% (canvas 720×300); section 3 uses `td.text-col3` 38% + two `td.viz-col3` 31% cells (canvases 420×340).
- **Text column structure:** `.tags` row of pill spans (`.tag.blue` rgba(26,82,118,0.12)/#1a5276, `.tag.green` rgba(39,174,96,0.15)/#27ae60, `.tag.red` rgba(231,76,60,0.12)/#e74c3c, `.tag.orange` rgba(230,126,34,0.15)/#e67e22; 0.72rem, weight 600, radius 10px); `<ul>` of one-line bullets each opening with `<b>` in `#1a5276`; italic `.example` paragraph (`#555`, 0.9rem); `.key-point` callout (background `#f8f9fa`, left border 3px solid `#e74c3c`, padding 8px 12px, 0.9rem) with a `<strong>` lead-in.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; canvases `width:100%` with 1px `#e0e0e0` border, 4px radius.
- **Canvas JS:** shared palette object `P = { blue:#2a78d6, green:#008300, magenta:#d55181, yellow:#c98500, aqua:#199e70, orange:#d95926, violet:#4a3aa7, ink:#1a5276, text:#2c3e50, mute:#6b7280, grid:#e5e9ef }`; shared `setup(id)` helper reading the canvas width/height attributes, scaling the backing store by `window.devicePixelRatio` and calls `ctx.scale` so drawing stays in logical coordinates; shared hardcoded data arrays `UP`, `ST`, `NEW` (no `Math.random()`).
- **Palette:** primary blue/ink `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`; chart series use the P palette above.
- **Links:** none on this page (no cross-page links, no nav); in regenerated HTML any card links elsewhere use `.html` extensions.
