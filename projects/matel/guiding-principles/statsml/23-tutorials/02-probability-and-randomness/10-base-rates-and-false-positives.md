# Base Rates & False Positives

**Page type:** detail page (tutorial page: 4 `.card-section` blocks, each h2 + two-column `table.layout` — text left 50%, canvas right 50%)
**HTML title tag:** Base Rates & False Positives

**Subtitle:** A fraud model that catches 95% of fraud can still produce a flag pile that is mostly innocent orders

## A Sharp Model, a Lopsided Flag Pile

**Tags:** `core idea` (blue), `surprise` (orange)

- **The model** — flags 95% of fraudulent orders, and wrongly flags 2% of legitimate ones
- **The catch** — fraud is rare: only 0.5% of all orders (1 in 200)
- **The question** — an order just got flagged; how likely is it actually fraud?
- **Gut answer** — "95%, that's the model's catch rate"
- **Real answer** — about 19%: four out of five flagged orders are innocent

*Example:* In 100,000 orders, the model flags 2,465 — and 1,990 of those are legitimate customers.

**Key point:** The base rate — how rare fraud is — decides what a flag means, not the model's catch rate.

### Visualization (canvas `c1`, 720×300)

Flow diagram: 100,000 orders split by type and by flag via labeled boxes and arrows.

- **Title (bold 15px ink `#1a5276`, centered):** "100,000 Orders Through the Fraud Model".
- **Boxes** (filled rect + 2px colored stroke; line 1 bold 13px, line 2 plain 12px, both in the stroke color):
  - Top (x=280, y=38, 160×34): fill `#eaf2fb`, stroke blue `#2a78d6`, text "100,000 orders".
  - Row 2 left (x=80, y=118, 190×40): fill `rgba(213,81,129,0.10)`, stroke magenta `#d55181`, "500 fraudulent" / "(0.5% base rate)".
  - Row 2 right (x=440, y=118, 200×40): fill `rgba(0,131,0,0.08)`, stroke green `#008300`, "99,500 legitimate" / "(99.5%)".
  - Row 3, four boxes at y=218: "475 flagged" / "95% caught" (magenta fill/stroke, 145×40); "25 missed" / "slip through" (fill `#f4f6f8`, stroke mute `#6b7280`, 120×40); "1,990 flagged" / "2% false alarms" (fill `rgba(217,89,38,0.12)`, stroke orange `#d95926`, 160×40); "97,510 clear" / "no flag" (fill `#f4f6f8`, stroke mute, 130×40).
- **Arrows:** mute gray `#6b7280`, 1.5px, with filled triangular heads, connecting top box to row 2 and row 2 boxes to their row-3 children.
- **Bottom line (bold 13px orange `#d95926`, centered):** "The flag pile mixes 475 fraud with 1,990 innocent — 4 to 1 innocent".

## Counting 100,000 Orders by Hand

**Tags:** `worked example` (green)

- **Fraud side** — 0.5% of 100,000 = 500 fraudulent orders; the model flags 95% = 475
- **Legit side** — 99,500 legitimate orders; the model wrongly flags 2% = 1,990
- **The flag pile** — 475 + 1,990 = 2,465 flagged orders land in the review queue
- **Flag quality** — fraction of flags that are real fraud: 475 / 2,465 ≈ 19%
- **Missed fraud** — 25 fraudulent orders (5% of 500) slip through unflagged

*Example:* The 2% error rate sounds tiny, but it applies to 99,500 orders — that is where 1,990 false flags come from.

**Key point:** A small error rate on a huge group outweighs a big catch rate on a tiny group.

### Visualization (canvas `c2`, 720×300)

Two-bar chart: innocent vs real-fraud orders inside the review queue.

- **Title (bold 15px ink, centered):** "Inside the Review Queue (2,465 flagged orders)".
- **Axes:** padding top 55, bottom 55, left 70, right 40; L-shaped axis `#999`. Y from 0 to 2,000 with labels every 500 (12px mute `#6b7280`, locale-formatted), gridlines `#e5e9ef`.
- **Bars (170px wide, centered at ~25% and ~72% of plot width):**
  - 1,990, orange `#d95926`, value label bold 14px "1,990" above, 12px `#2c3e50` label "innocent orders flagged" below.
  - 475, magenta `#d55181`, value label "475", label "real fraud flagged".
- **Bottom line (bold 13px violet `#4a3aa7`, centered):** "Flag quality: 475 / 2,465 ≈ 19% — about 4 of every 5 flags are innocent".

## Why the Base Rate Runs the Show

**Tags:** `where it's used` (blue), `rule of thumb` (green)

- **Same model, new market** — at 2% fraud the flag pile is half real; at 0.5% it's one-fifth real
- **Review cost** — analysts inspecting flags spend most of their time clearing innocent orders
- **Customer pain** — every false flag is a real customer blocked or delayed
- **Reporting trap** — quoting "95% catch rate" to leadership hides the 81% innocent pile
- **Rule of thumb** — for rare events, judge a model by its flag quality, not its accuracy

*Example:* Moving the same model from a 0.5%-fraud market to a 10%-fraud market lifts flag quality from 19% to 84%.

**Key point:** Before deploying any rare-event detector, compute the expected flag pile using the real base rate.

### Visualization (canvas `c3`, 720×300)

Line chart: flag quality as a function of the fraud base rate, same model throughout.

- **Title (bold 15px ink, centered):** "Same Model, Different Fraud Rates".
- **Axes:** padding top 55, bottom 65, left 70, right 40; L-shaped axis `#999`. Y 0–100% with labels every 25% (12px mute), gridlines `#e5e9ef`.
- **Data:** base rates x-labels `['0.5%', '1%', '2%', '5%', '10%']`; flag quality `[19, 32, 49, 71, 84]` (percent). Points spread from 8% to 92% of plot width.
- **Series:** blue `#2a78d6` line, width 3, with 5px-radius dots at each point; the first dot (0.5%, 19%) is orange `#d95926`, the rest blue. Bold 12px `#2c3e50` value labels ("19%" …) above each point; 12px mute base-rate labels below the baseline.
- **Captions:** 12px mute centered under axis: "fraud base rate in the order stream"; 12px mute top-left inside plot: "flag quality (share of flags that are fraud)".
- **Annotation (bold 13px orange, near the first point):** "at 0.5% fraud, only 19% of flags are real".

## The Common Confusion: Catch Rate Is Not Flag Quality

**Tags:** `common mistake` (red)

- **Two rates** — "% of fraud we catch" and "% of flags that are fraud" sound alike but aren't
- **Catch rate 95%** — measured on the 500 fraud orders (recall)
- **Flag quality 19%** — measured on the 2,465 flagged orders (precision)
- **Different denominators** — that is the whole trick: 500 vs 2,465
- **Accuracy misleads too** — "never flag anything" is 99.5% accurate here and useless

*Example:* A model that flags nothing is right on 99,500 of 100,000 orders — 99.5% accuracy, zero fraud caught.

**Key point:** For rare events, always report the pair — catch rate and flag quality — never a single accuracy number.

### Visualization (canvas `c4`, 720×300)

Three-bar chart: catch rate vs flag quality vs do-nothing accuracy.

- **Title (bold 15px ink, centered):** "Three Numbers That Sound Alike".
- **Axes:** padding top 60, bottom 80, left 70, right 40; L-shaped axis `#999`. Y 0–100% with labels every 25% (12px mute), gridlines `#e5e9ef`.
- **Bars (130px wide, 3 evenly spaced):**
  | Value | Top label | Bottom label | Color |
  |---|---|---|---|
  | 95% | catch rate | of 500 fraud orders | green `#008300` |
  | 19% | flag quality | of 2,465 flagged orders | violet `#4a3aa7` |
  | 99.5% | "flag nothing" accuracy | of 100,000 orders | mute `#6b7280` |
  Value in bold 14px (bar color) above each bar; top label bold 12px `#2c3e50` and bottom label 12px mute below the baseline.
- **Bottom line (bold 13px magenta `#d55181`, centered):** "Three denominators, three answers — quote the pair, not one number".

## Regeneration instructions

- **Template:** tutorials topic-page layout. `<h1>` concept name (no index number), `.subtitle` line, then 4 `.card-section` blocks. Each section: `<h2>` (1.3rem `#1a5276`, 2px solid `#2980b9` bottom border) + `table.layout` with one `<tr>`: left `td.text-col` (50%) holding `.tags` pill row, a 5-bullet `<ul>` (each `<li>` opens with a `<b>` term in `#1a5276`), one italic `.example` paragraph, one `.key-point` callout; right `td.viz-col` (50%) holding one `<canvas>` 720×300.
- **Page CSS:** body system-ui sans, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem; table cells padded 12px, no borders; canvas `width:100%`, 1px `#e0e0e0` border, 4px radius; `.key-point` background `#f8f9fa`, 3px solid `#e74c3c` left border, 0.9rem; `.example` italic `#555` 0.9rem; `ul` 0.92rem.
- **Tag pills:** `.tag` inline-block 0.72rem bold, padding 2px 10px, radius 10px. Colors — blue: bg `rgba(26,82,118,0.12)` / `#1a5276`; green: bg `rgba(39,174,96,0.15)` / `#27ae60`; red: bg `rgba(231,76,60,0.12)` / `#e74c3c`; orange: bg `rgba(230,126,34,0.15)` / `#e67e22`.
- **Chart palette (JS object `P`):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: #1a5276 primary blue, #27ae60 green, #e74c3c red, #e67e22 orange.
- **Canvas:** all canvases 720×300 logical; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. The flow diagram uses `box()` and `arrow()` helper functions. Chart titles bold 15px, labels 12–13px. Hardcoded literal data arrays, no `Math.random()`. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- No nav bar, no back/home links, no cross-page links. In regenerated HTML, any card links use `.html` extensions.
