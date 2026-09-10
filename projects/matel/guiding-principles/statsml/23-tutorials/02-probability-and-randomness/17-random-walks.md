# Random Walks

**Page type:** detail page (tutorial layout: `.card-section` blocks, each an h2 + two-column `table.layout` — text left 50%, canvas right 50%)
**HTML title tag:** Random Walks

**Subtitle:** Add up fair coin flips and the running total wanders far from zero for long stretches — with no pull to come back.

## A Gambler's Bankroll Is a Random Walk

**Tags:** `core idea` (blue), `running example` (green)

- **The game** — fair coin, $1 a flip: heads +$1, tails −$1; expected profit is exactly $0
- **The surprise** — the running total does not hug zero; it drifts into long excursions
- **Four players** — same fair game, 200 flips: one ends +$10, another −$34
- **Long stretches** — a walk can spend most of its life on one side of zero
- **The name** — a total built from independent random steps is a random walk

*Example:* Player 2 was in the red for all 200 flips — of a perfectly fair game.

**Key point:** Fair game ≠ flat bankroll — fairness lives in the average over many players, not in any single path.

### Visualization (canvas `c1`, 720×300)

Four-line chart: four 200-step fair-coin bankroll paths (generated once with fixed seeds, hardcoded).

- **Title (bold 15px, `#1a5276`, top center):** "Four Players, the Same Fair Coin Game, 200 Flips Each".
- **Data (201 values each, index 0..200):**
  - WALK1 (blue `#2a78d6`): `[0,1,0,-1,-2,-1,-2,-3,-4,-5,-4,-5,-6,-5,-6,-5,-4,-5,-4,-5,-4,-3,-2,-1,-2,-1,-2,-1,0,-1,0,-1,-2,-1,-2,-3,-4,-3,-2,-1,0,1,2,1,0,1,2,1,0,-1,0,1,2,3,2,3,4,3,4,5,6,7,8,9,8,7,8,9,8,9,8,7,8,9,8,9,8,9,10,11,12,11,10,9,10,11,12,13,14,15,16,17,18,19,18,19,20,19,20,19,20,21,20,21,20,21,20,19,20,19,20,19,18,19,20,19,18,17,18,19,20,21,22,21,20,19,20,21,22,23,22,23,22,23,22,21,20,19,20,19,20,19,18,19,18,17,16,17,16,15,14,13,14,15,16,15,14,13,12,11,10,9,8,7,8,7,6,7,8,9,8,7,6,5,6,5,4,3,4,3,4,3,4,5,4,5,4,3,2,1,2,3,4,5,6,7,8,9,10,9,10]`
  - WALK2 (magenta `#d55181`): `[0,-1,-2,-3,-4,-3,-4,-5,-6,-7,-6,-7,-6,-7,-8,-9,-10,-9,-10,-9,-8,-7,-8,-7,-8,-7,-8,-9,-10,-11,-12,-11,-10,-11,-10,-11,-10,-9,-10,-11,-10,-11,-12,-13,-12,-11,-12,-13,-12,-13,-12,-13,-14,-13,-14,-13,-14,-15,-14,-15,-16,-17,-18,-17,-16,-17,-18,-19,-20,-21,-22,-21,-20,-21,-22,-21,-22,-23,-24,-23,-22,-21,-22,-21,-22,-23,-24,-25,-26,-27,-26,-25,-24,-25,-26,-27,-26,-27,-26,-27,-28,-29,-28,-29,-28,-29,-30,-29,-28,-27,-28,-27,-26,-27,-26,-27,-26,-27,-28,-27,-28,-29,-30,-31,-32,-31,-32,-31,-32,-31,-32,-33,-34,-35,-36,-35,-36,-35,-36,-37,-38,-39,-38,-37,-36,-35,-36,-37,-36,-35,-34,-35,-36,-37,-36,-37,-38,-37,-38,-39,-38,-39,-38,-39,-38,-37,-38,-39,-38,-37,-36,-37,-38,-39,-40,-41,-40,-41,-40,-39,-38,-37,-36,-37,-36,-37,-36,-35,-36,-37,-38,-39,-38,-39,-38,-37,-36,-35,-34,-35,-34]`
  - WALK3 (aqua `#199e70`): `[0,-1,-2,-3,-4,-5,-4,-3,-4,-5,-4,-5,-6,-5,-6,-7,-6,-7,-6,-5,-6,-7,-8,-7,-8,-9,-8,-7,-6,-5,-4,-5,-6,-7,-8,-7,-8,-9,-8,-7,-6,-7,-6,-5,-4,-3,-4,-5,-6,-7,-6,-7,-6,-7,-8,-7,-6,-7,-8,-9,-10,-9,-8,-9,-8,-9,-8,-7,-8,-9,-10,-11,-10,-9,-8,-9,-10,-11,-12,-11,-12,-13,-14,-13,-14,-15,-14,-13,-14,-15,-14,-15,-16,-15,-14,-13,-12,-13,-14,-13,-14,-13,-14,-13,-12,-13,-12,-11,-12,-11,-12,-13,-12,-11,-12,-11,-12,-11,-10,-9,-8,-7,-6,-7,-6,-7,-8,-9,-10,-9,-10,-9,-10,-9,-10,-9,-10,-11,-12,-11,-12,-13,-14,-15,-14,-13,-12,-11,-12,-11,-12,-13,-14,-15,-14,-15,-14,-15,-14,-13,-14,-13,-12,-11,-10,-11,-12,-11,-10,-9,-10,-9,-8,-7,-6,-7,-6,-5,-6,-5,-4,-3,-2,-1,0,1,0,1,2,1,2,1,2,3,4,5,6,5,6,5,6]`
  - WALK4 (yellow `#c98500`): `[0,-1,0,-1,0,1,0,1,0,1,0,1,0,1,0,-1,0,1,0,-1,-2,-1,-2,-1,0,-1,-2,-3,-2,-3,-2,-3,-2,-3,-2,-1,0,1,2,3,2,1,0,-1,0,1,0,-1,-2,-1,0,-1,0,-1,0,1,0,-1,-2,-3,-2,-1,-2,-3,-2,-1,0,-1,-2,-3,-4,-5,-4,-5,-4,-5,-6,-5,-4,-3,-2,-1,0,-1,-2,-1,-2,-3,-2,-3,-2,-3,-2,-1,-2,-1,0,1,2,1,0,1,0,-1,0,-1,0,-1,0,-1,0,-1,-2,-3,-2,-1,-2,-1,-2,-1,-2,-3,-2,-3,-4,-5,-6,-5,-6,-7,-6,-7,-8,-7,-6,-7,-6,-7,-8,-7,-6,-5,-6,-7,-6,-5,-4,-3,-4,-5,-4,-3,-2,-3,-4,-3,-4,-3,-4,-5,-6,-7,-8,-9,-8,-9,-10,-9,-8,-7,-6,-7,-8,-9,-10,-9,-8,-9,-10,-9,-8,-9,-10,-11,-10,-9,-8,-7,-6,-7,-8,-7,-6,-7,-8,-7,-6,-5,-4,-3,-4]`
- **Axes:** x 0–200 flips with tick labels 0, 50, 100, 150, 200 and caption "flip number"; y −$45 to +$30 with gridlines/labels at $−40, $−20, $0, $20 (gray `#6b7280`, grid `#e5e9ef`, axis `#999`); padding top 44, bottom 46, left 58, right 24.
- **Zero line:** dashed dark line (`#2c3e50`, dash 5/4, width 1.5) at $0.
- **Series:** all four paths width 2. End labels bold 12px: blue "ends +$10" near WALK1's end; magenta "ends −$34" near WALK2's end.
- **Annotation (bold 13px orange `#d95926`, centered near top):** "every path is the SAME fair game — drifting far from $0 is normal".
- **Caption (12px `#6b7280`, bottom right):** "paths illustrative".

## Ten Flips by Hand

**Tags:** `worked example` (green), `hand math` (blue)

- **The flips** — H H T H H H T T H H
- **The bankroll** — $1, 2, 1, 2, 3, 4, 3, 2, 3, 4 after each flip
- **Typical distance** — after n flips the walk sits about √n dollars from zero
- **Check it** — 100 flips: typically ~$10 away; 10,000 flips: typically ~$100 away
- **Growing, not shrinking** — more flips mean drifting further in dollars, not closer to $0

*Example:* After our 10 flips the bankroll is +$4 — and √10 ≈ 3, so that's a typical distance, not luck.

**Key point:** The average stays $0 while the spread grows like √n — "fair" and "far from zero" happily coexist.

### Visualization (canvas `c2`, 720×300)

Two-panel chart: a 10-flip staircase plus typical-distance bars. Dashed light-gray vertical divider at x=392.

- **Title (bold 15px, `#1a5276`, top center):** "Ten Flips by Hand — and How Far Walks Typically Drift".
- **Left panel (staircase):** flips `['H','H','T','H','H','H','T','T','H','H']`, bankroll after each `[0, 1, 2, 1, 2, 3, 4, 3, 2, 3, 4]` drawn as a blue `#2a78d6` step line (width 3) from x=58 to x=368, baseline y=235, height 155, y scale 0–5 with "$0", "$2", "$4" labels. Flip letters bold 13px centered under each step — H in green `#008300`, T in magenta `#d55181`. Annotations: bold 12px blue "bankroll ends at +$4"; 12px gray "H = +$1, T = −$1" below.
- **Right panel (√n bars):** three aqua `#199e70` bars (62px wide, at x=435 + i·90) for flip counts `['100', '10,000', '1,000,000']` with typical distances `[$10, $100, $1000]`; bar heights log-scaled for display (1/3, 2/3, 1.0 of 155px; equal steps = x10 distance). Value labels bold 12px "~$10" etc. above; x caption 12px gray "flips (distance = √n; bar height log scale)". Annotation bold 12px aqua, two lines: "100x the flips," / "only 10x the distance".

## Trends That Aren't Trends

**Tags:** `where it's used` (blue), `common mistake` (red)

- **Cumulative charts** — plot a running total of zero-mean noise and your eye sees a rally
- **The demo** — 120 days of pure noise, summed: it "climbs" past +15 to +20; no trend exists
- **KPIs** — cumulative revenue-vs-target and cumulative A/B lift wander exactly like this
- **Stories stick** — a wandering line invites a narrative; a random walk needs none
- **The defense** — look at the daily steps, not the running sum, before declaring a trend

*Example:* "Sentiment has been improving since spring" — while the daily changes averaged exactly zero.

**Key point:** Before explaining why the line went up, check whether a coin-flip sum looks the same — it usually does.

### Visualization (canvas `c3`, 720×300)

Line chart of a cumulative sum of zero-mean noise, with a bottom strip of the daily steps.

- **Title (bold 15px, `#1a5276`, top center):** "Running Total of 120 Days of Zero-Mean Noise".
- **Data (zero-mean daily noise summed, generated once with a fixed seed, 121 values):** `[0,0.7,-1.2,0.7,0.4,-0.4,0.7,0.5,-0.8,-1.2,-2,0,-1.2,-1.2,-2.9,-2.7,-1.5,-3.3,-3.9,-2.4,-1.2,-1.3,-3.2,-3.2,-4,-3.9,-2.6,-4.2,-2.5,-0.6,0.3,-1.2,-0.6,0,2,3,4.7,5.4,4.3,3.4,5.2,6.7,6.5,7,5.4,7.2,7.2,6.5,8.1,8.5,10.3,11.4,12.7,12.9,14.8,15.3,14.6,13.4,14.8,13.2,13.7,14.6,12.6,13.7,14.7,15.7,13.7,12.1,12.4,14.1,13.4,13.4,13.1,13,14.1,13.7,14.4,15.4,16.3,16.1,17.7,19.6,19,17.2,17.3,19,18.7,18.7,20,18.9,18.6,17.9,16.2,16.3,14.3,13.3,13.3,13.6,14.4,14.2,13.1,11.7,13.5,12.1,13.1,14.8,15.2,14.7,12.8,12.9,14.9,13.8,14.2,14,15.4,13.6,13.8,13.8,14.9,13.6,13.9]`
- **Main chart:** violet `#4a3aa7` line, width 2.5; y −6 to 22 with gridlines/labels at 0, 10, 20; padding top 44, bottom 84, left 58, right 24; grid `#e5e9ef`, axis `#999`.
- **Annotation (bold 13px violet, centered near the plateau):** "\"a clear rally since day 30\" — pure noise, no trend exists".
- **Bottom strip (y = h−62, 40px tall):** the same data as first differences drawn as 3px-wide bars around a center gridline — green `#008300` for positive days, magenta `#d55181` for negative days, heights capped. Left label 12px gray: "the same data as daily changes (mean 0):". Right label bold 12px orange `#d95926`: "no trend visible here — this is the honest view".
- **Caption (12px `#6b7280`, bottom right):** "noise series illustrative".

## "It Has to Come Back" — No, It Doesn't

**Tags:** `common confusion` (orange), `gambler's fallacy` (red)

- **Down $20** — the coin doesn't know that; the expected future change is $0 from anywhere
- **Best forecast** — from −$20, the expected bankroll after any number of flips is still −$20
- **No rubber band** — nothing pulls the walk toward zero; it re-centers wherever it stands
- **Returns take forever** — the walk will revisit zero eventually, but the average wait is unbounded
- **Doubling down** — betting bigger to "catch up" raises the swings, not the expectation

*Example:* "The metric always reverts to baseline" — a random walk doesn't; only processes with a real anchor do.

**Key point:** Mean reversion is a property some processes have — a random walk isn't one of them.

### Visualization (canvas `c4`, 720×300)

Forecast-cone chart: from −$20, the future distribution re-centers on −$20, not $0.

- **Title (bold 15px, `#1a5276`, top center):** "You Are Down $20 After 400 Flips — What Happens Next?".
- **Axes:** x from "now" to "+400 flips" (labels "now", "+100 flips", "+400 flips" in gray); y −$70 to +$30 with gridlines/labels at $−60, $−40, $−20, $0, $20; padding top 44, bottom 48, left 58, right 30; grid `#e5e9ef`, axis `#999`.
- **Zero line:** dashed dark `#2c3e50` line at $0 labeled 12px "break even".
- **Cone:** filled region `rgba(42,120,214,0.14)` spanning −20 ± 2√n for n up to 400 more flips (parabolic-sideways envelope).
- **Median line:** solid blue `#2a78d6`, width 3, flat at −$20 with a 6px dot at the start; bold 13px blue label "expected bankroll: still −$20".
- **False-belief curve:** dashed magenta `#d55181` quadratic curve (dash 7/5, width 2.5) rising from −$20 toward $0, labeled bold 13px magenta "\"it must come back\" — false", with a magenta X crossing it out mid-curve.
- **Annotation (bold 13px orange `#d95926`):** "the future cone centers on −$20, not $0".

## Regeneration instructions

- **Layout:** tutorial detail page. h1 + `.subtitle`, then four `.card-section` blocks; each has an `<h2>` (1.3rem `#1a5276`, 2px solid `#2980b9` bottom border) and a `table.layout` with one row: `.text-col` td (50%) and `.viz-col` td (50%), 12px padding.
- **Left column structure per section:** a `.tags` row of colored pill spans, a `<ul>` of one-line bullets each opening with `<b>bold term</b>` (bold terms in `#1a5276`), one italic `.example` paragraph (`#555`, 0.9rem), one `.key-point` callout (background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem) starting with `<strong>Key point:</strong>`.
- **Tag pill styles:** 0.72rem, weight 600, padding 2px 10px, radius 10px; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page style:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` bottom border; subtitle `#666` 0.95rem. No nav bar, no back/home links.
- **Canvas:** each 720×300 intrinsic, `width:100%` CSS, `1px solid #e0e0e0` border, radius 4px; scaled by `window.devicePixelRatio` via a shared `setup(id)` helper (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates). All data as hardcoded literal arrays — no `Math.random()`.
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Doc palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
