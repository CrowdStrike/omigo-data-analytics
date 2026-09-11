# Type I & Type II Errors

**Page type:** detail page (tutorial: 4 card-sections, each h2 + two-column layout table, text left 50% with tag pills / canvas right 50%)
**HTML title tag:** Type I &amp; Type II Errors

**Subtitle:** The two ways a test can be wrong: crying wolf when there is no fire, or sleeping through a real one.

## A Smoke Alarm Can Fail in Exactly Two Ways

Tags: `core idea` (blue), `false alarm` (red), `missed catch` (orange)

- **The setup** — each day there either is a fire or there isn't; the alarm rings or stays silent
- **Two right answers** — ring during a fire; stay silent on a quiet day
- **Type I error** — burnt toast sets it off: a false alarm, crying wolf
- **Type II error** — a real fire smolders and the alarm sleeps: a missed catch
- **In test language** — Type I rejects a true "nothing is happening"; Type II misses a real effect

*Example:* Every yes/no detector — alarm, spam filter, medical test, A/B test — fails in these same two ways.

**Key point:** there is one way to be right in each world, and one way to be wrong — four boxes cover everything.

### Visualization (canvas `c1`, 720×300)

2×2 outcome grid: reality (rows) vs what the alarm did (columns).

- **Title (bold 15px, `#1a5276`, top center):** "The Four Boxes: Reality vs What the Alarm Did".
- **Grid:** 2×2 cells, each 212×84 (grid origin at (170, 70), cell stride 220×92), fill + 2px colored border + centered bold 14px main text and 12px subtext:
  - top-left (real fire, rings): green `#008300` border, fill `rgba(0,131,0,0.12)` — "RING = correct" / "fire caught"
  - top-right (real fire, silent): red `#e74c3c` border, fill `rgba(231,76,60,0.12)` — "SILENT = Type II" / "slept through the wolf"
  - bottom-left (no fire, rings): red `#e74c3c` border, fill `rgba(231,76,60,0.12)` — "RING = Type I" / "cried wolf (burnt toast)"
  - bottom-right (no fire, silent): green `#008300` border, fill `rgba(0,131,0,0.12)` — "SILENT = correct" / "quiet day, quiet alarm"
- **Row labels (right-aligned, bold 13px `#1a5276`):** "REAL FIRE" and "NO FIRE", each with an 11px gray `#6b7280` "(reality)" sublabel.
- **Column labels (centered, bold 12px gray `#6b7280`, above the grid):** "the alarm rings", "the alarm stays silent".
- **Takeaway (bold 13px red `#e74c3c`, centered, y=282):** "the two red boxes are the only two ways any yes/no test can be wrong".

## 1,000 Days of Alarm Logs, Counted by Hand

Tags: `worked example` (green), `small numbers` (blue)

- **The log** — 1,000 days: 10 had a real fire, 990 were quiet (illustrative)
- **Fire days** — the alarm rang on 8 of 10; it slept through 2 → Type II rate 2/10 = 20%
- **Quiet days** — it rang anyway on 50 of 990 → Type I rate 50/990 ≈ 5%
- **Totals** — 58 rings in all: 8 real, 50 false; 942 silences: 940 right, 2 tragic
- **Names** — the 5% is α (alpha), the 20% is β (beta); power = 1 − β = 80%

*Example:* Check it: 8 + 2 = 10 fire days, 50 + 940 = 990 quiet days — the four boxes always add up.

**Key point:** the two error rates live in different rows — one is a share of fire days, the other of quiet days.

### Visualization (canvas `c2`, 720×300)

Two horizontal 100%-stacked bars: the fire-day row (zoomed) and the quiet-day row (to scale).

- **Title (bold 15px, `#1a5276`, top center):** "1,000 Days Sorted: 10 Fire Days, 990 Quiet Days (illustrative)".
- **Bar geometry:** both bars start at x=70, full width 560, height 40.
- **Fire days bar** (y=78), header "10 fire days (zoomed in)" (bold 13px): 8/10 of the width filled `rgba(0,131,0,0.55)` with white bold 13px centered label "8 rang — fires caught"; remaining 2/10 filled red `#e74c3c` labeled "2 slept" in white. Below-right annotation bold 12px red: "Type II rate = 2/10 = 20% (this is β)".
- **Quiet days bar** (y=178), header "990 quiet days (to scale)": leading 50/990 sliver filled red `#e74c3c`, remaining 940/990 filled `rgba(42,120,214,0.45)` with centered bold 13px blue `#2a78d6` label "940 stayed silent — correct". Red pointer line and bold 12px red label above the sliver: "50 rang anyway"; below-left annotation bold 12px red: "Type I rate = 50/990 ≈ 5% (this is α)".
- **Takeaway (bold 13px violet `#4a3aa7`, centered, y=278):** "α lives on the quiet-day row; β lives on the fire-day row — never mix the rows".

## The Sensitivity Dial: Fixing One Error Feeds the Other

Tags: `where it's used` (blue), `trade-off` (orange)

- **The dial** — crank sensitivity up: fewer missed fires, but far more burnt-toast alarms
- **Turn it down** — peaceful kitchen, and a real fire gets 15 extra minutes head start
- **No free lunch** — with the same detector and data, the two error rates pull against each other
- **In stats** — choosing α = 0.05 IS setting the dial: you accept 5% false alarms up front
- **The escape** — a better sensor or more data shrinks both; the dial alone never does

*Example:* An A/B test's "significance level" is a smoke alarm's sensitivity knob wearing a suit.

**Key point:** you don't eliminate errors, you budget them — pick the mix whose costs you can live with.

### Visualization (canvas `c3`, 720×300)

Two crossing line curves: false-alarm rate rises and miss rate falls as the sensitivity dial turns.

- **Title (bold 15px, `#1a5276`, top center):** "One Dial, Two Error Rates Pulling Opposite Ways (illustrative)".
- **Data:** dial settings `[1, 2, 3, 4, 5, 6, 7]`; false alarms (% of quiet days) `[0.5, 1, 2, 5, 10, 20, 35]`; miss rate (% of fires) `[60, 45, 30, 20, 12, 6, 2]`.
- **Axes:** y scale max 65; x labeled by setting 1–7 with axis title "sensitivity dial (1 = deaf, 7 = hair-trigger)"; gray `#999` L-axes; padding top 56, bottom 56, left 65, right 160 (legend space).
- **Curves:** miss rate stroked orange `#d95926` width 3; false alarms stroked blue `#2a78d6` width 3; 4px dots at every point in the matching color.
- **Marker at setting 4:** vertical dashed violet `#4a3aa7` line (dash 5/4, width 2) with two-line bold 12px violet label above: "setting 4: 5% false alarms," / "20% missed — our alarm log".
- **Legend (right side, 12px swatches):** blue square "false alarms (α)"; orange square "missed fires (β)"; below it a three-line bold 12px green `#008300` note: "better sensor or" / "more data moves" / "BOTH curves down".

## The 86% Surprise: Most Rings Are False Even at α = 5%

Tags: `common mistake` (red), `base rates` (orange)

- **The misread** — "α = 5%, so only 5% of my alarms are wrong" — no
- **Count again** — 58 rings total: 50 false, 8 real → 50/58 ≈ 86% of rings are false
- **Why** — fires are rare (10 days in 1,000), so even a small α on 990 quiet days piles up
- **Two questions** — α asks "how often do quiet days ring?"; the 86% asks "when it rings, is it real?"
- **Which is worse?** — depends on cost: missed fire vs missed cancer vs shipped bad feature

*Example:* A fraud model with a 5% false-positive rate can still make 9 of 10 flagged orders innocent.

**Key point:** α is a rate per quiet day, not per ring — when the thing you hunt is rare, most catches are false.

### Visualization (canvas `c4`, 720×300)

Waffle chart of the 58 rings, sorted into real fires and false alarms.

- **Title (bold 15px, `#1a5276`, top center):** "When the Alarm Rings, Is It a Real Fire? The 58 Rings, Sorted".
- **Waffle:** 58 squares (24×24, 6px gap, 15 per row, origin (80, 66)); the first 8 filled green `#008300` (real fires), the remaining 50 filled `rgba(231,76,60,0.75)` (false alarms).
- **Labels:** bold 13px green "8 real fires" with a green pointer line from the 8th square; bold 13px red `#e74c3c` "50 false alarms" to the right of the shorter last row of the grid.
- **Takeaways (centered):** bold 15px red "50 of 58 rings are false: 86% — even though α was only 5%" (y=226); bold 12px violet `#4a3aa7` "rare fires (10 in 1,000 days) + many quiet days = false alarms swamp real ones" (y=252); 12px gray `#6b7280` "α answers \"how often do quiet days ring?\" — not \"when it rings, is it real?\"" (y=276).

## Regeneration instructions

- **Layout:** tutorial page — `<h1>` + `.subtitle`, then 4 `.card-section` blocks. Each has an `<h2>` (1.3rem `#1a5276`, 2px solid `#2980b9` bottom border) and a `table.layout` (width 100%, border-collapse, cell padding 12px, vertical-align top) with one row: `.text-col` (50%) and `.viz-col` (50%, containing the canvas).
- **Text cell structure:** `.tags` row of pills, then a `<ul>` of bullets each opening with `<b>bold term</b>` (bold terms colored `#1a5276`), one italic `.example` paragraph (italic, `#555`, 0.9rem), one `.key-point` callout (background `#f8f9fa`, left border 3px solid `#e74c3c`, padding 8px 12px, 0.9rem) whose "Key point:" prefix is `<strong>`. Greek letters appear as HTML entities (`&alpha;`, `&beta;`, `&rarr;`, `&asymp;`, `&minus;`) in the source.
- **Tag pills:** `.tag` inline-block, 0.72rem, weight 600, padding 2px 10px, radius 10px. Variants: blue `rgba(26,82,118,0.12)`/`#1a5276`; green `rgba(39,174,96,0.15)`/`#27ae60`; red `rgba(231,76,60,0.12)`/`#e74c3c`; orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page style:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` bottom border; subtitle `#666` 0.95rem. Canvases have `width: 100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas JS:** shared palette object `P = { blue:'#2a78d6', green:'#008300', magenta:'#d55181', yellow:'#c98500', aqua:'#199e70', orange:'#d95926', violet:'#4a3aa7', ink:'#1a5276', text:'#2c3e50', mute:'#6b7280', grid:'#e5e9ef' }`; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Hardcoded literal data arrays — no `Math.random()`. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Site palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- In regenerated HTML, any card links use `.html` extensions (this page has none).
