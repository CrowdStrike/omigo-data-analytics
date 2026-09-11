# Permutation Tests

**Page type:** detail page (tutorial layout: h1 + subtitle, 4 `.card-section` blocks each with h2 and a two-column `table.layout` — text left 50%, canvas right 50%)
**HTML title tag:** Permutation Tests

**Subtitle:** Shuffle the group labels over and over and see whether your real difference stands out from the shuffled ones

## Does Music Boost Café Sales? Ten Days, Two Labels

**Tags:** `core idea` (blue), `running example` (green), `shuffling` (orange)

- **The setup** — a café plays music on 5 days, none on 5 days, and logs daily sales ($)
- **Music days** — 230, 245, 260, 240, 225: average 240
- **Quiet days** — 210, 222, 205, 220, 218: average 215
- **The gap** — music days average $25 more; is that music, or just which days they were?
- **The shuffle idea** — if music did nothing, the labels are arbitrary — so try other labelings

*Example:* Peel the "music"/"quiet" stickers off the 10 days, reshuffle them, and see what gaps sticker-luck alone produces.

**Key point:** the null hypothesis becomes physical — "labels don't matter" is tested by literally rearranging the labels.

### Visualization (canvas `c1`, 720×300)

Dot plot of the ten days' sales, colored by label, with dashed group-mean lines.

- **Title (bold 15px, `#1a5276`, top center):** "Ten Days of Café Sales, Colored by Label"
- **Data (10 dots, left to right):** music days 230, 245, 260, 240, 225 (blue `#2a78d6`); quiet days 210, 222, 205, 220, 218 (mute gray `#6b7280`). Radius-7 filled dots, one per column slot.
- **Axes:** y from 190 to 270, gridlines and labels at $200, $220, $240, $260 (gray `#6b7280` labels, grid `#e5e9ef`); gray `#999` axis lines. Padding: top 56, bottom 66, left 65, right 30.
- **Labels:** each dot's value in bold 11px `#2c3e50` above it; below baseline, "music"/"quiet" in bold 11px in the dot's color.
- **Mean lines:** dashed (6/4) 2.5px lines — blue `#2a78d6` at y=240 across the left half labeled "avg 240" (bold 12px blue); gray `#6b7280` at y=215 across the right half labeled "avg 215" (bold 12px gray).
- **Caption (bold 13px magenta `#d55181`, bottom center):** "observed gap: $25 — real effect, or just how the stickers fell?"

## All 252 Relabelings, Counted by Hand

**Tags:** `worked example` (green), `small numbers` (blue)

- **Count them** — choosing which 5 of 10 days get the "music" label: exactly 252 ways
- **Score each** — for every relabeling, compute (music average) − (quiet average)
- **The pile** — most relabelings give gaps near 0; big gaps are rare
- **Our gap** — only 1 relabeling of 252 reaches +25 ... and it's the real one
- **The p-value** — counting both directions: p = 2/252 ≈ 0.008

*Example:* The real labeling produced the single most extreme gap possible from these 10 numbers — luck rarely does that.

**Key point:** the p-value is just a fraction — extreme shuffles divided by all shuffles. No formula, no lookup table.

### Visualization (canvas `c2`, 720×300)

Histogram of all 252 permutation mean-differences, with the extreme tails flagged red and an arrow marking the real gap.

- **Title (bold 15px `#1a5276`, top center):** "Gaps from All 252 Possible Relabelings (exact count)"
- **Data:** bin centers `[-25, -20, -15, -10, -5, 0, 5, 10, 15, 20, 25]` with counts `[3, 9, 21, 33, 38, 44, 38, 33, 21, 9, 3]`. Bar width 48px.
- **Colors:** bins with |center| ≥ 25 (the two end bins) filled red `#e74c3c`; all others `rgba(42,120,214,0.45)`.
- **Axes:** y max 50, tick labels at 0, 10, 20, 30, 40 (gray 12px); gray `#999` axis lines; padding top 56, bottom 62, left 65, right 30. Count printed bold 12px above each bar; bin center printed 11px below each bar.
- **X-axis caption (12px `#2c3e50`):** "gap: music-labeled average − quiet-labeled average ($, bins of 5)"
- **Arrow annotation:** red `#e74c3c` 2.5px vertical arrow pointing down at the rightmost bin, with right-aligned bold 13px red text on two lines: "the real labeling lands here: +25," / "the most extreme gap possible".
- **In-chart annotations:** bold 13px blue `#2a78d6` centered: "sticker-luck piles up near zero"; bold 12px red centered below it: "red bins hold gaps 23–25; only the two ±25 extremes count: p = 2/252 ≈ 0.008".

## Why Data Scientists Love It: Any Statistic Works

**Tags:** `where it's used` (blue), `few assumptions` (green)

- **Swap the stat** — medians, ratios, 90th percentiles, "revenue per visit": same shuffle recipe
- **No formula needed** — classic tests need a math derivation per statistic; shuffling doesn't
- **No bell curve** — the comparison pile comes from YOUR data, not a textbook curve
- **Big data** — with thousands of rows, sample 10,000 random shuffles instead of all of them
- **Where it shines** — A/B tests on weird metrics, tiny samples, skewed business data

*Example:* "Did the redesign lift the 90th-percentile checkout time?" has no textbook test — but shuffling handles it in four lines.

**Key point:** one recipe covers every statistic — compute, shuffle labels, recompute, count how often the shuffles beat reality.

### Visualization (canvas `c3`, 720×300)

Flow diagram: four recipe-step boxes with arrows, plus a row of four swappable-statistic boxes.

- **Title (bold 15px `#1a5276`, top center):** "One Recipe, Any Statistic"
- **Flow boxes (150×52px at y=60, `#f8f9fa` fill, 2px colored border, x = 40/215/390/565), each with a bold 12px `#1a5276` first line and 12px gray second line, joined by gray arrows:**
  1. "1. compute the stat" / "on the real labels" — border blue `#2a78d6`
  2. "2. shuffle labels" / "(10,000 times)" — border orange `#d95926`
  3. "3. recompute stat" / "per shuffle" — border aqua `#199e70`
  4. "4. p = share of" / "shuffles ≥ real" — border magenta `#d55181`
- **Middle label (bold 13px `#1a5276`, centered):** '"the stat" can be anything you care about:'
- **Statistic boxes (158×34px at y=172, white fill, 1.5px colored border, bold 12px text in border color):** "difference of means" (blue `#2a78d6`), "difference of medians" (aqua `#199e70`), "90th percentile gap" (orange `#d95926`), "revenue per visit ratio" (violet `#4a3aa7`).
- **Bottom annotations (centered):** bold 13px green `#008300`: "classic tests need a new formula for each of these — the shuffle recipe never changes"; 12px gray `#6b7280`: "the comparison distribution is built from your own data, not a textbook curve".

## What People Get Wrong: Too Few Data Points to Shuffle

**Tags:** `common mistake` (red), `limits` (orange)

- **Floor on p** — the smallest possible p is (2 ÷ number of relabelings); shuffles set your resolution
- **3 vs 3** — only 20 relabelings exist, so two-sided p can never go below 0.10
- **Stuck above 0.05** — with 3 per group, "significant at 0.05" is mathematically impossible
- **Shuffle what's exchangeable** — paired days, time trends, or clustered data need matched shuffles
- **Labels, not causes** — if music days were also weekends, shuffling can't untangle that

*Example:* A 3-day-vs-3-day pilot can end at best at p = 0.10 — the experiment was undersized before it began.

**Key point:** a permutation test can never be more surprised than "1 in all possible shuffles" — small samples cap how much evidence you can get.

### Visualization (canvas `c4`, 720×300)

Bar chart of the best achievable (minimum) two-sided p-value by group size, on a −log10 height scale, against a dashed p=0.05 threshold.

- **Title (bold 15px `#1a5276`, top center):** "The Best P-Value You Can Ever Reach (two-sided)"
- **Data (5 bars):** group sizes "3 vs 3", "4 vs 4", "5 vs 5", "6 vs 6", "8 vs 8"; number of splits "20 splits", "70 splits", "252 splits", "924 splits", "12,870 splits"; minimum p values 0.100, 0.029, 0.008, 0.0022, 0.00016 (bar labels "p ≥ 0.10", "p ≥ 0.029", "p ≥ 0.008", "p ≥ 0.0022", "p ≥ 0.00016"); bar heights are −log10(p) = `[1.0, 1.54, 2.10, 2.66, 3.80]` on a scale maxing at 4.2. Bar width 74px.
- **Colors:** bars where min p < 0.05 in green `#008300`, otherwise red `#e74c3c`; alpha 0.7 fill. Only "3 vs 3" is red.
- **Axes:** gray `#999` axis lines; padding top 56, bottom 66, left 75, right 35. Left-side rotated-role label as three right-aligned 12px gray lines: "stronger" / "evidence" / "possible ↑". Below each bar: group size (12px `#2c3e50`) and split count (11px gray).
- **Threshold line:** dashed (6/4) red `#e74c3c` 2px horizontal line at −log10(0.05) = 1.30, labeled "p = 0.05 bar" in bold 12px red at the right end.
- **Annotation (bold 13px red, left-aligned, two lines):** "3 vs 3 tops out at p = 0.10 —" / '"significant" is impossible'.
- **Caption (12px `#2c3e50`, bottom center):** "bar height = strength of the best achievable verdict (−log scale); floor p = 2 / number of splits"

## Regeneration instructions

- **Template:** tutorial detail page (see `tutorials/CLAUDE.md` and the social-graph reference page). h1 + `.subtitle`, then 4 `.card-section` blocks, each an `<h2>` followed by `table.layout` with one `<tr>`: left `<td class="text-col">` (50%) holding `.tags` pills, a `<ul>` of one-line bullets with `<b>` lead terms, an italic `.example` line, and a `.key-point` callout; right `<td class="viz-col">` (50%) holding one canvas.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6. h1 2rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem. h2 1.3rem `#1a5276` with 2px `#2980b9` bottom border. Bullets 0.92rem, `li b` colored `#1a5276`. `.example` italic `#555` 0.9rem. `.key-point`: background `#f8f9fa`, left border 3px solid `#e74c3c`, padding 8px 12px, 0.9rem.
- **Tag pills:** `.tag` inline-block, 0.72rem bold, padding 2px 10px, radius 10px; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Canvases:** 720×300 intrinsic, CSS `width:100%`, 1px `#e0e0e0` border, radius 4px; scaled by `window.devicePixelRatio` via a shared `setup(id)` helper (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates). Chart palette object `P`: blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Doc palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- No nav bar, no back/home links. In regenerated HTML, any card links use `.html` extensions.
