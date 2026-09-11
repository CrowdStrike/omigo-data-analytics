# Margin of Error

**Page type:** detail page (tutorial layout: h1 + subtitle, 4 `.card-section` blocks each with h2 and a two-column `table.layout` — text left 50%, canvas right 50%)
**HTML title tag:** Margin of Error

**Subtitle:** An election poll says "52% ± 3" — the ±3 is the wobble you would see across repeat random samples, it comes from the sample size, and it covers sampling luck only

## What "52% ± 3" Is Trying to Say

**Tags:** `core idea` (blue), `running example` (green)

- **The poll** — 1,067 randomly picked voters are asked; 555 back candidate A, which is 52%
- **The worry** — a different random 1,067 would give 51, 53, maybe 50 — not exactly 52
- **The fix** — publish the wobble: 52% ± 3 means "likely between 49% and 55%"
- **The catch** — 50% sits inside that range, so the "lead" may not exist at all
- **The habit** — every number computed from a sample carries a ±, printed or not

*Example:* Tasting one spoonful to judge a pot of soup — the ± says how much one spoonful can mislead you about the pot.

**Margin of error:** the give-or-take you would see if you could rerun the same random poll many times — a report on the sample, not a guarantee about the truth.

### Visualization (canvas `c1`, 720×300)

Interval band on a percent axis: the 52% ± 3 band with the 50% tie line falling inside it.

- **Title (bold 15px `#1a5276`, top center):** "The Poll Result Is a Band, Not a Number"
- **Axis:** horizontal percent scale 44% to 60%, tick labels every 2% (12px gray `#6b7280`); gray `#999` axis line at y=225; left pad 60, right pad 40. Axis caption: "support for candidate A (555 of 1,067 voters said A)".
- **Band:** translucent blue `rgba(42,120,214,0.14)` rectangle spanning 49% to 55% for the plot height.
- **Interval bar (y=145):** blue `#2a78d6` 5px line from 49 to 55 with 3px end caps (±10px) and an 8px center dot at 52; labels "52%" (bold 13px, above the dot), "49%" and "55%" (12px, below the end caps).
- **Tie line:** dashed (6/4) magenta `#d55181` 2px vertical line at 50%, with left-aligned bold 13px magenta annotation: "50% (a tie) sits INSIDE the band — too close to call".

## Where the 3 Comes From: 1,067 Voters

**Tags:** `worked example` (green), `arithmetic` (blue)

- **One input matters** — sample size: the 1,067 interviews set the ±3, not the millions of voters
- **The formula** — ± ≈ 2 × √(0.52 × 0.48 ÷ 1067) ≈ 0.03, i.e. 3 points
- **The shortcut** — ± ≈ 1 ÷ √n: here 1 ÷ √1067 ≈ 0.031 — three points again
- **Check n = 400** — 1 ÷ √400 = 0.05, so a 400-person poll carries about ±5
- **Diminishing returns** — halving the ± costs 4× the interviews; ±1 needs ~10,000 people

*Example:* This is why national polls all hover around 1,000 respondents — ±3 is the price-performance sweet spot.

**Bought with sample size:** precision follows √n, so every halving of the margin quadruples the interviewing bill.

### Visualization (canvas `c2`, 720×300)

Decay curve of margin of error vs sample size (± = 100/√n), with five marked points.

- **Title (bold 15px `#1a5276`, top center):** "The ± Is Bought With Sample Size (± ≈ 1 ÷ √n)"
- **Curve:** aqua `#199e70`, 3px, plotting 100/√n for n from 90 to 10,500 in steps of 30 (values above the y max clipped); deterministic formula.
- **Axes:** x from 0 to 10,500 with tick labels 0, 2,000, 4,000, 6,000, 8,000, 10,000; y max ±11 with labels ±0 through ±10 in steps of 2; gray `#999` axis lines; padding top 50, bottom 58, left 70, right 40. X-axis caption: "people polled (n)".
- **Marked points (aqua 5px dots; the n=1,067 point orange `#d95926`, 7px, bold label):**
  - "n=100: ±10"
  - "n=400: ±5"
  - "n=1,067: ±3  ← the classic poll"
  - "n=2,500: ±2"
  - "n=10,000: ±1" (label right-aligned left of the dot so it stays inside the canvas)
- **Annotation (bold 13px orange):** "to halve the ±, quadruple the sample"

## Covered: Sampling Luck. Not Covered: Everything Else

**Tags:** `common mistake` (red), `bias` (orange)

- **Covered** — the luck of which random 1,067 people happened to be reached
- **Not covered: a skewed list** — polling only landlines or only app users; ±3 says nothing
- **Not covered: non-response** — if one side hangs up more often, the center itself is off
- **Not covered: wording** — a leading question shifts every answer; no formula tracks it
- **Size cannot fix bias** — a skewed poll of 10,000 is precisely wrong: ±1 around the wrong spot

*Example:* The 1936 Literary Digest poll reached 2.4 million people through car and phone lists — and still called the election for the loser.

**The blind spot:** the ± measures random noise around the poll's center — bias moves the center itself, and no margin of error will confess to that.

### Visualization (canvas `c3`, 720×300)

Three stacked interval bars against a dashed truth line, showing how bias shifts the whole band and larger n only tightens it around the wrong spot.

- **Title (bold 15px `#1a5276`, top center):** "Bias Moves the Whole Band — More People Won't Move It Back"
- **Axis:** percent scale 42% to 58%, tick labels every 4% (12px gray); gray `#999` axis line at y=240; left pad 235 (row labels there), right pad 40. Axis caption: "measured support for candidate A (illustrative)".
- **Truth line:** dashed (7/4) green `#008300` 2.5px vertical line at 47%, labeled bold 13px green above: "true support: 47%".
- **Rows (interval bars 5px with ±9px end caps and 6px center dot, y = 85/137/189):**
  1. "random sample, n=1,067" — 44 to 50, center 47, blue `#2a78d6`, right note "±3, covers the truth"
  2. "app users only, n=1,067" — 49 to 55, center 52, orange `#d95926`, right note "±3, misses it"
  3. "app users only, n=10,000" — 51 to 53, center 52, magenta `#d55181`, right note "±1, misses it harder"
- Row labels bold 12px in row color, right-aligned left of the plot; notes 12px to the right of each bar.
- **Annotation (bold 13px magenta, right-aligned near bottom):** "a bigger skewed sample = precisely wrong"

## The Same ± Hides in Your Dashboards

**Tags:** `where it's used` (blue), `rule of thumb` (green)

- **Every metric** — conversion rate, click rate, churn: all computed from samples, all carry a ±
- **Small n, big ±** — 5% conversion on 400 users is really 5% ± 2.1 points
- **A/B trap** — a lift smaller than either arm's ± is a coin flip, not a win
- **Head check** — 1 ÷ √n gives the worst-case ± for any percentage in your head
- **Report it** — a ± next to the number stops a lucky week being read as growth

*Example:* Variant B's 6.5% vs A's 5.0% on 400 users each sounds like a 30% lift — but both arms carry a ±2-point margin.

**Carry the habit over:** if a poll of 1,067 earns a ±3, your dashboard metric on 400 users deserves its ± too — sampling noise does not care what the number is called.

### Visualization (canvas `c4`, 720×300)

Four bars with error whiskers: the same A/B conversion comparison (5.0% vs 6.5%) at n=400 and at n=10,000 per variant.

- **Title (bold 15px `#1a5276`, top center):** "The Same \"5% vs 6.5% Lift\", at Two Sample Sizes"
- **Axes:** y from 0% to 10%, labels every 2% (12px gray, right-aligned); gray `#999` axis lines; padding top 52, bottom 62, left 70, right 30.
- **Bars (74px wide, fill at alpha 0.5, whiskers 2.5px in bar color with 12px caps, bold 12px label below each):**
  - x=140: A 5.0% ± 2.1, blue `#2a78d6`, label "A: 5.0% ± 2.1"
  - x=255: B 6.5% ± 2.4, violet `#4a3aa7`, label "B: 6.5% ± 2.4"
  - x=445: A 5.0% ± 0.4, blue `#2a78d6`, label "A: 5.0% ± 0.4"
  - x=560: B 6.5% ± 0.5, violet `#4a3aa7`, label "B: 6.5% ± 0.5"
- **Group captions (12px gray under labels):** "400 users per variant" (left pair), "10,000 users per variant" (right pair).
- **Annotations (bold 13px at top of each group):** orange `#d95926`: "whiskers overlap: coin flip"; green `#008300`: "whiskers separate: a real lift".

## Regeneration instructions

- **Template:** tutorial detail page (see `tutorials/CLAUDE.md`). h1 + `.subtitle`, then 4 `.card-section` blocks, each an `<h2>` followed by `table.layout` with one `<tr>`: left `<td class="text-col">` (50%) holding `.tags` pills, a `<ul>` of one-line bullets with `<b>` lead terms, an italic `.example` line, and a `.key-point` callout; right `<td class="viz-col">` (50%) holding one canvas.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6. h1 2rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem. h2 1.3rem `#1a5276` with 2px `#2980b9` bottom border. Bullets 0.92rem, `li b` colored `#1a5276`. `.example` italic `#555` 0.9rem. `.key-point`: background `#f8f9fa`, left border 3px solid `#e74c3c`, padding 8px 12px, 0.9rem.
- **Tag pills:** `.tag` inline-block, 0.72rem bold, padding 2px 10px, radius 10px; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Canvases:** 720×300 intrinsic, CSS `width:100%`, 1px `#e0e0e0` border, radius 4px; scaled by `window.devicePixelRatio` via a shared `setup(id)` helper (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates). Chart palette object `P`: blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Doc palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- No nav bar, no back/home links. In regenerated HTML, any card links use `.html` extensions.
