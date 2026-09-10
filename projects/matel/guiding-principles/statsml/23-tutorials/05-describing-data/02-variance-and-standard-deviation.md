# Variance & Standard Deviation

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column `table.layout` — text left 50%, canvas right 50%)
**HTML title tag:** Variance & Standard Deviation

**Subtitle:** Two coffee shops both average a 5-minute wait — only one of them ever makes you wait 15. Spread is the difference.

## Same Average Wait, Very Different Mornings

**Tags:** `core idea` (blue), `running example` (green)

- **Shop A's** last five waits: 4, 5, 5, 5, 6 minutes — mean 5
- **Shop B's** last five waits: 1, 2, 3, 4, 15 minutes — mean also 5
- **The mean is identical**; your morning is not
- **Spread** — how far waits typically land from that 5-minute average — is what differs
- **Std dev** ≈ 0.6 min at A, ≈ 5.1 min at B: A is a promise, B is a gamble

*Example:* If you have 8 minutes before your bus, Shop A is safe every day; Shop B strands you one visit in five.

**Key point:** Standard deviation is the "typical distance from the average" — the number the mean alone can never tell you.

### Visualization (canvas `c1`, 720×300)

Two dot strips on the same 0–16 minute axis, mean line at 5.

- **Title (bold 15px, `#1a5276`, top center):** "Five Waits Each — Same Mean of 5 Minutes".
- **Axis:** shared bottom x-axis at y=240, from 0 to 16 minutes, ticks and labels every 2 (0, 2, 4, ..., 16), axis stroke `#999`, tick labels 12px `#6b7280`; axis title "wait (minutes)" centered below.
- **Mean line:** vertical dashed violet line (`#4a3aa7`, width 2, dash 6/4) at x=5 from y=44 down to the axis, labeled above in bold 12px violet: "both means = 5".
- **Shop A row (y=95):** green dots (`#008300`, radius 7) at values [4, 5, 5, 5, 6]; duplicate 5s stack vertically 16px apart. Left labels (right-aligned at x=92): "Shop A" bold 13px `#2c3e50`, "sd 0.6" 12px `#6b7280`.
- **Shop B row (y=180):** orange dots (`#d95926`, radius 7) at values [1, 2, 3, 4, 15], each with its value labeled below in 12px `#2c3e50`. Left labels: "Shop B" bold 13px, "sd 5.1" 12px muted.
- **Annotations:** bold 13px orange two-line callout above Shop B row near x≈13: "one 15-minute wait" / "hides inside a \"5-minute average\"". Bold 12px green above Shop A row near x≈10.5: "A: everything within 1 min of the mean".

## Computing Shop B's Spread by Hand

**Tags:** `worked example` (green)

- **Step 1 — deviations** from the mean of 5: −4, −3, −2, −1, +10
- **Step 2 — square each**: 16, 9, 4, 1, 100 (squaring kills the minus signs)
- **Step 3 — variance** = average of the squares = 130 ÷ 5 = 26 "square minutes"
- **Step 4 — std dev** = √26 ≈ 5.1 minutes — back in plain minutes
- **Shop A, same recipe**: (1+0+0+0+1) ÷ 5 = 0.4 → √0.4 ≈ 0.6 minutes

*Example:* Read "std dev 5.1" as: a typical Shop B wait misses the 5-minute average by about 5 minutes.

**Key point:** Variance is the machinery (squared units); standard deviation is the answer you report, in the data's own units.

### Visualization (canvas `c2`, 720×300)

Bar chart of squared deviations — Shop B's 100 towers over everything.

- **Title (bold 15px, `#1a5276`, top center):** "Squared Deviations: Shop B = 16 + 9 + 4 + 1 + 100 = 130".
- **Axes:** L-shaped axis (`#999`), padding top 52 / bottom 60 / left 60 / right 30; y scale 0–110 with gridline labels at 0, 50, 100 (12px `#6b7280`, light gridlines `#e5e9ef`).
- **Bars:** 10 bars, 46px wide, 12px gap, group gap 40px, starting at x = left pad + 20.
  - Shop A group: squared deviations [1, 0, 0, 0, 1], fill green `#008300`; below each bar the original wait [4, 5, 5, 5, 6] in muted 12px; squared value labeled above each bar in 12px `#2c3e50`.
  - Shop B group: squared deviations [16, 9, 4, 1, 100]; last bar (100) solid orange `#d95926`, others `rgba(217,89,38,0.45)`; below-bar wait labels [1, 2, 3, 4, 15].
- **Group labels (bold 12px, near top):** green "Shop A: total 2 / 5 = 0.4 -> sd 0.6" centered over the A group; orange "Shop B: total 130 / 5 = 26 -> sd 5.1" centered over the B group.
- **Annotation:** bold 13px orange near the tall bar: "the 15-min wait alone contributes 100 of the 130".
- **Caption (bottom center, 12px `#6b7280`):** "each bar = one wait; label below = the wait, height = its squared deviation from 5".

## Where a Data Scientist Meets This

**Tags:** `where it's used` (blue), `rule of thumb` (green)

- **Same-mean traps** — comparing only averages hides risk: delivery times, latencies, returns
- **Promises** — "served in under 10 minutes" always holds at A; B broke it with the 15
- **Rule of thumb** — for bell-shaped data, most values land within 2 std devs of the mean
- **Shop A**: 5 ± 2×0.6 → expect 3.8 to 6.2 min; **Shop B**: 5 ± 2×5.1 → almost anything
- **Everywhere in ML** — z-scores, feature scaling, control charts all run on std dev

*Example:* A dashboard showing only mean wait would score the two shops as identical forever.

**Key point:** The mean tells you where the center is; the standard deviation tells you how much to trust it.

### Visualization (canvas `c3`, 720×300)

Interval bands: mean ± 2 sd for both shops on a shared minutes axis.

- **Title (bold 15px, `#1a5276`, top center):** "What to Expect: Mean +/- 2 Standard Deviations".
- **Axis:** bottom x-axis at y=235, 0–16 minutes, ticks/labels every 2, stroke `#999`, labels 12px `#6b7280`; axis title "wait (minutes)"; values below 0 clamp to 0.
- **Promise line:** vertical dashed red line (`#e74c3c`, width 2, dash 6/4) at x=10 from y=46 to the axis, labeled above in bold 12px red: "\"under 10 minutes\" promise".
- **Bands** (32px tall rectangles, fill at 22% alpha, 2px stroke, vertical tick at the mean):
  - Shop A at y=100: 3.8 to 6.2, mean 5, green `#008300`; left labels "Shop A" (bold 13px) and "5 ± 1.2" (12px muted).
  - Shop B at y=180: 0 to 15.2, mean 5, orange `#d95926`; left labels "Shop B" and "5 ± 10.2".
- **Annotations:** bold 12px green at x≈6.8, y=96: "3.8 to 6.2 min — never near the promise". Bold 13px orange at x≈4.6, y=144: "same mean, but the band crosses the promise line".
- **Caption (12px `#6b7280`, y=208):** "bands: mean ± 2 sd (bell-curve rule of thumb; 5 waits is a tiny sample — illustrative)".

## The Confusion: Variance Is Not in Minutes

**Tags:** `common mistake` (red), `units` (orange)

- **Variance ≠ std dev** — variance 26 is in "squared minutes"; nobody waits 26 anything
- **Compare after the root** — 5.1 min of spread against the 5-min mean is meaningful; 26 is not
- **n vs n−1** — software often divides by n−1: 130 ÷ 4 = 32.5, sd ≈ 5.7; same story
- **Zero spread** — std dev 0 means every wait was identical, not that the shop is fast

*Example:* "Variance is 26 and the mean is 5, so spread is five times the mean" mixes minutes with squared minutes.

**Common mistake:** Reporting variance where humans expect minutes. Take the square root before talking.

### Visualization (canvas `c4`, 720×300)

Flow diagram: variance → √ → std dev conversion boxes for both shops.

- **Title (bold 15px, `#1a5276`, top center):** "Variance Lives in Squared Units — Take the Root Before Reporting".
- **Boxes** (220×78, 2px stroke, three text lines: label 12px `#2c3e50`, big value bold 16px in shop color, unit line 12px `#6b7280`):
  - Shop B row (y=60): left box at x=60 fill `rgba(217,89,38,0.10)` stroke orange — "Shop B variance" / "26" / "square minutes (min²)"; right box at x=440 fill `rgba(217,89,38,0.18)` — "Shop B std dev" / "5.1" / "minutes — report this".
  - Shop A row (y=170): left box fill `rgba(0,131,0,0.08)` stroke green — "Shop A variance" / "0.4" / "square minutes (min²)"; right box fill `rgba(0,131,0,0.14)` — "Shop A std dev" / "0.6" / "minutes — report this".
- **Arrows:** violet (`#4a3aa7`, width 2.5) horizontal arrows from x=290 to x=430 between each box pair, labeled above in bold 14px: "take √".
- **Bottom annotation (bold 13px magenta `#d55181`, centered, y=282):** "26 vs 0.4 exaggerates the gap (65x); in real minutes it is 5.1 vs 0.6 (about 8x)".

## Regeneration instructions

- **Layout:** tutorial detail page. h1 (no index number) with 2px `#2980b9` bottom border, `.subtitle` paragraph, then four `.card-section` blocks. Each section: `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one `<tr>`: left `td.text-col` (50%) holding `.tags` pills, a `<ul>` of bullets (each starting with `<b>` term in `#1a5276`), an italic `.example` paragraph, and a `.key-point` callout; right `td.viz-col` (50%) holding one canvas.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; bullets 0.92rem; `.example` italic `#555` 0.9rem; `.key-point` background `#f8f9fa`, left border 3px solid `#e74c3c`, padding 8px 12px, 0.9rem. No nav bar, no back/home links.
- **Tag pills:** `.tag` inline-block 0.72rem bold, padding 2px 10px, radius 10px; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Canvas:** intrinsic 720×300, CSS `width: 100%`, 1px `#e0e0e0` border, radius 4px; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object `P`:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- In regenerated HTML, any card links use `.html` extensions.
