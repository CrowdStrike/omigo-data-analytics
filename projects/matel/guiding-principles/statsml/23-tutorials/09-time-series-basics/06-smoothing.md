# Smoothing

**Page type:** detail page (tutorial card-sections: h2 per section, two-column layout table — text left 50% with tag pills/bullets/example/key-point, canvas right 50%)
**HTML title tag:** Smoothing

**Subtitle:** Every smoother is a weighted average of the recent past — the only real question is how much the past should outweigh today

## Two Ways to Calm the Same Traffic Line

**Tags:** `core idea` (blue), `running example` (green)

- **Same goal** — daily visitor counts jump around; we want the underlying level
- **Simple average** — a 7-day moving average gives all 7 days an equal vote (1/7 each)
- **Exponential smoothing** — weighs today most, yesterday a bit less, fading toward zero
- **The weights** — with weight 0.3 on today: 0.30, 0.21, 0.15, 0.10, 0.07, ... never quite zero
- **Why bother** — fading votes react faster to real change while still calming the noise

*Example:* In the 7-day average, a day from last week votes as much as today; exponentially, it votes barely a tenth as much.

**Key point:** Smoothing is a weighted average of the recent past — the only real difference between smoothers is the shape of the weights.

### Visualization (canvas `c1`, 720×300)

Two side-by-side bar panels comparing the weight given to each past day, separated by a dashed vertical divider (`#bdc3c7`, dash 4/3, at x=360).

- **Title (bold 15px, `#1a5276`, top center):** "How much does each past day count toward the smooth value?"
- **X labels (both panels):** `['today', '-1', '-2', '-3', '-4', '-5', '-6']`
- **Left panel (x0=35, 300px wide, baseline y=240, bar scale max 0.33):** title bold 13px `#1a5276` "7-day moving average: equal votes"; 7 bars of 0.143 each (32px wide), fill `rgba(42,120,214,0.55)`; 11px weight labels above bars rendered as ".14"; note below in bold 12px blue `#2a78d6`: "old news counts as much as today"
- **Right panel (x0=390, same geometry):** title "exponential (weight 0.3): fading votes"; bars `[0.300, 0.210, 0.147, 0.103, 0.072, 0.050, 0.035]`, fill `rgba(74,58,167,0.55)`; labels ".30", ".21", ".15", ".10", ".07", ".05", ".04"; note in bold 12px violet `#4a3aa7`: "today speaks loudest, the past fades"

## Exponential Smoothing by Hand: One Line of Arithmetic

**Tags:** `worked example` (green), `do it by hand` (blue)

- **The rule** — new smooth = 0.3 × today + 0.7 × yesterday's smooth; that is the whole method
- **Start** — the smooth value stands at 1,000 visitors
- **Day 1** — traffic 1,200: 0.3 × 1200 + 0.7 × 1000 = 1060
- **Day 2** — traffic 900: 0.3 × 900 + 0.7 × 1060 = 1012
- **Day 3** — traffic 1,100: 0.3 × 1100 + 0.7 × 1012 = 1038 (1038.4)
- **Memory** — one running number carries the whole history; no window of old days to store

*Example:* The smooth value moved 1000 → 1060 → 1012 → 1038 while raw traffic swung 1200 → 900 → 1100.

**Key point:** Exponential smoothing keeps a single running value and nudges it 30% of the way toward each new day.

### Visualization (canvas `c2`, 720×300)

Combo chart: three raw-traffic bars with the smooth value plotted as a dotted-point line across four steps, plus a legend.

- **Title (bold 15px, `#1a5276`, top center):** "Each step: new smooth = 0.3 × today + 0.7 × previous smooth"
- **Data:** actual `[1200, 900, 1100]` (bars on day 1–3); smooth `[1000, 1060, 1012, 1038.4]` at x labels `['start', 'day 1', 'day 2', 'day 3']`
- **Axes:** y from 800 to 1300 with labels at 900, 1000, 1100, 1200; L-shaped axis frame `#999`; padding t:52 b:48 l:58 r:150
- **Bars:** 54px wide, fill `rgba(42,120,214,0.4)` with blue `#2a78d6` outline; bold 12px blue value labels above
- **Smooth line:** orange `#d95926` width 3 connecting 4 points, orange dots radius 5, bold 12px labels "1000", "1060", "1012", "1038" beside each point (final value rounded to 1038)
- **Legend (right side, 12px, swatch squares):** `rgba(42,120,214,0.55)` "raw traffic"; orange "smooth value"
- **Annotation (bold 13px orange, near top right):** "moves 30% of the way toward each bar"

## The Noise-vs-Delay Dial: Pick the Weight for the Decision

**Tags:** `why it matters` (orange), `trade-off` (red)

- **The event** — traffic really dropped from about 1,000 to about 700 on day 16
- **Heavy weight (0.5)** — reads 738 by day 20; a bit noisy but only days behind reality
- **Light weight (0.1)** — still reads 760 on day 30, two weeks after the drop
- **Alerts** — an alarm watching the 0.1 line fires long after customers felt the problem
- **Forecasts** — the same running value doubles as the simplest forecast of tomorrow

*Example:* The on-call dashboard used a very light weight (0.1) to look tidy — and paged the team 12 days after the crash.

**Key point:** Every smoother trades noise for delay — pick fast-reacting weights for alerts, calm ones for reading long-term trend.

### Visualization (canvas `c3`, 720×300)

Line chart: 30-day series with a real drop after day 15, seen through exponential smoothing with weight 0.5 vs weight 0.1.

- **Title (bold 15px, `#1a5276`, top center):** "The drop on day 16, seen through weight 0.5 vs weight 0.1"
- **Data (30 days, drop after day 15):** `[1000,1052,990,1018,1033,946,976,1018,967,1022,1058,979,998,1012,939,690,738,685,728,744,656,683,708,652,714,754,686,716,721,639]`
- **Smoothed series:** EMA with alpha 0.5 and alpha 0.1, both seeded at the first value (`s = a[0]`, then `s = alpha·a[i] + (1−alpha)·s`)
- **Axes:** y from 550 to 1150 with labels at 700, 900, 1100; L-shaped axis frame `#999`; padding t:46 b:58 l:58 r:20; x labels "day 1", "day 8", "day 16", "day 23", "day 30"
- **Drop marker:** dashed red `#e74c3c` vertical line (dash 5/4, width 1.5) at day 16 with bold 12px red label "real drop"
- **Raw series:** faint line `rgba(42,120,214,0.35)` width 1.5
- **Smoothed lines:** alpha 0.5 in green `#008300`, alpha 0.1 in violet `#4a3aa7`, both width 3
- **Annotations:** bold 12px green: "weight 0.5: reads 738 by day 20"; bold 13px violet: "weight 0.1: still 760 on day 30"
- **Caption (12px gray, bottom center):** "faint line = raw daily visitors (illustrative)"

## The Smooth Line Is Not the Data

**Tags:** `common mistake` (red), `core idea` (blue)

- **The spike** — day 11 hit 1,400 visitors, the biggest day the site ever had
- **The smooth line** — peaked at just 1,122; smoothing shaved 278 off the spike
- **Shaved peaks** — every smoother pulls extremes toward the middle, so records vanish
- **Report right** — read peaks, records and single-day events from the raw series
- **Both lines** — plot raw and smoothed together so nobody mistakes one for the other

*Example:* "Our best day was 1,122 visitors" — the real record was 1,400; the report had quoted the smoothed line.

**Key point:** Smoothed values are estimates of the underlying level, not observations — never quote them as what actually happened.

### Visualization (canvas `c4`, 720×300)

Line chart: a 21-day series with a one-day record spike, raw vs exponentially smoothed, with the shaved gap highlighted.

- **Title (bold 15px, `#1a5276`, top center):** "The record day (1,400) vs what the smooth line shows (1,122)"
- **Data (21 days, spike on day 11):** `[1000,1020,970,1024,995,984,1029,973,1010,1011,1400,1029,985,994,1025,970,1019,1001,979,1030,977]`
- **Smoothed series:** EMA with alpha 0.3, seeded at the first value; its peak at the spike day is 1,122
- **Axes:** y from 850 to 1500 with labels at 900, 1100, 1300; L-shaped axis frame `#999`; padding t:46 b:44 l:58 r:20; x labels "day 1", "day 7", "day 14", "day 21"
- **Raw series:** blue `#2a78d6` line width 2; **smoothed series:** orange `#d95926` line width 3
- **Peak markers:** blue dot (radius 5) at the raw peak with bold 13px label "real record: 1,400"; orange dot at the smoothed peak with label "smooth peak: 1,122"
- **Gap marker:** dashed red `#e74c3c` vertical line (dash 4/3, width 1.5) connecting the two peak points; bold 13px red right-aligned annotation: "smoothing shaved 278 off the record"
- **Caption (12px gray, bottom center):** "raw daily visitors (blue) and exponential smooth, weight 0.3 (orange) — illustrative"

## Regeneration instructions

- **Layout:** tutorial detail page — `<h1>` + `.subtitle`, then four `.card-section` blocks, each an `<h2>` (1.3rem `#1a5276`, 2px solid `#2980b9` bottom border) followed by `table.layout` (full width, one row): left `td.text-col` 50% with `.tags` pill row, a `<ul>` of one-line bullets each opening with a `<b>` term (bold terms colored `#1a5276`), an italic `.example` paragraph, and a `.key-point` callout; right `td.viz-col` 50% holding one canvas.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem; bullets 0.92rem; `.example` italic `#555` 0.9rem; `.key-point` background `#f8f9fa`, left border 3px solid `#e74c3c`, padding 8px 12px, 0.9rem; canvases `width:100%` with 1px `#e0e0e0` border, 4px radius.
- **Tag pills:** inline-block, 0.72rem bold, padding 2px 10px, radius 10px — blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Charts:** shared JS palette `P = { blue:#2a78d6, green:#008300, magenta:#d55181, yellow:#c98500, aqua:#199e70, orange:#d95926, violet:#4a3aa7, ink:#1a5276, text:#2c3e50, mute:#6b7280, grid:#e5e9ef }`; site palette #1a5276 primary blue, #27ae60 green, #e74c3c red, #e67e22 orange. Canvases declare intrinsic 720×300 and scale by `window.devicePixelRatio` via a shared `setup(id)` helper that reads the `width`/`height` attributes, sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. A shared `ema(a, alpha)` helper computes exponential smoothing seeded at the first value. Both data arrays (`drop30`, `spike21`) are hardcoded literal arrays labeled "illustrative" in captions — no `Math.random()`. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- No nav bar, no back/home links, no cross-page links. In regenerated HTML any card links would use `.html` extensions.
