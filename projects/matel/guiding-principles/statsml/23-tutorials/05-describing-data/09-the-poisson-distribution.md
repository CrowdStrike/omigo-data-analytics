# The Poisson Distribution

**Page type:** detail page (tutorial page: 4 card-sections, each a two-column layout table — text left 50%, canvas right 50%)
**HTML title tag:** The Poisson Distribution

**Subtitle:** How many rare events land in a window of time — customer arrivals, typos, machine failures

## A Coffee Shop, Ten Minutes at a Time

**Tags:** `arrivals` (blue), `rates` (green), `core idea` (blue)

- **The setup** — a coffee shop averages 4 customers per 10-minute window, all day long
- **Watch 100 windows** — the counts wobble: 20 windows saw exactly 3, 19 saw 4, 2 saw none
- **One knob** — the whole shape comes from a single number: the average rate (here, 4)
- **When it applies** — arrivals are independent and the average rate holds steady
- **Same math** — typos per page, failures per month, goals per match, calls per minute

*Example:* Two of the 100 windows had zero customers — and one had ten; both from the same steady rate of 4.

**Key point:** The Poisson distribution answers "the average is 4 per window — so how often will I see 0, or 7, or 10?" using only that average.

### Visualization (canvas `c1`, 720×300)

Bar chart: observed tally of customers per window across 100 windows.

- **Title (bold 15px, ink #1a5276, centered):** "100 Ten-Minute Windows: Customers per Window (illustrative)"
- **Data:** counts of windows seeing k customers, k = 0..10: `[2, 7, 15, 20, 19, 16, 10, 6, 3, 1, 1]` (sums to 100, mean 3.98)
- **Axes:** padding top 48 / bottom 46 / left 55 / right 20; y max 24 with labels 0, 10, 20 (gridlines `#e5e9ef` above 0); x labels 0..10; axis lines `#999`
- **Bars:** k=3 and k=4 in solid aqua `#199e70`; all others in `rgba(25,158,112,0.45)`; count labels (12px, text `#2c3e50`) above each bar
- **X-axis caption (mute `#6b7280`):** "customers arriving in the window"
- **Annotations (left-aligned at ~5.2 bars in):** magenta (`#d55181`) bold 13px: "average = 4, but single windows range 0 to 10"; orange (`#d95926`) bold 12px below: "2 empty windows, 1 with ten people"

## Building the Whole Curve from One Number

**Tags:** `worked example` (green), `rates` (blue)

- **Start** — P(0 customers) = e⁻⁴ ≈ 1.8%, about 1 empty window in 55
- **Climb the ladder** — each next step: P(k) = P(k−1) × 4 ÷ k; nothing else needed
- **Check it** — P(1) = 1.8% × 4 = 7.3%; P(2) = 7.3% × 2 = 14.7%; P(3) = 14.7% × 4/3 = 19.5%
- **The peak** — P(4) = P(3) × 4/4, so 3 and 4 customers tie at 19.5% each
- **The fade** — past the average, the ×4/k factor shrinks below 1 and the bars taper off

*Example:* P(5) = 19.5% × 4/5 = 15.6% — you can extend the whole chart with a pocket calculator.

**Key point:** One average (4) generates every bar. That is why Poisson is the default model when all you know about rare events is their rate.

### Visualization (canvas `c2`, 720×300)

Bar chart: theoretical Poisson(4) probabilities with the ladder recursion annotated.

- **Title (bold 15px, ink #1a5276, centered):** "Poisson, Rate 4: Each Bar Is the Previous Bar × 4/k"
- **Data:** Poisson(4) percentages, k = 0..10: `[1.8, 7.3, 14.7, 19.5, 19.5, 15.6, 10.4, 6.0, 3.0, 1.3, 0.5]`
- **Axes:** padding top 48 / bottom 46 / left 55 / right 20; y max 23% with labels 0%, 10%, 20% (gridlines `#e5e9ef`); x labels 0..10
- **Bars:** k=3 and k=4 green `#008300`; others blue `#2a78d6`; percent labels (12px, text `#2c3e50`) above each bar ("1.8%" … "0.5%")
- **X-axis caption (mute):** "customers in a 10-minute window"
- **Annotations (left-aligned at ~5.6 bars in):** magenta (`#d55181`) bold 13px, two lines: "start: P(0) = e⁻⁴ ≈ 1.8%" / "then: P(k) = P(k−1) × 4/k"; green (`#008300`) bold 12px below: "3 and 4 tie at the top: ×4/4 = 1"

## Why It Matters: Staffing for the Spikes

**Tags:** `where it's used` (blue), `rule of thumb` (green)

- **Planning trap** — staffing for the average of 4 means being swamped in every above-average window
- **Tail math** — 9 or more customers arrive in about 2.1% of windows: one roughly every 8 hours
- **Spread for free** — Poisson variance equals the mean: rate 4 means sd = √4 = 2
- **Alerting** — server errors average 4/hour; an hour with 12 is a 1-in-1,000 event, page someone
- **Capacity** — queues, call centers, and ER beds are all sized off this tail, not the mean

*Example:* A solo barista who can serve 8 per window still faces a 9-plus rush about once per 8-hour shift.

**Key point:** The average tells you the typical window; the Poisson tail tells you how often the bad windows come — and the bad windows set the staffing.

### Visualization (canvas `c3`, 720×300)

Bar chart: Poisson(4) extended to k=12 with the 9+ tail highlighted and a mean marker.

- **Title (bold 15px, ink #1a5276, centered):** "Staff for the Tail, Not the Average"
- **Data:** percentages, k = 0..12: `[1.8, 7.3, 14.7, 19.5, 19.5, 15.6, 10.4, 6.0, 3.0, 1.3, 0.5, 0.2, 0.1]`
- **Axes:** padding top 48 / bottom 46 / left 55 / right 20; y max 23%; x labels 0..12; axis lines `#999`
- **Tail shading:** background rectangle over k=9..12 filled `rgba(231,76,60,0.08)` spanning full chart height
- **Bars:** k ≥ 9 red `#e74c3c`; others `rgba(42,120,214,0.55)`; x labels only (no value labels)
- **Mean marker:** vertical dashed green line (`#008300`, width 2, dash 6/4) at k=4.5 slot; green bold 12px label to its right: "mean 4, sd = √4 = 2"
- **Tail annotation (right-aligned at chart right edge, red `#e74c3c` bold 13px, two lines):** "9+ customers: 2.1% of windows" / "≈ one swamped window every 8 hours"
- **X-axis caption (mute):** "customers in a 10-minute window"

## The Mistake: Expecting Arrivals to Be Evenly Spaced

**Tags:** `common mistake` (red), `clumping` (orange)

- **The intuition** — "4 per 10 minutes" feels like one customer every 2.5 minutes, like clockwork
- **The reality** — random arrivals clump: three people in one minute, then a 7-minute silence
- **Clumps are normal** — gaps and bursts are what a steady random rate looks like
- **False alarms** — treating every burst as "something happened" means chasing pure noise
- **Real signal** — variance far above the mean means the rate itself shifts (lunch rush)

*Example:* The barista swears customers "always come in waves" — the waves are exactly what rate-4 randomness produces.

**Common mistake:** Reading clusters as a cause. Under Poisson, clumping is the default; only clumping beyond mean = variance needs an explanation.

### Visualization (canvas `c4`, 720×300)

Two horizontal timelines comparing evenly-spaced expectation vs clumped random arrivals over one hour.

- **Title (bold 15px, ink #1a5276, centered):** "One Hour, 24 Customers: Expectation vs Randomness (illustrative)"
- **Layout:** timelines from x=60 spanning width−100; time mapped 0–60 minutes; Row A at y=105, Row B at y=205; axis lines `#999`
- **Row A (evenly spaced):** mute (`#6b7280`) bold 12px label above: "what people expect: one every 2.5 minutes"; 24 mute-filled dots (radius 5) at t = 1.25 + i×2.5 minutes
- **Row B (random, clumped):** blue (`#2a78d6`) bold 12px label above: "what randomness delivers: same 24 people, same hour"; 24 blue dots at hardcoded times: `[1.2, 1.9, 2.3, 8.7, 9.1, 12.4, 12.8, 13.1, 13.5, 19.9, 24.2, 24.9, 28.3, 33.0, 33.4, 33.9, 41.1, 45.6, 46.0, 52.3, 55.1, 55.8, 56.2, 59.4]`
- **Clump annotation:** orange (`#d95926`) 2px bracket above Row B spanning t=12.0–13.9, with orange bold 12px centered label: "4 people in 1.1 min"
- **Gap annotation:** red (`#e74c3c`) 2px bracket below Row B spanning t=34.3–40.7, with red bold 12px label: "6.4-minute silence"
- **Time labels (mute 12px):** 0, 10, 20, 30, 40, 50, 60 below Row B; axis caption "minutes past the hour"
- **Takeaway (magenta `#d55181` bold 13px, centered at y=55):** "clumps and gaps ARE the steady rate — not a story"

## Regeneration instructions

- **Layout:** tutorial detail page. `<h1>` (no index number) + `.subtitle`, then four `.card-section` blocks, each with an `<h2>` (1.3rem, `#1a5276`, 2px solid `#2980b9` bottom border) and a `table.layout` (width 100%, border-collapse) containing one row: `td.text-col` (50%) and `td.viz-col` (50%), both `vertical-align: top`, padding 12px.
- **Text column structure:** `.tags` pill row first, then a `<ul>` of one-line bullets each opening with `<b>bold term</b>` (bold terms colored `#1a5276`), one italic `.example` paragraph, one `.key-point` callout (background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem).
- **Tag pills:** inline-block, 0.72rem, weight 600, padding 2px 10px, radius 10px. Colors — blue: bg `rgba(26,82,118,0.12)` text `#1a5276`; green: bg `rgba(39,174,96,0.15)` text `#27ae60`; red: bg `rgba(231,76,60,0.12)` text `#e74c3c`; orange: bg `rgba(230,126,34,0.15)` text `#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px solid `#2980b9` bottom border; `.subtitle` `#666` 0.95rem; canvases `width: 100%` with `1px solid #e0e0e0` border, 4px radius.
- **Canvases:** all 720×300 intrinsic; scaled via `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) through a shared `setup(id)` helper. Data arrays are hardcoded literals (no `Math.random()`); invented tallies are labeled "(illustrative)" in chart titles. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette (JS object `P`):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- No nav bar, no back/home links, no cross-page links. In regenerated HTML any card links use `.html` extensions (this page has none).
