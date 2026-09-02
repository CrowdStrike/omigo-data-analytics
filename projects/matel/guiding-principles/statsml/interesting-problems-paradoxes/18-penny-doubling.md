# The Penny That Beats a Million

**Page type:** detail page (h2-sectioned two-column obj-table layout: text left 45%, canvas right 55%, one table per section; philosophy callouts at top and bottom)
**HTML title tag:** The Penny That Beats a Million — Case Study

**Subtitle:** The famous riddle: would you rather take $1,000,000 in cash today, or one penny that doubles every day for 30 days? Almost everyone takes the million. Almost everyone loses $9.7 million by doing so.

## Callout (philosophy box, top)

**The offer:** Option A — $1,000,000 handed to you right now. Option B — a single penny today, doubled tomorrow, doubled again the next day, for 30 days.

**Your gut says:** "A penny doubled a few times is pocket change. Thirty days of pocket change can't touch a million. Take the cash."

**The actual answer:** Option B pays `$5,368,709.12` on day 30 alone, and `$10,737,418.23` in total. The penny beats the million more than ten times over — and for the first three weeks it genuinely looks like a scam.

## 1. Why Everyone Takes the Million

**Obj-title:** For 27 Days, the Penny Looks Like a Joke

Math box 1:

**The day-by-day amounts (day n = 2ⁿ⁻¹ cents):**

Day 1: `$0.01`
Day 5: $0.16
Day 10: $5.12
Day 15: $163.84
Day 20: $5,242.88
Day 25: $167,772.16
Day 28: `$1,342,177.28` ← first day that alone beats the million
Day 30: `$5,368,709.12`

Math box 2:

**Where a normal decision would be made:**

At day 10 you hold about $10 in total. At day 15, about $327.
Two-thirds of the way through — day 20 — the running total is `$10,485.75`. Roughly 1% of the million.

Anyone judging the deal by its first twenty days would cancel it. The curve gives no visible warning of what the last week will do.

Bullets:

- **The trap:** Exponential growth spends most of its timeline looking flat, so the evidence available early on argues against the better choice.
- **The flip:** From day 28 onward every single day pays more than the entire Option A.

### Visualization (canvas `canvas1`, 720×360)

Bar chart: daily payout for days 1–30 on a linear dollar scale.

- **Title (bold 14px `#1a5276`, top center):** "The Daily Payout: Invisible for 3 Weeks, Then Vertical".
- **Plot area:** origin x=80, baseline y=300, width 590, height 240; axes `#1a5276` 2px.
- **Data:** day n bar height = 2ⁿ⁻¹ cents in dollars; scale max $5,368,709 (day 30).
- **Bars:** fill `rgba(26,82,118,0.35)`, bars for days 28–30 fill `#e74c3c` (the days that individually beat $1M).
- **Axis labels:** x "Day" (13px `#1a5276`, centered below), tick labels 1, 5, 10, 15, 20, 25, 30; y "Payout that day" rotated −90°; y tick labels $0, $1M, $2M, $3M, $4M, $5M (11px `#666`) with `#eee` gridlines.
- **$1M reference line:** horizontal dashed gray `#999` (dash 6/4, 1px) at $1,000,000 with 11px `#999` label "Option A: $1,000,000" left-aligned above it near the left edge.
- **Annotation (bold 12px `#e74c3c`, above day-30 bar):** "day 30: $5.37M".
- **Annotation (11px `#666`, with a short arrow or tick near day 20):** "day 20: $5,243".

## 2. The Mental Math

**Obj-title:** Doubling Means Each Day Equals Everything Before It

Math box 1:

**The identity that runs the riddle:**

`2ⁿ⁻¹ = (2⁰ + 2¹ + ... + 2ⁿ⁻²) + 1`

Each day's payout equals ALL previous days combined, plus one cent.
So the day-30 payout alone outweighs days 1–29 put together.
Half of the entire $10.7M arrives on the very last day.

Math box 2:

**When does the penny actually win?**

Running total after day n = `(2ⁿ − 1)` cents.
After day 26: $671,088.63 — still losing to Option A.
After day 27: `$1,342,177.27` — the penny is now ahead for good.

**And a shorter month ruins it:**
Stop at day 28 (a February): total `$2,684,354.55`.
Cutting 2 days off a 30-day run deletes 75% of the money.

Bullets:

- **The lever:** The exponent is the number of doublings, so small changes in duration produce enormous changes in outcome.
- **The check:** 2¹⁰ ≈ 1,000 — ten doublings per factor of a thousand is enough to reproduce every number on this page in your head.

### Visualization (canvas `canvas2`, 720×360)

Line chart with filled area: cumulative total vs the flat $1,000,000 option, days 0–30.

- **Title (bold 14px `#1a5276`, top center):** "Running Total vs the Million — the Crossover Comes at Day 27".
- **Plot area:** origin x=80, baseline y=300, width 590, height 240; axes `#1a5276` 2px.
- **Data:** cumulative after day n = (2ⁿ − 1) cents in dollars, n = 0..30; scale max $10,737,418.
- **Curve:** red `#e74c3c`, 3px; area under it filled `rgba(231,76,60,0.1)`.
- **Option A line:** horizontal green `#27ae60` 2px solid at $1,000,000, 11px green label "Option A: flat $1,000,000" left-aligned above it near the left edge.
- **Crossover marker:** vertical dashed `#1a5276` line (dash 4/3, 1.5px) at day 27 from baseline to the curve, bold 12px `#1a5276` label "day 27: penny takes the lead".
- **End label (bold 12px `#e74c3c`, near day-30 point):** "$10,737,418".
- **Axis labels:** x "Day", ticks 0, 5, 10, 15, 20, 25, 30; y "Total received so far" rotated −90°, ticks $0, $2M, $4M, $6M, $8M, $10M with `#eee` gridlines.

## 3. Why Intuition Fails

**Obj-title:** Your Brain Adds. Doubling Multiplies.

Math box:

**Share of the final $10.7M that has arrived by each day:**

Day 20: `0.01%`
Day 24: 0.16%
Day 27: `12.5%`
Day 28: 25%
Day 29: 50%
Day 30: `100%`

Nearly 90% of all the money arrives in the last three days. A linear mind expects progress to be spread evenly across the month; doubling back-loads it almost entirely onto the finish.

Bullets:

- **The error:** We extrapolate the future as a straight line through the recent past, and for an exponential the recent past is always deceptively flat.
- **Why it persists:** Everyday quantities — wages, distances, groceries — grow by addition, so multiplicative growth has no intuitive reference point.
- **The tell:** If a process has a fixed doubling time, judging it by its current size is exactly the mistake this riddle is built to expose.

### Visualization (canvas `canvas3`, 720×360)

Step-style line chart: cumulative share of the final total, days 0–30, in percent.

- **Title (bold 14px `#1a5276`, top center):** "When the Money Actually Arrives".
- **Plot area:** origin x=80, baseline y=300, width 590, height 240; axes `#1a5276` 2px.
- **Data:** share(n) = (2ⁿ − 1) / (2³⁰ − 1) × 100, n = 0..30.
- **Curve:** orange `#e67e22`, 3px; area under it filled `rgba(230,126,34,0.12)`.
- **Linear-expectation line:** dashed gray `#999` (dash 6/4, 1.5px) diagonal from (0, 0%) to (30, 100%), 11px `#999` label "what a linear mind expects" placed along its middle.
- **Markers (bold 12px `#1a5276`):** dot + label at day 29 "day 29: only 50% has arrived"; dot + label at day 20 "day 20: 0.01%".
- **Axis labels:** x "Day", ticks 0, 5, 10, 15, 20, 25, 30; y "% of final total received" rotated −90°, ticks 0%, 25%, 50%, 75%, 100% with `#eee` gridlines.

## 4. Same Math, Different Costumes

**Obj-title:** Every Field Has Its Own Penny

Math box 1:

**The chessboard legend (the riddle's ancient cousin):**

One grain of rice on square 1, doubled on each of 64 squares.
Square 64 alone: `2⁶³ ≈ 9.2 × 10¹⁸` grains.
The full board: ~1.8 × 10¹⁹ grains — centuries of world rice production. The king who accepted the deal made the same mistake as the person who takes the million.

Math box 2:

**The same curve working against you:**

Credit card debt at ~24% APR doubles in roughly `3 years` (rule of 72: 72 ÷ 24). The flat early years are why balances feel manageable right up until they don't.

An epidemic with R = 2 doubles every generation of infection — 30 generations is the penny's 30 days, which is why "only a few hundred cases" is not reassurance.

Bullets:

- **Compound interest:** Steady returns look boring for decades and then most of the final wealth appears in the last few doublings.
- **Viral growth:** A product or a pathogen spreading multiplicatively is judged tiny at exactly the moment the exponent is already committed.
- **Algorithm cost:** An O(2ⁿ) brute force feels fine at n = 20 (a million steps) and is impossible at n = 60 (10¹⁸ steps) — the input grew by 3×, the cost by a trillion×.

### Visualization (canvas `canvas4`, 720×360)

Canvas-drawn table: the doubling pattern across domains.

- **Title (bold 14px `#1a5276`, top center):** "One Identity, Many Disguises: Each Step Equals Everything Before It".
- **Table layout:** starts at x=60, header row at y=60, row height 46, column x-offsets 0/170/370 (widths 170, 200, 290); header underline `#1a5276` 2px spanning 600px; even rows have `#f8fafb` background stripes (620px wide).
- **Header (bold 12px `#1a5276`):** "Setting", "What doubles", "Where it explodes".
- **Rows (13px `#333`; first column bold):**
  - Penny riddle | daily payout | day 28 alone beats $1M
  - Chessboard rice | grains per square | square 64 ≈ 9.2×10¹⁸ grains
  - Credit card, 24% APR | balance every ~3 yrs | decade 2, not year 1
  - Epidemic, R = 2 | cases per generation | while counts still look small
  - O(2ⁿ) brute force | steps per +1 of n | n = 60 → 10¹⁸ steps
- **Bottom note (12px `#666`, left-aligned at x=60, y≈330):** "Rule of 72: doubling time ≈ 72 ÷ growth rate in % per period."

## Callout (philosophy box, bottom)

**One sentence:** When a process doubles, its current size tells you almost nothing — each step equals everything that came before, so all the action lives in the last few doublings; count the doublings, not the dollars.

## Regeneration instructions

- **Layout:** case-study detail page. h1, `.subtitle`, `.philosophy` callout, then per numbered section: `<h2>` (1.4em `#1a5276`, bottom border `2px solid #2980b9`, padding-bottom 8px) followed by an `.obj-table` (full-width, one `<tr>`; left `<td>` 45% with `.obj-title` + `.math-box` blocks + bullets, right `<td>` 55% centered holding the canvas). Closing `.philosophy` callout at the end. No nav bar, no back/home links.
- **Math boxes:** `.math-box` — background `#f8fafb`, border `1px solid #e0e0e0`, radius 6px, padding 16px 20px, 0.9em; inline `code` on `#eef2f7` background, padding 2px 6px, radius 3px.
- **Callout style:** `.philosophy` — background `#f0f4f8`, left border `4px solid #2980b9`, padding 12px 16px, 0.9em.
- **Page style:** body system sans-serif, white background, text `#2a2a2a`, padding 40px 20px, line-height 1.6; h1 1.8em `#1a5276`; subtitle `#666` 1.05em; table cell borders `1px solid #e0e0e0`, padding 20px 24px; `.obj-title` 1.05em, weight 600, `#1a5276`; `strong` in `#1a5276`; ul 0.9em `#333`, margin `8px 0 8px 20px`.
- **Canvas:** intrinsic 720×360 per chart; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setupCanvas(id, w, h)` helper. Chart fonts are `-apple-system, BlinkMacSystemFont, sans-serif`. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, bar fill `rgba(26,82,118,0.35)`, gridlines `#eee`, gray text `#666`/`#333`/`#999`.
