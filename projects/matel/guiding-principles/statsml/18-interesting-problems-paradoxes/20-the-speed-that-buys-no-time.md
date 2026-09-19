# The Speed That Buys No Time

**Page type:** detail page (h2-sectioned two-column obj-table layout: text left 50%, canvas right 50%; philosophy callouts at top and bottom)
**HTML title tag:** The Speed That Buys No Time — Case Study

**Subtitle:** Going from 10 mph to 20 mph on a 20-mile trip saves a full hour. Going from 65 mph to 75 mph on the same trip saves two and a half minutes. Same extra 10 mph, 24 times less benefit — and most freeway speeding is the second case.

**Note on the data:** every number on this page is exact arithmetic from a stated distance and speed. No generated or random data anywhere, so no seeded PRNG is needed; all chart labels are computed at render time from the plotted values.

## Callout (philosophy box, top)

**The paradox in one line:** The same +10 mph does not save the same time. It saves a lot when you are slow and almost nothing when you are already fast.

**Your gut says:** "10 mph is 10 mph. If speeding up by 10 saved me an hour last time, it should save me something close to that this time."

**The arithmetic says:** On a 20-mile trip, `10 → 20 mph` saves `60 minutes`. `65 → 75 mph` saves `2.5 minutes`. The increment is identical. The payoff differs by a factor of 24.

## 1. The Two Identical Speed-Ups

**Obj-title:** Same +10 mph, Wildly Different Reward

Math box 1:

**A 20-mile trip, timed both ways:**

At 10 mph: 20 ÷ 10 = 2 hours = `120 min`
At 20 mph: 20 ÷ 20 = 1 hour = `60 min`
Saved: **60 minutes** — the trip is cut in half.

At 65 mph: 20 ÷ 65 = 0.3077 hours = `18.46 min`
At 75 mph: 20 ÷ 75 = 0.2667 hours = `16.00 min`
Saved: **2.46 minutes**.

Math box 2:

**Why "half the time" is not available up here:**

Doubling speed always halves the time. 10 → 20 mph is a doubling, so the hour goes.
65 → 75 mph is not a doubling — it is a `15%` bump, so it removes `13%` of the time.
To actually halve an 18.46-minute trip you would have to drive `130 mph`.

Bullets:

- **What people remember:** The dramatic case — crawling in traffic, then clearing it, and the trip collapses.
- **What people then assume:** That the same +10 mph keeps paying out at freeway speed, which it does not.
- **The real reward at 65:** Two and a half minutes on a 20-mile run, which is roughly one traffic light.
- **The erasure:** A single 90-second red light eats 1.5 of those 2.46 minutes on its own.

### Visualization (canvas `canvas1`, 720×360)

Line chart: trip time vs speed for a fixed 20-mile trip, with the two +10 mph steps marked.

- **Title (bold 14px `#1a5276`, top center):** "Trip Time vs Speed — 20 Miles. The Curve Flattens Fast."
- **Plot area:** origin x=80, baseline y=300, width 590, height 240; axes `#1a5276` 2px.
- **Data:** `T(v) = 1200 / v` minutes, v from 10 to 85 mph in 0.5 steps; y scale 0–120 min, x scale 10–85 mph.
- **Curve:** blue `#1a5276`, 3px.
- **Step A marker (red `#e74c3c`):** vertical dashed drop at v=10 and v=20 down to the axis, plus a red bracket/arrow spanning T(10)=120 to T(20)=60 with bold 12px `#e74c3c` label "10 → 20 mph: saves 60 min" — label text built at render time from the computed times.
- **Step B marker (green `#27ae60`):** same treatment at v=65 and v=75, spanning T(65) to T(75), bold 12px `#27ae60` label "65 → 75 mph: saves 2.5 min", drawn with a short leader line since the gap is only a few pixels tall.
- **Dots:** filled 4px circles at (10, 120), (20, 60), (65, 18.46), (75, 16.00).
- **Axis labels:** x "Speed (mph)" (13px `#1a5276`, centered below), ticks 10, 20, 30, 40, 50, 60, 70, 80; y "Trip time (minutes)" rotated −90°, ticks 0, 20, 40, 60, 80, 100, 120 (11px `#666`) with `#eee` gridlines.
- **Note (11px `#666`, inside plot area, right side):** "Same 10 mph step. The height of the drop is what changes."

## 2. Where the Difference Comes From

**Obj-title:** Speed Is the Upside-Down Version of What You Pay

Math box 1:

**Time saved is a ratio, not a difference:**

Fraction of time removed = `1 − v_old / v_new`

10 → 20 mph: 1 − 10/20 = `50%` of the time gone
65 → 75 mph: 1 − 65/75 = `13.3%` of the time gone

The +10 is the same. The ratio 10/20 versus 65/75 is not.

Math box 2:

**Minutes per mile is the honest unit:**

Time = pace × distance, where pace = minutes per mile.

At 10 mph, pace = `6.00 min/mile`
At 20 mph, pace = `3.00 min/mile` → saves 3.00 × 20 miles = 60 min
At 65 mph, pace = `0.923 min/mile`
At 75 mph, pace = `0.800 min/mile` → saves 0.123 × 20 miles = 2.46 min

Bullets:

- **The flip:** Speed is miles per hour; the thing you spend is hours per mile — the reciprocal.
- **Straight line vs curve:** Time is linear in pace and a hyperbola in speed, so equal speed steps are unequal.
- **The sensitivity:** Each extra mph saves `1200/v²` minutes — 12 min/mph at 10 mph, 0.28 at 65.
- **The ratio of those two:** `(65/10)² = 42×` less return per mph, purely from the squared term.
- **General rule:** Whenever the dial you turn is a rate, the cost you pay moves as one over that dial.

### Visualization (canvas `canvas2`, 720×360)

Bar chart: minutes saved by each successive +10 mph step on the same 20-mile trip.

- **Title (bold 14px `#1a5276`, top center):** "Every Step Is +10 mph. The Payoff Collapses."
- **Plot area:** origin x=80, baseline y=300, width 590, height 240; axes `#1a5276` 2px.
- **Data (computed at render time as 1200/v_old − 1200/v_new):** 10→20 = 60.00, 20→30 = 20.00, 30→40 = 10.00, 40→50 = 6.00, 50→60 = 4.00, 60→70 = 2.86, 70→80 = 2.14 minutes. y scale 0–64 min.
- **Bars:** 7 bars, fill `rgba(26,82,118,0.35)`; the first bar filled `#e74c3c`, the last two filled `#27ae60`.
- **Value labels (bold 11px, above each bar, computed):** "60.0", "20.0", "10.0", "6.0", "4.0", "2.9", "2.1" — matching each bar's colour.
- **Axis labels:** x "Speed step (mph)" with tick labels "10→20", "20→30", "30→40", "40→50", "50→60", "60→70", "70→80" (10px `#666`); y "Minutes saved" rotated −90°, ticks 0, 10, 20, 30, 40, 50, 60 with `#eee` gridlines.
- **Bottom note (11px `#666`, left-aligned at x=80, y≈332):** "All seven steps together save 105 min. The first step alone is 60 of them."

## 3. Where the Time Actually Went

**Obj-title:** The Slow Miles You Cannot Speed Up Own the Clock

Math box 1:

**A 22-mile commute: 20 freeway miles plus 2 city miles.**

Freeway 20 miles at 70 mph = `17.14 min`
City 2 miles at 15 mph = `8.00 min`
Total = `25.14 min`

Those 2 city miles are `9%` of the distance and `32%` of the time.

Math box 2:

**So speeding on the freeway hardly matters:**

Push the freeway leg 70 → 80 mph: saves `2.14 min`.
Remove the freeway leg entirely and you still spend `8.00 min`.
The best any amount of freeway speed can do is a `3.1×` faster trip.

**And the average speed is not 70 or even 60:**
22 miles ÷ 25.14 min = `52.5 mph` — the distance-weighted harmonic mean, not the plain average of 70 and 15.

Bullets:

- **The floor:** Any leg you cannot speed up sets a hard minimum the rest of the trip cannot go below.
- **Averaging trap:** Average speed over a route is a harmonic mean — 30 mph then 60 mph averages 40, not 45.
- **Why it is always lower:** Slow miles occupy more clock time, so they get more weight in the average.
- **Where to look first:** The leg with the worst pace, not the leg where more speed is easiest to get.
- **Same rule in systems:** Optimising a fast stage while a slow stage stays fixed moves the total barely at all.

### Visualization (canvas `canvas3`, 720×360)

Stacked horizontal bar chart: trip composition under three freeway speeds, city leg unchanged.

- **Title (bold 14px `#1a5276`, top center):** "The City Miles Never Move — 20 Freeway Miles + 2 City Miles at 15 mph".
- **Plot area:** bars start at x=200, bar height 44, gap 26, first bar top y=80; x scale 0–28 minutes across 440px; light `#eee` vertical gridlines at 0, 5, 10, 15, 20, 25 min with 11px `#666` labels along the bottom axis (y≈288).
- **Rows (labels right-aligned at x=190, 12px `#333`):** "Freeway at 70 mph", "Freeway at 80 mph", "Freeway leg removed".
- **Segments (computed at render time as 20/v × 60 and 2/15 × 60):** freeway segment fill `rgba(26,82,118,0.35)` with values 17.14, 15.00, 0.00 min; city segment fill `#e74c3c` with value 8.00 min in every row.
- **In-bar labels (bold 11px):** freeway minutes in `#1a5276` centred in its segment (omitted when the segment is zero-width), city minutes in white centred in the red segment.
- **Row totals (bold 12px `#1a5276`, just right of each bar):** "25.1 min", "23.0 min", "8.0 min".
- **Legend (11px, top right of plot area):** swatch `rgba(26,82,118,0.35)` "freeway, 20 miles"; swatch `#e74c3c` "city, 2 miles".
- **Bottom note (11px `#666`, left-aligned at x=200, y≈330):** "+10 mph on the freeway buys 2.1 min. The unchangeable 8 min is what the trip actually costs."

## 4. The Same Trap in Other Costumes

**Obj-title:** Any Rate Metric Hides a Reciprocal

Math box 1:

**Fuel economy does exactly this (per 100 miles):**

10 → 20 mpg: 10.00 gal → 5.00 gal, saves `5.00 gal`
30 → 40 mpg: 3.33 gal → 2.50 gal, saves `0.83 gal`

Same +10 mpg, `6×` less fuel saved. Replacing the worst vehicle in a fleet beats upgrading the best one.

Math box 2:

**And the speed you gain is not free:**

Crash energy rises with the square of speed: (75 ÷ 65)² = `1.33`.
Trading 2.46 minutes for `33%` more energy to absorb in a collision is the actual bargain on offer.
Braking distance carries the same square term, so the margin for error shrinks at the same time.

Bullets:

- **Throughput vs latency:** Requests per second is a rate; milliseconds per request is what the user waits.
- **Amdahl's law:** A fixed serial fraction caps total speedup exactly the way the city miles cap the trip.
- **Tail latency:** Halving a 200 ms step is small change if one 2-second step is still in the path.
- **Cost per unit:** Items per dollar flatters cheap gains; dollars per item shows what the change is worth.
- **The habit:** Convert every rate to its per-unit cost before deciding which improvement to fund.

### Visualization (canvas `canvas4`, 720×360)

Canvas-drawn table: the reciprocal trap across settings.

- **Title (bold 14px `#1a5276`, top center):** "One Pattern: The Dial You Turn Is One Over the Cost You Pay".
- **Table layout:** starts at x=50, header row at y=60, row height 46, column x-offsets 0/150/330 (widths 150, 180, 290); header underline `#1a5276` 2px spanning 620px; even rows have `#f8fafb` background stripes (630px wide).
- **Header (bold 12px `#1a5276`):** "Setting", "Rate you watch", "What actually costs you".
- **Rows (13px `#333`; first column bold):**
  - Freeway driving | miles per hour | minutes per mile — 65→75 saves 2.46 min / 20 mi
  - Fuel economy | miles per gallon | gallons per mile — 30→40 mpg saves 0.83 gal / 100 mi
  - Web service | requests per second | ms per request — the slowest stage sets the wait
  - Pipeline speedup | ×faster on one stage | fixed serial time — Amdahl's ceiling
  - Unit economics | items per dollar | dollars per item — where margin is really lost
- **Bottom note (12px `#666`, left-aligned at x=50, y≈330):** "Rule: if the number is 'X per Y', flip it to 'Y per X' before comparing improvements."

## Callout (philosophy box, bottom)

**One sentence:** Speed is the reciprocal of the minutes you actually spend, so equal speed increases buy wildly unequal time — big when you are slow, near-nothing when you are already fast, and never more than the slowest leg of the trip allows.

## Regeneration instructions

- **Layout:** case-study detail page. h1, `.subtitle`, `.philosophy` callout, then per numbered section: `<h2>` (1.4em `#1a5276`, bottom border `2px solid #2980b9`, padding-bottom 8px) followed by an `.obj-table` (full-width, one `<tr>`; left `<td>` 50% with `.obj-title` + `.math-box` blocks + bullets, right `<td>` 50% centered holding the canvas). Closing `.philosophy` callout at the end. No nav bar, no back/home links.
- **Math boxes:** `.math-box` — background `#f8fafb`, border `1px solid #e0e0e0`, radius 6px, padding 16px 20px, 0.9em; inline `code` on `#eef2f7` background, padding 2px 6px, radius 3px.
- **Callout style:** `.philosophy` — background `#f0f4f8`, left border `4px solid #2980b9`, padding 12px 16px, 0.9em.
- **Page style:** body system sans-serif, white background, text `#2a2a2a`, padding 40px 20px, line-height 1.6; h1 1.8em `#1a5276`; subtitle `#666` 1.05em; table cell borders `1px solid #e0e0e0`, padding 20px 24px; `.obj-title` 1.05em, weight 600, `#1a5276`; `strong` in `#1a5276`; ul 0.9em `#333`, margin `8px 0 8px 20px`.
- **Canvas:** intrinsic 720×360 per chart; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setupCanvas(id, w, h)` helper. Chart fonts are `-apple-system, BlinkMacSystemFont, sans-serif`. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Data rule:** all chart values derive from `T = distance / speed` evaluated in JS from the distances and speeds named above. Every printed statistic (minutes saved, totals, averages) is computed from the plotted numbers at render time — nothing is hardcoded as a string.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, bar fill `rgba(26,82,118,0.35)`, gridlines `#eee`, gray text `#666`/`#333`/`#999`.
