# Point vs Interval Estimates

**Page type:** detail page (tutorial layout: h1 + subtitle, 4 `.card-section` blocks each with h2 and a two-column `table.layout` — text left 50%, canvas right 50%)
**HTML title tag:** Point vs Interval Estimates

**Subtitle:** "Average delivery is 32 minutes" vs "the average is between 29 and 35 minutes" — the single number hides how sure you are; the range says it out loud

## One Number vs a Range, Same Ten Deliveries

**Tags:** `core idea` (blue), `running example` (green)

- **Two answers** — "average delivery is 32 minutes" vs "the average is 29 to 35 minutes"
- **Point estimate** — the single number: your one best guess computed from the data
- **Interval estimate** — a range that admits the best guess could be off, and by how much
- **Same data** — both come from the same ten timed deliveries; the range just adds honesty
- **What 32 hides** — whether it was computed from 10 orders or from 10,000

*Example:* Two apps both display "32 min" — one timed 10 deliveries, the other 10,000; the point estimates look identical.

**The split:** a point estimate answers "what is your best guess?"; an interval estimate answers "how sure are you about it?"

### Visualization (canvas `c1`, 720×300)

Two-row comparison on a single horizontal minutes axis: a point-estimate dot vs an interval-estimate error bar.

- **Title (bold 15px `#1a5276`, top center):** "Two Ways to Report the Same Ten Deliveries"
- **Axis:** horizontal minutes scale from 20 to 45, tick labels every 5 (12px gray `#6b7280`); gray `#999` axis line at y=235; left pad 60, right pad 40. Axis caption: "delivery time (minutes)".
- **Row 1 (y=105):** blue `#2a78d6` filled dot (9px radius) at 32 min, labeled bold 13px "point estimate: \"32 minutes\""; dotted (3/3) light-grid `#e5e9ef` drop line from the dot to the axis.
- **Row 2 (y=170):** green `#008300` interval bar from 29 to 35 (5px line with 3px end caps ±9px) with a 6px green center dot at 32, labeled bold 13px "interval estimate: \"29 to 35 minutes\""
- **Annotation (bold 13px orange `#d95926`, upper left):** "same data, same center — the range adds \"how sure\""

## From Ten Pizzas to 29–35 Minutes

**Tags:** `worked example` (green), `arithmetic` (blue)

- **The sample** — ten times, in minutes: 25, 28, 29, 30, 31, 33, 34, 35, 36, 39
- **Step 1: mean** — they add to 320, so the average is 320 ÷ 10 = 32 minutes
- **Step 2: spread** — times sit about ±4.2 minutes from 32 (the standard deviation)
- **Step 3: shrink** — averages wobble less than singles: 4.2 ÷ √10 ≈ 1.3 minutes
- **Step 4: widen** — go ~2.26 of those units each way: 32 ± 3, so 29 to 35

*Example:* Check it by hand: the deviations −7, −4, −3, −2, −1, 1, 2, 3, 4, 7 square-sum to 158, and √(158 ÷ 9) ≈ 4.2.

**Two ingredients only:** the interval's width is built from how spread out the data is (4.2) and how much data you have (10) — nothing else.

### Visualization (canvas `c2`, 720×300)

Dot plot of the ten delivery times on the 20–45 minutes axis, with a shaded confidence band and dashed mean line.

- **Title (bold 15px `#1a5276`, top center):** "The Ten Delivery Times, and the Interval They Produce"
- **Data dots:** ten blue `#2a78d6` 7px dots at times `[25, 28, 29, 30, 31, 33, 34, 35, 36, 39]` in a row just above the axis, each labeled with its value (12px `#2c3e50`).
- **CI band:** translucent green `rgba(0,131,0,0.12)` rectangle spanning 29 to 35 for the full plot height.
- **Mean line:** dashed (6/4) violet `#4a3aa7` 2px vertical line at 32, labeled "mean = 32" in bold 12px violet above.
- **Axis:** minutes 20–45, ticks every 5, caption "delivery time (minutes)"; gray `#999` axis line at y=235.
- **Annotations:** bold 13px green centered: "band = 32 ± 2.26 × (4.2 ÷ √10) ≈ 32 ± 3  →  29 to 35"; 12px gray left-aligned: "each dot = one timed delivery".

## When Each Form Is the Right Answer

**Tags:** `where it's used` (blue), `rule of thumb` (green)

- **Use the point** — dashboards, ETAs, a field in a report: one number people can act on
- **Use the interval** — comparisons and promises: "is our 32 really faster than their 34?"
- **Small samples shout** — 10 orders gives ±3 minutes; the width warns the point is shaky
- **More data narrows it** — 4× the deliveries roughly halves the interval's width
- **Overlap check** — two restaurants whose intervals overlap heavily have no clear winner

*Example:* Promising "under 35 minutes or it's free" on a 29–35 interval bets the business on the top edge of your own uncertainty.

**Rule of thumb:** report the point for action, the interval for judgment — a decision made on the point alone silently assumes the interval is narrow.

### Visualization (canvas `c3`, 720×300)

Four stacked interval bars showing the confidence interval narrowing as sample size grows (spread fixed at 4.2 min), on a zoomed 28–36 minutes axis.

- **Title (bold 15px `#1a5276`, top center):** "More Deliveries, Narrower Interval (spread fixed at 4.2 min)"
- **Axis:** zoomed 28–36 minutes, ticks every 2; caption "estimated average delivery time (minutes)"; left pad 175 (row labels live there), axis at y=245. Dotted grid vertical reference line at 32.
- **Rows (interval bars 5px with end caps, center dot at 32, y = 70/115/160/205):**
  1. "n = 10 deliveries" — 29.0 to 35.0, orange `#d95926`, right label "± 3.0 min"
  2. "n = 40 deliveries" — 30.7 to 33.3, yellow `#c98500`, right label "± 1.3 min"
  3. "n = 160 deliveries" — 31.3 to 32.7, aqua `#199e70`, right label "± 0.7 min"
  4. "n = 640 deliveries" — 31.7 to 32.3, green `#008300`, right label "± 0.3 min"
- Row labels bold 13px in row color, right-aligned left of the plot.
- **Annotation (bold 13px green, two lines):** "4x the data →" / "half the width"

## The Confusion: the Average vs Your Order

**Tags:** `common mistake` (red), `caution` (orange)

- **Wrong reading** — "95% of deliveries take 29 to 35 minutes" — no: single orders ran 25 to 39
- **Right reading** — the average is pinned to 29–35; individual orders swing far wider
- **95% of what** — the method: intervals built this way catch the true mean ~19 times in 20
- **Wide ≠ wrong** — a wide interval is honest reporting of thin data, not a failed analysis
- **Narrow ≠ true** — a tight interval from a biased sample is just confidently wrong

*Example:* Even if the true average is exactly 32 minutes, your own pizza can still take 39.

**The trap:** the interval is a statement about the average, not about the next delivery — mixing the two is the most common misread of a confidence interval.

### Visualization (canvas `c4`, 720×300)

Two-row comparison on the 20–45 minutes axis: the spread of individual orders vs the confidence interval for the mean.

- **Title (bold 15px `#1a5276`, top center):** "The Interval Pins the Average — Not Your Pizza"
- **Axis:** minutes 20–45, ticks every 5, caption "delivery time (minutes)"; gray `#999` axis line at y=235.
- **Row 1 (y=100):** violet `#4a3aa7` dashed (4/4) line spanning 25 to 39 with ten violet 6px dots at `[25, 28, 29, 30, 31, 33, 34, 35, 36, 39]`; label bold 13px violet: "single orders: 25 to 39 min".
- **Row 2 (y=170):** green `#008300` interval bar (5px, end caps, 6px center dot at 32) from 29 to 35; label bold 13px green: "the average: 29 to 35 min".
- **Annotation (bold 13px magenta `#d55181`, upper left):** "even with the average pinned near 32, one order took 39"

## Regeneration instructions

- **Template:** tutorial detail page (see `tutorials/CLAUDE.md`). h1 + `.subtitle`, then 4 `.card-section` blocks, each an `<h2>` followed by `table.layout` with one `<tr>`: left `<td class="text-col">` (50%) holding `.tags` pills, a `<ul>` of one-line bullets with `<b>` lead terms, an italic `.example` line, and a `.key-point` callout; right `<td class="viz-col">` (50%) holding one canvas.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6. h1 2rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem. h2 1.3rem `#1a5276` with 2px `#2980b9` bottom border. Bullets 0.92rem, `li b` colored `#1a5276`. `.example` italic `#555` 0.9rem. `.key-point`: background `#f8f9fa`, left border 3px solid `#e74c3c`, padding 8px 12px, 0.9rem.
- **Tag pills:** `.tag` inline-block, 0.72rem bold, padding 2px 10px, radius 10px; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Canvases:** 720×300 intrinsic, CSS `width:100%`, 1px `#e0e0e0` border, radius 4px; scaled by `window.devicePixelRatio` via a shared `setup(id)` helper (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates). A shared `makeX(pad, cw)` helper maps the 20–45 minute scale for c1, c2, c4. Chart palette object `P`: blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Doc palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- No nav bar, no back/home links. In regenerated HTML, any card links use `.html` extensions.
