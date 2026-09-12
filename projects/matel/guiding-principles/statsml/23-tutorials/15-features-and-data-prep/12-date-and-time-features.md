# Date & Time Features

**Page type:** detail page (tutorial page: h1 + subtitle, 4 `.card-section` blocks each with an h2 and a `table.layout` — sections 1, 2, 4 use text left 50% / canvas right 50%; section 3 uses a 3-column row: text 38% + two canvases 31% each)
**HTML title tag:** Date &amp; Time Features

**Subtitle:** One timestamp is a bundle of hidden signals — hour, weekday, month, weekend, tenure — that a model can only use once you unpack them

## One Timestamp Is Hiding Five Features

**Tags:** `core idea` (blue), `running example` (green)

- **The task** — a food delivery app wants to predict how busy the next hour will be
- **The raw data** — every order has one timestamp: `2024-07-18 08:37`
- **The problem** — to a model that's one opaque value, not "a Thursday breakfast order"
- **The move** — split it: hour, weekday, month, is-weekend, days-since-signup
- **Why it works** — demand repeats by hour and weekday, and the pieces expose the repetition

*Example:* Lunch rush, Friday spike, summer slump — all live inside that one column.

**Key point:** A timestamp compresses several rhythms into one number. Unpacking it into separate columns is the single highest-value move in time-based features.

### Visualization (canvas `c1`, 720×300)

Explosion diagram: one timestamp box fanning out via curved connectors into five feature boxes.

- **Title (bold 15px, `#1a5276`, top center):** "One Column In, Five Columns Out".
- **Timestamp box (x=50, y=120, 205×56):** `#f8f9fa` fill, 2px `#1a5276` border; bold 15px monospace `#1a5276` text "2024-07-18 08:37" with 12px muted sub-label "one opaque timestamp".
- **Five feature boxes (x=420, width 260, height 36, stacked from y=55 with 10px gaps):** `#f8f9fa` fill, 2px colored borders, bold 13px monospace labels in the border color:
  - "hour = 8" — blue `#2a78d6`
  - "weekday = Thu" — green `#008300`
  - "month = 7" — violet `#4a3aa7`
  - "is_weekend = 0" — yellow `#c98500`
  - "days_since_signup = 124" — magenta `#d55181`
- **Connectors:** light gray-blue (`#b9c2cc`, width 1.5) bezier curves from the timestamp box's right edge to each feature box, each ending in a small filled arrowhead.
- **Caption (bold 13px orange `#d95926`, lower left area):** "each piece answers one question the raw string cannot".

## Exploding 2024-07-18 08:37 by Hand

**Tags:** `worked example` (green)

- **Hour** — read it off: 8 (a breakfast-time order)
- **Weekday** — July 18, 2024 was a Thursday
- **Month** — 7; **is-weekend** — Thursday, so 0
- **Days since signup** — customer joined Mar 16: 15 + 30 + 31 + 30 + 18 = 124 days
- **Check the sum** — rest of Mar 15, Apr 30, May 31, Jun 30, then 18 days of Jul

*Example:* One string column becomes five numeric columns, each answering a different question.

**Key point:** Days-since-signup is a subtraction between two dates — a model can't do that subtraction itself from raw date strings, so you do it once and hand over the result.

### Visualization (canvas `c2`, 720×300)

Segmented horizontal calendar strip summing the days from signup to order.

- **Title (bold 15px, `#1a5276`, top center):** "Days Since Signup: Mar 16 → Jul 18 = 124".
- **Strip (from x=55 to x=665, y=110, height 46):** five segments with widths proportional to days/124, filled at 0.6 alpha with white 2px separators; white bold 14px day counts centered inside; bold 12px colored segment names alternating above/below the strip:
  - "rest of Mar" 15 days — blue `#2a78d6`
  - "April" 30 — green `#008300`
  - "May" 31 — violet `#4a3aa7`
  - "June" 30 — yellow `#c98500`
  - "Jul 1–18" 18 — magenta `#d55181`
- **Endpoints:** short `#1a5276` vertical ticks below both ends with bold 13px two-line labels: "signup" / "Mar 16" on the left and "order" / "Jul 18" on the right.
- **Caption (bold 14px orange `#d95926`, bottom center):** "15 + 30 + 31 + 30 + 18 = 124 — a number you can hand the model".

## The Rhythms the Raw Timestamp Hides

**Tags:** `why it matters` (blue), `where it's used` (blue)

- **Hourly rhythm** — orders peak at 12 (90/hr) and 19 (110/hr), bottom out at 4 (2/hr)
- **Weekly rhythm** — Fri–Sun average ~1,200 orders a day vs ~840 on Mon–Thu
- **Sorted timestamps can't show this** — the pattern repeats, it doesn't trend
- **With the pieces** — the model learns "hour 19 is busy" from every past day at once
- **Same trick elsewhere** — fraud by hour, hospital load by weekday, retail by month

*Example:* Hour 19 on any day predicts the dinner rush better than the full timestamp of last Tuesday.

**Key point:** Patterns that repeat every day or week only become learnable when the repeating part — hour, weekday — is its own column.

### Visualization (canvas `c3a`, 420×340)

Bar chart of orders per hour of day with the two rushes highlighted.

- **Title (bold 15px, `#1a5276`, top center):** "Orders per Hour (illustrative)".
- **Data (24 hourly bars, hours 0–23):** [12, 8, 5, 3, 2, 2, 4, 10, 22, 30, 38, 55, 90, 75, 45, 35, 40, 60, 95, 110, 80, 50, 30, 18]; y scale max 120; padding left 45, right 15, top 45, bottom 55; thin `#999` baseline.
- **Bar colors:** hours 12 and 19 orange `#d95926` at 0.85 alpha; all others blue `#2a78d6` at 0.45 alpha.
- **X ticks:** hour labels every 4 hours (0, 4, 8, 12, 16, 20) in 12px `#444`; axis label "hour of day" below.
- **Annotations (bold 12px orange):** "lunch 12h: 90" above the hour-12 bar; "dinner 19h: 110" near the hour-19 bar.
- **Caption (bold 13px blue `#2a78d6`, bottom center):** "two rushes, every single day".

### Visualization (canvas `c3b`, 400×340)

Bar chart of orders per weekday with the weekend highlighted.

- **Title (bold 15px, `#1a5276`, top center):** "Orders per Day (illustrative)".
- **Data:** Mon 850, Tue 820, Wed 830, Thu 860, Fri 1180, Sat 1250, Sun 1180; y scale max 1400; padding left 45, right 15, top 45, bottom 55; thin `#999` baseline.
- **Bar colors (0.6 alpha):** Fri–Sun green `#008300`; Mon–Thu blue `#2a78d6`. Bold 12px value labels above each bar in the bar color; 12px `#444` weekday labels below.
- **Caption (bold 13px green, bottom center):** "Fri–Sun run ~40% above Mon–Thu".

## Hour 23 Sits Next to Hour 0

**Tags:** `common mistake` (red), `worked example` (green)

- **The confusion** — as plain numbers, hour 23 and hour 0 look 23 apart
- **Reality** — 11 PM and midnight are 1 hour apart; the clock wraps around
- **The fix** — place each hour on a circle: sin(2π·h/24) and cos(2π·h/24)
- **Check it** — hour 23 → (−0.26, 0.97), hour 0 → (0, 1): nearly the same point
- **Same wrap** — weekday (Sun→Mon), month (Dec→Jan) get the same treatment

*Example:* A late-night snack model that treats 23 and 0 as opposites splits its own best signal in half.

**Common mistake:** Feeding raw hour as a number tells the model midnight is maximally far from 11 PM. Two columns — sin and cos — put the clock back into a circle.

### Visualization (canvas `c4`, 720×300)

Split panel: hour as a number line (left) vs hour on a clock circle (right), divided by a dashed vertical line at x=345.

- **Title (bold 15px, `#1a5276`, top center):** "Hour as a Number vs Hour on a Circle".
- **Left number line (x=45 to x=315 at y=140):** `#999` line with 24 small `#bbb` tick marks; endpoints 0 and 23 marked with red `#e74c3c` 7px dots and bold 13px labels "0" and "23" above; a red bracket below spanning the full line with bold 12px red caption "as numbers: 23 apart"; two muted 12px lines below: "raw hour column: midnight and 11 PM" / "land at opposite ends".
- **Right clock circle (center 500,155, radius 82):** `#999` circle with 24 hour dots placed clockwise from the top (hour 0 at 12 o'clock); hours 23 and 0 as red 6px dots, others blue `#2a78d6` 3px dots; hour labels at multiples of 6 plus 23 (bold 12px, red for hot hours, `#444` otherwise) placed 16px outside the circle. Bold 12px red annotation near the top of the circle: "23 and 0: neighbors again".
- **Sin/cos table (bold 12px monospace green `#008300`, lower right):** "h=23 → (sin −0.26, cos 0.97)" and "h=0  → (sin  0.00, cos 1.00)".
- **Caption (bold 13px orange `#d95926`, bottom center):** "two columns, sin and cos, give every hour its true neighbors".

## Regeneration instructions

- **Layout:** tutorial detail page. `<h1>` + `.subtitle` paragraph, then four `.card-section` divs, each an `<h2>` (bottom border `2px solid #2980b9`) followed by a `table.layout`. Sections 1, 2, 4 use `td.text-col` (50%) + `td.viz-col` (50%) with one 720×300 canvas; section 3 uses `td.text-col3` (38%) + two `td.viz-col3` (31% each) holding canvases c3a (420×340) and c3b (400×340). Left cells hold `.tags` pills, a `<ul>` of bold-term bullets (inline `<code>` for the timestamp literal), an italic `.example` line, and a `.key-point` callout (lead-ins "Key point:" or "Common mistake:").
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6. h1 2rem `#1a5276` with `2px solid #2980b9` bottom border; `.subtitle` `#666` 0.95rem; h2 1.3rem `#1a5276`. `ul` 0.92rem; `li b` in `#1a5276`; `li code` ui-monospace on `#f4f6f8`. `.example` italic `#555` 0.9rem. `.key-point` background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem. Canvases `width:100%`, border `1px solid #e0e0e0`, radius 4px. No nav bar, no back/home links.
- **Tag pills:** `.tag` inline-block, 0.72rem bold, padding 2px 10px, radius 10px; `.tag.blue` background rgba(26,82,118,0.12) color `#1a5276`; `.tag.green` rgba(39,174,96,0.15) `#27ae60`; `.tag.red` rgba(231,76,60,0.12) `#e74c3c`; `.tag.orange` rgba(230,126,34,0.15) `#e67e22`.
- **Chart palette (JS object `P`):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- **Canvas:** the shared `setup(id)` helper reads each canvas's `width`/`height` attributes, sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`), and calls `ctx.scale` so drawing stays in logical coordinates.
- In regenerated HTML, any card links use `.html` extensions (this page has no outgoing links).
