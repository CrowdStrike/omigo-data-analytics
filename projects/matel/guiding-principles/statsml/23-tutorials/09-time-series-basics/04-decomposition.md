# Decomposition

**Page type:** detail page (tutorial card-sections: h2 per section, two-column layout table — text left 50% / canvas right 50%)
**HTML title tag:** Decomposition

**Subtitle:** Splitting one messy sales line into trend + season + leftover — and why anomalies only show up in the leftover

## One Line, Four Ingredients

**Tags:** `core idea` (blue), `running example` (green)

- **The raw line** — two years of daily coffee sales: a mess, until you name its parts
- **Trend** — the slow climb: $200 a day at opening, $309 a day two years in
- **Season** — the calendar's share: weekends up to +$38, weekdays dip, December +$55
- **Noise** — the leftover wobble within about ±$16, belonging to no pattern
- **Decomposition** — un-baking the cake: recovering each ingredient from the finished line

*Example:* Every day's till = trend + season + leftover. Decomposition is just filling in the three blanks for every day.

**Key point:** The raw line answers "how much did we sell?" — the three parts answer "why?". Most questions are about a part, not the total.

### Visualization (canvas `c1`, 720×300)

Line chart: 730 days of raw sales with the trend line overlaid and each ingredient called out.

- **Title (bold 15px, `#1a5276`, top center):** "Two years of daily sales — four ingredients in one line"
- **Data (deterministic model, days d = 0..729, day 0 = Monday opening day):** sales(d) = trend(d) + season(d) + noise(d), where trend(d) = 200 + 0.15·d (+$55/year); season(d) = weekly pattern `WK = [-18, -20, -16, -12, -2, 30, 38]` (Mon..Sun, sums to 0) + holiday bump of +55 when day-of-year is 328–358 (December block); noise(d) = 10·sin(d·7.9) + 6·sin(d·3.3) (within ±16)
- **Axes:** y from 140 to 440 with gridlines/labels at $150, $250, $350; L-shaped axis frame `#999`; padding l:58 r:20 t:46 b:44; x labels "Jan y1" (d=0), "Jul y1" (181), "Jan y2" (365), "Jul y2" (546), "Dec y2" (729)
- **Raw series:** line `rgba(42,120,214,0.55)` width 1 across all 730 days
- **Trend hint:** dashed orange `#d95926` line (dash 8/5, width 2.5) from (d=0, $200) to (d=729, $309.4)
- **Ingredient callouts (bold 12px):** orange `#d95926` "trend: the slow climb (+$55/yr)"; yellow `#c98500` right-aligned "holiday season: December blocks"; green `#008300` "weekly season: the 7-day zigzag"; violet `#4a3aa7` "noise: ±$16 fuzz on everything"
- **Caption (12px `#6b7280`, bottom right):** "illustrative data"

## Rebuild One Saturday by Hand

**Tags:** `worked example` (green), `rule of thumb` (blue)

- **Pick a day** — a Saturday in mid-January of year 2; the till shows $289
- **Trend share** — the drift line passes through $257 that day
- **Season share** — Saturdays run +$30; mid-January adds no holiday: +$0
- **Leftover** — 289 − 257 − 30 = +$2: a whisper of noise, nothing more
- **Every day splits this way** — same three blanks, different numbers

*Example:* $289 = $257 (trend) + $30 (Saturday) + $0 (holiday) + $2 (leftover).

**Key point:** A small leftover means the patterns explain the day. A huge leftover means something happened that no pattern predicted — go look.

### Visualization (canvas `c2`, 720×300)

Waterfall chart: rebuilding the $289 Saturday from its parts, five bars with dashed connectors.

- **Title (bold 15px, `#1a5276`, top center):** "One Saturday, rebuilt: $257 + $30 + $0 + $2 = $289"
- **Axes:** y from 0 to 320 with gridlines/labels at $0, $150, $300; L-shaped axis frame `#999`; padding l:58 r:20 t:50 b:44
- **Bars (5 slots, width 52% of slot; bold 12px value label above, gray 12px x label below):**
 1. "trend" — 0 to 257, `rgba(42,120,214,0.5)`, label "$257"
 2. "Saturday" — 257 to 287, `rgba(0,131,0,0.55)`, label "+$30"
 3. "holiday" — flat at 287 (minimum 1.5px sliver), `rgba(201,133,0,0.8)`, label "+$0"
 4. "leftover" — 287 to 289, `rgba(74,58,167,0.7)`, label "+$2"
 5. "the till" — 0 to 289, `rgba(25,158,112,0.55)`, label "$289"
- **Connectors:** dashed gray (dash 3/3) horizontal lines linking the top of each step to the next bar at heights 257, 287, 287, 289
- **Annotation (bold 12px violet `#4a3aa7`, centered at y=$80 under bar 3):** "the leftover is a $2 whisper — the patterns explain this day"
- **Caption (12px `#6b7280`, bottom right):** "illustrative data"

## The Four Panels

**Tags:** `core idea` (blue), `where it's used` (orange)

- **Panel 1: observed** — the messy line the till actually produced
- **Panel 2: trend** — the smooth climb, $200 to $309 over two years
- **Panel 3: seasonal** — the repeating part: weekly zigzag plus the December block
- **Panel 4: residual** — what no pattern explains; healthy = a flat band around $0
- **Check the math** — panels 2 + 3 + 4 add back, day by day, to exactly panel 1

*Example:* This four-panel picture is what every stats library prints when you call its decompose function on a series.

**Key point:** The residual panel is the health report. A flat band means the model explains the data; spikes or drifts mean it doesn't — investigate there.

This section's viz cell holds both canvases side by side in a `.viz-pair` flex row.

### Visualization (canvas `c3a`, 310×340)

Stacked mini-panels 1–2 of the classic decomposition plot (full 730 days).

- **Panel 1 (top, at y 30–150):** title bold 14px `#1a5276` "1. observed = what the till rang" (drawn at x=6); boxed frame `#999`; y range $140 to $440 (12px gray min/max labels left of frame); the full sales series in `rgba(42,120,214,0.7)` width 1
- **Panel 2 (at y 185–305):** title "2. trend = the slow climb"; same frame and y range $140–$440; trend line in orange `#d95926` width 2.5
- **Shared x labels (12px gray, at bottom y=322):** "Jan y1" (left-aligned at the frame's left edge), "Jan y2" (centered on d=365), "Dec y2" (right-aligned at the frame's right edge); left padding 46, right 10
- **Caption (12px `#6b7280`, right-aligned in the gap between the panels, y=168):** "illustrative data"

### Visualization (canvas `c3b`, 310×340)

Stacked mini-panels 3–4 of the decomposition plot (full 730 days).

- **Panel 3 (top, at y 30–150):** title bold 14px `#1a5276` "3. seasonal = week + Dec" (drawn at x=6); boxed frame `#999`; y range −$30 to +$100 (labels "+$100" and "−$30"); zero gridline `#e5e9ef`; season series in green `#008300` width 1; bold 12px yellow `#c98500` "Dec" labels on both December blocks (d≈343 centered; the second right-aligned at the frame's right edge so it stays inside the 310px frame)
- **Panel 4 (at y 185–305):** title "4. residual = noise, ±$16"; y range −$25 to +$25; zero gridline; noise series in violet `#4a3aa7` width 1
- **Shared x labels (12px gray, y=322):** "Jan y1" left-aligned, "Jan y2" centered, "Dec y2" right-aligned; left padding 46, right 10
- **Caption (12px `#6b7280`, right-aligned in the gap between the panels, y=168):** "illustrative data"

## Anomalies Only Show Up After Removing the Boring Parts

**Tags:** `common mistake` (red), `illustrative scenario` (orange)

- **The event** — one Saturday the espresso machine died at noon; the till showed $240
- **On the raw line** — $240 looks like a routine Wednesday; nobody's eye stops on it
- **Expected that day** — trend $262 + Saturday $30 = $292
- **In the residual** — 240 − 292 = −$52, three times the usual ±$16 band: unmissable
- **The lesson** — anomalies hide inside normal-looking numbers until trend and season are stripped away

*Example:* A $240 day is fine on a Tuesday and a disaster on a Saturday — only the residual knows the difference.

**Key point:** Watch the residual, not the raw line. "Is this number weird?" can only be answered after subtracting what the calendar already promised.

### Visualization (canvas `c4`, 720×300)

Two stacked panels: five weeks of raw sales (anomaly invisible) vs the residual (anomaly obvious).

- **Title (bold 15px, `#1a5276`, top center):** "Five weeks around the broken-espresso-machine Saturday"
- **Data:** 35 days starting at d0=393; day 411 (the broken Saturday) forced to $240 in place of the model value; residual(d) = actual − trend − season, which is −$52 on the bad day and inside ±16 elsewhere
- **Top panel ("observed sales", boxed at y 44–144):** y range $215 to $320 (11px gray labels "$320"/"$215"); raw line `rgba(42,120,214,0.7)` width 1.5; a dashed gray circle (radius 9) around the $240 point; bold 12px gray annotation to its right: "raw line: $240 here — looks like any weekday"
- **Bottom panel ("residual (observed − trend − season)", boxed at y 178–270):** y range −$60 to +$25 with labels at +$16, −$16, −$52; shaded ±16 band `rgba(42,120,214,0.10)`; residual line `rgba(74,58,167,0.7)` width 1.5; red `#e74c3c` filled dot (radius 5) on the −$52 point; bold 13px red annotation: "residual: −$52 — three band-widths down"
- **Panel titles (bold 12px `#1a5276`, inside top-left of each box);** shared x labels "week 1".."week 5" (11px gray) below the bottom panel

## Regeneration instructions

- **Layout:** tutorial detail page — `<h1>` + `.subtitle`, then four `.card-section` blocks, each an `<h2>` (1.3rem `#1a5276`, 2px solid `#2980b9` bottom border) followed by `table.layout` (full width, one row). Every section: left `td.text-col` 50%, right `td.viz-col` 50%. Sections 1, 2, 4 hold one canvas; section 3 ("The Four Panels") places canvases `c3a`/`c3b` (310×340 each) side by side inside its single viz cell, wrapped in a `.viz-pair` flex row (`display:flex; gap:10px`, each canvas `flex:1 1 0; min-width:0`). Text cells hold a `.tags` pill row, a `<ul>` of one-line bullets each opening with a `<b>` term (bold terms colored `#1a5276`), an italic `.example` paragraph, and a `.key-point` callout.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem; bullets 0.92rem; `.example` italic `#555` 0.9rem; `.key-point` background `#f8f9fa`, left border 3px solid `#e74c3c`, padding 8px 12px, 0.9rem; canvases `width:100%` with 1px `#e0e0e0` border, 4px radius.
- **Tag pills:** inline-block, 0.72rem bold, padding 2px 10px, radius 10px — blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Charts:** shared JS palette `P = { blue:#2a78d6, green:#008300, magenta:#d55181, yellow:#c98500, aqua:#199e70, orange:#d95926, violet:#4a3aa7, ink:#1a5276, text:#2c3e50, mute:#6b7280, grid:#e5e9ef }`; site palette #1a5276 primary blue, #27ae60 green, #e74c3c red, #e67e22 orange. Canvases scale by `window.devicePixelRatio` via a shared `setup(id)` helper (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates). All series come from the deterministic model above (trendOf/seasonOf/holidayOf/noiseOf/salesOf) — no `Math.random()`; same running coffee-shop example as the other time-series pages.
- No nav bar, no back/home links, no cross-page links. In regenerated HTML any card links would use `.html` extensions.
