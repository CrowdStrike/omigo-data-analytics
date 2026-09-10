# Outliers

**Page type:** detail page (tutorial topic page: `.card-section` blocks, each an h2 + two-column `table.layout` — text left 50%, canvas right 50%)
**HTML title tag:** Outliers

**Subtitle:** One sales day looks nothing like the others — a value that far from the rest gets investigated before it gets deleted or averaged

## Ten Days of Sales, One Impossible Day

Tags: `core idea` (blue), `running example` (green)

- **The sales log** — ten days of shop revenue; nine days sit between $290 and $720
- **The odd one** — day 5 shows $50,000, roughly 100x a normal day
- **Instant effect** — the ten-day total is $54,250, and one day is $50,000 of it
- **Outlier** — a value sitting far away from the rest of the data, like day 5 here
- **The question** — before computing anything, ask where the $50,000 came from

*Example:* Nine days pay for the coffee beans; day 5 alone is 92% of the ten-day revenue.

**Key point:** **Key point:** An outlier is a value so far from the rest that it needs its own explanation — and it gets one before any averaging or deleting.

### Visualization (canvas `c1`, 720×300)

Bar chart: ten daily sales bars where day 5 breaks the scale (drawn with an axis break).

- **Title (bold 15px, `#1a5276`, top center):** "Daily Sales: Nine Ordinary Days and One $50,000 Day"
- **Data:** `SALES = [420, 380, 510, 290, 50000, 610, 450, 330, 720, 540]` for days d1–d10.
- **Axes:** y scale 0–800 with horizontal gridlines `#e5e9ef` every $200 labeled `$0…$800` (12px `#6b7280`, right-aligned); plot area x=70, width 580, baseline y=240, chart height 160; gray `#999` baseline; bar width 38, even gaps.
- **Normal bars:** blue `#2a78d6` at 75% alpha, value labels (11px `#444`) above each bar; day labels "d1"…"d10" (11px `#222`) below the baseline.
- **Outlier bar (day 5):** orange `#d95926` at 85% alpha, drawn from y=58 down to the baseline (shooting past the top); two diagonal white break marks (4px white strokes) across the bar near y≈90–105; bold 13px orange label "$50,000" above the bar.
- **Bottom annotation (bold 13px orange, centered, y=285):** "day 5 is ~100x a normal day — it does not even fit on the axis"

## Drawing a Fence Around Normal

Tags: `worked example` (green), `rule of thumb` (blue)

- **Sort the ten days** — 290, 330, 380, 420, 450, 510, 540, 610, 720, 50,000
- **Quartiles** — Q1 = 380 (middle of the lower five), Q3 = 610 (middle of the upper five)
- **The spread** — IQR = 610 − 380 = 230, the width of the middle half
- **The fence** — upper fence = 610 + 1.5 × 230 = 955; anything above gets flagged
- **The verdict** — 720 stays inside the fence; 50,000 is over 50x past it

*Example:* The 1.5 × IQR fence is the rule behind the dots you see beyond box plot whiskers.

**Key point:** **Key point:** The fence is a flagging rule, not a delete rule — it only marks which points deserve a closer look.

### Visualization (canvas `c2`, 720×300)

Number line with IQR band, 1.5×IQR fence, and the outlier beyond an axis break.

- **Title (bold 15px, `#1a5276`, top center):** "Fence = Q3 + 1.5 × IQR = 610 + 1.5 × 230 = 955"
- **Axes:** main number line at y=165 from x=60, width 460, scale $0–$1000 with ticks/labels every $250 (12px `#6b7280`, `$` prefixed); gray `#999` line.
- **IQR band:** translucent blue `rgba(42,120,214,0.12)` rectangle from 380 to 610, 52px tall above the line; bold 12px blue `#2a78d6` labels "Q1 = 380" and "Q3 = 610" above the band edges; 11px `#6b7280` "IQR = 230" centered inside.
- **Data dots:** nine blue `#2a78d6` 6px-radius dots at `[290, 330, 380, 420, 450, 510, 540, 610, 720]`, 12px above the line.
- **Fence:** dashed red `#e74c3c` vertical line (width 2, dash 6/4) at 955; bold 12px red label "fence: $955" above it.
- **Axis break + outlier:** after the main line, two short slanted gray `#6b7280` break strokes, then a continuation line segment; orange `#d95926` 8px-radius dot on it labeled bold 13px orange "$50,000" above and 11px `#6b7280` "(axis break)" below.
- **Bottom annotations (centered):** bold 13px red `#e74c3c` "720 stays inside the fence — 50,000 is over 50x past it" (y=262); 12px `#6b7280` "flagged means \"look at me\", not \"delete me\"" (y=284).

## Same $50,000 — Three Stories, Three Actions

Tags: `where it's used` (blue), `judgment call` (orange)

- **Error** — a $500.00 order typed as $50,000: fix the value, or drop it with a note
- **Real but rare** — a genuine one-off corporate order: keep it, it is real revenue
- **Different population** — a corporate channel in a walk-in shop's log: analyze it separately
- **Same number, three actions** — the value alone cannot tell you which story is true
- **How to check** — pull the receipt, ask the clerk, look for the customer name

*Example:* One phone call to whoever ran the register that day settles what no formula can.

**Key point:** **Key point:** Investigate before deleting — the right action depends on the story behind the number, not on the number itself.

### Visualization (canvas `c3`, 720×300)

Flow diagram: one "$50,000" chip fans into three story boxes, each with an arrow to an action box.

- **Title (bold 15px, `#1a5276`, top center):** "The Value Is Identical — the Story Decides the Action"
- **Three columns (each 196px wide, at x = 30, 262, 494), colored orange `#d95926`, green `#008300`, violet `#4a3aa7` respectively. Each column top-to-bottom:**
  - A "$50,000" chip: 92×26 box, `#f8f9fa` fill, `#6b7280` border, bold 12px `#333` text, at y=44.
  - A colored downward arrow.
  - A story box (full column width × 74, at y=100): 12% alpha colored fill, 2px colored border, bold 12px colored heading + 11px `#333` detail line:
    - Column 1: "ERROR" / "$500.00 typed as $50,000"
    - Column 2: "REAL BUT RARE" / "a one-off corporate order"
    - Column 3: "DIFFERENT POPULATION" / "a corporate channel, not walk-in"
  - Another colored downward arrow.
  - An action box (column width − 28 × 46, white fill, 1.5px colored border, bold 11px colored two-line text, at y=204):
    - Column 1: "fix the value," / "or drop with a note"
    - Column 2: "keep it —" / "it is real revenue"
    - Column 3: "analyze it" / "separately"
- **Bottom annotation (bold 13px `#e74c3c`, centered, y=282):** "no formula picks the column — the receipt does"

## The Reflex to Avoid: "Outlier = Delete"

Tags: `common mistake` (red), `watch out` (orange)

- **Blind keeping** — the mean becomes $5,425, over 11x a typical $480 day
- **Blind deleting** — dropping day 5 erases $50,000 of $54,250: 92% of revenue gone
- **Planning trap** — stock inventory for a $5,425 "average" day and it rots on the shelf
- **Reporting trap** — if day 5 is real, deleting it hides your biggest customer
- **The habit** — flag it, investigate it, then decide: fix, keep, or split off

*Example:* The median barely cares either way — $450 without day 5, $480 with it.

**Key point:** **Common mistake:** Deleting on sight destroys real information; keeping without thought corrupts every average. Both skip the investigation step.

### Visualization (canvas `c4`, 720×300)

Split panel: left, three summary bars (mean with/without, median); right, a stacked revenue bar showing day 5's share.

- **Title (bold 15px, `#1a5276`, top center):** "Keep Blindly: a Useless Mean · Delete Blindly: 92% of Revenue Gone"
- **Left panel (bars 74px wide, 28px gaps, from x=60; baseline y=220, chart height 150, scale max 5800; 75% alpha fills, bold 13px colored `$` value labels above, two-line 11px `#333` captions below):**
  - "$5,425" orange `#d95926`, caption "mean, all" / "10 days"
  - "$472" blue `#2a78d6`, caption "mean, day 5" / "removed"
  - "$480" green `#008300`, caption "median," / "either way"
  - Bold 12px orange annotation "11x a typical day" above the first bar (y=58).
- **Divider:** dashed vertical `#bdc3c7` line at x=400 (dash 4/3).
- **Right panel (stacked horizontal bar at x=430, width 260, y=110, height 44):**
  - Header (bold 12px `#1a5276`, centered): "Ten-day revenue: $54,250"
  - Orange `#d95926` segment sized 50000/54250 of the width, white bold 12px in-segment label "day 5: $50,000"; blue `#2a78d6` remainder with blue bold 11px label "other 9 days: $4,250" below (connected by a thin blue leader line).
  - Bold 12px red `#e74c3c` two lines centered below (y=200/216): "delete day 5 and 92% of the" / "ten days’ revenue disappears"
- **Bottom annotation (bold 13px violet `#4a3aa7`, centered, y=285):** "both reflexes skip the only step that matters: finding out what day 5 was"

## Regeneration instructions

- **Layout:** tutorial topic page. `<h1>` (no index number) with 2px `#2980b9` bottom border, `.subtitle` paragraph, then four `.card-section` blocks. Each section: `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) followed by `table.layout` (100% width, collapsed) with one row: `td.text-col` (50%) and `td.viz-col` (50%), both 12px padding, top-aligned.
- **Text cell structure:** `.tags` pill row, `<ul>` of 5 one-line bullets each opening with `<b>bold term</b>` (bold terms `#1a5276`), one italic `.example` paragraph, one `.key-point` callout.
- **Tag pills:** 0.72rem, weight 600, padding 2px 10px, radius 10px. Colors — blue: bg `rgba(26,82,118,0.12)` text `#1a5276`; green: bg `rgba(39,174,96,0.15)` text `#27ae60`; red: bg `rgba(231,76,60,0.12)` text `#e74c3c`; orange: bg `rgba(230,126,34,0.15)` text `#e67e22`.
- **Key-point callout:** background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem. `.example`: italic, `#555`, 0.9rem.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; canvases `width:100%` with `1px solid #e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** each canvas declares intrinsic `width="720" height="300"`; a shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Shared data array `SALES = [420, 380, 510, 290, 50000, 610, 450, 330, 720, 540]`; all data hardcoded/deterministic (no `Math.random()`). Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object `P`:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette anchors: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- **Links:** this page has no card links; any grid page linking here uses the `.html` extension in regenerated HTML.
