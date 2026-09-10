# Derivative as Slope

**Page type:** detail page (tutorial layout: 4 card-sections, each h2 + two-column table.layout with text left 50% / canvas right 50%)
**HTML title tag:** Derivative as Slope

**Subtitle:** How fast is it changing right now — the speedometer reading behind every trend line

## The Speedometer on a Road Trip

**Tags:** `core idea` (blue), `running example` (green), `tangent line` (blue)

- **The trip** — a car accelerates onto a highway; the odometer traces distance = 20×t² km
- **Two gauges** — the odometer shows where you are; the speedometer shows how fast that changes
- **Slope is speed** — on the distance-vs-time plot, the curve's steepness at t is your speed at t
- **Tangent line** — the straight line that just grazes the curve at one instant; its slope is the derivative
- **Right now** — the derivative is the speedometer needle at this instant, not the trip average

*Example:* Two hours in, the odometer reads 80 km and the curve climbs at 80 km/h — that steepness is the derivative.

**Key point:** A derivative is just a slope read off a curve at one point — "how much does the output move if I nudge the input a tiny bit, right here".

### Visualization (canvas `c1`, 720×300)

Line chart: distance curve d = 20t² with tangent line at t=2.

- **Title (bold 15px, `#1a5276`, top center):** "Odometer Curve: distance = 20 × t²  (tangent slope = speed)".
- **Data:** t = [0, 0.25, 0.5, 0.75, 1, 1.25, 1.5, 1.75, 2, 2.25, 2.5, 2.75, 3]; d = [0, 1.25, 5, 11.25, 20, 31.25, 45, 61.25, 80, 101.25, 125, 151.25, 180].
- **Axes:** x 0–3 (integer tick labels 0–3), y 0–190 with horizontal gridlines (`#e5e9ef`) and labels at 40, 80, 120, 160 (gray `#6b7280` 12px, right-aligned); gray `#999` L-shaped axes; padding top 46 / bottom 46 / left 62 / right 30. Axis titles gray `#444` 12px: "time (hours)" bottom center, "distance (km)" rotated on left.
- **Curve:** blue `#2a78d6`, 3px polyline through the data points.
- **Tangent:** orange `#d95926` 2.5px line through (2, 80) with slope 80, drawn from t=1.2 to t=2.8; orange 6px dot at (2, 80).
- **Annotations:** bold orange 13px left-aligned near (0.55, 150): "tangent at t = 2 h: slope = 80 km/h" / "= the speedometer reading"; bold blue 12px near (2.05, 70): "odometer: 80 km".

## Shrinking the Stopwatch Window

**Tags:** `worked example` (green), `instantaneous` (blue)

- **Average speed** — distance covered ÷ time taken, over any window you pick
- **1-hour window** — t=2 to 3: (180 − 80) / 1 = 100 km/h; too wide, it mixes in later speeding-up
- **Half hour** — t=2 to 2.5: (125 − 80) / 0.5 = 90 km/h; getting closer
- **Six minutes** — t=2 to 2.1: (88.2 − 80) / 0.1 = 82 km/h; closer still
- **36 seconds** — t=2 to 2.01: (80.802 − 80) / 0.01 = 80.2 km/h; the answers settle at 80
- **The limit** — that settling value, 80 km/h, is the derivative of 20t² at t=2 (formula: 40t)

*Example:* Check one by hand: 20 × 2.1² = 88.2, so the 0.1 h window gives (88.2 − 80) / 0.1 = 82 km/h.

**Key point:** The derivative is an average speed over a window you shrink until it stops mattering — here the answers march 100, 90, 82, 80.2 toward exactly 80.

### Visualization (canvas `c2`, 720×300)

Bar chart: average speed over shrinking windows converging to 80.

- **Title (bold 15px, `#1a5276`, top center):** "Average Speed From t = 2 h, Over Smaller and Smaller Windows".
- **Data:** window labels ['1 h', '0.5 h', '0.1 h', '0.01 h']; speeds [100, 90, 82, 80.2]; y scale max 110.
- **Bars:** 74px wide, fill `rgba(42,120,214,0.4)`, stroke blue `#2a78d6` 1.5px; value labels bold 13px `#1a5276` above each bar; window labels gray `#444` 12px below. Padding top 52 / bottom 58 / left 62 / right 200.
- **Limit line:** dashed green `#008300` (dash 6/4, 2px) horizontal at y=80, extending past the bars to the right.
- **Axis titles (gray `#444` 12px):** "stopwatch window width (shrinking →)" bottom center; "average speed (km/h)" rotated on left.
- **Annotation (bold green 13px, left-aligned right of the plot, near the limit line):** "the answers settle at" / "80 km/h — that limit" / "IS the derivative".

## Trend Lines, Growth Rates, and Training Loss

**Tags:** `where it's used` (blue), `watch out` (orange)

- **Growth reports** — "users grew this week" is a derivative; the total alone hides the trend
- **Early warning** — the total keeps rising long after the rate of new signups has turned down
- **Model training** — the loss curve's slope says whether learning is fast, stalling, or done
- **Gradients** — every gradient in ML is a bundle of derivatives: one slope per model weight
- **Without it** — you report record totals while growth quietly dies underneath

*Example:* In the illustrative chart, total users still climb at week 12 while weekly new signups peaked back at week 6.

**Key point:** Data scientists rarely care about the value; they care about its slope. The total is the odometer — the business runs on the speedometer.

### Visualization (canvas `c3`, 720×300)

Two side-by-side line panels: cumulative total vs weekly new users.

- **Title (bold 15px, `#1a5276`, top center):** "Total Users vs Weekly New Users (illustrative)".
- **Data:** weeks 1–12; newUsers = [120, 140, 160, 180, 185, 190, 180, 160, 130, 100, 70, 40]; total = [120, 260, 420, 600, 785, 975, 1155, 1315, 1445, 1545, 1615, 1655].
- **Left panel** (x=60, y=52, 280×170, y max 1800): total users as blue `#2a78d6` 3px line; panel title bold 13px `#1a5276` "total users (still climbing)"; x labels "week 1" and "week 12" (gray `#444` 12px).
- **Right panel** (x=410, y=52, 280×170, y max 200): weekly new users as orange `#d95926` 3px line; title "weekly new users (the derivative)"; same week labels.
- **Divider:** vertical dashed gray `#bdc3c7` line (dash 4/3) at x=372.
- **Peak marker:** magenta `#d55181` 5px dot at week 6 on the right panel, labeled "peak: week 6" bold 12px above.
- **Bottom caption (bold magenta 13px, centered):** "growth turned down at week 6 — the total never shows it".

## High Is Not the Same as Rising

**Tags:** `common mistake` (red), `rule of thumb` (blue)

- **The mix-up** — people read a big value as "doing well"; the derivative asks "getting better?"
- **Product A** — scores 95 today but its slope is negative: it loses ground every week
- **Product B** — scores only 10 but its slope is positive and steepening: it is the one to watch
- **Zero slope** — a flat curve means "no change right now", even at the top of a mountain
- **Rule** — value answers "where are we?"; derivative answers "where are we headed?"

*Example:* By week 8 product A has slid from 95 to 60 while B has climbed from 10 to 59 — the slopes told the story at week 1.

**Common mistake:** Judging by level instead of slope. A high, falling metric and a low, rising one look opposite on a dashboard — the derivative ranks them correctly.

### Visualization (canvas `c4`, 720×300)

Two-line chart: a high-but-falling series vs a low-but-rising one.

- **Title (bold 15px, `#1a5276`, top center):** "Same Dashboard, Opposite Slopes (illustrative scores)".
- **Data:** weeks 1–8; A = [95, 93, 90, 86, 81, 75, 68, 60] (blue `#2a78d6`, 3px); B = [10, 14, 19, 25, 32, 40, 49, 59] (green `#008300`, 3px); y scale 0–100.
- **Axes:** gray `#999` L-shape; week numbers 1–8 as x tick labels, "week" axis title (gray `#444` 12px); padding top 50 / bottom 48 / left 62 / right 170.
- **Series labels (bold 13px):** blue "A: high but falling" near the start of A; green "B: low but rising" near the start of B.
- **Annotation (bold magenta 13px, left-aligned right of the plot):** "the slopes called it" / "at week 1 — levels" / "took 8 weeks to agree".

## Regeneration instructions

- **Template:** tutorials topic page (see `tutorials/CLAUDE.md`): `<h1>` (no index number) with 2px `#2980b9` bottom border, `.subtitle` gray one-liner, then 4 `.card-section` blocks. Each section: `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` row of colored pill spans (0.72rem bold, radius 10px; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`); then a `<ul>` of one-line bullets each opening with `<b>` in `#1a5276`; one italic `.example` line (`#555`); one `.key-point` callout (background `#f8f9fa`, left border `3px solid #e74c3c`, 0.9rem) starting with `<strong>Key point:</strong>` or `<strong>Common mistake:</strong>`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; canvases `width:100%` with `1px solid #e0e0e0` border, 4px radius.
- **Canvases:** intrinsic 720×300, scaled by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Chart JS palette object: blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. All data arrays hardcoded (no Math.random). Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Site palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- No nav bar, no back/home links, no cross-page links. In regenerated HTML any card links would use `.html` extensions.
