# Exponential Growth

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Exponential Growth

**Subtitle:** Doubling looks harmless for a while and then explodes — compounding multiplies instead of adds, and our linear intuition keeps underestimating it

## An App That Doubles Its Signups Every Week

**Tags:** `core idea` (blue), `doubling time` (green), `compounding` (orange)

- **The app** — one founder signs up week 0; every user brings one friend, so signups double weekly
- **The count** — 1, 2, 4, 8, 16, ... after 12 weeks that innocent doubling reaches 4,096
- **Slow start** — weeks 0 through 6 add up to just 127 users; it feels like nothing is happening
- **Last step rule** — each week adds more than all previous weeks combined (2,048 in week 11 → 12)
- **Multiply, not add** — growth is "×2 per week", not "+N per week"; that one word changes everything

*Example (italic):* At week 6 only 64 people sign up (127 users total) and it looks dead; six more doublings later week 12 alone adds 4,096.

**Key point:** Exponential growth means the amount added each step grows with the total. The curve is not "slow then fast" — it is the same ×2 the whole time; only our linear eyes change.

### Visualization (canvas `c1`, 720×300)

Vertical bar chart of weekly doubling signups, weeks 0–12, with a bracket over the flat early weeks.

- **Title (bold 15px, `#1a5276`, top center):** "Signups per Week When Every User Brings One Friend".
- **Data:** weeks 0–12, values `[1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096]`.
- **Axes:** origin x=60, width 620, baseline y=245, chart height 185, y scale max 4200; week numbers 12px `#444` below each bar; x-axis title "week" (12px, bottom center); minimum bar height 1.5px.
- **Bars:** weeks 0–6 fill `rgba(217,89,38,0.6)` (orange), weeks 7–12 fill `rgba(42,120,214,0.5)` (blue); bold 12px `#444` value labels "4,096" and "2,048" above the last two bars.
- **Bracket:** orange `#d95926` 2px bracket under weeks 0–6 with bold 13px orange label "weeks 0–6 total: 127 signups — barely visible".
- **Annotation (bold 13px blue `#2a78d6`, right-aligned near top, two lines):** "week 12 alone beats" / "all 12 weeks before it".

## The Rule of 72, Checked by Hand

**Tags:** `worked example` (blue), `rule of 72` (green)

- **The shortcut** — doubling time ≈ 72 ÷ growth rate in %; at 12% per month: 72 ÷ 12 = 6 months
- **Check it** — take 500 users ×1.12 six times: 500 → 560 → 627 → 702 → 787 → 881 → 987
- **Close enough** — 987 ≈ 2 × 500; the shortcut predicted the doubling within 2%
- **It repeats** — six more months of 12% lands at 1,948 ≈ 4 × 500; every 6 months doubles again
- **Works broadly** — 72 ÷ 8 = 9 years for 8% interest; 72 ÷ 2 = 36 for 2% inflation

*Example (italic):* A product manager hears "only 12% monthly growth" and misses that it means roughly 16× in two years.

**Key point:** 72 divided by the percent growth per period gives the periods to double. It turns any steady growth rate into a doubling clock you can run in your head.

### Visualization (canvas `c2`, 720×300)

Line chart of 12% monthly compounding over 12 months, with reference lines at 2× and 4× and highlighted doubling points.

- **Title (bold 15px, `#1a5276`, top center):** "500 Users Growing 12% per Month — Doubles Every ~6 Months".
- **Data:** months 0–12, values `[500, 560, 627, 702, 787, 881, 987, 1105, 1238, 1387, 1553, 1739, 1948]`.
- **Axes:** origin x=65, width 600, baseline y=245, chart height 180, y scale max 2100; month numbers 12px `#444` below points; x-axis title "month" (bottom center).
- **Reference lines:** dashed `#ccc` (dash 5/4) horizontal lines at 1,000 and 2,000, labeled "2× = 1,000" and "4× = 2,000" (12px `#888`).
- **Series:** blue `#2a78d6` 3px line; 3.5px blue dots, except months 6 and 12 which get 6px green `#008300` dots.
- **Annotations (bold 13px green):** "month 6: 987 ≈ 2×" above the month-6 point; "month 12: 1,948 ≈ 4×" right-aligned above the month-12 point.
- **Callout (bold 14px orange `#d95926`, near top-left):** "rule of 72: 72 ÷ 12 = 6 months per doubling ✓".

## Where the Linear Forecast Breaks

**Tags:** `where it's used` (blue), `failure mode` (red)

- **The trap** — a team sees signups go 100, 200, 400 and pencils in "+150 per week" as the trend
- **The miss** — the straight line predicts 1,000 by week 6; the actual doubling curve hits 6,400
- **6× off** — capacity, support staff, and server budget were all planned for the wrong world
- **Both directions** — viral spread, cache misses, and compounding costs all blow past linear plans
- **The test** — plot on a log axis: exponential data goes straight; if it does, forecast in ratios

*Example (italic):* The ops team provisioned for 1,000 users by week 6 and got 6,400 — the pager did the rest.

**Key point:** Fitting a straight line to compounding data always underestimates the future. If each period grows by a percentage, forecast with multiplication, never with a ruler.

### Visualization (canvas `c3`, 720×300)

Two-line chart: actual doubling curve vs the dashed linear forecast fitted to the first three weeks, with a gap marker at week 6.

- **Title (bold 15px, `#1a5276`, top center):** "The Straight-Line Forecast vs What Doubling Actually Did".
- **Data:** weeks 0–6; actual `[100, 200, 400, 800, 1600, 3200, 6400]`; forecast `[100, 250, 400, 550, 700, 850, 1000]`.
- **Axes:** origin x=65, width 480, baseline y=245, chart height 180, y scale max 6600; week numbers 12px below points; x-axis title "week".
- **Series:** actual — solid blue `#2a78d6` 3px line with 4px dots; forecast — dashed orange `#d95926` 3px line (dash 7/5) with 4px dots.
- **Gap marker:** magenta `#d55181` 2px vertical line at week 6 between the two series, with bold 13px magenta labels right-aligned, two lines: "week 6 gap:" / "6,400 vs 1,000".
- **Legend (x=580):** blue swatch "actual (×2/week)"; orange swatch "linear forecast" / "(+150/week)" (12px `#222`).
- **Annotation (bold 12px orange, near forecast line at week 2):** "fit through weeks 0–2".

## "Exponential" Does Not Mean "Fast"

**Tags:** `common mistake` (red), `compounding` (orange)

- **The race** — steady ads bring 100 signups every week; word-of-mouth starts at 1 and doubles
- **Early lead** — at week 5 it is ads 500, doubling 32; the "exponential" channel looks pathetic
- **The cross** — week 10: ads 1,000, doubling 1,024 — and from there it is no contest
- **Week 12** — ads 1,200 vs doubling 4,096; two weeks past the cross it is already 3.4× ahead
- **The definition** — exponential = constant ratio per step; it can be slow for a long time first

*Example (italic):* Judged at week 5, the doubling channel loses 32 to 500 — judged at week 12, it wins 4,096 to 1,200.

**Common mistake:** Calling anything fast "exponential" and dismissing slow exponentials. The word describes the growth pattern, not the speed — and the pattern always wins eventually.

### Visualization (canvas `c4`, 720×300)

Two-line chart: cumulative signups from steady ads vs doubling word-of-mouth, with the week-10 crossover highlighted.

- **Title (bold 15px, `#1a5276`, top center):** "Cumulative Signups: Steady Ads vs Doubling Word-of-Mouth".
- **Data:** weeks 0–12; ads `[0, 100, 200, 300, 400, 500, 600, 700, 800, 900, 1000, 1100, 1200]`; word-of-mouth `[1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096]`.
- **Axes:** origin x=65, width 470, baseline y=245, chart height 180, y scale max 4300; even week numbers (0, 2, ..., 12) labeled 12px `#444` below the axis; x-axis title "week".
- **Series:** ads — aqua `#199e70` 3px line; word-of-mouth — violet `#4a3aa7` 3px line.
- **Crossover marker:** magenta `#d55181` 7px dot at week 10 / 1,024, with bold 13px label "week 10: 1,024 passes 1,000".
- **End labels (bold 13px, right-aligned):** violet "week 12: 4,096"; aqua "week 12: 1,200".
- **Annotation (bold 12px violet, lower-left region):** "behind for 9 straight weeks".
- **Legend (x=575):** aqua swatch "ads: +100/week"; violet swatch "word-of-mouth:" / "×2/week from 1" (12px `#222`).

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (hardcodes W=720, H=300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
