# Law of Large Numbers

**Page type:** detail page (tutorial layout: `.card-section` blocks, each an h2 + two-column `table.layout` — text left 50%, canvas right 50%)
**HTML title tag:** Law of Large Numbers

**Subtitle:** One bet is a coin toss; a hundred thousand bets are a paycheck — averages settle down as counts grow, single events never do.

## The Casino's Boring Superpower

**Tags:** `core idea` (blue), `running example` (green)

- **The game** — every $1 bet: the player wins 49% of the time, the house wins 51%
- **The edge** — on average the house keeps 2 cents per dollar bet (51% − 49%)
- **Over 10 bets** — anything happens: the house can easily be down money
- **Over 100,000 bets** — the average take per bet settles almost exactly on 2 cents
- **The name** — this settling of averages is the Law of Large Numbers

*Example:* In one simulated run the house was still losing after 200 bets — and sat at exactly +2.0% by 100,000.

**Key point:** The casino never needs to win the next bet — it needs the average of many bets, and that average is a near-sure thing.

### Visualization (canvas `c1`, 720×300)

Line chart on a log-x axis: running average house take per bet converging to +2%.

- **Title (bold 15px, `#1a5276`, top center):** "House Take per Bet, Averaged over the First n Bets (one simulated run)".
- **Data (seeded simulation, sampled, literal values):** n = `[1, 2, 3, 5, 7, 10, 20, 50, 100, 200, 500, 1000, 2000, 5000, 10000, 20000, 50000, 100000]`; averages (%) = `[-100, 0, 33.3, 60, 71.4, 20, -10, 0, -2, -8, -6.4, -4, -1.3, 1, 1.8, 1, 1.6, 2]`.
- **Axes:** x log10 scale spanning 1 to 100,000 with tick labels 1, 10, 100, 1k, 10k, 100k and caption "number of bets (log scale)"; y −100% to +100% with gridlines/labels at −100, −50, 0, 50, 100 (gray `#6b7280` labels, grid `#e5e9ef`, axis `#999`). Padding top 46, bottom 52, left 62, right 30.
- **Reference lines:** dashed gray zero line (dash 4/4); dashed green `#008300` line at +2% (dash 6/4, width 2) labeled bold 12px green "true edge: +2%".
- **Series:** blue `#2a78d6` line width 3 with 3.5px dots at each sampled n.
- **Annotations (bold 13px):** magenta `#d55181` "down 8% after 200 bets" near the n=200 point; green right-aligned "locked on +2.0% by 100k" near the right end.

## The Worked Numbers: 10 vs 1,000 vs 100,000 Bets

**Tags:** `worked example` (green), `hand math` (blue)

- **Per $1 bet** — the house expects +$0.02, but a single bet swings a full ±$1
- **10 bets** — expected +$0.20; the usual range runs about −$6 to +$6
- **1,000 bets** — expected +$20; usual range about −$43 to +$83: losing still possible
- **100,000 bets** — expected +$2,000; usual range +$1,368 to +$2,632: losing is gone
- **The pattern** — the wobble grows like √n while the expected total grows like n

*Example:* √100,000 ≈ 316, so the wobble (±2 × $316 = ±$632) is tiny next to the +$2,000 expectation.

**Key point:** Expectation outgrows the wobble — past some number of bets, the usual range no longer touches zero.

### Visualization (canvas `c2`, 720×300)

Range-bar chart: usual range of house take per bet at three bet counts.

- **Title (bold 15px, `#1a5276`, top center):** "Usual Range of House Take per Bet (expected ±2 standard deviations)".
- **Data (three vertical range bars, 56px wide, at 18%/50%/82% of plot width):**
  - "10 bets": range −61% to +65%, dollar caption "−$6 to +$6" — blue outline `#2a78d6`, fill `rgba(42,120,214,0.20)`.
  - "1,000 bets": range −4.3% to +8.3%, caption "−$43 to +$83" — blue outline, same fill.
  - "100,000 bets": range +1.37% to +2.63%, caption "+$1,368 to +$2,632" — green outline `#008300`, fill `rgba(0,131,0,0.25)` (entirely positive).
- **Axes:** y −80% to +80% with gridlines/labels at −80, −40, 0, 40, 80; padding top 46, bottom 56, left 62, right 30. Dashed gray zero line labeled "break even" (12px gray, right side above line).
- **Expected-value ticks:** thick orange `#d95926` horizontal tick (width 3) at +2% across each bar (extending 8px past each side); legend text bold 12px orange top-left: "orange tick = expected +2%".
- **Annotation (bold 13px green, centered above the third bar, two lines):** "range entirely above zero:" / "the house cannot lose".
- **Labels:** bar labels bold 12px `#2c3e50` below the axis; dollar captions 12px gray beneath.

## Where You Meet It: Small Samples Lie

**Tags:** `where it's used` (blue), `common mistake` (red)

- **Same page, two sites** — both truly convert 10% of visitors; one gets 50 visits a day, one 5,000
- **Small site** — the daily rate swings from 2% to 16%; every swing looks like news
- **Big site** — the daily rate stays between 9.3% and 10.6%; the truth is visible
- **The trap** — reading meaning into an average built on a handful of events
- **Rule of thumb** — before reacting to a rate, ask how many events sit under it

*Example:* "Conversion doubled on Tuesday!" — from 3 sales to 6, on 50 visitors.

**Key point:** An average is only as trustworthy as the count behind it — the law only works for you once n is large.

### Visualization (canvas `c3`, 720×300)

Two-line time series: daily conversion rate over 30 days for a small and a big site.

- **Title (bold 15px, `#1a5276`, top center):** "Daily Conversion Rate, 30 Days — Both Sites Truly Convert 10%".
- **Data (simulated once with a fixed seed, then hardcoded):**
  - 50 visitors/day (orange `#d95926`, width 2.5): `[6, 14, 8, 14, 8, 12, 14, 10, 6, 16, 8, 16, 4, 12, 14, 10, 14, 4, 16, 6, 6, 8, 12, 6, 8, 12, 8, 12, 2, 6]`.
  - 5,000 visitors/day (blue `#2a78d6`, width 2.5): `[9.9, 10.5, 9.5, 9.6, 9.3, 10.3, 9.5, 10.2, 9.9, 10.3, 10.6, 10.2, 9.4, 9.6, 9.8, 9.9, 10, 10.3, 9.4, 10.5, 10.1, 10.1, 10, 9.6, 9.5, 10.1, 10.6, 9.7, 10.4, 9.6]`.
- **Axes:** y 0–18% with gridlines/labels every 6%; x labels "day 1", "day 10", "day 20", "day 30"; padding top 46, bottom 50, left 62, right 165; grid `#e5e9ef`, axis `#999`.
- **Reference:** dashed gray line (dash 5/4, width 1.5) at the true rate 10%.
- **Legend (right side at x = w−155):** orange swatch "50 visitors/day", blue swatch "5,000 visitors/day", thin gray bar "true rate 10%".
- **Annotations (bold 13px):** orange "2% to 16% — pure noise" upper left; blue "9.3% to 10.6% — truth visible" mid-chart below the blue line.

## What the Law Does NOT Say

**Tags:** `common confusion` (orange), `hand math` (blue)

- **No memory** — after 10 straight losses the house isn't "due" a win; the next bet is still 51/49
- **No balancing** — an early deficit is never paid back; it is diluted by new bets
- **Worked check** — 60 heads in 100 flips: after 1,000 more, expect 560 of 1,100 → 50.9%
- **The excess stays** — those 10 extra heads never disappear; they just stop mattering
- **Averages settle, totals don't** — the running total keeps wandering forever

*Example:* A roulette table that hit red 8 times in a row owes black exactly nothing.

**Key point:** The law fixes proportions by dilution, not events by compensation — nothing is ever "due".

### Visualization (canvas `c4`, 720×300)

Two-panel chart: excess heads stays constant while the heads share dilutes.

- **Title (bold 15px, `#1a5276`, top center):** "60 Heads in 100 Flips, Then Keep Flipping: Dilution, Not Payback". Dashed light-gray vertical divider at x=360.
- **Shared x labels:** `['100 flips', '1,100 flips', '10,100 flips']`.
- **Left panel (title bold 13px ink "expected extra heads"):** three violet `#4a3aa7` bars, 52px wide at x centers 95/190/285, all the same height (value +10 on a 0–14 scale, baseline y=225, height 130); bold 13px "+10" above each; annotation bold 12px violet below: "the surplus never gets paid back".
- **Right panel (title bold 13px ink "share of heads"):** aqua `#199e70` line (width 3, 5px dots) through shares `[60, 50.9, 50.1]`% at x centers 455/550/645, y scale 46–62%; dashed gray reference line at 50% labeled "50%"; bold 13px value labels above each dot; annotation bold 12px aqua below: "the share drowns in new flips".
- **Bottom annotation (bold 13px orange `#d95926`, centered, y=288):** "same data, two views: the count stays, the proportion settles".

## Regeneration instructions

- **Layout:** tutorial detail page. h1 + `.subtitle`, then four `.card-section` blocks; each has an `<h2>` (1.3rem `#1a5276`, 2px solid `#2980b9` bottom border) and a `table.layout` with one row: `.text-col` td (50%) and `.viz-col` td (50%), 12px padding.
- **Left column structure per section:** a `.tags` row of colored pill spans, a `<ul>` of one-line bullets each opening with `<b>bold term</b>` (bold terms in `#1a5276`), one italic `.example` paragraph (`#555`, 0.9rem), one `.key-point` callout (background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem) starting with `<strong>Key point:</strong>`.
- **Tag pill styles:** 0.72rem, weight 600, padding 2px 10px, radius 10px; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page style:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` bottom border; subtitle `#666` 0.95rem. No nav bar, no back/home links.
- **Canvas:** each 720×300 intrinsic, `width:100%` CSS, `1px solid #e0e0e0` border, radius 4px; scaled by `window.devicePixelRatio` via a shared `setup(id)` helper (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates). All data as hardcoded literal arrays — no `Math.random()`.
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Doc palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
