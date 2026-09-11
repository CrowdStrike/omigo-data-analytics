# Crypto Exchange

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Crypto Exchange

**Subtitle:** When the price moves violently, everyone opens the app at once — an exchange's load is 10–50× spikes that track market volatility, not the clock

## The Afternoon the Price Broke 12%

**Tags:** `core idea` (blue), `correlated demand` (red), `volatility` (orange)

- **The quiet baseline** — a crypto exchange idles at ~2,000 requests/sec on an ordinary afternoon
- **The trigger** — at 14:07 the asset price drops 12% in twenty minutes; alerts and headlines fire
- **The stampede** — holders, traders, and the merely curious all open the app in the same minutes
- **The spike** — traffic hits 40,000 req/s by 14:12: 20× the baseline, five minutes after the break
- **The cruel timing** — peak load lands exactly when order flow and market-data fan-out are heaviest

*Example (italic):* At 14:06 the exchange serves 2,200 req/s; at 14:12 it serves 40,000 — and every one of those users wants a live price and a working buy/sell button.

**Key point:** An exchange's traffic is a function of an external signal — price volatility. The spike is not diurnal, not seasonal, and not forecastable from last week's curve; it arrives whenever the market does.

### Visualization (canvas `c1`, 720×300)

Dual-line chart on a shared time axis: request rate (real y-axis) and the asset price (schematic line) — the traffic spike is a mirror of the price break.

- **Title (bold 15px, `#1a5276`, top center):** "Price Breaks 12%, Traffic Jumps 20× in Five Minutes".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = time 14:00 to 14:40 with 12px `#444` tick labels every 10 minutes ("14:00"…"14:40"); y = requests/sec 0 to 40,000, gridlines `#e5e9ef` at 10,000 / 20,000 / 30,000 with 12px `#444` labels ("10k", "20k", "30k").
- **Traffic line:** blue `#2a78d6` 3px line through minutes-past-14:00 `[0, 5, 7, 8, 9, 10, 12, 15, 20, 30, 40]`, req/sec `[2000, 2100, 2200, 9000, 24000, 34000, 40000, 38000, 30000, 18000, 9000]` — flat, near-vertical wall from 14:07, slow decay.
- **Price line:** orange `#d95926` 2px dashed (dash 6/4) line at schematic pixel heights above baseline `[150, 150, 148, 120, 95, 78, 70, 68, 72, 80, 84]` on the same minute grid — flat, cliff at 14:07, slight rebound; 12px `#d95926` label "price (schematic)" above its left end.
- **Break marker:** vertical dashed `#6b7280` (dash 4/3) line at minute 7, 12px `#6b7280` label "price breaks 14:07" at its top.
- **Annotation (bold 13px `#1a5276`, near minute 16, y=70):** "20× load in 5 minutes".
- **Caption (12px `#444`, bottom right):** "traffic and price illustrative".

## Racing the Spike: Boot Time vs Ramp Time

**Tags:** `worked example` (blue), `autoscaling` (green), `headroom` (orange)

- **The ramp** — load climbs 2,200 → 40,000 req/s in the five minutes after the 14:07 price break
- **Reactive lag** — the CPU alarm needs 1 min of sustained load; instances boot 3 min, warm 1 min
- **The arithmetic** — 1 + 3 + 1 means the first reactive capacity lands 5 minutes in, at the peak
- **Hand-check** — at minute 2 load is 24,000 but reactive capacity is still 6,000: 3 of 4 requests fail
- **The fix** — hold 20,000 req/s pre-provisioned (10× baseline) and trigger scaling on the price move
- **The residue** — the brief 34,000-vs-20,000 gap just before minute 3 is absorbed by shedding low tiers

*Example (italic):* With headroom, the worst moment sheds ~2 requests in 5 (34,000 arriving vs 20,000 capacity just before minute 3); reactive-only autoscaling drops 3 in 4 for most of the ramp.

**Key point:** When the spike ramps faster than instances boot, reactive autoscaling always arrives late — you buy survival with pre-provisioned headroom and by scaling on the external signal (the price move), not on the CPU graph it causes.

### Visualization (canvas `c2`, 720×300)

Three-line chart: the load ramp vs two capacity step-lines — reactive autoscaling (arrives at the peak) and pre-provisioned headroom with a price-move trigger (already there).

- **Title (bold 15px, `#1a5276`, top center):** "The Autoscaler Arrives at the Peak; Headroom Is Already There".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = minutes after the price break 0 to 15 with 12px `#444` tick labels every 3 minutes ("0"…"15"); y = requests/sec 0 to 40,000, gridlines `#e5e9ef` at 10,000 / 20,000 / 30,000 with 12px `#444` labels.
- **Load line:** blue `#2a78d6` 3px line through minutes `[0, 1, 2, 3, 5, 8, 13, 15]`, req/sec `[2200, 9000, 24000, 34000, 40000, 38000, 30000, 27000]`.
- **Reactive capacity:** red `#e74c3c` 3px step line: flat 6,000 from minute 0 to 5, step to 12,000 at 5, to 24,000 at 7, to 40,000 at 9.
- **Overload shading:** red fill `rgba(231,76,60,0.12)` between the load line and the reactive step line wherever load exceeds it (roughly minutes 1–9), bold 12px `#e74c3c` label "dropped requests" inside the region near minute 4, y=110.
- **Headroom capacity:** green `#008300` 3px step line: flat 20,000 from minute 0, step to 40,000 at minute 3 (price-signal trigger at minute 0 + 3-minute boot).
- **Legend (12px, top left inside plot):** blue swatch "load", red swatch "reactive autoscale", green swatch "headroom + price trigger".
- **Annotation (bold 13px red `#e74c3c`, near minute 10, y=60):** "reactive fleet lands after the peak".
- **Caption (12px `#444`, bottom right):** "capacities and boot times illustrative".

## Shedding in Tiers, Protecting the Matching Engine

**Tags:** `where it's used` (blue), `load shedding` (red), `serialized core` (orange)

- **The core** — a matching engine serializes every order in a market and cannot scale horizontally
- **Sized for the worst** — the engine is capacity-planned for the worst historical spike, not the average
- **Tier 1: protected** — order placement and cancels always work; people exiting positions need cancels most
- **Tier 2: degraded** — charts, candles, and tickers switch to cached copies that are seconds stale
- **Tier 3: queued** — at extreme peaks, new logins land in a waiting room instead of overloading auth
- **The order** — shed from the bottom tier up: a stale chart is annoying, a failed cancel is money lost

*Example (italic):* At the 40,000 req/s peak, all 4,000 order requests execute live, 8,000 balance checks stay live, 22,000 chart requests get cached data a few seconds old, and 6,000 new sessions wait in a queue.

**Key point:** Around a serialized core you cannot scale, everything else is arranged by sacrifice order — degrade the cheap-to-degrade first so the matching engine and order flow never feel the spike.

### Visualization (canvas `c3`, 720×300)

Two stacked bars — normal traffic vs peak traffic — with the peak bar split into shedding tiers, showing what stays live, what goes stale, and what waits.

- **Title (bold 15px, `#1a5276`, top center):** "At 40,000 req/s, Shed From the Bottom Tier Up".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; y = requests/sec 0 to 40,000, gridlines `#e5e9ef` at 10,000 / 20,000 / 30,000 with 12px `#444` labels; two bar centers at x=210 and x=470, bars 120px wide, 12px `#444` labels under the baseline: "normal 14:00" and "peak 14:12".
- **Normal bar:** single blue `rgba(42,120,214,0.30)` bar with 2px `#2a78d6` border, total 2,000 req/s (about 9px tall), 11px `#444` label "2,000/s" above it.
- **Peak bar, stacked bottom to top:** orders + matching 4,000 solid green `#008300`; balances/portfolio 8,000 blue `rgba(42,120,214,0.30)` with 2px `#2a78d6` border; charts/market data 22,000 orange `rgba(217,89,38,0.30)` with 2px `#d95926` border; new sessions 6,000 red `rgba(231,76,60,0.25)` with 2px `#e74c3c` border.
- **Segment labels (12px, right of each peak segment at x=545):** green "orders — always live (4k)", `#2a78d6` "balances (8k)", `#d95926` "charts — cached, stale (22k)", `#e74c3c` "new sessions — queued (6k)".
- **Annotation (bold 13px green `#008300`, top left inside plot, y=55):** "the matching engine never sees the spike".
- **Caption (12px `#444`, bottom right):** "traffic mix illustrative".

## Your Traffic Is a Chart of the Price, Not the Clock

**Tags:** `common mistake` (red), `forecasting` (orange)

- **The mistake** — capacity-planning an exchange from last week's average or a retail-style diurnal curve
- **Averages lie** — the spike day averages ~2,400 req/s against a 40,000 peak: the mean hides a 17× spike
- **No schedule** — volatility clusters, but the timing is unpredictable; you cannot pre-warm by cron
- **Correlated everywhere** — one price move spikes trading, market data, auth, and support all at once
- **The general lesson** — find the external signal your traffic tracks and size for its spikes, not your mean

*Example (italic):* A retailer sizing to 2× the daily average is comfortable; an exchange sizing to 2× its ~2,400 req/s average (4,800) covers barely a tenth of the 40,000 spike.

**Common mistake:** Treating demand as self-generated and forecastable. When traffic is driven by an external signal — a price, a headline, a storm — the diurnal component is noise; provision headroom, shedding tiers, and the serialized core for the signal's worst spike.

### Visualization (canvas `c4`, 720×300)

Two-line week view on a shared time axis: retail traffic as a smooth repeating daily wave vs exchange traffic as a flat line punctured by violent spikes at arbitrary hours.

- **Title (bold 15px, `#1a5276`, top center):** "Retail Traffic Follows the Clock; Exchange Traffic Follows the Price".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = one week, 12px `#444` tick labels "Mon"…"Sun" at 7 evenly spaced day boundaries (one every ~86px); y unlabeled rate axis — pixel heights schematic, no gridline values.
- **Retail line:** green `#008300` 2px smooth sinusoid, one hump per day: midday peaks at pixel height 70 above baseline, overnight troughs at height 15, seven identical humps.
- **Exchange line:** blue `#2a78d6` 3px line, flat at pixel height 22, with a narrow spike to height 170 late Tuesday and a second spike to height 110 Friday morning; each spike rises and falls within about 20px of x.
- **Labels:** 12px green "retail: same shape every day" above the Wednesday hump; bold 12px blue "exchange: 20× spike, Tue 11pm" beside the tall spike.
- **Annotation (bold 13px red `#e74c3c`, right side near y=70):** "the average (~2,400/s) predicts nothing about the peak".
- **Caption (12px `#444`, bottom right):** "heights schematic, shapes illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); the running example — 2,000 req/s baseline, 12% price break at 14:07, 40,000 req/s peak (20×) five minutes later, 6,000/20,000/40,000 capacity steps, 4k/8k/22k/6k peak traffic mix, ~2,400 req/s day average — is invented, labeled illustrative, and must stay consistent between text and charts.
- **Framing:** the page describes generic exchange-design concepts motivated by the publicly observable volatility–traffic correlation; it makes no claims about any company's internal systems.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
