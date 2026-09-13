# The Cost of a Feature

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** The Cost of a Feature

**Subtitle:** A feature is not free once it ships — it keeps billing you in subscriptions, pipelines, and breakage, so every input the model uses must earn back more than it costs to keep

## One Model, Five Inputs, One Monthly Bill

**Tags:** `core idea` (blue), `hidden costs` (orange), `features as liabilities` (green)

- **The shop** — a pizza shop's model predicts delivery time so the site can promise "at your door by 7:40"
- **Five inputs** — order size, day & hour, distance to the address, a live traffic feed, and a weather forecast
- **The bill** — the traffic feed costs $120 subscription + $60 pipeline upkeep + $20 outage cleanup = $200 a month
- **Free vs paid** — order size comes from the shop's own till at $0; the traffic feed never stops billing
- **The idea** — a feature is not just a column in a table; it is a small recurring liability the model carries

*Example (italic):* The traffic feed looked great in testing, but a year later the shop has quietly paid $2,400 for it — the question is what that money bought.

**Key point:** Every feature has a running cost — subscriptions, pipelines, and breakage — that keeps arriving long after training day.

### Visualization (canvas `c1`, 720×300)

Horizontal bar chart: monthly running cost of each of the five inputs, with the traffic feed's bar split into its three cost pieces, showing that one input dominates the bill.

- **Title (bold 15px, `#1a5276`, top center):** "What Each Input Costs the Shop per Month".
- **Legend (11px `#444`, one row under the title):** three 10px squares — blue `#2a78d6` "subscription", orange `#d95926` "pipeline upkeep", magenta `#d55181` "outage cleanup".
- **Axis:** horizontal 2px `#999` line at y=260 from x=230 to x=680 (width 450), cost $0 to $225 (2px per dollar); tick labels "$0", "$50", "$100", "$150", "$200" (12px `#444`) below.
- **Rows (top to bottom at y = 80, 118, 156, 194, 232), each 20px-tall bar starting at x=230, with a left-aligned 12px `#444` feature label at x=20:**
  - "live traffic feed": stacked segments $120 blue `#2a78d6`, $60 orange `#d95926`, $20 magenta `#d55181`; bold 12px `#2c3e50` total label "$200" at the bar end
  - "weather forecast": solid blue bar to $50, bold 12px label "$50"
  - "distance (maps API)": solid blue bar to $20, bold 12px label "$20"
  - "day & hour": no bar; 12px `#6b7280` text "$0 — from the shop's own clock"
  - "order size": no bar; 12px `#6b7280` text "$0 — from the shop's own till"
- **Annotation (bold 13px orange `#d95926`, near x=480, y=60):** "one input is 74% of the whole feature bill".
- **Caption (12px `#444`, bottom right):** "illustrative monthly costs".

## Putting a Dollar on Every Input

**Tags:** `worked example` (blue), `feature roi` (green)

- **The value rule** — every minute shaved off the average delivery error saves about $100 a month in refunds
- **What each buys** — order size cuts 3.0 min, day & hour 2.0, distance 1.5, traffic 0.5, weather 0.2
- **Turn to dollars** — order size is worth $300 a month, day & hour $200, distance $150, traffic $50, weather $20
- **Subtract cost** — net per month: +$300, +$200, +$130 (150−20), −$150 (50−200), −$30 (20−50)
- **The verdict** — two of the five features lose the shop money every month they stay in the model

*Example (italic):* The traffic feed earns $50 of refund savings but bills $200 — keeping it costs the shop $150 a month.

**Key point:** Feature ROI = (error cut × value per unit) − running cost; for the traffic feed that is 0.5 × $100 − $200 = −$150 a month.

### Visualization (canvas `c2`, 720×300)

Vertical bar chart of net monthly dollars per feature around a zero line: three green bars above, two red bars below, making the money-losers unmistakable.

- **Title (bold 15px, `#1a5276`, top center):** "Net Dollars per Month = Refund Savings − Running Cost".
- **Axes:** origin x=60, plot width 600; y = net dollars from −$200 (y=245) to +$300 (y=60), 12px `#444` tick labels at −$200, −$100, $0, $100, $200, $300 with light `#e5e9ef` gridlines; solid 2px `#999` zero line at y=171 across the plot.
- **Bars (width 70, centers at x = 130, 240, 350, 460, 570), values `[+300, +200, +130, -150, -30]`:** positive bars green `#008300` fill `rgba(0,131,0,0.75)`, negative bars red `#e74c3c` fill `rgba(231,76,60,0.75)`; bold 12px value labels ("+$300", "+$200", "+$130", "−$150", "−$30") just past each bar end, green or red to match.
- **Feature labels (12px `#444`, under the zero line for positive bars, above it for negative bars):** "order size", "day & hour", "distance", "traffic feed", "weather".
- **Annotation (bold 12px red `#e74c3c`, two lines near x=505, y=255):** "cut the two red bars:" / "save $180/mo, give up 0.7 min".
- **Caption (12px `#444`, bottom left):** "illustrative — $100 per minute of error".

## Why Models Get Heavier Every Year

**Tags:** `where it bites` (blue), `feature creep` (orange)

- **Feature creep** — each new input adds a little accuracy, so nobody ever says no; the bill compounds
- **Diminishing returns** — the first free input cut 3.0 minutes of error; the fifth paid one cut 0.2
- **The split** — the last two features deliver 10% of the total gain but 93% of the total cost
- **Silent failures** — every external feed is one more thing that can break in the middle of dinner rush
- **The habit** — review features like subscriptions: re-justify each one every year or cut it

*Example (italic):* Adding all five inputs in order, the minutes-cut curve climbs 3.0, 5.0, 6.5, 7.0, 7.2 and flattens — while the cost line jumps from $20 to $270.

**Key point:** Accuracy gains flatten while running costs stack up — a feature list that is never reviewed drifts toward negative ROI.

### Visualization (canvas `c3`, 720×300)

Two-line chart over the order features were added: cumulative minutes of error cut (green, left scale) flattening out while cumulative monthly cost (orange dashed, right scale) keeps climbing.

- **Title (bold 15px, `#1a5276`, top center):** "Gain Flattens, Cost Keeps Climbing".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 190; x = six stages at x = 60, 180, 300, 420, 540, 660 with 11px `#444` labels below: "none", "+order", "+hour", "+distance", "+traffic", "+weather"; left y = minutes cut 0 to 8 (12px green `#008300` labels 0, 2, 4, 6, 8; light `#e5e9ef` gridlines); right y = monthly cost $0 to $300 (12px orange `#d95926` labels $0, $100, $200, $300 at x=668).
- **Minutes line:** green `#008300` 3px solid through cumulative values `[0, 3.0, 5.0, 6.5, 7.0, 7.2]` (scale 190px / 8 min), 5px green dots at each point; bold 12px green label "minutes cut (left)" near x=300, y=105.
- **Cost line:** orange `#d95926` 3px dashed (dash 6/4) through cumulative values `[0, 0, 0, 20, 220, 270]` (scale 190px / $300), 5px orange dots; bold 12px orange label "monthly cost (right)" near x=520, y=205.
- **Annotation (bold 13px violet `#4a3aa7`, two lines near x=395, y=70):** "last two inputs: 10% of the gain," / "93% of the cost".
- **Caption (12px `#444`, bottom right):** "illustrative — features added in the order the shop adopted them".

## The Notebook Says Yes, the Ledger Says No

**Tags:** `common mistake` (red), `offline vs production` (orange)

- **The trap** — judging a feature by validation accuracy alone; the notebook never sees the invoice
- **Looks great** — in offline tests the traffic feed cut 0.5 min, the biggest gain of any paid input
- **Costs more** — at $100 per minute of value, 0.5 min earns $50 against a $200 running cost
- **Break-even line** — a feature pays off only when its monthly cost < $100 × the minutes it cuts
- **Same trap at scale** — swap refunds for revenue and the same arithmetic runs at any company size

*Example (italic):* Two features cleared the accuracy bar and still failed the money bar — the bar that pays salaries.

**Common mistake:** Treating "improves the metric" as "worth shipping". A feature must beat its own running cost, not just beat zero — state both numbers when you propose it.

### Visualization (canvas `c4`, 720×300)

Scatter plot of the five features: minutes of error cut on x, monthly running cost on y, with the break-even line drawn diagonally — dots above the line lose money no matter what the notebook said.

- **Title (bold 15px, `#1a5276`, top center):** "Accuracy Gain vs Running Cost: the Break-Even Line".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 190; x = minutes of error cut 0 to 3.5, 12px `#444` tick labels "0.0"–"3.5" every 0.5; y = monthly cost $0 to $250, 12px `#444` labels at $0, $50, $100, $150, $200, $250 with light `#e5e9ef` gridlines.
- **Break-even line:** dashed `#6b7280` (dash 6/4) 2px line for cost = $100 × minutes, from (0 min, $0) = (60, 245) to (2.5 min, $250) = (489, 55); 12px `#6b7280` label along it near x=330, y=155: "break-even: $100 per minute cut".
- **Dots (8px), each with a 12px label beside it showing the net:** green `#008300` — "order size +$300" at (3.0, $0), "day & hour +$200" at (2.0, $0), "distance +$130" at (1.5, $20); red `#e74c3c` — "traffic feed −$150" at (0.5, $200), "weather −$30" at (0.2, $50).
- **Annotation (bold 13px red `#e74c3c`, near x=140, y=80):** "above the line: costs more than it earns".
- **Caption (12px `#444`, bottom right):** "illustrative — same numbers as the worked example".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`. Red `#e74c3c` is used only for money-losing features (a genuine loss state).
- **Data:** all bar values, line points, and dot coordinates are the hardcoded literal arrays above (no randomness); the shared numbers are: minutes cut `[3.0, 2.0, 1.5, 0.5, 0.2]`, monthly costs `[$0, $0, $20, $200, $50]`, value $100 per minute, nets `[+300, +200, +130, -150, -30]` — text, table logic, and all four charts must agree with them exactly.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
