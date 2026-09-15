# Datadog & Splunk

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Datadog & Splunk

**Subtitle:** Observability sold as a product — ship every log, metric, and trace to a vendor and search it in one place — with a bill that grows byte for byte with the data you send

## The 3am Outage You Can Finally Search

**Tags:** `core idea` (blue), `buy vs build` (green), `SaaS` (orange)

- **The stack** — a web shop's 20 services write logs, metrics, and traces no one can grep at 3am
- **The buy** — an agent on each host ships it all to the vendor; search and alerts come built in
- **Splunk** — indexes logs and makes them searchable; priced by GB of data ingested per day
- **Datadog** — one SaaS for metrics, traces, and logs; priced per host, per GB, per feature
- **The trade** — no search cluster to babysit, but every byte you emit is now a line item

*Example (italic):* At 3:07am checkout starts failing; one search across all 20 services finds the bad deploy in 4 minutes.

**Key point:** Observability vendors sell the whole pipeline — collect, index, search, alert — so a team buys in an afternoon what a platform team would build in months; the price is metered on what you send.

### Visualization (canvas `c1`, 720×300)

Left-to-right flow diagram: three signal boxes feed an agent box, which feeds one vendor box; the middle arrow carries a price-meter label.

- **Title (bold 15px, `#1a5276`, top center):** "Buy the Pipeline: Every Signal Flows Through a Metered Pipe".
- **Signal boxes (left column):** three rounded boxes at x=30, y = 70 / 135 / 200, each 150×42, 8px radius, fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border, 12px `#2c3e50` centered labels "logs — 20 services", "metrics — every host", "traces — every request".
- **Agent box:** rounded box at x=260, y=132, 150×48, fill `rgba(25,158,112,0.12)`, 2px `#199e70` border, 12px label "agent on each host"; 3px `#6b7280` arrows from each signal box's right edge to its left edge.
- **Vendor box:** rounded box at x=490, y=105, 200×100, fill `rgba(26,82,118,0.10)`, 2px `#1a5276` border, bold 13px `#1a5276` label "Datadog / Splunk" with 12px `#2c3e50` lines below: "index · search", "dashboards · alerts"; single 3px `#6b7280` arrow from agent box to its left edge.
- **Meter label (bold 13px orange `#d95926`, centered under the agent→vendor arrow at ≈(450, 250)):** "$ per GB ingested · $ per host · $ per feature".
- **Caption (12px `#444`, bottom right):** "pricing dimensions as publicly documented".

## The Bill That Tracks Your Bytes

**Tags:** `worked example` (blue), `pay per GB` (orange)

- **The plan** — in January the shop ships 40 GB of logs a day at $2.50 per GB (illustrative)
- **The math** — 40 GB/day × 30 days × $2.50/GB = $3,000 a month; the invoice is a volume meter (exact)
- **The growth** — traffic, new services, and chattier debug logs push volume to 100 GB/day by July
- **Hand-check** — 100 × 30 × $2.50 = $7,500 a month; nobody shipped a "spend more" feature
- **December** — 220 GB/day means $16,500 a month, 5.5× January's bill for 5.5× the bytes

*Example (italic):* The July invoice reads $7,500 — same dashboards as January, same alerts, just 2.5× the log bytes.

**Key point:** Pay-per-byte pricing makes the bill a straight linear function of data volume — it grows with traffic and logging habits, not with the value the team gets out of the tool.

### Visualization (canvas `c2`, 720×300)

Bar chart of the monthly bill, January through December, rising in lockstep with log volume.

- **Title (bold 15px, `#1a5276`, top center):** "One Year of the Observability Bill: 5.5× the Bytes, 5.5× the Bill".
- **Axes:** origin x=70, baseline y=245, plot width 600, plot height 180; x = months "Jan"–"Dec" (12px `#444` labels under each bar); y = dollars per month 0 to 18,000, gridlines `#e5e9ef` at 4,500 / 9,000 / 13,500 with 12px `#444` labels "$4.5k" / "$9k" / "$13.5k" at x=15.
- **Volume data (GB/day, hardcoded):** `[40, 46, 54, 63, 74, 86, 100, 117, 137, 160, 188, 220]`.
- **Bill data ($/month = GB/day × 30 × 2.50, hardcoded):** `[3000, 3450, 4050, 4725, 5550, 6450, 7500, 8775, 10275, 12000, 14100, 16500]`.
- **Bars:** 12 bars, 34px wide, 16px gap, fill `rgba(42,120,214,0.35)` with 2px `#2a78d6` top edge; July and December bars filled `rgba(217,89,38,0.35)` with `#d95926` edge to mark the hand-check months.
- **Bar labels:** 11px `#444` "$3,000" above Jan, "$7,500" above Jul, "$16,500" above Dec only.
- **Annotation (bold 13px orange `#d95926`, near x=250, y=70):** "bill = bytes × price — a pure meter".
- **Caption (12px `#444`, bottom right):** "volume and $2.50/GB illustrative; arithmetic exact".

## Why Teams Start Sampling and Dropping

**Tags:** `where it's used` (blue), `cost control` (green)

- **The review** — at $16,500 a month, finance asks why logging costs more than the database
- **Sampling** — keep 1 trace in 10; cost shrinks, and so does the chance the bad request was kept
- **Dropping** — debug logs, 40 of the 220 GB/day here, get filtered before the agent ships them
- **Tiering** — cheap archive tiers park bytes in object storage, searchable only after rehydration
- **The result** — index 60, archive 120, drop 40 GB/day: the bill falls to $5,400 a month

*Example (italic):* One config line dropping debug logs saves $3,000 a month (40 GB/day × 30 × $2.50) — the cheapest infra win of the quarter.

**Key point:** The pricing model shapes engineering behavior — once the meter runs per byte, teams spend real effort deciding which bytes are worth observing at all.

### Visualization (canvas `c3`, 720×300)

Two horizontal bars comparing the monthly bill before and after cost controls, the second broken into index / archive / dropped segments.

- **Title (bold 15px, `#1a5276`, top center):** "Trimming the Bill: Index Less, Archive More, Drop the Rest".
- **Layout:** row labels 12px `#444` left-aligned at x=20; bars start at x=230, height 26; scale 440px = $16,500 (26.67px per $1,000).
- **Row 1 (y=85), label "before: index all 220 GB/day":** single bar fill `rgba(42,120,214,0.35)`, 2px `#2a78d6` border, width 440; bold 12px `#2a78d6` label "$16,500/mo" at the bar's right end.
- **Row 2 (y=175), label "after: 60 index / 120 archive / 40 drop":** stacked segments — index $4,500 as `rgba(42,120,214,0.35)` width 120; archive $900 as `rgba(25,158,112,0.30)` width 24; then a dashed 2px `#6b7280` outline (no fill) width 296 labeled inside with 11px `#6b7280` "saved $11,100"; bold 12px `#008300` total label "$5,400/mo" at x=390.
- **Segment key (11px, under row 2 at y=225):** blue square + "indexed $4,500", aqua square + "archive $900 ($0.25/GB)", dashed square + "no longer billed".
- **Annotation (bold 13px green `#008300`, centered near y=260):** "same incidents debugged — 60 of 220 GB/day indexed".
- **Caption (12px `#444`, bottom right):** "prices illustrative; arithmetic exact".

## Cutting Volume Without Cutting Blindly

**Tags:** `common mistake` (red), `sampling` (orange)

- **The cut** — the team samples uniformly, keeping 1 request in 10 to shrink the bill
- **The math** — of 10,000 requests an hour, 100 fail; uniform 10% keeps ~10 failures (exact)
- **The incident** — the one request that corrupted an order has a 90% chance of never being stored
- **The fix** — sample the boring 9,900 successes, keep 100% of errors and slow requests
- **The cost** — errors are 1% of traffic, so keeping all of them barely moves the byte count

*Example (italic):* Keeping every error plus 5% of successes stores 595 requests an hour instead of 1,000 — cheaper AND every failure kept.

**Common mistake:** Shrinking the bill with a uniform sample. The whole point of observability is the rare bad event, and uniform sampling is precisely a machine for throwing rare events away.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: uniform sampling loses the failures, error-first sampling keeps them all — same input box on both rows.

- **Title (bold 15px, `#1a5276`, top center):** "Two Ways to Keep 1 in 10: Uniform Loses the Errors".
- **Row 1 (y=95), label 12px `#444` at x=20:** "uniform 10%"; blue `#2a78d6` rounded box at x=150 (170×44, fill `rgba(42,120,214,0.15)`) labeled "10,000 req/hr — 100 errors" (12px), 3px `#6b7280` arrow to a red `#e74c3c` box at x=430 (190×44, fill `rgba(231,76,60,0.12)`) labeled "keep 1,000 — ~10 errors" with bold 12px red "✗ 90% of failures gone" to its right/below.
- **Row 2 (y=205), label:** "errors first"; identical blue input box at x=150, 3px arrow to a green `#008300` box at x=430 (190×44, fill `rgba(0,131,0,0.12)`) labeled "keep 595 — all 100 errors" with bold 12px green "✓ every failure stored".
- **Box style:** 8px radius, 12px `#2c3e50` text, 2px borders matching each fill's hue.
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=270):** "sample the successes, never the failures".
- **Caption (12px `#444`, bottom right):** "100 errors + 5% of 9,900 successes = 595 kept — exact".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness). Splunk's per-GB-ingested licensing and Datadog's per-host / per-GB / per-feature pricing dimensions are publicly documented facts; every dollar figure ($2.50/GB ingest, $0.25/GB archive, all monthly bills) and every volume number is invented and labeled illustrative; the bill multiplications, the $11,100 saving, and the 595-of-10,000 sampling count are exact arithmetic on those illustrative inputs.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
