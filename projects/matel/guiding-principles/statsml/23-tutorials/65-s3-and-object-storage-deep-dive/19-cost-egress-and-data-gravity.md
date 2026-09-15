# Cost, Egress & Data Gravity

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Cost, Egress &amp; Data Gravity

**Subtitle:** Storage per GB is the line everyone estimates, and rarely the one that decides the bill — requests, reading bytes back, minimum billing periods, and the price of sending data out do that, and the last of them is why data does not move

## One Month's Bill for a 2,000 GB Dataset

**Tags:** `core idea` (blue), `worked example` (green), `illustrative rates` (orange)

- **The dataset** — 2,000 GB of sensor readings kept as 40,000 objects of 50 MB each
- **The traffic** — 40,000 new writes a month, and 5 million reads averaging 160 KB
- **The obvious estimate** — 2,000 GB at $0.0125 per GB a month is $25.00, and most stop there
- **Writes cost the most** — $0.005 per thousand requests, so the 40,000 writes cost $0.20
- **Reads cost far less** — $0.0004 per thousand, so 5 million reads add only $2.00
- **A fee to read bytes** — the cool tier charges $0.01 a GB, so the 800 GB read costs $8.00
- **Sending it out** — $0.09 a GB on the same 800 GB is $72.00, the biggest line of all
- **The real total** — $107.20, about 4.3 times what the per-GB estimate predicted

*Example (italic):* Storage is 23% of this bill and sending the data out is 67%, so an estimate built on the per-GB rate alone was four times too low.

**Key point:** Per-GB storage is one line out of five, and on a dataset that gets read it is not the tall one. Count requests, the per-GB read fee, and outbound transfer as real line items.

### Visualization (canvas `c1`, 720×300)

Two horizontal bars on one shared dollar scale: the actual bill split into its five lines, and beneath it the storage-only figure a per-GB estimate would have produced.

- **Constants (all dollar figures derive from these, computed at render time — never typed as strings):** `GB = 2000`, `OBJECTS = 40000`, `READS = 5000000`, `OUT_GB = 800`; rates `STORE = 0.0125` per GB-month, `WRITE = 0.005` per 1,000, `READ = 0.0004` per 1,000, `READ_FEE = 0.01` per GB, `SEND = 0.09` per GB.
- **Line items (name, formula, colour):** `storage` = `GB × STORE` = 25.00 blue `#2a78d6`; `writes` = `OBJECTS/1000 × WRITE` = 0.20 violet `#4a3aa7`; `reads` = `READS/1000 × READ` = 2.00 aqua `#199e70`; `read fee` = `OUT_GB × READ_FEE` = 8.00 yellow `#c98500`; `sending out` = `OUT_GB × SEND` = 72.00 orange `#d95926`. Total summed in JS = 107.20. The names are short so no legend entry runs into the next column.
- **Title (bold 15.5px, `#1a5276`, centred at y=24):** "One Month's Bill for 2,000 GB: Storage Is Not the Tall Slice".
- **Geometry:** plot x=170, width 480 mapped to the computed total, so px-per-dollar = 480/total; bar height 54; actual bar at y=78, estimate bar at y=152. Segment widths therefore sum to exactly 480 px.
- **Row labels (bold 12.5px, right-aligned at x=160, on each bar's centre line):** "actual bill" in `#1a5276`, "per-GB estimate" in `#6b7280`.
- **Segments:** 0.85 alpha fill with a 1.5px border in the same colour; a bold 12px white dollar label centred in any segment at least 40 px wide (storage and sending out qualify).
- **Estimate bar:** single blue block the width of the storage figure, 0.35 alpha, 1.5px border; one bold 12px blue label 10 px to its right reading "$25.00 — only 23.3% of the real bill", with both numbers computed.
- **Total label (bold 13px `#d95926`, right-aligned at (650, 70)):** the computed total plus " total".
- **Legend (five entries, rows at y=220 and y=242, columns at x=100, 290, 480):** 11 px colour swatch then 12px `#2c3e50` "name $amount (share%)" using the short names above; every share computed as `amt/total × 100`.
- **Annotation (bold 13px magenta `#d55181`, centred at y=272):** "sending it out is 67.2% of the bill — the per-GB estimate missed 4.3×", both figures computed.
- **Caption (12px `#444`, right-aligned at y=294):** "rates illustrative; " then the five amounts joined with " + " and the computed total.

## Deleting from the Archive Tier on Day 9

**Tags:** `counterintuitive` (red), `minimum billing period` (green), `worked example` (orange)

- **The minimums** — 30 days in the cool tier, 90 in the cold one, 180 in deep archive
- **The rule** — delete early and the unused days are still billed, with no refund
- **The scenario** — 500 GB in 10,000 objects goes to deep archive, deleted on day 9
- **Storage billed** — 500 GB × $0.00099 × 180/30 = $2.97, six months for nine days
- **The move itself** — 10,000 objects at $0.05 per thousand is $0.50, paid up front
- **Reading it back** — $0.02 a GB on 500 GB is $10.00, since archives charge to read
- **The comparison** — $13.47 archived against $3.45 at the $0.023 hot rate: 3.9× more
- **The crossover** — on storage alone the hot tier wins below about 7.7 days of life

*Example (italic):* $13.47 for 500 GB held nine days works out to $0.0898 per GB a month, nearly four times the $0.023 hot rate the archive was supposed to beat.

**Key point:** Archiving short-lived data costs more than leaving it where it was. The cold rate only wins once the object outlives the minimum billing period, so rules that move data down after days raise the bill.

### Visualization (canvas `c2`, 720×300)

Log-log line chart: the total paid to hold 1 GB for a given number of days, one line per tier, each point computed at render time as `rate/30 × max(days, minimum)`. The minimum billing periods show up as flat left-hand segments.

- **Tiers (name, monthly rate per GB, minimum days, colour):** hot 0.023 / 0 / blue `#2a78d6`; cool 0.0125 / 30 / aqua `#199e70`; cold 0.0036 / 90 / yellow `#c98500`; deep archive 0.00099 / 180 / violet `#4a3aa7` — the same four the first bullet names.
- **Title (bold 15.5px, `#1a5276`, centred at y=22):** "Cost to Keep 1 GB for N Days: Cold Tiers Bill a Minimum Period" — kept to roughly 62 characters so it fits inside 720 px at this weight.
- **Axes:** x is log10 days from 3 to 365 mapped to x=80..660; y is log10 dollars from 0.002 to 0.3 mapped to y=250..70; both axis lines 1.5px `#1a5276`. Every plotted position comes from the two mapping functions, never a literal pixel.
- **X ticks (12px `#6b7280`, centred at y=264):** 3, 7, 15, 30, 90, 180, 365, each with a 1px `#e5e9ef` vertical gridline.
- **Y ticks (12px `#6b7280`, right-aligned at x=72):** $0.002, $0.005, $0.01, $0.02, $0.05, $0.10, $0.20 with 1px `#e5e9ef` horizontal gridlines.
- **Lines (2.5px, sampled at [3,4,5,6,7,8,9,10,12,15,20,25,30,40,60,90,120,180,250,365]):** the hot tier rises straight from $0.0023 at three days; the other three sit flat at `rate/30 × minimum` until their minimum day, then rise.
- **Series labels (bold 12px in the line colour, right-aligned at x=656, 8 px above each line's 365-day endpoint — computed, so they land at y≈65, 86, 131 and 178 and cannot collide):** "hot tier", "cool tier", "cold tier", "deep archive".
- **Crossover marker:** the day count where the hot tier reaches the deep-archive floor, computed as `floor × 30 / hotRate` = 7.7 (x≈195, y≈211); a 1.5px dashed (4/4) `#e74c3c` vertical from the axis up to that cost, a 4px `#e74c3c` dot there, and a bold 12.5px `#e74c3c` label left-aligned at (110, 96) reading "below 7.7 days the hot tier is cheaper than deep archive" with the 7.7 computed. No leader line to the dot — a vertical one would cross the two flat tier lines at y≈184 and y≈189. The label ends near x=480, where the hot line is still 90 px below it.
- **Flat-segment note (bold 12px `#6b7280`, left-aligned at (230, 244)):** "flat = you pay the minimum days even if you delete sooner" — starts right of the dashed crossover line, and sits below the hot line, which is at y≈200 there.
- **Axis title (bold 12.5px `#2c3e50`, left-aligned at (80, 286)):** "days the object actually lives" — left-aligned on the caption's own baseline, ending near x=278 while the right-aligned caption starts near x=336.
- **Caption (12px `#444`, right-aligned at y=286):** "rates illustrative; each point = rate/30 × max(days, minimum)".

## Sending the Data Out Costs More Than Keeping It

**Tags:** `data gravity` (blue), `sending data out` (green), `rates are moving` (orange)

- **Coming in is free** — uploading the 2,000 GB costs nothing beyond the write requests
- **Going out is not** — at $0.09 a GB, sending all 2,000 GB out costs $180.00 one time
- **Another region counts** — copying it to a second region bills $0.02 a GB, so $40.00
- **Bytes cost, requests do not** — reading all 40,000 objects is $0.016 in requests
- **In months of storage** — that $180.00 is 3.9 months of the $46.00 hot monthly bill
- **Colder makes it worse** — against $1.98 a month archived, $180.00 buys 91 months
- **Why data stays put** — when leaving costs months of staying, the compute moves instead
- **Keep it nearby** — run the compute in the same region and cache heavy reads at the edge

*Example (italic):* Storing the whole 2,000 GB for a full quarter costs $138.00 — still less than the $180.00 it costs to send those same bytes out once.

**Key point:** Data gravity is a ratio, not a metaphor. When leaving costs several months of staying, the dataset stays where it was born and the compute moves to it — and treat the ratio as directional, because exit fees are being cut.

### Visualization (canvas `c3`, 720×300)

Cumulative storage cost climbing month by month, with the one-time cost of sending the data out drawn as a flat threshold; where the two meet is the "months of storage equal one exit" figure.

- **Constants:** `GB = 2000`; rates hot 0.023, archived 0.00099, sending out 0.09, another region 0.02 — so `hotMonth = 46.00`, `archMonth = 1.98`, `sendOut = 180.00`, `otherRegion = 40.00`, every one multiplied in JS.
- **Title (bold 15.5px, `#1a5276`, centred at y=22):** "One Exit vs Months of Storage: 2,000 GB Costs $180.00 to Send Out", with the dollar figure computed into the string.
- **Axes:** x is months 0..6 mapped to x=80..660; y is dollars 0..300 mapped to y=250..70; axis lines 1.5px `#1a5276`.
- **X ticks (12px `#6b7280`, centred at y=264):** 0..6 with 1px `#e5e9ef` gridlines. **Y ticks (right-aligned at x=72):** $0, $60, $120, $180, $240, $300 with gridlines.
- **Hot cumulative line (2.5px blue `#2a78d6`, 4px dot per month):** `hotMonth × month`, so 0 … 276 at month 6; bold 12px blue label right-aligned at (x(5.2), y(hotMonth×5.2) − 12) reading "hot storage, $46.00/mo adding up".
- **Archived cumulative line (2.5px violet `#4a3aa7`, 4px dots):** `archMonth × month`, near flat on the baseline; bold 12px violet label right-aligned at (656, y(archMonth×6) − 10) reading "archived, $1.98/mo".
- **Send-out threshold (2.5px dashed (6/4) orange `#d95926` horizontal line at `sendOut`):** bold 12.5px orange label left-aligned at (88, y(sendOut) − 10) reading "sending it all out, one time: $180.00".
- **Other-region threshold (1.5px dashed (4/4) yellow `#c98500` at `otherRegion`):** bold 12px yellow label left-aligned at (88, y(otherRegion) − 8) reading "copy to another region: $40.00".
- **Intersection:** month = `sendOut / hotMonth` = 3.913 (x≈458); a 5px `#e74c3c` dot on the threshold there, a 1.5px dashed `#e74c3c` drop to the x-axis, and a bold 13px `#e74c3c` label **right-aligned at x=656 on baseline y(sendOut) + 30** reading "3.9 months of storage = one exit", the 3.9 computed. Below the threshold, not above, so it clears the rising hot line and the hot label; right-aligned so it cannot run past the canvas edge.
- **Axis title (bold 12.5px `#2c3e50`, left-aligned at (80, 286)):** "months the dataset simply sits there", ending near x=300.
- **Caption (12px `#444`, right-aligned at y=286):** "rates illustrative; 2,000 × $0.09 = $180.00 and 180/46 = 3.9", built from the constants — kept short so its left edge (x≈354) clears the axis title.

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `23-tutorials/CLAUDE.md`), matching the sibling `11-cross-region-replication.html` in this folder. Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then **three** `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links, no cross-page links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms). Keep the `roundRect` and `rgba` helpers.
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Red `#e74c3c` appears only on the two genuine bad outcomes: the early-deletion crossover in `c2` and the exit-threshold intersection in `c3`.
- **Bullet length — this is the deliberate calibration of the page.** Eight bullets per section, each roughly **75–90 characters** including the bold lead term, so it holds one line at the 50/50 split while still carrying a full clause. An earlier pass clipped them to ~55–70 characters and was rejected for compromising quality; do **not** pad them out to the folder's ~90–100 default either. The measured range here is 76–88.
- **Register.** Plain simple technical English for someone meeting cost models for the first time. No analogies, no invented scenes. Little API or product surface: say "writes" and "reads", not GET/PUT/LIST; say "hot tier", "cool tier", "cold tier", "deep archive", not the vendor storage-class names; say "sending data out", not egress-as-jargon (the word appears in the page title only). Cut detail rather than cut facts — a tutorial, not a pricing reference.
- **Every figure is computed at render time.** No dollar amount, total, percentage, or multiple is typed into a chart as a string. Each chart declares named quantity and rate constants and builds every label from them with `toFixed`, so text, example and chart cannot drift. The bullets state the same arithmetic in the same order so it can be checked by hand.
- **Deliberate omissions — do not reintroduce on regeneration.**
  - **No object-count section.** A fourth section priced the same 2,000 GB as 40,000 objects versus 20 million (the 128 KB minimum billable size, per-object monitoring, per-object transition requests, archive per-object metadata, a 41.6× total, `c4`'s two stacked bars). It was cut: it is about key and object layout rather than the cost drivers this page teaches, its chart could not show its own data (the small bar was 4 px tall and needed a callout apologising for it), and its bullets listed more charges than its total summed. That material belongs with the object-layout pages (`02-directories-are-a-lie`, `05-hot-spots-and-key-design`).
  - **No pricing material pulled in from sibling pages.** Several siblings had their cost sections cut with a note pointing here; that was to shorten *them*, not to grow this page. This page stays at three sections.
  - **No second crossover marker in `c2`.** The cool-tier-versus-hot crossover at 16.3 days was dropped: its label sat directly on the hot line, and one crossover teaches the point.
  - **No free-allowance detail, no free-egress-on-account-closure detail, no per-service network levers.** The bullet "the rates are moving" plus a directional caveat in the key point is the whole treatment.
- **Illustrative rates used throughout (not quoted prices; labelled illustrative in every caption):** hot $0.023/GB-mo, cool $0.0125/GB-mo, cold $0.0036/GB-mo, deep archive $0.00099/GB-mo; writes $0.005 per 1,000; reads $0.0004 per 1,000; cool-tier read fee $0.01/GB; archive read fee $0.02/GB; move-between-tiers request $0.05 per 1,000 objects; sending out $0.09/GB; another region $0.02/GB.
- **Computed and verifiable by hand:** 2,000 × 0.0125 = 25.00; 40 × 0.005 = 0.20; 5,000 × 0.0004 = 2.00; 800 × 0.01 = 8.00; 800 × 0.09 = 72.00; total 107.20 with shares 23.3 / 0.2 / 1.9 / 7.5 / 67.2 % and 107.20 / 25.00 = 4.29. The 5 million reads average 160 KB, and 5,000,000 × 160 KB = 800 GB, which is the same 800 GB the read fee and the send-out line are charged on. 500 × 0.00099 × 6 = 2.97; 10 × 0.05 = 0.50; 500 × 0.02 = 10.00; total 13.47; nine days hot = 500 × 0.023 × 9/30 = 3.45; 13.47 / 3.45 = 3.90; effective rate 13.47 / 500 × 30/9 = 0.0898 per GB-month and 0.0898 / 0.023 = 3.90. Deep-archive floor 0.00099/30 × 180 = 0.00594 per GB, so the hot tier reaches it at 0.00594 × 30 / 0.023 = 7.75 days. 2,000 × 0.09 = 180.00; 2,000 × 0.02 = 40.00; 2,000 × 0.023 = 46.00; 180 / 46 = 3.91 months; 46 × 3 = 138.00 for a quarter, under 180.00; 2,000 × 0.00099 = 1.98 and 180 / 1.98 = 90.9 ≈ 91 months.
- **Documented structural facts (not illustrative):** requests are priced per thousand and writes cost far more than reads; the infrequent-access and archive tiers add a per-GB fee to read bytes back, which the hot tier does not have; minimum billing periods of 30, 90 and 180 days apply to those tiers, and deleting early still bills the remaining days; moving objects between tiers is billed as a request per thousand objects; inbound transfer is free while outbound and cross-region transfer are priced per GB.
- **Framing caveat:** exit pricing is stated as directional, because data-portability rules and competition have pushed providers to cut or drop it. No vendor is named in any price claim.
- **Chart geometry:** derived from mapping functions (`xOf`, `yOf`, px-per-dollar), never hardcoded pixels. Two collision fixes are load-bearing and must survive regeneration — the x-axis titles in `c2` and `c3` are **left-aligned at x=80 on the same baseline as the right-aligned caption** (centred titles collided with the caption), and `c3`'s intersection label sits **below** the threshold line (above, it grazed the rising blue line).
- **Data:** all values are hardcoded literal constants. Never `Math.random()`.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
