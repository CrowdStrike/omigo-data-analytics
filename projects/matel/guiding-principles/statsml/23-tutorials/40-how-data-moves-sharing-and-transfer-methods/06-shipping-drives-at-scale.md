# Shipping Drives at Scale

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Shipping Drives at Scale

**Subtitle:** "Never underestimate the bandwidth of a station wagon full of tapes" — for petabytes, a shipped box of drives still beats the network

## Alice's Petabyte: The Box Beats the Wire

**Tags:** `core idea` (blue), `sneakernet` (green)

- **The job** — Alice must move 1 PB of telescope images from her lab to Bob's datacenter
- **The wire** — her 1 Gbps line, running perfectly nonstop, needs about 93 days for the petabyte
- **The box** — she copies the data onto a case of drives and hands it to a courier: ~2 days
- **Sneakernet** — the old joke name for moving data by physically carrying the storage
- **Old truth** — "never underestimate the bandwidth of a station wagon full of tapes"

*Example (italic):* Alice's upload would still be running in November; the shipped box is at Bob's dock on Thursday.

**Key point:** Past a certain data size, the fastest route for bits is a courier van — shipping the storage beats sending the data down a wire.

### Visualization (canvas `c1`, 720×300)

Horizontal bar chart: days to move 1 PB by three methods, linear axis.

- **Title (bold 15px, `#1a5276`, top center):** "Moving 1 PB: Wire vs Box (days)".
- **Axis:** 1px `#999` vertical baseline at x=215; x scale 0–100 days mapped to x=215…685; vertical `#e5e9ef` gridlines with 12px `#444` tick labels "0 / 25 / 50 / 75 / 100 days" at y=262.
- **Rows (bars 34px tall), each with a right-aligned bold 13px `#2c3e50` label at x=205:**
  - y=78 "1 Gbps line": orange `#d95926` bar to 93 days (x 215→652), bold 12px orange value "93 d" right of the bar end
  - y=142 "10 Gbps line": yellow `#c98500` bar to 9.3 days (x 215→259), 12px yellow value "9.3 d" right of the bar end
  - y=206 "shipped box of drives": green `#008300` bar to 2 days (x 215→224), bold 12px green value "~2 d" right of the bar end
- **Bar style:** fills at 80% opacity with 1.5px same-hue borders.
- **Annotation (bold 13px green `#008300`, centered near x=450, y=245):** "the box arrives with the upload barely 2% done".
- **Caption (12px `#6b7280`, bottom right):** "1 PB = 8,000,000 gigabits; courier time illustrative".

## Do the Arithmetic: 93 Days vs 2 Days

**Tags:** `worked example` (blue), `redo by hand` (orange)

- **Convert** — 1 PB = 1,000 TB = 8,000,000 gigabits, because every byte is 8 bits
- **Divide** — 8,000,000 gigabits / 1 gigabit per second = 8,000,000 seconds on the wire
- **To days** — 8,000,000 / 86,400 seconds per day ≈ 93 days of flawless, nonstop transfer
- **The box** — courier delivery is ~2 days = 172,800 seconds, whatever the data size
- **Effective speed** — 8,000,000 Gb / 172,800 s ≈ 46 Gbps: the box outruns the wire 46 to 1
- **Crossover** — below ~22 TB the wire wins; above it, the 2-day box is already faster

*Example (italic):* Check the crossover: 22 TB × 8,000 seconds per TB ≈ 176,000 s ≈ 2 days — right where the lines meet.

**Key point:** Network time grows with data size; shipping time barely does — so some size always exists past which the box wins.

### Visualization (canvas `c2`, 720×300)

Log-log crossover chart: transfer time (days) vs data size (TB) for the 1 Gbps line and the shipped box.

- **Title (bold 15px, `#1a5276`, top center):** "Transfer Time vs Data Size: The Lines Cross (log-log)".
- **Axes:** 1px `#999` L-frame, plot area x=70…560, y=55…235; x log scale 1→1000 TB with `#e5e9ef` gridlines and 12px `#444` ticks at "1 / 10 / 100 / 1,000 TB"; y log scale 0.05→100 days with gridlines and ticks at "0.1 / 1 / 10 / 100 days"; 12px `#444` axis captions "data size (TB, log)" bottom center and rotated "days (log)" at left.
- **Network line (blue `#2a78d6`, 3px):** y = 0.0926 × x days; hardcoded points at x = [1, 2, 5, 10, 22, 50, 100, 200, 500, 1000] TB, y = [0.093, 0.185, 0.463, 0.926, 2.04, 4.63, 9.26, 18.5, 46.3, 92.6] days (straight on log-log).
- **Shipping line (green `#008300`, 3px, dashed 7/4):** flat at 2 days across the full x range.
- **Crossover marker:** 6px green-filled dot with white 2px ring at (21.6 TB, 2 days).
- **Annotation (bold 13px green `#008300`, near the dot, above-right):** "~22 TB: lines cross — bigger than this, ship it".
- **Legend (top-right of plot, 12px):** blue swatch "1 Gbps network", green swatch "shipped drives (~2 days flat)".
- **Caption (12px `#6b7280`, bottom right):** "courier time illustrative; copy time at each end ignored".

## Cloud Transfer Appliances: Sneakernet, Productized

**Tags:** `where it's used` (blue), `transfer appliances` (green)

- **Productized** — cloud vendors courier you a rugged box of drives; you fill it and ship it back
- **Scale tiers** — pocket drives, then multi-drive appliances, then a literal truck for PB moves
- **Who ships** — datacenter migrations, genomics archives, and film libraries move by box
- **Encrypted** — appliance drives travel encrypted, so a lost box leaks nothing readable
- **Data science** — "just download the training set" quietly fails once the set is 500 TB

*Example (italic):* A lab's 400 TB archive reached the cloud in about a week by appliance, versus over a month of a saturated 1 Gbps line.

**Key point:** Transfer appliances are the truck-of-drives idea sold as a service — the cloud's own answer to "the network is too slow."

### Visualization (canvas `c3`, 720×300)

Horizontal decision band on a log data-size axis: which transfer method wins at each size.

- **Title (bold 15px, `#1a5276`, top center):** "Which Method Wins, by Data Size (illustrative)".
- **Axis:** log scale 0.1→10,000 TB mapped to x=60…670; 1px `#999` baseline at y=200 with 8px tick marks and 12px `#444` labels at "0.1 / 1 / 10 / 100 / 1,000 / 10,000 TB"; 12px `#444` axis caption "data size (TB, log scale)" at y=245.
- **Band (y=130…200, three segments, fills at 25% opacity with 2px same-hue borders, bold 13px same-hue centered segment labels):**
  - 0.1→22 TB: blue `#2a78d6`, label "network"
  - 22→1,000 TB: green `#008300`, label "shipped appliance"
  - 1,000→10,000 TB: orange `#d95926`, label "truck-scale container"
- **Typical-time notes (12px same-hue, centered above each segment at y=118):** "hours–days" / "about a week" / "weeks — but it finishes".
- **Boundary markers:** dashed `#6b7280` 1px verticals at 22 TB and 1,000 TB rising to y=70, with 11px `#6b7280` labels "~22 TB (1 Gbps crossover)" and "1 PB".
- **Annotation (bold 13px ink `#1a5276`, centered at y=50):** "regions are illustrative — a faster line pushes the crossover right".

## The Confusion: Bandwidth Is Not Latency

**Tags:** `common mistake` (red), `bandwidth vs latency` (orange)

- **Bandwidth** — how much arrives per second once flowing; the box delivers ~46 Gbps effective
- **Latency** — how long the first byte takes to arrive; for the box, that is two whole days
- **The truck** — terrible latency, unbeatable bandwidth; the fiber line is the exact reverse
- **Wrong job** — never serve a live dashboard by courier; never bulk-move a petabyte by wire
- **Rule** — latency-critical work rides the network; volume-critical work rides the box

*Example (italic):* Bob's ping answers in 50 milliseconds; the box "answers" in 2 days — but carries a petabyte with it.

**Key point:** "Fast" means two different things — the wire and the truck each win one of them, and picking the wrong one wastes months.

### Visualization (canvas `c4`, 720×300)

Log-log scatter map: latency (x) vs effective bandwidth (y); every transfer method is one point.

- **Title (bold 15px, `#1a5276`, top center):** "Latency vs Bandwidth: Each Method Is a Point on This Map".
- **Axes:** 1px `#999` L-frame, plot area x=80…670, y=55…225; x log scale 1 ms→10^9 ms with `#e5e9ef` gridlines and 12px `#444` ticks at "1 ms" (10^0), "1 s" (10^3), "17 min" (10^6), "12 days" (10^9); y log scale 0.1→100 Gbps with gridlines and ticks at "0.1 / 1 / 10 / 100"; 12px `#444` captions "time to first byte (log)" bottom center and rotated "effective Gbps (log)" at left.
- **Points (8px filled dots, bold 12px same-hue labels beside each):**
  - "1 Gbps fiber" blue `#2a78d6` at (50 ms, 1 Gbps), label to the right
  - "10 Gbps leased line" aqua `#199e70` at (50 ms, 10 Gbps), label to the right
  - "shipped box of drives" orange `#d95926` at (172,800,000 ms ≈ 2 days, 46 Gbps), label below-left
- **Guides:** dashed `#6b7280` 1px lines from the box point down to the x-axis and left to the y-axis.
- **Annotation (bold 13px orange `#d95926`, centered under the title at y=48):** "the truck: days of latency, 46x the fiber's bandwidth".
- **Caption (12px `#6b7280`, bottom right):** "box figures from the 1 PB worked example; illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row (classes blue/green/red/orange), `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>Key point:</strong>` label).
- **Charts:** each canvas 720×300 logical, `devicePixelRatio`-scaled via a shared `setup(id)` helper that reads the element's own width/height attributes, CSS `width:100%`; all data hardcoded literal arrays, no `Math.random()`; redraw all charts on window resize (debounced 150 ms).
- **Palette:** `const P = {blue:'#2a78d6', green:'#008300', magenta:'#d55181', yellow:'#c98500', aqua:'#199e70', orange:'#d95926', violet:'#4a3aa7', ink:'#1a5276', text:'#2c3e50', mute:'#6b7280', grid:'#e5e9ef'}`; navy ink for headings and axes; red reserved for error states (none on this page).
- **Fonts:** chart titles bold 15px, axis/data labels 12px, annotations bold 13px, nothing below 11px.
