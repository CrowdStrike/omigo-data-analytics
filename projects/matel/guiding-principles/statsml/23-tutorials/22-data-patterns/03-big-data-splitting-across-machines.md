# Big Data: Splitting Across Machines

**Page type:** detail page (tutorial topic page: `.card-section` blocks, each h2 + two-column `table.layout` — text left 50%, canvas right 50%)
**HTML title tag:** Big Data: Splitting Across Machines

**Subtitle:** When no single machine can hold or process the data, the work gets divided across many machines that share the job

## The Video Site's Logs Outgrow Every Single Machine

**Tags:** `core idea` (blue), `running example` (green)

- **A video site's logs** — every click and play is one log line; 500 GB of new lines a day
- **A year later** — 180 TB of logs; the biggest single disk here holds 2 TB
- **No machine fits it** — 180 TB is 90 disks' worth; one computer simply cannot hold it
- **So split it** — spread the logs over 100 machines, about 1.8 TB each
- **That is big data** — data so large that storing and processing it must be shared

*Example:* No shelf holds 180 encyclopedias' worth of paper — so the library uses 100 shelves.

**Key point:** Big data starts where one machine ends — the defining move is splitting the data across many machines.

### Visualization (canvas `c1`, 720×300)

Two-panel diagram: one giant data pile vs a 10×10 grid of machines, split by a dashed vertical divider at x=340 (`#e5e9ef`, dash 4/3).

- **Title (bold 16px, `#1a5276`, top center):** "180 TB of Logs: One Machine vs One Hundred".
- **Left panel:** large rectangle (x=60, y=60, 190×140), fill `rgba(42,120,214,0.18)`, stroke blue `#2a78d6` width 2; centered bold 13px blue labels inside: "one year of logs" / "180 TB". Below it, a small orange `#d95926` box 34×26 (area ~1/90 of the pile) labeled in 12px `#2c3e50` "one machine: 2 TB"; then orange bold 13px "90x too small".
- **Right panel:** 10×10 grid of green `#008300` squares (15px cells, 3px gaps) starting at x=390, y=62. Right of the grid, bold 13px `#2c3e50`: "100 machines," / "~1.8 TB each". Below the grid, green bold 13px centered: "together: 180 TB — the logs fit".

## Split, Count, Combine: 12 Log Lines by Hand

**Tags:** `worked example` (green), `core idea` (blue)

- **The question** — how many times was the homepage visited?
- **Split** — deal 12 log lines to 3 workers, 4 lines each, like dealing cards
- **Count** — each worker counts "/home" in its own pile: worker A: 3, B: 2, C: 4
- **Combine** — one coordinator adds the three partial counts: 3 + 2 + 4 = 9 visits
- **Same answer** — exactly what one person counting all 12 lines would get
- **Scale it up** — the same recipe works when the piles are billions of lines each

*Example:* Three friends each count their own pile of receipts, then shout out totals to add up.

**Key point:** Split the data, do the same small job on each piece, combine the partial answers — that recipe is the heart of every big data system.

### Visualization (canvas `c2`, 720×300)

Flow diagram: source box → three worker boxes → coordinator box, connected by muted `#6b7280` arrows with filled arrowheads.

- **Title (bold 16px, `#1a5276`, top center):** "Counting \"/home\" Visits: Split, Count, Combine".
- **Source box:** x=40, y=110, 130×56, fill `rgba(42,120,214,0.14)`, stroke blue `#2a78d6`; two lines of bold 12px text: "12 log lines" / "(the whole day)".
- **Worker boxes:** three boxes at x=280, 160×46, fill `rgba(0,131,0,0.12)`, stroke green `#008300`, at y=52 / 122 / 192:
  - "worker A: 4 lines" / "finds /home x 3"
  - "worker B: 4 lines" / "finds /home x 2"
  - "worker C: 4 lines" / "finds /home x 4"
- **Coordinator box:** x=540, y=110, 140×56, fill `rgba(74,58,167,0.12)`, stroke violet `#4a3aa7`; text "coordinator" / "3 + 2 + 4 = 9".
- **Stage labels (muted 12px):** "split" at (224, 100) and "combine" at (490, 100).
- **Caption (violet bold 13px, bottom center):** "same answer as one person counting all 12 lines".

## What 100 Machines Buy You — and What They Don't

**Tags:** `where it's used` (blue), `trade-off` (orange)

- **One machine** — scanning all 180 TB alone would take about 100 hours
- **100 machines** — each scans its own 1.8 TB slice at the same time: ~1.4 hours
- **Not quite 100x** — splitting, coordinating, and combining eat part of the gain
- **Failures are normal** — with 100 machines, one dying mid-job is routine, not rare
- **Systems must retry** — the coordinator re-runs a dead worker's slice automatically

*Example:* Overnight, machine 37 died; its 1.8 TB slice was quietly redone elsewhere and the report still landed by morning.

**Key point:** Many machines buy speed and capacity, but the system must now plan for coordination overhead and routine failures.

### Visualization (canvas `c3`, 720×300)

Bar chart of wall-clock scan time vs number of machines, with dashed ideal markers.

- **Title (bold 16px, `#1a5276`, top center):** "Hours to Scan All 180 TB vs Machines Used".
- **Data:** machines `['1', '10', '50', '100']`; actual hours `[100, 10.4, 2.4, 1.4]` (illustrative: 100/n plus overhead); ideal hours `[100, 10, 2, 1]`.
- **Axes:** padding top 55, bottom 58, left 80, right 40; y scale max 110; baseline in muted `#6b7280`; bar width 90, equal gaps; blue `#2a78d6` bars; bold 12px value labels "100 h", "10.4 h", "2.4 h", "1.4 h" above bars; machine counts 12px below.
- **Ideal markers:** dashed orange `#d95926` horizontal segments (dash 4/3, width 2) spanning each bar at the ideal-hours height.
- **Annotations:** muted 12px x-caption "machines working in parallel (illustrative)"; orange bold 13px "dashes = perfect split; the gap is coordination overhead"; blue bold 13px "100 machines: ~70x faster, not 100x".

## The Confusion: Not Every Job Splits Cleanly

**Tags:** `common mistake` (red), `trade-off` (orange)

- **Easy jobs** — counting and summing: each worker uses only its own pile
- **Hard jobs** — joining logs to the users table: matching rows sit on different machines
- **The shuffle** — workers must mail rows to each other over the network first
- **Network is slow** — moving data between machines costs far more than reading it
- **Design rule** — put rows that will be matched together on the same machine up front

*Example:* The count finished in minutes; the join spent most of its time just moving rows between machines.

**Key point:** Splitting is free only when workers never need each other's data — the moment they do, the network becomes the bottleneck.

### Visualization (canvas `c4`, 720×300)

Two-panel machine diagram: local count vs all-to-all join shuffle, split by a dashed vertical divider at x=360 (`#e5e9ef`, dash 4/3).

- **Title (bold 16px, `#1a5276`, top center):** "Why a Count Is Easy and a Join Is Not".
- **Machine boxes:** 70×44 rectangles, fill `#f4f7fa`, 2px colored stroke, bold 12px centered labels "M1", "M2", "M3".
- **Left panel (green `#008300`):** heading bold 13px "COUNT: each pile stays put"; three green-stroked machine boxes at x=55/155/255, y=90, each with a small green self-loop arc arrow beneath it. Captions: green bold 13px "zero network traffic"; muted 12px "each worker only reads its own disk".
- **Right panel (orange `#d95926`):** heading bold 13px "JOIN: rows must move first"; three orange-stroked machine boxes at x=420/520/620, y=90; orange quadratic curves with arrowheads on both ends connecting every pair of boxes below them (adjacent pairs dip 34px, the outer pair dips 60px). Captions: orange bold 13px "the shuffle: every machine mails rows to every other"; muted 12px "network moves cost far more than disk reads".
- **Bottom caption (ink `#1a5276` bold 13px, center):** "the split is only free while workers never need each other's data".

## Regeneration instructions

- **Layout:** tutorial topic page. `<h1>` (no index number) with 2px `#2980b9` bottom border, `.subtitle` paragraph, then four `.card-section` blocks. Each section: `<h2>` (1.3rem, `#1a5276`, 2px `#2980b9` bottom border) followed by `table.layout` (one `<tr>`; left `td.text-col` 50% width, right `td.viz-col` 50% width, cells padded 12px, no cell borders).
- **Text column structure:** `.tags` row of colored pill spans (`.tag` — 0.72rem, weight 600, padding 2px 10px, radius 10px; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`); then a `<ul>` (0.92rem) of one-line bullets each starting with `<b>` in `#1a5276`; one italic `.example` paragraph (`#555`, 0.9rem); one `.key-point` callout (background `#f8f9fa`, left border 3px solid `#e74c3c`, padding 8px 12px, 0.9rem) opening with `<strong>Key point:</strong>`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; universal `* { margin:0; padding:0; box-sizing:border-box; }`; h1 2rem `#1a5276`; subtitle `#666` 0.95rem. No nav bar, no back/home links. In regenerated HTML, any card links use `.html` extensions.
- **Canvas:** each canvas declared `width="720" height="300"`, CSS `width:100%`, border `1px solid #e0e0e0`, radius 4px; a shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette (JS object `P`):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
