# Stream Joins

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Stream Joins

**Subtitle:** Joining two streams that never finish — you can't wait for "all" rows, so every join becomes a bet about how far apart matching events can be

## Joining Clicks to Ads That Never Stop Arriving

**Tags:** `core idea` (blue), `windowed join` (green), `infinite data` (orange)

- **Two streams** — an ad platform emits impressions (ad shown) and clicks (ad clicked) as separate endless feeds
- **The goal** — pair each click with the impression that caused it, to bill the advertiser
- **The batch trap** — a database join waits for both tables to be complete; a stream is never complete
- **The bet** — declare that a click belongs to an impression only if it arrives within 30 minutes of it
- **The window** — the join buffers each impression for 30 minutes, matches clicks against the buffer, then lets go

*Example (italic):* Impression A fires at 2:00pm and its click lands at 2:17pm — matched; impression B fires at 2:05pm but its click arrives at 2:46pm, 41 minutes later — the window has closed and the pair is lost.

**Key point:** A stream join cannot wait for all rows, so it matches events within a time window — the window size is a bet about how far apart a real pair can be.

### Visualization (canvas `c1`, 720×300)

Two-row timeline: each row is one impression with its 30-minute window drawn as a shaded band; a click dot lands inside the band (matched) or after it (missed).

- **Title (bold 15px, `#1a5276`, top center):** "The 30-Minute Bet: a Click Inside the Window Matches, Outside It Is Lost".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 170; x = wall-clock time "2:00pm" to "2:50pm", 12px `#444` tick labels every 10 minutes; no y axis, two horizontal lanes at y=110 (impression A) and y=190 (impression B), lane labels 12px `#444` at x=15.
- **Window bands:** for A, shaded rect `rgba(42,120,214,0.15)` from 2:00 to 2:30 on lane A, 30px tall; for B, same fill from 2:05 to 2:35 on lane B; each band's right edge a vertical dashed `#6b7280` line (dash 4/3) with 11px `#6b7280` label "window closes".
- **Impression markers:** blue `#2a78d6` filled squares (10px) at 2:00 (lane A) and 2:05 (lane B), bold 12px blue labels "impression".
- **Click markers:** green `#008300` filled circle (r=6) at 2:17 on lane A with bold 12px green label "click +17 min ✓"; red `#e74c3c` filled circle at 2:46 on lane B with bold 12px red label "click +41 min ✗ lost".
- **Annotation (bold 13px violet `#4a3aa7`, near x=2:38, y=70):** "same click behavior, different fate — the window decides".
- **Caption (12px `#444`, bottom right):** "times illustrative".

## Ten Clicks, One Cutoff, Counted by Hand

**Tags:** `worked example` (blue), `match rate` (green)

- **The sample** — 10 impressions each get exactly one click; the click delays in minutes are 1, 3, 6, 9, 14, 19, 24, 28, 36, 52
- **The rule** — a 30-minute window keeps a delay if delay ≤ 30, drops it otherwise
- **Hand-check** — delays 1 through 28 pass (8 clicks); delays 36 and 52 fail (2 clicks)
- **The score** — 8 of 10 pairs match: an 80% join match rate under the 30-minute bet
- **The lost revenue** — the 2 unmatched clicks are real clicks the advertiser is never billed for

*Example (italic):* The click that took 36 minutes missed the cutoff by 6 minutes — one slow phone on a train platform costs the join a match.

**Key point:** The window turns a join into a yes/no test on the time gap — with these 10 delays, a 30-minute window matches exactly 8 pairs and silently drops 2.

### Visualization (canvas `c2`, 720×300)

Vertical bar chart of the 10 click delays with a horizontal cutoff line at 30 minutes; bars at or below the line are green (matched), bars above are red (missed).

- **Title (bold 15px, `#1a5276`, top center):** "10 Click Delays vs the 30-Minute Cutoff: 8 Matched, 2 Missed".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = click index 1–10, 12px `#444` labels under each bar; y = delay in minutes 0 to 60, gridlines `#e5e9ef` at 15/30/45, 12px `#444` tick labels.
- **Bars:** widths 40px, gaps 18px; heights from the hardcoded delays `[1, 3, 6, 9, 14, 19, 24, 28, 36, 52]`; fill green `rgba(0,131,0,0.30)` with 2px `#008300` top edge for delays ≤ 30, red `rgba(231,76,60,0.25)` with 2px `#e74c3c` top edge for 36 and 52; 11px value labels above each bar.
- **Cutoff line:** horizontal dashed `#d95926` (dash 6/4, 2px) at y for 30 minutes, bold 12px orange `#d95926` label "30-min window" at its right end.
- **Annotation (bold 13px green `#008300`, near x of bar 4, y=70):** "8 of 10 clicks land inside — 80% match rate".
- **Caption (12px `#444`, bottom right):** "delays illustrative".

## Two Kinds of Stream Join, Two Kinds of State

**Tags:** `where it's used` (blue), `stream-table` (green), `buffering` (orange)

- **Stream-stream** — clicks joined to impressions: both sides move, so both sides are buffered for the window
- **Stream-table** — each impression enriched with the latest row of a campaign table (name, bid, advertiser)
- **The table side** — the dimension table is kept as changing state; each event reads its current version, no window needed
- **The buffer bill** — at 10,000 impressions per second, a 30-minute stream-stream buffer holds 18 million rows in memory
- **Everywhere** — attribution, fraud pairing (login + password-reset), order-and-payment matching all run on windowed joins

*Example (italic):* An impression arriving at 2:14pm is stamped with the campaign's 2:10pm bid — the latest table row at that moment — while its click waits in a 30-minute buffer.

**Key point:** Stream-stream joins buffer both sides for the window; stream-table joins keep one side as continuously-updated state — the two patterns solve different problems and pay different memory bills.

### Visualization (canvas `c3`, 720×300)

Two-row flow diagram contrasting the join patterns: top row a stream-stream join with two feeds entering a shared windowed buffer, bottom row a stream-table join with one feed reading the latest dimension row.

- **Title (bold 15px, `#1a5276`, top center):** "Stream-Stream Buffers Both Sides; Stream-Table Reads the Latest Row".
- **Row 1 (centered y=105), label 12px `#444` at x=15:** "stream-stream"; blue `#2a78d6` rounded box at x=110 labeled "impressions" and green `#008300` rounded box at x=110, y offset +38, labeled "clicks", both with 3px arrows into a violet `#4a3aa7` rounded box at x=330 labeled "30-min window buffer (18M rows)", then a 3px arrow to an ink `#1a5276` box at x=560 labeled "matched pairs".
- **Row 2 (centered y=225), label:** "stream-table"; blue box at x=110 labeled "impressions", 3px arrow to an aqua `#199e70` box at x=330 labeled "campaign table (latest row)", a small curved refresh arrow `#6b7280` looping above the table box with 11px label "updates replace rows", then a 3px arrow to an ink box at x=560 labeled "enriched events".
- **Box style:** 150–190px wide, 36px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(0,131,0,0.12)` / `rgba(74,58,167,0.12)` / `rgba(25,158,112,0.12)` / `rgba(26,82,118,0.10)`, 12px `#2c3e50` text.
- **Annotation (bold 13px orange `#d95926`, centered near y=270):** "the buffer is the price of joining two moving things".
- **Caption (12px `#444`, top right under title):** "10,000 impressions/sec × 30 min = 18M buffered rows, illustrative".

## A Bigger Window Is Not a Free Fix

**Tags:** `common mistake` (red), `state cost` (orange)

- **The reflex** — missed matches? widen the window; 30 minutes becomes 60, then 120
- **Diminishing returns** — over a full day's traffic, match rate goes 55% → 68% → 80% → 88% → 93% across 5/15/30/60/120-minute windows
- **Linear cost** — the buffer grows with the window: 3M, 9M, 18M, 36M, 72M rows at those same window sizes
- **Late results** — a wider window also delays the answer: a 120-minute window can't confirm "no click" for 2 hours
- **The mistake** — chasing the last few percent of matches with a window that quadruples memory and latency

*Example (italic):* Doubling the window from 30 to 60 minutes buys 8 points of match rate (80% to 88%) but doubles the buffer from 18M to 36M rows and doubles the wait for a verdict.

**Common mistake:** Treating the window as a knob with no cost. Every extra minute of window is extra buffered state and extra delay — the window is a business decision about how long a real pair can take, not a tuning parameter to max out.

### Visualization (canvas `c4`, 720×300)

Dual-line chart over window size: match rate (green, left axis, rising then flattening) vs buffered rows (orange, right axis, rising linearly), showing the crossover where cost outruns benefit.

- **Title (bold 15px, `#1a5276`, top center):** "Match Rate Flattens While Buffer Cost Keeps Climbing".
- **Axes:** origin x=70, baseline y=245, plot width 560, plot height 180; x = window size with category ticks at equal spacing for `[5, 15, 30, 60, 120]` minutes, 12px `#444` labels "5m" "15m" "30m" "60m" "120m"; left y = match rate 0–100% with gridlines `#e5e9ef` at 25/50/75, 12px green `#008300` tick labels; right y (at x=630) = buffered rows 0–80M, 12px orange `#d95926` tick labels at 20M/40M/60M/80M.
- **Match-rate line:** green `#008300` 3px line with 5px dots through the five x ticks at values `[55, 68, 80, 88, 93]` (percent), 11px green value labels above each dot.
- **Buffer line:** orange `#d95926` 3px dashed line (dash 6/4) with 5px dots at values `[3, 9, 18, 36, 72]` (millions of rows), 11px orange value labels below each dot.
- **Marker:** vertical dashed `#6b7280` (dash 4/3) line at the 30m tick, 12px `#6b7280` label "chosen window" at its top.
- **Annotation (bold 13px red `#e74c3c`, near the 120m tick, y=90):** "last 13 points of match cost 4× the memory".
- **Caption (12px `#444`, bottom right):** "rates and row counts illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); click delays `[1, 3, 6, 9, 14, 19, 24, 28, 36, 52]`, match rates `[55, 68, 80, 88, 93]`, and buffered row counts `[3, 9, 18, 36, 72]` million are invented and labeled illustrative; the 18M-row figure is 10,000 events/sec × 1,800 sec, consistent across sections 3 and 4.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
