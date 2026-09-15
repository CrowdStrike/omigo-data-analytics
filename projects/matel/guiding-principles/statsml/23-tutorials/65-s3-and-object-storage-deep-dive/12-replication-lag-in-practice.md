# Replication Lag in Practice

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Replication Lag in Practice

**Subtitle:** The second region is always a little behind — this page is about measuring how far behind it is, and what a read over there returns while the copy is still in flight

## The Canonical Lag Sample (used by c1; stated here so the two files cannot drift)

One night's 100 measured per-object copy delays, in **seconds**, already sorted ascending
(illustrative, hardcoded literal array in both files):

```
3, 3, 4, 4, 4, 5, 5, 5, 5, 6, 6, 6, 6, 7, 7, 7, 7, 7, 8, 8,
8, 8, 9, 9, 9, 9, 10, 10, 10, 10, 11, 11, 11, 12, 12, 12, 13, 13, 13, 14,
14, 14, 15, 15, 16, 16, 17, 17, 18, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28,
29, 31, 33, 35, 37, 39, 41, 44, 47, 50, 54, 58, 62, 67, 72, 78, 84, 91, 98, 106,
115, 124, 134, 146, 158, 171, 186, 202, 220, 239, 260, 283, 308, 335, 412, 590, 905, 1480, 2610, 7020
```

Statistics this array yields (all recomputed in JS at render time from the array,
never hardcoded as chart labels):

| Statistic | Value | How it is computed |
|-----------|-------|--------------------|
| n | 100 | array length |
| median | 18.5 s | mean of the 50th and 51st sorted values (18 and 19) |
| mean | 177.76 s | sum 17,776 ÷ 100 |
| 95th percentile | 412 s | nearest-rank: `sorted[ceil(0.95 × 100) − 1]` = `sorted[94]` |
| 99th percentile | 2,610 s | nearest-rank: `sorted[ceil(0.99 × 100) − 1]` = `sorted[98]` |
| max | 7,020 s | last element (1.95 h, "almost two hours") |
| mean ÷ median | 9.6× | 177.76 ÷ 18.5 |
| share below the mean | 86 of 100 | count of values < 177.76 |
| count at or above 412 s | 6 of 100 | indices 94…99: 412, 590, 905, 1480, 2610, 7020 |
| the max's share of the mean | 70.2 s | 7,020 ÷ 100 |

## Measuring One Night of Copy Delays

**Tags:** `worked example` (blue), `right-skewed` (green), `use a percentile` (orange)

- **The sample** — one night's batch, the delay measured separately for each of 100 objects
- **Per object** — a small index file and a huge data file never travel across together
- **The median** — half the objects arrive in 18.5 seconds or less, which looks fast
- **The average** — the same 100 objects average 177.8 seconds, 9.6 times the median
- **Almost everyone is below it** — 86 of the 100 objects are faster than that average
- **Why the tail** — big objects, a big batch queued at once, and a busy network link
- **The slow end** — 6 objects took 412 seconds or more and one took 7,020 seconds
- **Report a percentile** — the 99th percentile is 2,610 seconds, far past the average

*Example (illustrative):* Half the night's objects are copied inside 18.5 seconds, and the slowest single object takes almost two hours.

**Key point:** The average delay hides the risk, because a few very slow objects pull it upward. Report a high percentile and the worst case, never the average alone.

### Visualization (canvas `c1`, 720×300)

Histogram of the 100 delays on a log-spaced x axis, with median, average, 95th and 99th
percentile drawn as labelled vertical lines — all four computed in JS from the plotted array.

- **Title (bold 15px, `#1a5276`, top center at y=22):** "Copy Delay for 100 Objects in One Night".
- **Sample:** the 100-value array above, hardcoded. `median`, `mean`, `p95`, `p99` computed by helper functions over that array; every printed number formats the computed value.
- **Binning:** 16 log10 bins from 10^0.25 (≈1.8 s) to 10^4.25 (≈17,783 s), bin width 0.25 dex. Bin index = `floor((log10(x) − 0.25) / 0.25)`, clamped to 0..15. Counts computed in JS; they come out `[2, 7, 17, 22, 14, 9, 8, 7, 7, 2, 2, 1, 1, 0, 1, 0]`, summing to 100.
- **Plot box:** x from 60 to 690, y baseline 236, top 62; bar fill `rgba(42,120,214,0.35)`, 1px `#2a78d6` edge; tallest bin maps to `plotH − 12` = 162 px.
- **X axis (1.5px `#1a5276`)** with log ticks at 3, 10, 30, 100, 300, 1000, 3000 s, labels 12px `#2c3e50` at y=254; axis caption 12px `#6b7280` centered at y=272: "copy delay per object, seconds (log scale)".
- **Y axis:** left tick labels 12px `#6b7280` at counts 0, 5, 10, 15, 20 with 1px `#e5e9ef` gridlines; "objects per bin" 12px `#6b7280` left-aligned at (60, 54).
- **Four marker lines (2px, y=62 to y=236), labels bold 12px left-aligned at line + 5px, stacked at y = 74 / 96 / 118 / 140 so they cannot collide:**
  - median → aqua `#199e70`, `"median " + med + "s"`
  - average → violet `#4a3aa7`, `"average " + mean.toFixed(1) + "s"`
  - 95th → orange `#d95926`, `"95th " + p95 + "s"`
  - 99th → magenta `#d55181`, `"99th " + p99 + "s"`
- **Annotation (bold 13px violet `#4a3aa7`, right-aligned at x=690, y=54):** `"average is " + (mean/med).toFixed(1) + "× the median"`.
- **Caption (12px `#444`, bottom right):** "delays illustrative; median, average and both percentiles computed from the plotted 100 values".

## How to Watch the Gap While It Happens

**Tags:** `where it's used` (blue), `metrics` (green), `heartbeat` (orange)

- **The waiting queue** — the service reports how many objects still wait to be copied
- **The same queue in bytes** — a second reading that the largest objects dominate
- **A reported delay** — the service also publishes the worst delay it is seeing now
- **Per-object status** — each object carries a flag: waiting, done, failed, or arrived
- **Failures alert you** — an event fires for any object the copy could not complete
- **A queue is not a delay** — queue length tells you nothing about one object's wait
- **Zero is not proof** — an empty queue says nothing is waiting, not that yours landed
- **The heartbeat trick** — write a file holding the time, then time when it appears

*Example (illustrative):* A heartbeat written at 23:02:10 shows up in the copy at 23:02:17, a gap of 7 seconds; the same heartbeat written at 01:00:05 mid-batch is not there until 01:41:35.

**Key point:** Queue numbers tell you the copier is busy. Only a time-stamped heartbeat tells you how far behind the second region's visible data is right now.

### Visualization (canvas `c2`, 720×300)

Two paired timelines — written time vs first-seen time in the copy — one quiet, one mid-batch,
with each gap computed in JS from the two timestamps.

- **Title (bold 15px, `#1a5276`, y=22):** "A Time-Stamped Heartbeat Measures the Gap Directly".
- **Data (hardcoded, seconds-of-day pairs with display labels):**
  - Quiet period: written `23:02:10` (82,930 s), first seen `23:02:17` (82,937 s) → gap **7 s**
  - Mid-batch: written `01:00:05` (3,605 s), first seen `01:41:35` (6,095 s) → gap **2,490 s**
- **Computed at render time:** `gap = seen − written` for each row; printed with a `fmtDur` helper giving "7 s" and "41 min 30 s" (2,490 s = 41 min 30 s).
- **Layout:** two bands, y=80 (quiet period, aqua `#199e70`) and y=180 (mid-batch, red `#e74c3c`); each band has a 2px `#e5e9ef` rail from x=120 to x=660 and a bold 12.5px band label left-aligned at (60, y−26).
- **Travelled portion:** 3px band-coloured segment from the written marker (x=160) to the first-seen marker; first-seen x is 300 for the quiet band and 620 for the mid-batch band (fixed positions; the *labels* carry the computed numbers).
- **Markers:** 6px filled circle in `#1a5276` at the written x, 6px filled circle in the band colour at the first-seen x.
- **Marker labels:** bold 12px "written" / "first seen in the copy" at `y + 22`, the timestamp 12px `#2c3e50` at `y + 38`, both centred on their marker.
- **Gap bracket:** 2px band-coloured bracket from written to first-seen at `y − 16`, bold 13px centred label `"gap " + fmtDur(gap)` at `y − 22`.
- **Contrast annotation (bold 13px violet `#4a3aa7`, centered at y=268):** `"same check, same code: " + fmtDur(g1) + " vs " + fmtDur(g2) + " — a " + Math.round(g2/g1) + "× swing"` (2,490 ÷ 7 = 355.7 → 356×).
- **Caption (12px `#444`, bottom right):** "timestamps illustrative; each gap computed from the two plotted times".

## Reading the Copy Mid-Batch Returns a Wrong Answer

**Tags:** `common mistake` (red), `silent wrong answer` (green), `failover` (orange)

- **A short listing** — a job reading the copy mid-batch sees only some of the hours
- **No error is raised** — the query finishes normally, only the number is wrong
- **The wrong total** — the 8 hours present sum to 31,800 of the true 75,710 events
- **Just 42% of the truth** — four missing hours quietly drop 43,910 of the events
- **Failing over loses them** — switching regions mid-batch drops whatever still waits
- **Your recovery gap** — how much you lose is the slow tail of this delay distribution
- **Unmeasured is unknown** — without a sample nobody can say how much a failover loses
- **Deletes lag too** — a file can be gone in one region and still readable in the other

*Example (illustrative):* A 02:00 query against the copy sees hours 00–07 but not 08–11, and returns 31,800 events instead of 75,710 — 42% of the truth, with no warning.

**Common mistake:** Trusting a read of the second region while a batch is still being copied. Read the copy only after something marks the batch complete, or expect a partial answer that arrives with no error at all.

### Visualization (canvas `c3`, 720×300)

The destination-side read mid-batch: twelve hourly objects, eight present and four not yet
arrived, with the true and returned totals computed in JS from the plotted per-hour values.

- **Title (bold 15px, `#1a5276`, y=22):** "Reading the Copy at 02:00: Four Hours Have Not Arrived".
- **Data (hardcoded per-hour event counts, illustrative):** `00 → 4120, 01 → 3080, 02 → 2450, 03 → 1980, 04 → 2310, 05 → 3670, 06 → 5940, 07 → 8250, 08 → 10430, 09 → 11760, 10 → 12180, 11 → 9540`. Hours 08–11 are flagged `present: false`.
- **Computed at render time:** `complete = sum(all)` = **75,710**; `partial = sum(present only)` = **31,800**; `lost = 43,910`; `partial / complete = 42.0%`. Every printed figure derives from the array.
- **Bars:** twelve slots, x from 70 to 660, slot width `(660−70)/12` = 49.17, bar width `slot − 12` = 37.17, offset 6 px into the slot; baseline y=210, height scaled so the largest value (12,180) reaches 120 px.
  - present hours → fill `rgba(42,120,214,0.35)`, 1.5px `#2a78d6` edge, count printed bold 11.5px `#1a5276` 7 px above the bar
  - not-yet-arrived hours → drawn at their true height as ghost bars: fill `rgba(231,76,60,0.07)`, 2px dashed `#e74c3c` edge, **no per-bar count** (the numbers would be wider than a 37 px bar and would collide)
- **Group label (bold 12px `#e74c3c`, centered over the four ghost bars at y=78):** "not arrived yet". Its centre is computed from the slot geometry, and y=78 sits above the tallest ghost bar's top at y=90.
- **Hour labels (12px `#2c3e50`, y=228):** `00` … `11` centred in each slot; axis caption 12px `#6b7280` left-aligned at (70, 246): "hour of data in the second region's copy".
- **Annotation (bold 13px magenta `#d55181`, centered at y=46):** `"four missing hours = " + lost + " events dropped silently"`.
- **Two result chips (rounded rects, y=258, height 30, 6px radius):**
  - left at x=70 width 270 — fill `rgba(0,131,0,0.12)`, 2px `#008300`, bold 12.5px `#008300` centred at x=205: `"true total = " + complete`
  - right at x=360 width 300 — fill `rgba(231,76,60,0.10)`, 2px `#e74c3c`, bold 12.5px `#e74c3c` centred at x=510: `"query returns " + partial + " (" + pct + "% of truth)"`
- **Caption (12px `#444`, bottom right):** "counts illustrative; both totals and the percentage computed from the plotted values".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `23-tutorials/CLAUDE.md`), matching the sibling `11-cross-region-replication.html` in this folder. Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then **three** `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label). Section three's callout uses the label "Common mistake:".
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links, no cross-page links, no `.nav` rules.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150 ms). Helpers `roundRect`, `arrowHead` and `rgba` as on page 11, plus the statistics helpers `meanOf`, `medianOf`, `quantileNearestRank`, `fmtInt`, `fmtDur`.
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Red `#e74c3c` is reserved for genuine risk: the mid-batch band in `c2`, the ghost bars and the wrong total in `c3`.
- **Bullet length — this is the deliberate calibration of the page.** Bullets run roughly 75–90 characters including the bold lead term, so each holds one line at the 50/50 split without wrapping while still carrying a full clause. An earlier pass clipped them to ~55–70 characters and was rejected for compromising quality; equally, do **not** pad them back to the folder default of ~90–100. Eight bullets per section.
- **Register and vocabulary.** Plain technical English for a first-time reader. No analogies, no invented scenes. Say "read" and "write", never GET/PUT/LIST/HEAD; say "the service reports how many objects still wait", never `OperationsPendingReplication`; say "each object carries a flag", never `x-amz-replication-status`; say "a file holding the time", never a canary product; say "the second region's copy", never a bucket ARN; say "how much you lose on a failover", never RPO. The exact metric, header and event names belong on a reference page, not here.
- **Deliberate omissions — do not reintroduce on regeneration.**
  - **No setup/"the copy starts after the write" section.** The earlier draft opened with a fourth section (Alice's nightly job, Bob's reader, asynchronous copying, "region A says yes and region B says no", `c1`'s two-region lane diagram). It was cut because `11-cross-region-replication` already teaches exactly that: the copy begins after the write succeeds, what a rule copies, and the paid 15-minute deadline.
  - **No consistency-model inventory.** Bullets contrasting strong read-after-write inside one bucket against no cross-region guarantee belong on `13-cap-theorem-for-object-storage`, which covers the same-region-versus-other-region guarantee list in plain language.
  - **No backlog time series.** A pending-count-over-the-night line chart was dropped when the section count came down; the queue-versus-delay lesson survives as two bullets in section two, and the heartbeat chart carries the row.
  - **No named characters.** The nightly batch is described as "one night's batch", not as a person's job.
  - **Three sections, eight short bullets each, one canvas per section.**
- **Data integrity:** no `Math.random()` and no seeded PRNG needed — every series is a hardcoded literal array. Statistics printed in charts (median, average, both percentiles, average ÷ median, the two heartbeat gaps and their ratio, the true and returned totals, the lost count, the percentage) are computed in JS from those arrays at render time, so labels cannot drift from the shapes.
- **Verifiable arithmetic:** the 100-value delay sample sums to 17,776, giving mean 177.76 and median 18.5, so mean ÷ median = 9.61 and 86 values fall below the mean; the largest value contributes 7,020 ÷ 100 = 70.2 s of the mean; nearest-rank gives the 95th percentile 412 s (`sorted[94]`) with six values at or above it, and the 99th percentile 2,610 s (`sorted[98]`); the max 7,020 s = 1.95 h, hence "almost two hours"; the log-bin counts `[2,7,17,22,14,9,8,7,7,2,2,1,1,0,1,0]` total 100. Heartbeat: 82,937 − 82,930 = 7 s; 6,095 − 3,605 = 2,490 s = 41 min 30 s; 2,490 ÷ 7 = 355.7 ≈ 356×. Hourly counts total 75,710; hours 00–07 total 31,800; difference 43,910; 31,800 ÷ 75,710 = 42.00%.
- **Documented behaviour the page stands on:** the service publishes a pending-object count, the same backlog in bytes, and a latency figure for the worst current delay; each object carries a replication status flag with waiting / done / failed / arrived-by-replication values; an event fires when an object fails to replicate; deletes replicate asynchronously as well. Every delay, count, timestamp and event volume on the page is invented and labelled illustrative.
- **Computed geometry:** `c1` maps seconds through `xOfSec(v) = 60 + ((log10(v) − 0.25) / 4) × 630`, so all four marker lines and all seven x ticks are placed from the data rather than from pixel literals; the four marker labels sit at fixed stacked y values (74/96/118/140) so they cannot overlap even when two markers are close. `c3` derives every bar x from the slot width, and the "not arrived yet" group label centre from the first and last ghost slot.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
