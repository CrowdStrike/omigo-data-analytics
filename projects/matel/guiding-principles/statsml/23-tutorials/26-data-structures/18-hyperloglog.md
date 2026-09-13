# HyperLogLog

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** HyperLogLog

**Subtitle:** You can guess the size of a crowd from one lucky coin streak — HyperLogLog turns that trick into a count of a billion distinct users in 1.5 KB, off by only a couple percent

## The Longest Run of Heads in a Crowd

**Tags:** `core idea` (blue), `coin streaks` (green), `hashing` (orange)

- **The game** — everyone in a crowd flips a coin until tails and remembers their streak of heads
- **Rare streaks** — about 1 person in 8 reaches 3 heads, and only 1 in 4,096 reaches 12 heads
- **The record** — you keep just the single best streak: a record of 12 suggests about 2¹² = 4,096 people
- **Hashing** — a hash turns each user ID into coin flips, the same flips every time for that user
- **Repeats free** — a returning visitor re-flips the exact same streak, so the record never double-counts

*Example (italic):* If the best streak in a room is 6 heads, guess about 2⁶ = 64 people — one lucky number stands in for the whole crowd.

**Key point:** One tiny number — the record heads streak — estimates crowd size, because a streak of k heads shows up about once per 2^k people.

### Visualization (canvas `c1`, 720×300)

Single-panel bar chart: four crowd sizes on the x axis, the typical record heads streak each crowd produces as bar height, showing the record climbing by one for every doubling-cubed of the crowd.

- **Title (bold 15px, `#1a5276`, top center):** "Bigger Crowd, Longer Record Streak".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 190; x axis = four category labels "8 people", "64", "512", "4,096" (12px `#444`, centered under bars); y axis = record streak 0 to 14, light `#e5e9ef` gridlines at 4, 8, 12 with 12px `#444` labels.
- **Bars:** blue `#2a78d6` fill `rgba(42,120,214,0.35)` with 2px `#2a78d6` border, width 70px, centered at x = 135, 285, 435, 585; heights for streak values `[3, 6, 9, 12]` (12 → top at y≈82); bold 13px `#1a5276` value labels "3", "6", "9", "12" above each bar.
- **Guide:** thin dashed `#6b7280` (dash 4/3) curve through the bar tops, 11px `#6b7280` label "record ≈ log₂(crowd)" near x=340, y=140.
- **Annotation (bold 13px orange `#d95926`, near x=430, y=70):** two lines: "see a 12-heads record ⇒" / "guess about 2¹² = 4,096 people".
- **Caption (12px `#444`, bottom right):** "illustrative — typical record streaks".

## Eight Visitors, Four Buckets, One Guess

**Tags:** `worked example` (blue), `buckets` (green), `records` (orange)

- **One record is jumpy** — a single lucky streak can wreck the guess, so split visitors into buckets
- **Bucket bits** — the first 2 hash bits pick a bucket (00–11); the last 4 bits are the coin flips (0 = heads)
- **Record keeping** — each bucket stores only the longest heads streak it has ever seen
- **Eight visitors** — the records land at 2, 3, 2, 4 for buckets 0–3 (redo it by hand from the chart)
- **The guess** — a simplified harmonic-average formula: 0.532 × 4² ÷ (¼ + ⅛ + ¼ + ¹⁄₁₆) ≈ 12
- **Rough here, tight at scale** — 4 buckets wobble about ±50%; 2,048 buckets tighten to about ±2%

*Example (italic):* dev hashes to 00 0010 — bucket 00's flips run heads, heads, tails, a 2-heads streak, so bucket 0's record becomes 2.

**Key point:** The whole sketch is just a handful of tiny records; the estimate of 12 versus the true 8 is rough only because there are 4 buckets here.

### Visualization (canvas `c2`, 720×300)

Two-column flow diagram: eight visitor rows on the left (name, 6 hash bits split into bucket bits and flips, streak), four register boxes on the right holding each bucket's record, rows color-coded by bucket.

- **Title (bold 15px, `#1a5276`, top center):** "8 Visitors → 4 Bucket Records → Guess ≈ 12".
- **Left column header (bold 12px `#444`, x=40, y=62):** "visitor → bucket bits | flips → streak".
- **Visitor rows (13px monospace, x=40, y = 84, 106, 128, 150, 172, 194, 216, 238), text colored by bucket — bucket 0 blue `#2a78d6`, bucket 1 green `#008300`, bucket 2 orange `#d95926`, bucket 3 violet `#4a3aa7`:**
  - "ana  → 00 | 0100 → 1"
  - "dev  → 00 | 0010 → 2"
  - "hugo → 00 | 0110 → 1"
  - "ben  → 01 | 0001 → 3"
  - "finn → 01 | 0100 → 1"
  - "cara → 10 | 0011 → 2"
  - "gia  → 10 | 0101 → 1"
  - "elle → 11 | 0000 → 4"
- **Register boxes (x=430, width 240, height 34, at y = 75, 118, 161, 204):** rounded 4px rect, 2px border in the bucket's color, fill at 0.12 alpha of the same color; bold 13px text inside, in the bucket's color: "bucket 00 — record 2", "bucket 01 — record 3", "bucket 10 — record 2", "bucket 11 — record 4".
- **Connectors:** thin 1px `#e5e9ef` lines from each row's right edge (x≈330) to its bucket box's left edge.
- **Annotation (bold 12px ink `#1a5276`, centered near x=550, y=262):** "harmonic average of 2, 3, 2, 4 ⇒ guess ≈ 12 (true: 8)".
- **Caption (11px `#444`, bottom left):** "illustrative 6-bit hashes — real sketches use 64 bits".

## A Billion Distinct Users in 1.5 KB

**Tags:** `where it's used` (blue), `memory` (green), `mergeable` (orange)

- **The exact way** — remembering every distinct visitor ID costs 8 bytes each: a billion users ≈ 8 GB
- **The bitmap way** — one bit per possible user ID still costs about 125 MB for a billion IDs
- **The sketch way** — 2,048 buckets × 6 bits per record = 12,288 bits = exactly 1.5 KB
- **The accuracy** — expected error is 1.04 ÷ √2,048 ≈ 2.3%, so a true billion reads ≈ 0.98–1.02 billion
- **Free unions** — merge two servers' sketches by keeping each bucket's larger record; no rescan needed
- **Everywhere** — the distinct-count feature in many databases and analytics engines runs on this sketch

*Example (italic):* A dashboard slicing distinct viewers by country and by device keeps thousands of 1.5 KB sketches instead of thousands of 8 GB lists.

**Key point:** 1.5 KB buys a distinct count of a billion within about 2% — because the sketch stores records of streaks, not people.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart on a logarithmic bytes axis: three ways to count a billion distinct users, from the exact ID list down to the HyperLogLog sketch.

- **Title (bold 15px, `#1a5276`, top center):** "Counting 1 Billion Distinct Users: Memory Needed (log scale)".
- **Axis:** horizontal 2px `#999` line at y=245 from x=200 to x=680 (width 480), log₁₀ bytes from 3 (1 KB) to 10 (10 GB); 12px `#444` tick labels "1 KB", "1 MB", "1 GB" at x = 200, 406, 611 with short ticks; bars grow rightward from x=200.
- **Rows (bar height 30px, at y = 80, 140, 200), each with a left-aligned 12px `#444` label at x=20:**
  - "exact ID list (8 B/user)": orange `#d95926` fill `rgba(217,89,38,0.35)`, 2px orange border, bar to x≈673 (8 GB); bold 13px white label "8 GB" inside the bar near its right edge.
  - "bitmap (1 bit/ID)": yellow `#c98500` fill `rgba(201,133,0,0.35)`, 2px `#c98500` border, bar to x≈550 (125 MB); bold 13px `#c98500` label "125 MB" just right of the bar end.
  - "HyperLogLog (2,048 × 6 bits)": green `#008300` fill `rgba(0,131,0,0.35)`, 2px green border, short bar to x≈213 (1.5 KB); bold 13px green label "1.5 KB" just right of the bar end.
- **Annotation (bold 13px green `#008300`, near x=380, y=215):** two lines: "5,000,000× smaller than the exact list —" / "and still within about 2%".
- **Caption (12px `#444`, bottom right):** "log scale — each gridline step is 1,000×".

## It Counts, It Doesn't Remember

**Tags:** `common mistake` (red), `distinct only` (orange)

- **Not a sample** — every single visitor updates the sketch; nothing is skipped or sampled away
- **Repeats add nothing** — a returning user re-flips identical flips, so no bucket record can grow
- **No memory of who** — the sketch can say ≈286 million distinct, never whether ana was one of them
- **No subtraction** — records only ratchet upward; you cannot remove a user or un-merge a day
- **Distinct, not volume** — total visits and distinct visitors drift far apart; HLL tracks only the second

*Example (italic):* By day 10 the site has logged 1.95 billion visits from 280 million people — the sketch reads ≈286 million, unmoved by the 1.67 billion repeats.

**Common mistake:** Treating the sketch as a compressed visitor list — HyperLogLog answers "how many distinct", never "who" or "how often".

### Visualization (canvas `c4`, 720×300)

Single-panel line chart over ten days: cumulative total visits climbing steeply while true distinct users flatten out, with the HLL estimate hugging the distinct line the whole way.

- **Title (bold 15px, `#1a5276`, top center):** "Visits Keep Climbing — the Sketch Only Counts Distinct People".
- **Axes:** origin x=70, baseline y=245, plot width 590, plot height 185; x axis = day 1 to 10, 12px `#444` tick labels "day 1" through "day 10" every day (abbreviate to "1".."10" with "day" once); y axis = millions 0 to 2,000, light `#e5e9ef` gridlines at 500, 1,000, 1,500, 2,000 with 12px `#444` labels "500M", "1B", "1.5B", "2B".
- **Total visits line:** mute `#6b7280` 2px dashed (dash 6/4) line through day values (millions) `[100, 240, 410, 600, 800, 1010, 1230, 1460, 1700, 1950]`; 12px `#6b7280` label "total visits" above its right end.
- **True distinct line:** blue `#2a78d6` 3px line through `[80, 140, 190, 220, 240, 255, 265, 272, 277, 280]`; 12px blue label "true distinct" below its right end.
- **HLL estimate line:** green `#008300` 2px dashed (dash 4/3) line through `[82, 137, 193, 224, 236, 251, 259, 278, 271, 286]`, 6px green dots at each day; bold 12px green label "HLL estimate ≈286M" at its right end.
- **Annotation (bold 13px green `#008300`, near x=280, y=95):** two lines: "repeat visits add nothing —" / "same user, same coin flips".
- **Caption (12px `#444`, bottom right):** "illustrative — 2,048-bucket sketch, ≈2% error".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all bar heights, hash bits, register values, byte sizes, and day-series points are the hardcoded arrays above (no randomness); c2's records 2/3/2/4 and guess ≈12 follow from the listed hashes via 0.532 × 4² ÷ Σ 2^(−record); c4's estimate points stay within ≈2.5% of the true-distinct points.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
