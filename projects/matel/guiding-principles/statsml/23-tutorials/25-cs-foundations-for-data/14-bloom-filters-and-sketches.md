# Bloom Filters & Sketches

**Page type:** detail page (tutorial card-sections, two-column layout: text left 50%, canvas right 50%, one table row per section)
**HTML title tag:** Bloom Filters & Sketches

**Subtitle:** Answer "have we seen this before?" across a billion rows with a fistful of bits — by accepting a tiny error rate you choose

## Have We Seen This Email Before? 1B Rows, 1GB of RAM

**Tags:** core idea (blue), running example (green)

- **The problem** — 1 billion emails seen so far; each new signup asks "seen before?", fast
- **Exact answer** — a hash set of 1 billion addresses needs roughly 50 GB of RAM (illustrative)
- **The trick** — keep only a huge array of bits; hash each email to a few positions, set them to 1
- **Query** — hash the new email: ANY position still 0 means definitely never seen
- **All positions 1** — "probably seen": a never-inserted email gets a wrong "yes" ~1% of the time
- **The prize** — the billion emails answer from ~1.2 GB of bits, roughly 40x less memory

*Example:* "No" from a bloom filter is a guarantee; "yes" means "worth checking the database to be sure."

**Key point:** A bloom filter never misses a real duplicate — its only error is an occasional false alarm, at a rate you pick when sizing it.

### Visualization (canvas `c1`, 720×300)

Mechanism diagram: an email hashed to three positions on a bit strip, with two outcome boxes.

- **Title (bold 15px, `#1a5276`, top center):** "One Query: Hash to 3 Positions, Read 3 Bits"
- **Query box (top left):** 150×34px box, fill `rgba(42,120,214,0.12)`, blue `#2a78d6` stroke width 2, bold 13px blue text "new@signup.com".
- **Bit strip:** 20 cells (30px each) with bit values `[0, 1, 0, 0, 1, 0, 1, 1, 0, 0, 1, 0, 1, 0, 0, 1, 0, 1, 1, 0]`; set bits fill `rgba(42,120,214,0.35)` with bold navy digit, zero bits `#f7f9fb` with gray digit; `#b9c2cc` cell borders. Caption below in 12px gray: "a wall of bits (billions of them, ~1.2 GB)".
- **Hash arrows:** three arrows from the query box to strip positions 4, 8, and 17 — violet `#4a3aa7` for positions 4 and 17 (bits = 1), green `#008300` for position 8 (bit = 0); bold 12px violet label "hash 1, hash 2, hash 3".
- **Highlight:** the zero bit at position 8 outlined in green `#008300` stroke width 3, with bold 12px green annotation above: "bit 8 = 0 → this email is new, guaranteed".
- **Outcome boxes (bottom):** left 290×52px green box (fill `rgba(0,131,0,0.10)`, green stroke) with bold 13px green two-line text "any bit is 0" / "DEFINITELY never seen"; right 290×52px orange box (fill `rgba(217,89,38,0.10)`, orange `#d95926` stroke) with bold 13px orange text "all bits are 1" / "PROBABLY seen (~1% false alarm)".

## A 10-Bit Filter You Can Run on Paper

**Tags:** worked example (green)

- **The setup** — 10 bits, all 0; two hash functions that turn an email into two positions
- **Insert ann@** — hashes to positions 2 and 7: set both bits to 1
- **Insert bob@** — hashes to positions 4 and 9: set them too; four bits are now 1
- **Query carol@** — hashes to 3 and 7: bit 3 is 0, so carol@ was definitely never inserted
- **Query dave@** — hashes to 2 and 9: both 1, so "probably yes" — but dave@ was never inserted
- **That's the error** — dave@ landed on bits borrowed from ann@ and bob@: a false positive

*Example:* Real filters use ~10 bits and several hashes per item — this toy is deliberately small enough to collide.

**Key point:** The filter cannot tell "these bits are yours" from "these bits are borrowed" — that ambiguity is the whole trade.

### Visualization (canvas `c2`, 720×300)

Worked-example diagram: a 10-cell bit strip with insert arrows from above and query arrows from below.

- **Title (bold 15px, `#1a5276`, top center):** "10 Bits, 2 Hashes: Two Inserts, Then Two Queries"
- **Bit strip (center):** 10 cells (46px each) with values `[0, 0, 1, 0, 1, 0, 0, 1, 0, 1]`; set bits fill `rgba(42,120,214,0.35)` with bold navy digit, zero bits `#f7f9fb` with gray digit; position indexes 0–9 in 12px gray below each cell.
- **Inserts (arrows from above):** bold 13px blue `#2a78d6` label "insert ann@ → 2, 7" with blue arrows to positions 2 and 7; bold 13px aqua `#199e70` label "insert bob@ → 4, 9" with aqua arrows to positions 4 and 9.
- **Queries (arrows from below):** bold 13px green `#008300` label "query carol@ → 3, 7" with green arrows to positions 3 and 7, and bold 12px green result: "bit 3 = 0 → definitely not seen"; bold 13px magenta `#d55181` label "query dave@ → 2, 9" with magenta arrows to positions 2 and 9, and bold 12px magenta result: "both 1 → \"probably yes\" — FALSE POSITIVE".
- **Annotation (center, on white strip, bold 13px orange `#d95926`):** "dave@'s bits were set by ann@ and bob@"

## Counting Uniques in Kilobytes: HyperLogLog

**Tags:** core idea (blue), where it's used (blue)

- **New question** — not "seen this one?" but "how MANY distinct emails did we see?"
- **Coin-flip insight** — hashes look like coin flips; 20 leading zeros is a 1-in-a-million event
- **So** — if the luckiest hash seen starts with ~20 zeros, you saw ~1 million distinct values (2²⁰)
- **Steady it** — keep the record zero-run in each of ~2,048 buckets, then average (harmonically)
- **The result** — HyperLogLog counts billions of distinct values in ~1.5 KB, within about ±2%
- **Where** — approximate `COUNT(DISTINCT)` in BigQuery, Redis, and Presto/Trino runs on this

*Example:* Distinct visitors across 200 servers: each keeps 1.5 KB, and the sketches merge by taking maximums.

**Key point:** One lucky hash is noise; thousands of bucketed record-runs averaged together become a measurement.

### Visualization (canvas `c3`, 720×300)

Line chart with marked points: longest leading-zero run vs distinct count on a log-scale x-axis, plus a side note about buckets.

- **Title (bold 15px, `#1a5276`, top center):** "The Luckiest Hash Reveals the Count: Record Zero-Run vs Uniques"
- **Axes:** x = distinct values hashed on log scale (tick labels 1, 1k, 1M, 1B; caption "distinct values hashed (log scale)"); y = longest leading-zero run seen, 0 to 32 with gridlines at 0/10/20/30 (rotated y-axis label "longest leading-zero run seen"); gray `#999` axis frame. Padding: top 52, bottom 58, left 80, right 200.
- **Line:** violet `#4a3aa7`, width 3, straight line run = log₂(d) from d=1 (run 0) to d=1B (run ~30); bold 13px violet label near top left of plot: "record run ≈ log₂(uniques)".
- **Points (orange `#d95926` dots, radius 6, bold 12px `#444` labels above):**
  - 1 thousand → ~10 zeros ("1 thousand → ~10 zeros")
  - 1 million → ~20 zeros ("1 million → ~20 zeros")
  - 1 billion → ~30 zeros ("1 billion → ~30 zeros")
- **Side note (right, 12px `#444`, four lines):** "one record is noisy, so" / "HLL keeps the record in" / "each of ~2,048 buckets" / "and averages them:", followed by bold 13px green `#008300`: "~1.5 KB, ±2%," / "merges across servers".

## The Trade: A Tiny Known Error for a Huge Memory Cut

**Tags:** rule of thumb (blue), common mistake (red)

- **The menu** — exact set: 50 GB, no error; bloom filter: 1.2 GB, 1% false alarms; HLL: 1.5 KB, ±2%
- **Thousandfold-plus** — the sketch answers in kilobytes what exactness needs gigabytes for
- **Match tool to question** — membership → bloom filter; distinct counts → HLL; audits → exact
- **Errors differ** — bloom never says no falsely; HLL is off a little in either direction
- **Common mistake** — using a sketch where one false positive is costly (blocking users, billing)
- **Tune it** — more bits per item, fewer alarms: ~10 bits/item ≈ 1%, ~20 bits/item ≈ 0.01%

*Example:* A dedupe pipeline lets ~1% duplicate alerts through — and skips 99% of its database lookups.

**Key point:** Ask "what does one wrong answer cost?" — when the answer is "almost nothing", a sketch buys a thousandfold memory cut.

### Visualization (canvas `c4`, 720×300)

Horizontal bar chart on a log scale: memory needed by three approaches, with error labels.

- **Title (bold 15px, `#1a5276`, top center):** "Memory for \"1 Billion Emails\" — Three Answers (log scale, illustrative)"
- **Rows** (bars 26px tall starting at x=175, width = log10(bytes)/11 of 330px, 0.65 alpha fill; row label bold 13px `#333` at left; size label bold 14px in bar color right of bar; error note 12px gray under the bar):
  - "exact hash set" — 50 GB (50,000,000,000 bytes), blue `#2a78d6`, "error: none"
  - "bloom filter" — 1.2 GB (1,200,000,000 bytes), aqua `#199e70`, "error: 1% false alarms"
  - "HyperLogLog" — 1.5 KB (1,500 bytes), violet `#4a3aa7`, "error: ±2% on the count"
- **Vertical gridlines** (light `#e5e9ef`) at KB, MB, GB with 12px gray labels.
- **Annotations (bottom center):** bold 14px orange `#d95926`: "gigabytes → kilobytes: a 30-million-fold cut, for ±2%"; 12px gray: "remember: log scale — each gridline is 1,000x the previous".

## Regeneration instructions

- **Template:** tutorials topic-page layout (social-graph reference style): `<h1>` (no index number), `.subtitle`, then four `.card-section` blocks each with an `<h2>` and a `table.layout` with one `<tr>`: left `<td class="text-col">` (50%) holding `.tags` pills, a `<ul>` of one-line bullets with `<b>` lead terms, an italic `.example` line, and a `.key-point` callout; right `<td class="viz-col">` (50%) holding one canvas.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6. h1 2rem `#1a5276` with 2px `#2980b9` bottom border; section h2 1.3rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem. Bullets 0.92rem, `li b` in `#1a5276`; inline `code` in ui-monospace on `#f4f6f8`. `.example` italic `#555` 0.9rem. `.key-point` background `#f8f9fa`, left border 3px solid `#e74c3c`, padding 8px 12px, 0.9rem.
- **Tag pills:** `.tag` inline-block, 0.72rem bold, padding 2px 10px, radius 10px; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Canvas:** intrinsic 720×300, CSS `width:100%`, 1px `#e0e0e0` border, radius 4px; scaled with `window.devicePixelRatio` via a shared `setup(id)` helper (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates); a shared `arrowLine(ctx, x1, y1, x2, y2, color, lw)` helper draws arrows with filled triangular heads.
- **Chart palette (`P` object):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- No nav bar, no back/home links, no cross-page links. In regenerated HTML any card links would use `.html` extensions.
