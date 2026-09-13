# Partitioning & Sharding

**Page type:** detail page (tutorial layout: `.card-section` blocks, each an h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Partitioning &amp; Sharding

**Subtitle:** A 500-million-row table is too big to search page by page — so you slice it, like a phone book slices people by last name

## One Orders Table, Sliced Two Ways

**Tags:** `core idea` (blue), `running example` (green)

- **The table** — 500M orders from 3 years of sales, all in one giant pile
- **Partitioning** — same machine, table cut into 36 monthly chunks of ~14M rows
- **Sharding** — three machines, customers split A–F, G–M, N–Z
- **Phone book** — nobody reads every page; the A–Z tabs skip you to the right slice
- **Same rows either way** — slicing changes where rows live, not what they say

*Example (italic):* Finding "Garcia" in a phone book takes seconds because you never open the A, B, or Z sections at all.

**Key point:** Partitioning slices one machine's table into chunks; sharding spreads the chunks across machines. Both exist so a query can skip data instead of reading it.

### Visualization (canvas `c1`, 720×300)

Split panel: one machine with stacked month partitions on the left vs three sharded machines on the right.

- **Title (bold 15px, `#1a5276`, top center):** "The Same 500M Orders — Sliced by Month vs Spread Across Machines".
- **Divider:** vertical dashed grey line (`#bdc3c7`, dash 4/3) at x 360.
- **Left panel** — header bold 13px blue `#2a78d6`: "PARTITIONING — one machine". Outer machine box 220×190 at (70,64), fill `#f4f8fc`, blue stroke, grey "machine 1" label. Inside: three stacked 180×28 chunk boxes (fill `rgba(42,120,214,0.14)`, blue stroke) labeled "Jan 2026  (~14M rows)", "Feb 2026  (~14M rows)", "Mar 2026  (~14M rows)", then bold ". . ." and grey "36 monthly chunks in total". Caption blue bold 12px: "one table, cut into month-sized chunks".
- **Right panel** — header bold 13px orange `#d95926`: "SHARDING — three machines". Three 88×120 boxes at x 400/500/600, y 90, fill `#fdf3ec`, orange stroke; each with grey machine label ("machine 1/2/3"), bold range on two lines ("customers" / "A–F", "G–M", "N–Z"), and grey "~1/3 of rows". Caption orange bold 12px, two lines: "like the A–Z tabs of a phone book," / "but each tab is its own computer".
- **Takeaway** (violet `#4a3aa7` bold 13px, right side bottom): "same rows either way — only their address changes".

## Garcia's March Orders: 500M Rows Shrinks to 4.6M

**Tags:** `worked example` (green), `core idea` (blue)

- **The query** — "show customer Garcia's orders from March 2026"
- **Step 1: pick the month** — March is 1 of 36 chunks, so 500M drops to ~14M rows
- **Step 2: pick the machine** — Garcia is in G–M, so only shard 2 even wakes up
- **What's left** — March rows on shard 2: about 4.6M rows to actually scan
- **The math** — 500M ÷ 36 months ÷ 3 shards ≈ 4.6M, roughly 1% of the table

*Example (italic):* Two skips — the right month, the right machine — and 99% of the data was never touched.

**Key point:** The database never "searches fast" — it skips. Every slice the query can rule out is data nobody has to read.

### Visualization (canvas `c2`, 720×300)

Funnel of three shrinking boxes left to right, connected by labeled skip arrows.

- **Title (bold 15px, `#1a5276`, top center):** "Two Skips: How Garcia's March Query Avoids 99% of the Data".
- **Stages** (boxes vertically centered on y 150, bold 14px value inside, grey 12px sub-label below):
  - x 40, 170×150: "500M rows" / "whole orders table", fill `rgba(42,120,214,0.14)`, blue `#2a78d6` stroke.
  - x 300, 130×80: "~14M rows" / "March 2026 chunk", fill `rgba(25,158,112,0.16)`, aqua `#199e70` stroke.
  - x 530, 110×44: "~4.6M rows" / "March on shard 2", fill `rgba(217,89,38,0.18)`, orange `#d95926` stroke.
- **Arrows:** aqua arrow between stage 1 and 2 labeled bold 12px "skip 1: keep only March" with grey "(1 of 36 month chunks)"; orange arrow between stage 2 and 3 labeled "skip 2: only machine G–M" with grey "(Garcia lives on shard 2)".
- **Takeaway** (violet bold 14px center, y 258): "4.6M of 500M scanned — roughly 1% of the table, ~99% never read"; grey 12px below: "500M ÷ 36 months ÷ 3 shards ≈ 4.6M".

## One-Shard Queries Fly, Cross-Shard Queries Crawl

**Tags:** `where it's used` (blue), `trade-off` (orange)

- **One-shard query** — "Garcia's orders" routes to shard 2 alone: ~40 ms
- **Cross-shard query** — "top 10 products overall" hits all 3 shards, then merges: ~1,200 ms
- **The slow part** — waiting for the slowest machine, then combining 3 answer piles
- **Why you care** — your feature-building SQL groups by product, not customer: full fan-out
- **The fix** — filter on the shard key (customer) when you can; expect crawl when you can't

*Example (italic):* The same JOIN that ran in 40 ms per customer took 20 minutes across all customers — nothing was broken.

**Key point:** Sharding makes queries fast only when the query names the shard key. Ask "which machines does this touch?" before blaming the database.

### Visualization (canvas `c3`, 720×300)

Split panel: routed single-shard query on the left vs fan-out-and-merge on the right.

- **Title (bold 15px, `#1a5276`, top center):** "Route to One Machine vs Fan Out to All Three".
- **Divider:** vertical dashed grey line at x 360.
- **Left panel** — green `#008300` header bold 13px: '"orders for customer Garcia"'. "query router" box (130×32, fill `#eef7ee`, green stroke) with one green arrow down to shard 2 only, annotated grey 12px "G is here". Three shard boxes (84×60 at x 60/180/300, y 150) labeled "shard 1 A–F", "shard 2 G–M", "shard 3 N–Z"; shard 2 highlighted (fill `rgba(0,131,0,0.16)`, green stroke, bold), shards 1 and 3 greyed (`#f5f5f5` fill, `#c5c9ce` stroke). Grey note: "shards 1 and 3 never even wake up". Result bold 16px green: "~40 ms".
- **Right panel** — magenta `#d55181` header bold 13px: '"top 10 products, all customers"'. "query router" box (fill `#fdf0f5`, magenta stroke) with magenta arrows to all three shard boxes (84×46 at x 430/545/660, fill `rgba(213,81,129,0.14)`), each labeled "shard 1/2/3" and "full scan"; violet arrows converge into a "merge 3 piles" box (100×30, violet `#4a3aa7` stroke). Result bold 16px magenta: "~1,200 ms"; orange bold 13px beside it: "30x slower".

## The Hot Shard: When One Machine Does the Work of Two

**Tags:** `common mistake` (red), `trade-off` (orange)

- **Names aren't uniform** — A–F holds 210M rows, G–M 180M, N–Z only 110M
- **Even split assumed** — a fair 3-way split would give each shard ~167M rows
- **Hot shard** — A–F carries almost double N–Z's load, so it answers slowest
- **Speed limit** — cross-shard queries finish when the hottest shard finishes
- **One big customer** — a single huge account can overheat a shard all by itself

*Example (italic):* The team bought a third machine expecting 3x speed and got ~2.4x — most rows piled onto one shard.

**Common mistake:** Splitting by an uneven key (names, countries, dates with a busy season) and assuming equal shards. Check the row counts per shard — the split is only as good as its key.

### Visualization (canvas `c4`, 720×300)

Vertical bar chart of rows per shard with a dashed fair-split reference line.

- **Title (bold 15px, `#1a5276`, top center):** "Rows per Shard: The A–F Split Is Nobody's Fair Third".
- **Axes:** padding top 56, bottom 56, left 70, right 200; grey `#999` axis lines; y ticks at 0, 100M, 200M (12px grey labels, `#e5e9ef` gridlines); y scale max 240M.
- **Bars** (width 90, bold 13px value labels above, 12px two-line x labels below):
  - "shard 1 / A–F": 210M rows, orange `#d95926`.
  - "shard 2 / G–M": 180M rows, blue `#2a78d6`.
  - "shard 3 / N–Z": 110M rows, aqua `#199e70`.
- **Fair-split line:** violet `#4a3aa7` dashed (7/4, width 2) horizontal line at 167M, labeled bold 12px to the right: "fair split: ~167M each".
- **Right margin annotation:** orange bold 13px "the hot shard:" then 12px text lines "almost 2x shard 3," / "so every cross-shard" / "query waits for it".
- **Caption (grey 12px bottom center):** "customer last names are not spread evenly across the alphabet (illustrative counts)".

## Regeneration instructions

- **Template:** tutorials topic-page layout (see `tutorials/CLAUDE.md`). h1 (no index number) with 2px `#2980b9` bottom border, `.subtitle` grey one-liner, then four `.card-section` blocks: each has an `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) and a `table.layout` with `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, one-line `<ul>` bullets each opening with `<b>bold term</b>` (`#1a5276`), one italic `.example` paragraph, one `.key-point` callout (background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem).
- **Tag pills:** inline-block, 0.72rem, weight 600, padding 2px 10px, radius 10px. Colors: blue `rgba(26,82,118,0.12)`/`#1a5276`; green `rgba(39,174,96,0.15)`/`#27ae60`; red `rgba(231,76,60,0.12)`/`#e74c3c`; orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; canvases `width:100%`, `1px solid #e0e0e0` border, radius 4px.
- **Canvas:** each declared 720×300 intrinsic; scaled by `window.devicePixelRatio` via a shared `setup(id)` helper (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates). Shared `box()` and `arrow()` (filled triangular head) helpers.
- **Chart palette object `P`:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- No nav bar, no back/home links. In regenerated HTML, any card links use `.html` extensions (this page has no outbound links).
