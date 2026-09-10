# SVD (Singular Value Decomposition)

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** SVD (Singular Value Decomposition)

**Subtitle:** SVD rewrites any table as a few simple patterns ranked by size — keep the big ones and you get compression, recommendations, and denoising from one trick

## Four Movie Fans, One Hidden Taste

**Tags:** `core idea` (blue), `running example` (orange), `rank-1 pattern` (green)

- **The table** — four friends rate four movies from 0 to 5, giving 16 numbers that look arbitrary
- **The secret** — every rating is one person number × one movie number: appetite × action level
- **Appetites** — Ann 1.0, Ben 0.5, Cara 0.8, Dev 0.2 measure how much each fan loves action
- **Action levels** — Blast 4, Rom-Com 2, Chase 5, Quiet 1 measure how action-packed each film is
- **Check one** — Cara × Chase = 0.8 × 5 = 4.0, exactly her rating in the table
- **Compression** — 16 ratings collapse to 4 + 4 = 8 numbers; one pattern explains the whole grid

*Example (italic):* Ben's Blast rating is 0.5 × 4 = 2.0 — every one of the 16 cells works the same way.

**Key point:** A table built from one column of numbers times one row of numbers is called rank-1. SVD is the tool that discovers such hidden patterns inside real tables.

### Visualization (canvas `c1`, 720×300)

Heatmap of the 4×4 ratings grid with the appetite column (blue) on the left and the action-level row (green) on top, showing each cell as their product.

- **Title (bold 15px, `#1a5276`, top center):** "16 Ratings = 4 Appetites × 4 Action Levels".
- **Data (row-major, users × movies):** `[[4.0, 2.0, 5.0, 1.0], [2.0, 1.0, 2.5, 0.5], [3.2, 1.6, 4.0, 0.8], [0.8, 0.4, 1.0, 0.2]]`; users Ann, Ben, Cara, Dev; movies Blast, Rom-Com, Chase, Quiet.
- **Grid:** 4×4 cells, each 62 wide × 40 tall, top-left cell corner at x=215, y=88; cell fill `rgba(42,120,214, 0.08 + 0.50*value/5)`; value text bold 12px centered, white when value ≥ 3.2 else `#1a5276`.
- **Column headers:** movie names bold 12px `#2c3e50` centered above each column at y=64; action levels "×4 ×2 ×5 ×1" bold 12px green `#008300` at y=80.
- **Row labels:** fan names 12px `#2c3e50` right-aligned at x=150; appetites "1.0 ×", "0.5 ×", "0.8 ×", "0.2 ×" bold 12px blue `#2a78d6` right-aligned at x=207, vertically centered per row.
- **Annotation (violet `#4a3aa7`, bold 13px, three lines starting x=490, y=120):** "16 numbers in the grid" / "stored as 4 + 4 = 8" / "one pattern: action taste".
- **Caption (12px `#6b7280`, centered at y=288):** "rating = appetite × action level".

## Letting SVD Find the Pattern

**Tags:** `worked example` (blue), `singular values` (green)

- **The recipe** — SVD splits any table into A = U Σ Vᵀ: fan patterns, sizes, and movie patterns
- **Σ (sigma)** — a short ranked list of sizes σ1 ≥ σ2 ≥ ... saying how strong each pattern is
- **Our table** — with small ±0.2 personal quirks added, SVD returns σ = 9.4, 0.7, 0.3, 0.1
- **Energy** — squaring them, the first pattern alone carries 99.3% of the table's total energy
- **Rank-1 rebuild** — keeping only the σ1 pattern rebuilds every rating within about ±0.2

*Example (italic):* Pattern 1 is "action taste": its U column ranks the fans, its V column ranks the movies, and σ1 = 9.4 sets the scale.

**Key point:** Singular values are a ranked bill of materials for a table — read them first. When one or two dominate, the table is secretly simple.

### Visualization (canvas `c2`, 720×300)

Dual-panel chart: bar chart of the four singular values (left) and cumulative energy share as patterns are kept (right), split by a vertical dashed divider at x=360.

- **Title (bold 15px, `#1a5276`, top center):** "Singular Values of the Ratings Table (±0.2 quirks, illustrative)".
- **Left panel (σ bars):** values `[9.4, 0.7, 0.3, 0.1]` labeled "σ1", "σ2", "σ3", "σ4" (12px `#444` below baseline); axis origin x=55, width 280, baseline y=245, chart height 185, y scale 0–10; bars 44px wide, fill `rgba(42,120,214,0.55)`, 2px `#2a78d6` border; bold 12px blue value labels above each bar; orange `#d95926` bold 12px annotation over σ2–σ4: "one big, three tiny".
- **Right panel (energy):** cumulative energy % `[99.3, 99.9, 100.0, 100.0]` at x labels "keep 1".."keep 4"; axis origin x=400, width 280, same baseline/height, y scale 98–100 with ticks 98, 99, 100 (11px `#6b7280`, gridlines `#e5e9ef`); green `#008300` 3px line with 4px dots and 12px value labels above; green bold 13px annotation "pattern 1 alone = 99.3% of the energy"; caption 12px `#444` "energy share = σ² / total".
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=360 from y=38 to h-12.

## Compression, Recommendations, Denoising

**Tags:** `where it's used` (blue), `compression` (orange), `recommenders` (green)

- **Storage math** — keeping k patterns of a 1000×1000 image costs k × (1000 + 1000 + 1) numbers
- **Compression** — 20 patterns need 40,020 numbers instead of 1,000,000 — about 4% of the space
- **Recommenders** — a taste pattern learned from filled cells predicts a fan's missing ratings
- **Denoising** — random noise spreads across the tiny-σ patterns; dropping them cleans the table
- **One trick** — all three are the same move: keep the big-σ patterns and discard the rest

*Example (italic):* A photo kept at rank 5 uses 1% of the numbers and is already recognizable; rank 50 uses 10% and looks sharp.

**Key point:** Compression, recommendation, and denoising all reduce to the same decision — how many singular values to keep — which is why SVD shows up everywhere.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart comparing the storage cost of a full 1000×1000 image against rank-50, rank-20, and rank-5 SVD versions.

- **Title (bold 15px, `#1a5276`, top center):** "Storing a 1000×1000 Image: Full vs Rank-k SVD".
- **Data:** `[1000000, 100050, 40020, 10005]` with row labels "full image", "keep 50", "keep 20", "keep 5" and value labels "1,000,000 (100%)", "100,050 (10%)", "40,020 (4%)", "10,005 (1%)".
- **Bars:** rows start at y=70, one every 52px, each bar 24px tall, starting x=150, width proportional to value with max 520px at 1,000,000 (minimum 5px); full-image bar fill `rgba(217,89,38,0.55)` with 2px `#d95926` border, the three rank-k bars fill `rgba(42,120,214,0.55)` with 2px `#2a78d6` border; row labels 12px `#444` right-aligned at x=140; value labels bold 12px (orange for full, blue for rank-k) just right of each bar end.
- **Annotation (green `#008300`, bold 13px, under the "keep 20" row at x=250):** "4% of the numbers, most of the picture".
- **Caption (12px `#6b7280`, centered at y=288):** "rank-k storage = k × (1000 + 1000 + 1) numbers (illustrative image)".

## Keeping Too Many Pieces

**Tags:** `common mistake` (red), `rule of thumb` (green)

- **The trap** — noise lives in the small-σ patterns, so keeping more of them can make things worse
- **Fits the seen** — error against the noisy table falls to zero once all 4 patterns are kept
- **Misses the truth** — error against the true clean taste table is lowest at k = 1, then climbs
- **Memorizing quirks** — patterns 2–4 encode the ±0.2 personal quirks, not real shared taste
- **Elbow rule** — keep the patterns before the sharp σ drop; here 9.4 → 0.7 says keep one
- **PCA cousin** — PCA is SVD on a centered table; uncentered, pattern 1 mostly captures the mean

*Example (italic):* Rebuilding with all 4 patterns copies the noisy table perfectly — quirks included — and predicts new ratings worse than the rank-1 version.

**Common mistake:** Judging the rank cut by how well it reproduces the table you saw — that error always improves with k. The right k is where the error against reality bottoms out.

### Visualization (canvas `c4`, 720×300)

Two-line chart of reconstruction error versus number of patterns kept: error against the observed noisy table falls to zero, while error against the true clean table bottoms out at k=1 and rises.

- **Title (bold 15px, `#1a5276`, top center):** "How Many Patterns to Keep? Error vs k (illustrative)".
- **Data:** k = 1..4 at x positions `[140, 280, 420, 560]`; error vs noisy table `[0.20, 0.10, 0.04, 0.00]` (blue); error vs true clean table `[0.05, 0.11, 0.16, 0.20]` (magenta).
- **Axes:** origin x=70, baseline y=240, plot width 560, height 175; y scale 0–0.25 with ticks 0, 0.1, 0.2 (11px `#6b7280`, gridlines `#e5e9ef`); x labels "k=1".."k=4" 12px `#444` below baseline.
- **Blue line (`#2a78d6`, 3px, 5px dots):** series label bold 12px blue near its right end: "error vs the table you saw".
- **Magenta line (`#d55181`, 3px, 5px dots):** series label bold 12px magenta near its right end: "error vs the true taste table".
- **Marker:** dashed green `#008300` (dash 4/3) vertical line at x=140 from y=65 to y=240 with green bold 13px label "best: keep 1 pattern" beside it.
- **Caption (12px `#6b7280`, centered at y=288):** "RMS error; truth = appetite × action level table, noise = ±0.2 quirks".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (here it hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- All chart data is hardcoded literal arrays — no randomness; invented numbers are labeled "illustrative" in captions/titles.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
