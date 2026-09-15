# Inconsistent Categories

**Page type:** detail page (tutorial layout: h2 card-sections, two-column table with text left 50% / canvas right 50%; one section holds both canvases side by side in a `.viz-pair` flex row.)
**HTML title tag:** Inconsistent Categories

**Subtitle:** Four spellings of one country split a single group into four small ones — and group-by reports them as strangers

## One Country, Four Spellings

**Tags:** `core idea` (blue), `running example` (green)

- **The column** — 35 customers typed their country into a free-text box
- **The truth** — 20 are from the United States, 10 from the UK, 5 from Canada
- **The spellings** — the 20 Americans wrote "US" (8), "USA" (6), "United States" (4), "us " (2)
- **The computer's view** — four different strings, so four different countries
- **The sneaky one** — "us " carries a trailing space; even "us" would not match it

*Example:* Nobody typed anything wrong — each of the four spellings is a perfectly reasonable way to name the same country.

**Key point:** Categories typed by humans splinter. The category list you see is not the real set of categories — it is the union of everyone's typing habits.

### Visualization (canvas `c1`, 720×300)

Drawn distinct-values table with counts, US-family rows tinted alike, plus right-side annotations.

- **Title (bold 15px, `#1a5276`, top center):** "Distinct Values in the Country Column (with counts)".
- **Table:** at x=70, y=44; row height 30; value column 190px, count column 80px; muted bold 12px headers "value (printed in quotes)" and "count"; grid borders `#e5e9ef`.
- **Rows (value / count / US-family flag):** `'UK'` 10 (no), `'US'` 8 (yes), `'USA'` 6 (yes), `'Canada'` 5 (no), `'United States'` 4 (yes), `'us '` 2 (yes). US-family rows get blue tint `rgba(42,120,214,0.12)` and bold 13px monospace blue (`#2a78d6`) values; others plain 13px monospace `#2c3e50`.
- **Right-side notes (x=400, left-aligned):** blue bold 14px "the 4 highlighted rows are one country" (y=90); plain 13px "8 + 6 + 4 + 2 = 20 American customers" (y=116); orange (`#d95926`) bold 13px two lines "'us ' ends with an invisible space —" / "it would not even match \"us\"" (y=152/172); magenta (`#d55181`) bold 13px "6 rows on screen, 3 real categories" (y=214); muted 12px "the computer counts strings, not countries" (y=236).

## The Group-By That Crowned the Wrong Winner

**Tags:** `worked example` (green)

- **The question** — "which country has the most customers?"
- **Raw group-by** — UK 10, "US" 8, "USA" 6, Canada 5, "United States" 4, "us " 2
- **Raw answer** — UK wins with 10; the US seems second at best
- **Clean group-by** — map all four spellings to US: US 20, UK 10, Canada 5
- **Clean answer** — the US has twice the UK's customers

*Example:* 8 + 6 + 4 + 2 = 20 — every piece was on screen, yet the raw table reported them as four separate countries.

**Key point:** Group-by trusts the strings exactly as they are. It will never volunteer that four of its groups are one thing.

### Visualization (canvas `c2a`, 310×300)

Horizontal bar chart: raw group-by where UK appears to win.

- **Title (bold 14px, `#1a5276`, top center):** "Raw group-by: UK \"wins\"".
- **Bars (labels right-aligned in 12px monospace, values bold to the right of each bar):** `'UK'` 10, `'US'` 8, `'USA'` 6, `'Canada'` 5, `'United States'` 4, `'us '` 2. Scale max 22. US-family bars (`US`, `USA`, `United States`, `us `) blue `#2a78d6`; UK magenta `#d55181`; Canada aqua `#199e70`. Padding: top 46, bottom 62, left 120, right 36.
- **Annotations (centered, stacked above the caption):** magenta bold 12px "headline: \"top country: UK, 10\""; blue bold 12px "blue bars = one country, split four ways".
- **Caption (bottom center, muted 12px):** "customers per typed value".

### Visualization (canvas `c2b`, 310×300)

Horizontal bar chart: clean group-by where US wins by 2x.

- **Title (bold 14px, `#1a5276`, top center):** "After cleanup: US wins by 2x".
- **Bars (32px tall):** `'US'` 20 blue `#2a78d6`, `'UK'` 10 magenta `#d55181`, `'Canada'` 5 aqua `#199e70`. Scale max 22. Padding: top 46, bottom 62, left 76, right 36.
- **Annotations (left-aligned at the bar baseline x):** blue bold 13px "8+6+4+2 reunited = 20" under the US bar; orange bold 12px "same data, opposite headline" under the UK bar.
- **Caption (bottom center, muted 12px):** "customers per real country".

## Why a Data Scientist Cares

**Tags:** `where it's used` (blue), `downstream damage` (orange)

- **Dashboards mislead** — "top country: UK" goes into a slide deck and gets believed
- **Joins go quiet** — a shipping table keyed on "US" matches only 8 of the 20 American rows
- **Filters undercount** — WHERE country = 'US' silently returns 40% of the real US
- **One-hot bloat** — a model gets four US columns, each too small to learn from
- **Rare-category traps** — "us " (2 rows) gets bucketed into "Other" and vanishes

*Example:* A shipping-cost join keyed on 'US' left 12 American customers with blank shipping — all downstream of one messy column.

**Key point:** Every downstream tool — joins, filters, models, charts — inherits the splintering and multiplies it. The error never announces itself; things just quietly shrink.

### Visualization (canvas `c3`, 720×300)

Join diagram: a small lookup table on the left, a 2×10 grid of 20 squares in the middle showing matched vs unmatched rows.

- **Title (bold 15px, `#1a5276`, top center):** "A Join Keyed on \"US\" Meets the Four Spellings".
- **Lookup table (x=55, y=62, header "shipping lookup table" in muted bold 12px):** three rows of 100px + 50px cells, 28px tall, grid borders `#e5e9ef`, 12px monospace: `'US'` $5, `'UK'` $9, `'Canada'` $7.
- **Row grid (x=300, y=66, header "the 20 American customer rows"):** 20 squares (26px cells, 8px gaps, 10 per row); first 8 filled solid green `#008300` (matched), remaining 12 filled `rgba(217,89,38,0.25)` with 1.5px orange `#d95926` outline (unmatched).
- **Legend lines (bold 12px, left-aligned under grid):** green "8 matched: typed exactly 'US' → shipping $5"; orange "12 unmatched: 'USA', 'United States', 'us ' → shipping blank".
- **Bottom annotations (centered):** magenta bold 13px "12 of 20 rows fell out of the join — no error, no warning, just blanks" (y=230); muted 12px "every chart built on the joined table now describes 40% of the real US" (y=254).

## The Fix — and the Confusion That Defeats Eyeballing

**Tags:** `rule of thumb` (blue), `common mistake` (red)

- **Step 1: look** — print the distinct values WITH counts before trusting any group-by
- **Step 2: normalize** — trim spaces and fix case: "us " becomes "US" for free
- **Step 3: map** — keep a small mapping table: "USA" → "US", "United States" → "US"
- **The confusion** — eyeballing the list misses "us " because the space is invisible
- **Trust counts, not eyes** — two lines that print alike but count separately = hidden whitespace

*Example:* A counts list showing both us (2) and US (8) as separate lines is the smoking gun a scroll-through never shows.

**Rule of thumb:** Look, normalize, map — and the mapping table is data: save it, review it, reuse it on the next load.

### Visualization (canvas `c4`, 720×300)

Three-stage cleanup funnel: three outlined boxes connected by arrows, each listing the distinct values and counts at that stage.

- **Title (bold 15px, `#1a5276`, top center):** "Look → Normalize → Map: 6 Values Become 3 Real Categories".
- **Stage boxes:** 196×178 outlined boxes at y=48, 2px colored borders, bold 13px colored title, 11px muted subtitle, then 12px monospace value/count lines:
  - Box 1 (x=30, orange `#d95926`): "1. raw: 6 values" / "print distinct + counts" — lines: "UK ........ 10", "US ........  8", "USA .......  6", "Canada ....  5", "United S...  4", "us  .......  2".
  - Box 2 (x=262, yellow `#c98500`): "2. trim + case: 5" / "'us ' merges into US" — lines: "UK ........ 10", "US ........ 10", "USA .......  6", "Canada ....  5", "United S...  4".
  - Box 3 (x=494, green `#008300`): "3. mapping table: 3" / "USA, United States → US" — lines: "US ........ 20", "UK ........ 10", "Canada ....  5".
- **Arrows:** muted gray (`#6b7280`) 2px arrows with filled triangular heads between box 1→2 and 2→3, at mid-height.
- **Bottom annotations (centered):** magenta bold 13px "step 2 is free; step 3 is a saved, reviewable mapping table — not one-off edits" (y=258); muted 12px "counts never change (35 customers throughout) — only the labels heal" (y=280).

## Regeneration instructions

- **Template:** tutorial topic page (tutorials/CLAUDE.md conventions). `<h1>` concept name, `.subtitle`, four `.card-section` blocks each `<h2>` + `table.layout`. Every section uses two columns (`td.text-col` 50% / `td.viz-col` 50%); sections 1, 3, 4 hold one 720×300 canvas. One section places canvases `c2a`/`c2b` (310×300 each) side by side inside its single viz cell, wrapped in a `.viz-pair` flex row (`display:flex; gap:10px`, each canvas `flex:1 1 0; min-width:0`).
- **Left column structure per section:** `.tags` pill row, `<ul>` of one-line bullets with `<b>` lead terms (colored `#1a5276`), italic `.example` line, `.key-point` callout (background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem) with a bold lead-in ("Key point:" / "Rule of thumb:").
- **Tag pill CSS:** inline-block, 0.72rem, weight 600, padding 2px 10px, radius 10px; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` bottom border; h2 1.3rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem; canvas `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic sizes as given per chart, scaled by `window.devicePixelRatio` via a shared `setup(id)` helper reading the width/height attributes.
- **Chart palette object `P`:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- Card links in regenerated HTML (if referenced from grids) use `.html` extensions.
