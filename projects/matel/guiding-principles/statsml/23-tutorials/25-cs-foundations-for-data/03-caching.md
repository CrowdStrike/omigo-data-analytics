# Caching

**Page type:** detail page (tutorial layout: h1 + subtitle, then four `.card-section` blocks each with an h2 and a `table.layout` — text column 50% left with tag pills / bullets / example / key-point, viz column 50% right with one 720×300 canvas)
**HTML title tag:** Caching

**Subtitle:** The same "top products" question asked 10,000 times an hour gets the same answer — remember it for 60 seconds and the database barely works at all

## The Question Everyone Keeps Asking

Tags: `core idea` (blue), `running example` (green)

- **The setup** — a shop's homepage shows "top 10 products"; 10,000 visitors load it each hour
- **Without a cache** — every load runs the same heavy database query, 10,000×/hour
- **The observation** — the answer barely changes from one second to the next
- **With a cache** — compute once, remember for 60 s, hand the saved copy to everyone else
- **The result** — at most 60 real queries per hour; the other 9,940 are free

*Example:* 9,940 of 10,000 requests never touch the database — a 99.4% drop in load.

**Key point:** A cache is just remembering an answer instead of recomputing it — worth it whenever the same question repeats.

### Visualization (canvas `c1`, 720×300)

Flow diagram: requests → cache → database, with a return path.

- **Title (bold 15px, `#1a5276`, centered):** "One Hour of Homepage Traffic Through a 60 s Cache".
- **Boxes (150×70, tinted fill + 2px colored border, bold 12px two-line centered labels):**
  - (40, 110): "10,000 requests" / "per hour" — blue `#2a78d6`, fill `rgba(42,120,214,0.12)`.
  - (290, 110): "cache" / "remembers 60 s" — aqua `#199e70`, fill `rgba(25,158,112,0.12)`.
  - (540, 110): "database" / "200 ms per query" — magenta `#d55181`, fill `rgba(213,81,129,0.12)`.
- **Arrows (filled arrowheads):** blue 3px from requests to cache; magenta 2px from cache to database; green `#008300` 3px return path going down from the cache, left under the boxes, and back up into the requests box.
- **Annotations (bold 13px):** green "9,940 answered from memory (99.4%)" at (250, 250); magenta "only 60 reach the database" at (487, 100) and "(one per minute)" at (487, 200); orange `#d95926` "same question 10,000× → computed 60×" bottom center (y=285).

## The Hour, Counted Out

Tags: `worked example` (green)

- **The query** — costs the database 200 ms of work each time it runs
- **Before** — 10,000 × 0.2 s = 2,000 s of database work per hour (33 min of every 60)
- **After** — 60 × 0.2 s = 12 s of database work per hour
- **Hit rate** — 9,940 ÷ 10,000 = 99.4% of requests answered from memory
- **Visitor speed** — a cache read is ~1 ms vs 200 ms: pages feel 200× snappier too

*Example:* The database goes from busy 55% of the hour to busy 0.3% of it — same site, same traffic.

**Key point:** Cache math is just counting: requests per refresh window × query cost is the work you stop paying.

### Visualization (canvas `c2`, 720×300)

Two before/after bar panels separated by a vertical dashed light-gray divider (`#bdc3c7`, dash 4/3) at x=360.

- **Title (bold 15px, `#1a5276`, centered):** "Database Load per Hour: Before vs After the Cache".
- **Left panel — "queries per hour" (bold 13px `#444` panel title; baseline y=230 with thin `#999` line, bar height scale 155px, bars 75px wide, alpha 0.75, minimum visual height 5px):** "before" 10,000 in magenta `#d55181` (bold 14px value label "10,000"); "after" 60 in green `#008300` (label "60"). Scale max 10,000.
- **Right panel — "database busy time per hour" (same styling):** "before" 2,000 s in magenta (bold 13px label "2,000 s (33 min)"); "after" 12 s in green (label "12 s"). Scale max 2,000.
- **Bottom line (bold orange `#d95926` 13px, centered, y=285):** "10,000 × 0.2 s = 2,000 s saved down to 60 × 0.2 s = 12 s".

## Where a Data Scientist Uses This Daily

Tags: `where it's used` (blue)

- **Dashboards** — BI tools cache query results; ten viewers, one query
- **Notebooks** — saving a cleaned dataframe to disk is caching the expensive prep step
- **Code** — `@lru_cache` memoizes a function: same inputs, remembered output
- **Feature stores** — precomputed features are a cache in front of raw event data
- **Model serving** — repeated identical inputs can reuse yesterday's prediction

*Example:* Re-running a notebook that re-reads and re-cleans 10 GB every time is a missing cache.

**Key point:** Anything expensive and repeated is a caching candidate — the question is only how long an old answer stays acceptable.

### Visualization (canvas `c3`, 720×300)

Paired horizontal bars: recompute time vs cached time for three scenarios.

- **Title (bold 15px, `#1a5276`, centered):** "Answer the Same Question Again: Recompute vs Remember".
- **Rows (bar track from x=280, width 330px, row height 62px from y=58; each row: bold 12px `#333` left label, a full-width recompute bar (16px tall, alpha 0.7) with its time at the end, and below it a green `#008300` cached bar scaled as fast/slow of the same track (minimum 4px, alpha 0.85) labeled "<fast> <unit> cached"):**
  - "heavy SQL query (dashboard)" — 200 ms recompute (blue `#2a78d6`) vs 1 ms cached.
  - "reclean 10 GB (notebook)" — 600 s recompute (violet `#4a3aa7`) vs 8 s cached.
  - "recompute features (pipeline)" — 90 min recompute (aqua `#199e70`) vs 2 min cached.
- **Bottom line (bold orange `#d95926` 13px, centered, y=278):** "illustrative times — the pattern is the point: repeated work collapses to a lookup".

## Staleness: The Price You Pay

Tags: `common mistake` (red), `trade-off` (orange)

- **The price** — a 60 s cache means any answer can be up to 60 s out of date
- **The scenario** — a price changes at t = 45 s; the cache keeps serving the old price until t = 60 s
- **Usually fine** — a 15 s stale "top products" list hurts nobody
- **Sometimes not** — stale account balances, inventory counts, or model features do hurt
- **Invalidation** — evicting the moment data changes is correct but famously hard to get right

*Example:* "Why does the dashboard disagree with the database?" — someone is reading a cache.

**Common mistake:** Treating cached numbers as live. Every cache trades freshness for speed — know the window, and size it per use case.

### Visualization (canvas `c4`, 720×300)

Step-line timeline: true price vs cached price with a shaded stale window.

- **Title (bold 15px, `#1a5276`, centered):** "A 60 s Cache Serving a Price That Changed at t = 45 s".
- **Axes (padding top 52, bottom 52, left 70, right 160; lines `#999`):** x = time 0..120 s with gray 12px tick labels "0s", "30s", "45s", "60s", "90s", "120s"; y = price 10..25 with tick labels "$20" and "$15".
- **Stale window:** shaded rectangle `rgba(231,76,60,0.12)` spanning t=45..60 for the full plot height.
- **True price (solid blue `#2a78d6`, 3px):** step line at $20 from t=0 to t=45, dropping to $15 at t=45, continuing to t=120.
- **Cached price (dashed orange `#d95926`, 3px, dash 7/5, drawn 4px above the true line):** stays at $20 until t=60, then drops to $15 to t=120 (refreshed at 0, 60, 120).
- **Refresh markers:** aqua `#199e70` 5px dots on the x-axis at t=0, 60, 120, with bold 12px label "cache refresh points" below the t=60 dot.
- **Annotation (bold red `#e74c3c` 13px, two lines near top of the stale window):** "15 s of wrong answers" / "— the price of a 60 s cache".
- **Legend (top-right, 12px):** solid blue line sample "true price"; dashed orange line sample "cached price".

## Regeneration instructions

- **Layout:** tutorial detail page. `<h1>` + `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px solid `#2980b9` bottom border) + `table.layout` (full width, border-collapse) with `td.text-col` 50% and `td.viz-col` 50%, one canvas per section.
- **Text column structure:** `.tags` pill row (0.72rem, weight 600, padding 2px 10px, radius 10px; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`), `<ul>` of one-line bullets with `<b>` lead terms in `#1a5276`, italic `.example` line (`#555`, 0.9rem), `.key-point` callout (background `#f8f9fa`, 3px `#e74c3c` left border, padding 8px 12px, 0.9rem). Inline `code`: ui-monospace, background `#f4f6f8`, padding 1px 4px, radius 3px.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem. Canvases `width: 100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Chart palette (JS object `P`):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Page palette anchors: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- **Canvas:** all canvases 720×300 logical; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Shared helpers: `box(...)` draws tinted boxes with 2px colored borders and multi-line bold 12px centered labels; `arrow(...)` draws lines with filled triangular arrowheads.
- No cross-page links; in regenerated HTML any card links elsewhere would use `.html` extensions.
