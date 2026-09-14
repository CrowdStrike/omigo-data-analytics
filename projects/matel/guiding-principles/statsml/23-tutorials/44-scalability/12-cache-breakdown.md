# Cache Breakdown

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Cache Breakdown

**Subtitle:** A cache absorbing 95% of reads leaves the database seeing only 5% — so a small hit-rate dip, an expiring hot key, or a cold restart can multiply backend load overnight

## The Database Behind the Shield

**Tags:** `core idea` (blue), `load shield` (green), `caching` (orange)

- **The setup** — a product-page cache sits in front of a database; the site serves 10,000 reads/s
- **The shield** — 95% of reads are answered from cache; the database sees only the 500/s that miss
- **The sizing trap** — the database is provisioned for 500 reads/s, not the 10,000 the site takes
- **The dependence** — the site's real capacity lives in the cache; the database alone cannot carry it
- **The definition** — a cache failure mode is anything that lets the hidden 9,500/s reach the database

*Example (italic):* At 2pm the site serves 10,000 product-page reads/s; 9,500 come straight from cache and the database quietly handles 500.

**Key point:** A 95% hit rate means the database sees 5% of traffic — the cache is not just a speed-up, it is a load shield the database can no longer live without.

### Visualization (canvas `c1`, 720×300)

Flow diagram: incoming traffic splitting into a wide cache stream and a thin database stream, with proportional bar thickness.

- **Title (bold 15px, `#1a5276`, top center):** "10,000 Reads/s In, Only 500 Reach the Database".
- **Source box:** blue `#2a78d6` rounded box (170×44, 8px radius, fill `rgba(42,120,214,0.15)`) at x=40, y=130, 13px `#2c3e50` label "clients — 10,000 reads/s".
- **Cache stream:** thick horizontal band (height 76px, fill `rgba(0,131,0,0.30)`, 2px `#008300` edge) from x=210 to a green box (170×44) at x=430, y=80 labeled "cache — 9,500/s (95% hit)"; bold 13px green label "9,500/s served from memory" above the band.
- **DB stream:** thin band (height 4px, fill `rgba(217,89,38,0.55)`) from x=210 down to an orange `#d95926` box (170×44) at x=430, y=210 labeled "database — 500/s (5% miss)".
- **Band heights:** 76px vs 4px — drawn proportional to 9,500 vs 500 (ratio 19:1).
- **Annotation (bold 13px violet `#4a3aa7`, near x=470, y=270):** "the database is sized for the thin stream".
- **Caption (12px `#444`, bottom right):** "split percentages exact; 10,000 reads/s illustrative".

## Drop 5 Points of Hit Rate, Double the Database Load

**Tags:** `worked example` (blue), `exact arithmetic` (green)

- **The miss rate** — database load is set by misses, not hits: DB reads/s = traffic × (1 − hit rate)
- **At 95%** — misses are 5% of 10,000/s, so the database serves 500 reads/s
- **At 90%** — misses are 10% of 10,000/s, so the database serves 1,000 reads/s — exactly double
- **Hand-check** — 10,000 × 0.05 = 500 and 10,000 × 0.10 = 1,000; the doubling is plain arithmetic
- **The cliff** — 80% hit rate is 4× the baseline database load; 50% is 10×; small dips compound fast

*Example (italic):* A dashboard shows the hit rate slipping 95% → 90% and everyone shrugs — the database just watched its load double from 500 to 1,000 reads/s.

**Key point:** Backend load scales with the miss rate, so 95%→90% is not a 5% change — it doubles the database's work. This multiplication is exact; only the 10,000/s traffic figure is illustrative.

### Visualization (canvas `c2`, 720×300)

Vertical bar chart: database reads/s at five hit rates, showing the miss-rate multiplier at a fixed 10,000 reads/s of traffic.

- **Title (bold 15px, `#1a5276`, top center):** "Same Traffic, Falling Hit Rate: Database Load at 10,000 Reads/s".
- **Axes:** origin x=70, baseline y=245, plot width 590, plot height 180; y = DB reads/s 0 to 5,000, gridlines `#e5e9ef` at 1,000/2,000/3,000/4,000 with 12px `#444` tick labels.
- **Bars (5, width 72px, evenly spaced starting x=110):** hit rates `["99%", "95%", "90%", "80%", "50%"]`, DB reads/s `[100, 500, 1000, 2000, 5000]`; fills: 99% and 95% green `rgba(0,131,0,0.35)`, 90% orange `rgba(217,89,38,0.45)`, 80% and 50% red `rgba(231,76,60,0.45)`; 2px solid top edges in the matching solid color.
- **Value labels:** bold 12px `#2c3e50` above each bar: "100", "500", "1,000", "2,000", "5,000".
- **Multiplier labels (bold 12px `#4a3aa7`, inside or above bars):** "0.2×", "1× baseline", "2×", "4×", "10×".
- **Annotation (bold 13px red `#e74c3c`, arrow from the 95% bar to the 90% bar, near y=90):** "5 points of hit rate = 2× database load".
- **Caption (12px `#444`, bottom right):** "bar heights exact arithmetic; 10,000 reads/s illustrative".

## One Hot Key Expires and the Herd Arrives

**Tags:** `why it matters` (blue), `stampede` (red), `mitigations` (green)

- **The hot key** — one bestseller's page is read 800 times/s, all served from a single cached entry
- **The expiry** — its 60s TTL lapses; the rebuild query takes 50ms, so ~40 requests miss together
- **The stampede** — all 40 run the same expensive query at once; the database slows, so more pile on
- **Coalescing** — a lock lets one request rebuild the entry while the other 39 wait for its answer
- **Staggered TTLs, early refresh** — TTL jitter and refresh-before-expiry stop synchronized misses

*Example (italic):* The bestseller's cache entry expires at 2:03pm; without a lock, the database receives 40 identical rebuild queries within 50ms (800/s × 0.05s).

**Key point:** Expiry synchronizes misses — every concurrent reader of a hot key misses at the same instant. Request coalescing turns 40 identical rebuilds into 1 query plus 39 waiters.

### Visualization (canvas `c3`, 720×300)

Timeline chart of total database reads/s across 10 seconds around a hot-key expiry: uncontrolled stampede vs request coalescing, on a shared time axis.

- **Title (bold 15px, `#1a5276`, top center):** "Hot Key Expires at t=3s: Stampede vs Request Coalescing".
- **Axes:** origin x=70, baseline y=245, plot width 590, plot height 180; x = seconds 0 to 10 with 12px `#444` tick labels every 2s; y = DB reads/s 0 to 2,500, gridlines `#e5e9ef` at 500/1,000/1,500/2,000.
- **Stampede line:** red `#e74c3c` 3px line through seconds `[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]`, DB reads/s `[500, 502, 498, 1300, 2200, 1500, 850, 600, 520, 505, 500]` — spike at expiry, then a pile-on peak as the slowed database makes rebuilds take longer.
- **Coalescing line:** green `#008300` 3px line through the same seconds, DB reads/s `[500, 502, 498, 501, 500, 503, 499, 501, 500, 502, 500]` — flat; one request rebuilds, the rest wait.
- **Expiry marker:** vertical dashed `#6b7280` (dash 4/3) line at t=3, 12px `#6b7280` label "TTL lapses" at its top.
- **Annotation (bold 13px red `#e74c3c`, near t=4.5s, y=70):** "40 identical queries, then the pile-on"; bold 12px green `#008300` near t=8s, y=210: "coalesced: 1 rebuild, 39 waiters".
- **Caption (12px `#444`, bottom right):** "stampede curve illustrative; 40 = 800/s × 50ms exact".

## Restarting the Cache Is a Stampede You Scheduled

**Tags:** `common mistake` (red), `cold cache` (orange)

- **The restart** — a deploy flushes the cache node; every entry is gone, so the hit rate is briefly 0%
- **The math** — at 0% hit rate the database sees all 10,000 reads/s — 20× its steady 500/s
- **The warm-up** — entries refill one miss at a time; load falls back to 500/s only over minutes
- **The mistake** — sizing the database for steady state and treating a cache flush as harmless
- **The fixes** — pre-warm before taking traffic, restart nodes one at a time, keep TTLs staggered

*Example (italic):* A 2:03pm deploy bounces the cache; the database absorbs 10,000 reads/s cold — 20× its normal 500/s — until the entries refill over the next few minutes.

**Common mistake:** Treating the cache as an optimization you can bounce freely. A cold cache sends 100% of traffic to a database sized for 5% — the hot-key stampede, self-inflicted and at full width.

### Visualization (canvas `c4`, 720×300)

Line chart of database reads/s in the minutes after a cache flush: a 20× spike at t=0 decaying back to the 500/s steady state as the cache warms.

- **Title (bold 15px, `#1a5276`, top center):** "Cold Cache at t=0: 20× Load Until the Entries Refill".
- **Axes:** origin x=70, baseline y=245, plot width 590, plot height 180; x = minutes after flush 0 to 8 with 12px `#444` tick labels every 2 min; y = DB reads/s 0 to 10,000, gridlines `#e5e9ef` at 2,500/5,000/7,500.
- **Warm-up line:** orange `#d95926` 3px line through minutes `[0, 1, 2, 3, 4, 5, 6, 7, 8]`, DB reads/s `[10000, 5800, 3200, 1700, 950, 640, 530, 505, 500]` — steep decay as popular entries refill first.
- **Steady-state reference:** horizontal dashed green `#008300` (dash 4/3) line at 500 reads/s, 12px green label "steady state — 500/s (95% hit)" at its right end.
- **Peak marker:** bold 13px red `#e74c3c` annotation at (t≈0.4, y≈45): "20× steady load — every read is a miss".
- **Annotation (bold 12px violet `#4a3aa7`, near t=5, y=170):** "warm-up is a stampede in slow motion".
- **Caption (12px `#444`, bottom right):** "20× = 100%/5% miss ratio, exact; warm-up curve illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness). The hit-rate → database-load multiplication is exact arithmetic (miss rate × traffic: 500 / 1,000 / 2,000 / 5,000 at 10,000 reads/s; the 2× for 95%→90% and the 20× cold-cache ratio are exact); the 10,000 reads/s traffic figure, the 800/s hot key, and the stampede/warm-up curves are invented and labeled illustrative. 40 concurrent misses = 800/s × 50ms is exact given those inputs.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
