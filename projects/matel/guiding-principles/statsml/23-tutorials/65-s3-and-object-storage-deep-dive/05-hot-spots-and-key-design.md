# Hot Spots & Key Design

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Hot Spots &amp; Key Design

**Subtitle:** A key that starts with the time sends every write to the same place; putting something varied at the front spreads the same load across many places

## Why a Timestamp Key Sends Every Write to One Place

**Tags:** `core idea` (blue), `sorted keys` (green), `one busy range` (orange)

- **The stream** — a fleet of sensors writes 12,000 small files a second into one bucket
- **The key** — every object's name starts with the hour first, then the device id
- **Sorted, not hashed** — the service splits the whole key space into sorted ranges
- **One end only** — a clock-led name only ever grows, so all writes stay at one end
- **The hot range** — the range holding hour 14 takes all 12,000 writes a second on its own
- **The idle rest** — every one of the other seven ranges takes no writes at all
- **It moves, never spreads** — at 15:00 the load shifts to the next range, alone
- **Same trap elsewhere** — a key that only counts upward in a sorted database does the same

*Example (illustrative):* At 14:59 the hour-14 range is saturated while the hour-13 range beside it does nothing; at 15:00 the load simply moves one slot over.

**Key point:** Sorted ranges plus a name that only grows means exactly one busy range. More hours add more key space, never more places to write at once.

### Visualization (canvas `c1`, 720×300)

Three stacked horizontal bars, one per consecutive hour, each showing the sorted key space divided into 8 ranges, with an arrow marking the single active range sliding one slot right per hour.

- **Title (bold 16px, `#1a5276`, top center at y=24):** "The Hot Range Moves, It Never Spreads".
- **Annotation (bold 13px violet `#4a3aa7`, left-aligned at (150, 52)):** "8 ranges, 1 doing work — every hour".
- **Layout:** `barX = 150`, `barW = 520`, `barH = 34`, 8 cells so `cellW = 65`; cell left edge is `cellX(i) = barX + i × cellW`. Rows at y = 78, 150, 222 with 12px `#444` right-aligned labels at x=138, vertically centred (`y + barH/2 + 4`): "13:00", "14:00", "15:00".
- **Ranges:** every cell filled `rgba(107,114,128,0.10)` with a 1px `#e5e9ef` outline; 11px `#6b7280` "idle" centred in every cold cell. A 1px `#6b7280` 4px-radius `roundRect` outlines the whole 8-cell strip of each row.
- **Hot cell:** index `5 + rowIndex`, so row 0 → 5, row 1 → 6, row 2 → 7; fill solid red `#e74c3c`, bold 12px white centred label formatted from the single `TOTAL = 12000` constant → "12,000/s".
- **Hot arrow:** 3px red `#e74c3c` vertical arrow above each hot cell, from `y − 22` down to the bar's top edge, centred on the hot cell; bold 12px red "all writes" above the first row's arrow only.
- **Key-order axis:** 1px `#6b7280` line at y=268 from x=150 to x=670 with 12px `#6b7280` labels at y=284 — "keys sorted low → high" left-aligned at x=150, "newest keys" right-aligned at x=670.
- **Caption (12px `#444`, bottom right at y=296):** "write rate illustrative; splitting the key space into sorted ranges is documented behaviour".

## Counting the Writes That Land on One Prefix

**Tags:** `worked example` (blue), `3,500 a second` (orange), `per-prefix limit` (green)

- **The documented limit** — about 3,500 writes a second on any single key prefix
- **Hour-first** — all 12,000 writes hit that one prefix, which is 3.43× over the limit
- **The symptom** — the writes above the limit come back as slow-down errors, not failures
- **How many** — roughly 8,500 writes a second must be retried or buffered somewhere
- **Spread it** — put one hash character of the device id first, giving 16 prefixes
- **Now each one** — 12,000 / 16 = 750 writes a second, just 21.4% of the limit
- **Headroom** — 3,500 / 750 = 4.67×, so even a 4× traffic spike still fits under it
- **Hand-check** — 16 × 750 = 12,000, the whole stream accounted for and none over

*Example (illustrative):* The same 12,000 writes a second is a 3.43× overload on one prefix and a calm 750 each across sixteen — only the first character of the key changed.

**Key point:** What throttles you is the rate per prefix, which is the total rate divided by the number of active prefixes. An hour-first key holds that divisor at 1 forever.

### Visualization (canvas `c2`, 720×300)

Two side-by-side bar panels of writes per second per prefix across 16 prefixes, sharing one y scale and one 3,500 limit line: left is hour-first (one red spike far over the line), right is hash-first (16 short even bars far under it).

- **Title (bold 16px, `#1a5276`, top center at y=24):** "Writes per Prefix: 12,000 on One vs 750 on Sixteen".
- **Constants:** `TOTAL = 12000`, `LIMIT = 3500`, `N = 16`, `PER = TOTAL / N` (= 750). Every printed figure derives from these at render time: `TOTAL/LIMIT` → "3.43×" and `PER/LIMIT` → "21.4%". The 4.67× headroom figure is printed in `c3`, not here.
- **Shared y scale:** 0 to 13,000, plot height 175, baseline y=250, so `py(v) = 250 − (v / 13000) × 175`; gridlines `#e5e9ef` at 3,000 / 6,000 / 9,000 / 12,000 drawn across both panels with 12px `#444` right-aligned labels "3k" / "6k" / "9k" / "12k" at x=52. 1px `#999` baseline under each panel.
- **Left panel:** origin x=60, width 275; 16 slots of `275/16`, bar centres at `60 + (i + 0.5) × slot`, bars 12px wide. Only slot 14 (the current hour's prefix) is active, at `TOTAL`; it is solid red `#e74c3c` with a bold 12px red label above it formatted from `TOTAL` → "12,000/s". The other 15 slots are drawn as a 2px `#e5e9ef` stub on the baseline. Bold 13px `#2c3e50` panel label "hour-first" centred at y=272.
- **Right panel:** origin x=400, width 275, same y scale; 16 bars all at `PER`, fill `rgba(42,120,214,0.45)` with a 1px `#2a78d6` edge; bold 12px `#2a78d6` label above them at y=205 formatted from `PER` → "750/s each". Bold 13px `#2c3e50` panel label "hash-first (one hash character, 16 prefixes)" centred at y=272.
- **Limit line:** dashed red `#e74c3c` (dash 5/4) at `py(LIMIT)` across BOTH panels, with a bold 12px red centred label at (250, `py(LIMIT) − 8`) reading "3,500 writes/sec per prefix (documented)".
- **Verdicts:** bold 13px red "✗ 3.43× over — throttled" centred at (200, 90); bold 13px green `#008300` "✓ 21.4% of the limit" centred at (537, 90). Both numbers computed, not typed.
- **Divider:** 1px `#e5e9ef` vertical line at x=352 from y=60 to y=250.
- **Caption (12px `#444`, bottom right at y=296):** "12,000 writes/sec illustrative; 3,500 per prefix is documented".

## Choosing How Many Prefixes to Spread Across

**Tags:** `derive, don't guess` (blue), `rule of thumb` (green), `not free` (orange)

- **The rule** — the total write rate divided by the prefix count must stay under 3,500
- **Do the division** — 12,000 / 3,500 = 3.43, so you need at least 4 prefixes to clear it
- **Leave room for bursts** — a 2× burst needs 6.86 prefixes, so round that up to 8
- **One hash character** — gives 16 prefixes, clearing the limit with room to spare
- **Two characters** — gives 256 prefixes and only 46.9 writes each, far more than needed
- **Not free** — a hash at the front destroys the neat time order of the key names
- **Often unneeded** — the service now splits a busy range on its own as load arrives
- **Measure first** — spread only for a real sustained write rate, not as a precaution

*Example (illustrative):* At 12,000 writes a second the answer is 16 prefixes, not 256 — and 16 comes from dividing 12,000 by 3,500 and rounding up with room to spare.

**Key point:** Divide your measured peak write rate by 3,500, round up to the next power of two, then allow for bursts. That is the whole calculation — copying "use two hash characters" just swaps one guessed number for another.

### Visualization (canvas `c3`, 720×300)

Log-scale bar chart of writes per second per prefix against the number of prefixes N, with the 3,500 limit drawn across it, bars above the line in red and bars below in blue, and the first N that clears the line annotated.

- **Title (bold 16px, `#1a5276`, top center at y=24):** "Writes per Prefix = 12,000 / N: Where the Limit Is Cleared".
- **Axes:** origin x=70, baseline y=248, plot width 590, plot height 185; log10 y scale from 1 to 100,000 — `py(v) = 248 − (log10(v) / 5) × 185`; gridlines `#e5e9ef` at 1 / 10 / 100 / 1,000 / 10,000 / 100,000 with 12px `#444` right-aligned labels "1" / "10" / "100" / "1k" / "10k" / "100k" at x=62. 1px `#999` left axis and baseline.
- **Bars:** the N list is the literal `[1, 2, 4, 8, 16, 256, 4096]` and every bar's height is **computed** as `TOTAL / N` with `TOTAL = 12000` — 12,000 / 6,000 / 3,000 / 1,500 / 750 / 46.875 / 2.9296875. Seven slots of `590/7`, bars 46px wide, centred at `70 + (i + 0.5) × slot`.
- **Bar colours:** value over `LIMIT = 3500` → solid red `#e74c3c` (N = 1, 2); under the limit but with less than 4× headroom, i.e. `LIMIT / value < 4` → fill `rgba(201,133,0,0.45)` with 1px `#c98500` edge (N = 4 at 1.17×, N = 8 at 2.33×); otherwise fill `rgba(42,120,214,0.45)` with 1px `#2a78d6` edge (N = 16 at 4.67×, and 256, 4096). The banding is derived from the computed value, not hardcoded per bar.
- **Value labels (bold 12px in the bar colour, centred 7px above each bar):** formatted from the computed value — ≥100 rounded with a thousands separator ("12,000", "6,000", "3,000", "1,500", "750"), ≥10 to one decimal ("46.9"), below 10 to two ("2.93").
- **X labels (12px `#444`, centred at y=266):** "N=1", "N=2", "N=4", "N=8", "N=16", "N=256", "N=4096"; a second line 11px `#6b7280` at y=282 under the last three: "1 hex", "2 hex", "3 hex".
- **Limit line:** dashed red `#e74c3c` (dash 5/4) at `py(3500)` across the plot, with a bold 12px red right-aligned label "3,500 writes/sec limit" at (660, `py(3500) − 7`).
- **Annotations:** bold 13px `#c98500` centred at (300, 118) naming the first N whose computed value clears the limit and that value — "N=4 is the first that clears it (3,000)" — with a 2px `#c98500` leader line from (300, 126) to that bar's top; bold 13px green `#008300` centred at (470, 160) reading "N=16 → 4.67× headroom", the multiple computed as `LIMIT / (TOTAL / 16)`.
- **Caption (12px `#444`, bottom right at y=296):** "every bar is an exact division of 12,000; the limit is documented".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `23-tutorials/CLAUDE.md`), matching the sibling `11-cross-region-replication.html` in this folder. Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then **three** `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>Key point:</strong>` label). No `<code>` spans — the plain wording replaced them.
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links, no cross-page links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Helpers `roundRect`, `arrowHead` and `rgba` as in page 11. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Red `#e74c3c` is reserved for the genuinely throttled state: the hot range in `c1`, the over-limit spike and the limit line in `c2`, the over-limit bars and the limit line in `c3`.
- **Bullet length — this is the deliberate calibration of the page.** Bullets run roughly 75–90 characters including the bold lead term, holding one line at the 50/50 split while still carrying a full clause. An earlier pass clipped them to ~55–70 characters and was rejected for compromising quality — the bullets read as stubs. Do not shorten them further, and do not pad them back out to the folder's ~90–100 default either.
- **Register and vocabulary.** Plain technical English for a first-time reader. No analogies and no invented scenes. Say "writes a second", not PUT/COPY/POST/DELETE per second; say "slow-down errors", not `503 SlowDown`; say "each name starts with the hour, then the device id", not a literal key template; say "the service splits keys into sorted ranges", not range-partitioning by lexicographic order; say "one hash character in front", not `hash(device-id)` in hex. The precise key syntax and API names live on the sibling pages.
- **Deliberate omissions — do not reintroduce on regeneration.**
  - **No listing-trade-off section.** A fourth section weighed write throughput against listing locality (the 43,200,000-key hour versus the 1,036,800,000-key day scan, the flat 24× penalty, the write-ceiling-versus-selectivity `c4` panels, manifests and table formats); it was cut for making the page longer than a tutorial needs. Listing cost and table formats belong on the listing and data-layout pages, not here.
  - **No reversed-key or UUID-prefix alternatives, and no ranking of the three fixes.** They travelled with that section and are design trivia at this level.
  - **No cost or throttling-retry mechanics.** Retry budgets and backoff belong on the request-and-error pages.
  - Two short bullets in section three keep the page honest — that a hash in front gives up time order, and that the service splits busy ranges on its own — so the page never reads as "always hash". Keep those two; do not expand them back into a section.
  - **Three sections, 8 short bullets each, one canvas each (`c1`, `c2`, `c3`).**
- **Data:** all values are hardcoded literals, no randomness anywhere.
  - **Documented behaviour the page stands on:** the key space is split into sorted key ranges rather than by hashing; the write limit is about 3,500 writes a second per prefix; excess requests are rejected with a slow-down error; busy ranges are split automatically by the service.
  - **Illustrative and labelled as such:** the 12,000 writes a second stream, the 8-range picture, the hour-then-device-id key shape, and the choice of slot 14 as the current hour's prefix.
  - **Computed at render time, not typed:** `c2`'s "3.43×" is `12000/3500`, its "21.4%" is `750/3500`, and both bar labels come from `TOTAL` and `PER`; `c3`'s seven bar heights are all `12000 / N` from the literal N list, its colour banding is derived from those values against the 3,500 limit, its value labels are formatted from them, the "first N that clears it" annotation scans the same list, and its "4.67× headroom" is `3500 / (12000/16)`.
  - **Arithmetic (all exact):** 12,000 / 3,500 = 3.4286 → at least 4 prefixes; 2 × 12,000 / 3,500 = 6.857 → round up to 8; 12,000 − 3,500 = 8,500 a second over; 12,000 / 1 = 12,000; / 2 = 6,000; / 4 = 3,000; / 8 = 1,500; / 16 = 750; / 256 = 46.875; / 4,096 = 2.9296875; 750 / 3,500 = 21.43%; 3,500 / 750 = 4.667; 16 × 750 = 12,000; one hex character = 16 prefixes, two = 256, three = 4,096.
- This page has no links; in regenerated HTML any card links would use `.html` extensions.
