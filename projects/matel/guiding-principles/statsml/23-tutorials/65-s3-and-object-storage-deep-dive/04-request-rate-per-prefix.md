# Request Rate Per Prefix

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Request Rate Per Prefix

**Subtitle:** The request limit applies to each prefix on its own, not to the whole bucket — so how many requests a second you can make is decided by how your key names spread out

## One Bucket, 20,000 Writes a Second

**Tags:** `core idea` (blue), `per prefix` (green), `documented limits` (orange)

- **The setup** — one bucket must take 20,000 writes a second from an ingest service
- **Write limit** — about 3,500 writes a second for each prefix, documented by the store
- **Read limit** — about 5,500 reads a second for each prefix, on the same documented list
- **Per prefix** — the limit is per prefix, not for the whole bucket, so one cannot carry it
- **The shortfall** — one prefix serves 3,500 and leaves 16,500 a second refused
- **No cap on prefixes** — a bucket may hold as many prefixes as your keys create
- **No bucket limit** — nothing caps the bucket on top; it grows by adding prefixes
- **Decided early** — the ceiling follows from key names, chosen before any data lands

*Example (illustrative):* One prefix serves 3,500 of the 20,000 writes a second; the other 16,500 are refused.

**Key point:** The limit belongs to the prefix, not to the bucket. You buy throughput by spreading keys across more prefixes.

### Visualization (canvas `c1`, 720×300)

Horizontal bar chart: the per-prefix write and read limits drawn against the demand, so both demand bars run far past their limit bar.

- **Title (bold 15px, `#1a5276`, top center at y=24):** "One Prefix vs the Demand: Limits Against 20,000 Writes / 40,000 Reads".
- **Values (literals):** write limit 3,500; write demand 20,000; read limit 5,500; read demand 40,000.
- **Axes:** origin x=170, plot width 500, x scale 0 → 44,000 requests/sec, so `px(v) = 170 + v / 44000 × 500`. Vertical gridlines `#e5e9ef` at 10,000 / 20,000 / 30,000 / 40,000 from y=60 to y=258, 12px `#444` labels centered at y=272 ("10k"…"40k"); axis lines `#999` down x=170 and along y=258.
- **Bars (26px tall, 2px border, fill at 0.85 alpha), row centers y = 90, 128, 190, 228:**
  - write limit / prefix 3,500 → blue `#2a78d6`
  - write demand 20,000 → orange `#d95926`
  - read limit / prefix 5,500 → aqua `#199e70`
  - read demand 40,000 → violet `#4a3aa7`
- **Row labels (12px `#444`, right-aligned at x=160):** "write limit / prefix", "write demand", "read limit / prefix", "read demand".
- **Value labels (bold 12px, bar color, 8px right of each bar end):** the value formatted at render time with `toLocaleString('en-US')` plus "/s".
- **Demand marker lines:** dashed (4/3) 2px `#e74c3c` vertical at `px(20000)` from y=70 to y=150, and at `px(40000)` from y=170 to y=250.
- **Annotations (bold 13px, left-aligned at x=300, y=56 and y=166), both computed in JS:** demand ÷ limit to two decimals and `Math.ceil` of it — orange `#d95926` "20,000 ÷ 3,500 = 5.71 → 6 prefixes"; violet `#4a3aa7` "40,000 ÷ 5,500 = 7.27 → 8 prefixes".
- **Caption (12px `#444`, bottom right at y=294):** "3,500 and 5,500 per prefix are documented figures; the demand is illustrative".

## A Prefix Is a Slice of the Sorted Keys, Not a Folder

**Tags:** `common mistake` (red), `key names` (green), `the store decides` (orange)

- **Not a folder** — the folder view is only drawn from the slashes in your key names
- **Keys are sorted** — every key in the bucket sits in one long sorted list
- **A prefix is a slice** — one continuous stretch of that list, served together
- **The store decides** — it picks where one slice ends and the next one begins
- **Splits on load** — a slice under heavy load gets cut in two, on its own
- **Your lever** — keys spread across the list give it clean places to cut
- **Common error** — reading the folder view as though it were the real slice
- **In practice** — put a character that varies early in the key, before the date

*Example (illustrative):* Keys starting `00a7-` and `e7d5-` sit far apart in the sorted list, while two date folders under one busy path can sit in the same slice.

**Common mistake:** You do not create these slices. Your key names only decide whether the store can split them easily.

### Visualization (canvas `c2`, 720×300)

Key-list diagram: the sorted key list drawn as one long bar cut into three unequal prefixes, six sample keys ticked onto it, and a slash-based folder edge falling inside a prefix.

- **Title (bold 15px, `#1a5276`, top center at y=24):** "The Sorted Key List, Cut Into Prefixes by the Store — Not by Your Slashes".
- **Key-list bar (x=60 to x=680, y=100, height 46, 6px radius):** x maps the first key character 0x00–0xff linearly, `bx(b) = 60 + b / 256 × 620` (2.4219 px per step).
- **Cut points chosen by the store:** at 0x4c (76) → x=244.1 and 0xa0 (160) → x=447.5; drawn as 3px `#1a5276` vertical lines from y=92 to y=154 with bold 12px `#1a5276` "cut" centered above at y=88.
- **Prefix fills:** first x=60→244 `rgba(42,120,214,0.18)` border 2px `#2a78d6`; second x=244→448 `rgba(25,158,112,0.18)` border 2px `#199e70`; third x=448→680 `rgba(74,58,167,0.15)` border 2px `#4a3aa7`.
- **Prefix labels (bold 12px, prefix color, centered inside each fill at y=129):** "prefix 1", "prefix 2", "prefix 3"; below each in 12px `#444` at y=170 the range: "00… → 4c…", "4c… → a0…", "a0… → ff…".
- **Sample-key ticks (2px `#6b7280`, from y=190 up to y=148) at first-character positions:** `00a7-…` at 0x00, `3d12-…` at 0x3d, `5f88-…` at 0x5f, `8b04-…` at 0x8b, `c391-…` at 0xc3, `e7d5-…` at 0xe7; each labelled 12px `#2c3e50` centered, staggered y=204 / y=222, label x clamped to `[110, 630]` so nothing runs off the canvas.
- **Folder-edge marker:** dashed (5/4) 2px `#d95926` vertical at 0x77 (119) → x=348.2 from y=96 to y=248, with bold 12px orange `#d95926` label left-aligned at (356, 244) "a folder edge from a slash — ignored here".
- **Annotation (bold 13px green `#008300`, left-aligned at (60, 270)):** "the folder edge falls inside a prefix — the cuts are not yours".
- **Caption (12px `#444`, bottom right at y=294):** "keys and cut points illustrative; automatic splitting is documented behaviour".

## Dividing 20,000 Writes Across Prefixes

**Tags:** `worked example` (blue), `round up` (green), `take the larger` (orange)

- **The division** — 20,000 ÷ 3,500 = 5.71, so one prefix is nowhere near enough
- **Round up** — you cannot have 0.71 of a prefix, so the answer is 6 of them
- **Check it** — 6 × 3,500 = 21,000 a second, just above the 20,000 being asked for
- **Each prefix** — 20,000 ÷ 6 = 3,333.3 a second each, sitting under the limit
- **Spare room** — 3,500 − 3,333.3 = 166.7 a second left over on every prefix
- **Four is short** — 20,000 ÷ 4 = 5,000 each, which is 1,500 over the limit
- **Reads too** — 40,000 ÷ 5,500 = 7.27, so the reads want 8 prefixes, not 6
- **Take the larger** — 8 beats 6, so the key names have to give you 8 slices

*Example (illustrative):* At 6 prefixes each one carries 3,333.3 writes a second and fits; at 4 prefixes each carries 5,000 and every one hits the limit.

**Key point:** Divide the demand by the per-prefix limit and round up. Do it once for writes and once for reads, then take the larger of the two answers.

### Visualization (canvas `c3`, 720×300)

Vertical bar chart: the same 20,000 writes a second split across 1, 2, 4 and 6 prefixes, with the 3,500 limit drawn as a line that only the 6-prefix bar sits under.

- **Title (bold 15px, `#1a5276`, top center at y=24):** "20,000 Writes/sec Split Across Prefixes — Only 6 Stays Under 3,500".
- **Axes:** origin x=70, baseline y=248, plot width 590, plot height 190, y = 0 → 21,000, so `py(v) = 248 − v / 21000 × 190`. Gridlines `#e5e9ef` at 3,500 / 7,000 / 10,500 / 14,000 / 17,500 / 21,000 with 12px `#444` right-aligned labels at x=62; axis lines `#999`.
- **Bars:** four counts `[1, 2, 4, 6]`, each bar's value computed at render time as `20000 / n` → 20,000 / 10,000 / 5,000 / 3,333.3. Width 80px; centers derived as `70 + 590 × (i + 0.5) / 4` = 143.75 / 291.25 / 438.75 / 586.25. Bars over the limit red `#e74c3c`, the 6-prefix bar green `#008300`; fill 0.80 alpha, 2px border.
- **Value labels (bold 13px, bar color, centered 6px above the bar):** the computed value formatted with at most one decimal — "20,000", "10,000", "5,000", "3,333.3".
- **Category labels (12px `#444`, centered at y=266):** "1 prefix", "2 prefixes", "4 prefixes", "6 prefixes".
- **Verdict marks (bold 12px, centered at y=284):** red `#e74c3c` "over the limit" under the first three bars; green `#008300` under the fourth reading "fits, 166.7/s spare", where the spare is computed as `3500 − 20000 / 6` and rounded to one decimal.
- **Limit line:** solid 2px `#1a5276` at `py(3500)` across the plot, bold 12px `#1a5276` label "limit: 3,500 writes/sec per prefix" centered at (365, `py(3500) − 6`).
- **Annotation (bold 13px violet `#4a3aa7`, centered at (365, 46)), computed in JS:** "20,000 ÷ 3,500 = 5.71 → round up to 6". It sits at y=46 rather than y=52 to clear the value label above the tall 1-prefix bar, whose baseline is `py(20000) − 6` ≈ 61.
- **Caption (12px `#444`, bottom right at y=296):** "3,500 is a documented figure; 20,000/sec is illustrative; divisions exact".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `23-tutorials/CLAUDE.md`), matching the sibling `11-cross-region-replication.html` in this folder. Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then **three** `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label). Section two's callout uses the label "Common mistake:".
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links, no cross-page links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms). Helpers `roundRect`, `arrowHead` and `rgba` as in page 11.
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Red `#e74c3c` is reserved for genuine over-limit states: the demand marker lines in `c1` and the three over-limit bars in `c3`.
- **Bullet length — this is the deliberate calibration of the page.** Bullets run roughly 75–90 characters, including the bold lead term, so each holds one line at the 50/50 split while still carrying a full clause rather than a clipped stub. An earlier pass clipped them to ~55–70 characters and was rejected for compromising quality. Do not shorten them further, and do not pad them back out to the folder's ~90–100 default either.
- **Register and vocabulary.** Plain technical English for a first-time reader, and fewer product and API names than the earlier draft: say "writes" and "reads", never PUT / COPY / POST / DELETE / GET / HEAD / LIST; say "the store" rather than naming the service in every sentence; say "a slice of the sorted key list" rather than "a partitioned index range"; say "over the limit" rather than naming an HTTP status code. No analogies, no invented scenes, no named tools. Precise API names belong on the sibling reference pages.
- **Deliberate omissions — do not reintroduce on regeneration.**
  - **No warm-up / throttling-response section.** A fourth section covered the cold start on a new prefix, the minutes-long automatic split, the HTTP 503 throttle signal, retries with exponential backoff and jitter, and a 2,000-task batch job writing under one date path, with a `c4` offered-vs-served burst timeline. It was cut for turning a tutorial into a reference course. That material belongs with the throttling and retry pages, not here.
  - **No 4-section layout.** Three sections, 7–8 short bullets each, one canvas per section (`c1`, `c2`, `c3`).
  - **No hardware or tuning advice.** Instance sizes, connection pools and multipart tuning are out of scope for this page.
- **Data:** all values are hardcoded literals, no randomness anywhere.
  - **Documented behaviour the page stands on:** roughly 3,500 writes and 5,500 reads per second per prefix; the limit applies per prefix rather than per bucket; no limit on how many prefixes one bucket may hold; no default whole-bucket request cap; the store partitions the sorted key list on boundaries it chooses and splits a busy range automatically; the delimiter-based folder view is a display convention, not a partition boundary.
  - **Illustrative and labelled as such:** the 20,000 writes/sec and 40,000 reads/sec demand, the sample key names, and the two cut points in `c2`.
  - **Exact arithmetic, computed in JS at render time:** 20,000 ÷ 3,500 = 5.71 → 6; 40,000 ÷ 5,500 = 7.27 → 8; 20,000 ÷ 6 = 3,333.3; 3,500 − 3,333.3 = 166.7; 20,000 ÷ 4 = 5,000 (1,500 over 3,500); 20,000 ÷ 2 = 10,000; 6 × 3,500 = 21,000; 20,000 − 3,500 = 16,500 refused on one prefix.
  - **Computed geometry:** `c1` uses `px(v) = 170 + v / 44000 × 500`; `c2` uses `bx(b) = 60 + b / 256 × 620`; `c3` uses `py(v) = 248 − v / 21000 × 190` and derives bar centers from `70 + 590 × (i + 0.5) / 4`. No magic pixel constants for data positions.
