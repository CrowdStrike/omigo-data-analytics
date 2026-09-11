# Read-Heavy NoSQL Store

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Read-Heavy NoSQL Store

**Subtitle:** A catalog lookup service reads the same items a hundred times for every edit — that one ratio decides the design: spend on the write path so a read is a single cheap hop to an answer that is already shaped

## A Hundred Reads for Every Write

**Tags:** `core idea` (blue), `measure first` (green), `100 : 1` (orange)

- **The service** — an item-lookup API for a product catalog: read constantly, edited rarely
- **The split** — count reads and writes separately; at peak it is 1,000,000 reads/s against 10,000 writes/s
- **The ratio** — that is 100 reads for every write, and the ratio matters far more than either raw number
- **Why first** — the ratio is the design input; caches, replicas, and data shape are all chosen after it
- **The trap** — a single "total QPS" figure hides the split, and the split is the only actionable part

*Example (italic):* Two services with the same total traffic but ratios of 100:1 and 1:1 get completely different designs — same size, opposite shape.

**Key point:** Read-heavy is not a vague adjective, it is a measured ratio. At 100 reads per write, anything you remove from the read path is saved a hundred times over and anything you add to the write path is paid once.

### Visualization (canvas `c1`, 720×300)

Conceptual before/after: the same traffic as one undifferentiated "total QPS" bar, then split into
a read block and the hairline write sliver that the total was hiding.

- **Title (bold 15px, `#1a5276`, top center):** "One Total Hides the Shape of the Traffic".
- **Divider:** vertical dashed `#6b7280` (dash 4/3) at x=360, y=44 to y=274.
- **Panel labels (bold 12px, y=54):** `#6b7280` "one number" centered at x=175; `#1a5276` "the same traffic, split" centered at x=545.
- **Scale:** one shared pixels-per-QPS scale, `s = 150 / 1,000,000`, so the read segment is 150px, the write segment is exactly 1.5px, and both bars total 151.5px — the hairline is the 100:1 ratio drawn to scale, not a stylistic choice.
- **Left bar (x=110, width 130, baseline y=250, height 151.5):** fill `rgba(107,114,128,0.25)`, 2px `#6b7280` border, 12px `#2c3e50` label "total QPS" centered inside at y=184.
- **Left caption (bold 13px `#2c3e50`, centered at x=175, y=270):** "reads and writes lumped".
- **Right bar (x=480, width 130, same baseline y=250):** read segment from y=100 to y=250, fill `rgba(42,120,214,0.30)` with 2px `#2a78d6` border; write segment stacked on top from y=98.5 to y=100, fill `rgba(217,89,38,0.9)` with 1px `#d95926` border.
- **Segment labels:** bold 12px blue `#2a78d6` "reads — 1,000,000/s" right-aligned at x=468, y=188; bold 12px orange `#d95926` "writes — 10,000/s" right-aligned at x=468, y=92, with a 1.5px orange arrow from (472, 90) to (478, 98.5) pointing at the sliver.
- **Right caption (bold 13px `#2c3e50`, centered at x=545, y=270):** "one bar is a hairline".
- **Computed annotation (bold 13px green `#008300`, left-aligned at x=25, y=290):** built at render time from the two plotted values as `fmt(reads) + " ÷ " + fmt(writes) + " = " + (reads/writes) + " reads : 1 write"` (renders 1,000,000 ÷ 10,000 = 100 reads : 1 write).
- **Caption (12px `#444`, right-aligned at x=700, y=290):** "Illustrative Example".

## Paying on the Write Path So a Read Is One Hop

**Tags:** `worked example` (blue), `denormalize` (green), `write amplification` (orange)

- **The read shape** — one item page wants the item, its brand, its categories, its price, all together
- **Normalized** — those pieces live apart, so the read fans out and stitches them at query time
- **Denormalize** — store one document already stitched, and the same read becomes a single lookup
- **The write cost** — an edit now rewrites that whole document plus every index built over it
- **Why that is safe** — the extra work lands on the rare write, and the saving lands on every single read
- **The rule** — precompute the exact shape the query asks for; never join at read time

*Example (italic):* Adding one more secondary index makes each edit slightly more expensive and leaves the read at exactly one seek — a trade you will take every time at 100:1.

**Key point:** Work removed from the read path is multiplied by the read count; work added to the write path is multiplied by the write count. With a hundred-to-one ratio those multipliers are what make denormalizing obviously correct.

### Visualization (canvas `c2`, 720×300)

Flow diagram, no axes. Left: a normalized read fanning out to several places versus a denormalized
read hitting one document. Right: where that saving is paid back — one edit becoming several writes.

- **Title (bold 15px, `#1a5276`, top center):** "Fan Out at Read Time, or Once at Write Time".
- **Divider:** vertical dashed `#6b7280` (dash 4/3) at x=400, y=56 to y=280.
- **Row tags (bold 12px `#6b7280`, left-aligned at x=25):** "normalized" at y=44, "denormalized" at y=206.
- **Normalized row:** blue box "read" at x=25, y=93, 90×38 (8px radius, fill `rgba(42,120,214,0.15)`, 2px `#2a78d6`, 12px `#2c3e50` text); six thin target boxes at x=195, width 130, height 14, tops at y = 56, 75, 94, 113, 132, 151 (fill `rgba(231,76,60,0.14)`, 1px `#e74c3c`), labelled 11px `#2c3e50` "item", "brand", "category", "category", "category", "price"; six 1.5px `rgba(231,76,60,0.6)` arrows from the read box's right edge (115, 112) to each target's left edge.
- **Normalized annotation (bold 12px red `#e74c3c`, centered at x=190, y=186):** "many lookups, stitched per read".
- **Denormalized row:** blue box "read" at x=25, y=214, 90×38; one green box at x=195, y=214, 130×38 (fill `rgba(0,131,0,0.14)`, 2px `#008300`) labelled 12px "one document"; a single 3px `#008300` arrow from (115, 233) to (195, 233).
- **Denormalized annotation (bold 12px green `#008300`, centered at x=190, y=272):** "one lookup, answer already shaped".
- **Right header (bold 13px `#1a5276`, centered at x=563, y=48):** "the write path pays for it".
- **Right flow:** magenta box "edit" at x=430, y=88, 90×34 (fill `rgba(213,81,129,0.14)`, 2px `#d55181`); three orange boxes at x=560, width 140, height 26, tops at y = 64, 98, 132 (fill `rgba(217,89,38,0.14)`, 1px `#d95926`), labelled 11px "rewrite document", "update index", "update index"; three 1.5px `#d95926` arrows from (520, 105) to each box's left edge at its vertical mid.
- **Right annotations:** bold 12px orange `#d95926` "one edit becomes several writes" centered at (563, 186); bold 12px violet `#4a3aa7` two lines centered at (563, 216) and (563, 238): "saving on a read × every read", "cost on a write × the few writes".
- **Caption (12px `#444`, right-aligned at x=700, y=292):** "Illustrative Example".

## The Cache Absorbs the Reads, Nothing Absorbs the Writes

**Tags:** `worked example` (blue), `cache tier` (green), `read replicas` (orange)

- **The cache tier** — an in-memory tier in front of the store answers any read whose key it already holds
- **What arrives** — a 97% hit rate on 1,000,000 reads/s leaves 30,000 QPS for the store to serve
- **Read replicas** — copies of the data behind the cache; the leftover read load splits across them
- **Writes do not split** — every write still lands on one primary, so a replica adds no write capacity
- **The asymmetry** — reads scale by adding machines, writes scale only by sharding or writing less

*Example (italic):* Doubling the replicas halves the read load each one carries and changes the write path not at all — replicas are a read lever only.

**Key point:** Origin load is (1 − hit rate) × read QPS, so the last couple of points of hit rate matter more than the first ninety. Replicas then divide what is left — but only on the read side.

### Visualization (canvas `c3`, 720×300)

Funnel on the left: reads entering, the cache absorbing most of them, the remainder reaching the
store. On the right: reads fanning out across replicas while writes converge on one primary.

- **Title (bold 15px, `#1a5276`, top center):** "Where a Million Reads a Second Actually Go".
- **Divider:** vertical dashed `#6b7280` (dash 4/3) at x=372, y=44 to y=278.
- **Funnel boxes (x=30, width 175, height 40, 8px radius, 12px `#2c3e50` text):** y=70 "1,000,000 read QPS" (fill `rgba(42,120,214,0.15)`, 2px `#2a78d6`); y=150 cache box (fill `rgba(0,131,0,0.14)`, 2px `#008300`) whose label is computed at render as `"cache tier — " + hitPct + "% hit"`; y=230 origin box (fill `rgba(217,89,38,0.14)`, 2px `#d95926`) whose label is computed as `"origin: " + fmt(origin) + " read QPS"`.
- **Funnel arrows:** 2px `#6b7280` arrows at x=117 from (117,110)→(117,150) and (117,190)→(117,230).
- **Arrow labels (12px `#444`, left-aligned at x=212):** "answered in memory" at y=134; "the rest reaches the store" at y=214.
- **Left panel label (bold 12px `#6b7280`, left-aligned at x=30, y=54):** "the cache absorbs most reads".
- **Right header (bold 13px `#1a5276`, centered at x=545, y=54):** "reads fan out, writes do not".
- **Right sources (width 66, height 28, 8px radius, 11px text):** "reads" box at x=395, y=88 (fill `rgba(42,120,214,0.15)`, 2px `#2a78d6`); "writes" box at x=395, y=196 (fill `rgba(213,81,129,0.14)`, 2px `#d55181`).
- **Replicas:** three boxes at x=520, width 150, height 26, tops y = 68, 100, 132 (fill `rgba(217,89,38,0.14)`, 1px `#d95926`, 11px text "read replica"); three 1.5px `#d95926` arrows from (461, 102) to each box's left edge at its vertical mid (y = 81, 113, 145).
- **Replica annotation (bold 12px orange `#d95926`, centered at x=595, y=176):** "add replicas → more read capacity".
- **Primary:** magenta box at x=520, y=196, 150×28 (fill `rgba(213,81,129,0.14)`, 2px `#d55181`, 11px "single primary"); one 2.5px `#d55181` arrow from (461, 210) to (520, 210).
- **Primary annotations:** bold 12px magenta `#d55181` "add replicas → no write capacity" centered at (595, 244); 12px violet `#4a3aa7` "every write lands on one node" centered at (595, 266).
- **Computed annotation (bold 13px green `#008300`, left-aligned at x=30, y=292):** `hitPct + "% hit leaves " + fmt(origin) + " of " + fmt(reads)` where `reads = 1000000`, `origin = 30000`, and `hitPct = (100 * (1 - origin / reads)).toFixed(1)` (renders 97.0% hit leaves 30,000 of 1,000,000).
- **Caption (12px `#444`, right-aligned at x=700, y=292):** "Illustrative Example".

## The Hot Key Expires and Everyone Runs at Once

**Tags:** `common mistake` (red), `stampede` (orange), `staleness` (green)

- **The blind spot** — a high average hit rate says nothing about what one popular key does when it expires
- **The stampede** — the moment that key's entry is gone, every request for it misses and queries the store
- **Single flight** — let one request fetch and make the rest wait on its result; the burst collapses to one query
- **Also needed** — stagger expiry times so a whole range of keys does not go cold on the same second
- **The tradeoff** — keeping entries longer buys fewer origin queries and staler reads; no setting gives both

*Example (italic):* The same knob moved one way cuts store load and moves stale answers further behind; moved the other way it does the reverse.

**Common mistake:** Reading a high hit rate as a steady state. Averages hide bursts — one hot key expiring concentrates its entire arrival rate onto the store in a single refill window, and only coalescing the duplicate requests removes it.

### Visualization (canvas `c4`, 720×300)

Two conceptual panels of the same expiry moment: without coalescing every waiting request reaches
the store; with single flight only one does. A tradeoff line runs underneath both.

- **Title (bold 15px, `#1a5276`, top center):** "One Hot Key Expires: A Herd, or One Query".
- **Divider:** vertical dashed `#6b7280` (dash 4/3) at x=378, y=48 to y=262.
- **Panel headers:** bold 12px red `#e74c3c` "no coalescing" centered at (195, 60); bold 12px green `#008300` "single flight" centered at (548, 60).
- **Expiry boxes (width 150, height 30, 8px radius, 12px `#2c3e50` text "hot key expires"):** left at x=120, y=72 (fill `rgba(231,76,60,0.12)`, 2px `#e74c3c`); right at x=473, y=72 (fill `rgba(0,131,0,0.12)`, 2px `#008300`).
- **Request markers:** 12px `#6b7280` label "requests for that key" centered at (195, 126) and (548, 126); left panel nine filled `#e74c3c` circles radius 5 at y=142, x = 60 + 32i for i = 0..8; right panel nine circles radius 5 at y=142, x = 420 + 24i, the first filled `#008300` and the other eight filled `rgba(107,114,128,0.45)`.
- **Left arrows:** nine 1px `rgba(231,76,60,0.55)` lines from each circle (x, 149) converging on the origin box top center (195, 194).
- **Right arrows:** one 2.5px `#008300` arrow from (420, 149) to (500, 194); 12px `#6b7280` label "the others wait and reuse it" centered at (575, 172).
- **Origin boxes (width 150, height 32, 8px radius, 12px text "origin store"):** left at x=120, y=196 (fill `rgba(231,76,60,0.12)`, 2px `#e74c3c`); right at x=473, y=196 (fill `rgba(0,131,0,0.12)`, 2px `#008300`).
- **Panel footers:** bold 12px red `#e74c3c` "every waiting request queries the store" centered at (195, 250); bold 12px green `#008300` "exactly one query reaches the store" centered at (548, 250).
- **Tradeoff line (bold 12px orange `#d95926`, centered at x=360, y=278):** "keep entries longer → fewer origin queries, staler answers".
- **Caption (12px `#444`, right-aligned at x=700, y=294):** "Illustrative Example".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then exactly four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%) holding one canvas. No lead section, no derivation table, no `table.model` / `.model-note` / `.model-lead` styles.
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Shared helpers: `roundRect(ctx,x,y,w,h,r)`, `arrow(ctx,x1,y1,x2,y2,color,width)`, `fmt(n)` (thousands separators), `dashedLine(ctx,x1,y1,x2,y2)` (1px `#6b7280`, dash 4/3), and `labelBox(ctx,x,y,w,h,fill,border,borderWidth,text,fontSize)` (8px-radius rounded box with centered text). Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** the charts are conceptual diagrams, not plotted series — no randomness anywhere and no `Math.random()`. Only three figures appear on the page: 1,000,000 reads/s, 10,000 writes/s, and 30,000 QPS reaching the store at a 97% hit rate. They are invented, labeled "Illustrative Example", and every statistic printed beside them (the 100:1 ratio in `c1`, the hit percentage in `c3`) is computed in JS at render time from those same variables, so text and chart cannot drift. The design content — the read/write ratio as the sizing input, denormalizing to a single-hop read, a cache tier absorbing reads, replicas scaling reads but not writes, and request coalescing against a hot-key stampede — is standard published practice, described without naming a product.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
