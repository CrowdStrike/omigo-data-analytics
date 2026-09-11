# Link Aggregator

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Link Aggregator

**Subtitle:** Designing a link-aggregator discussion site — a 50,000-comment tree can't be sorted per read, and a front page that rewards fresh-and-rising needs scores that decay with age; both are solved by precomputing

## Fifty Thousand Comments, One Tree

**Tags:** `core idea` (blue), `comment trees` (green), `pagination` (orange)

- **The thread** — a viral post collects 50,000 nested comments while readers keep refreshing it
- **The naive read** — walking and sorting the whole tree per request re-sorts 50,000 rows per reader
- **Store the tree** — each comment stores its parent id; the tree shape is data, never recomputed
- **Precompute the order** — a background job flattens the tree once per sort (best, top, new)
- **Paginate the tree** — render the first 200; "load more replies" is a pointer into the stored order

*Example (italic):* A reader opens the 50,000-comment thread and gets the top 200 by the precomputed "best" order in one fetch; "load more replies" fetches the next slice by stored rank — the tree is never re-sorted at read time.

**Key point:** Big comment trees get cheap by moving the sort off the read path — store the tree once, precompute one flattened ordering per sort, and make "load more replies" a cursor into that precomputed order.

### Visualization (canvas `c1`, 720×300)

Tree diagram: a post with 50,000 comments rendered as a small visible slice (root, three top comments, two replies) plus gray "load more" stub boxes that are pointers into the precomputed ordering.

- **Title (bold 15px, `#1a5276`, top center):** "One Thread, Precomputed Slices: Render 200, Point at 49,800".
- **Root box:** blue `#2a78d6` rounded box centered at x=360, y=62, 220px wide, 36px tall, fill `rgba(42,120,214,0.15)`, 12px `#2c3e50` text "post — 50,000 comments".
- **Depth-1 row (boxes 150px wide, 34px tall, 8px radius, y=130):** blue boxes at x=110 "comment #1 (best)", x=300 "comment #2", x=490 "comment #3"; 2px `#6b7280` connector lines from the root's bottom edge to each box top.
- **Depth-2 row (y=192):** two blue boxes under comment #1 at x=60 "reply 1.1" and x=230 "reply 1.2" (130px wide), connectors from comment #1.
- **Stub boxes (gray dashed 2px `#6b7280` border, fill `#f8f9fa`, 12px `#6b7280` text):** at x=400, y=192, 200px wide: "load more — next replies by stored rank"; at x=490, y=248, 190px wide: "load more — comments #4–#200".
- **Annotation (bold 13px green `#008300`, near x=60, y=262):** "the sort ran once, in the background — reads just follow pointers".
- **Caption (12px `#444`, bottom right):** "comment counts illustrative".

## Hot: Log of Votes Against the Clock

**Tags:** `worked example` (blue), `hot ranking` (green), `decay` (orange)

- **The formula** — the old open-source hot score is log10 of net votes plus an age bonus for newer posts
- **The unit** — the age bonus grows one point per 45,000 seconds: 12.5 hours equals a factor of 10 in votes
- **Post A** — a fresh post with 100 net votes contributes log10(100) = 2 vote points
- **Post B** — a 25-hour-older post with 10,000 votes has log10(10,000) = 4 but sits 2 age points behind
- **The tie** — a 4 − 2 = 2 point vote lead exactly cancels a 25 h ÷ 12.5 h = 2 point age deficit

*Example (italic):* To outrank the fresh 100-vote post, a 12.5-hour-old post needs 1,000 votes and a 25-hour-old post needs 10,000 — fresh-and-rising beats old-and-big by design.

**Key point:** Hot ranking pits log(votes) against a linear clock — because votes enter through a log, age always wins eventually, so the front page keeps turning over instead of freezing on last week's biggest post.

### Visualization (canvas `c2`, 720×300)

Vertical bar chart: net votes an older post needs to match a fresh post with 100 votes, at four ages; log-feel via hardcoded pixel heights (45px per decade).

- **Title (bold 15px, `#1a5276`, top center):** "Votes Needed to Match a Fresh 100-Vote Post".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; no y gridlines (log-feel is schematic); x = four bars centered at x = 130, 280, 430, 580, width 90.
- **Bars:** ages `["fresh", "12.5 h old", "25 h old", "37.5 h old"]` (12px `#444` labels under each bar), votes needed `[100, 1000, 10000, 100000]`, pixel heights `[45, 90, 135, 180]`; first three bars blue fill `rgba(42,120,214,0.30)` with 2px `#2a78d6` border, last bar orange fill `rgba(217,89,38,0.20)` with 2px `#d95926` border.
- **Labels:** bold 12px value labels above each bar ("100", "1,000", "10,000", "100,000 votes").
- **Annotation (bold 13px violet `#4a3aa7`, near x=180, y=70):** "every 12.5 h of age costs a factor of 10 in votes".
- **Caption (12px `#444`, bottom right):** "heights schematic (1 decade = 45px); vote decades exact per the formula".

## The Front Page Is a Cache, the Viral Thread Is a Hot Key

**Tags:** `where it's used` (blue), `caching` (green), `hot key` (orange)

- **The listing** — each community's page per sort (hot, new, top) is precomputed and refreshed continuously
- **The read** — serving a page fetches a cached, already-ranked id list, then hydrates the posts
- **The viral thread** — one post becomes a hot key: illustratively 92% of a community's reads hit it
- **The defense** — hot keys are served from many cache replicas; one database row can't take the load
- **Eventual counts** — displayed vote totals are cached and slightly stale; the ranking job reads fresher ones

*Example (italic):* During a viral hour, one thread takes 920,000 of the community's 1,000,000 reads per minute — a 92% share on one key, absorbed by cache replicas while the listing refreshes on its own clock.

**Key point:** Reads are served from precomputed, cached artifacts — ranked listings and vote totals — so a viral post stresses the cache tier, which replicates cheaply, instead of the database, which does not.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart: reads per minute per post in one community during a viral hour, showing one post absorbing nearly all traffic.

- **Title (bold 15px, `#1a5276`, top center):** "A Viral Hour: One Post Absorbs 92% of the Community's Reads".
- **Axis:** horizontal 2px `#999` baseline at x=230, bars extend right, max width 440; widths proportional to reads.
- **Rows (top to bottom at y = 70, 120, 170, 220), each with a left-aligned 12px `#444` label at x=20:**
  - "viral thread": red `#e74c3c` solid bar width 440, bold 12px red label "920,000 reads/min"
  - "post #2": blue `#2a78d6` bar width 16, 11px label "34,000"
  - "post #3": blue bar width 12, 11px label "26,000"
  - "post #4": blue bar width 10, 11px label "20,000"
- **Bar style:** 14px tall, blue bars fill `rgba(42,120,214,0.30)` with solid 2px border, red bar solid.
- **Annotation (bold 13px magenta `#d55181`, right side near y=255):** "one cache key, replicated — the database never sees this".
- **Caption (12px `#444`, bottom right):** "reads/min illustrative; shares sum to 1,000,000".

## The Mistake: Exact Live Vote Counts

**Tags:** `common mistake` (red), `consistency` (orange)

- **The trap** — treating the vote count as a bank balance that must be exact and live on every page view
- **Counting on read** — summing the votes table per view turns 1M reads a minute into 1M aggregations
- **Fuzzed anyway** — the displayed score is deliberately fuzzed a little as an anti-cheating measure
- **Cache it** — increment counters asynchronously and refresh the cached total every few seconds
- **The test** — no reader can tell 15,204 from 15,187; spend exactness on money, not karma

*Example (italic):* A page showing "15.2k points" refreshes from a counter cache every few seconds; the true total might be 15,187 or 15,204, and no product behavior depends on which.

**Common mistake:** Spending database consistency on a number that the product deliberately fuzzes and displays rounded. Vote totals are eventual by design — exact live counts buy nothing and cost an aggregation per page view.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: counting votes on every read (database melts) vs asynchronously maintained cached counter (cheap and indistinguishable to readers).

- **Title (bold 15px, `#1a5276`, top center):** "Vote Totals: Count on Every Read vs Cached and Fuzzed".
- **Row 1 (y=95), label 12px `#444` at x=20:** "count on read"; blue `#2a78d6` rounded box at x=170 labeled "page view" (12px), 3px arrow to a red `#e74c3c` box at x=380 labeled "SUM 50,000 vote rows" with bold 12px red "✗ 1M aggregations/min".
- **Row 2 (y=205), label:** "cached counter"; blue box at x=170 "vote arrives", 3px arrow to a green `#008300` box at x=380 labeled "async increment", then arrow to a green box at x=580 labeled "cached total ~15.2k" with bold 12px green "✓ one cache read".
- **Box style:** 130–170px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(231,76,60,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px orange `#d95926`, centered near y=270):** "readers see 15.2k either way — exactness is wasted on display".
- **Caption (12px `#444`, bottom right):** "row and read counts illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); comment counts, reads/min, and vote totals are invented and labeled illustrative; the exact figures are the hot-score decades — log10(100) = 2, log10(10,000) = 4, and one age point per 45,000 seconds (12.5 hours) — which match the widely published open-source hot-ranking formula. Frame everything as a generic link-aggregator design exercise using open-source concepts only; make no claims about any company's internal systems.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
