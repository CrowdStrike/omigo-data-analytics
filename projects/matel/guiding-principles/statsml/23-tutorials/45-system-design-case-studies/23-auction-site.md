# Auction Site

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Auction Site

**Subtitle:** The great monolith split, as large auction sites have publicly told it: one giant application becomes function pools and sharded databases — rebuilt piece by piece while the site kept selling

## Three Architectures for One Auction Site

**Tags:** `core idea` (blue), `architecture eras` (green), `public history` (orange)

- **The garage era** — the site begins as one program and one database; every feature lives in one place
- **The v2 era** — growth answer: one giant compiled application, the same build deployed to every server
- **The strain** — every team edits one codebase; a one-line change means rebuilding and shipping everything
- **The v3 split** — the application divides by feature domain: search, items, bidding, users
- **The lesson** — none of the three was wrong; each architecture was right for the scale of its own era

*Example (italic):* The same site runs on three architectures in one decade while daily listings climb from 5 thousand to 8 million (illustrative) — and it rewrites itself in flight each time.

**Key point:** Architecture is a function of scale, not taste — the monolith that strangled the site at 8 million listings a day is the same design that shipped it fastest at 5 thousand.

### Visualization (canvas `c1`, 720×300)

Growth line of daily listings across a decade with vertical era boundaries splitting the plot into the three architecture eras.

- **Title (bold 15px, `#1a5276`, top center):** "Each Architecture Was Right for Its Scale".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = years 1996 to 2006 at 60px per year, 12px `#444` tick labels every 2 years; y = daily listings 0 to 8M, gridlines `#e5e9ef` at 2M/4M/6M (y = 200/155/110), 12px `#444` labels "2M"/"4M"/"6M" at x=50.
- **Growth line:** blue `#2a78d6` 3px line through years `[1996, 1997, 1998, 1999, 2000, 2001, 2002, 2003, 2004, 2005, 2006]`, daily listings in thousands `[5, 20, 80, 250, 600, 1200, 2200, 3400, 4800, 6400, 8000]`.
- **Era boundaries:** vertical dashed `#6b7280` (dash 4/3) lines at 1999 (x=240) and 2002 (x=420), full plot height.
- **Era labels (12px, y=42):** "v1: one script" mute `#6b7280` centered at x=150; "v2: one giant app" blue `#2a78d6` centered at x=330; "v3: functional segmentation" green `#008300` centered at x=540.
- **Annotation (bold 13px green `#008300`, near x=110, y=130):** "each era was right for its scale".
- **Caption (12px `#444`, bottom right):** "listing counts illustrative; era boundaries approximate".

## Split by Function, Then Shard Within Function

**Tags:** `worked example` (blue), `functional split` (green), `sharding` (orange)

- **Code split** — one application becomes function pools — search, items, bidding, users — deployed alone
- **First data cut** — the single database divides by function: an items DB, a bids DB, a users DB
- **Second cut** — within a function, shard: items spread across 20 databases keyed by item id (illustrative)
- **The routing** — the application computes shard = item_id mod 20 and connects only to that database
- **Hand-check** — item 8,314,507 → 8,314,507 mod 20 = 7 → items shard 7 alone answers the query

*Example (italic):* A bid on item 8,314,507 enters the bidding pool, which routes to items shard 7 (8,314,507 mod 20 = 7) — the other 19 item shards never see the request.

**Key point:** These are two different cuts — splitting by function separates unrelated load; sharding within a function spreads one kind of load across copies of the same schema.

### Visualization (canvas `c2`, 720×300)

Three-tier flow diagram: one app-plus-DB box fanning into four function pools, with the items pool fanning further into item shards, and a routing annotation for the hand-check.

- **Title (bold 15px, `#1a5276`, top center):** "One App, One DB → Four Function Pools → Twenty Item Shards".
- **Box style:** rounded 8px radius, 12px `#2c3e50` text; blue fill `rgba(42,120,214,0.15)` with 2px `#2a78d6` edge unless stated.
- **Tier 1:** one box 220×36 centered at (360, 62) labeled "one application + one database".
- **Tier 2 (centers on y=140), four boxes 128×36 at x = 130, 285, 440, 595:** "search", "items", "bidding", "users"; 11px `#6b7280` label "own DB" 12px under each box; the "items" box gets a green `#008300` 2px edge instead of blue.
- **Tier 3 (centers on y=232), five boxes 76×32 at x = 180, 280, 380, 480, 580:** "shard 0", "shard 1", "…", "shard 7", "shard 19"; "shard 7" filled `rgba(0,131,0,0.12)` with 2px `#008300` edge, others blue-tinted with 1px `#2a78d6` edge.
- **Arrows:** 2px `#2c3e50` lines from tier-1 box bottom to each tier-2 box top; 2px `#008300` lines from the "items" box bottom fanning to all five tier-3 boxes.
- **Routing annotation (bold 12px green `#008300`, near x=470, y=190):** "item 8,314,507 mod 20 = 7 → shard 7".
- **Caption (12px `#444`, bottom right):** "shard count illustrative".

## The Joins Don't Survive the Split

**Tags:** `why it matters` (blue), `joins & transactions` (orange)

- **The old page** — one SQL join across items, bids, and users built the auction page in a single query
- **After the split** — those three tables live in three databases; the join has nowhere left to run
- **The move** — the application now makes 3 calls and stitches the rows together in its own code
- **Transactions too** — a bid can no longer update item and user rows inside one ACID transaction
- **The trade** — the publicly stated answer: avoid distributed transactions, reconcile asynchronously

*Example (italic):* The auction page that was 1 query against 1 database becomes 3 calls against 3 databases, merged in application code.

**Key point:** The database split hurts more than the code split — cross-entity joins and transactions don't migrate, they disappear, and the application inherits their job.

### Visualization (canvas `c3`, 720×300)

Two-row flow diagram: the auction page before the split (one join, one transaction) vs after (three calls converging on an app-side join, no cross-DB transaction).

- **Title (bold 15px, `#1a5276`, top center):** "The Auction Page: One Join Before, Three Calls After".
- **Box style:** rounded 8px radius, 12px `#2c3e50` text; fills `rgba(42,120,214,0.15)` blue / `rgba(0,131,0,0.12)` green / `rgba(217,89,38,0.12)` orange.
- **Row 1 (centered on y=100), label 12px `#444` at x=15:** "before: one DB"; blue box 230×40 centered at (250, 100) labeled "items JOIN bids JOIN users"; 3px `#2c3e50` arrow to green box 190×40 centered at (530, 100) labeled "1 query, 1 transaction" with bold 12px green `#008300` "✓" beside it.
- **Row 2 (centered on y=210), label 12px `#444` at x=15:** "after: three DBs"; three blue boxes 100×34 centered at (150, 210), (270, 210), (390, 210) labeled "items call", "bids call", "users call"; 2px `#2c3e50` arrows from all three converging into an orange box 160×40 centered at (555, 210) labeled "join in app code".
- **Warning (bold 12px red `#e74c3c`, near x=430, y=252):** "no cross-DB transaction".
- **Annotation (bold 13px orange `#d95926`, centered near y=282):** "the joins didn't move — they disappeared; the app inherited the work".

## The Big-Bang Rewrite That Never Ships

**Tags:** `common mistake` (red), `incremental migration` (green)

- **The temptation** — freeze features, rewrite the whole system, cut over on one heroic weekend
- **The reality** — traffic keeps doubling during the freeze, so the rewrite chases a moving target
- **The incremental way** — migrate one function at a time, old and new serving live traffic side by side
- **The dial** — route a slice of traffic to the new pool, watch, widen; rollback is turning the dial down
- **The finish** — the old application dies by starvation: its last function moves and nothing calls it

*Example (italic):* Over months 0 to 24 the old application's traffic share steps down 100% → 85% → 60% → 40% → 22% → 8% → 0% as search, items, bidding, then users migrate (illustrative).

**Common mistake:** Treating re-architecture as a project with a cutover date. The publicly told lesson is the opposite: you re-architect while the plane is flying — incremental migration, never a big-bang rewrite.

### Visualization (canvas `c4`, 720×300)

Stacked area chart of 24 months of migration: traffic share on the old application (blue, shrinking) vs the new function pools (green, growing), total constant at 100%.

- **Title (bold 15px, `#1a5276`, top center):** "The Old App Dies by Starvation, Not by Cutover".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = months 0 to 24 at 25px per month, 12px `#444` tick labels "0"–"24" every 4 months; y = traffic share 0 to 100%, gridlines `#e5e9ef` at 25/50/75, 12px `#444` labels at x=50.
- **New-pools area (bottom):** green fill `rgba(0,131,0,0.30)` under a 2px `#008300` line through months `[0, 4, 8, 12, 16, 20, 24]`, share % `[0, 15, 40, 60, 78, 92, 100]`.
- **Old-app area (top):** blue fill `rgba(42,120,214,0.25)` between the green line and the constant 100% top, 2px `#2a78d6` upper edge.
- **Migration labels (11px `#6b7280`, along y=60):** "search" at x≈month 4, "items" at x≈month 9, "bidding" at x≈month 14, "users" at x≈month 19.
- **Band labels:** bold 12px blue `#2a78d6` "still on old app" at (x≈month 5, y≈110); bold 12px green `#008300` "moved to function pools" at (x≈month 17, y≈200).
- **Annotation (bold 12px violet `#4a3aa7`, near x=month 10, y=80):** "total never dips below 100% — the site keeps selling".
- **Caption (12px `#444`, bottom right):** "shares and timeline illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); listing volumes, era boundary years, shard counts, item and traffic-share numbers, and the 24-month migration timeline are invented and labeled illustrative. The three-era arc (early monolith → one giant application deployed everywhere → functional segmentation with function-split then sharded databases) and the prefer-asynchronous-reconciliation-over-distributed-transactions lesson come from auction-site re-architecture histories publicly presented at conferences — internal specifics are kept generic and no undocumented behavior is attributed to any company.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
